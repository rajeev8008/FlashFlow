import asyncio
import logging
import secrets
from contextlib import asynccontextmanager, suppress
from fastapi import FastAPI, WebSocket, HTTPException, Header, Depends
from redis.asyncio import Redis
from sqlalchemy import select, text

from .config import settings
from .database import Session
from .models import ProductRow
from .schemas import Product
from .stream import ConnectionManager, InventoryConsumer
from .resilience import Catalog, Faults

manager = ConnectionManager()
faults = Faults()
catalog = Catalog(faults)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.basicConfig(level=logging.INFO)
    worker = InventoryConsumer(manager, faults)
    app.state.worker = worker
    task = asyncio.create_task(worker.run())
    app.state.consumer_task = task
    try:
        yield
    finally:
        for queue in tuple(manager.clients):
            while not queue.empty():
                queue.get_nowait()
            queue.put_nowait(None)
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
        await catalog.redis.aclose()


app = FastAPI(title="FlashFlow API", version="0.4.0", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, str]:
    if app.state.consumer_task.done():
        raise HTTPException(503, "inventory consumer stopped")
    async with Session() as session:
        await session.execute(text("SELECT 1"))
    redis = Redis.from_url(settings.redis_url)
    try:
        await redis.ping()
    finally:
        await redis.aclose()
    return {"status": "ok", "postgres": "ok", "redis": "ok"}


@app.get("/products", response_model=list[Product])
async def products(limit: int = 100) -> list[ProductRow]:
    async with Session() as session:
        return list((await session.scalars(select(ProductRow).order_by(ProductRow.product_id).limit(min(max(limit, 1), 500)))).all())


@app.get("/metrics")
async def metrics():
    return {**{key: app.state.worker.counters[key] for key in ("processed", "failed", "duplicate", "stale", "dlq")}, "active_websocket_clients": len(manager.clients), "inventory_circuit": catalog.breaker.describe(), "consumer_paused": faults.consumer_paused}


@app.get("/inventory")
async def inventory():
    return await catalog.snapshot(consumer_available=not app.state.consumer_task.done())


def require_chaos(authorization: str | None = Header(None)):
    if settings.app_env != "development" or not settings.enable_chaos or not settings.chaos_token:
        raise HTTPException(404, "Not found")
    if not secrets.compare_digest(authorization or "", f"Bearer {settings.chaos_token}"):
        raise HTTPException(403, "Invalid development control token")


@app.get("/dev/faults", dependencies=[Depends(require_chaos)])
async def fault_state():
    return {"faults": faults.model_dump(), "breaker": catalog.breaker.describe()}


@app.post("/dev/faults", dependencies=[Depends(require_chaos)])
async def set_faults(update: Faults):
    for key, value in update.model_dump().items():
        setattr(faults, key, value)
    return await fault_state()


@app.post("/dev/faults/invalid-event", dependencies=[Depends(require_chaos)])
async def inject_invalid_event():
    await asyncio.wait_for(app.state.worker.producer.send_and_wait(settings.kafka_inventory_topic, key=b"development-invalid", value=b'{"invalid":true}'), 3)
    return {"status": "queued", "destination": settings.kafka_dlq_topic}


@app.post("/dev/faults/disconnect", dependencies=[Depends(require_chaos)])
async def disconnect_clients():
    for queue in tuple(manager.clients):
        while not queue.empty():
            queue.get_nowait()
        queue.put_nowait(None)
    return {"status": "clients interrupted"}


@app.websocket("/ws")
async def websocket(socket: WebSocket):
    await manager.serve(socket)
