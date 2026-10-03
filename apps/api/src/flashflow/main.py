import asyncio
import logging
import secrets
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, HTTPException, Header, Depends
from redis.asyncio import Redis
from sqlalchemy import select, text

from .config import settings
from .database import Session, engine
from .models import ProductRow, PricingDecision
from uuid import UUID
from fastapi.encoders import jsonable_encoder
from .schemas import Product
from .stream import ConnectionManager, ConsumerGroup
from .resilience import Catalog, Faults
from .observability import Metrics
from .timing import timings

manager = ConnectionManager()
faults = Faults()
catalog = Catalog(faults)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.basicConfig(level=settings.logging_level)
    worker = ConsumerGroup(manager, faults)
    app.state.worker = worker
    task = asyncio.create_task(worker.run())
    app.state.consumer_task = task
    app.state.metrics = Metrics(worker, manager)
    metrics_task = asyncio.create_task(app.state.metrics.run())
    try:
        yield
    finally:
        for queue in tuple(manager.clients):
            while not queue.empty():
                queue.get_nowait()
            queue.put_nowait(None)
        worker.stop()
        metrics_task.cancel()
        await asyncio.gather(metrics_task, return_exceptions=True)
        try:
            await asyncio.wait_for(task, settings.shutdown_timeout_seconds)
        except (TimeoutError, asyncio.CancelledError):
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await catalog.redis.aclose()
        await engine.dispose()


app = FastAPI(title="FlashFlow API", version="0.6.0", lifespan=lifespan)


@app.get("/pricing/decisions")
async def pricing_decisions(product_id: UUID | None = None, limit: int = 50):
    query = select(PricingDecision).order_by(PricingDecision.created_at.desc()).limit(min(max(limit, 1), 200))
    if product_id:
        query = query.where(PricingDecision.product_id == str(product_id))
    async with Session() as session:
        rows = (await session.scalars(query)).all()
        return [{column.name: jsonable_encoder(getattr(row, column.name)) for column in PricingDecision.__table__.columns} for row in rows]


@app.get("/health")
async def health() -> dict[str, str]:
    if app.state.consumer_task.done():
        raise HTTPException(503, "inventory consumer stopped")
    async def check_database():
        async with Session() as session:
            await session.execute(text("SELECT 1"))
    await asyncio.wait_for(check_database(), 2)
    redis = Redis.from_url(settings.redis_url, socket_timeout=1, socket_connect_timeout=1)
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
    return {**{key: app.state.worker.counters[key] for key in ("processed", "failed", "duplicate", "stale", "dlq", "kafka_received", "pricing_received", "completed", "inventory_completed")},
            **app.state.metrics.sample, "stage_latencies": timings.snapshot(), "active_websocket_clients": len(manager.clients), "websocket_messages": manager.counters["sent"],
            "slow_client_disconnects": manager.counters["slow_disconnects"], "redis_cache_reads": catalog.cache_reads,
            "redis_cache_hits": catalog.cache_hits, "redis_cache_hit_rate": catalog.cache_hits / catalog.cache_reads if catalog.cache_reads else None,
            "inventory_circuit": catalog.breaker.describe(), "consumer_paused": faults.consumer_paused,
            "consumer_instances": settings.consumer_instances, "consumer_batch_size": settings.consumer_batch_size,
            "consumer_batch_enabled": settings.consumer_batch_enabled,
            "failed_socket_sends": manager.counters["failed_sends"],
            "disconnected_clients": manager.counters["disconnected"],
            "server_now": datetime.now(timezone.utc).isoformat()}


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
