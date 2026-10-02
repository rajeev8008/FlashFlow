import asyncio
import logging
from contextlib import asynccontextmanager, suppress
from fastapi import FastAPI, WebSocket, HTTPException
from redis.asyncio import Redis
from sqlalchemy import select, text

from .config import settings
from .database import Session
from .models import ProductRow
from .schemas import Product
from .stream import ConnectionManager, InventoryConsumer

manager = ConnectionManager()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.basicConfig(level=logging.INFO)
    worker = InventoryConsumer(manager)
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


app = FastAPI(title="FlashFlow API", version="0.2.0", lifespan=lifespan)


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
    return {**{key: app.state.worker.counters[key] for key in ("processed", "failed", "duplicate", "stale", "dlq")}, "active_websocket_clients": len(manager.clients)}


@app.websocket("/ws")
async def websocket(socket: WebSocket):
    await manager.serve(socket)
