from fastapi import FastAPI
from redis.asyncio import Redis
from sqlalchemy import select, text

from .config import settings
from .database import Session
from .models import ProductRow
from .schemas import Product

app = FastAPI(title="FlashFlow API", version="0.1.0")


@app.get("/health")
async def health() -> dict[str, str]:
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
