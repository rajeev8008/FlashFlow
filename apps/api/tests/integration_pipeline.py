"""Real database batch atomicity, redelivery and monotonic Redis cache checks."""
import asyncio
from uuid import uuid4
from sqlalchemy import select
from redis.asyncio import Redis

from flashflow.database import Session, engine
from flashflow.config import settings
from flashflow.inventory import process_batch
from flashflow.models import ProductRow, ProcessedEvent
from flashflow.schemas import EventEnvelope, EventType, Product
from flashflow.seed import build_products
from flashflow.stream import CACHE_UPDATE


async def main():
    row = build_products(1)[0]
    row.product_id, row.category = str(uuid4()), 'Test'
    async with Session() as session, session.begin():
        session.add(row)
    first = EventEnvelope(product_id=row.product_id, event_type=EventType.STOCK_RESERVED,
        source='test', payload={'quantity': 1, 'version': 2})
    second = first.model_copy(update={'event_id': uuid4(), 'payload': {'quantity': 1, 'version': 3}})
    try:
        results = await process_batch([first, second, second])
        assert [outcome for _, outcome in results] == ['processed', 'processed', 'duplicate']
        assert results[-1][0].reserved_stock == 2
        replay = await process_batch([first, second])
        assert all(outcome == 'duplicate' for _, outcome in replay)
        valid = first.model_copy(update={'event_id': uuid4(), 'payload': {'quantity': 1, 'version': 4}})
        bad = first.model_copy(update={'event_id': uuid4(), 'payload': {'quantity': 1, 'version': 6}})
        try:
            await process_batch([valid, bad])
            raise AssertionError('invalid batch accepted')
        except ValueError:
            pass
        async with Session() as session:
            stored = await session.get(ProductRow, row.product_id)
            assert stored.version == 3 and stored.reserved_stock == 2
            assert await session.get(ProcessedEvent, str(valid.event_id)) is None
            latest = Product.model_validate(stored)
        redis = Redis.from_url(settings.redis_url)
        key = f'product:{row.product_id}'
        try:
            assert await redis.eval(CACHE_UPDATE, 1, key, latest.model_dump_json()) == 1
            older = latest.model_copy(update={'version': 2, 'reserved_stock': 1})
            assert await redis.eval(CACHE_UPDATE, 1, key, older.model_dump_json()) == 0
            assert Product.model_validate_json(await redis.get(key)).version == 3
        finally:
            await redis.delete(key)
            await redis.aclose()
        print('Verified: atomic batch rollback, in-batch duplicates, redelivery, monotonic Redis revisions.')
    finally:
        from sqlalchemy import delete
        from flashflow.models import PricingDecision
        async with Session() as session, session.begin():
            for model in (PricingDecision, ProcessedEvent, ProductRow):
                await session.execute(delete(model).where(model.product_id == row.product_id))
        await engine.dispose()


if __name__ == '__main__':
    asyncio.run(main())
