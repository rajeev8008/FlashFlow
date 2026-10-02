"""Run inside the API container against the running Compose stack.

docker compose exec api python tests/integration_phase2.py
"""
import asyncio
import json
from datetime import datetime, timezone
from uuid import uuid4

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from redis.asyncio import Redis
from sqlalchemy import select
from websockets.asyncio.client import connect

from flashflow.config import settings
from flashflow.database import Session
from flashflow.inventory import process_event
from flashflow.models import ProductRow, ProcessedEvent
from flashflow.schemas import EventEnvelope, EventType, Product


async def main():
    # A separate test product avoids racing the simulator's catalog.
    identity = str(uuid4())
    async with Session() as session, session.begin():
        row = ProductRow(product_id=identity, name="Integration probe", category="Test", base_price=10,
                         current_price=10, stock=10, reserved_stock=0, sales_velocity=0,
                         status="ACTIVE", last_updated=datetime.now(timezone.utc), version=1)
        session.add(row)
    producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)
    dlq = AIOKafkaConsumer(settings.kafka_dlq_topic, bootstrap_servers=settings.kafka_bootstrap_servers,
                           group_id=f"test-{uuid4()}", auto_offset_reset="earliest")
    redis = Redis.from_url(settings.redis_url)
    await producer.start()
    await dlq.start()
    try:
        async with connect("ws://localhost:8000/ws") as first, connect("ws://localhost:8000/ws") as second:
            event = EventEnvelope(event_type=EventType.STOCK_RESERVED, product_id=identity, source="integration", payload={"quantity": 2, "version": 2})
            await producer.send_and_wait(settings.kafka_inventory_topic, key=identity.encode(), value=event.model_dump_json().encode())
            async def receive(socket):
                while True:
                    message = json.loads(await asyncio.wait_for(socket.recv(), 20))
                    if message["type"] == "heartbeat":
                        await socket.send("pong")
                    elif message.get("event_id") == str(event.event_id):
                        return message["product"]
            snapshots = await asyncio.wait_for(asyncio.gather(receive(first), receive(second)), 30)
            assert all(p["reserved_stock"] == 2 and p["version"] == 2 for p in snapshots)
            cached = Product.model_validate_json(await redis.get(f"product:{identity}"))
            assert cached.reserved_stock == 2
            _, outcome = await process_event(event)
            assert outcome == "duplicate"
            await producer.send_and_wait(settings.kafka_inventory_topic, key=identity.encode(), value=event.model_dump_json().encode())
            assert (await receive(first))["reserved_stock"] == 2
            stale = event.model_copy(update={"event_id": uuid4()})
            _, outcome = await process_event(stale)
            assert outcome == "stale"
            marker = f"invalid-{uuid4()}".encode()
            await producer.send_and_wait(settings.kafka_inventory_topic, key=identity.encode(), value=marker)
            async def find_dlq():
                async for record in dlq:
                    failure = json.loads(record.value)
                    if failure["raw_value_hex"] == marker.hex():
                        assert failure["attempts"] == settings.consumer_max_attempts
                        return
            await asyncio.wait_for(find_dlq(), 30)
            async with Session() as session:
                stored = await session.scalar(select(ProductRow).where(ProductRow.product_id == identity))
                assert stored.reserved_stock == 2 and stored.version == 2
        print("PASS: Kafka -> PostgreSQL -> Redis -> two WebSocket clients; duplicate, stale, and DLQ checks")
    finally:
        await producer.stop()
        await dlq.stop()
        await redis.aclose()


if __name__ == "__main__":
    import sys
    async def restart_check():
        async with Session() as session:
            row = await session.scalar(select(ProductRow).where(ProductRow.name == "Integration probe").order_by(ProductRow.last_updated.desc()))
            before = Product.model_validate(row)
            receipt = await session.scalar(select(ProcessedEvent).where(ProcessedEvent.product_id == row.product_id, ProcessedEvent.outcome == "processed"))
        event = EventEnvelope(event_id=receipt.event_id, product_id=before.product_id, event_type=EventType.STOCK_RESERVED,
                              source="restart-check", payload={"version": before.version, "quantity": 2})
        producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)
        await producer.start()
        try:
            async with connect("ws://localhost:8000/ws") as socket:
                await producer.send_and_wait(settings.kafka_inventory_topic, key=str(before.product_id).encode(), value=event.model_dump_json().encode())
                async def wait_for_replay():
                    while True:
                        message = json.loads(await socket.recv())
                        if message.get("event_id") == str(event.event_id):
                            assert Product.model_validate(message["product"]) == before
                            return
                await asyncio.wait_for(wait_for_replay(), 20)
            print("PASS: duplicate after API/consumer restart leaves persisted product unchanged")
        finally:
            await producer.stop()
    asyncio.run(restart_check() if "--restart-check" in sys.argv else main())
