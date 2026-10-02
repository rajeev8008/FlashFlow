from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import pytest

from flashflow.inventory import transition
from flashflow.schemas import EventEnvelope, EventType, Product
from flashflow.seed import build_products
from flashflow.stream import ConnectionManager, InventoryConsumer


def product():
    return Product.model_validate(build_products(1)[0])


def event(p, **payload):
    return EventEnvelope(event_type=EventType.STOCK_RESERVED, product_id=p.product_id,
                         source="test", payload={"version": p.version + 1, "quantity": 1, **payload})


def test_valid_stale_gap_and_invalid_transitions():
    p = product()
    update = event(p)
    next_product = transition(p, update)
    assert next_product.reserved_stock == 1
    assert transition(next_product, update) == next_product
    with pytest.raises(ValueError):
        transition(p, event(p, version=4))
    with pytest.raises(ValueError):
        transition(p, event(p, quantity=p.stock + 1))
    with pytest.raises(ValueError):
        transition(p, event(p, stock=0))


@pytest.mark.asyncio
async def test_cache_broadcast_retry_and_dlq():
    manager = ConnectionManager()
    first, second = manager.connect(), manager.connect()
    worker = InventoryConsumer.__new__(InventoryConsumer)
    worker.manager = manager
    from collections import Counter
    worker.counters = Counter()
    worker.redis = AsyncMock()
    worker.producer = AsyncMock()
    p = product()
    record = SimpleNamespace(key=str(p.product_id).encode(), value=event(p).model_dump_json().encode(), topic="test", partition=0, offset=3)
    with patch("flashflow.stream.process_event", AsyncMock(return_value=(p, "duplicate"))):
        await worker.handle(record)
    worker.redis.set.assert_awaited_once()
    assert (await first.get()) == (await second.get())
    assert worker.counters["duplicate"] == 1
    record.value = b"invalid"
    with patch("flashflow.stream.asyncio.sleep", AsyncMock()):
        await worker.handle(record)
    assert worker.counters["dlq"] == 1
    worker.producer.send_and_wait.assert_awaited_once()
    manager.disconnect(first)
    manager.disconnect(second)
    assert not manager.clients


def test_slow_client_disconnects_without_blocking_other_clients():
    manager = ConnectionManager()
    queue = manager.connect()
    for _ in range(101):
        manager.broadcast({"type": "test"})
    assert queue.get_nowait() is None
    assert not manager.clients


@pytest.mark.asyncio
async def test_retry_after_database_commit_repairs_cache():
    worker = InventoryConsumer.__new__(InventoryConsumer)
    worker.manager = ConnectionManager()
    from collections import Counter
    worker.counters = Counter()
    worker.redis = AsyncMock()
    worker.redis.set.side_effect = [ConnectionError("redis unavailable"), None]
    worker.producer = AsyncMock()
    p = product()
    record = SimpleNamespace(key=str(p.product_id).encode(), value=event(p).model_dump_json().encode(), topic="test", partition=0, offset=3)
    with patch("flashflow.stream.process_event", AsyncMock(side_effect=[(p, "processed"), (p, "duplicate")])), patch("flashflow.stream.asyncio.sleep", AsyncMock()):
        await worker.handle(record)
    assert worker.redis.set.await_count == 2
    assert worker.counters["duplicate"] == 1
    worker.producer.send_and_wait.assert_not_awaited()


@pytest.mark.asyncio
async def test_failed_dlq_does_not_commit_offset():
    from aiokafka import TopicPartition
    worker = InventoryConsumer.__new__(InventoryConsumer)
    record = SimpleNamespace(topic="inventory-events", partition=2, offset=7)
    class FakeConsumer:
        start = AsyncMock()
        stop = AsyncMock()
        commit = AsyncMock()
        def __aiter__(self):
            async def records():
                yield record
            return records()
    worker.consumer = FakeConsumer()
    worker.producer = AsyncMock()
    worker.redis = AsyncMock()
    async def failed():
        worker.consumer.commit.assert_not_awaited()
        raise ConnectionError("DLQ unavailable")
    calls = 0
    async def handle(_):
        nonlocal calls
        calls += 1
        if calls == 1:
            await failed()
    worker.handle = handle
    with patch("flashflow.stream.asyncio.sleep", AsyncMock()):
        await worker.run()
    assert calls == 2
    worker.consumer.commit.assert_awaited_once_with({TopicPartition("inventory-events", 2): 8})
    worker.consumer.stop.assert_awaited_once()
@pytest.mark.asyncio
async def test_dependency_outage_retains_valid_record_instead_of_dlq():
    from collections import Counter
    from flashflow.resilience import Faults
    worker = InventoryConsumer.__new__(InventoryConsumer)
    worker.manager, worker.counters = ConnectionManager(), Counter()
    worker.redis, worker.producer = AsyncMock(), AsyncMock()
    worker.faults = Faults(redis_unavailable=True)
    p = product()
    record = SimpleNamespace(key=str(p.product_id).encode(), value=event(p).model_dump_json().encode(), topic="test", partition=0, offset=3)
    with patch("flashflow.stream.process_event", AsyncMock(return_value=(p, "duplicate"))), patch("flashflow.stream.asyncio.sleep", AsyncMock()):
        with pytest.raises(ConnectionError):
            await worker.handle(record)
        worker.producer.send_and_wait.assert_not_awaited()
        worker.faults.redis_unavailable = False
        await worker.handle(record)
        worker.redis.set.assert_awaited_once()
