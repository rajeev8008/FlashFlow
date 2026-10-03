import asyncio
from collections import Counter
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from aiokafka import TopicPartition
from flashflow.config import settings
from flashflow.stream import InventoryConsumer, ConnectionManager
from flashflow.timing import Timings


def test_timing_window_is_bounded_and_rejects_invalid_samples():
    metrics = Timings(3)
    for value in [-1, float('nan'), float('inf'), 1, 2, 3, 100]:
        metrics.observe('db', value)
    assert metrics.snapshot()['db'] == {
        'count': 4, 'window_count': 3, 'total_ms': 106,
        'p50_ms': 3, 'p95_ms': 100, 'p99_ms': 100}
    with pytest.raises(ValueError), metrics.measure('failed'):
        raise ValueError('rollback')
    assert metrics.snapshot()['failed']['count'] == 1


@pytest.mark.asyncio
async def test_batch_offsets_follow_success_and_retry_without_skipping():
    partition = TopicPartition(settings.kafka_inventory_topic, 0)
    worker = InventoryConsumer.__new__(InventoryConsumer)
    worker.stopping, worker.faults, worker.counters = asyncio.Event(), None, Counter()
    records = [SimpleNamespace(topic=partition.topic, offset=n) for n in (4, 5)]
    calls = []
    async def getmany(**kwargs):
        if calls:
            worker.stopping.set()
            return {}
        return {partition: records}
    async def handle_batch(batch, received):
        worker.consumer.commit.assert_not_awaited()
        calls.append([r.offset for r in batch])
        if len(calls) == 1:
            raise ConnectionError('database unavailable')
    worker.consumer = SimpleNamespace(start=AsyncMock(), stop=AsyncMock(),
        getmany=getmany, assignment=lambda: {partition}, commit=AsyncMock())
    worker.producer, worker.redis = AsyncMock(), AsyncMock()
    worker.handle_batch = handle_batch
    with patch('flashflow.stream.asyncio.sleep', AsyncMock()):
        await worker.run()
    assert calls == [[4, 5], [4, 5]]
    worker.consumer.commit.assert_awaited_once_with({partition: 6})


@pytest.mark.asyncio
async def test_revoked_partition_is_not_acknowledged():
    partition = TopicPartition(settings.kafka_inventory_topic, 0)
    worker = InventoryConsumer.__new__(InventoryConsumer)
    worker.stopping, worker.faults, worker.counters = asyncio.Event(), None, Counter()
    assigned = {partition}
    async def getmany(**kwargs):
        return {partition: [SimpleNamespace(topic=partition.topic, offset=9)]}
    async def handle_batch(*args):
        assigned.clear()
        worker.stopping.set()
    worker.consumer = SimpleNamespace(start=AsyncMock(), stop=AsyncMock(), getmany=getmany,
        assignment=lambda: assigned, commit=AsyncMock())
    worker.producer, worker.redis = AsyncMock(), AsyncMock()
    worker.handle_batch = handle_batch
    await worker.run()
    worker.consumer.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_uncommitted_batch_never_reaches_cache_or_socket():
    from flashflow.schemas import EventEnvelope, EventType
    from uuid import uuid4
    event = EventEnvelope(product_id=uuid4(), event_type=EventType.STOCK_RESERVED,
                          source='test', payload={'quantity': 1, 'version': 2})
    record = SimpleNamespace(topic=settings.kafka_inventory_topic,
        key=str(event.product_id).encode(), value=event.model_dump_json().encode())
    worker = InventoryConsumer.__new__(InventoryConsumer)
    worker.manager, worker.redis, worker.producer = ConnectionManager(), AsyncMock(), AsyncMock()
    worker.handle = AsyncMock()
    with patch('flashflow.stream.process_batch', AsyncMock(side_effect=ConnectionError('commit failed'))):
        with pytest.raises(ConnectionError):
            await worker.handle_batch([record], datetime.now(timezone.utc))
    worker.handle.assert_not_awaited()
    worker.redis.eval.assert_not_awaited()
    worker.producer.send_and_wait.assert_not_awaited()


@pytest.mark.asyncio
async def test_socket_send_timeout_removes_client_and_counts_failure(monkeypatch):
    manager = ConnectionManager()
    monkeypatch.setattr(settings, 'websocket_send_timeout_seconds', .01)
    waiting = asyncio.Event()
    socket = SimpleNamespace(accept=AsyncMock(), close=AsyncMock(),
        send_json=lambda message: waiting.wait(), receive_text=waiting.wait)
    task = asyncio.create_task(manager.serve(socket))
    await asyncio.sleep(.001)
    manager.broadcast({'type': 'product_update', 'emitted_at': datetime.now(timezone.utc).isoformat()})
    await asyncio.wait_for(task, 1)
    assert not manager.clients
    assert manager.counters['failed_sends'] == 1
    assert manager.counters['disconnected'] == 1
    socket.close.assert_awaited()


@pytest.mark.asyncio
async def test_lag_aggregates_all_colocated_group_members():
    from flashflow.observability import Metrics
    first = TopicPartition(settings.kafka_inventory_topic, 0)
    second = TopicPartition(settings.kafka_inventory_topic, 1)
    consumer = SimpleNamespace(assignment=lambda: {first},
        end_offsets=AsyncMock(return_value={first: 20, second: 30}),
        committed=AsyncMock(side_effect=lambda partition: 10 if partition == first else 25))
    group = SimpleNamespace(counters=Counter(), consumer=consumer, workers=[
        SimpleNamespace(consumer=consumer),
        SimpleNamespace(consumer=SimpleNamespace(assignment=lambda: {second}))])
    metrics = Metrics(group, ConnectionManager())
    with patch('flashflow.observability.asyncio.sleep', AsyncMock(side_effect=[None, asyncio.CancelledError])):
        with pytest.raises(asyncio.CancelledError):
            await metrics.run()
    assert metrics.sample['consumer_lag'] == 15
    assert metrics.sample['lag_by_partition'][f'{second.topic}:1'] == 5


@pytest.mark.asyncio
async def test_producer_telemetry_expires_and_cache_failure_is_noncritical():
    import json
    from redis.exceptions import RedisError
    from flashflow.simulator import report_producer
    redis = AsyncMock()
    await report_producer(redis, 500, 1000)
    assert redis.set.call_args.kwargs['ex'] == 5
    assert json.loads(redis.set.call_args.args[1])['configured_rate'] == 500
    redis.set.side_effect = RedisError('offline')
    await report_producer(redis, 500, 1001)
