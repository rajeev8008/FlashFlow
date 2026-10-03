from flashflow.observability import rates
from flashflow.observability import Metrics
from flashflow.config import settings
from aiokafka import TopicPartition
from types import SimpleNamespace
from collections import Counter
from unittest.mock import AsyncMock, patch
import asyncio
import pytest


def test_rates_are_deltas_not_configured_producer_throughput():
    assert rates({'events': 20}, {'events': 10}, 2) == {'events': 5}
    assert rates({'events': 0}, {'events': 10}, 2) == {'events': 0}


@pytest.mark.asyncio
async def test_lag_uses_committed_offsets_and_unavailable_is_not_zero():
    partition = TopicPartition(settings.kafka_inventory_topic, 0)
    consumer = SimpleNamespace(assignment=lambda: {partition}, end_offsets=AsyncMock(return_value={partition: 12}), committed=AsyncMock(return_value=4))
    metrics = Metrics(SimpleNamespace(counters=Counter(), consumer=consumer), SimpleNamespace(counters=Counter()))
    with patch('flashflow.observability.asyncio.sleep', AsyncMock(side_effect=[None, asyncio.CancelledError])):
        with pytest.raises(asyncio.CancelledError):
            await metrics.run()
    assert metrics.sample['consumer_lag'] == 8
    consumer.end_offsets.side_effect = ConnectionError('offline')
    with patch('flashflow.observability.asyncio.sleep', AsyncMock(side_effect=[None, asyncio.CancelledError])):
        with pytest.raises(asyncio.CancelledError):
            await metrics.run()
    assert metrics.sample['consumer_lag'] is None
