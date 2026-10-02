from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import Mock, patch

from flashflow.config import settings
from flashflow.pricing import recommend, record_decision
from flashflow.schemas import EventEnvelope, EventType
from flashflow.seed import build_products
import pytest
from unittest.mock import AsyncMock
from types import SimpleNamespace
from collections import Counter
from aiokafka.errors import KafkaError
from flashflow.stream import InventoryConsumer, ConnectionManager
from flashflow.schemas import Product


def test_deterministic_bounded_rules_cooldown_reversal_and_disabled():
    row = build_products(1)[0]
    row.base_price = row.current_price = Decimal('100.00')
    row.demand_state = 'HIGH'
    now = datetime.now(timezone.utc)
    assert recommend(row, now)[0] == Decimal('105.00')
    assert recommend(row, now) == recommend(row, now)
    row.current_price = Decimal('120.00')
    assert recommend(row, now)[0] <= Decimal('120.00')
    row.current_price = Decimal('100.00')
    row.last_price_change = now
    assert recommend(row, now)[0] == row.current_price
    row.last_price_change = now - timedelta(seconds=31)
    row.price_direction = 'DOWN'
    assert recommend(row, now)[1] == 'Reversal protection active'
    row.last_price_change = now - timedelta(seconds=121)
    assert recommend(row, now)[0] == Decimal('105.00')
    row.demand_state = 'LOW'
    assert recommend(row, now)[0] == Decimal('95.00')
    with patch.object(settings, 'pricing_enabled', False):
        assert recommend(row, now)[0] == row.current_price
    row.pending_price_decision = 'pending'
    assert recommend(row, now)[0] == row.current_price
    row.pending_price_decision = None
    row.stock = 0
    assert recommend(row, now)[0] == row.current_price


def test_decisions_capture_inputs_and_preserve_inventory_revision():
    row = build_products(1)[0]
    row.reserved_stock = row.stock // 2 + 1
    session = Mock()
    event = EventEnvelope(product_id=row.product_id, event_type=EventType.STOCK_RESERVED, source='test', payload={'quantity': 1})
    record_decision(session, row, event)
    decision = session.add.call_args.args[0]
    assert decision.status == 'PENDING' and decision.decision_source == 'RULES'
    assert decision.signals['reservation_ratio'] >= .5
    assert decision.recommended_price <= row.current_price * Decimal('1.05')
    assert row.version == 1 and row.price_version == 0
    assert decision.source_event_id == str(event.event_id)
    record_decision(session, row, event.model_copy(update={'timestamp': event.timestamp - timedelta(minutes=5)}))
    assert session.add.call_args.args[0].status == 'HOLD'


def test_cent_bounds_never_round_outside_limits():
    row = build_products(1)[0]
    row.base_price = row.current_price = Decimal('.01')
    row.demand_state = 'HIGH'
    assert recommend(row, datetime.now(timezone.utc))[0] == Decimal('.01')
    row.base_price = row.current_price = Decimal('99999999.99')
    assert recommend(row, datetime.now(timezone.utc))[0] == row.current_price


@pytest.mark.asyncio
async def test_pricing_publication_failure_retains_source_for_replay():
    worker = InventoryConsumer.__new__(InventoryConsumer)
    worker.manager, worker.counters = ConnectionManager(), Counter()
    worker.redis, worker.producer = AsyncMock(), AsyncMock()
    product = Product.model_validate(build_products(1)[0])
    event = EventEnvelope(product_id=product.product_id, event_type=EventType.STOCK_RESERVED, source='test', payload={'quantity': 1, 'version': 2})
    record = SimpleNamespace(topic=settings.kafka_inventory_topic, key=str(product.product_id).encode(), value=event.model_dump_json().encode(), partition=0, offset=0)
    with patch('flashflow.stream.process_event', AsyncMock(return_value=(product, 'duplicate'))), patch('flashflow.stream.publish_decision', AsyncMock(side_effect=KafkaError('broker unavailable'))), patch('flashflow.stream.asyncio.sleep', AsyncMock()):
        with pytest.raises(KafkaError):
            await worker.handle(record)
        worker.producer.send_and_wait.assert_not_awaited()
        worker.redis.set.assert_not_awaited()
