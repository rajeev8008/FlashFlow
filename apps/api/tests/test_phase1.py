import random
from decimal import Decimal
from uuid import uuid4

import pytest
from pydantic import ValidationError

from flashflow.schemas import EventEnvelope, EventType, Product, ProductStatus, kafka_record
from flashflow.seed import build_products
from flashflow.simulator import InventoryState, next_event, parse_event_mix


def test_product_and_event_validation() -> None:
    with pytest.raises(ValidationError):
        Product(product_id=uuid4(), name="Bad", category="Test", base_price=Decimal("1"), current_price=Decimal("1"), stock=1, reserved_stock=2, sales_velocity=0, status=ProductStatus.ACTIVE, last_updated="2026-01-01T00:00:00Z", version=1)
    with pytest.raises(ValidationError):
        EventEnvelope(event_type="UNKNOWN", product_id=uuid4(), source="test", payload={})


def test_kafka_record_uses_product_id_key_and_valid_json() -> None:
    event = EventEnvelope(event_type=EventType.STOCK_RESERVED, product_id=uuid4(), source="test", payload={"quantity": 1})
    record = kafka_record(event)
    assert record.key.decode() == str(event.product_id)
    assert EventEnvelope.model_validate_json(record.value) == event


def test_seed_is_deterministic() -> None:
    first, second = build_products(5, 7), build_products(5, 7)
    assert [(row.product_id, row.base_price, row.stock) for row in first] == [(row.product_id, row.base_price, row.stock) for row in second]


def test_simulator_preserves_stock_invariants_and_is_deterministic() -> None:
    def generate() -> list[tuple[EventType, dict[str, int]]]:
        rng = random.Random(9)
        state = InventoryState("00000000-0000-0000-0000-000000000001", stock=10, reserved=0, version=1)
        return [(event.event_type, event.payload) for event in (next_event(state, rng) for _ in range(200))]

    rng = random.Random(9)
    state = InventoryState(str(uuid4()), stock=10, reserved=0, version=1)
    types = []
    for _ in range(200):
        event = next_event(state, rng)
        types.append(event.event_type)
        assert 0 <= state.reserved <= state.stock
    assert set(types) >= {EventType.STOCK_RESERVED, EventType.PURCHASE_COMPLETED, EventType.STOCK_RELEASED, EventType.INVENTORY_RESTOCKED}
    assert generate() == generate()


def test_event_mix_validation() -> None:
    assert parse_event_mix("STOCK_RESERVED:2,INVENTORY_RESTOCKED:1")[EventType.STOCK_RESERVED] == 2
    with pytest.raises(ValueError):
        parse_event_mix("STOCK_RESERVED:0")
