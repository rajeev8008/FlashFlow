import asyncio
import random
import time
from dataclasses import dataclass
from enum import StrEnum

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient
from sqlalchemy import select

from .config import settings
from .database import Session
from .models import ProductRow
from .schemas import EventEnvelope, EventType, kafka_record


class TrafficMode(StrEnum):
    NORMAL = "NORMAL"
    BUSY = "BUSY"
    FLASH_SALE = "FLASH_SALE"
    EXTREME = "EXTREME"


DEFAULT_RATES = {TrafficMode.NORMAL: 10, TrafficMode.BUSY: 100, TrafficMode.FLASH_SALE: 500, TrafficMode.EXTREME: 2000}
DEFAULT_EVENT_WEIGHTS = {
    EventType.STOCK_RESERVED: 45,
    EventType.PURCHASE_COMPLETED: 25,
    EventType.STOCK_RELEASED: 20,
    EventType.INVENTORY_RESTOCKED: 10,
}


@dataclass
class InventoryState:
    product_id: str
    stock: int
    reserved: int
    version: int


def parse_event_mix(value: str) -> dict[EventType, int]:
    try:
        weights = {EventType(name.strip()): int(weight) for item in value.split(",") for name, weight in [item.split(":", 1)]}
    except (ValueError, TypeError) as error:
        raise ValueError("event mix must use EVENT_TYPE:positive_weight pairs") from error
    if any(weight <= 0 for weight in weights.values()) or not weights:
        raise ValueError("event weights must be positive")
    return weights


def next_event(state: InventoryState, rng: random.Random, event_weights: dict[EventType, int] = DEFAULT_EVENT_WEIGHTS) -> EventEnvelope:
    allowed: list[EventType] = [EventType.INVENTORY_RESTOCKED]
    weights = [event_weights.get(EventType.INVENTORY_RESTOCKED, 0)]
    if state.stock > state.reserved and EventType.STOCK_RESERVED in event_weights:
        allowed.append(EventType.STOCK_RESERVED)
        weights.append(event_weights[EventType.STOCK_RESERVED])
    if state.reserved:
        for event_type in (EventType.PURCHASE_COMPLETED, EventType.STOCK_RELEASED):
            if event_type in event_weights:
                allowed.append(event_type)
                weights.append(event_weights[event_type])

    if not any(weights):
        raise ValueError("event mix cannot produce a valid event for the current inventory state")

    event_type = rng.choices(allowed, weights=weights, k=1)[0]
    quantity = 1 if event_type != EventType.INVENTORY_RESTOCKED else rng.randint(5, 25)
    if event_type == EventType.STOCK_RESERVED:
        state.reserved += quantity
    elif event_type == EventType.STOCK_RELEASED:
        state.reserved -= quantity
    elif event_type == EventType.PURCHASE_COMPLETED:
        state.reserved -= quantity
        state.stock -= quantity
    else:
        state.stock += quantity
    state.version += 1
    return EventEnvelope(
        event_type=event_type,
        product_id=state.product_id,
        source="traffic-simulator",
        payload={"quantity": quantity, "stock": state.stock, "reserved_stock": state.reserved, "version": state.version},
    )


async def load_catalog(count: int) -> list[InventoryState]:
    async with Session() as session:
        rows = (await session.scalars(select(ProductRow).where(ProductRow.category != "Test").order_by(ProductRow.product_id).limit(count))).all()
    if not rows:
        raise RuntimeError("product catalog is empty; run the seed command first")
    return [InventoryState(row.product_id, row.stock, row.reserved_stock, row.version) for row in rows]


async def wait_for_backlog() -> None:
    """Load a fresh catalog only after previously published events finish processing."""
    admin = AIOKafkaAdminClient(bootstrap_servers=settings.kafka_bootstrap_servers)
    probe = AIOKafkaConsumer(bootstrap_servers=settings.kafka_bootstrap_servers)
    try:
        await admin.start()
        await probe.start()
        partitions = [TopicPartition(settings.kafka_inventory_topic, index) for index in range(settings.kafka_inventory_partitions)]
        ends = await probe.end_offsets(partitions)
        while True:
            offsets = await admin.list_consumer_group_offsets(settings.kafka_consumer_group)
            if all(offsets.get(partition) and offsets[partition].offset >= end or end == 0 for partition, end in ends.items()):
                return
            await asyncio.sleep(1)
    finally:
        await admin.close()
        await probe.stop()


async def run() -> None:
    mode = TrafficMode(settings.simulator_mode)
    rate = settings.simulator_events_per_second or DEFAULT_RATES[mode]
    rng = random.Random(settings.simulator_random_seed)
    event_weights = parse_event_mix(settings.simulator_event_mix)
    await wait_for_backlog()
    catalog = await load_catalog(settings.simulator_product_count)
    producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)
    await producer.start()
    started = time.monotonic()
    try:
        while not settings.simulator_burst_duration_seconds or time.monotonic() - started < settings.simulator_burst_duration_seconds:
            tick = time.monotonic()
            for _ in range(rate):
                event = next_event(rng.choice(catalog), rng, event_weights)
                record = kafka_record(event)
                await producer.send(settings.kafka_inventory_topic, key=record.key, value=record.value)
            await producer.flush()
            await asyncio.sleep(max(0, 1 - (time.monotonic() - tick)))
    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(run())
