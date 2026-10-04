"""Durable seeded plans; deterministic IDs make scenario retries harmless."""

from datetime import datetime
from decimal import Decimal
from uuid import UUID, uuid5
from sqlalchemy import select
from .database import Session
from .models import ProductRow, RetailScenario, ProcessedEvent
from .schemas import EventEnvelope, EventType, kafka_record
from .retail import now
from .config import settings

DEMO_NAMESPACE = UUID("1e12a220-0804-4ad6-a86d-c2f6e90cb984")


async def seed_demo():
    async with Session() as session, session.begin():
        for name, price in [
            ("Studio headphones", 129),
            ("Trail running shoes", 89),
            ("Everyday coffee kit", 49),
        ]:
            pid = str(uuid5(DEMO_NAMESPACE, name))
            if not await session.get(ProductRow, pid):
                session.add(
                    ProductRow(
                        product_id=pid,
                        name=name,
                        category="Retail Demo",
                        base_price=Decimal(price),
                        current_price=Decimal(price),
                        stock=40,
                        reserved_stock=0,
                        sales_velocity=0,
                        status="ACTIVE",
                        last_updated=now(),
                        version=1,
                        price_version=0,
                        reference_stock=40,
                        demand_units=0,
                        demand_events=0,
                        demand_state="NORMAL",
                        pricing_reason="No price changes yet",
                        price_direction="UNCHANGED",
                    )
                )


async def send(producer, scenario, pid, suffix, kind, quantity):
    eid = uuid5(UUID(scenario.scenario_id), f"{pid}:{suffix}")
    async with Session() as session:
        receipt = await session.get(ProcessedEvent, str(eid))
        if receipt:
            if receipt.outcome != "processed":
                raise ValueError("Scenario event was not applied")
            return True
    event = EventEnvelope(
        event_id=eid,
        event_type=kind,
        product_id=pid,
        source="retail-scenario",
        payload={"quantity": quantity},
    )
    record = kafka_record(event)
    await producer.send_and_wait(
        settings.kafka_inventory_topic, key=record.key, value=record.value
    )
    return False


async def scenario_tick(producer):
    async with Session() as session:
        scenario = await session.scalar(
            select(RetailScenario)
            .where(RetailScenario.status.in_(["QUEUED", "RUNNING"]))
            .order_by(RetailScenario.started_at)
            .limit(1)
        )
        if not scenario:
            return
        data = dict(scenario.data)
        scenario.status = "RUNNING"
        tick = data["tick"]
        if tick >= data["bins"]:
            scenario.status, scenario.ended_at = "COMPLETED", now()
            await session.commit()
            return
        # A phase persists planned accepted quantities before publication. Replays use exact IDs/payloads.
        phase = data.get("phase")
        if not phase:
            if (
                data.get("last_tick_at")
                and (
                    now() - datetime.fromisoformat(data["last_tick_at"])
                ).total_seconds()
                < settings.forecast_bucket_seconds
            ):
                return
            phase = {}
            for pid in data["product_ids"]:
                p = await session.get(ProductRow, pid)
                attempted = (
                    data["plans"][pid][tick] if scenario.kind != "RESTOCK" else 0
                )
                phase[pid] = {
                    "attempted": attempted,
                    "quantity": min(attempted, p.stock - p.reserved_stock)
                    if scenario.kind != "RESTOCK"
                    else data["restock_quantity"],
                }
            data["phase"] = phase
            scenario.data = data
            await session.commit()
        complete = True
        for pid, plan in phase.items():
            quantity = plan["quantity"]
            if quantity <= 0:
                continue
            if scenario.kind == "RESTOCK":
                complete = (
                    await send(
                        producer,
                        scenario,
                        pid,
                        f"{tick}:restock",
                        EventType.INVENTORY_RESTOCKED,
                        quantity,
                    )
                    and complete
                )
            else:
                reserved = await send(
                    producer,
                    scenario,
                    pid,
                    f"{tick}:reserve",
                    EventType.STOCK_RESERVED,
                    quantity,
                )
                purchased = await send(
                    producer,
                    scenario,
                    pid,
                    f"{tick}:purchase",
                    EventType.PURCHASE_COMPLETED,
                    quantity,
                )
                complete = reserved and purchased and complete
        if complete:
            data["tick"] += 1
            data["last_tick_at"] = now().isoformat()
            data["simulated_minutes"] = data["tick"] * 5
            data["attempted"] += sum(p["attempted"] for p in phase.values())
            if scenario.kind != "RESTOCK":
                data["fulfilled"] += sum(p["quantity"] for p in phase.values())
                data["censored"] += sum(
                    p["attempted"] - p["quantity"] for p in phase.values()
                )
            else:
                data["tick"] = data["bins"]
                data["restocked"] = sum(p["quantity"] for p in phase.values())
            del data["phase"]
            scenario.data = data
            await session.commit()
