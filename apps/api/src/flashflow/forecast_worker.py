"""Independent advisory process: Kafka features, periodic forecasts and outboxes.

No model or feature work is called from the inventory consumer. A second group
observes only durably processed events; feature receipts make its own replay safe.
"""

import asyncio
import json
import logging
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from uuid import uuid4, uuid5, UUID

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from .config import settings
from .database import Session, engine
from .models import (
    ProductRow,
    ProcessedEvent,
    RetailBucket,
    FeatureReceipt,
    ForecastRow,
    RecommendationRow,
    RetailScenario,
)
from .schemas import EventEnvelope, EventType, kafka_record
from .forecasting import ForecastModel, features, predict, assess
from .retail import now
from .retail_contracts import ForecastOutput

log = logging.getLogger("flashflow.forecast")


class ForecastWorker:
    def __init__(self):
        self.counters = Counter()
        self.last_success = None
        self.started = now()
        self.feature_watermark = None
        self.consumer = AIOKafkaConsumer(
            settings.kafka_inventory_topic,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id="flashflow-retail-features-v1",
            enable_auto_commit=False,
            auto_offset_reset="latest",
            max_poll_records=500,
        )
        self.producer = AIOKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers
        )
        self.admin = AIOKafkaAdminClient(
            bootstrap_servers=settings.kafka_bootstrap_servers
        )
        self.redis = Redis.from_url(
            settings.redis_url, socket_timeout=1, socket_connect_timeout=1
        )
        try:
            self.model = ForecastModel(settings.forecast_model_path)
        except Exception as error:
            self.model = None
            log.warning(
                "Model unavailable; baseline fallback: %s", type(error).__name__
            )

    async def consume(self, partition, records):
        parsed = []
        for r in records:
            try:
                parsed.append((r, EventEnvelope.model_validate_json(r.value)))
            except ValueError:
                continue
        if not parsed:
            await self.consumer.commit({partition: records[-1].offset + 1})
            return
        ids = [str(e.event_id) for _, e in parsed]
        async with Session() as session:
            receipts = {
                r.event_id: r
                for r in (
                    await session.scalars(
                        select(ProcessedEvent).where(ProcessedEvent.event_id.in_(ids))
                    )
                ).all()
            }
        missing = [(r, e) for r, e in parsed if str(e.event_id) not in receipts]
        if missing:
            offsets = await self.admin.list_consumer_group_offsets(
                settings.kafka_consumer_group
            )
            core = offsets.get(partition)
            if core is None or any(r.offset >= core.offset for r, _ in missing):
                # Wait for core processing/DLQ rather than counting attempted events as sales.
                self.consumer.seek(partition, records[0].offset)
                await asyncio.sleep(0.2)
                return
        async with Session() as session, session.begin():
            seen = set(
                (
                    await session.scalars(
                        select(FeatureReceipt.event_id).where(
                            FeatureReceipt.event_id.in_(ids)
                        )
                    )
                ).all()
            )
            products = {
                r.product_id: r
                for r in (
                    await session.scalars(
                        select(ProductRow).where(
                            ProductRow.product_id.in_(
                                {str(e.product_id) for _, e in parsed}
                            )
                        )
                    )
                ).all()
            }
            bins = defaultdict(int)
            for record, event in parsed:
                eid, pid = str(event.event_id), str(event.product_id)
                if eid in seen:
                    continue
                seen.add(eid)
                session.add(FeatureReceipt(event_id=eid))
                receipt, product = receipts.get(eid), products.get(pid)
                if (
                    not receipt
                    or receipt.outcome != "processed"
                    or product is None
                    or product.category == "Test"
                ):
                    continue
                bucket = (
                    int(event.timestamp.timestamp())
                    // settings.forecast_bucket_seconds
                    * settings.forecast_bucket_seconds
                )
                if event.event_type == EventType.PURCHASE_COMPLETED:
                    bins[(pid, bucket)] += event.payload["quantity"]
                else:
                    bins[(pid, bucket)] += 0
            if bins:
                values = [
                    {
                        "product_id": pid,
                        "bucket": bucket,
                        "sales": sales,
                        "stock": products[pid].stock,
                        "reserved": products[pid].reserved_stock,
                        "price": float(products[pid].current_price),
                    }
                    for (pid, bucket), sales in bins.items()
                ]
                stmt = insert(RetailBucket).values(values)
                await session.execute(
                    stmt.on_conflict_do_update(
                        index_elements=["product_id", "bucket"],
                        set_={
                            "sales": RetailBucket.sales + stmt.excluded.sales,
                            "stock": stmt.excluded.stock,
                            "reserved": stmt.excluded.reserved,
                            "price": stmt.excluded.price,
                        },
                    )
                )
        await self.consumer.commit({partition: records[-1].offset + 1})
        self.counters["feature_events"] += len(parsed)
        self.feature_watermark = now()

    async def forecasting(self):
        started = time.perf_counter()
        end = (
            int(time.time())
            // settings.forecast_bucket_seconds
            * settings.forecast_bucket_seconds
        )
        # Only completed buckets enter predictors. Alignment keeps maturity evaluation exact.
        at = now()
        target_start = datetime.fromtimestamp(
            end + settings.forecast_bucket_seconds, timezone.utc
        )
        horizon = settings.forecast_bucket_seconds * 12
        async with Session() as session, session.begin():
            products = (
                await session.scalars(
                    select(ProductRow)
                    .where(ProductRow.category != "Test")
                    .order_by(ProductRow.product_id)
                    .limit(500)
                )
            ).all()
            bins = (
                await session.scalars(
                    select(RetailBucket).where(
                        RetailBucket.bucket >= end - horizon * 2,
                        RetailBucket.bucket < end,
                    )
                )
            ).all()
            snapshots = insert(RetailBucket).values(
                [
                    {
                        "product_id": p.product_id,
                        "bucket": end - settings.forecast_bucket_seconds,
                        "sales": 0,
                        "stock": p.stock,
                        "reserved": p.reserved_stock,
                        "price": float(p.current_price),
                    }
                    for p in products
                ]
            )
            await session.execute(
                snapshots.on_conflict_do_nothing(
                    index_elements=["product_id", "bucket"]
                )
            )
            by_id = defaultdict(dict)
            for b in bins:
                by_id[b.product_id][b.bucket] = b.sales
            active = (
                await session.scalars(
                    select(RetailScenario).where(
                        RetailScenario.status.in_(["RUNNING", "QUEUED"])
                    )
                )
            ).all()
            promotions = {
                pid
                for s in active
                if s.kind in ("FLASH_SALE", "DEMAND_SPIKE")
                for pid in s.data["product_ids"]
            }
            pending = (
                await session.scalars(
                    select(RecommendationRow)
                    .where(RecommendationRow.status == "PENDING")
                    .with_for_update()
                )
            ).all()
            pending_map = {r.product_id: r for r in pending}
            current_ids = [
                str(uuid5(UUID(p.product_id), f"forecast:{end}")) for p in products
            ]
            existing = set(
                (
                    await session.scalars(
                        select(ForecastRow.product_id).where(
                            ForecastRow.forecast_id.in_(current_ids)
                        )
                    )
                ).all()
            )
            earliest = {pid: min(history) for pid, history in by_id.items() if history}
            for p in products:
                if p.product_id in existing:
                    continue
                # Dense zero buckets represent monitored no-sale intervals, not missing pre-start history.
                if (
                    self.started.timestamp() > end - horizon
                    and earliest.get(p.product_id, end) > end - horizon
                ):
                    continue
                sales = [
                    by_id[p.product_id].get(i, 0)
                    for i in range(end - horizon, end, settings.forecast_bucket_seconds)
                ]
                minute = (end // settings.forecast_bucket_seconds * 5) % 1440
                x = features(
                    sales,
                    p.stock - p.reserved_stock,
                    float(p.current_price / p.base_price),
                    minute,
                    p.product_id in promotions,
                )
                self.counters["forecast_requests"] += 1
                output = predict(self.model, x)
                output.update(
                    assess(
                        p.stock - p.reserved_stock,
                        output,
                        settings.forecast_safety_stock,
                        settings.forecast_max_restock,
                    )
                )
                output.update(
                    available_stock=p.stock - p.reserved_stock,
                    feature_age_seconds=(now() - self.feature_watermark).total_seconds()
                    if self.feature_watermark
                    else (now() - self.started).total_seconds(),
                    features=x,
                    feature_schema="causal-5min-v1",
                    simulated_clock_scale=60,
                    horizon_real_seconds=horizon,
                    bucket_seconds=settings.forecast_bucket_seconds,
                    input_end=datetime.fromtimestamp(end, timezone.utc).isoformat(),
                    target_start=target_start.isoformat(),
                    issued_at=at.isoformat(),
                )
                output = ForecastOutput.model_validate(output).model_dump()
                fid = str(uuid5(UUID(p.product_id), f"forecast:{end}"))
                session.add(
                    ForecastRow(
                        forecast_id=fid,
                        product_id=p.product_id,
                        generated_at=at,
                        target_end=target_start + timedelta(seconds=horizon),
                        data=output,
                    )
                )
                self.counters["forecast_successes"] += 1
                self.counters["baseline_fallbacks"] += int(output["fallback"])
                old = pending_map.get(p.product_id)
                if (
                    old
                    and output["recommended_quantity"]
                    and (now() - old.created_at).total_seconds()
                    <= settings.forecast_stale_seconds
                ):
                    continue
                if old:
                    old.status, old.updated_at = "EXPIRED", now()
                if (
                    output["recommended_quantity"]
                    and output["feature_age_seconds"] <= settings.forecast_stale_seconds
                ):
                    session.add(
                        RecommendationRow(
                            recommendation_id=str(uuid4()),
                            product_id=p.product_id,
                            forecast_id=fid,
                            quantity=output["recommended_quantity"],
                            reason=output["recommendation_reason"],
                            risk=output["risk_level"],
                            status="PENDING",
                            created_at=at,
                            updated_at=at,
                            published=False,
                        )
                    )
                    self.counters["recommendations_created"] += 1
            # Mature only after this observer has passed the horizon, preventing partial actuals.
            lagged = await self.consumer.end_offsets(list(self.consumer.assignment()))
            positions = {tp: await self.consumer.position(tp) for tp in lagged}
            caught_up = bool(lagged) and all(
                positions[tp] >= offset for tp, offset in lagged.items()
            )
            if caught_up:
                mature = (
                    await session.scalars(
                        select(ForecastRow)
                        .where(
                            ForecastRow.actual_sales.is_(None),
                            ForecastRow.target_end <= at,
                        )
                        .order_by(ForecastRow.target_end)
                        .limit(500)
                    )
                ).all()
                evaluation = defaultdict(dict)
                if mature:
                    observed = (
                        await session.scalars(
                            select(RetailBucket).where(
                                RetailBucket.bucket
                                >= int(min(f.generated_at for f in mature).timestamp()),
                                RetailBucket.bucket < end,
                            )
                        )
                    ).all()
                    for b in observed:
                        evaluation[b.product_id][b.bucket] = b.sales
                for f in mature:
                    start = datetime.fromisoformat(
                        f.data.get("target_start", f.generated_at.isoformat())
                    ).timestamp()
                    f.actual_sales = sum(
                        v
                        for b, v in evaluation[f.product_id].items()
                        if start <= b < f.target_end.timestamp()
                    )
        self.last_success = now()
        self.counters["last_forecast_ms"] = (time.perf_counter() - started) * 1000

    async def actions(self):
        async with Session() as session:
            rows = (
                await session.scalars(
                    select(RecommendationRow)
                    .where(RecommendationRow.status == "ACCEPTED")
                    .limit(100)
                )
            ).all()
            for r in rows:
                if not r.published:
                    event = EventEnvelope(
                        event_id=r.event_id,
                        event_type=EventType.INVENTORY_RESTOCKED,
                        product_id=r.product_id,
                        source="retail-operator",
                        payload={"quantity": r.quantity},
                    )
                    record = kafka_record(event)
                    await self.producer.send_and_wait(
                        settings.kafka_inventory_topic,
                        key=record.key,
                        value=record.value,
                    )
                    r.published = True
                    r.published_at = now()
                    await session.commit()
                receipt = await session.get(ProcessedEvent, r.event_id)
                if receipt:
                    r.status = (
                        "EXECUTED" if receipt.outcome == "processed" else "FAILED"
                    )
                    r.updated_at = now()
                    r.executed_at = r.updated_at
                    await session.commit()

    async def telemetry(self):
        assigned = list(self.consumer.assignment())
        ends = await self.consumer.end_offsets(assigned) if assigned else {}
        committed = {tp: await self.consumer.committed(tp) for tp in ends}
        positions = {tp: await self.consumer.position(tp) for tp in ends}
        lag = sum(
            max(
                0,
                offset
                - (committed[tp] if committed[tp] is not None else positions[tp]),
            )
            for tp, offset in ends.items()
        )
        if assigned and lag == 0:
            self.feature_watermark = now()
        data = {
            "status": "running",
            "sampled_at": now().isoformat(),
            "model_version": self.model.artifact["version"]
            if self.model
            else "moving-average-v1",
            "last_successful_forecast": self.last_success.isoformat()
            if self.last_success
            else None,
            "feature_lag": lag,
            "feature_age_seconds": (now() - self.feature_watermark).total_seconds()
            if self.feature_watermark
            else None,
            **dict(self.counters),
        }
        await self.redis.set("retail:model-health", json.dumps(data), ex=15)

    async def run(self):
        await self.consumer.start()
        await self.producer.start()
        await self.admin.start()
        from .scenario_worker import scenario_tick, seed_demo

        await seed_demo()
        last_forecast = 0
        try:
            while True:
                records = {}
                try:
                    records = await self.consumer.getmany(
                        timeout_ms=250, max_records=500
                    )
                    for tp, batch in records.items():
                        await self.consume(tp, batch)
                    await scenario_tick(self.producer)
                    await self.actions()
                    if (
                        time.monotonic() - last_forecast
                        >= settings.forecast_interval_seconds
                    ):
                        try:
                            await self.forecasting()
                        except Exception:
                            self.counters["forecast_failures"] += 1
                            raise
                        try:
                            await self.telemetry()
                        except Exception:
                            self.counters["telemetry_failures"] += 1
                        last_forecast = time.monotonic()
                except Exception:
                    self.counters["worker_failures"] += 1
                    log.exception(
                        "Advisory worker retry; core inventory is independent"
                    )
                    # Failed feature DB work must replay fetched records, not skip on next poll.
                    for tp in self.consumer.assignment():
                        batch = records.get(tp)
                        offset = (
                            batch[0].offset
                            if batch
                            else await self.consumer.committed(tp)
                        )
                        if offset is not None:
                            self.consumer.seek(tp, offset)
                    await asyncio.sleep(1)
        finally:
            await self.consumer.stop()
            await self.producer.stop()
            await self.admin.close()
            await self.redis.aclose()
            await engine.dispose()


async def main():
    await ForecastWorker().run()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
