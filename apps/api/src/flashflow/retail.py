"""Bounded retail read models and protected, audited operator actions."""

import math
from datetime import datetime, timezone
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, Header, HTTPException
from fastapi.encoders import jsonable_encoder
from sqlalchemy import select, func, case

from .config import settings
from .database import Session
from .models import (
    ProductRow,
    RetailBucket,
    ForecastRow,
    RecommendationRow,
    RetailScenario,
    PricingDecision,
)
from .schemas import Product
from .retail_contracts import ScenarioRequest, DecisionRequest

router = APIRouter(prefix="/retail", tags=["Retail operations"])


def now():
    return datetime.now(timezone.utc)


def row_dict(row):
    return {
        c.name: jsonable_encoder(getattr(row, c.name)) for c in row.__table__.columns
    }


def controls(authorization: str | None = Header(None)):
    import secrets

    if (
        settings.app_env != "development"
        or not settings.enable_retail_controls
        or not settings.chaos_token
    ):
        raise HTTPException(404, "Retail demo controls disabled")
    if not secrets.compare_digest(
        authorization or "", f"Bearer {settings.chaos_token}"
    ):
        raise HTTPException(403, "Invalid control token")


async def overview():
    async with Session() as session:
        products = (
            await session.scalars(
                select(ProductRow)
                .where(ProductRow.category != "Test")
                .order_by(ProductRow.product_id)
                .limit(500)
            )
        ).all()
        latest = (
            select(
                ForecastRow.product_id, func.max(ForecastRow.generated_at).label("at")
            )
            .group_by(ForecastRow.product_id)
            .subquery()
        )
        rows = (
            await session.scalars(
                select(ForecastRow).join(
                    latest,
                    (ForecastRow.product_id == latest.c.product_id)
                    & (ForecastRow.generated_at == latest.c.at),
                )
            )
        ).all()
        forecasts = {r.product_id: r for r in rows}
        pending = (
            await session.scalars(
                select(RecommendationRow)
                .where(RecommendationRow.status.in_(["PENDING", "ACCEPTED"]))
                .order_by(RecommendationRow.created_at.desc())
                .limit(500)
            )
        ).all()
        recs = {r.product_id: row_dict(r) for r in reversed(pending)}
        items = []
        for p in products:
            f = forecasts.get(p.product_id)
            age = (now() - f.generated_at).total_seconds() if f else None
            data = f.data if f else None
            stale = (
                not f
                or age > settings.forecast_stale_seconds
                or data.get("feature_age_seconds", 0) > settings.forecast_stale_seconds
            )
            items.append(
                {
                    "product": Product.model_validate(p).model_dump(mode="json"),
                    "forecast": None
                    if not f
                    else {
                        **data,
                        "forecast_id": f.forecast_id,
                        "generated_at": f.generated_at.isoformat(),
                    },
                    "forecast_age_seconds": age,
                    "stale": stale,
                    "recommendation": recs.get(p.product_id),
                    "recent_price_change": bool(
                        p.last_price_change
                        and (now() - p.last_price_change).total_seconds() < 300
                    ),
                }
            )
        order = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "HEALTHY": 3}
        items.sort(
            key=lambda v: (
                order.get((v["forecast"] or {}).get("risk_level"), 4),
                v["product"]["name"],
            )
        )
        counts = {
            key: sum(
                not i["stale"] and (i["forecast"] or {}).get("risk_level") == key
                for i in items
            )
            for key in order
        }
        return {
            "generated_at": now().isoformat(),
            "items": items,
            "summary": {
                "products": len(items),
                **counts,
                "pending": sum(r.status == "PENDING" for r in pending),
                "stale": sum(i["stale"] for i in items),
                "recent_price_changes": sum(i["recent_price_change"] for i in items),
            },
            "clock": {
                "scale": 60,
                "bucket_simulated_minutes": 5,
                "horizon_simulated_minutes": 60,
                "horizon_real_seconds": settings.forecast_bucket_seconds * 12,
            },
            "controls_enabled": settings.app_env == "development"
            and settings.enable_retail_controls
            and bool(settings.chaos_token),
        }


router.add_api_route("/overview", overview, methods=["GET"])


@router.get("/products/{product_id}")
async def product_detail(product_id: UUID):
    pid = str(product_id)
    async with Session() as session:
        p = await session.get(ProductRow, pid)
        if p is None:
            raise HTTPException(404, "Unknown product")
        history = (
            await session.scalars(
                select(RetailBucket)
                .where(RetailBucket.product_id == pid)
                .order_by(RetailBucket.bucket.desc())
                .limit(120)
            )
        ).all()
        forecasts = (
            await session.scalars(
                select(ForecastRow)
                .where(ForecastRow.product_id == pid)
                .order_by(ForecastRow.generated_at.desc())
                .limit(120)
            )
        ).all()
        prices = (
            await session.scalars(
                select(PricingDecision)
                .where(
                    PricingDecision.product_id == pid,
                    PricingDecision.status == "APPLIED",
                )
                .order_by(PricingDecision.created_at.desc())
                .limit(20)
            )
        ).all()
        recs = (
            await session.scalars(
                select(RecommendationRow)
                .where(RecommendationRow.product_id == pid)
                .order_by(RecommendationRow.created_at.desc())
                .limit(20)
            )
        ).all()
        return {
            "retrieved_at": now().isoformat(),
            "product": Product.model_validate(p).model_dump(mode="json"),
            "history": [row_dict(r) for r in reversed(history)],
            "forecasts": [row_dict(r) for r in forecasts],
            "pricing_decisions": [row_dict(r) for r in prices],
            "recommendations": [row_dict(r) for r in recs],
            "forecast_stale": not forecasts
            or (now() - forecasts[0].generated_at).total_seconds()
            > settings.forecast_stale_seconds
            or forecasts[0].data.get("feature_age_seconds", 0)
            > settings.forecast_stale_seconds,
            "history_note": "Sales use event-time buckets; stock and price are durable snapshots observed by the feature worker, not exact per-sale states.",
        }


@router.get("/recommendations")
async def recommendations():
    async with Session() as session:
        rows = (
            await session.execute(
                select(RecommendationRow, ProductRow.name, ForecastRow.data)
                .join(ProductRow, ProductRow.product_id == RecommendationRow.product_id)
                .outerjoin(
                    ForecastRow,
                    ForecastRow.forecast_id == RecommendationRow.forecast_id,
                )
                .order_by(
                    case(
                        (RecommendationRow.status.in_(["PENDING", "ACCEPTED"]), 0),
                        else_=1,
                    ),
                    RecommendationRow.created_at.desc(),
                )
                .limit(100)
            )
        ).all()
        return [
            {**row_dict(r), "product_name": name, "forecast_data": data}
            for r, name, data in rows
        ]


@router.post(
    "/recommendations/{recommendation_id}/decision", dependencies=[Depends(controls)]
)
async def decide(recommendation_id: UUID, request: DecisionRequest):
    async with Session() as session, session.begin():
        r = await session.scalar(
            select(RecommendationRow)
            .where(RecommendationRow.recommendation_id == str(recommendation_id))
            .with_for_update()
        )
        if r is None:
            raise HTTPException(404, "Unknown recommendation")
        if r.status != "PENDING":
            raise HTTPException(409, "Recommendation already decided or expired")
        if (now() - r.created_at).total_seconds() > settings.forecast_stale_seconds:
            r.status, r.updated_at = "EXPIRED", now()
            return row_dict(r)
        f = await session.get(ForecastRow, r.forecast_id)
        if (
            f is None
            or f.data.get("feature_age_seconds", 0) > settings.forecast_stale_seconds
        ):
            raise HTTPException(409, "Forecast data is stale")
        r.status = "ACCEPTED" if request.decision == "APPROVE" else "REJECTED"
        r.decision, r.decided_at = request.decision, now()
        r.operator, r.decision_note, r.updated_at = (
            request.operator,
            request.note,
            now(),
        )
        if r.status == "ACCEPTED":
            r.event_id = str(uuid4())
        # Accepted rows are a durable outbox. Independent worker publishes same ID on retry.
        return row_dict(r)


@router.get("/scenarios")
async def scenarios():
    async with Session() as session:
        return [
            row_dict(r)
            for r in (
                await session.scalars(
                    select(RetailScenario)
                    .order_by(RetailScenario.started_at.desc())
                    .limit(20)
                )
            ).all()
        ]


@router.post("/scenarios", dependencies=[Depends(controls)])
async def start_scenario(request: ScenarioRequest):
    from .retail_simulation import scenario_plan

    async with Session() as session, session.begin():
        # Serialize scenario admission; concurrent scenarios cannot share a producer's product state.
        from sqlalchemy import text

        await session.execute(text("SELECT pg_advisory_xact_lock(817204)"))
        if await session.scalar(
            select(RetailScenario.scenario_id)
            .where(RetailScenario.status.in_(["QUEUED", "RUNNING"]))
            .limit(1)
        ):
            raise HTTPException(409, "Finish the active scenario first")
        ids = sorted({str(i) for i in request.product_ids})
        rows = (
            await session.scalars(
                select(ProductRow).where(ProductRow.product_id.in_(ids))
            )
        ).all()
        if len(rows) != len(ids) or any(r.category != "Retail Demo" for r in rows):
            raise HTTPException(
                400, "Scenarios target dedicated Retail Demo products only"
            )
        data = request.model_dump(mode="json")
        data.update(
            plans={
                pid: scenario_plan(
                    request.seed + i, request.kind, request.bins, request.strength
                )
                for i, pid in enumerate(ids)
            },
            tick=0,
            attempted=0,
            fulfilled=0,
            censored=0,
            simulated_minutes=0,
        )
        r = RetailScenario(
            scenario_id=str(uuid4()),
            kind=request.kind,
            seed=request.seed,
            started_at=now(),
            status="QUEUED",
            data=data,
        )
        session.add(r)
        return row_dict(r)


@router.get("/model-health")
async def model_health():
    from redis.asyncio import Redis
    import json

    client = Redis.from_url(
        settings.redis_url, socket_timeout=1, socket_connect_timeout=1
    )
    try:
        value = await client.get("retail:model-health")
        telemetry = (
            json.loads(value)
            if value
            else {"status": "unavailable", "reason": "Forecast worker heartbeat absent"}
        )
    except Exception:
        telemetry = {"status": "unavailable", "reason": "Model telemetry unavailable"}
    finally:
        await client.aclose()
    async with Session() as session:
        rows = (
            await session.scalars(
                select(ForecastRow)
                .where(ForecastRow.actual_sales.is_not(None))
                .order_by(ForecastRow.generated_at.desc())
                .limit(200)
            )
        ).all()
        errors = [r.data["expected_sales"] - r.actual_sales for r in rows]
        telemetry.update(
            recent_evaluated=len(rows),
            recent_mae=sum(abs(e) for e in errors) / len(errors) if errors else None,
            recent_rmse=math.sqrt(sum(e * e for e in errors) / len(errors))
            if errors
            else None,
        )
        statuses = dict(
            (
                await session.execute(
                    select(RecommendationRow.status, func.count()).group_by(
                        RecommendationRow.status
                    )
                )
            ).all()
        )
        telemetry["recommendations_by_status"] = statuses
    return telemetry
