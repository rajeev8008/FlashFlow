import json
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

from flashflow.config import settings
from flashflow.main import require_chaos
from flashflow.resilience import Catalog, CircuitBreaker, DependencyUnavailable, Faults
from flashflow.schemas import Product
from flashflow.seed import build_products


@pytest.mark.asyncio
async def test_breaker_opens_rejects_calls_and_recovers_with_bounded_trials():
    now = [0]
    breaker = CircuitBreaker(threshold=2, recovery_seconds=5, trial_limit=2, clock=lambda: now[0])
    failure = AsyncMock(side_effect=ConnectionError("down"))
    for _ in range(2):
        with pytest.raises(ConnectionError):
            await breaker.call(failure)
    assert breaker.state == "OPEN"
    with pytest.raises(DependencyUnavailable):
        await breaker.call(failure)
    assert failure.await_count == 2
    now[0] = 5
    success = AsyncMock(return_value=42)
    assert await breaker.call(success) == 42
    assert breaker.state == "HALF_OPEN"
    assert await breaker.call(success) == 42
    assert breaker.state == "CLOSED"
    assert [event["to"] for event in breaker.transitions] == ["OPEN", "HALF_OPEN", "CLOSED"]


@pytest.mark.asyncio
async def test_failed_half_open_probe_reopens():
    now = [0]
    breaker = CircuitBreaker(threshold=1, recovery_seconds=1, clock=lambda: now[0])
    failure = AsyncMock(side_effect=ConnectionError("down"))
    with pytest.raises(ConnectionError):
        await breaker.call(failure)
    now[0] = 1
    with pytest.raises(ConnectionError):
        await breaker.call(failure)
    assert breaker.state == "OPEN" and breaker.opened_at == 1


@pytest.mark.asyncio
async def test_validated_redis_fallback_and_automatic_recovery():
    p = Product.model_validate(build_products(1)[0]).model_dump(mode="json")
    now = [0]
    faults, redis = Faults(), AsyncMock()
    catalog = Catalog(faults, redis, AsyncMock(return_value=[p]), CircuitBreaker(1, 1, clock=lambda: now[0]))
    live = await catalog.snapshot()
    assert live["metadata"]["source"] == "live" and not live["metadata"]["stale"]
    redis.get.return_value = redis.set.call_args.args[1]
    faults.inventory_unavailable = True
    fallback = await catalog.snapshot()
    assert fallback["products"] == live["products"]
    assert fallback["metadata"]["source"] == "redis" and fallback["metadata"]["stale"]
    assert fallback["metadata"]["snapshot_at"] == live["metadata"]["snapshot_at"]
    assert fallback["metadata"]["age_seconds"] >= 0 and fallback["metadata"]["breaker"]["state"] == "OPEN"
    faults.inventory_unavailable = False
    now[0] = 1
    recovered = await catalog.snapshot()
    assert not recovered["metadata"]["stale"] and catalog.breaker.state == "CLOSED"


@pytest.mark.asyncio
async def test_invalid_cache_and_double_failure_are_explicitly_unavailable():
    faults, redis = Faults(inventory_unavailable=True), AsyncMock()
    catalog = Catalog(faults, redis)
    for raw in [None, "bad json", json.dumps({"products": [{}], "snapshot_at": datetime.now(timezone.utc).isoformat()}), json.dumps({"products": [], "snapshot_at": "2026-01-01T00:00:00"})]:
        redis.get.return_value = raw
        snapshot = await catalog.snapshot()
        assert snapshot["products"] == []
        assert snapshot["metadata"]["source"] == "unavailable" and snapshot["metadata"]["stale"]
    faults.redis_unavailable = True
    assert (await catalog.snapshot())["metadata"]["source"] == "unavailable"


@pytest.mark.asyncio
async def test_pause_keeps_freshness_timestamp_and_redis_failure_marks_degraded():
    p = Product.model_validate(build_products(1)[0]).model_dump(mode="json")
    faults, redis = Faults(consumer_paused=True), AsyncMock()
    catalog = Catalog(faults, redis, AsyncMock(return_value=[p]))
    catalog.cached_at = (datetime.now(timezone.utc) - timedelta(seconds=10)).isoformat()
    snapshot = await catalog.snapshot()
    assert snapshot["metadata"]["stale"] and snapshot["metadata"]["age_seconds"] >= 10
    redis.set.assert_not_awaited()
    faults.consumer_paused, faults.redis_unavailable = False, True
    snapshot = await catalog.snapshot()
    assert snapshot["products"] == [p] and snapshot["metadata"]["stale"]
    assert "Redis" in snapshot["metadata"]["reason"]


@pytest.mark.asyncio
async def test_latency_timeout_opens_circuit(monkeypatch):
    monkeypatch.setattr(settings, "inventory_timeout_seconds", 0.01)
    catalog = Catalog(Faults(latency_ms=50), AsyncMock(), AsyncMock(), CircuitBreaker(1))
    catalog.redis.get.return_value = None
    assert (await catalog.snapshot())["metadata"]["stale"]
    assert catalog.breaker.state == "OPEN"
    catalog.loader.assert_not_awaited()


def test_chaos_is_disabled_in_production_and_requires_explicit_enable_and_token(monkeypatch):
    monkeypatch.setattr(settings, "app_env", "production")
    monkeypatch.setattr(settings, "enable_chaos", True)
    monkeypatch.setattr(settings, "chaos_token", "test-only")
    with pytest.raises(HTTPException) as error:
        require_chaos("Bearer test-only")
    assert error.value.status_code == 404
    monkeypatch.setattr(settings, "app_env", "development")
    with pytest.raises(HTTPException) as error:
        require_chaos("Bearer wrong")
    assert error.value.status_code == 403
    require_chaos("Bearer test-only")
    monkeypatch.setattr(settings, "enable_chaos", False)
    with pytest.raises(HTTPException):
        require_chaos("Bearer test-only")
    monkeypatch.setattr(settings, "enable_chaos", True)
    monkeypatch.setattr(settings, "chaos_token", "")
    with pytest.raises(HTTPException):
        require_chaos("Bearer ")
