import asyncio
import json
import logging
import time
from collections import deque
from datetime import datetime, timezone

from pydantic import BaseModel, ConfigDict, Field
from redis.asyncio import Redis
from sqlalchemy import select

from .config import settings
from .database import Session
from .models import ProductRow
from .schemas import Product


class DependencyUnavailable(ConnectionError):
    pass


class Faults(BaseModel):
    model_config = ConfigDict(extra="forbid")
    inventory_unavailable: bool = False
    redis_unavailable: bool = False
    consumer_paused: bool = False
    latency_ms: int = Field(0, ge=0, le=5000, strict=True)


class CircuitBreaker:
    def __init__(self, threshold=3, recovery_seconds=5, trial_limit=1, clock=time.monotonic):
        self.threshold, self.recovery_seconds, self.trial_limit = threshold, recovery_seconds, trial_limit
        self.clock = clock
        self.state = "CLOSED"
        self.failures = self.trials = 0
        self.opened_at = 0.0
        self.transitions = deque(maxlen=20)
        self.lock = asyncio.Lock()

    def transition(self, state):
        previous, self.state = self.state, state
        if state == "OPEN":
            self.opened_at = self.clock()
        event = {"from": previous, "to": state, "at": datetime.now(timezone.utc).isoformat()}
        self.transitions.append(event)
        logging.getLogger("flashflow").info(json.dumps({"action": "circuit_transition", **event}))

    async def call(self, operation):
        # ponytail: serialize catalog probes; use bounded concurrent trials if read traffic demands it.
        async with self.lock:
            if self.state == "OPEN":
                if self.clock() - self.opened_at < self.recovery_seconds:
                    raise DependencyUnavailable("inventory circuit is open")
                self.trials = 0
                self.transition("HALF_OPEN")
            try:
                result = await operation()
            except asyncio.CancelledError:
                if self.state == "HALF_OPEN":
                    self.transition("OPEN")
                raise
            except Exception:
                self.failures += 1
                if self.state == "HALF_OPEN" or self.failures >= self.threshold:
                    self.transition("OPEN")
                raise
            self.failures = 0
            if self.state == "HALF_OPEN":
                self.trials += 1
                if self.trials >= self.trial_limit:
                    self.transition("CLOSED")
            return result

    def describe(self):
        return {"state": self.state, "failures": self.failures, "transitions": list(self.transitions),
                "retry_after_seconds": max(0, self.recovery_seconds - (self.clock() - self.opened_at)) if self.state == "OPEN" else 0}


async def load_products():
    async with Session() as session:
        rows = (await session.scalars(select(ProductRow).order_by(ProductRow.product_id).limit(500))).all()
        return [Product.model_validate(row).model_dump(mode="json") for row in rows]


class Catalog:
    cache_key = "catalog:snapshot"

    def __init__(self, faults, redis=None, loader=load_products, breaker=None):
        self.faults = faults
        self.redis = redis if redis is not None else Redis.from_url(settings.redis_url, socket_timeout=1, socket_connect_timeout=1)
        self.loader = loader
        self.breaker = breaker or CircuitBreaker(settings.breaker_failure_threshold, settings.breaker_recovery_seconds, settings.breaker_half_open_trials)
        self.cached_at = None
        self.cache_reads = self.cache_hits = 0

    async def live(self):
        if self.faults.inventory_unavailable:
            raise DependencyUnavailable("inventory read failure injected")
        async def read():
            await asyncio.sleep(self.faults.latency_ms / 1000)
            values = await self.loader()
            return [Product.model_validate(value).model_dump(mode="json") for value in values]
        return await asyncio.wait_for(read(), settings.inventory_timeout_seconds)

    def metadata(self, source, snapshot_at, reason=None):
        pipeline_paused = self.faults.consumer_paused
        return {"source": source, "stale": source != "live" or bool(reason) or pipeline_paused,
                "snapshot_at": snapshot_at, "age_seconds": max(0, (datetime.now(timezone.utc) - datetime.fromisoformat(snapshot_at)).total_seconds()) if snapshot_at else None,
                "reason": reason or ("Inventory consumer paused" if pipeline_paused else None),
                "breaker": self.breaker.describe()}

    async def snapshot(self, consumer_available=True):
        try:
            products = await self.breaker.call(self.live)
        except Exception:
            self.cache_reads += 1
            try:
                if self.faults.redis_unavailable:
                    raise DependencyUnavailable("Redis failure injected")
                raw = await self.redis.get(self.cache_key)
                if not raw:
                    raise DependencyUnavailable("No cached catalog")
                cached = json.loads(raw)
                timestamp = datetime.fromisoformat(cached["snapshot_at"])
                if timestamp.tzinfo is None or timestamp > datetime.now(timezone.utc):
                    raise ValueError("Invalid cache timestamp")
                products = [Product.model_validate(p).model_dump(mode="json") for p in cached["products"]]
                self.cache_hits += 1
                return {"products": products, "metadata": self.metadata("redis", cached["snapshot_at"], "Live inventory unavailable; showing a cached snapshot")}
            except Exception:
                return {"products": [], "metadata": self.metadata("unavailable", None, "Inventory and fallback unavailable; retaining last known browser data")}
        now = datetime.now(timezone.utc).isoformat()
        reason = None
        try:
            if self.faults.redis_unavailable:
                raise DependencyUnavailable("Redis failure injected")
            if not self.faults.consumer_paused and consumer_available:
                await self.redis.set(self.cache_key, json.dumps({"products": products, "snapshot_at": now}))
                self.cached_at = now
        except Exception:
            reason = "Redis unavailable; snapshot refresh and inventory delivery may lag"
        if not consumer_available:
            reason = "Inventory consumer stopped"
        metadata = self.metadata("live", now, reason)
        if self.faults.consumer_paused or reason:
            # Reading a paused durable catalog does not make inventory fresher.
            metadata["snapshot_at"] = self.cached_at
            metadata["age_seconds"] = max(0, (datetime.now(timezone.utc) - datetime.fromisoformat(self.cached_at)).total_seconds()) if self.cached_at else None
        return {"products": products, "metadata": metadata}
