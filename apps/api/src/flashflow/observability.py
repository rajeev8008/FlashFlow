"""Two-second process-local samples; unavailable broker gauges stay explicitly null."""
import asyncio
import time
import json
from pathlib import Path
try:
    import resource
except ImportError:
    resource = None  # Windows host checks; the documented runtime is a Linux container.
from datetime import datetime, timezone

from .config import settings


def rates(current, previous, seconds):
    return {key: max(0, value - previous.get(key, 0)) / seconds for key, value in current.items()}


class Metrics:
    def __init__(self, worker, manager):
        self.worker, self.manager = worker, manager
        self.sample = {"rates": {}, "consumer_lag": None, "lag_by_topic": None, "sampled_at": None}

    async def run(self):
        previous, started, previous_cpu = {}, time.monotonic(), time.process_time()
        previous_end = None
        while True:
            await asyncio.sleep(2)
            now = time.monotonic()
            cpu = time.process_time()
            elapsed = max(now - started, .000001)
            current = {"kafka_events_per_second": self.worker.counters["kafka_received"],
                       "inventory_processed_per_second": self.worker.counters["inventory_completed"],
                       "processed_events_per_second": self.worker.counters["completed"],
                       "pricing_events_per_second": self.worker.counters["pricing_received"],
                       "websocket_messages_per_second": self.manager.counters["sent"]}
            lag = None
            produced_rate = None
            simulator = None
            try:
                async def read_lag():
                    workers = getattr(self.worker, "workers", [self.worker])
                    partitions = set().union(*(worker.consumer.assignment() for worker in workers))
                    if not partitions:
                        return None
                    ends = await self.worker.consumer.end_offsets(partitions)
                    values = {settings.kafka_inventory_topic: 0, settings.kafka_pricing_topic: 0}
                    by_partition = {}
                    for partition, end in ends.items():
                        committed = await self.worker.consumer.committed(partition)
                        value = max(0, end - (committed or 0))
                        values[partition.topic] = values.get(partition.topic, 0) + value
                        by_partition[f"{partition.topic}:{partition.partition}"] = value
                    return values, by_partition, sum(end for partition, end in ends.items() if partition.topic == settings.kafka_inventory_topic)
                lag = await asyncio.wait_for(read_lag(), 1.5)
                if lag is not None:
                    if previous_end is not None and lag[2] >= previous_end:
                        produced_rate = (lag[2] - previous_end) / elapsed
                    previous_end = lag[2]
            except Exception:
                pass  # Broker trouble must not break the dashboard or consumer.
            try:
                raw = await asyncio.wait_for(self.worker.workers[0].redis.get('simulator:status'), .3)
                simulator = json.loads(raw) if raw else None
            except Exception:
                pass  # Missing/expired producer telemetry is unavailable, not zero traffic.
            self.sample = {"rates": {**rates(current, previous, elapsed), "broker_produced_per_second": produced_rate},
                           "simulator": simulator,
                           "consumer_lag": sum(lag[0].values()) if lag is not None else None,
                           "lag_by_topic": lag[0] if lag is not None else None,
                           "lag_by_partition": lag[1] if lag is not None else None, "sampled_at": datetime.now(timezone.utc).isoformat(),
                           "process_cpu_percent": (cpu - previous_cpu) / elapsed * 100,
                           "process_rss_bytes": int(Path("/proc/self/statm").read_text().split()[1]) * 4096 if Path("/proc/self/statm").exists() else None,
                           "process_peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024 if resource else None}
            previous, started, previous_cpu = current, now, cpu
