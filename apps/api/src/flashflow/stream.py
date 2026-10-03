import asyncio
import json
import logging
import time
from collections import Counter
from contextlib import suppress
from datetime import datetime, timezone

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from fastapi import WebSocket
from redis.asyncio import Redis
from redis.exceptions import RedisError
from sqlalchemy.exc import SQLAlchemyError

from .config import settings
from .inventory import process_event, process_batch
from .pricing import apply_decision, publish_decision, publish_decisions
from aiokafka.errors import KafkaError
from .schemas import EventEnvelope
from .resilience import DependencyUnavailable

from .timing import timings

logger = logging.getLogger("flashflow")

# Cross-topic/group-member delivery can race; never replace a newer cache revision.
CACHE_UPDATE = """
local old = redis.call('GET', KEYS[1])
if old then
  local before = cjson.decode(old)
  local after = cjson.decode(ARGV[1])
  if before.version > after.version or (before.price_version or 0) > (after.price_version or 0) then
    return 0
  end
end
redis.call('SET', KEYS[1], ARGV[1])
return 1
"""


class ConnectionManager:
    def __init__(self):
        self.clients: set[asyncio.Queue] = set()
        self.counters = Counter()

    def connect(self) -> asyncio.Queue:
        queue = asyncio.Queue(maxsize=settings.websocket_queue_size)
        self.clients.add(queue)
        logger.info(json.dumps({"action": "client_connected", "clients": len(self.clients)}))
        return queue

    def disconnect(self, queue):
        if queue not in self.clients:
            return
        self.counters["disconnected"] += 1
        self.clients.discard(queue)
        logger.info(json.dumps({"action": "client_disconnected", "clients": len(self.clients)}))

    def broadcast(self, message: dict):
        started = time.perf_counter()
        message = {**message, '_queued_clock': started}
        for queue in tuple(self.clients):
            if queue.full():
                # Disconnect slow clients; reconnect fetches a durable snapshot.
                while not queue.empty():
                    queue.get_nowait()
                queue.put_nowait(None)
                self.counters["slow_disconnects"] += 1
                self.disconnect(queue)
            else:
                queue.put_nowait(message)

        timings.observe("websocket_enqueue", (time.perf_counter() - started) * 1000)

    async def serve(self, socket: WebSocket):
        await socket.accept()
        queue = self.connect()

        async def send():
            while True:
                try:
                    message = await asyncio.wait_for(queue.get(), settings.websocket_heartbeat_seconds)
                except TimeoutError:
                    message = {"type": "heartbeat"}
                if message is None:
                    await socket.close(code=1013)
                    return
                if message.get("type") == "product_update":
                    message = {**message, "websocket_sent_at": datetime.now(timezone.utc).isoformat()}
                    queued_clock = message.pop('_queued_clock', None)
                    if queued_clock is not None:
                        timings.observe("websocket_queue", (time.perf_counter() - queued_clock) * 1000)
                try:
                    with timings.measure("websocket_send"):
                        await asyncio.wait_for(socket.send_json(message), settings.websocket_send_timeout_seconds)
                except Exception:
                    self.counters["failed_sends"] += 1
                    raise
                if message.get("type") == "product_update":
                    self.counters["sent"] += 1

        async def receive():
            while True:
                if await asyncio.wait_for(socket.receive_text(), settings.websocket_heartbeat_seconds * 3) != "pong":
                    await socket.close(code=1008)
                    return

        tasks = [asyncio.create_task(send()), asyncio.create_task(receive())]
        try:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                with suppress(Exception):
                    task.result()
        finally:
            self.disconnect(queue)
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            with suppress(Exception):
                await asyncio.wait_for(socket.close(code=1013), settings.websocket_send_timeout_seconds)


class InventoryConsumer:
    def __init__(self, manager: ConnectionManager, faults=None, counters=None):
        self.manager = manager
        self.faults = faults
        self.counters = counters if counters is not None else Counter()
        self.stopping = asyncio.Event()
        self.redis = Redis.from_url(settings.redis_url, socket_timeout=1, socket_connect_timeout=1)
        self.consumer = AIOKafkaConsumer(settings.kafka_inventory_topic, settings.kafka_pricing_topic,
            bootstrap_servers=settings.kafka_bootstrap_servers, group_id=settings.kafka_consumer_group,
            enable_auto_commit=False, auto_offset_reset="earliest", max_poll_interval_ms=300000)
        self.producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)

    async def handle(self, record, durable_result=None, received_at=None, processing_started_at=None, processing_started_clock=None):
        received_at = received_at or datetime.now(timezone.utc)
        started = time.perf_counter()
        for attempt in range(1, settings.consumer_max_attempts + 1):
            try:
                raw = json.loads(record.value)
                if not isinstance(raw, dict) or not {"event_id", "event_type", "product_id", "timestamp", "schema_version", "source", "payload"} <= raw.keys():
                    raise ValueError("event envelope is missing required fields")
                event = EventEnvelope.model_validate_json(record.value)
                if event.schema_version != "1.0" or event.timestamp.tzinfo is None:
                    raise ValueError("unsupported schema version or naive timestamp")
                if record.key != str(event.product_id).encode():
                    raise ValueError("Kafka key must match product_id")
                timings.observe("producer_to_consumer", (received_at - event.timestamp).total_seconds() * 1000)
                if getattr(record, 'timestamp_type', None) == 1:
                    appended_at = datetime.fromtimestamp(record.timestamp / 1000, timezone.utc)
                    timings.observe('broker_to_consumer', (received_at - appended_at).total_seconds() * 1000)
                    if event.producer_enqueued_at is not None and event.producer_enqueued_at.tzinfo is not None:
                        timings.observe('producer_to_broker', (appended_at - event.producer_enqueued_at).total_seconds() * 1000)
                processing_at = processing_started_at or datetime.now(timezone.utc)
                if durable_result is not None and attempt == 1:
                    product, outcome = durable_result
                else:
                    with timings.measure("database"):
                        product, outcome = await (apply_decision(event) if record.topic == settings.kafka_pricing_topic else process_event(event))
                if record.topic == settings.kafka_inventory_topic and (durable_result is None or attempt > 1):
                    with timings.measure("pricing_publish"):
                        await publish_decision(event, self.producer)
                if getattr(self, "faults", None) and self.faults.redis_unavailable:
                    raise DependencyUnavailable("Redis failure injected")
                # Replays repair cache/broadcast after a database commit interrupted by failure.
                with timings.measure("redis"):
                    await self.redis.eval(CACHE_UPDATE, 1, f"product:{product.product_id}", product.model_dump_json())
                self.manager.broadcast({"type": "product_update", "event_id": str(event.event_id), "event_at": event.timestamp.isoformat(),
                                        "kafka_received_at": received_at.isoformat(), "processing_started_at": processing_at.isoformat(),
                                        "processing_finished_at": datetime.now(timezone.utc).isoformat(),
                                        "event_created_at": event.timestamp.isoformat(),
                                        "emitted_at": datetime.now(timezone.utc).isoformat(), "product": product.model_dump(mode="json")})
                timings.observe("processing", (time.perf_counter() - (processing_started_clock or started)) * 1000)
                self.counters["completed"] += 1
                if record.topic == settings.kafka_inventory_topic:
                    self.counters["inventory_completed"] += 1
                self.counters[outcome] += 1
                return
            except Exception as error:
                self.counters["failed"] += 1
                logger.warning(json.dumps({"action": "event_failed", "partition": record.partition, "offset": record.offset, "attempt": attempt, "error_type": type(error).__name__}))
                if attempt < settings.consumer_max_attempts:
                    if not isinstance(error, ValueError):
                        await asyncio.sleep(settings.consumer_retry_seconds * 2 ** (attempt - 1))
                else:
                    if isinstance(error, (ConnectionError, TimeoutError, RedisError, SQLAlchemyError, KafkaError)):
                        # Infrastructure outages retain the Kafka offset; only bad events go to DLQ.
                        raise
                    failure = {"topic": record.topic, "partition": record.partition, "offset": record.offset,
                               "key": record.key.hex() if record.key else None, "raw_value_hex": record.value.hex(),
                               "error": str(error), "attempts": attempt, "timestamp": datetime.now(timezone.utc).isoformat()}
                    await self.producer.send_and_wait(settings.kafka_dlq_topic, key=record.key, value=json.dumps(failure).encode())
                    self.counters["dlq"] += 1

    async def handle_batch(self, records, received_at):
        if isinstance(received_at, tuple):
            received_at, received_clock = received_at
        else:
            received_clock = time.perf_counter()
        processing_at = datetime.now(timezone.utc)
        processing_clock = time.perf_counter()
        for record in records:
            timings.observe("consumer_queue_wait", (processing_clock - received_clock) * 1000)
        results = None
        if settings.consumer_batch_enabled and records[0].topic == settings.kafka_inventory_topic:
            try:
                events = [EventEnvelope.model_validate_json(record.value) for record in records]
                for record, event in zip(records, events):
                    raw = json.loads(record.value)
                    if not {"event_id", "event_type", "product_id", "timestamp", "schema_version", "source", "payload"} <= raw.keys():
                        raise ValueError("event envelope is missing required fields")
                    if event.schema_version != "1.0" or event.timestamp.tzinfo is None or record.key != str(event.product_id).encode():
                        raise ValueError("invalid envelope or partition key")
                started = time.perf_counter()
                with timings.measure("database_batch"):
                    results = await process_batch(events)
                timings.observe("database_amortized", (time.perf_counter() - started) * 1000 / len(records))
            except ValueError:
                # The transaction rolled back. Isolate bad records without losing valid neighbors.
                results = None
            if results is not None:
                with timings.measure("pricing_publish_batch"):
                    await publish_decisions(events, self.producer)
        for index, record in enumerate(records):
            await self.handle(record, results[index] if results is not None else None, received_at, processing_at, processing_clock)

    async def run(self):
        try:
            await self.producer.start()
            await self.consumer.start()
            while not self.stopping.is_set():
                batches = await self.consumer.getmany(timeout_ms=settings.consumer_poll_ms,
                    max_records=settings.consumer_batch_size * settings.kafka_inventory_partitions)
                received_at = datetime.now(timezone.utc), time.perf_counter()
                for partition, fetched in batches.items():
                    for start in range(0, len(fetched), settings.consumer_batch_size):
                        records = fetched[start:start + settings.consumer_batch_size]
                        self.counters["kafka_received"] += len(records)
                        if partition.topic == settings.kafka_pricing_topic:
                            self.counters["pricing_received"] += len(records)
                        # Commit only contiguous successful records; a revoke leaves replayable work.
                        while partition in self.consumer.assignment():
                            try:
                                while self.faults and self.faults.consumer_paused and not self.stopping.is_set():
                                    await asyncio.sleep(.2)
                                with timings.measure("processing_batch"):
                                    await self.handle_batch(records, received_at)
                                if partition in self.consumer.assignment():
                                    with timings.measure("offset_commit"):
                                        await self.consumer.commit({partition: records[-1].offset + 1})
                                break
                            except Exception as error:
                                logger.error(json.dumps({"action": "consumer_retry", "error_type": type(error).__name__}))
                                await asyncio.sleep(max(settings.consumer_retry_seconds, .1))
        finally:
            await self.consumer.stop()
            await self.producer.stop()
            await self.redis.aclose()


class ConsumerGroup:
    """Co-located group members share the gateway and process-local metrics."""
    def __init__(self, manager, faults):
        self.counters = Counter()
        self.workers = [InventoryConsumer(manager, faults, self.counters)
                        for _ in range(settings.consumer_instances)]
        self.producer = self.workers[0].producer
        self.consumer = self.workers[0].consumer

    async def run(self):
        tasks = [asyncio.create_task(worker.run()) for worker in self.workers]
        try:
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    def stop(self):
        for worker in self.workers:
            worker.stopping.set()
