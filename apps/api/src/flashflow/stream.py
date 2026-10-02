import asyncio
import json
import logging
from collections import Counter
from contextlib import suppress
from datetime import datetime, timezone

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from fastapi import WebSocket
from redis.asyncio import Redis

from .config import settings
from .inventory import process_event
from .schemas import EventEnvelope

logger = logging.getLogger("flashflow")


class ConnectionManager:
    def __init__(self):
        self.clients: set[asyncio.Queue] = set()

    def connect(self) -> asyncio.Queue:
        queue = asyncio.Queue(maxsize=100)
        self.clients.add(queue)
        logger.info(json.dumps({"action": "client_connected", "clients": len(self.clients)}))
        return queue

    def disconnect(self, queue):
        self.clients.discard(queue)
        logger.info(json.dumps({"action": "client_disconnected", "clients": len(self.clients)}))

    def broadcast(self, message: dict):
        for queue in tuple(self.clients):
            if queue.full():
                # Disconnect slow clients; reconnect fetches a durable snapshot.
                while not queue.empty():
                    queue.get_nowait()
                queue.put_nowait(None)
                self.disconnect(queue)
            else:
                queue.put_nowait(message)

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
                await asyncio.wait_for(socket.send_json(message), settings.websocket_heartbeat_seconds)

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


class InventoryConsumer:
    def __init__(self, manager: ConnectionManager):
        self.manager = manager
        self.counters = Counter()
        self.redis = Redis.from_url(settings.redis_url)
        self.consumer = AIOKafkaConsumer(settings.kafka_inventory_topic,
            bootstrap_servers=settings.kafka_bootstrap_servers, group_id=settings.kafka_consumer_group,
            enable_auto_commit=False, auto_offset_reset="earliest", max_poll_interval_ms=300000)
        self.producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)

    async def handle(self, record):
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
                product, outcome = await process_event(event)
                # Replays repair cache/broadcast after a database commit interrupted by failure.
                await self.redis.set(f"product:{product.product_id}", product.model_dump_json())
                self.manager.broadcast({"type": "product_update", "event_id": str(event.event_id), "product": product.model_dump(mode="json")})
                self.counters[outcome] += 1
                return
            except Exception as error:
                self.counters["failed"] += 1
                logger.warning(json.dumps({"action": "event_failed", "partition": record.partition, "offset": record.offset, "attempt": attempt, "error": str(error)}))
                if attempt < settings.consumer_max_attempts:
                    if not isinstance(error, ValueError):
                        await asyncio.sleep(settings.consumer_retry_seconds * 2 ** (attempt - 1))
                else:
                    failure = {"topic": record.topic, "partition": record.partition, "offset": record.offset,
                               "key": record.key.hex() if record.key else None, "raw_value_hex": record.value.hex(),
                               "error": str(error), "attempts": attempt, "timestamp": datetime.now(timezone.utc).isoformat()}
                    await self.producer.send_and_wait(settings.kafka_dlq_topic, key=record.key, value=json.dumps(failure).encode())
                    self.counters["dlq"] += 1

    async def run(self):
        try:
            await self.producer.start()
            await self.consumer.start()
            async for record in self.consumer:
                # Failed DLQ publication or commit retains this offset and retries the same record.
                while True:
                    try:
                        await self.handle(record)
                        await self.consumer.commit({TopicPartition(record.topic, record.partition): record.offset + 1})
                        break
                    except Exception as error:
                        logger.error(json.dumps({"action": "consumer_retry", "error": str(error)}))
                        await asyncio.sleep(max(settings.consumer_retry_seconds, 0.1))
        finally:
            await self.consumer.stop()
            await self.producer.stop()
            await self.redis.aclose()
