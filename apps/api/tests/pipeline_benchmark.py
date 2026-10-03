"""Real Kafka -> durable state -> socket benchmark; no browser-render claims.

Stop the normal simulator first. Run in the API container and redirect JSON.
Each case waits for committed drain before loading product versions.
"""
import argparse
import asyncio
import json
import random
import time
from collections import deque
from datetime import datetime, timezone
from websockets.asyncio.client import connect
from aiokafka import AIOKafkaProducer

from flashflow.config import settings
from flashflow.schemas import kafka_record
from flashflow.simulator import load_catalog, next_event, wait_for_backlog, report_producer
from redis.asyncio import Redis
from load_probe import request, percentile


async def run_case(rate, seconds, drain_timeout):
    await asyncio.wait_for(wait_for_backlog(), drain_timeout)
    catalog = await load_catalog(500)
    rng = random.Random(42)
    latencies = deque(maxlen=100000)
    fanout = deque(maxlen=100000)
    producer_ack = deque(maxlen=100000)
    samples, errors = [], []
    seen = 0
    stop = asyncio.Event()
    before = await asyncio.to_thread(request, '/metrics')
    async def receive(socket):
        nonlocal seen
        while not stop.is_set():
            try:
                raw = await asyncio.wait_for(socket.recv(), 1)
            except TimeoutError:
                continue
            message = json.loads(raw)
            if message['type'] == 'heartbeat':
                await socket.send('pong')
            elif message['type'] == 'product_update':
                seen += 1
                now = datetime.now(timezone.utc)
                for field, values in [('event_at', latencies), ('emitted_at', fanout)]:
                    value = (now - datetime.fromisoformat(message[field])).total_seconds() * 1000
                    if value >= 0:
                        values.append(value)

    async def sample():
        while not stop.is_set():
            try:
                samples.append({'elapsed_seconds': time.monotonic() - started,
                                **await asyncio.to_thread(request, '/metrics')})
            except Exception as error:
                errors.append(str(error))
            await asyncio.sleep(1)

    async def heartbeat(socket):
        while not stop.is_set():
            await asyncio.sleep(10)
            await socket.send('pong')

    producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)
    await producer.start()
    sent = 0
    reported_at = 0
    redis = Redis.from_url(settings.redis_url, socket_timeout=.2, socket_connect_timeout=.2)
    started = time.monotonic()
    async with connect('ws://localhost:8000/ws', max_queue=4096) as socket:
        reader = asyncio.create_task(receive(socket))
        sampler = asyncio.create_task(sample())
        pongs = asyncio.create_task(heartbeat(socket))
        try:
            # Evenly paced 20ms micro-bursts instead of a one-second burst.
            while time.monotonic() - started < seconds:
                target = min(int((time.monotonic() - started) * rate), rate * seconds)
                while sent < target:
                    event = next_event(rng.choice(catalog), rng)
                    record = kafka_record(event)
                    enqueued = time.perf_counter()
                    acknowledgement = await producer.send(settings.kafka_inventory_topic, key=record.key, value=record.value)
                    acknowledgement.add_done_callback(lambda future, at=enqueued:
                        producer_ack.append((time.perf_counter() - at) * 1000) if not future.cancelled() and future.exception() is None else None)
                    sent += 1
                await asyncio.sleep(.02)
                if time.monotonic() - reported_at >= 1:
                    await report_producer(redis, rate, sent)
                    reported_at = time.monotonic()
            await producer.flush()
            producer_seconds = time.monotonic() - started
            at_producer_stop = await asyncio.to_thread(request, '/metrics')
            drain_started = time.monotonic()
            drained = True
            try:
                await asyncio.wait_for(wait_for_backlog(), drain_timeout)
            except TimeoutError:
                drained = False
                errors.append('Committed drain timed out')
            drain_seconds = time.monotonic() - drain_started
            await asyncio.sleep(1)
        finally:
            stop.set()
            sampler.cancel()
            reader.cancel()
            pongs.cancel()
            results = await asyncio.gather(reader, sampler, pongs, return_exceptions=True)
            errors.extend(str(result) for result in results
                          if isinstance(result, Exception))
            await producer.stop()
            await redis.aclose()
    after = await asyncio.to_thread(request, '/metrics')
    return {'configured_rate': rate, 'configured_seconds': seconds, 'produced': sent,
            'producer_seconds': producer_seconds, 'actual_producer_rate': sent / producer_seconds,
            'drained': drained, 'drain_seconds': drain_seconds, 'socket_messages': seen,
            'event_to_client_ms': {f'p{int(p*100)}': percentile(latencies, p) for p in (.5, .95, .99)},
            'enqueue_to_client_ms': {f'p{int(p*100)}': percentile(fanout, p) for p in (.5, .95, .99)},
            'producer_ack_ms': {f'p{int(p*100)}': percentile(producer_ack, p) for p in (.5, .95, .99)},
            'latency_window_count': len(latencies),
            'maximum_sampled_lag': max((s['consumer_lag'] for s in samples if s['consumer_lag'] is not None), default=None),
            'inventory_completed_during_production': at_producer_stop['inventory_completed'] - before['inventory_completed'],
            'inventory_throughput_including_drain': (after['inventory_completed'] - before['inventory_completed']) / (producer_seconds + drain_seconds),
            'backend_at_producer_stop': at_producer_stop,
            'errors': errors, 'backend_before': before, 'backend_after': after, 'samples': samples}


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--rates', nargs='+', type=int, default=[100, 500, 1000, 2000])
    parser.add_argument('--seconds', type=int, default=180)
    parser.add_argument('--drain-timeout', type=int, default=600)
    parser.add_argument('--label', default='pipeline')
    args = parser.parse_args()
    if not 1 <= args.seconds <= 3600 or any(not 1 <= r <= 10000 for r in args.rates):
        parser.error('seconds: 1..3600; rates: 1..10000')
    results = [await run_case(rate, args.seconds, args.drain_timeout) for rate in args.rates]
    print(json.dumps({'kind': 'real-pipeline', 'label': args.label,
                      'recorded_at': datetime.now(timezone.utc).isoformat(),
                      'notes': 'Same-host clocks; API-colocated generator; latest 100000 socket latency samples; rolling 4096 backend stage samples; no rendered latency or FPS',
                      'results': results}, indent=2))


if __name__ == '__main__':
    asyncio.run(main())
