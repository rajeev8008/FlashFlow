"""Bounded real HTTP/WebSocket clients. Run inside the API container, redirect JSON.

python tests/load_probe.py --clients 1 10 100 --seconds 10
"""
import argparse
import asyncio
import json
import time
import urllib.request
from datetime import datetime, timezone
from websockets.asyncio.client import connect


def percentile(values, fraction):
    if not values:
        return None
    import math
    return sorted(values)[max(0, math.ceil(len(values) * fraction) - 1)]


def request(path):
    with urllib.request.urlopen(f'http://localhost:8000{path}', timeout=5) as response:
        return json.load(response)


async def run_case(clients, seconds):
    connected = errors = messages = http_ok = http_errors = 0
    fanout = []
    event_latency = []
    start = asyncio.Event()
    ready = 0
    async def api_client():
        nonlocal http_ok, http_errors
        await start.wait()
        until = time.monotonic() + seconds
        while time.monotonic() < until:
            tick = time.monotonic()
            try:
                await asyncio.to_thread(request, '/products?limit=10')
                http_ok += 1
            except Exception:
                http_errors += 1
            await asyncio.sleep(max(0, 2 - (time.monotonic() - tick)))
    async def client():
        nonlocal connected, errors, messages, ready
        announced = False
        try:
            async with connect('ws://localhost:8000/ws', open_timeout=10, max_queue=1024) as socket:
                connected += 1
                ready += 1
                announced = True
                await start.wait()
                until = time.monotonic() + seconds
                async def heartbeat():
                    while time.monotonic() < until:
                        await asyncio.sleep(10)
                        await socket.send('pong')
                pongs = asyncio.create_task(heartbeat())
                try:
                    while time.monotonic() < until:
                        try:
                            raw = await asyncio.wait_for(socket.recv(), max(.01, until - time.monotonic()))
                        except TimeoutError:
                            break
                        message = json.loads(raw)
                        if message['type'] == 'heartbeat':
                            await socket.send('pong')
                        elif message['type'] == 'product_update':
                            messages += 1
                            now = datetime.now(timezone.utc)
                            for field, values in [('emitted_at', fanout), ('event_at', event_latency)]:
                                if message.get(field):
                                    latency = (now - datetime.fromisoformat(message[field])).total_seconds() * 1000
                                    if latency >= 0:
                                        values.append(latency)
                finally:
                    pongs.cancel()
                    await asyncio.gather(pongs, return_exceptions=True)
        except Exception:
            errors += 1
        finally:
            if not announced:
                ready += 1
    before = await asyncio.to_thread(request, '/metrics')
    tasks = [asyncio.create_task(client()) for _ in range(clients)]
    tasks += [asyncio.create_task(api_client()) for _ in range(clients)]
    while ready < clients:
        await asyncio.sleep(.01)
    start.set()
    started = time.monotonic()
    lags = []
    while time.monotonic() - started < seconds:
        tick = time.monotonic()
        try:
            snapshot = await asyncio.to_thread(request, '/metrics')
            lags.append(snapshot['consumer_lag'])
        except Exception:
            lags.append(None)
        await asyncio.sleep(max(0, 1 - (time.monotonic() - tick)))
    await asyncio.gather(*tasks)
    after = await asyncio.to_thread(request, '/metrics')
    return {'clients_requested': clients, 'successful_connections': connected, 'connection_errors': errors,
            'seconds': time.monotonic() - started, 'product_messages': messages, 'http_success': http_ok, 'http_errors': http_errors,
            'fanout_p50_ms': percentile(fanout, .5), 'fanout_p95_ms': percentile(fanout, .95),
            'event_to_client_p95_ms': percentile(event_latency, .95), 'lag_samples': lags,
            'slow_disconnects_delta': after['slow_client_disconnects'] - before['slow_client_disconnects'],
            'backend_before': before, 'backend_after': after}


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--clients', nargs='+', type=int, default=[1, 10, 100])
    parser.add_argument('--seconds', type=int, default=10)
    args = parser.parse_args()
    if not 1 <= args.seconds <= 120 or any(n < 1 or n > 200 for n in args.clients):
        parser.error('Use 1..200 clients and 1..120 seconds')
    results = [await run_case(n, args.seconds) for n in args.clients]
    print(json.dumps({'kind': 'real-http-websocket-load', 'recorded_at': datetime.now(timezone.utc).isoformat(),
                      'notes': 'Sequential cases; each client also requests /products?limit=10 every 2 seconds; existing simulator traffic; same-host clocks required; no rendering', 'results': results}, indent=2))


if __name__ == '__main__':
    asyncio.run(main())
