# FlashFlow

FlashFlow streams simulated retail inventory through Kafka, persists validated changes in PostgreSQL, caches the latest products in Redis, and broadcasts updates to a Next.js dashboard over WebSockets. Phases 1–3 are implemented.

## Start locally

Requirements: Docker Desktop with Compose.

```bash
cp .env.example .env
docker compose up --build
```

Open:

- Frontend: http://localhost:3000
- API docs: http://localhost:8000/docs
- Health check: http://localhost:8000/health
- Seeded products: http://localhost:8000/products

The API container runs Alembic migrations and idempotently seeds 500 products before starting. The simulator then publishes inventory events to `inventory-events`, keyed by `product_id` to preserve per-product partition ordering.

Inspect events:

```bash
docker compose exec kafka kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic inventory-events --from-beginning --max-messages 5 --formatter org.apache.kafka.tools.consumer.DefaultMessageFormatter --formatter-property print.key=true
```

Run backend tests:

```bash
docker compose run --rm api sh -c "pip install -e '.[test]' && pytest"
```

## Configuration

Copy `.env.example` to `.env`. Important settings:

| Variable | Default | Purpose |
| --- | --- | --- |
| `DATABASE_URL` | container PostgreSQL URL | Async database connection |
| `REDIS_URL` | `redis://redis:6379/0` | Redis connectivity |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka brokers |
| `KAFKA_INVENTORY_TOPIC` | `inventory-events` | Inventory topic |
| `KAFKA_INVENTORY_PARTITIONS` | `6` | Topic partitions |
| `SIMULATOR_MODE` | `NORMAL` | `NORMAL`, `BUSY`, `FLASH_SALE`, or `EXTREME` |
| `SIMULATOR_EVENTS_PER_SECOND` | `10` | Explicit rate; omit to use the selected mode default |
| `SIMULATOR_PRODUCT_COUNT` | `100` | Seeded products included in simulation |
| `SIMULATOR_BURST_DURATION_SECONDS` | `0` | Run duration; `0` means continuous |
| `SIMULATOR_RANDOM_SEED` | `42` | Reproducible event choices |
| `SIMULATOR_EVENT_MIX` | weighted event pairs | Comma-separated `EVENT_TYPE:weight` values |

Mode defaults are 10, 100, 500, and 2,000 events/sec respectively. Explicit `SIMULATOR_EVENTS_PER_SECOND` takes precedence.

Run the live integration and restart checks against the running stack:

```bash
docker compose exec api python tests/integration_phase2.py
docker compose restart api
docker compose exec api python tests/integration_phase2.py --restart-check
```

The integration probe creates an isolated product, verifies two WebSocket clients, Redis, duplicate/stale handling, and DLQ publication. Wait for `/health` to return OK after restarting the API.

## Phase 2 architecture

```text
seeded catalog -> simulator -> Kafka inventory-events
                                 |
                          FastAPI async consumer
                                 |
                     PostgreSQL transaction + event ledger
                                 |
                         Redis product snapshots
                                 |
                         WebSocket /ws -> Next.js
invalid/exhausted events -> inventory-events-dlq
```

PostgreSQL updates and event-ID receipts commit together under a product row lock. Duplicate events cannot double-apply, stale versions are ignored, and version gaps or invalid stock transitions are retried and dead-lettered. Payload snapshots are checked against computed transitions rather than trusted as inventory state.

Kafka auto-commit is disabled. Each partition offset advances only after database/cache processing and broadcast enqueue, or acknowledged DLQ publication. A cache failure after a database commit is repaired on replay without applying the event again. DLQ publication or offset-commit failures retry the same record. Delivery to browsers is best-effort; reconnecting clients fetch a durable catalog snapshot and ignore older versions.

WebSockets use heartbeat messages and client `pong` replies. Each client has a bounded queue; slow clients disconnect and reconnect without blocking the consumer. `/metrics` exposes processed, failed-attempt, duplicate, stale, DLQ, and active-client counters. Counters reset when the API restarts. Structured JSON event-failure and connection logs are written to container output.

Additional configuration: `KAFKA_CONSUMER_GROUP`, `KAFKA_DLQ_TOPIC`, `CONSUMER_MAX_ATTEMPTS`, `CONSUMER_RETRY_SECONDS`, and `WEBSOCKET_HEARTBEAT_SECONDS`; defaults are listed in `.env.example`.

## Current limits

Run one API worker and one simulator. The consumer and connection manager share the API process; multiple gateways need brokered fanout later. The simulator waits for the previous Kafka backlog to drain before loading its starting catalog. A rejected version can require manual reconciliation and DLQ replay for that product. Redis outages exceeding retries can leave its snapshot behind PostgreSQL until another event or replay repairs it. The event ledger is retained indefinitely for this portfolio scope.

Dynamic pricing, cache fallback, circuit breakers, fault injection, authentication, production TLS, and systematic load benchmarks remain later work. This Compose stack is for local development.

## Phase 3 frontend

`apps/frontend/lib/live.ts` owns product validation, WebSocket lifecycle, Zustand state, version guards, and the frame buffer. `app/page.tsx` contains the product grid, per-product subscriptions, memoized cards, mode selector, and sampled engineering panel. `app/styles.css` provides the responsive dashboard without an additional UI dependency.

```text
WebSocket -> validate -> buffer keyed by product_id -> requestAnimationFrame
          -> latest version per product -> one Zustand transaction
          -> subscribed product cards -> commit measurements
```

The normalized store contains `productsById`, stable `ids`, connection state, and render mode. Updates retain unrelated product object identities; equal/older versions cannot replace newer ones, including snapshots fetched after reconnect. BATCHED drains at most once per animation frame. Switching modes drains pending events; unmount cancels the pending frame, timers, fetch, and socket.

The lifecycle supports CONNECTING, LIVE, RECONNECTING, DEGRADED, and OFFLINE. Reconnect delays grow from 1 to 30 seconds and reset on a successful socket open. Each reconnect fetches a fresh durable catalog. Invalid messages/catalog responses mark the retained dashboard DEGRADED; network loss marks it OFFLINE. Heartbeats detect silent connections after 45 seconds. Phase 4 will add explicit backend dependency/fallback metadata; DEGRADED currently means a client-side ingestion/snapshot problem, not a Redis fallback guarantee.

### Comparing modes

| Mode | Subscription | Card behavior | Ingestion |
| --- | --- | --- | --- |
| NAIVE | Entire product map, converted to an array for the grid | Whole grid rerenders | Immediate per valid event |
| ATOMIC | Product-specific selector | Unmemoized; parent renders reach cards | Immediate |
| MEMOIZED | Product-specific selector | Memoized card boundary | Immediate |
| BATCHED | Product-specific selector | Memoized card boundary | Latest product state per frame |

The optimized grid includes an explicit one-second parent pulse to demonstrate ATOMIC versus MEMOIZED. This is benchmark overhead, not an inventory update. React may independently batch immediate updates, so do not assume every event produces a commit. Mode switches remount some cards; wait for a fresh sample before comparing. Use the same event rate, product count, browser, foreground tab, and run duration for each mode.

The panel samples actual counters every second: valid socket updates (including stale ones), event-driven store flushes, accepted events per flush, committed card renders, browser receipt-to-card-commit latency, and animation-frame FPS. Catalog hydration and mode remounts count as card commits but not socket flushes. Latency measures only rendered products and excludes server/network time; coalesced-away versions have no render sample. FPS is an estimate affected by background throttling. Demand displays the backend `sales_velocity` value, which is currently zero; demand calculation belongs to Phase 5.

### Frontend checks

From `apps/frontend`, with Node.js 20+ and the frontend running:

```bash
npm ci
npm test
npm run lint
npx playwright install chromium
npm run test:e2e
```

Enable the optional real-stack test in PowerShell:

```powershell
$env:LIVE_STACK='1'
npm run test:e2e
```

Unit checks cover validation, coalescing, frame scheduling/cancellation, targeted state identities, stale versions, and capped backoff. Browser checks cover changing prices/stock, unrelated-card isolation, genuine mode differences, reconnect snapshot refresh, degraded/offline transitions, a synthetic 2,000-message burst across a 500-card dashboard, and optional live Kafka delivery with desktop/mobile layout checks.

For a bounded real surge, stop the normal simulator first (never run two concurrently):

```bash
docker compose stop simulator
docker compose run --rm --no-deps -e SIMULATOR_MODE=FLASH_SALE -e SIMULATOR_EVENTS_PER_SECOND=500 -e SIMULATOR_BURST_DURATION_SECONDS=20 simulator
docker compose start simulator
```

Validation on the development machine: five frontend unit tests, three browser tests with the live stack enabled, ten backend tests, TypeScript checking, and a production build passed. A 2,000-message synthetic burst produced five updated-card commits in one run; this varies with scheduling. During a bounded 500-events/s producer run/backlog drain, one browser sample observed 92 received updates/s, 60 flushes/s, approximately 6 ms receipt-to-commit latency and 60 FPS. The configured producer rate is **not** measured end-to-end throughput. These are illustrative local samples, not controlled benchmark claims.

Phase 4 can build on the retained product store and connection states to add cache freshness, dependency failure metadata, circuit breakers, and restricted chaos controls. Those features are deliberately not implemented here. The client currently renders the full catalog and expects the existing API on port 8000; virtualization and configurable production WebSocket routing should be added when deployment or catalog size requires them.
