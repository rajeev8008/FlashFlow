# FlashFlow

FlashFlow streams simulated retail inventory and audited rule-based prices through Kafka, persists validated changes in PostgreSQL, caches the latest products in Redis, and broadcasts updates to a Next.js dashboard over WebSockets. Phases 1–5 are implemented.

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

Kafka auto-commit is disabled. Each partition offset advances only after database/cache processing and broadcast enqueue, or acknowledged DLQ publication. A cache failure after a database commit is repaired on replay without applying the event again. Infrastructure connection/time-out failures retain the record for retry rather than dead-lettering valid updates. DLQ publication or offset-commit failures retry the same record. Delivery to browsers is best-effort; reconnecting clients fetch a durable catalog snapshot and ignore older versions.

WebSockets use heartbeat messages and client `pong` replies. Each client has a bounded queue; slow clients disconnect and reconnect without blocking the consumer. `/metrics` exposes processed, failed-attempt, duplicate, stale, DLQ, and active-client counters. Counters reset when the API restarts. Structured JSON event-failure and connection logs are written to container output.

Additional configuration: `KAFKA_CONSUMER_GROUP`, `KAFKA_DLQ_TOPIC`, `CONSUMER_MAX_ATTEMPTS`, `CONSUMER_RETRY_SECONDS`, and `WEBSOCKET_HEARTBEAT_SECONDS`; defaults are listed in `.env.example`.

## Current limits

Run one API worker and one simulator. The consumer and connection manager share the API process; multiple gateways need brokered fanout later. The simulator waits for the previous Kafka backlog to drain before loading its starting catalog. A rejected version can require manual reconciliation and DLQ replay for that product. Redis outages retain the affected Kafka record and temporarily hold consumer progress until its cache/broadcast can be repaired. The event ledger is retained indefinitely for this portfolio scope.

Application authentication, production TLS, and systematic load benchmarks remain later work. Development fault controls use token authentication, not a general application login. This Compose stack is for local development.

## Phase 3 frontend

`apps/frontend/lib/live.ts` owns product validation, WebSocket lifecycle, Zustand state, version guards, and the frame buffer. `app/page.tsx` contains the product grid, per-product subscriptions, memoized cards, mode selector, and sampled engineering panel. `app/styles.css` provides the responsive dashboard without an additional UI dependency.

```text
WebSocket -> validate -> buffer keyed by product_id -> requestAnimationFrame
          -> latest version per product -> one Zustand transaction
          -> subscribed product cards -> commit measurements
```

The normalized store contains `productsById`, stable `ids`, connection state, and render mode. Updates retain unrelated product object identities; equal/older versions cannot replace newer ones, including snapshots fetched after reconnect. BATCHED drains at most once per animation frame. Switching modes drains pending events; unmount cancels the pending frame, timers, fetch, and socket.

The lifecycle supports CONNECTING, LIVE, RECONNECTING, DEGRADED, and OFFLINE. Reconnect delays grow from 1 to 30 seconds and reset on a successful socket open. Each reconnect fetches a durable catalog, followed by periodic health snapshots. Invalid messages/catalog responses mark the retained dashboard DEGRADED; network loss marks it OFFLINE. Heartbeats detect silent connections after 45 seconds. Backend cache/freshness metadata now also controls degraded mode, as described below.

### Comparing modes

| Mode | Subscription | Card behavior | Ingestion |
| --- | --- | --- | --- |
| NAIVE | Entire product map, converted to an array for the grid | Whole grid rerenders | Immediate per valid event |
| ATOMIC | Product-specific selector | Unmemoized; parent renders reach cards | Immediate |
| MEMOIZED | Product-specific selector | Memoized card boundary | Immediate |
| BATCHED | Product-specific selector | Memoized card boundary | Latest product state per frame |

The optimized grid includes an explicit one-second parent pulse to demonstrate ATOMIC versus MEMOIZED. This is benchmark overhead, not an inventory update. React may independently batch immediate updates, so do not assume every event produces a commit. Mode switches remount some cards; wait for a fresh sample before comparing. Use the same event rate, product count, browser, foreground tab, and run duration for each mode.

The panel samples actual counters every second: valid socket updates (including stale ones), event-driven store flushes, accepted events per flush, committed card renders, browser receipt-to-card-commit latency, and animation-frame FPS. Catalog hydration and mode remounts count as card commits but not socket flushes. Latency measures only rendered products and excludes server/network time; coalesced-away versions have no render sample. FPS is an estimate affected by background throttling. Demand now displays the backend window-based `sales_velocity` described below.

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

The client currently renders the full catalog and expects the existing API on port 8000; virtualization and configurable production WebSocket routing should be added when deployment or catalog size requires them.

## Resilience and development lab

`GET /inventory` returns `{products, metadata}`. PostgreSQL remains authoritative. Validated live catalog reads run through a configurable circuit breaker; successful reads save an atomic full-catalog Redis snapshot under `catalog:snapshot`. Existing `product:<id>` event caches remain unchanged. The legacy `/products` list endpoint is retained for inspection/integration compatibility; the frontend proxy uses `/inventory` for resilience metadata.

After three consecutive read failures, the circuit becomes OPEN and skips live reads for five seconds. The next request admits a serialized HALF_OPEN probe; one successful probe closes it, while a failed probe reopens it. `BREAKER_FAILURE_THRESHOLD`, `BREAKER_RECOVERY_SECONDS`, `BREAKER_HALF_OPEN_TRIALS`, and `INVENTORY_TIMEOUT_SECONDS` configure these values. Half-open trials are sequential successes, not parallel requests. Transition history (last 20 transitions), state, failures, and retry delay are visible in metadata, `/metrics`, structured logs, and the dashboard.

If live reads fail or cannot be validated, the API validates and serves the last Redis catalog with `source: redis`, `stale: true`, snapshot timestamp, age, reason, and circuit metadata. If Redis is also unavailable or its snapshot is missing/corrupt, the response is explicitly unavailable, not an empty live catalog. The browser retains its current products and last known snapshot timestamp. A failed cache refresh or paused/stopped consumer also marks data degraded. Reading paused inventory does not refresh its freshness timestamp.

The client polls the health/catalog envelope two seconds after each completed request while its socket is open. This discovers outages and recovery even when no product events arrive. Version guards prevent cached/older snapshots from rolling cards back; unchanged products preserve their object identities and render isolation. Recovery clears the stale warning and restores LIVE without a page reload. Receipt-to-render instrumentation still refers to WebSocket events, not periodic snapshot hydration.

### Enable local controls explicitly

Controls are disabled by default. The API requires **all three**: `APP_ENV=development`, `ENABLE_CHAOS=true`, and a non-empty `CHAOS_TOKEN`. Production rejects controls even if a token and enable flag are set. The Next.js proxy additionally requires development mode, retains the token server-side, accepts only fixed action paths and loopback hostnames, and rejects POST requests without a matching Origin. Compose binds the API/frontend ports to `127.0.0.1` rather than all network interfaces. Do not expose this development lab publicly.

In PowerShell, from the repository root:

```powershell
$env:APP_ENV='development'
$env:ENABLE_CHAOS='true'
$env:CHAOS_TOKEN=[guid]::NewGuid().ToString('N')
docker compose up -d --build api frontend
```

Alternatively set these values in your ignored local `.env`; never commit a real token. Open the dashboard's **Development lab** panel. It supports inventory-read failure, artificial read latency (0/500/3,000 ms), Redis failure, consumer pause, fixed invalid-event injection, WebSocket interruption, and reset. Artificial latency over the default two-second read timeout triggers fallback and breaker failure accounting. Invalid events enter the real inventory topic and reach its DLQ; they cannot supply arbitrary stock changes.

Reproducible recovery demo:

1. Wait for LIVE, which also warms the Redis catalog snapshot.
2. Enable Inventory read failure. The dashboard retains cards and displays DEGRADED, Redis source, snapshot age, and eventually OPEN.
3. Clear the failure or use Reset faults. After the recovery timeout, transition history shows OPEN → HALF_OPEN → CLOSED and the dashboard returns to LIVE.
4. Try Redis unavailable: data is marked degraded and valid Kafka records are retained/replayed, not sent to DLQ. Reset allows processing to resume.
5. Pause the consumer briefly: stock delivery pauses and snapshot age increases. Reset resumes delivery. Interrupt WebSockets to demonstrate retained cards and reconnect snapshot refresh.

Disable the lab again by setting `APP_ENV=production`, `ENABLE_CHAOS=false`, clearing `CHAOS_TOKEN`, and recreating API/frontend containers. No token is sent to the browser.

### Verification

With development controls enabled and the full stack running:

```bash
docker compose exec api sh -c "pip install -e '.[test]' && pytest -q"
docker compose exec api python tests/integration_resilience.py
```

From `apps/frontend` in PowerShell:

```powershell
npm test
npm run lint
$env:LIVE_STACK='1'
$env:CHAOS_TEST='1'
npm run test:e2e
```

Backend tests cover circuit transitions, rejected calls, failed probes, configurable half-open trials, validated fallback, missing/corrupt caches, stale age, latency timeouts, replay after dependency failures, and production/token guards. The live probe verifies actual Redis fallback, circuit recovery, consumer pause, valid-record retention during a simulated Redis outage, and invalid-event DLQ delivery. Browser checks cover retained newer cards, total fallback loss, automatic recovery without reconnect, hidden controls when unavailable, same-origin write rejection, and actual control-driven breaker recovery/WebSocket interruption.

Current local verification passed: 18 backend tests, six frontend unit tests, five browser tests with live-stack/chaos checks enabled, TypeScript checking, and a production frontend build. The live recovery probe passed against Redis/Kafka, and a separate production-mode frontend check returned 404 despite a configured server token. The hostile Host and missing Origin checks returned 403. These checks demonstrate the local implementation, not a multi-node availability guarantee.

### Remaining limits and next work

The circuit and fault state are process-local and reset on API restart. Probes are serialized; per-browser catalog polling is intended for this small local dashboard, not large fleets. The full-catalog fallback is refreshed by healthy `/inventory` reads, not every individual Kafka event, and needs at least one successful read before it is available. Redis has no persistent Compose volume; a Redis restart may remove snapshots. Cached products are retained indefinitely but always marked stale with their actual timestamp; they must not authorize checkout decisions.

Inventory failure/Redis failure are application-boundary simulations, not container destruction. Inventory-read failure does not stop PostgreSQL event writes. A paused or Redis-blocked consumer can process one already-in-flight update. Keep consumer pauses short (below Kafka's five-minute max poll interval); prolonged group rebalances may require restarting the API consumer, with idempotent replay protecting stock. Kafka broker interruption is not implemented as a chaos action. Startup/terminal Kafka consumer failures still need an API restart; dependency failures during record processing retry automatically. These boundaries are visible rather than disguised as production failover.

The retained store and recovery metadata now carry independent inventory and price revisions. Optional ML remains deferred.

## Demand-based pricing

Every accepted inventory transition records a durable pricing decision in the same PostgreSQL transaction as stock and its event receipt. A changed recommendation is a transactional outbox entry: the consumer publishes its deterministic decision ID to `pricing-events`, keyed by product ID, before acknowledging the source inventory offset. The same consumer processes both topics. The pricing handler looks up the recorded decision, rechecks guards under the product row lock, applies it once, and uses the existing Redis/WebSocket product-update path. Kafka/Redis/database outages retain offsets; duplicate publication or interrupted broadcasts replay safely.

Prices have a separate `price_version`; they never advance inventory `version` or change stock. Store ingestion, reconnect snapshots, and frame buffering reject regressions in either revision while accepting price-only advances. Cards show direction, demand, RULES source, and an expandable reason/revision with a link to recent audit inputs/outcomes. `GET /pricing/decisions?product_id=<uuid>&limit=30` exposes the durable history; limits are clamped to 200.

### Rules and limits

- Each product tracks an event-time window (default 30 seconds), purchased units, and event count. Sales velocity is purchased units divided by configured window length, not an extrapolated instantaneous rate. Reservation pressure is reserved/stock; available ratio is unreserved stock/reference stock. Reference stock is initialized on seed/migration.
- HIGH demand: reservation pressure ≥50% or sales velocity ≥0.05 units/s. LOW: available ratio ≥80%, velocity ≤0.01 units/s, and at least half a window observed. Otherwise NORMAL. HIGH targets 110% of base price, LOW 95%, NORMAL 100%; targets do not compound.
- Every move is bounded by configured absolute min/max, base-relative maximum increase/discount (20% each), and per-change step (5%). Cent rounding cannot cross bounds. Cooldown is 30 seconds; reversals wait 120 seconds. Sold-out, disabled, pending, incompatible bounds, warming windows, and non-live events hold price. Old events (>two windows) and future events (>five seconds) cannot trigger pricing.
- One pending decision per product prevents floods. Delivery must occur within two windows and still match the recorded price revision/target; otherwise it is audited as REJECTED. Applied decisions retain previous/recommended/applied prices, adjustment percentage, input signals, source, timestamps, and outcome. HOLD decisions are also persisted. Duplicate/stale inventory events do not create new decisions.

Settings are in `.env.example` (`PRICING_*`, `KAFKA_PRICING_TOPIC`). `PRICING_ENABLED=false` keeps stock processing and HOLD audits working without changing prices. Direct unaudited PRICE_UPDATED inventory events are rejected; price records must reference a stored decision on the pricing topic.

### Verify pricing

```bash
docker compose exec api sh -c "pip install -e '.[test]' && pytest -q"
docker compose exec api python tests/integration_pricing.py
```

The isolated live probe verifies a 5% increase, audit persistence, Kafka-to-WebSocket delivery, unchanged inventory revision/stock, duplicate price application, cooldown HOLD persistence, and rejection of an untrusted pricing source. Unit checks cover deterministic bounds, cent rounding, cooldown, reversal protection, pending/sold-out/disabled holds, captured inputs, and retention of the source record when pricing publication fails. Frontend unit/browser checks cover price-only revisions and stale catalog rollback prevention; prior inventory/resilience tests remain in place.

Pricing verification on the local stack passed: 22 backend tests, seven frontend unit tests, six browser tests with live-stack/development controls enabled, TypeScript checking, and a production frontend build. The pricing, inventory, and resilience live probes passed. These are functional checks, not controlled throughput or financial-impact measurements.

This is a transparent heuristic, not trained ML or a profit optimizer. No model accuracy, revenue uplift, or pricing throughput is claimed. Windows reset on the next inventory event and displayed demand does not decay without events. Audit rows are retained indefinitely; production volume needs retention/archival. Single-worker/fanout and Kafka polling limits above still apply. The next step is Phase 6's final documentation, controlled benchmarking, and demo polish, not an assumed production rollout.
