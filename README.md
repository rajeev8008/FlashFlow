# FlashFlow

FlashFlow streams simulated retail inventory through Kafka, persists validated changes in PostgreSQL, caches the latest products in Redis, and broadcasts updates to a Next.js client over WebSockets. Phases 1 and 2 are implemented.

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

The basic client updates React state per message and reconnects every two seconds. Phase 3 adds atomic subscriptions, exponential backoff, and rendering batches. Dynamic pricing, fault injection, authentication, production TLS, and measured benchmarks remain later work. This Compose stack is for local development.
