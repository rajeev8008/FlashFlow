# FlashFlow

Phase 1 foundation for a real-time retail surge platform: FastAPI, PostgreSQL, Redis, Kafka, a deterministic catalog, an asynchronous traffic simulator, and a Next.js TypeScript shell.

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

## Phase 1 architecture

```text
PostgreSQL <- migrations + deterministic seed <- FastAPI
                                               |
seeded catalog -> async simulator -> Kafka inventory-events

Redis <- connectivity health check
Next.js <- foundation page (real-time UI intentionally deferred)
```

PostgreSQL is the durable catalog. Redis is connected but intentionally has no cache behavior yet. The simulator maintains a private evolving stock view so it only emits valid inventory transitions; consuming and persisting those events begins in Phase 2.

## Current limits

Phase 1 deliberately excludes Kafka consumption, WebSockets, dynamic pricing, fault injection, and benchmark claims.
