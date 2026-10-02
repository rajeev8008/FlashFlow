# FlashFlow — Real-Time Event-Driven Retail Surge Platform

## 1. Project Overview

FlashFlow is a production-style real-time retail platform designed to remain responsive during extreme flash-sale traffic.

The system simulates thousands of inventory and pricing events, processes them through an event-driven Kafka pipeline, broadcasts state changes to browsers over WebSockets, and renders updates efficiently using client-side micro-batching and atomic state subscriptions.

The project focuses on five engineering goals:

1. High-throughput event ingestion
2. Low-latency real-time delivery
3. Efficient frontend rendering
4. Fault tolerance and graceful degradation
5. Demand-aware dynamic pricing

The system is intentionally designed as an engineering benchmark, not just a visual dashboard.

---

## 2. Core Problem

During high-traffic retail events, backend inventory can change far faster than a browser should render.

A naive architecture such as:

Backend -> WebSocket -> React setState -> Full UI re-render

can cause excessive rendering, CPU usage, dropped frames, and poor user experience.

FlashFlow instead uses:

Traffic Simulator
-> Kafka
-> Event Consumers
-> Redis/PostgreSQL
-> WebSocket Gateway
-> Browser Event Buffer
-> requestAnimationFrame Micro-Batching
-> Zustand Atomic Store
-> Product-Level Selectors
-> Memoized Product Cards

This decouples event ingestion rate from UI rendering rate.

---

## 3. Primary Features

### Real-Time Inventory Pipeline
- Simulated retail inventory events
- Kafka-backed event streaming
- Product-based partitioning
- Structured versioned event schema
- Consumer offset management
- Idempotent processing

### Real-Time WebSocket Delivery
- Long-lived WebSocket connections
- Connection manager
- Heartbeats
- Reconnect support
- Exponential backoff
- Client connection states

### High-Performance Frontend
- Next.js
- TypeScript
- Zustand
- Product-level selectors
- React.memo
- Key-value product state
- requestAnimationFrame-based micro-batching
- Coalescing multiple updates for the same product

### Fault Tolerance
- Redis fallback cache
- Circuit breaker
- Retry strategy
- Dead-letter topic
- Degraded frontend mode
- Fault injection controls

### Dynamic Pricing
- Rule-based demand pricing first
- Optional ML-based demand prediction later
- Constrained price adjustments
- Pricing audit history

### Observability
- Event throughput
- Kafka consumer lag
- Active WebSocket clients
- Event-to-UI latency
- UI flush rate
- Batch size
- Cache hit rate
- Circuit breaker state
- Invalid/dead-letter events

### Benchmarking
Compare:
1. Naive React state updates
2. Atomic Zustand updates
3. Zustand + memoized product cards
4. Zustand + memoization + micro-batching

Measure:
- FPS
- React renders/sec
- event-to-UI latency
- backend throughput
- WebSocket fanout latency
- CPU usage
- memory usage

---

## 4. Technology Stack

### Frontend
- Next.js
- TypeScript
- Zustand
- React
- WebSocket API

### Backend
- Python
- FastAPI
- asyncio
- Pydantic

### Messaging
- Apache Kafka

### Data
- PostgreSQL
- Redis

### Infrastructure
- Docker
- Docker Compose

### Testing / Benchmarking
- pytest
- Playwright
- k6
- React Profiler
- browser Performance APIs

### Optional Deployment
- GCP / Cloud Run
- managed PostgreSQL
- managed Redis
- managed Kafka provider

---

## 5. High-Level Architecture

```text
                          +----------------------+
                          |  Flash Sale Simulator |
                          +----------+-----------+
                                     |
                                     v
                          +----------------------+
                          |        Kafka          |
                          | inventory-events      |
                          | pricing-events        |
                          +----------+-----------+
                                     |
                 +-------------------+-------------------+
                 |                                       |
                 v                                       v
        +-------------------+                  +-------------------+
        | Inventory Service |                  | Pricing Service   |
        | PostgreSQL        |                  | Rules / ML        |
        | Redis Cache       |                  +---------+---------+
        +---------+---------+                            |
                  |                                      |
                  +-------------------+------------------+
                                      |
                                      v
                          +----------------------+
                          | WebSocket Gateway    |
                          +----------+-----------+
                                     |
                                     v
                          +----------------------+
                          | Next.js Client       |
                          | Event Buffer         |
                          | rAF Micro-Batching   |
                          | Zustand Store        |
                          | Memoized Cards       |
                          +----------------------+
```

---

## 6. Event Model

Every event must have a consistent schema.

Recommended fields:

```text
event_id
event_type
product_id
timestamp
schema_version
source
payload
```

Example event types:

```text
STOCK_RESERVED
STOCK_RELEASED
PURCHASE_COMPLETED
INVENTORY_RESTOCKED
PRICE_UPDATED
PRODUCT_SOLD_OUT
```

Kafka inventory events must be partitioned using:

```text
key = product_id
```

This preserves ordering for individual products.

---

## 7. Product Model

Suggested fields:

```text
product_id
name
category
base_price
current_price
stock
reserved_stock
sales_velocity
last_updated
status
version
```

The frontend should normalize products into:

```text
productsById: {
  product_id: Product
}
```

Avoid using a deep globally replaced product array for high-frequency updates.

---

# 8. Implementation Phases

## Phase 1 — Foundation, Domain Model, Infrastructure, and Simulator

### Goals
Build a complete local development environment and event-producing backend foundation.

### Deliverables
- Monorepo structure
- Docker Compose setup
- Kafka
- PostgreSQL
- Redis
- FastAPI backend skeleton
- Next.js TypeScript frontend skeleton
- Product schema
- Event schema
- Database migrations
- Seed dataset
- Traffic simulator
- Kafka producer
- Configurable traffic modes

### Traffic Modes

```text
NORMAL
BUSY
FLASH_SALE
EXTREME
```

Simulator settings should support configurable:
- events/sec
- product count
- event mix
- burst duration
- random seed

### Exit Criteria
- Full stack starts with one command.
- Kafka receives valid inventory events.
- Events can be inspected.
- PostgreSQL contains seeded products.
- Redis connectivity is verified.
- Simulator can generate sustained event traffic.

---

## Phase 2 — Event Processing, Persistence, WebSockets, and Reliability

### Goals
Build the real-time backend event path.

### Deliverables
- Kafka consumer
- Inventory service
- Idempotent event handling
- Product version checking
- PostgreSQL persistence
- Redis hot cache
- WebSocket connection manager
- Event broadcasting
- Heartbeats
- Reconnect support
- Retry handling
- Dead-letter topic
- Structured logging

### Expected Flow

```text
Simulator
-> Kafka
-> Inventory Consumer
-> PostgreSQL / Redis
-> WebSocket Gateway
-> Browser
```

### Reliability Requirements
- duplicate event protection
- safe consumer restart
- manual or controlled offset commits
- malformed event validation
- failed event retries
- DLQ after retry exhaustion
- graceful shutdown

### Exit Criteria
- Multiple browser clients receive real-time product updates.
- Restarting a consumer does not corrupt stock.
- Duplicate events do not double-apply.
- Bad events reach the dead-letter topic.

---

## Phase 3 — High-Performance Frontend and Render Optimization

### Goals
Build the defining frontend optimization layer.

### Deliverables
- Product grid
- Zustand store
- `productsById` state model
- Product-specific selectors
- `React.memo`
- WebSocket lifecycle manager
- exponential reconnect backoff
- client event buffer
- requestAnimationFrame batching
- per-product update coalescing
- UI connection states
- render counters and performance instrumentation

### Required Optimization

Instead of applying every WebSocket event immediately:

```text
socket event
-> buffer
-> merge updates by product_id
-> flush once per animation frame
-> update Zustand
-> rerender only affected products
```

### Exit Criteria
- High incoming event rates do not cause global rerenders.
- Only changed product cards rerender.
- UI remains responsive during simulator surge mode.
- Performance metrics are visible.

---

## Phase 4 — Resilience, Fault Injection, and Graceful Degradation

### Goals
Make failures visible, recoverable, and measurable.

### Deliverables
- circuit breaker
- Redis snapshot fallback
- stale-data metadata
- degraded frontend mode
- fault injection API
- engineering control panel

### Fault Controls
Support simulated:
- inventory service failure
- Redis failure
- consumer pause
- artificial latency
- invalid event injection
- Kafka disconnect if practical

### Circuit Breaker States

```text
CLOSED
OPEN
HALF_OPEN
```

### UI Behavior
If live data becomes unavailable:
- keep displaying the latest known snapshot
- show stale-data age
- show degraded mode
- restore live mode automatically after recovery

### Exit Criteria
- Backend failures do not blank or freeze the dashboard.
- Circuit breaker behavior can be demonstrated interactively.
- Recovery occurs without restarting the entire stack.

---

## Phase 5 — Dynamic Pricing and Intelligent Retail Layer

### Goals
Add useful AI/ML functionality without turning the project into an AI demo.

### Stage 1: Rule-Based Pricing
Use:
- sales velocity
- remaining stock
- stock percentage
- recent conversion proxy
- flash-sale time remaining

Generate:
- recommended price
- reason
- confidence/strength
- timestamp

Constraints:
- minimum price
- maximum price
- maximum percentage adjustment
- cooldown interval

### Stage 2: ML Extension
Generate synthetic historical retail data.

Train a lightweight model such as:
- XGBoost
- LightGBM
- gradient boosting

Predict:
- near-term demand
or
- expected units sold over a future window

Use predictions as one input to pricing logic.

### Auditability
Every price change should store:
- previous price
- new price
- demand features
- rule/model decision
- reason
- timestamp

### Exit Criteria
- Pricing changes stream through Kafka.
- UI updates prices in real time.
- Pricing decisions are bounded and auditable.
- Rule-based pricing works even if ML is disabled.

---

## Phase 6 — Observability, Load Testing, Benchmarking, and Production Polish

### Goals
Prove the system works rather than merely claiming it works.

### Engineering Dashboard
Display:
- Kafka events/sec
- consumer lag
- WebSocket clients
- WebSocket messages/sec
- event-to-UI latency
- P50/P95 latency
- UI flushes/sec
- events merged per batch
- Redis cache hit rate
- circuit breaker state
- DLQ count
- pricing events/sec

### Frontend Benchmark Modes
Implement selectable modes:

```text
NAIVE
ATOMIC
MEMOIZED
BATCHED
```

Compare:
- FPS
- renders/sec
- CPU
- latency

### Load Testing
Test:
- 1 client
- 10 clients
- 100 clients
- larger counts if hardware permits

Test traffic rates such as:
- 50 events/sec
- 500 events/sec
- 2,000 events/sec
- higher where stable

### Documentation
Produce:
- architecture diagram
- sequence diagrams
- benchmark methodology
- benchmark results
- failure scenarios
- trade-offs
- scaling plan
- local setup guide
- demo instructions

### Exit Criteria
- Reproducible benchmark results exist.
- Naive vs optimized rendering is directly comparable.
- Load test results are recorded.
- Entire stack runs through documented commands.
- README clearly explains architecture and results.

---

# 9. Non-Functional Requirements

## Correctness
- stock must never become negative
- duplicate events must not double-apply
- stale events must not overwrite newer state
- price changes must respect configured bounds

## Performance
- backend event processing must not block WebSocket handling
- frontend must avoid global rerenders
- event batching must happen independently from network ingestion

## Reliability
- graceful shutdown
- reconnectable consumers
- retry handling
- dead-letter routing
- circuit breaker recovery

## Maintainability
- typed event contracts
- modular services
- environment-based configuration
- clear package boundaries
- reusable domain models

## Security
For portfolio scope:
- validate external input
- protect internal control endpoints
- avoid arbitrary fault injection in production mode
- never trust client-provided inventory state

---

# 10. Suggested Repository Structure

```text
flashflow/
|
|-- apps/
|   |-- frontend/
|   |-- api/
|   |-- websocket-gateway/
|
|-- services/
|   |-- inventory/
|   |-- pricing/
|   |-- simulator/
|
|-- shared/
|   |-- schemas/
|   |-- config/
|
|-- infra/
|   |-- docker/
|   |-- kafka/
|
|-- tests/
|   |-- unit/
|   |-- integration/
|   |-- e2e/
|   |-- load/
|
|-- docs/
|   |-- architecture/
|   |-- benchmarks/
|
|-- docker-compose.yml
|-- PROJECT.md
|-- README.md
```

The exact service split may be simplified during early development. Do not create microservices purely for appearance.

---

# 11. Testing Strategy

## Unit Tests
Test:
- event validation
- stock update rules
- duplicate handling
- version ordering
- pricing rules
- circuit breaker transitions
- batching/coalescing logic

## Integration Tests
Test:
- Kafka producer -> consumer
- consumer -> PostgreSQL
- consumer -> Redis
- Kafka -> WebSocket
- retry -> DLQ

## End-to-End Tests
Test:
- start stack
- run simulator
- browser receives updates
- correct card changes
- connection recovery
- degraded mode
- recovery to live mode

## Load Tests
Measure:
- throughput
- latency
- connection scaling
- CPU
- memory
- Kafka lag

---

# 12. Important Engineering Decisions to Preserve

1. Kafka is the event backbone.
2. PostgreSQL is the durable source of truth.
3. Redis is a hot-state/cache/fallback layer, not the only durable store.
4. Product ID is the Kafka partition key.
5. Events must have IDs and schema versions.
6. Consumers must be idempotent.
7. Browser network ingestion and rendering must be decoupled.
8. Frontend updates must be normalized by product ID.
9. The final system must include measurable benchmarks.
10. ML pricing must be bounded, auditable, and optional.

---

# 13. Final Resume Goal

Once benchmarked, the project should support bullets in this style:

- Built a Kafka-based retail event pipeline processing X+ inventory events/sec and streaming real-time updates through WebSockets with Y ms P95 event-to-UI latency.
- Designed requestAnimationFrame micro-batching and atomic Zustand subscriptions, reducing React rerenders by X% while sustaining approximately Y FPS under surge traffic.
- Implemented Redis-backed degraded mode, circuit-breaker recovery, idempotent Kafka consumers, retries, and dead-letter handling for resilient inventory processing.
- Developed a demand-aware pricing engine using live stock velocity and predictive demand signals with bounded, auditable price adjustments.

Never invent benchmark numbers. Replace placeholders only after measurement.
