Current interview answers and the five-minute walkthrough are in [interview.md](interview.md); current measured resume bullets are in [throughput-report.md](throughput-report.md). The October 2 bullets below are preserved historical evidence.

# Local demo and interview guide

## Exact local demo

From the repository root in PowerShell, with Docker Desktop running:

```powershell
Copy-Item .env.example .env  # First setup only; do not overwrite an existing configuration.
$env:APP_ENV='development'
$env:ENABLE_CHAOS='true'
$env:CHAOS_TOKEN=[guid]::NewGuid().ToString('N')
docker compose up -d --build
```

Open http://localhost:3000 after the API is healthy. Confirm the 500-card seeded catalog, LIVE status, changing stock, independent price revisions, measured rates/lag, and the RULES badge. Do not share the token or enable these controls on a public service.

Run an isolated bounded surge (never two simulators together):

```powershell
docker compose stop simulator
docker compose run --rm --no-deps -e SIMULATOR_EVENTS_PER_SECOND=500 -e SIMULATOR_BURST_DURATION_SECONDS=6 simulator
docker compose start simulator
```

The configured rate is a target, not measured throughput. Watch committed lag and actual consumed records/s during the burst and subsequent drain. Keep BATCHED selected, then compare NAIVE/ATOMIC/MEMOIZED with the synthetic harness under identical inputs.

In Development lab, enable Inventory read failure: cards stay present, source becomes Redis, status becomes DEGRADED, and the breaker opens. Reset faults: OPEN → HALF_OPEN → CLOSED and LIVE return without page reload. Interrupt WebSockets to demonstrate reconnect repair. Run the pricing probe to generate a deterministic HIGH-demand reaction on an isolated Test product:

```powershell
docker compose exec -T api python tests/integration_pricing.py
docker compose exec -T api python tests/integration_resilience.py
docker compose exec -T api python tests/integration_phase2.py
docker compose exec -T api sh -c "pip install -e '.[test]' && pytest -q"
```

Run frontend tests/benchmarks from `apps/frontend`:

```powershell
npm ci
npx playwright install chromium
npm test
npm run lint
$env:LIVE_STACK='1'
$env:CHAOS_TEST='1'
npm run test:e2e
npm run benchmark
```

For load testing, run `docker compose exec -T api python tests/load_probe.py --clients 1 10 50 100 --seconds 30`. Save the JSON output to a new file under `docs/benchmarks`; never overwrite a prior run. Measured result tables and definitions are in [performance.md](performance.md).

Disable the lab when done by setting APP_ENV=production, ENABLE_CHAOS=false, clearing CHAOS_TOKEN, and recreating API/frontend. `docker compose down` stops services but preserves the PostgreSQL volume; do not use `down -v` unless deleting all persisted data is intentional.

## Four evidence-based resume bullets

- Built a product-keyed Kafka inventory/pricing pipeline with a deterministic 500-product catalog, PostgreSQL row-locked idempotent effects, durable decision audits, Redis cache repair, and real-time WebSocket updates.
- Compared four React/Zustand rendering modes across 24 short synthetic trials; BATCHED averaged 60 FPS and 34 UI flushes/s at a 500-events/s target (499 actual sends/s), with 25.5 ms mean trial event-to-UI P95 on the local test machine.
- Tested 100 colocated WebSocket clients for eight seconds alongside 400 HTTP requests with zero observed connection/HTTP errors and 11.45 ms fanout P95; preserved raw JSON and stated the normal-traffic/local-network limitations.
- Implemented validated Redis fallback, circuit recovery, manual-offset retry/DLQ handling, and bounded rule-based pricing with separate stock/price revisions; verified correctness and recovery using live Kafka/PostgreSQL/Redis integration probes.

## Five-minute explanation

**0:00–0:45 — Problem.** Inventory can arrive faster than a UI should render. FlashFlow separates durable event processing from presentation, and exposes enough instrumentation to test that separation instead of guessing from how smooth the page looks.

**0:45–1:45 — Backend flow.** One seeded simulator sends versioned, product-keyed Kafka events. The consumer uses manual commits and locks the product row. It validates transitions and persists an event receipt with stock changes. Redis/cache and WebSocket delivery follow. If those fail after database commit, replay sees the receipt and repairs delivery without applying stock twice. Invalid records go to an acknowledged DLQ; infrastructure failures retain offsets.

**1:45–2:45 — Frontend.** WebSocket input is validated and normalized into productsById. Selectors retain unrelated object identities, memoized cards skip parent-driven work, and an animation-frame buffer keeps only the newest snapshot per product. Reconnect fetches a durable catalog. Independent inventory and price revisions prevent late stock messages from rolling prices back. Show four modes, explaining the intentional one-second parent pulse and the fact that React can batch immediate updates too.

**2:45–3:30 — Recovery.** Catalog reads use a small circuit breaker. Failures show a validated Redis snapshot, its age, and DEGRADED status; loss of fallback keeps the browser's last data. A successful half-open probe restores LIVE. This is visible local recovery, not a claim of multi-node availability. Controls require explicit development environment, a server-only token, and same-origin loopback requests.

**3:30–4:15 — Pricing.** Demand windows and reservation pressure produce a bounded target relative to base price. Every accepted transition records HOLD or PENDING decisions. A durable outbox publishes pricing records, and the consumer applies them with cooldown/reversal/revision checks. Price revision does not change stock revision. ML is deliberately not claimed: the shipped model is explainable rules.

**4:15–5:00 — Evidence and limits.** Show raw benchmark/load JSON and the result table, requested versus achieved traffic, actual received samples, latency definitions, CPU/memory boundaries, and error counts. Explain why a synthetic browser benchmark is not Kafka throughput. Scaling requires separate gateways plus brokered fanout, managed data services, group-wide telemetry, authentication, and measured capacity planning—not just increasing Uvicorn workers.

## Ten likely questions

1. **Why Kafka rather than directly pushing updates?** It separates production from processing, provides ordered per-product partitions and replayable offsets, and lets a paused consumer recover. It does not make database/cache/browser writes one atomic transaction.
2. **Is delivery exactly once?** Kafka records can repeat. The database has exactly-once effects for a given receipt/decision ID, with row locks and transactions; cache/broadcast can repeat. Browser revision guards tolerate duplicates. Socket enqueue is not client acknowledgement.
3. **When do offsets advance?** After successful database processing, pricing publication when required, Redis repair/write, and broadcast enqueue—or after acknowledged DLQ publication. Infrastructure outages keep the record for retry.
4. **Why separate price and stock revisions?** The simulator maintains an inventory sequence. Incrementing that for independent pricing would create inventory gaps. Two monotonic dimensions protect both fields against late snapshots.
5. **Why does batching help?** It bounds state flushes by animation frames and coalesces hot-product updates. Selectors and memoization reduce unrelated card commits. Costs depend on hot-product distribution, catalog size, scheduling, and actual received rate.
6. **What does P95 mean here?** Event timestamp to committed card over the most recent 512 rendered samples; coalesced events never render and are absent. Receipt latency is a different monotonic-clock metric. Cross-host measurements need synchronized clocks.
7. **How does the circuit recover?** Threshold failures open it, the timeout admits a serialized half-open read, and success closes it. Failed probes reopen it. Circuit/fault state is local to the API process.
8. **Can price rules oscillate or compound?** Targets are base-relative, capped in cents by absolute/base/step guards, with cooldown and a longer reversal interval. Only one pending decision exists per product; delivery rechecks state and expiry. Idle demand is not continuously decayed.
9. **Why not multiple API workers now?** Each consumer group member gets only some partitions, while sockets live in separate process memory. Without shared fanout, a client would miss events consumed elsewhere. Gateway scaling needs a broadcast layer and durable snapshot repair.
10. **What would you change for production?** Separate consumers/gateways, use managed services and secrets, TLS/auth/rate limits, deployment-aware socket routing, migrations as a job, schema compatibility, retention/archival, backups, tracing, group-wide lag, and controlled multi-host tests. None of that is implied by local Compose results.
