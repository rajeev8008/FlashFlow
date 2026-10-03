Current throughput work and corrected long-run results are in [throughput-report.md](throughput-report.md). Stage definitions are in [pipeline-timing.md](pipeline-timing.md); [raw-run annotations](benchmarks/annotations.md) identify superseded/incomplete distributions. The historical October 2 evidence below is unchanged.

# Measurement methodology and local evidence

These are short, single-machine portfolio checks, not capacity guarantees, financial results, or a production availability claim. Raw JSON retains timestamps, requested/achieved traffic, sampling counts, CPU/memory scope, and null measurements. Do not replace null with zero or call a configured producer rate measured throughput.

## Reproduce

Start the production frontend using the README's Docker Compose path. From `apps/frontend`:

```powershell
npm ci
npx playwright install chromium
$env:BENCH_SECONDS='10'
$env:BENCH_REPEATS='3'
$env:BENCH_RATES='50,500,2000'
npm run benchmark
```

The harness creates a fresh Chromium context per mode/rate/repeat, renders 500 cards, selects the mode, warms up for 2.1 seconds, and sends identical round-robin updates to 10 hot products. A separate 20ms timer produces traffic independently of renderer sampling. Read waits are bounded at 1.5 seconds; an overloaded renderer returns missing samples rather than silently throttling the producer. Actual send rate and sampled receipt rate are both retained. The backend is bypassed using routed catalog/WebSocket fixtures; these numbers isolate rendering and must not be called Kafka throughput.

The sampled dashboard has normal one-second measurement overhead and the documented parent pulse (ATOMIC intentionally includes roughly 500 parent-driven card commits/s). NAIVE subscribes to the whole map. React can batch immediate updates; do not assume an event equals a render. The harness reads the visible gauges, which are one-second rates and a rolling 512-commit latency window, not a lossless per-event trace. Missing samples make that run unsuitable for percentile comparisons. P50/P95 are each run's last available rolling percentile; table aggregation averages trial values and is **not** a pooled percentile.

Node timestamps and Chromium clocks run on the same machine. Event-to-UI includes the synthetic Playwright transport plus rendering, not a remote backend. Browser CPU is CDP renderer TaskDuration/wall interval; memory is JS heap only. CPU observation can extend beyond the send interval while sampling/draining finishes. The 2,000-events/s target may be missed by the generator/transport; compare achieved rates before comparing modes. Samples exclude hydration but include any in-window mode overhead. No randomized trial ordering or long soak was performed. Re-run on your hardware and report dispersion, not just a favorable trial.

### Real HTTP/WebSocket load

```powershell
docker compose exec -T api python tests/load_probe.py --clients 1 10 100 --seconds 10
```

This standard-library asyncio/websockets probe is the k6 equivalent used here: sequential 1/10/100-client cases, real connections to the API, and one `/products?limit=10` request per client every two seconds. Each case samples committed Kafka lag every second. Product messages retain source and emission timestamps. Fanout latency is server enqueue/emission to receiver, including queue/network time; event-to-client additionally includes earlier pipeline work. These are not rendered UI measurements. The probe is colocated in the API container, so results include contention with the load generator and short local network distance. API CPU snapshots and **peak**, not current, RSS are in the raw backend samples. Peak RSS cannot be attributed solely to a particular case.

Connection errors include failed handshakes or unexpected closes; slow-queue disconnects are separately counted. HTTP successes count valid JSON HTTP responses, not business correctness assertions. The functional integration suite verifies stock/pricing correctness. Client count increases while the existing simulator stays at its normal configured 10 inventory events/s; this does not prove 100-client delivery at a sustained 500/2,000 events/s.

## Browser results, October 2, 2026

Source: [browser-1790931874348.json](benchmarks/browser-1790931874348.json), harness version 2. Two six-second trials per combination, 500 cards, 10 hot products, production Next.js build, headless Chromium 153.0.8010.12, 12th Gen Intel(R) Core(TM) i5-12500H (16 logical CPUs), win32 10.0.26300. Docker services were running; no CPU isolation was used. Table entries average available trial observations, not independent whole-run event totals or pooled percentiles.

| Target events/s | Mode | Actual send/s | Sampled receive/s | FPS | Card commits/s | UI flushes/s | Mean trial P95 ms | Renderer CPU % |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 50 | NAIVE | 49.8 | 45.6 | 23.0 | 22825.0 | 45.6 | 207.0 | 96.2 |
| 50 | ATOMIC | 49.8 | 47.2 | 59.9 | 547.3 | 47.2 | 8.5 | 40.3 |
| 50 | MEMOIZED | 49.8 | 47.4 | 59.9 | 47.4 | 47.4 | 7.0 | 35.7 |
| 50 | BATCHED | 49.8 | 47.4 | 60.0 | 47.4 | 31.2 | 20.0 | 35.4 |
| 500 | NAIVE | 499.3 | N/A | N/A | N/A | N/A | N/A | 99.5 |
| 500 | ATOMIC | 499.2 | 472.6 | 43.2 | 969.9 | 472.6 | 43.0 | 85.5 |
| 500 | MEMOIZED | 499.7 | 474.1 | 53.0 | 474.1 | 474.1 | 32.0 | 78.8 |
| 500 | BATCHED | 499.3 | 474.8 | 60.0 | 331.3 | 33.9 | 25.5 | 53.5 |
| 2000 | NAIVE | 1995.6 | N/A | N/A | N/A | N/A | N/A | 99.2 |
| 2000 | ATOMIC | 1995.9 | N/A | N/A | N/A | N/A | N/A | 97.8 |
| 2000 | MEMOIZED | 1984.2 | N/A | N/A | N/A | N/A | N/A | 97.3 |
| 2000 | BATCHED | 1958.0 | 1674.7 | 56.4 | 233.0 | 23.4 | 285.5 | 54.8 |

At the 500/s target, BATCHED averaged about 60 FPS with roughly 34 UI flushes/s and 331 card commits/s. At the 2,000/s target it remained observable, but achieved send rate, sampled receipt rate, and latency varied substantially across trials; this is not proof of sustained 2,000-event/s end-to-end processing. NAIVE at 500/s and all immediate modes at 2,000/s had no dashboard samples within the bounded read waits. They are marked N/A, not assigned zero FPS or fabricated latency. Renderer CPU remained observable through CDP. Missing samples and two short trials prevent definitive high-rate percentile comparisons.

The earlier [browser-1790931434070.json](benchmarks/browser-1790931434070.json) is a **superseded pilot**: renderer reads shared the producer loop, throttling actual sends in overloaded modes. It is preserved transparently but must not be used for matched-rate comparison. Version 2 separates that loop and bounds unavailable reads.

## Initial load results, October 2, 2026

Source: [load-2026-10-02.json](benchmarks/load-2026-10-02.json). Eight seconds per case; same-machine development stack, normal simulator traffic, and a separate browser benchmark running on the host. The latter is a confounder, so these results establish bounded functional scale, not isolated maximum capacity.

| Requested clients | Connected | Connection errors | HTTP successes / errors | Fanout P95 ms | Observed lag range | Final API CPU % | API peak RSS MiB |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 1 | 0 | 4 / 0 | 1.06 | 2–7 | 16.29 | 83.20 |
| 10 | 10 | 0 | 40 / 0 | 2.22 | 0–10 | 12.48 | 84.86 |
| 100 | 100 | 0 | 400 / 0 | 11.45 | 0–10 | 38.44 | 109.40 |

No slow-client disconnect was observed in these cases. CPU is the final two-second API-process sample, not a case-wide average; background polling and the colocated probe are included. Memory conversions use bytes/1,048,576. Handshake success does not establish long-duration stability. Larger client counts and multi-host/network tests remain unmeasured.

## CI and validation

### Real flash-sale and end-to-end evidence

Source: [surge-2026-10-02.json](benchmarks/surge-2026-10-02.json). Stopped the normal simulator, ran a six-second producer burst targeting 500 inventory events/s, observed the real browser, waited for drain, and restored normal traffic. The consumer received all **3,000 inventory records** plus **81 pricing records**; DLQ count did not increase, and both topic lags returned to zero. Pricing records are recommendations/deliveries, not necessarily applied price changes; guards can reject superseded ones.

During drain, one browser sample observed 72.4 product updates/s, 52.3 UI flushes/s, 71.4 card commits/s, 60.3 FPS, and 5.7 ms receipt-to-commit latency. Event-to-UI P95 was **13,616 ms** with sampled committed lag **1,395** and consumed Kafka rate **98.2 records/s**. This is a real backlog/capacity boundary: the browser remained responsive but data was delayed. Do not turn a 500/s producer target or a quick burst into a sustained 500/s processing claim. The reported P95 is one rolling browser sample, not a percentile over the whole surge.

The final local validation passed **24 backend tests, eight frontend unit tests, and six browser tests (38 total)**, plus the inventory, pricing, and resilience live probes, backend fatal-error lint, TypeScript checking, production Docker builds, healthy API/frontend startup, and a separate real-browser surge check. The resilience scenario verified retained cards, Redis fallback, OPEN/HALF_OPEN/CLOSED recovery, LIVE restoration, paused freshness, and valid-record replay. The pricing probe verified a guarded HIGH-demand reaction and audit persistence. Mobile/desktop screenshots were inspected; the mobile test confirmed no horizontal overflow. These are local results; GitHub CI is still unverified remotely.

`.github/workflows/ci.yml` runs backend pytest, targeted fatal-error Ruff lint, frontend unit tests and TypeScript checking, a production Docker build, live Kafka/inventory/pricing/resilience probes, and Playwright (including optional real-stack cases). Expensive benchmarks/load probes are deliberately manual, not triggered on every push. Python dependencies are pinned at the package level; frontend installs from its lockfile. Docker image tags and transitive Python dependencies are not fully digest-locked.

The workflow is configuration until it has run remotely after publication. Local checks do not imply GitHub CI success. Fatal-error lint is intentionally narrower than a full style policy; frontend `lint` is TypeScript checking, not ESLint. Raw results should be preserved with a new filename for each run.

## Limits before making performance claims

Use longer repeated/randomized trials, constant production builds, fixed hardware/background load, synchronized clocks, matched achieved traffic, controlled product distribution, isolated generators, and whole-pipeline traces. Measure broker ingress, committed processing, socket receipt, rendered latency, queue drops, and resource use separately. High throughput claims require stable bounded lag over a sustained interval, not a rapid producer burst followed by a long drain. Failure tests must distinguish application-level simulations from real container/network destruction. See [architecture.md](architecture.md) for production boundaries and scaling strategy.
