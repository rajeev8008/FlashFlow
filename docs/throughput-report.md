# FlashFlow throughput phase report — October 3, 2026

## Outcome

The measured bottleneck was serialized per-record database work, amplified by repeated pending-price queries and individual offset commits. Bounded transactional batches and grouped commits reduced socket P95 from **23.1 seconds to 1.06 seconds** in matched short local trials. Multiple co-located Kafka group members improved the 500/s result further.

The five-minute test produced and durably completed **149,994 inventory events** at **499.96 produced events/s**, with zero recorded errors, failed attempts or DLQ growth, lag peak **389** and socket P95 **757.53 ms**. A separate overlapping browser observation measured estimated clock-aligned rendered P95 **629.5 ms** and mean FPS **59.83**. These are different sample populations, not interchangeable percentiles or production capacity guarantees.

At 1,000/s and 2,000/s the machine accumulated backlog. Synthetic frontend results were less favorable than historical measurements; that difference is reported explicitly. Raw values, derived figures and source SHA-256 hashes are in [summary JSON](benchmarks/summary-2026-10-03.json). Read [raw-run annotations](benchmarks/annotations.md) and [stage/clock definitions](pipeline-timing.md) with every table.

## A. Bottleneck analysis

The historical October 2 sample observed 13,616 ms rolling rendered P95, lag 1,395 and roughly 98.2 consumed records/s. It establishes backlog, but lacks stage traces. No exact retrospective decomposition of those unrecorded historical stages is claimed.

A timing-only serial reproduction at 498.11 produced events/s captured socket P95 23,131.78 ms and lag peak 2,254. Its 3,140 inventory/pricing records spent **20.64 seconds in database transactions**, **5.04 seconds in publication checks**, **3.92 seconds in offset commits** and **1.12 seconds in Redis writes**. Database work accounted for approximately 66% of serial processing plus commit duration. Mean DB transaction time was 6.57 ms; P95 was 10.67 ms. Enqueue-to-socket-receiver P95 was only **0.861 ms**.

The hot path locked/read a product, checked a receipt, persisted stock/receipt/pricing audit and committed for every record. A separate publication query/transaction repeated for every inventory event. SQLAlchemy pooling already existed; connection creation was not demonstrated as the primary problem. Socket I/O was already asynchronous, so it was not the primary source of the serial backlog.

Broker append timestamps separated producer and consumption delays in the corrected 2,000/s case: producer acknowledgement P95 **4.83 ms**, producer-to-broker rolling P95 **2.56 ms**, broker-to-consumer rolling P95 **84,546.71 ms**. That last interval combines broker residence, fetch/network and consumer-client buffering; broker internal disk/network work is not independently traced. Together with DB timers, lag growth and the batching intervention, this identifies consumer service rate as the limiting side of the pipeline.

Sources: [serial reproduction](benchmarks/baseline-serial-2026-10-03.json), [corrected matrix](benchmarks/pipeline-corrected-2026-10-03.json), [preserved baseline source](benchmarks/baseline-source-2026-10-03.zip).

## B. Optimizations and correctness

| Problem | Change | Safety and measured effect |
| --- | --- | --- |
| Per-event SQL round trips/commits | Bound inventory batches by partition; preload sorted locked products and receipt IDs; apply transitions in order; commit stock, receipts and audits together | Bad batches roll back, then individual processing isolates invalid records. Cumulative DB time per successful record fell in the short comparison. |
| Repeated publication queries | Query pending unpublished decisions once per batch | Required publications remain acknowledged before source offsets advance. Repeated delivery uses durable decision IDs and guards. |
| Individual offset commits | Commit the last contiguous successful offset per partition batch | No commit before durable/cache/publication/enqueue success. Crashes can replay a batch; receipts prevent double effects. Commit count fell from 3,140 to 209 in the short comparison. |
| One member serializes partitions | Configurable actual Kafka group members, tested at 1/2/3 | Product keys and sequential partition order remain. Sorted DB locks and independent price revisions protect cross-topic work. All members share one process/gateway. |
| Cross-topic cache races | Atomic Redis Lua revision guard | Older stock or price revisions cannot replace newer cached state. Redis remains non-authoritative. Live integration verifies the guard. |
| Slow socket leakage | Existing bounded queues retained; separate send timeout, failure/disconnect metrics and explicit close | One slow client cannot block others. Reconnect repairs visual state; authoritative Kafka/SQL events are retained. |
| Long-run probe stopped receiving | Periodic client pongs and explicit gateway close | Initial 120-second socket distributions are marked incomplete; corrected receivers stayed connected throughout the repeated runs. |
| Ambiguous timing/clocks | Bounded stage windows, broker LogAppendTime, producer acknowledgements, monotonic local queue timers and browser clock estimate | Trace metadata adds no SQL timing columns. Raw UTC and approximate aligned browser samples remain separate, with offset/RTT uncertainty exposed. |
| Runtime configuration/shutdown | Configurable pools, batch/group/queue/send/grace/logging values; bounded socket inputs; finish safe fetched work within grace | Development controls remain token/origin protected and disabled in production. Cancellation leaves unacknowledged durable work replayable. |

Pricing audits/outbox decisions remain transactional. No speculative asynchronous audit service, Redis authority, Kafka replacement, Kubernetes or generic e-commerce features were added. Redis was a small share of the serial bottleneck, so no separate cache-pipelining service was justified.

## C. Measured tables

### Short before/after

| Metric | Serial | Batched pilot |
| --- | ---: | ---: |
| Actual producer/s | 498.11 | 498.24 |
| Submitted inventory events | 2,996.00 | 2,992.00 |
| Inventory/s including drain wait | 96.33 | 372.82 |
| Maximum sampled lag | 2,254.00 | 499.00 |
| Drain seconds | 25.09 | 2.02 |
| Redis rolling P95 ms | 0.69 | 0.66 |
| WS enqueue rolling P95 ms | 0.01 | 0.01 |
| Socket P95 ms | 23,131.78 | 1,057.93 |

### Consumer count, 30 seconds at 500/s

| Members | Actual producer/s | Max lag | Socket P50/P95/P99 ms |
| --- | ---: | ---: | --- |
| 1 | 499.62 | 783 | 838.66 / 1,624.43 / 1,960.51 |
| 2 | 499.64 | 356 | 378.11 / 817.45 / 986.45 |
| 3 | 499.62 | 355 | 406.19 / 777.06 / 880.03 |

### Corrected pipeline, 60 seconds per rate, three members

| Target/s | Actual producer/s | Inventory completion/s during production | Max lag | Drain s | Socket P50/P95/P99 ms | DB batch P95 ms | Redis P95 ms | WS enqueue P95 ms | Errors / DLQ delta |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: | --- |
| 100 | 99.95 | 99.89 | 8 | 2.03 | 23.98 / 77.06 / 108.84 | 21.79 | 1.60 | 0.02 | 0 / 0 |
| 500 | 499.81 | 496.04 | 354 | 2.03 | 382.78 / 743.97 / 852.88 | 53.87 | 2.10 | 0.01 | 0 / 0 |
| 1000 | 999.64 | 823.77 | 10756 | 17.08 | 5,165.68 / 15,311.63 / 17,254.26 | 88.10 | 2.60 | 0.01 | 0 / 0 |
| 2000 | 1,999.24 | 832.22 | 69529 | 87.30 | 49,351.84 / 79,834.64 / 83,855.28 | 116.75 | 0.90 | 0.01 | 0 / 0 |

### Raw rendered matrix near producer stop

| Target/s | Mean FPS | Raw rendered P50/P95/P99 ms | Samples |
| --- | ---: | --- | ---: |
| 100 | 59.91 | 93.00 / 870.00 / 902.00 | 45 |
| 500 | 59.81 | 891.00 / 1,438.00 / 1,581.00 | 59 |
| 1000 | 59.73 | 8,983.00 / 14,556.00 / 14,627.00 | 58 |
| 2000 | 59.56 | 33,744.00 / 34,920.00 / 34,938.00 | 59 |

### Frontend revalidation, two six-second synthetic trials

| Mode | Actual send/s | Mean FPS | Mean trial P95 ms | Renderer task CPU % |
| --- | ---: | ---: | ---: | ---: |
| NAIVE | 498.33 | N/A | N/A | 99.41 |
| ATOMIC | 498.83 | 41.62 | 63.50 | 86.33 |
| MEMOIZED | 499.42 | 47.58 | 45.50 | 85.15 |
| BATCHED | 498.83 | 58.40 | 31.50 | 64.80 |

### Real client load, 30 seconds each

| Clients | Connected | Connection errors | HTTP successes / errors | Enqueue-to-client P95 ms | Slow disconnects |
| --- | ---: | ---: | --- | ---: | ---: |
| 1 | 1 | 0 | 15 / 0 | 1.36 | 0 |
| 10 | 10 | 0 | 150 / 0 | 3.12 | 0 |
| 50 | 50 | 0 | 750 / 0 | 8.89 | 0 |
| 100 | 100 | 0 | 1500 / 0 | 12.65 | 0 |


### Interpretation and limits

The short comparison used one consumer, six seconds, a 500/s target, paced traffic and 500 selectable products. Counts differ slightly because the generator stops on elapsed time. SQL/demand state was not reset and trial order was not randomized. The early batch pilot predates final clock/queue fixes: its old `processing` timer measured a record tail; use its full `processing_batch` timer. Inventory-batch DB P95 was **45.25 ms**, versus **10.67 ms** per single transaction before; these are different units. Offset commit request P95 did not materially improve; the number of commits fell. Matched rendered latency/FPS is unavailable for that serial pair and is not invented. Inclusive throughput includes drain polling, not a steady-state capacity estimate.

The 30-second consumer-count comparison restarted the API between configurations. The third member gave a modest tail improvement over two. Defaults remain one member for normal 10/s traffic; sustained tests used three. This is co-located scaling, not a multi-host worker/gateway deployment.

Corrected rate cases lasted 60 seconds each. All submitted inventory records completed and committed lag drained. Socket percentiles retain the latest up-to-100,000 messages; the 2,000/s case excludes some early messages. Stage P95 is the final rolling latest-4,096 observations. DB batch windows can contain an earlier case when fewer than 4,096 new batches occur. At 1,000/s and 2,000/s production exceeded completion rate, and backlog grew. The preceding four 120-second cases also demonstrate that backend limit, but their incomplete socket measurements are superseded.

Raw rendered values in the matrix are the last available rolling 512-commit UTC percentiles near producer stop. They are uncalibrated and exclude coalesced events. The approximate observation interval uses backend sampled timestamps, which can be stale by two seconds. Mean FPS averages available one-second samples. Smooth frames do not imply fresh data.

### Clock-aligned browser observation

[Real browser JSON](benchmarks/live-aligned-2026-10-03.json) overlaps only part of [the 60-second pipeline case](benchmarks/pipeline-aligned-browser-2026-10-03.json), ending before final drain. Last rolling rendered P50/P95/P99 was **171.5 / 629.5 / 703.5 ms**. Mean FPS over 40 available aligned observations was **59.83**; browser-local queue P95 was **8.2 ms**, flush-to-card-commit P95 **2.4 ms**.

The last estimated server clock offset was **−466.5 ms**, with **7.5 ms RTT/2**. Across samples the offset ranged from −772.5 to +565.5 ms, and RTT/2 from 7.5 to 33.5 ms. HTTP midpoint alignment has asymmetric-path bias and between-poll drift; RTT/2 is not a complete error bound. Raw UTC is retained. This is an approximate rolling rendered observation, not a pooled whole-run percentile or direct matched comparison with the historical uncalibrated 13.6-second sample. Layout commit precedes physical paint/display scanout.

### Five-minute stability

[Soak JSON](benchmarks/soak-500-2026-10-03.json): 300 seconds, three members, **499.96/s production**, **499.07/s production-window inventory completion**, all **149,994** inventory events completed, peak lag **389**, approximately **2.03 seconds** drain. Socket P50/P95/P99: **298.93 / 757.53 / 903.45 ms**, latest 100,000 messages. No recorded errors, failed attempts, DLQ growth or slow-client disconnects.

Current API RSS rose from **78.25 to 99.42 MiB** during warmup; the last-minute range was approximately **98.43–99.42 MiB**. Mean process CPU was **81.24%**, where 100% represents one core, excluding other containers. Mean lag over successive 100-second blocks was **159.25 / 158.71 / 231.93**; mean sampled rolling producer-to-consumer P95 was **511.91 / 419.22 / 581.33 ms**. There was variability and a higher late-block latency, while lag remained bounded during this observed interval. Longer leak/drift guarantees are not established. The separate browser soak observer started late and includes idle/rebuild time; use its timestamps.

### Frontend and client-load caveats

Frontend mode runs used two six-second synthetic trials per mode, production build, fresh contexts and a 500/s target; Kafka was bypassed. Values average trial observations, not pooled percentiles. NAIVE had no bounded dashboard samples and is N/A, not zero FPS. BATCHED averaged **58.4 FPS / 31.5 ms** mean trial P95, less favorable than historical **60 FPS / 25.5 ms**. Functional isolation/coalescing/four-mode behavior passed, but these new measurements do not establish a frontend speedup or strict performance non-regression. The cause of the difference was not isolated. Real pipeline rendering remained near 60 FPS at measured delivery rates.

Multi-client cases lasted 30 seconds each under normal 10/s simulation with one default consumer, without a concurrent heavy browser benchmark. The 100-client case completed **1,500 HTTP requests**, with zero observed connection/HTTP errors, fanout P95 **12.65 ms** and no slow-client disconnect. This does not prove 100-client delivery at 2,000/s. Generators share the API container and network distance is local.

## D. Validation

| Final successful suite | Passed | Failed | Skipped |
| --- | ---: | ---: | ---: |
| Backend pytest | 31 | 0 | 0 |
| Frontend unit | 10 | 0 | 0 |
| Playwright with live stack and development recovery | 6 | 0 | 0 |
| Total unique tests | 47 | 0 | 0 |

Also observed: Ruff fatal-error lint, TypeScript checking, production API/frontend/simulator builds, healthy Kafka/PostgreSQL/Redis/API/frontend, real inventory/pricing/batch probes, actual consumer/API restart redelivery, authenticated controls, Redis stale fallback, OPEN/HALF_OPEN/CLOSED recovery, paused freshness, valid-record retention during Redis failure, automatic browser LIVE/reconnect recovery and desktop/mobile layout. Mobile overflow checks passed and screenshots were inspected. Final monotonic queue timers were rechecked with all backend tests and live inventory/batch probes.

An earlier browser run started during container recreation and one navigation failed with connection refused; the complete rerun after readiness passed all six. The first long-run receiver flaw is documented in the annotations. Unit revocation simulation is not a destructive multi-host rebalance drill; development fault injection is not container destruction.

Normal CI runs backend tests/lint and frontend unit/type/build checks. Separate manual [integration](../.github/workflows/integration.yml) and [performance](../.github/workflows/benchmark.yml) workflows run Docker probes and heavy benchmarks. Frontend `lint` is TypeScript checking; no ESLint pass is claimed. **Remote GitHub CI was not run or observed.** No commit or push was made. The local stack is running normal traffic with one member, production mode and development controls disabled.

## E–F. Architecture and five-minute explanation

[Architecture and failure paths](architecture.md), [main/scaling diagram and timing semantics](pipeline-timing.md), [five-minute interview walkthrough](interview.md).

Kafka is the replayable product-keyed backbone. PostgreSQL durably stores transactional inventory, receipts and price audits. Redis stores hot/fallback state. Co-located consumers batch business processing and share a gateway with bounded client queues. The browser separates ingestion from rendering through rAF, normalized Zustand selectors and memoized cards. Catalog circuit recovery retains stale data; pricing remains bounded audited rules. Delivery is at least once with idempotent business effects, not Kafka exactly-once.

Production scaling needs cross-instance fanout before adding gateway processes, WebSocket-aware TLS load balancing, managed data services, authentication/authorization, schema compatibility, backups/restore, audit/receipt retention and group-wide Prometheus/Grafana/tracing. No public production deployment was verified; login/cart/checkout/payments, trained pricing ML and Kubernetes were not added.

## G. Resume bullets using measured facts

- Built a 500-product Kafka/PostgreSQL/Redis retail dashboard with product-keyed ordering, idempotent transactional inventory, audited rule-based pricing and rAF/Zustand product-level rendering.
- Reduced local socket-delivery P95 from 23.1 seconds to 1.06 seconds in matched six-second, approximately 498-events/s trials through inventory transaction batches, receipt preloading and grouped Kafka commits.
- Processed all 149,994 simulated inventory events during a five-minute, approximately 500-events/s local test with three Kafka group members, zero recorded processing errors, bounded lag and 758 ms socket P95.
- Validated 100 local WebSocket clients and 1,500 HTTP requests over 30 seconds with zero observed connection/HTTP errors, alongside 47 passing unit/browser tests and restart/fallback recovery checks.

## H. Interview questions and reproduction

The [15 implementation-grounded questions and answers](interview.md#fifteen-questions-and-answers) cover Kafka, product keys, Redis Pub/Sub, lag, consumer groups, ordering, idempotency, PostgreSQL/Redis, circuit recovery, rAF, Zustand, slow clients, gateway scaling, delivery semantics and production changes.

Use README commands, [baseline replay instructions](benchmarks/annotations.md#reproduce-the-serial-baseline), `tests/pipeline_benchmark.py`, `tests/load_probe.py`, `npm run benchmark` and `npm run benchmark:live`. Set three members explicitly for the sustained configuration and stop the normal simulator before any independent producer. Preserve new raw files rather than overwriting prior observations.

Hardware is retained in the summary/browser JSON. Runs were on one Windows development machine using Docker and Chromium, with colocated generators, changing SQL/demand/audit state, no database reset, randomized trial order or CPU isolation. Stage, socket and rendered windows differ. Negative cross-process intervals are excluded; Kafka append timestamps have millisecond precision and browser clocks required approximate calibration. The high-rate backlog boundary, synthetic frontend differences, indefinite audit/receipt retention, process-local circuit/fault state and unverified remote CI remain explicit limitations.
