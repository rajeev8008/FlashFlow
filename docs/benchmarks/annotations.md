# Raw run annotations

Raw JSON measurements remain in the repository to support the published reports. The serial-baseline source archive, replay Compose override, screenshots and validation logs are retained only on the original local PC. The baseline replay commands below require those local files; they cannot be run from a fresh clone alone. Current pipeline and browser probes remain available in the source tree.

October 2 files are preserved historical observations. `browser-1790931434070.json` remains a superseded, producer-throttled pilot as explained in `../performance.md`.

October 3 baseline and consumer-count comparisons are short runs below the 45-second input heartbeat timeout. Their socket samples cover their test intervals. The baseline source archive contains the timing-only serial implementation used for `baseline-serial-2026-10-03.json`; SHA-256: `0dc34e927360b68289a4dde626a7c67556ae6d01c80cae216e2da6e734204ac8`.

`sustained-three-2026-10-03.json` has valid producer counts, durable inventory-completion counts, committed drain, lag and backend samples for four 120-second cases. Its **socket latency distributions are incomplete and superseded**: the receiver replied only to idle server heartbeats, while continuous traffic requires periodic pongs. The old gateway removed that client after 45 seconds but did not explicitly close its socket, so the probe incorrectly reported no receiver error while recording fewer messages. Do not use its socket P50/P95/P99 as whole-case percentiles. `live-sustained-2026-10-03.json` is an independently sampled browser, which sends periodic pongs; it also spans a final container rebuild/reconnect and idle periods.

The probe now sends periodic pongs, and the gateway explicitly closes removed sockets. `pipeline-corrected-2026-10-03.json` repeats 60-second rate cases with the corrected receiver. `live-corrected-2026-10-03.json` samples the browser during those cases. The five-minute soak uses the corrected receiver too. Inspect message counts, errors and sample windows before making claims.

Backend percentiles are rolling latest-4,096 stage observations, not lossless case-wide traces. If fewer than 4,096 new batches occur, transaction-stage windows can include an earlier case. Event-to-client percentiles retain the latest 100,000 messages per case; high-volume tail windows are not a pooled percentile over all messages. Rendered percentiles are rolling latest-512 commits and exclude superseded/coalesced events. Percentiles from these different populations cannot be equated or added.

The early optimized `processing` stage timed only the post-transaction record tail for batched records; use `processing_batch` for those pilot files. Final instrumentation times batch dispatch to per-record enqueue for `processing`. Historical files remain unchanged. `database_batch` is full transaction time and `database_amortized` is transaction time divided by record count; neither should be presented as the other's units.

Runs are sequential on one development machine, without database resets, randomized trial order, CPU isolation or matched historical demand state. The generator shares the API container. Product distribution and pacing differ from the original October 2 one-second burst. These results support a local bottleneck diagnosis and bounded-load observations, not production capacity guarantees or an exact retrospective decomposition of the original 13.6-second sample.

## Reproduce the serial baseline

The local image is tagged `flashflow-baseline:timed`. On another machine, copy `apps/api` into a **new isolated build directory**, overlay the source ZIP there, place its root `pipeline_benchmark.py` under that directory's `tests`, and build it with the copied Dockerfile as `flashflow-baseline:timed`. The archive deliberately reuses this repository's Dockerfile, migrations and `load_probe.py` helper; do not overlay it onto the working checkout.

```powershell
docker compose stop simulator
docker compose -f docker-compose.yml -f docs/benchmarks/baseline.compose.yml up -d --no-build --wait api
docker compose exec -T api python tests/pipeline_benchmark.py --rates 500 --seconds 6 --label serial-replay
# Restore the current API only after committed backlog drain:
docker compose up -d --wait api
```

The old baseline probe has the idle-heartbeat flaw described above, so its reproduction command stays below 45 seconds. Replay on a different machine or later database state will not recreate identical numeric results. Keep outputs under new filenames and preserve the source archive/hash.

`live-aligned-2026-10-03.json` observes only the overlapping part of `pipeline-aligned-browser-2026-10-03.json`; it started before that producer, and ends before its final drain. Its last valid rendered sample is a rolling percentile, not a whole-case distribution. The preceding `pipeline-aligned-2026-10-03.json` is a separate 60-second socket-only run. Browser clock alignment estimates offset through an HTTP request midpoint, with RTT/2 path uncertainty and possible between-sample clock drift. Raw UTC values remain available. `live-soak-2026-10-03.json` started late in the soak and continues through idle time and a subsequent container rebuild, so use its timestamps rather than treating its entire duration as 500/s traffic.
