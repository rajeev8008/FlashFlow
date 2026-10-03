// Synthetic browser-only traffic isolates rendering; it is NOT Kafka throughput.
import { chromium } from "@playwright/test";
import { mkdir, writeFile } from "node:fs/promises";
import { cpus, platform, release } from "node:os";
import { performance } from "node:perf_hooks";

const duration = Number(process.env.BENCH_SECONDS ?? 10);
const repeats = Number(process.env.BENCH_REPEATS ?? 3);
const rates = (process.env.BENCH_RATES ?? "50,500,2000").split(",").map(Number);
if (!(
  duration >= 3 &&
  repeats >= 1 &&
  repeats <= 10 &&
  rates.every((n) => n > 0 && n <= 10000)
))
  throw new Error("Invalid benchmark settings");
const browser = await chromium.launch();
const results = [];
async function bounded(promise, milliseconds = 1500) {
  let timer;
  try {
    return await Promise.race([
      promise,
      new Promise((resolve) => {
        timer = setTimeout(() => resolve(null), milliseconds);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}
try {
  for (let repeat = 1; repeat <= repeats; repeat++)
    for (const rate of rates)
      for (const mode of ["NAIVE", "ATOMIC", "MEMOIZED", "BATCHED"]) {
        const context = await browser.newContext({
          viewport: { width: 1440, height: 900 },
        });
        const page = await context.newPage();
        const products = Array.from({ length: 500 }, (_, i) => ({
          product_id: `bench-${i}`,
          name: `Benchmark ${i}`,
          category: "Test",
          current_price: "10.00",
          stock: 100,
          reserved_stock: 0,
          version: 1,
          status: "ACTIVE",
          sales_velocity: 0,
          last_updated: new Date().toISOString(),
        }));
        let socket;
        await page.route("**/api/products", (route) =>
          route.fulfill({
            json: {
              products,
              metadata: {
                source: "live",
                stale: false,
                snapshot_at: new Date().toISOString(),
                age_seconds: 0,
                reason: null,
                breaker: {
                  state: "CLOSED",
                  retry_after_seconds: 0,
                  transitions: [],
                },
              },
            },
          }),
        );
        await page.route("**/api/metrics", (route) =>
          route.fulfill({
            status: 503,
            json: { error: "Synthetic benchmark" },
          }),
        );
        await page.route("**/api/chaos", (route) =>
          route.fulfill({ status: 404, json: { error: "Disabled" } }),
        );
        await page.routeWebSocket("**/ws", (ws) => {
          socket = ws;
        });
        await page.goto(process.env.FRONTEND_URL ?? "http://localhost:3000");
        await page.locator("[data-product]").last().waitFor();
        await page.getByLabel("Render mode").selectOption(mode);
        await page.waitForTimeout(2100); // Exclude hydration/remounts; preserve the documented parent pulse.
        const cdp = await context.newCDPSession(page);
        await cdp.send("Performance.enable");
        const metrics = async () =>
          Object.fromEntries(
            (await cdp.send("Performance.getMetrics")).metrics.map((m) => [
              m.name,
              m.value,
            ]),
          );
        const before = await metrics();
        const samples = [];
        let sent = 0;
        const started = performance.now();
        // Sampling a busy renderer must not throttle the independent producer clock.
        const producer = setInterval(() => {
          if (performance.now() - started >= duration * 1000) {
            clearInterval(producer);
            return;
          }
          const target = Math.floor(
            ((performance.now() - started) / 1000) * rate,
          );
          for (; sent < target; sent++) {
            const p = products[sent % 10]; // Identical hot-product distribution in every mode.
            p.version++;
            socket.send(
              JSON.stringify({
                type: "product_update",
                event_at: new Date().toISOString(),
                product: { ...p },
              }),
            );
          }
        }, 20);
        try {
          while (performance.now() - started < duration * 1000) {
            await new Promise((resolve) => setTimeout(resolve, 1000));
            const sample = await bounded(
              page
                .locator("[data-metric]")
                .evaluateAll((nodes) =>
                  Object.fromEntries(
                    nodes.map((n) => [
                      n.getAttribute("data-metric"),
                      n.textContent === "N/A" ? null : Number(n.textContent),
                    ]),
                  ),
                ),
            );
            if (sample) samples.push(sample);
          }
        } finally {
          clearInterval(producer);
        }
        const elapsed = (performance.now() - started) / 1000;
        const after = await bounded(metrics());
        const mean = (key) => {
          const values = samples
            .map((s) => s[key])
            .filter((v) => typeof v === "number");
          return values.length
            ? values.reduce((a, b) => a + b, 0) / values.length
            : null;
        };
        const result = {
          repeat,
          mode,
          requested_events_per_second: rate,
          sent,
          seconds: duration,
          observation_seconds: elapsed,
          achieved_send_rate: sent / duration,
          received_events_per_second: mean("events"),
          fps: mean("fps"),
          card_commits_per_second: mean("renders"),
          ui_flushes_per_second: mean("flushes"),
          event_to_ui_p50_ms: samples.at(-1)?.p50 ?? null,
          event_to_ui_p95_ms: samples.at(-1)?.p95 ?? null,
          renderer_task_cpu_percent: after
            ? ((after.TaskDuration - before.TaskDuration) / elapsed) * 100
            : null,
          js_heap_bytes: after?.JSHeapUsedSize ?? null,
          observed_samples: samples.length,
          samples,
        };
        results.push(result);
        console.log(JSON.stringify({ ...result, samples: undefined }));
        await context.close();
      }
} finally {
  await browser.close();
}
const output = {
  kind: "synthetic-browser-rendering",
  harness_version: 2,
  recorded_at: new Date().toISOString(),
  browser: browser.version(),
  environment: {
    os: `${platform()} ${release()}`,
    cpu: cpus()[0]?.model,
    logical_cpus: cpus().length,
  },
  methodology:
    "500 cards, 10 round-robin hot products, independent 20ms producer, 2.1s warmup, fresh context per run; metric reads bounded at 1.5s; backend bypassed; CDP renderer task time is not total system CPU; null means renderer unavailable; observation interval can exceed send interval",
  results,
};
await mkdir("../../docs/benchmarks", { recursive: true });
const path = `../../docs/benchmarks/browser-${Date.now()}.json`;
await writeFile(path, JSON.stringify(output, null, 2), { flag: "wx" });
console.log(`Saved ${path}`);
