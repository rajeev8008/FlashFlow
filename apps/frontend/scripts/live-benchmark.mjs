// Samples the real rendered dashboard; does not replace socket delivery statistics.
import { chromium } from '@playwright/test';
import { writeFile } from 'node:fs/promises';
const seconds = Number(process.env.LIVE_BENCH_SECONDS ?? 180);
if (!(seconds >= 5 && seconds <= 3600)) throw new Error('Use 5..3600 seconds');
const browser = await chromium.launch();
const samples = [];
const errors = [];
try {
  const page = await browser.newPage();
  page.on('pageerror', error => errors.push(String(error)));
  await page.goto(process.env.FRONTEND_URL ?? 'http://localhost:3000');
  await page.locator('[data-product]').last().waitFor();
  const started = Date.now();
  while (Date.now() - started < seconds * 1000) {
    await new Promise(resolve => setTimeout(resolve, 1000));
    let timer;
    const metrics = await Promise.race([
      page.locator('[data-metric]').evaluateAll(nodes =>
        Object.fromEntries(nodes.map(node => [node.dataset.metric,
          node.textContent === 'N/A' ? null : Number(node.textContent)]))),
      new Promise(resolve => { timer = setTimeout(() => resolve(null), 1500); }),
    ]);
    clearTimeout(timer);
    samples.push({ at: new Date().toISOString(), ...metrics });
  }
  const output = process.env.LIVE_BENCH_OUTPUT ?? `../../docs/benchmarks/live-browser-${Date.now()}.json`;
  await writeFile(output, JSON.stringify({ kind: 'real-rendered-dashboard', seconds,
    browser: browser.version(), errors, notes: 'One-second dashboard samples; rolling 512 rendered-event percentiles, coalesced events excluded; synchronized clocks required; React layout commit is not physical display scanout', samples }, null, 2), { flag: 'wx' });
  console.log(`Saved ${output}`);
} finally {
  await browser.close();
}
