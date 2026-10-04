import { test } from "node:test";
import assert from "node:assert/strict";
import {
  calibrateClock,
  clockEstimate,
  applyProducts,
  arrivals,
  observedProducts,
  resetLatencySamples,
  pruneLatencySamples,
  flushedAt,
  clientStages,
  recordStage,
  createBuffer,
  isProduct,
  isSnapshot,
  isNewer,
  percentile,
  recordLatency,
  uiLatencies,
  Product,
  reconnectDelay,
  useInventory,
} from "../lib/live";
const product: Product = {
  product_id: "a",
  name: "A",
  category: "Test",
  current_price: "10.00",
  stock: 20,
  reserved_stock: 2,
  version: 1,
  status: "ACTIVE",
  sales_velocity: 0,
  last_updated: "2026-10-02T00:00:00Z",
};
test("latency percentiles are bounded and unavailable without event timestamps", () => {
  uiLatencies.length = 0;
  assert.equal(percentile(uiLatencies, 0.95), null);
  recordLatency(-1);
  recordLatency(NaN);
  assert.equal(uiLatencies.length, 0);
  for (let i = 1; i <= 600; i++) recordLatency(i);
  assert.equal(uiLatencies.length, 512);
  assert.equal(percentile([1, 2, 3, 4, 100], 0.95), 100);
  assert.equal(percentile([1, 2, 3, 4, 100], 0.5), 3);
});
test("price-only revisions update cards without inventory rollback", () => {
  const priced = { ...product, price_version: 1, current_price: "10.50" };
  assert.ok(isNewer(priced, product));
  assert.equal(isNewer({ ...product, version: 2 }, priced), false);
  useInventory.setState({ productsById: {}, ids: [] });
  applyProducts([product]);
  applyProducts([priced]);
  assert.equal(useInventory.getState().productsById.a.current_price, "10.50");
  let callback: FrameRequestCallback = () => {};
  const buffer = createBuffer(
    applyProducts,
    (cb) => {
      callback = cb;
      return 1;
    },
    () => {},
  );
  buffer.push({ ...priced, price_version: 2, current_price: "11.00" });
  buffer.push({ ...priced, version: 2 });
  callback(0);
  assert.equal(useInventory.getState().productsById.a.current_price, "11.00");
});
test("trust boundary rejects invalid products", () => {
  assert.ok(isProduct(product));
  for (const invalid of [
    null,
    {},
    { ...product, reserved_stock: 21 },
    { ...product, current_price: "" },
    { ...product, sales_velocity: Infinity },
    { ...product, version: 0 },
    { ...product, last_updated: "invalid" },
  ])
    assert.equal(isProduct(invalid), false);
});
test("snapshot metadata is mandatory and preserves explicit stale-cache semantics", () => {
  const value = {
    products: [product],
    metadata: {
      source: "redis",
      stale: true,
      snapshot_at: "2026-10-02T00:00:00Z",
      age_seconds: 30,
      reason: "Cached",
      breaker: { state: "OPEN", retry_after_seconds: 5, transitions: [] },
    },
  };
  assert.ok(isSnapshot(value));
  assert.equal(isSnapshot([product]), false);
  assert.equal(
    isSnapshot({ ...value, metadata: { ...value.metadata, age_seconds: -1 } }),
    false,
  );
});
test("buffer merges latest versions and schedules exactly once per frame", () => {
  let callback: FrameRequestCallback = () => {},
    scheduled = 0;
  const flushed: Product[][] = [];
  const buffer = createBuffer(
    (p) => flushed.push(p),
    (cb) => {
      callback = cb;
      return ++scheduled;
    },
    () => {},
  );
  buffer.push(product);
  buffer.push({ ...product, version: 3 });
  buffer.push({ ...product, version: 2 });
  buffer.push({ ...product, product_id: "b" });
  assert.equal(scheduled, 1);
  assert.equal(flushed.length, 0);
  callback(0);
  assert.equal(flushed.length, 1);
  assert.equal(flushed[0].length, 2);
  assert.equal(flushed[0][0].version, 3);
  buffer.push({ ...product, version: 4 });
  assert.equal(scheduled, 2);
  callback(16);
  assert.equal(flushed.length, 2);
});
test("buffer cancellation and explicit mode-change drain", () => {
  let cancelled = 0,
    flushes = 0;
  const buffer = createBuffer(
    () => flushes++,
    () => 7,
    (id) => {
      assert.equal(id, 7);
      cancelled++;
    },
  );
  buffer.push(product);
  buffer.drain();
  assert.equal(flushes, 1);
  assert.equal(cancelled, 1);
  buffer.push(product);
  buffer.stop();
  assert.equal(flushes, 1);
  assert.equal(cancelled, 2);
});
test("atomic updates preserve unrelated identities and IDs; stale snapshots cannot roll back", () => {
  useInventory.setState({ productsById: {}, ids: [] });
  const other = { ...product, product_id: "b" };
  applyProducts([product, other]);
  const before = useInventory.getState();
  applyProducts([{ ...product, version: 2 }]);
  const after = useInventory.getState();
  assert.equal(after.ids, before.ids);
  assert.equal(after.productsById.b, before.productsById.b);
  applyProducts([product]);
  assert.equal(useInventory.getState(), after);
  assert.equal(after.productsById.a.version, 2);
});
test("exponential reconnect backoff is capped", () => {
  assert.deepEqual(
    [0, 1, 2, 3, 4, 5, 100].map(reconnectDelay),
    [1000, 2000, 4000, 8000, 16000, 30000, 30000],
  );
});

test("client stage samples are bounded and flush timing uses the newest arrival", () => {
  useInventory.setState({ productsById: {}, ids: [] });
  clientStages.browserQueue = [];
  recordStage("browserQueue", -1);
  recordStage("browserQueue", NaN);
  assert.equal(clientStages.browserQueue.length, 0);
  for (let i = 0; i < 600; i++) recordStage("browserQueue", i);
  assert.equal(clientStages.browserQueue.length, 512);
  let callback: FrameRequestCallback = () => {};
  arrivals.set(product.product_id, performance.now());
  const buffer = createBuffer(
    () => {},
    (cb) => {
      callback = cb;
      return 1;
    },
    () => {},
  );
  buffer.push(product);
  const newest = { ...product, version: 2 };
  observedProducts.set(product.product_id, newest);
  buffer.push(newest);
  callback(0);
  assert.ok(flushedAt.has(product.product_id));
  arrivals.clear();
  flushedAt.clear();
});

test("cross-process clock estimate uses request midpoint and exposes uncertainty", () => {
  calibrateClock(1600, 1000, 1200);
  assert.equal(clockEstimate.offsetMs, 500);
  assert.equal(clockEstimate.uncertaintyMs, 100);
  calibrateClock(NaN, 1000, 1200);
  calibrateClock(1600, 1000, 4000);
  assert.equal(clockEstimate.offsetMs, 500);
});

test("latency windows expire and reset while a real stalled frame stays measurable", () => {
  resetLatencySamples();
  recordStage("browserQueue", 12000);
  assert.equal(percentile(clientStages.browserQueue, 0.95), 12000);
  pruneLatencySamples(performance.now() + 30001);
  assert.equal(percentile(clientStages.browserQueue, 0.95), null);
  recordLatency(30);
  recordStage("render", 2);
  resetLatencySamples();
  assert.equal(uiLatencies.length, 0);
  assert.equal(clientStages.render.length, 0);
  assert.equal(clockEstimate.offsetMs, null);
});

test("queue timing never uses a prior product's receipt or a superseded catalog revision", () => {
  resetLatencySamples();
  useInventory.setState({ productsById: {}, ids: [] });
  let run: FrameRequestCallback = () => {};
  const buffer = createBuffer(
    () => {},
    (cb) => {
      run = cb;
      return 1;
    },
    () => {},
  );
  arrivals.set(product.product_id, performance.now() - 12000);
  observedProducts.set(product.product_id, product);
  buffer.push(product);
  run(0);
  assert.ok(clientStages.browserQueue.at(-1)! >= 12000);
  resetLatencySamples();
  arrivals.set(product.product_id, performance.now() - 12000);
  observedProducts.set(product.product_id, { ...product });
  buffer.push(product);
  run(0);
  assert.equal(clientStages.browserQueue.length, 0);
  observedProducts.set(product.product_id, product);
  applyProducts([{ ...product, version: 2 }]);
  buffer.push(product);
  run(0);
  assert.equal(clientStages.browserQueue.length, 0);
  resetLatencySamples();
});
