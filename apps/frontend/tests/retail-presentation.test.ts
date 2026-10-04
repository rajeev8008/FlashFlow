import { test } from "node:test";
import assert from "node:assert/strict";
import {
  successRate,
  chartGeometry,
  filterAdvice,
  activeAdvice,
  number,
} from "../lib/retail-presentation";
test("model health percentages require a valid known denominator", () => {
  assert.equal(
    successRate({ forecast_requests: 10, forecast_successes: 8 }),
    80,
  );
  for (const h of [
    {},
    { forecast_requests: 0, forecast_successes: 0 },
    { forecast_requests: 10 },
    { forecast_requests: 10, forecast_successes: 11 },
  ])
    assert.equal(successRate(h), null);
  assert.equal(number(null), "Unavailable");
});
test("dynamic chart range reveals small changes without changing observed values", () => {
  const values = [5256, 5255, 5224],
    before = [...values],
    axis = chartGeometry(values);
  assert.ok(axis.min > 5000 && axis.max > 5256 && axis.y(5256) < axis.y(5224));
  assert.deepEqual(values, before);
  assert.equal(chartGeometry([2, 3], true).min, 0);
  const price = chartGeometry([129, 129]);
  assert.ok(Number.isFinite(price.y(129)));
});
test("recommendation filters preserve all audit rows and classify recent executions", () => {
  const at = Date.now(),
    rows = [
      {
        status: "PENDING",
        product_id: "a",
        risk: "HIGH",
        updated_at: new Date(at).toISOString(),
      },
      {
        status: "EXPIRED",
        product_id: "a",
        risk: "HIGH",
        updated_at: new Date(at).toISOString(),
      },
      {
        status: "EXECUTED",
        product_id: "b",
        risk: "MEDIUM",
        updated_at: new Date(at - 600000).toISOString(),
      },
    ];
  assert.equal(filterAdvice(rows, "PENDING", "a", "HIGH").length, 1);
  assert.equal(filterAdvice(rows, "", "", "CRITICAL").length, 0);
  assert.ok(activeAdvice(rows[0], at));
  assert.equal(activeAdvice(rows[1], at), false);
  assert.equal(activeAdvice(rows[2], at), false);
  assert.equal(rows.length, 3);
});
