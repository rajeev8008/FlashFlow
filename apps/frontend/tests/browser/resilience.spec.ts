import { test, expect } from "@playwright/test";
const product = {
  product_id: "retained",
  name: "Retained product",
  category: "Test",
  current_price: "25.00",
  stock: 10,
  reserved_stock: 2,
  version: 5,
  status: "ACTIVE",
  sales_velocity: 0,
  last_updated: "2026-10-02T00:00:00Z",
};
test("stale Redis snapshots retain newer cards and recover automatically without reconnect", async ({
  page,
}) => {
  let stale = false,
    unavailable = false;
  await page.route("**/api/chaos", (route) =>
    route.fulfill({ status: 404, json: {} }),
  );
  await page.route("**/api/products", (route) =>
    route.fulfill({
      json: {
        products: unavailable ? [] : [{ ...product, version: stale ? 4 : 5 }],
        metadata: {
          source: unavailable ? "unavailable" : stale ? "redis" : "live",
          stale,
          snapshot_at: unavailable
            ? null
            : new Date(Date.now() - 30000).toISOString(),
          age_seconds: unavailable ? null : 30,
          reason: stale ? "Inventory unavailable; cached data" : null,
          breaker: {
            state: stale ? "OPEN" : "CLOSED",
            retry_after_seconds: stale ? 5 : 0,
            transitions: [],
          },
        },
      },
    }),
  );
  await page.routeWebSocket("**/ws", () => {});
  await page.goto("/");
  await expect(page.getByRole("status")).toContainText("LIVE");
  const card = page.locator('[data-product="retained"]');
  await expect(card).toHaveAttribute("data-version", "5");
  stale = true;
  await expect(page.getByRole("status")).toContainText("DEGRADED");
  await expect(page.getByLabel("Data freshness")).toContainText(
    "Source: redis",
  );
  await expect(page.locator("[data-stale-age]")).toContainText("old");
  await expect(card).toHaveAttribute("data-version", "5");
  unavailable = true;
  await expect(page.getByLabel("Data freshness")).toContainText(
    "Source: unavailable",
  );
  await expect(card).toContainText("$25.00");
  stale = false;
  unavailable = false;
  await expect(page.getByRole("status")).toContainText("LIVE");
  await expect(card).toHaveAttribute("data-version", "5");
  await expect(page.getByText("Development lab · fault controls")).toHaveCount(
    0,
  );
});
test("real development controls demonstrate circuit recovery without blanking the dashboard", async ({
  page,
}) => {
  test.skip(
    !process.env.CHAOS_TEST,
    "Enable development controls and set CHAOS_TEST=1.",
  );
  test.setTimeout(60000);
  await page.setViewportSize({ width: 1280, height: 1100 });
  const denied = await page.request.post("/api/chaos", {
    data: { inventory_unavailable: true },
  });
  expect(denied.status()).toBe(403);
  const foreign = await page.request.get("/api/chaos", {
    headers: { Host: "untrusted.invalid:3000" },
  });
  expect(foreign.status()).toBe(403);
  await page.goto("/");
  await expect(page.getByRole("status")).toContainText("LIVE");
  await page.getByText("Development lab · fault controls").click();
  const fault = page.getByLabel("Inventory read failure");
  try {
    await fault.click();
    await expect(fault).toBeChecked();
    await expect(page.getByRole("status")).toContainText("DEGRADED");
    await expect(page.locator("[data-breaker]")).toContainText("OPEN", {
      timeout: 15000,
    });
    await expect(page.locator("article")).toHaveCount(500);
    await expect(page.getByLabel("Data freshness")).toContainText(
      "Source: redis",
    );
    await page.screenshot({
      path: "../../.tmp/resilience-degraded.png",
      fullPage: false,
    });
    await fault.click();
    await expect(fault).not.toBeChecked();
    await expect(page.getByRole("status")).toContainText("LIVE", {
      timeout: 15000,
    });
    await expect(page.getByLabel("Data freshness")).toContainText(
      "HALF_OPEN → CLOSED",
    );
    await page.getByRole("button", { name: "Interrupt WebSockets" }).click();
    await expect(page.getByRole("status")).toContainText("RECONNECTING");
    await expect(page.getByRole("status")).toContainText("LIVE");
  } finally {
    await page.evaluate(async () => {
      await fetch("/api/chaos", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          inventory_unavailable: false,
          redis_unavailable: false,
          consumer_paused: false,
          latency_ms: 0,
        }),
      });
    });
  }
});
