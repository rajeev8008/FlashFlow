import { test, expect, WebSocketRoute } from "@playwright/test";
const product = {
  product_id: "a",
  name: "Test headphones",
  category: "Audio",
  current_price: "10.00",
  stock: 20,
  reserved_stock: 2,
  version: 1,
  status: "ACTIVE",
  sales_velocity: 0,
  last_updated: "2026-10-02T00:00:00Z",
};
const envelope = (products: unknown[]) => ({
  products,
  metadata: {
    source: "live",
    stale: false,
    snapshot_at: new Date().toISOString(),
    age_seconds: 0,
    reason: null,
    breaker: { state: "CLOSED", retry_after_seconds: 0, transitions: [] },
  },
});
test("price-only Kafka snapshots update direction, reason and revision without stock rollback", async ({
  page,
}) => {
  let socket: WebSocketRoute;
  await page.route("**/api/products", (route) =>
    route.fulfill({ json: envelope([product]) }),
  );
  await page.routeWebSocket("**/ws", (ws) => {
    socket = ws;
  });
  await page.goto("/");
  const card = page.locator('[data-product="a"]');
  await expect(card).toHaveAttribute("data-version", "1");
  socket!.send(
    JSON.stringify({
      type: "product_update",
      product: {
        ...product,
        price_version: 1,
        current_price: "10.50",
        demand_state: "HIGH",
        price_direction: "UP",
        pricing_reason: "High demand target",
        pricing_source: "RULES",
      },
    }),
  );
  await expect(card).toContainText("$10.50");
  await expect(card).toContainText("↑ HIGH demand · RULES");
  await expect(card).toHaveAttribute("data-version", "1");
  await expect(card).toHaveAttribute("data-price-version", "1");
  await card.locator("summary").click();
  await expect(card).toContainText("High demand target");
  // Periodic older catalog snapshots must not undo the delivered price.
  await page.waitForTimeout(2300);
  await expect(card).toContainText("$10.50");
  await expect(card).toContainText("18 available");
});
test("live changes, isolation, benchmark modes, reconnect and degraded/offline states", async ({
  page,
  context,
}) => {
  let sockets: WebSocketRoute[] = [];
  let catalogRequests = 0;
  await page.route("**/api/products", (route) => {
    catalogRequests++;
    return route.fulfill({
      json: envelope([
        product,
        { ...product, product_id: "b", name: "Unrelated product" },
      ]),
    });
  });
  await page.routeWebSocket("**/ws", (ws) => {
    sockets.push(ws);
  });
  await page.goto("/");
  await expect(page.getByRole("status")).toContainText("LIVE");
  const a = page.locator('[data-product="a"]'),
    b = page.locator('[data-product="b"]');
  await expect(a).toHaveAttribute("data-version", "1");
  const before = await b.getAttribute("data-commits");
  sockets[0].send(
    JSON.stringify({
      type: "product_update",
      product: {
        ...product,
        version: 2,
        current_price: "12.00",
        reserved_stock: 5,
      },
    }),
  );
  await expect(a).toHaveAttribute("data-version", "2");
  await expect(a).toContainText("$12.00");
  await expect(a).toContainText("15 available");
  await page.waitForTimeout(1100);
  expect(await b.getAttribute("data-commits")).toBe(before);
  await page.getByLabel("Render mode").selectOption("NAIVE");
  const naiveBefore = await b.getAttribute("data-commits");
  sockets[0].send(
    JSON.stringify({
      type: "product_update",
      product: { ...product, version: 3 },
    }),
  );
  await expect(a).toHaveAttribute("data-version", "3");
  expect(await b.getAttribute("data-commits")).not.toBe(naiveBefore);
  await page.getByLabel("Render mode").selectOption("ATOMIC");
  const atomicBefore = await b.getAttribute("data-commits");
  await page.waitForTimeout(1100);
  expect(await b.getAttribute("data-commits")).not.toBe(atomicBefore);
  await page.getByLabel("Render mode").selectOption("MEMOIZED");
  const memoBefore = await b.getAttribute("data-commits");
  await page.waitForTimeout(1100);
  expect(await b.getAttribute("data-commits")).toBe(memoBefore);
  sockets[0].close();
  await expect(page.getByRole("status")).toContainText("RECONNECTING");
  await expect.poll(() => sockets.length).toBe(2);
  await expect(page.getByRole("status")).toContainText("LIVE");
  expect(catalogRequests).toBeGreaterThanOrEqual(2);
  await expect(a).toHaveAttribute("data-version", "3");
  sockets[1].send("invalid json");
  await expect(page.getByRole("status")).toContainText("DEGRADED");
  await context.setOffline(true);
  await expect(page.getByRole("status")).toContainText("OFFLINE");
  await context.setOffline(false);
  await expect(page.getByRole("status")).toContainText("LIVE");
});
test("running Kafka stack delivers changing versions and a responsive dashboard", async ({
  page,
}) => {
  test.skip(
    !process.env.LIVE_STACK,
    "Set LIVE_STACK=1 with the Compose stack and simulator running.",
  );
  await page.goto("/");
  await expect(page.getByRole("status")).toContainText("LIVE");
  await expect(page.locator("article")).toHaveCount(500);
  const versions = await page
    .locator("article")
    .evaluateAll((cards) =>
      Object.fromEntries(
        cards.map((card) => [
          card.getAttribute("data-product"),
          card.getAttribute("data-version"),
        ]),
      ),
    );
  await expect
    .poll(
      () =>
        page
          .locator("article")
          .evaluateAll(
            (cards, previous) =>
              cards.some(
                (card) =>
                  Number(card.getAttribute("data-version")) >
                  Number(previous[card.getAttribute("data-product")!]),
              ),
            versions,
          ),
      { timeout: 20000 },
    )
    .toBe(true);
  await page.getByLabel("Render mode").selectOption("ATOMIC");
  await page.getByLabel("Render mode").selectOption("BATCHED");
  await page.waitForTimeout(2000);
  console.log(
    "Live browser sample:",
    await page
      .locator("[data-metric]")
      .evaluateAll((items) =>
        Object.fromEntries(
          items.map((item) => [
            item.getAttribute("data-metric"),
            item.textContent,
          ]),
        ),
      ),
  );
  await page.setViewportSize({ width: 1280, height: 1100 });
  await page.screenshot({ path: "../../.tmp/phase3-desktop.png" });
  await page.setViewportSize({ width: 390, height: 844 });
  await page.screenshot({ path: "../../.tmp/phase3-mobile.png" });
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
  ).toBe(true);
});
test("2000 socket updates coalesce while the dashboard stays interactive", async ({
  page,
}) => {
  let socket: WebSocketRoute;
  await page.route("**/api/products", (route) =>
    route.fulfill({
      json: envelope(
        Array.from({ length: 500 }, (_, i) => ({
          ...product,
          product_id: String(i),
          name: `Product ${i}`,
        })),
      ),
    }),
  );
  await page.routeWebSocket("**/ws", (ws) => {
    socket = ws;
  });
  await page.goto("/");
  await expect(page.locator("article")).toHaveCount(500);
  const before = await page
    .locator('[data-product="0"]')
    .getAttribute("data-commits");
  for (let version = 2; version <= 2001; version++)
    socket!.send(
      JSON.stringify({
        type: "product_update",
        product: { ...product, product_id: "0", version },
      }),
    );
  const card = page.locator('[data-product="0"]');
  await expect(card).toHaveAttribute("data-version", "2001");
  const commits =
    Number(await card.getAttribute("data-commits")) - Number(before);
  expect(commits).toBeLessThan(2000);
  await page.getByLabel("Render mode").selectOption("MEMOIZED");
  await expect(page.getByLabel("Render mode")).toHaveValue("MEMOIZED");
  console.log(
    `Synthetic browser burst: 2000 messages, ${commits} updated-card commits; 500 cards.`,
  );
});
