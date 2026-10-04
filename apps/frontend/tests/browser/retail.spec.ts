import { test, expect } from "@playwright/test";
const id = "00000000-0000-0000-0000-000000000001";
const product = {
  product_id: id,
  name: "Studio headphones",
  category: "Retail Demo",
  current_price: "129.00",
  stock: 24,
  reserved_stock: 0,
  version: 1,
  status: "ACTIVE",
  sales_velocity: 0,
  last_updated: new Date().toISOString(),
};
const forecast = {
  forecast_id: id,
  generated_at: new Date().toISOString(),
  expected_sales: 24,
  lower_bound: 18,
  upper_bound: 30,
  risk_level: "HIGH",
  estimated_stockout_minutes: 60,
  model_version: "test-model",
  interval_method: "validation residual band",
  risk_reason: "24 available; expected 24, upper band 30",
  fallback: false,
};
const recommendation = {
  recommendation_id: id,
  product_id: id,
  quantity: 16,
  reason: "upper 30 + safety 10 - available 24",
  risk: "HIGH",
  status: "PENDING",
  updated_at: new Date().toISOString(),
};
test.beforeEach(async ({ page }) => {
  await page.route("**/api/products", (r) =>
    r.fulfill({
      json: {
        products: [product],
        metadata: {
          source: "live",
          stale: false,
          snapshot_at: new Date().toISOString(),
          age_seconds: 0,
          reason: null,
          breaker: { state: "CLOSED", retry_after_seconds: 0, transitions: [] },
        },
      },
    }),
  );
  await page.routeWebSocket("**/ws", () => {});
  await page.route("**/api/retail/overview", (r) =>
    r.fulfill({
      json: {
        generated_at: new Date().toISOString(),
        items: [
          {
            product,
            forecast,
            stale: false,
            recommendation,
            recent_price_change: false,
          },
        ],
        summary: {
          products: 1,
          CRITICAL: 0,
          HIGH: 1,
          pending: 1,
          stale: 0,
          recent_price_changes: 0,
        },
        controls_enabled: true,
        clock: { horizon_real_seconds: 60 },
      },
    }),
  );
  await page.route("**/api/retail/scenarios", (r) =>
    r.fulfill({
      json: r.request().method() === "POST" ? { status: "QUEUED" } : [],
    }),
  );
  await page.route(`**/api/retail/products/${id}`, (r) =>
    r.fulfill({
      json: {
        product,
        history: [
          {
            bucket: Math.floor(Date.now() / 1000),
            sales: 3,
            stock: 24,
            price: 129,
          },
        ],
        forecasts: [
          {
            data: forecast,
            generated_at: forecast.generated_at,
            actual_sales: null,
          },
        ],
        pricing_decisions: [],
        history_note: "Observed snapshots, not exact per-sale state.",
      },
    }),
  );
  await page.route("**/api/retail/recommendations", (r) =>
    r.fulfill({ json: [recommendation] }),
  );
  await page.route("**/api/retail/model-health", (r) =>
    r.fulfill({ json: { status: "running", model_version: "test-model" } }),
  );
});
test("operations prioritizes risk and separates observed histories from forecasts", async ({
  page,
}) => {
  await page.goto("/");
  await expect(
    page.getByRole("heading", { name: "Needs Attention" }),
  ).toBeVisible();
  await expect(page.locator("[data-retail-product]").first()).toContainText(
    "HIGH",
  );
  await page
    .getByRole("button", { name: "View product & explanation" })
    .click();
  await expect(
    page.getByRole("region", { name: "Product detail" }),
  ).toContainText("Predicted next hour · advisory");
  await expect(
    page.getByRole("img", {
      name: "Completed sales per five simulated minutes",
    }),
  ).toBeVisible();
  await expect(
    page.getByRole("region", { name: "Product detail" }),
  ).toContainText("18–30 units");
  await page.setViewportSize({ width: 390, height: 844 });
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= innerWidth,
    ),
  ).toBeTruthy();
});
test("recommendation approval and rejection expose the audited result", async ({
  page,
}) => {
  const decisions: string[] = [];
  await page.route(`**/api/retail/recommendations/${id}/decision`, (r) => {
    const decision = r.request().postDataJSON().decision;
    decisions.push(decision);
    return r.fulfill({
      json: {
        ...recommendation,
        status: decision === "APPROVE" ? "ACCEPTED" : "REJECTED",
      },
    });
  });
  await page.goto("/?view=recommendations");
  await page.getByRole("button", { name: "Approve restock" }).click();
  await expect(
    page.getByRole("status").filter({ hasText: "ACCEPTED" }),
  ).toBeVisible();
  await page.getByRole("button", { name: "Reject", exact: true }).click();
  expect(decisions).toEqual(["APPROVE", "REJECT"]);
});
test("seeded scenario requests use protected API rather than local stock mutations", async ({
  page,
}) => {
  let body: unknown;
  await page.route("**/api/retail/scenarios", (r) => {
    if (r.request().method() === "POST") body = r.request().postDataJSON();
    return r.fulfill({
      json: r.request().method() === "POST" ? { status: "QUEUED" } : [],
    });
  });
  await page.goto("/");
  await page.getByLabel("Scenario product").selectOption(id);
  await page.getByRole("button", { name: "FLASH SALE", exact: true }).click();
  await expect(
    page.getByRole("status").filter({ hasText: "queued" }),
  ).toBeVisible();
  expect(body).toMatchObject({
    kind: "FLASH_SALE",
    product_ids: [id],
    seed: 42,
  });
});
test("analyst loading and provider failure retain verified evidence", async ({
  page,
}) => {
  await page.route("**/api/retail/analyst", async (r) => {
    await new Promise((resolve) => setTimeout(resolve, 300));
    await r.fulfill({
      json: {
        mode: "Evidence-only fallback",
        answer: "24 available units. Forecast unavailable.",
        retrieved_at: new Date().toISOString(),
        evidence: [{ tool: "get_product_details", data: { stock: 24 } }],
        error: "Provider unavailable",
      },
    });
  });
  await page.goto("/?view=analyst");
  await page
    .getByRole("button", { name: "Which products need attention?" })
    .click();
  await expect(
    page.getByRole("button", { name: "Retrieving verified facts…" }),
  ).toBeDisabled();
  await expect(page.locator(".analyst-answer")).toContainText(
    "Evidence-only fallback",
  );
  await expect(page.locator(".analyst-answer")).toContainText(
    "Forecast unavailable",
  );
});
