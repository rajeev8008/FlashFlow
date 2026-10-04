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
  available_stock: 22,
  safety_stock: 10,
  max_restock: 500,
};
const recommendation = {
  recommendation_id: id,
  product_id: id,
  quantity: 16,
  reason: "upper 30 + safety 10 - available 24",
  risk: "HIGH",
  status: "PENDING",
  updated_at: new Date().toISOString(),
  forecast_data: { ...forecast, available_stock: 24 },
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

test("polished health, friendly summary and raw evidence remain inspectable", async ({
  page,
}) => {
  await page.route("**/api/retail/model-health", (r) =>
    r.fulfill({
      json: {
        status: "running",
        model_version: "test-model",
        forecast_requests: 10,
        forecast_successes: 8,
        recent_mae: 44.88,
        recent_rmse: 44.95,
        recommendations_by_status: { PENDING: 2 },
      },
    }),
  );
  await page.route("**/api/retail/analyst", (r) =>
    r.fulfill({
      json: {
        mode: "Evidence-only",
        answer: "Flash sale summary\n83 purchase attempts were recorded.",
        retrieved_at: new Date().toISOString(),
        evidence: [{ tool: "get_scenario_history", data: { attempted: 83 } }],
      },
    }),
  );
  await page.goto("/?view=analyst");
  const health = page.getByRole("region", { name: "Model health" });
  await expect(health).toContainText("80%");
  await expect(health).toContainText("44.88");
  await health.getByText("View raw diagnostics", { exact: true }).click();
  await expect(health.locator("pre")).toContainText('"forecast_requests": 10');
  await page
    .getByRole("button", {
      name: "What happened during the flash sale?",
      exact: true,
    })
    .click();
  await expect(page.locator(".analyst-answer")).toContainText(
    "Flash sale summary",
  );
  await page.getByText("Verified tool evidence (1)", { exact: true }).click();
  await expect(page.locator(".analyst-answer pre")).toContainText(
    "get_scenario_history",
  );
});
test("history filters collapse expired audit rows and show exact restock inputs", async ({
  page,
}) => {
  await page.route("**/api/retail/recommendations", (r) =>
    r.fulfill({
      json: [
        recommendation,
        { ...recommendation, recommendation_id: "old", status: "EXPIRED" },
      ],
    }),
  );
  await page.goto("/?view=recommendations");
  await expect(
    page.getByText("Active / needs action", { exact: true }),
  ).toBeVisible();
  await expect(
    page.getByText("Recommended restock: 16 units", { exact: true }).first(),
  ).toBeVisible();
  await expect(
    page.getByText("Forecast upper bound", { exact: true }).first(),
  ).toBeVisible();
  await page.getByText("Calculation details", { exact: true }).first().click();
  await expect(
    page
      .getByText("upper 30 + safety 10 - available 24", { exact: true })
      .first(),
  ).toBeVisible();
  await page.getByLabel("Status", { exact: true }).selectOption("EXPIRED");
  await expect(
    page.getByText("No matching active recommendations.", { exact: true }),
  ).toBeVisible();
  await page.locator(".history-group > summary").click();
  await expect(page.locator(".history-group")).toContainText("EXPIRED");
  await page.getByLabel("Risk level", { exact: true }).selectOption("CRITICAL");
  await expect(
    page.getByText("No recommendations match these filters.", { exact: true }),
  ).toBeVisible();
});
test("forecast snapshot and live stock are distinct with predicted trend history", async ({
  page,
}) => {
  await page.goto("/");
  await page
    .getByRole("button", { name: "View product & explanation" })
    .click();
  const detail = page.getByRole("region", { name: "Product detail" });
  await expect(detail).toContainText("Current available stock");
  await expect(detail).toContainText("Available when forecast generated");
  await expect(detail).toContainText("22");
  await expect(detail).toContainText("24");
  await expect(
    page.getByRole("img", {
      name: "Forecast trend: expected sales and prediction bounds",
    }),
  ).toBeVisible();
  await expect(detail).toContainText(
    "These are forecasts, not observed sales.",
  );
  await page
    .getByRole("button", { name: "Show full retrieved history" })
    .click();
  await expect(detail).toContainText("test-model");
  await detail.getByText("Forecast audit information", { exact: true }).click();
  await expect(detail.locator("pre").last()).toContainText(
    '"available_stock": 22',
  );
});
