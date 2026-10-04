"use client";
import { useEffect, useState, memo } from "react";
import { Product, startLive, useInventory } from "../lib/live";
import "./retail.css";
import { ModelHealthCard } from "./model-health";
import {
  Chart,
  ForecastTrend,
  RecommendationExplanation,
  Bucket,
} from "./retail-charts";
import {
  number,
  timestamp,
  filterAdvice,
  activeAdvice,
} from "../lib/retail-presentation";
type Forecast = {
  forecast_id: string;
  generated_at: string;
  expected_sales: number;
  lower_bound: number;
  upper_bound: number;
  risk_level: string;
  estimated_stockout_minutes: number | null;
  model_version: string;
  interval_method: string;
  risk_reason: string;
  fallback: boolean;
  available_stock?: number;
  safety_stock?: number;
  max_restock?: number;
  features?: number[];
};
type Recommendation = {
  recommendation_id: string;
  product_id: string;
  quantity: number;
  reason: string;
  risk: string;
  status: string;
  updated_at: string;
  operator?: string;
  decision_note?: string;
  forecast_data?: Forecast;
  product_name?: string;
  created_at?: string;
  decided_at?: string;
  published_at?: string;
  executed_at?: string;
  event_id?: string;
};
type Item = {
  product: Product;
  forecast: Forecast | null;
  stale: boolean;
  recommendation: Recommendation | null;
  recent_price_change: boolean;
};
type Overview = {
  items: Item[];
  summary: Record<string, number>;
  generated_at: string;
  controls_enabled: boolean;
  clock: { horizon_real_seconds: number };
};
type PriceDecision = {
  previous_price: string;
  applied_price: string;
  reason: string;
  signals: Record<string, string | number>;
  applied_at: string;
};
type Detail = {
  product: Product;
  history: Bucket[];
  forecasts: {
    data: Forecast;
    generated_at: string;
    actual_sales: number | null;
    forecast_id?: string;
    target_end?: string;
  }[];
  pricing_decisions: PriceDecision[];
  history_note: string;
};
type Scenario = {
  scenario_id: string;
  kind: string;
  status: string;
  data: {
    attempted: number;
    fulfilled: number;
    censored: number;
    simulated_minutes: number;
  };
};
type AnalystAnswer = {
  mode: string;
  answer: string;
  retrieved_at: string;
  evidence: unknown[];
  error?: string;
};
const views = ["operations", "products", "recommendations", "analyst"] as const;
type View = (typeof views)[number];
const time = (s: string) => new Date(s).toLocaleTimeString();
const money = (v: string | number) => `$${Number(v).toFixed(2)}`;
async function api<T>(path: string, body?: unknown): Promise<T> {
  const r = await fetch(`/api/retail/${path}`, {
    method: body ? "POST" : "GET",
    headers: { "Content-Type": "application/json" },
    body: body ? JSON.stringify(body) : undefined,
    cache: "no-store",
  });
  const data = await r.json();
  if (!r.ok) throw new Error(data.detail ?? data.error ?? "Request failed");
  return data;
}
function Risk({ value }: { value: string }) {
  return <span className={`risk risk-${value.toLowerCase()}`}>{value}</span>;
}
const RetailCard = memo(function RetailCard({
  item,
  open,
}: {
  item: Item;
  open: (id: string) => void;
}) {
  const live = useInventory((s) => s.productsById[item.product.product_id]);
  const p =
      live &&
      live.version >= item.product.version &&
      (live.price_version ?? 0) >= (item.product.price_version ?? 0)
        ? live
        : item.product,
    f = item.forecast;
  return (
    <article className="retail-card" data-retail-product={p.product_id}>
      <div className="card-top">
        <small>{p.category}</small>
        <Risk value={item.stale ? "STALE" : (f?.risk_level ?? "WARMING")} />
      </div>
      <h2>{p.name}</h2>
      <p className="retail-price">{money(p.current_price)}</p>
      <dl className="retail-facts">
        <div>
          <dt>Current available stock</dt>
          <dd>{p.stock - p.reserved_stock}</dd>
        </div>
        <div>
          <dt>Prediction range · next simulated hour</dt>
          <dd>
            {f
              ? `${Math.round(f.lower_bound)}–${Math.ceil(f.upper_bound)} units`
              : "Collecting history"}
          </dd>
        </div>
        <div>
          <dt>Expected next-hour sales</dt>
          <dd>{number(f?.expected_sales)} units</dd>
        </div>
        <div>
          <dt>Estimated stockout</dt>
          <dd>
            {f?.estimated_stockout_minutes != null
              ? `~${number(f.estimated_stockout_minutes)} sim min`
              : "Not estimated"}
          </dd>
        </div>
      </dl>
      <details className="forecast-snapshot">
        <summary>Forecast snapshot</summary>
        <p>Available when generated: {number(f?.available_stock, 0)} units</p>
        <p>
          Generated: {timestamp(f?.generated_at)} · age{" "}
          {f
            ? number(
                Math.max(0, (Date.now() - Date.parse(f.generated_at)) / 1000),
              )
            : "Unavailable"}{" "}
          real seconds
        </p>
      </details>
      <p className="retail-reason">
        {item.stale
          ? "Forecast unavailable or stale. Do not act on old predictions."
          : f?.risk_reason}
      </p>
      {f?.features && (
        <p className="action-hint">
          Recent completed sales: {number(f.features[0], 0)} in the latest bin;{" "}
          {number(f.features[1], 0)} in the preceding bin.
        </p>
      )}
      {item.recommendation && (
        <p className="action-hint">
          Review replenishment +{item.recommendation.quantity} ·{" "}
          {item.recommendation.status}
        </p>
      )}
      <button onClick={() => open(p.product_id)}>
        View product & explanation ↗
      </button>
    </article>
  );
});
export default function RetailHome() {
  const [view, setView] = useState<View>("operations"),
    [data, setData] = useState<Overview | null>(null),
    [error, setError] = useState("");
  const [selected, setSelected] = useState<string | null>(null),
    [detail, setDetail] = useState<Detail | null>(null),
    [recs, setRecs] = useState<Recommendation[]>([]),
    [scenarios, setScenarios] = useState<Scenario[]>([]);
  const [query, setQuery] = useState(""),
    [answer, setAnswer] = useState<AnalystAnswer | null>(null),
    [busy, setBusy] = useState(false),
    [action, setAction] = useState("");
  const [demoProduct, setDemoProduct] = useState(""),
    [seed, setSeed] = useState(42),
    [health, setHealth] = useState<Record<string, unknown> | null>(null);
  const [recStatus, setRecStatus] = useState(""),
    [recProduct, setRecProduct] = useState(""),
    [recRisk, setRecRisk] = useState("");
  const [fullForecastHistory, setFullForecastHistory] = useState(false);
  const selectedLive = useInventory((s) =>
    selected ? s.productsById[selected] : undefined,
  );
  const connection = useInventory((s) => s.connection);
  useEffect(startLive, []);
  useEffect(() => {
    const v = new URLSearchParams(location.search).get("view");
    if (views.includes(v as View)) setView(v as View);
  }, []);
  useEffect(() => {
    let alive = true;
    async function refresh() {
      try {
        const d = await api<Overview>("overview");
        if (alive) {
          setData(d);
          setError("");
        }
      } catch (e) {
        if (alive) setError((e as Error).message);
      }
    }
    void refresh();
    const timer = setInterval(refresh, 5000);
    return () => {
      alive = false;
      clearInterval(timer);
    };
  }, []);
  useEffect(() => {
    let alive = true;
    async function refresh() {
      try {
        if (selected) {
          const d = await api<Detail>(`products/${selected}`);
          if (alive) setDetail(d);
        }
        if (view === "recommendations") {
          const r = await api<Recommendation[]>("recommendations");
          if (alive) setRecs(r);
        }
        if (view === "operations") {
          const s = await api<Scenario[]>("scenarios");
          if (alive) setScenarios(s);
        }
        if (view === "analyst") {
          const h = await api<Record<string, unknown>>("model-health");
          if (alive) setHealth(h);
        }
      } catch (e) {
        if (alive) setAction((e as Error).message);
      }
    }
    void refresh();
    const timer = setInterval(refresh, 5000);
    return () => {
      alive = false;
      clearInterval(timer);
    };
  }, [selected, view]);
  useEffect(() => {
    if (selected && detail && view !== "analyst")
      document
        .getElementById("product-detail")
        ?.scrollIntoView({ behavior: "smooth", block: "start" });
  }, [selected, Boolean(detail), view]);
  const open = (id: string) => {
    setDetail(null);
    setSelected(id);
  };
  async function decision(r: Recommendation, value: "APPROVE" | "REJECT") {
    setBusy(true);
    try {
      const result = await api<Recommendation>(
        `recommendations/${r.recommendation_id}/decision`,
        { decision: value },
      );
      setAction(
        `${result.status}: ${value === "APPROVE" ? "worker will publish the approved restock through Kafka" : "decision recorded"}.`,
      );
      setView("recommendations");
    } catch (e) {
      setAction((e as Error).message);
    } finally {
      setBusy(false);
    }
  }
  async function scenario(kind: string) {
    setBusy(true);
    try {
      if (!demoProduct) throw new Error("Select a Retail Demo product first");
      await api("scenarios", {
        kind,
        product_ids: [demoProduct],
        seed,
        bins: kind === "FLASH_SALE" ? 36 : 24,
        demo_start_stock: kind === "FLASH_SALE" ? 80 : null,
        strength: 6,
        restock_quantity: 50,
      });
      setAction(
        `${kind.replaceAll("_", " ")} queued. Events pass through Kafka.`,
      );
    } catch (e) {
      setAction((e as Error).message);
    } finally {
      setBusy(false);
    }
  }
  async function ask(q: string) {
    setQuery(q);
    setBusy(true);
    setAnswer(null);
    try {
      setAnswer(
        await api<AnalystAnswer>("analyst", {
          question: q,
          product_id: selected,
        }),
      );
    } catch (e) {
      setAction((e as Error).message);
    } finally {
      setBusy(false);
    }
  }
  const items = (data?.items ?? []).map((i) => ({
      ...i,
      stale: i.stale || !!error || connection !== "LIVE",
    })),
    attention = items.filter(
      (i) =>
        i.stale || i.recommendation || i.forecast?.risk_level !== "HEALTHY",
    );
  const selectedItem = items.find((i) => i.product.product_id === selected),
    forecast = detail?.forecasts[0]
      ? {
          ...detail.forecasts[0].data,
          generated_at: detail.forecasts[0].generated_at,
        }
      : selectedItem?.forecast;
  const currentProduct =
    selectedLive &&
    detail &&
    selectedLive.version >= detail.product.version &&
    (selectedLive.price_version ?? 0) >= (detail.product.price_version ?? 0)
      ? selectedLive
      : detail?.product;
  const filteredRecs = filterAdvice(recs, recStatus, recProduct, recRisk);
  const activeRecs = filteredRecs.filter((r) => activeAdvice(r)),
    historyRecs = filteredRecs.filter((r) => !activeAdvice(r));
  function recommendationCard(r: Recommendation) {
    return (
      <article key={r.recommendation_id}>
        <div className="card-top">
          <Risk value={r.risk} />
          <span>
            {r.status} · {timestamp(r.updated_at)}
          </span>
        </div>
        <h3>
          {r.product_name ??
            items.find((i) => i.product.product_id === r.product_id)?.product
              .name ??
            r.product_id}
        </h3>
        <RecommendationExplanation
          quantity={r.quantity}
          reason={r.reason}
          data={r.forecast_data}
        />
        {r.operator && (
          <p>
            {r.operator} · {r.decision_note || "No note"}
          </p>
        )}
        <details>
          <summary>Action audit</summary>
          <p>
            Decision: {timestamp(r.decided_at)} · Published:{" "}
            {timestamp(r.published_at)} · Executed: {timestamp(r.executed_at)}
          </p>
          <p>Event ID: {r.event_id ?? "Not published"}</p>
          <pre>{JSON.stringify(r, null, 2)}</pre>
        </details>
        <div className="decision-buttons">
          <button onClick={() => open(r.product_id)}>Inspect product</button>
          {r.status === "PENDING" && (
            <>
              <button
                disabled={busy || !data?.controls_enabled}
                onClick={() => decision(r, "APPROVE")}
              >
                Approve restock
              </button>
              <button
                disabled={busy || !data?.controls_enabled}
                onClick={() => decision(r, "REJECT")}
              >
                Reject
              </button>
            </>
          )}
        </div>
      </article>
    );
  }
  return (
    <main className="retail-main">
      <header>
        <a className="brand" href="/">
          F<span>↗</span> FlashFlow{" "}
          <small className="brand-sub">RETAIL OPERATIONS</small>
        </a>
        <span
          className={`connection ${connection === "LIVE" ? "live" : ""}`}
          role="status"
        >
          ● {connection}
        </span>
      </header>
      <nav className="retail-nav" aria-label="Primary">
        {views.map((v) => (
          <button
            key={v}
            aria-current={view === v ? "page" : undefined}
            onClick={() => {
              setView(v);
              setSelected(null);
              history.replaceState(
                null,
                "",
                v === "operations" ? "/" : `/?view=${v}`,
              );
            }}
          >
            {v === "analyst" ? "AI Analyst" : v[0].toUpperCase() + v.slice(1)}
          </button>
        ))}
        <a href="/engineering">Engineering ↗</a>
      </nav>
      <section className="retail-hero">
        <p className="eyebrow">KNOW WHAT NEEDS ATTENTION</p>
        <h1>
          {view === "operations" ? (
            <>
              See the risk.
              <br />
              <span>Decide the next move.</span>
            </>
          ) : view === "analyst" ? (
            "Ask your retail analyst."
          ) : view === "products" ? (
            "The live product floor."
          ) : (
            "Review. Decide. Trace."
          )}
        </h1>
        <p>
          Simulated retail data · forecasts are advisory · consequential actions
          require your approval.
        </p>
        <p className="clock-note">
          Accelerated demo: 1 real minute = 1 simulated hour. Forecast: 60
          simulated minutes ({data?.clock.horizon_real_seconds ?? 60} real
          seconds).
        </p>
      </section>
      {error && (
        <p className="retail-alert" role="alert">
          {error} Last known data retained.
        </p>
      )}
      {action && (
        <p className="retail-message" role="status">
          {action}
          <button onClick={() => setAction("")} aria-label="Dismiss message">
            ×
          </button>
        </p>
      )}
      {view === "operations" && (
        <>
          <section className="retail-summary" aria-label="Operations summary">
            {[
              ["Products monitored", "products"],
              ["Critical", "CRITICAL"],
              ["High risk", "HIGH"],
              ["Pending actions", "pending"],
              ["Recent price changes", "recent_price_changes"],
              ["Forecasts warming / stale", "stale"],
            ].map(([label, key]) => (
              <div key={key}>
                <small>{label}</small>
                <strong>{data?.summary[key] ?? "—"}</strong>
              </div>
            ))}
          </section>
          <section className="scenario-panel">
            <div>
              <p className="eyebrow">REPEATABLE RETAIL DEMO</p>
              <h2>Start with a scenario.</h2>
              <p>
                Choose a dedicated demo product. Attempted demand is recorded
                separately from fulfilled sales.
              </p>
            </div>
            <div className="scenario-inputs">
              <label>
                Product
                <select
                  aria-label="Scenario product"
                  value={demoProduct}
                  onChange={(e) => setDemoProduct(e.target.value)}
                >
                  <option value="">Choose a demo product</option>
                  {items
                    .filter((i) => i.product.category === "Retail Demo")
                    .map((i) => (
                      <option
                        value={i.product.product_id}
                        key={i.product.product_id}
                      >
                        {i.product.name}
                      </option>
                    ))}
                </select>
              </label>
              <label>
                Seed
                <input
                  type="number"
                  min="0"
                  max="2147483647"
                  value={seed}
                  onChange={(e) => setSeed(Number(e.target.value))}
                />
              </label>
              <div className="scenario-buttons">
                {["NORMAL", "FLASH_SALE", "DEMAND_SPIKE", "RESTOCK"].map(
                  (k) => (
                    <button
                      key={k}
                      disabled={busy || !data?.controls_enabled || !demoProduct}
                      onClick={() => scenario(k)}
                    >
                      {k.replaceAll("_", " ")}
                    </button>
                  ),
                )}
              </div>
              {!data?.controls_enabled && (
                <small>
                  Protected demo controls disabled in production mode.
                </small>
              )}
            </div>
          </section>
          {scenarios[0] && (
            <p className="scenario-progress">
              Latest: {scenarios[0].kind} · {scenarios[0].status} ·{" "}
              {scenarios[0].data.simulated_minutes} sim min · attempted{" "}
              {scenarios[0].data.attempted} / fulfilled{" "}
              {scenarios[0].data.fulfilled} / stock-constrained{" "}
              {scenarios[0].data.censored}
            </p>
          )}
          <p>
            Flash Sale uses 12 normal warm-up bins then 24 ramped demand bins.
            Preparation tops up the selected simulated product to 80 available
            units through Kafka; it never removes excess inventory.
          </p>
          <div className="section-title">
            <div>
              <p className="eyebrow">PRIORITIZED BY BACKEND RISK</p>
              <h2>Needs Attention</h2>
            </div>
            <small>
              Forecast refresh 5s · unavailable forecasts are never classified
              healthy
            </small>
          </div>
          <section className="retail-grid">
            {attention.slice(0, 24).map((i) => (
              <RetailCard key={i.product.product_id} item={i} open={open} />
            ))}
          </section>
          <details className="recent-activity">
            <summary>
              Recent price activity ({data?.summary.recent_price_changes ?? 0})
            </summary>
            <p>Recorded price changes within the last five real minutes.</p>
            {items
              .filter((i) => i.recent_price_change)
              .slice(0, 10)
              .map((i) => (
                <p key={i.product.product_id}>
                  {i.product.name}: {i.product.pricing_reason} ·{" "}
                  {money(i.product.current_price)}
                </p>
              ))}
          </details>
          {!attention.length && (
            <p className="retail-empty">
              {data
                ? "No products require attention under the current policy."
                : "Loading the operations read model…"}
            </p>
          )}
        </>
      )}
      {view === "products" && (
        <>
          <p>
            {items.length} monitored products · inspect observed history and
            predictions.
          </p>
          <section className="retail-grid">
            {items.map((i) => (
              <RetailCard key={i.product.product_id} item={i} open={open} />
            ))}
          </section>
        </>
      )}
      {view === "recommendations" && (
        <section>
          <h2>Recommendations & action history</h2>
          <p>
            Approval is audited. EXECUTED confirms the restock has a durable
            inventory receipt. Similar advice is suppressed between meaningful
            changes.
          </p>
          <div className="history-filters">
            <label>
              Status
              <select
                aria-label="Status" value={recStatus}
                onChange={(e) => setRecStatus(e.target.value)}
              >
                <option value="">All statuses</option>
                {[
                  "PENDING",
                  "ACCEPTED",
                  "EXECUTED",
                  "REJECTED",
                  "EXPIRED",
                  "SUPERSEDED",
                ].map((v) => (
                  <option key={v}>{v}</option>
                ))}
              </select>
            </label>
            <label>
              Product
              <select
                aria-label="Product" value={recProduct}
                onChange={(e) => setRecProduct(e.target.value)}
              >
                <option value="">All products</option>
                {[...new Set(recs.map((r) => r.product_id))].map((id) => (
                  <option key={id} value={id}>
                    {items.find((i) => i.product.product_id === id)?.product
                      .name ??
                      recs.find((r) => r.product_id === id)?.product_name ??
                      id}
                  </option>
                ))}
              </select>
            </label>
            <label>
              Risk level
              <select
                aria-label="Risk level" value={recRisk}
                onChange={(e) => setRecRisk(e.target.value)}
              >
                <option value="">All risks</option>
                {["CRITICAL", "HIGH", "MEDIUM", "HEALTHY"].map((v) => (
                  <option key={v}>{v}</option>
                ))}
              </select>
            </label>
          </div>
          <h3>Active / needs action</h3>
          <p>
            Pending, accepted and executions within the last five real minutes.
          </p>
          <div className="recommendation-list">
            {activeRecs.map(recommendationCard)}
          </div>
          {!activeRecs.length && <p>No matching active recommendations.</p>}
          <h3>History</h3>
          <p>
            Up to 100 prioritized records returned by the bounded API. Older
            records remain stored for auditing.
          </p>
          {[...new Set(historyRecs.map((r) => r.product_id))].map((id) => (
            <details className="history-group" key={id}>
              <summary>
                {items.find((i) => i.product.product_id === id)?.product.name ??
                  id}{" "}
                · {historyRecs.filter((r) => r.product_id === id).length}{" "}
                historical decisions
              </summary>
              <div className="recommendation-list">
                {historyRecs
                  .filter((r) => r.product_id === id)
                  .map(recommendationCard)}
              </div>
            </details>
          ))}
          {!filteredRecs.length && (
            <p>No recommendations match these filters.</p>
          )}
        </section>
      )}
      {selected && view !== "analyst" && (
        <section
          className="product-detail"
          id="product-detail"
          aria-label="Product detail"
        >
          <div className="section-title">
            <h2>{detail?.product.name ?? "Loading product…"}</h2>
            <button onClick={() => setSelected(null)}>Close details</button>
          </div>
          {detail && (
            <>
              <p>
                Current status: {currentProduct!.status.replaceAll("_", " ")} ·
                inventory updated {timestamp(currentProduct!.last_updated)}
              </p>
              <div className="retail-summary">
                <div>
                  <small>Stock / reserved / available</small>
                  <strong>
                    {currentProduct!.stock} / {currentProduct!.reserved_stock} /{" "}
                    {currentProduct!.stock - currentProduct!.reserved_stock}
                  </strong>
                </div>
                <div>
                  <small>Current / previous price</small>
                  <strong>
                    {money(currentProduct!.current_price)} /{" "}
                    {detail.pricing_decisions[0]
                      ? money(detail.pricing_decisions[0].previous_price)
                      : "—"}
                  </strong>
                </div>
                <div>
                  <small>Forecast next simulated hour</small>
                  <strong>
                    {forecast ? `${forecast.expected_sales} units` : "Warming"}
                  </strong>
                </div>
                <div>
                  <small>Risk at last forecast</small>
                  <Risk
                    value={
                      selectedItem?.stale
                        ? "STALE"
                        : (forecast?.risk_level ?? "WARMING")
                    }
                  />
                </div>
              </div>
              <h3>Forecast · advisory</h3>
              <p>
                Estimated stockout:{" "}
                {forecast?.estimated_stockout_minutes == null
                  ? "Not estimated"
                  : `${number(forecast.estimated_stockout_minutes)} simulated minutes`}{" "}
                · assumes a constant forecast rate with no inbound stock.
              </p>
              <dl className="health-grid">
                <div>
                  <dt>Current available stock</dt>
                  <dd>
                    {number(
                      currentProduct!.stock - currentProduct!.reserved_stock,
                      0,
                    )}
                  </dd>
                </div>
                <div>
                  <dt>Available when forecast generated</dt>
                  <dd>{number(forecast?.available_stock, 0)}</dd>
                </div>
                <div>
                  <dt>Forecast generated</dt>
                  <dd>{timestamp(forecast?.generated_at)}</dd>
                </div>
                <div>
                  <dt>Data age</dt>
                  <dd>
                    {forecast
                      ? `${number(Math.max(0, (Date.now() - Date.parse(forecast.generated_at)) / 1000))} real seconds`
                      : "Unavailable"}
                  </dd>
                </div>
              </dl>
              <h3>Recommended action</h3>
              {selectedItem?.recommendation ? (
                <RecommendationExplanation
                  quantity={selectedItem.recommendation.quantity}
                  reason={selectedItem.recommendation.reason}
                  data={
                    selectedItem.recommendation.forecast_data ??
                    detail.forecasts.find(
                      (f) =>
                        f.forecast_id ===
                        (
                          selectedItem.recommendation as Recommendation & {
                            forecast_id?: string;
                          }
                        ).forecast_id,
                    )?.data
                  }
                />
              ) : (
                <p>
                  No active replenishment recommendation. Check history for
                  previous decisions.
                </p>
              )}
              {selectedItem?.recommendation?.status === "PENDING" && (
                <div className="decision-buttons">
                  <p>
                    Review +{selectedItem.recommendation.quantity} replenishment
                  </p>
                  <button
                    disabled={
                      busy || selectedItem.stale || !data?.controls_enabled
                    }
                    onClick={() =>
                      decision(selectedItem.recommendation!, "APPROVE")
                    }
                  >
                    Approve restock
                  </button>
                  <button
                    disabled={busy || !data?.controls_enabled}
                    onClick={() =>
                      decision(selectedItem.recommendation!, "REJECT")
                    }
                  >
                    Reject
                  </button>
                </div>
              )}
              <h3>Observed history</h3>
              <div className="detail-charts">
                <Chart
                  data={detail.history}
                  field="sales"
                  label="Completed sales per five simulated minutes"
                />
                <Chart
                  data={detail.history}
                  field="stock"
                  label="Observed stock snapshots"
                />
                <Chart
                  data={detail.history}
                  field="price"
                  label="Observed price snapshots"
                />
                <figure className="forecast-figure">
                  <figcaption>Predicted next hour · advisory</figcaption>
                  <strong>
                    {forecast
                      ? `${Math.round(forecast.lower_bound)}–${Math.ceil(forecast.upper_bound)} units`
                      : "Collecting twelve completed buckets"}
                  </strong>
                  <p>
                    Expected sales: {number(forecast?.expected_sales)} units
                  </p>
                  <p>
                    Prediction range reflects uncertainty; it is not guaranteed.
                  </p>
                  <details>
                    <summary>Interval method</summary>
                    <p>{forecast?.interval_method}</p>
                  </details>
                  <p>{forecast?.risk_reason}</p>
                  <small>
                    Forecast{" "}
                    {forecast ? time(forecast.generated_at) : "unavailable"} ·{" "}
                    {forecast?.model_version} ·{" "}
                    {selectedItem?.stale ? "STALE" : "current"}
                  </small>
                </figure>
              </div>
              <small>{detail.history_note}</small>
              <h3>Why did the price change?</h3>
              {detail.pricing_decisions[0] ? (
                <>
                  <p>
                    {detail.pricing_decisions[0].reason} ·{" "}
                    {money(detail.pricing_decisions[0].previous_price)} →{" "}
                    {money(detail.pricing_decisions[0].applied_price)} at{" "}
                    {time(detail.pricing_decisions[0].applied_at)}
                  </p>
                  <details>
                    <summary>Stored pricing signals</summary>
                    <pre>
                      {JSON.stringify(
                        detail.pricing_decisions[0].signals,
                        null,
                        2,
                      )}
                    </pre>
                  </details>
                </>
              ) : (
                <p>
                  {detail.product.pricing_reason} · no applied price-change
                  audit yet.
                </p>
              )}
              <h3>Forecast history</h3>
              <ForecastTrend rows={detail.forecasts} />
              <p>
                Latest{" "}
                {fullForecastHistory
                  ? detail.forecasts.length
                  : Math.min(6, detail.forecasts.length)}{" "}
                predictions · up to 120 available here; all records remain
                stored.
              </p>
              <button onClick={() => setFullForecastHistory((v) => !v)}>
                {fullForecastHistory
                  ? "Show compact history"
                  : "Show full retrieved history"}
              </button>
              <div className="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Generated</th>
                      <th>Expected</th>
                      <th>Range</th>
                      <th>Actual after horizon</th>
                      <th>Model</th>
                    </tr>
                  </thead>
                  <tbody>
                    {(fullForecastHistory
                      ? detail.forecasts
                      : detail.forecasts.slice(0, 6)
                    ).map((f, i) => (
                      <tr key={i}>
                        <td>{time(f.generated_at)}</td>
                        <td>{f.data.expected_sales}</td>
                        <td>
                          {f.data.lower_bound}–{f.data.upper_bound}
                        </td>
                        <td>{f.actual_sales ?? "Pending / not mature"}</td>
                        <td>{f.data.model_version}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              <details>
                <summary>Forecast audit information</summary>
                <pre>
                  {JSON.stringify(
                    detail.forecasts[0] ?? { status: "Unavailable" },
                    null,
                    2,
                  )}
                </pre>
              </details>
            </>
          )}
        </section>
      )}
      {view === "analyst" && (
        <section className="analyst-panel">
          <p>
            Read-only analyst. Backend tools supply facts and timestamps.
            Provider failure cannot stop inventory processing.
          </p>
          <div className="question-chips">
            {[
              "Which products need attention?",
              "What happened during the flash sale?",
              "Which recommendations are pending?",
              "How healthy is FlashFlow?",
            ].map((q) => (
              <button disabled={busy} key={q} onClick={() => ask(q)}>
                {q}
              </button>
            ))}
          </div>
          <label>
            Optional product context
            <select
              aria-label="Analyst product context"
              value={selected ?? ""}
              onChange={(e) => setSelected(e.target.value || null)}
            >
              <option value="">All products</option>
              {items.map((i) => (
                <option value={i.product.product_id} key={i.product.product_id}>
                  {i.product.name}
                </option>
              ))}
            </select>
          </label>
          <form
            onSubmit={(e) => {
              e.preventDefault();
              void ask(query);
            }}
          >
            <label htmlFor="analyst-question">Your question</label>
            <textarea
              id="analyst-question"
              value={query}
              maxLength={1000}
              onChange={(e) => setQuery(e.target.value)}
              required
            />
            <button disabled={busy}>
              {busy ? "Retrieving verified facts…" : "Ask analyst"}
            </button>
          </form>
          {answer && (
            <article className="analyst-answer">
              <small>
                {answer.mode} · retrieved {time(answer.retrieved_at)}
              </small>
              <p style={{ whiteSpace: "pre-wrap" }}>{answer.answer}</p>
              {answer.error && <p role="alert">{answer.error}</p>}
              <details>
                <summary>
                  Verified tool evidence ({answer.evidence.length})
                </summary>
                <pre>{JSON.stringify(answer.evidence, null, 2)}</pre>
              </details>
            </article>
          )}
          <ModelHealthCard health={health} />
        </section>
      )}
      <footer>
        FLASHFLOW · Simulated data, measured engineering, explainable decisions.{" "}
        {data && `Snapshot ${time(data.generated_at)}`}
      </footer>
    </main>
  );
}
