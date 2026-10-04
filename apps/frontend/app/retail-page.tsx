"use client";
import { useEffect, useState, memo } from "react";
import { Product, startLive, useInventory } from "../lib/live";
import "./retail.css";
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
type Bucket = { bucket: number; sales: number; stock: number; price: number };
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
          <dt>Available stock</dt>
          <dd>{p.stock - p.reserved_stock}</dd>
        </div>
        <div>
          <dt>Next simulated hour</dt>
          <dd>
            {f
              ? `${Math.round(f.lower_bound)}–${Math.ceil(f.upper_bound)} units`
              : "Collecting history"}
          </dd>
        </div>
        <div>
          <dt>Estimated stockout</dt>
          <dd>
            {f?.estimated_stockout_minutes != null
              ? `~${f.estimated_stockout_minutes} sim min`
              : "Not estimated"}
          </dd>
        </div>
      </dl>
      <p className="retail-reason">
        {item.stale
          ? "Forecast unavailable or stale. Do not act on old predictions."
          : f?.risk_reason}
      </p>
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
function Chart({
  data,
  field,
  label,
}: {
  data: Bucket[];
  field: "sales" | "stock" | "price";
  label: string;
}) {
  if (!data.length) return <p>History is collecting; no observations yet.</p>;
  const max = Math.max(1, ...data.map((p) => p[field]));
  const points = data
    .map(
      (p, i) =>
        `${20 + (i / Math.max(1, data.length - 1)) * 560},${145 - (p[field] / max) * 120}`,
    )
    .join(" ");
  return (
    <figure className="retail-chart">
      <figcaption>{label} · observed</figcaption>
      <svg viewBox="0 0 600 170" role="img" aria-label={label}>
        <line x1="20" y1="145" x2="580" y2="145" stroke="#31453b" />
        <polyline
          points={points}
          fill="none"
          stroke="#b6ed85"
          strokeWidth="2.5"
        />
        <text x="20" y="165" fill="#aab9af" fontSize="11">
          {time(new Date(data[0].bucket * 1000).toISOString())}
        </text>
        <text x="490" y="165" fill="#aab9af" fontSize="11">
          {time(new Date(data.at(-1)!.bucket * 1000).toISOString())}
        </text>
      </svg>
      <small>
        Latest{" "}
        {field === "price" ? money(data.at(-1)![field]) : data.at(-1)![field]} ·
        real clock timestamps
      </small>
    </figure>
  );
}
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
        bins: 24,
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
        i.stale ||
        i.recommendation ||
        i.recent_price_change ||
        i.forecast?.risk_level !== "HEALTHY",
    );
  const selectedItem = items.find((i) => i.product.product_id === selected),
    forecast = selectedItem?.forecast;
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
            Approval records an audited decision. EXECUTED means a durable
            inventory receipt confirmed the restock.
          </p>
          <div className="recommendation-list">
            {recs.map((r) => (
              <article key={r.recommendation_id}>
                <div className="card-top">
                  <Risk value={r.risk} />
                  <span>
                    {r.status} · {time(r.updated_at)}
                  </span>
                </div>
                <h3>
                  {items.find((i) => i.product.product_id === r.product_id)
                    ?.product.name ?? r.product_id}{" "}
                  · +{r.quantity} units
                </h3>
                <p>{r.reason}</p>
                {r.operator && (
                  <small>
                    {r.operator} · {r.decision_note || "No note"}
                  </small>
                )}
                <div className="decision-buttons">
                  <button onClick={() => open(r.product_id)}>
                    Inspect product
                  </button>
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
            ))}
          </div>
          {!recs.length && <p>No recommendations yet.</p>}
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
              <div className="retail-summary">
                <div>
                  <small>Stock / reserved / available</small>
                  <strong>
                    {detail.product.stock} / {detail.product.reserved_stock} /{" "}
                    {detail.product.stock - detail.product.reserved_stock}
                  </strong>
                </div>
                <div>
                  <small>Current / previous price</small>
                  <strong>
                    {money(detail.product.current_price)} /{" "}
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
                  <p>{forecast?.interval_method}</p>
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
                    {detail.forecasts.map((f, i) => (
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
          <details>
            <summary>Model health</summary>
            <pre>{JSON.stringify(health, null, 2)}</pre>
          </details>
        </section>
      )}
      <footer>
        FLASHFLOW · Simulated data, measured engineering, explainable decisions.{" "}
        {data && `Snapshot ${time(data.generated_at)}`}
      </footer>
    </main>
  );
}
