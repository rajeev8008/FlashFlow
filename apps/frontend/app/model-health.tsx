"use client";
import { useEffect, useState } from "react";
import { number, timestamp, successRate } from "../lib/retail-presentation";
export function ModelHealthCard({
  health,
}: {
  health: Record<string, unknown> | null;
}) {
  const h = health ?? {},
    rate = successRate(h),
    statuses = h.recommendations_by_status as
      Record<string, number> | undefined;
  const rows = [
    ["Status", String(h.status ?? "Loading")],
    ["Model", String(h.model_version ?? "Unavailable")],
    [
      "Feature freshness",
      typeof h.feature_age_seconds === "number"
        ? `${number(h.feature_age_seconds)} seconds old`
        : "Unavailable",
    ],
    ["Last forecast", timestamp(h.last_successful_forecast)],
    ["Forecast requests", number(h.forecast_requests, 0)],
    [
      "Forecast success rate",
      rate === null ? "Unavailable" : `${number(rate)}%`,
    ],
    ["Recent MAE", number(h.recent_mae, 2)],
    ["Recent RMSE", number(h.recent_rmse, 2)],
    ["Baseline fallbacks", number(h.baseline_fallbacks, 0)],
  ];
  return (
    <section className="model-health" aria-label="Model health">
      <h2>Model health</h2>
      <p>
        Recent errors use matured completed sales. Live performance differs from
        offline synthetic evaluation.
      </p>
      <dl className="health-grid">
        {rows.map(([label, value]) => (
          <div key={label}>
            <dt>{label}</dt>
            <dd>{value}</dd>
          </div>
        ))}
      </dl>
      <h3>Recommendations</h3>
      <dl className="health-grid">
        {[
          "PENDING",
          "ACCEPTED",
          "EXECUTED",
          "REJECTED",
          "EXPIRED",
          "SUPERSEDED",
        ].map((s) => (
          <div key={s}>
            <dt>{s.toLowerCase()}</dt>
            <dd>{statuses ? number(statuses[s] ?? 0, 0) : "Unavailable"}</dd>
          </div>
        ))}
      </dl>
      <details>
        <summary>View raw diagnostics</summary>
        <pre>{JSON.stringify(health, null, 2)}</pre>
      </details>
    </section>
  );
}
export function ModelHealth() {
  const [health, setHealth] = useState<Record<string, unknown> | null>(null);
  useEffect(() => {
    let alive = true;
    async function sample() {
      try {
        const r = await fetch("/api/retail/model-health", {
          cache: "no-store",
        });
        if (!r.ok) throw new Error();
        const h = await r.json();
        if (alive) setHealth(h);
      } catch {
        if (alive) setHealth({ status: "unavailable" });
      }
    }
    void sample();
    const timer = setInterval(sample, 5000);
    return () => {
      alive = false;
      clearInterval(timer);
    };
  }, []);
  return <ModelHealthCard health={health} />;
}
