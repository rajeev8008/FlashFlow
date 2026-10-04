"use client";
import { chartGeometry, number, timestamp } from "../lib/retail-presentation";
export type Bucket = {
  bucket: number;
  sales: number;
  stock: number;
  reserved?: number;
  price: number;
};
const time = (seconds: number) => new Date(seconds * 1000).toLocaleTimeString();
export function Chart({
  data,
  field,
  label,
}: {
  data: Bucket[];
  field: "sales" | "stock" | "price";
  label: string;
}) {
  if (!data.length) return <p>History is collecting; no observations yet.</p>;
  const values = data.map((p) => p[field]),
    axis = chartGeometry(values, field === "sales");
  const x = (i: number) => 70 + (i / Math.max(1, data.length - 1)) * 500;
  const points = values.map((v, i) => `${x(i)},${axis.y(v)}`).join(" ");
  return (
    <figure className="retail-chart">
      <figcaption>{label} · observed</figcaption>
      <svg viewBox="0 0 600 175" role="img" aria-label={label}>
        <line x1="70" y1="140" x2="570" y2="140" stroke="#31453b" />
        <text x="2" y="25" fill="#aab9af" fontSize="11">
          {number(axis.max, field === "price" ? 2 : 0)}
        </text>
        <text x="2" y="140" fill="#aab9af" fontSize="11">
          {number(axis.min, field === "price" ? 2 : 0)}
        </text>
        <polyline
          points={points}
          fill="none"
          stroke="#b6ed85"
          strokeWidth="2.5"
        />
        {data.map((p, i) => (
          <circle
            key={p.bucket}
            cx={x(i)}
            cy={axis.y(p[field])}
            r="5"
            fill="transparent"
            stroke="transparent"
            tabIndex={0}
          >
            <title>
              {timestamp(new Date(p.bucket * 1000).toISOString())}: {field}{" "}
              {p[field]}
              {field === "stock"
                ? `; reserved ${p.reserved ?? "unavailable"}; available ${p.reserved === undefined ? "unavailable" : p.stock - p.reserved}`
                : field === "price"
                  ? `; change from previous ${i ? (p.price - data[i - 1].price).toFixed(2) : "unavailable"}`
                  : " completed units"}
            </title>
          </circle>
        ))}
        <text x="70" y="165" fill="#aab9af" fontSize="11">
          {time(data[0].bucket)}
        </text>
        <text x="480" y="165" fill="#aab9af" fontSize="11">
          {time(data.at(-1)!.bucket)}
        </text>
      </svg>
      <small>
        Y-axis: {field === "price" ? "USD" : "units"}
        {field !== "sales" && " · range around observed values"}. Hover or focus
        a point for exact values.
      </small>
      <p>
        Latest {number(values.at(-1), field === "price" ? 2 : 0)}
        {field === "stock" &&
          ` · Change over window: ${number(values.at(-1)! - values[0], 0)} units`}
      </p>
    </figure>
  );
}
type Prediction = {
  generated_at: string;
  data: { expected_sales: number; lower_bound: number; upper_bound: number };
};
export function ForecastTrend({ rows }: { rows: Prediction[] }) {
  const data = [...rows].reverse();
  if (!data.length) return <p>No forecast history yet.</p>;
  const axis = chartGeometry(
    data.flatMap((p) => [p.data.lower_bound, p.data.upper_bound]),
    true,
  );
  const points = (key: "expected_sales" | "lower_bound" | "upper_bound") =>
    data
      .map(
        (p, i) =>
          `${60 + (i / Math.max(1, data.length - 1)) * 510},${axis.y(p.data[key])}`,
      )
      .join(" ");
  return (
    <figure className="forecast-figure">
      <figcaption>Forecast trend · predicted · advisory</figcaption>
      <p>
        Expected sales, lower and upper prediction bounds for each future
        simulated hour.
      </p>
      <svg
        viewBox="0 0 600 175"
        role="img"
        aria-label="Forecast trend: expected sales and prediction bounds"
      >
        <text x="0" y="25" fill="#aab9af" fontSize="11">
          {number(axis.max, 0)} units
        </text>
        {(["lower_bound", "upper_bound", "expected_sales"] as const).map(
          (k) => (
            <polyline
              key={k}
              points={points(k)}
              fill="none"
              stroke={k === "expected_sales" ? "#8ccfff" : "#698fa4"}
              strokeWidth="2"
              strokeDasharray={k === "expected_sales" ? undefined : "5 5"}
            />
          ),
        )}
        <text x="60" y="165" fill="#aab9af" fontSize="11">
          {new Date(data[0].generated_at).toLocaleTimeString()}
        </text>
        <text x="480" y="165" fill="#aab9af" fontSize="11">
          {new Date(data.at(-1)!.generated_at).toLocaleTimeString()}
        </text>
        {data.map((p, i) => (
          <circle
            key={i}
            cx={60 + (i / Math.max(1, data.length - 1)) * 510}
            cy={axis.y(p.data.expected_sales)}
            r="5"
            fill="transparent"
            tabIndex={0}
          >
            <title>
              {timestamp(p.generated_at)}: expected {p.data.expected_sales};
              lower {p.data.lower_bound}; upper {p.data.upper_bound} units
            </title>
          </circle>
        ))}
      </svg>
      <small>
        Solid blue: expected sales · dashed: prediction bounds. These are
        forecasts, not observed sales.
      </small>
    </figure>
  );
}
export function RecommendationExplanation({
  quantity,
  reason,
  data,
}: {
  quantity: number;
  reason: string;
  data?: {
    upper_bound?: number;
    available_stock?: number;
    safety_stock?: number;
    max_restock?: number;
  } | null;
}) {
  return (
    <div className="advice-explanation">
      <p>
        <strong>Recommended restock: {number(quantity, 0)} units</strong>
      </p>
      <p>
        The replenishment policy adds inventory to cover the upper sales
        estimate and safety stock, within the configured limit.
      </p>
      <dl className="retail-facts">
        <div>
          <dt>Forecast upper bound</dt>
          <dd>{number(data?.upper_bound, 2)} units</dd>
        </div>
        <div>
          <dt>Available at forecast</dt>
          <dd>{number(data?.available_stock, 0)} units</dd>
        </div>
        <div>
          <dt>Safety stock target</dt>
          <dd>{number(data?.safety_stock, 0)} units</dd>
        </div>
      </dl>
      <details>
        <summary>Calculation details</summary>
        <p>{reason}</p>
        <p>
          Maximum allowed: {number(data?.max_restock, 0)} units. Stored
          recommendation: {quantity} units.
        </p>
      </details>
    </div>
  );
}
