export const number = (v: unknown, digits = 1) =>
  typeof v === "number" && Number.isFinite(v)
    ? v.toLocaleString(undefined, { maximumFractionDigits: digits })
    : "Unavailable";
export const timestamp = (v: unknown) =>
  typeof v === "string" && Number.isFinite(Date.parse(v))
    ? new Date(v).toLocaleString()
    : "Unavailable";
export function successRate(h: Record<string, unknown>) {
  return typeof h.forecast_requests === "number" &&
    Number.isFinite(h.forecast_requests) &&
    h.forecast_requests > 0 &&
    typeof h.forecast_successes === "number" &&
    Number.isFinite(h.forecast_successes) &&
    h.forecast_successes >= 0 &&
    h.forecast_successes <= h.forecast_requests
    ? (100 * h.forecast_successes) / h.forecast_requests
    : null;
}
export function chartGeometry(values: number[], zero = false) {
  const low = zero ? 0 : Math.min(...values),
    high = Math.max(...values);
  const padding = Math.max(
    (high - low) * 0.15,
    high === low ? Math.max(Math.abs(high) * 0.001, zero ? 1 : 0.01) : 0.01,
  );
  const min = zero ? 0 : Math.max(0, low - padding),
    max = Math.max(min + 0.01, high + padding);
  return { min, max, y: (v: number) => 140 - ((v - min) / (max - min)) * 115 };
}
export type Advice = {
  status: string;
  product_id: string;
  risk: string;
  updated_at: string;
};
export function filterAdvice<T extends Advice>(
  rows: T[],
  status: string,
  product: string,
  risk: string,
) {
  return rows.filter(
    (r) =>
      (!status || r.status === status) &&
      (!product || r.product_id === product) &&
      (!risk || r.risk === risk),
  );
}
export function activeAdvice(r: Advice, now = Date.now()) {
  return (
    ["PENDING", "ACCEPTED"].includes(r.status) ||
    (r.status === "EXECUTED" && now - Date.parse(r.updated_at) < 300000)
  );
}
