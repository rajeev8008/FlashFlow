"use client";
import { useEffect, useState } from "react";
import { useInventory } from "../lib/live";
type Faults = {
  inventory_unavailable: boolean;
  redis_unavailable: boolean;
  consumer_paused: boolean;
  latency_ms: number;
};
const reset: Faults = {
  inventory_unavailable: false,
  redis_unavailable: false,
  consumer_paused: false,
  latency_ms: 0,
};
export function ResiliencePanel() {
  const metadata = useInventory((state) => state.metadata);
  const connection = useInventory((state) => state.connection);
  const staleSince = useInventory((state) => state.staleSince);
  const lastSnapshotAt = useInventory((state) => state.lastSnapshotAt);
  const count = useInventory((state) => state.ids.length);
  const [now, setNow] = useState(0);
  const [faults, setFaults] = useState<Faults | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  useEffect(() => {
    const timer = setInterval(() => setNow(Date.now()), 1000);
    let stopped = false;
    async function load() {
      try {
        const response = await fetch("/api/chaos", {
          signal: AbortSignal.timeout(5000),
        });
        if (response.ok && !stopped) setFaults((await response.json()).faults);
      } catch {
        /* Controls stay hidden unless development access is confirmed. */
      }
    }
    void load();
    return () => {
      stopped = true;
      clearInterval(timer);
    };
  }, []);
  async function control(action = "", update?: Faults) {
    const previous = faults;
    if (update) setFaults(update);
    setBusy(true);
    setError("");
    try {
      const response = await fetch(`/api/chaos${action ? `/${action}` : ""}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: update ? JSON.stringify(update) : undefined,
        signal: AbortSignal.timeout(6000),
      });
      if (!response.ok)
        throw new Error(
          "Control failed. Check development access and backend availability.",
        );
      const result = await response.json();
      if (result.faults) setFaults(result.faults);
    } catch (e) {
      if (update) setFaults(previous);
      setError(e instanceof Error ? e.message : "Control failed");
    } finally {
      setBusy(false);
    }
  }
  const stale = count > 0 && connection !== "LIVE";
  const reference = lastSnapshotAt ? Date.parse(lastSnapshotAt) : staleSince;
  const age = reference
    ? Math.max(0, ((now || Date.now()) - reference) / 1000)
    : null;
  return (
    <section className="resilience" aria-label="Resilience controls">
      <div
        className={`freshness ${stale ? "stale" : ""}`}
        role="region"
        aria-label="Data freshness"
      >
        <div>
          <strong>
            {stale ? "Stale data · dashboard retained" : "Inventory health"}
          </strong>
          <p>
            {stale
              ? (metadata?.reason ??
                "Live delivery interrupted; showing last known data.")
              : connection === "LIVE"
                ? "Live updates and durable snapshots are connected."
                : "Waiting for verified live inventory."}
          </p>
        </div>
        <div>
          <span>Source: {metadata?.source ?? "waiting"}</span>
          <span data-breaker> Circuit: {metadata?.breaker.state ?? "—"}</span>
          <small>
            {metadata?.breaker.transitions
              .slice(-4)
              .map((event) => `${event.from} → ${event.to}`)
              .join(" · ")}
          </small>
          {stale && (
            <span data-stale-age>
              Last known snapshot:{" "}
              {age === null ? "age unknown" : `${Math.floor(age)}s old`}
            </span>
          )}
        </div>
      </div>
      {faults && (
        <details className="chaos">
          <summary>Development lab · fault controls</summary>
          <p>
            Process-local simulations. Reset restores all controls; recovery
            probes close the circuit automatically.
          </p>
          <fieldset disabled={busy}>
            {(
              [
                ["inventory_unavailable", "Inventory read failure"],
                ["redis_unavailable", "Redis unavailable"],
                ["consumer_paused", "Pause consumer"],
              ] as const
            ).map(([key, label]) => (
              <label key={key}>
                <input
                  type="checkbox"
                  checked={faults[key]}
                  onChange={(event) =>
                    void control("", { ...faults, [key]: event.target.checked })
                  }
                />
                {label}
              </label>
            ))}
            <label>
              Backend latency
              <select
                value={faults.latency_ms}
                onChange={(event) =>
                  void control("", {
                    ...faults,
                    latency_ms: Number(event.target.value),
                  })
                }
              >
                {[0, 500, 3000].map((ms) => (
                  <option key={ms} value={ms}>
                    {ms} ms
                  </option>
                ))}
              </select>
            </label>
            <button onClick={() => void control("invalid-event")}>
              Inject invalid event
            </button>
            <button onClick={() => void control("disconnect")}>
              Interrupt WebSockets
            </button>
            <button onClick={() => void control("", reset)}>
              Reset faults
            </button>
          </fieldset>
          <p role="alert">
            {error ||
              (busy
                ? "Applying control…"
                : "Development controls enabled. Tokens stay on the server.")}
          </p>
        </details>
      )}
    </section>
  );
}
