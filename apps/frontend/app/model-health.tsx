"use client";
import { useEffect, useState } from "react";
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
  return (
    <details className="model-health">
      <summary>
        Advisory model health · {String(health?.status ?? "loading")}
      </summary>
      <p>
        Independent Kafka observer · 60 simulated minute horizon · recent errors
        use matured observed sales, not latent demand.
      </p>
      <pre>{JSON.stringify(health, null, 2)}</pre>
    </details>
  );
}
