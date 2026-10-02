"use client";
import { memo, useEffect, useLayoutEffect, useRef, useState } from "react";
import { ResiliencePanel } from "./resilience-panel";
import {
  arrivals,
  Mode,
  Product,
  startLive,
  stats,
  useInventory,
} from "../lib/live";
function Card({ product }: { product: Product }) {
  const element = useRef<HTMLElement>(null);
  const commits = useRef(0);
  useLayoutEffect(() => {
    commits.current++;
    stats.renders++;
    element.current?.setAttribute("data-commits", String(commits.current));
    const arrival = arrivals.get(product.product_id);
    if (arrival !== undefined) {
      stats.latency += performance.now() - arrival;
      stats.samples++;
      arrivals.delete(product.product_id);
    }
  });
  const available = product.stock - product.reserved_stock;
  return (
    <article
      ref={element}
      data-product={product.product_id}
      data-version={product.version}
      data-price-version={product.price_version ?? 0}
    >
      <div className="card-top">
        <small>{product.category}</small>
        <span className={`pill ${available ? "" : "sold"}`}>
          {product.status.replaceAll("_", " ")}
        </span>
      </div>
      <h2>{product.name}</h2>
      <p className="price">${Number(product.current_price).toFixed(2)}</p>
      <p className="pricing-note">
        {product.price_direction === "UP"
          ? "↑"
          : product.price_direction === "DOWN"
            ? "↓"
            : "—"}{" "}
        {product.demand_state ?? "NORMAL"} demand ·{" "}
        {product.pricing_source ?? "RULES"}
      </p>
      <details className="pricing-note">
        <summary>
          Price decision · revision {product.price_version ?? 0}
        </summary>
        <p>{product.pricing_reason ?? "No price changes yet"}</p>
        <a
          href={`/api/pricing?product_id=${encodeURIComponent(product.product_id)}`}
          target="_blank"
          rel="noreferrer"
        >
          Inspect audit inputs and outcomes ↗
        </a>
      </details>
      <div className="inventory">
        <span>{available} available</span>
        <small>{product.reserved_stock} reserved</small>
      </div>
      <progress
        aria-label={`${product.name} available inventory`}
        value={available}
        max={Math.max(product.stock, 1)}
      />
      <div className="card-bottom">
        <span>↗ {product.sales_velocity.toFixed(1)} sales/s</span>
        <time
          dateTime={product.last_updated}
          title={`Last updated: ${product.last_updated}`}
        >
          {new Date(product.last_updated).toLocaleTimeString()}
        </time>
      </div>
      <small className="version">Version {product.version}</small>
    </article>
  );
}
function AtomicCard({ id }: { id: string }) {
  const product = useInventory((state) => state.productsById[id]);
  return <Card product={product} />;
}
const MemoizedCard = memo(AtomicCard);
function Grid({ mode }: { mode: Mode }) {
  const ids = useInventory((state) => state.ids);
  // Benchmark parent pulse: ATOMIC exposes parent-driven renders, MEMOIZED skips them.
  const [pulse, setPulse] = useState(0);
  useEffect(() => {
    const timer = setInterval(() => setPulse((n) => n + 1), 1000);
    return () => clearInterval(timer);
  }, []);
  const Component = mode === "ATOMIC" ? AtomicCard : MemoizedCard;
  return (
    <div className="grid" data-pulse={pulse}>
      {ids.map((id) => (
        <Component key={id} id={id} />
      ))}
    </div>
  );
}
function NaiveGrid() {
  const products = useInventory((state) => state.productsById);
  return (
    <div className="grid">
      {Object.values(products).map((product) => (
        <Card key={product.product_id} product={product} />
      ))}
    </div>
  );
}
function EngineeringPanel() {
  const [metrics, setMetrics] = useState({
    events: 0,
    flushes: 0,
    merged: 0,
    renders: 0,
    latency: 0,
    fps: 0,
  });
  useEffect(() => {
    let previous = { ...stats },
      started = performance.now(),
      frame = 0;
    function tick() {
      stats.frames++;
      frame = requestAnimationFrame(tick);
    }
    frame = requestAnimationFrame(tick);
    const timer = setInterval(() => {
      const now = performance.now(),
        seconds = (now - started) / 1000;
      const flushes = stats.flushes - previous.flushes,
        samples = stats.samples - previous.samples;
      setMetrics({
        events: (stats.events - previous.events) / seconds,
        flushes: flushes / seconds,
        merged: flushes ? (stats.merged - previous.merged) / flushes : 0,
        renders: (stats.renders - previous.renders) / seconds,
        latency: samples ? (stats.latency - previous.latency) / samples : 0,
        fps: (stats.frames - previous.frames) / seconds,
      });
      previous = { ...stats };
      started = now;
    }, 1000);
    return () => {
      clearInterval(timer);
      cancelAnimationFrame(frame);
    };
  }, []);
  const labels: Record<string, string> = {
    events: "Socket events/s",
    flushes: "UI flushes/s",
    merged: "Events/flush",
    renders: "Card commits/s",
    latency: "Receipt → commit ms",
    fps: "Estimated FPS",
  };
  return (
    <section className="engineering" aria-label="Engineering metrics">
      <div>
        <p className="eyebrow">UNDER THE HOOD</p>
        <h2>Rendering, measured.</h2>
        <p>One-second samples · real browser activity</p>
      </div>
      <dl>
        {Object.entries(metrics).map(([key, value]) => (
          <div key={key}>
            <dt>{labels[key]}</dt>
            <dd data-metric={key}>{value.toFixed(1)}</dd>
          </div>
        ))}
      </dl>
    </section>
  );
}
export default function Home() {
  const connection = useInventory((state) => state.connection);
  const mode = useInventory((state) => state.mode);
  const count = useInventory((state) => state.ids.length);
  useEffect(startLive, []);
  return (
    <main>
      <header>
        <a className="brand" href="/">
          F<span>↗</span> FlashFlow
        </a>
        <span
          className={`connection ${connection === "LIVE" ? "live" : ""}`}
          role="status"
          aria-live="polite"
        >
          ● {connection}
        </span>
      </header>
      <section className="hero">
        <p className="eyebrow">THE DROP IS LIVE</p>
        <h1>
          Fast sales.
          <br />
          <span>Calm interface.</span>
        </h1>
        <p>Live prices. Moving inventory. Every update, without the noise.</p>
        <div className="hero-meta">
          <span>{count} products on the floor</span>
          <span>Kafka → PostgreSQL → WebSocket</span>
        </div>
      </section>
      <EngineeringPanel />
      <ResiliencePanel />
      <section className="toolbar">
        <div>
          <h2>The live collection</h2>
          <p>Stock and prices update as events arrive.</p>
        </div>
        <label>
          Render mode
          <select
            value={mode}
            onChange={(event) =>
              useInventory.setState({ mode: event.target.value as Mode })
            }
          >
            {["NAIVE", "ATOMIC", "MEMOIZED", "BATCHED"].map((value) => (
              <option key={value}>{value}</option>
            ))}
          </select>
        </label>
      </section>
      <p className="mode-note">
        {
          {
            NAIVE: "Whole-grid subscription · immediate updates",
            ATOMIC:
              "Product subscriptions · immediate updates · unmemoized parent pulse",
            MEMOIZED:
              "Product subscriptions · memoized cards · immediate updates",
            BATCHED:
              "Product subscriptions · memoized cards · latest update per animation frame",
          }[mode]
        }
      </p>
      {count ? (
        mode === "NAIVE" ? (
          <NaiveGrid />
        ) : (
          <Grid mode={mode} />
        )
      ) : (
        <p className="empty">
          Waiting for the catalog… The connection will retry automatically.
        </p>
      )}
      <footer>
        FLASHFLOW · Performance is a feature. Metrics describe this browser, not
        backend throughput.
      </footer>
    </main>
  );
}
