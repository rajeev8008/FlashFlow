"use client";
import { memo, useEffect, useLayoutEffect, useRef, useState } from "react";
import { ResiliencePanel } from "./resilience-panel";
import {
  arrivals,
  observedProducts,
  calibrateClock,
  clockEstimate,
  serverAlignedNow,
  recordAlignedLatency,
  alignedUiLatencies,
  flushedAt,
  clientStages,
  recordStage,
  eventArrivals,
  percentile,
  recordLatency,
  uiLatencies,
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
    if (arrival !== undefined && observedProducts.get(product.product_id) === product) {
      stats.latency += performance.now() - arrival;
      stats.samples++;
      const flushed = flushedAt.get(product.product_id);
      if (flushed !== undefined) recordStage("render", performance.now() - flushed);
      flushedAt.delete(product.product_id);
      arrivals.delete(product.product_id);
      const eventAt = eventArrivals.get(product.product_id);
      if (eventAt !== undefined) recordLatency(Date.now() - eventAt);
      const alignedNow = serverAlignedNow();
      if (eventAt !== undefined && alignedNow !== null) recordAlignedLatency(alignedNow - eventAt);
      eventArrivals.delete(product.product_id);
      observedProducts.delete(product.product_id);
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
    p50: null as number | null,
    p95: null as number | null,
    p99: null as number | null,
    browserQueue: null as number | null,
    render: null as number | null,
    socketTransport: null as number | null,
    coalesced: 0,
    alignedP50: null as number | null,
    alignedP95: null as number | null,
    alignedP99: null as number | null,
    clockOffset: null as number | null,
    clockUncertainty: null as number | null,
  });
  const [backend, setBackend] = useState<Record<string, number | null> | null>(
    null,
  );
  useEffect(() => {
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout>;
    async function poll() {
      try {
        const sentAt = Date.now();
        const response = await fetch("/api/metrics", {
          signal: AbortSignal.any([
            controller.signal,
            AbortSignal.timeout(4000),
          ]),
        });
        if (!response.ok) throw new Error("Unavailable");
        const data = await response.json();
        calibrateClock(Date.parse(data.server_now), sentAt, Date.now());
        if (!controller.signal.aborted)
          setBackend({
            kafka: data.rates?.kafka_events_per_second,
            processedRate: data.rates?.processed_events_per_second,
            producedRate: data.rates?.broker_produced_per_second,
            simulatorRate: data.simulator?.configured_rate,
            inventoryRate: data.rates?.inventory_processed_per_second,
            socketFailures: data.failed_socket_sends,
            disconnected: data.disconnected_clients,
            ...Object.fromEntries(Object.entries(data.stage_latencies ?? {}).flatMap(([stage, values]) =>
              [50, 95, 99].map((p) => [stage + p, (values as Record<string, number>)[`p${p}_ms`]]))),
            ...Object.fromEntries(Object.entries(data.lag_by_partition ?? {}).map(([key, value]) => ["partition " + key, value])),
            pricing: data.rates?.pricing_events_per_second,
            websocket: data.rates?.websocket_messages_per_second,
            lag: data.consumer_lag,
            processed: data.processed,
            duplicate: data.duplicate,
            failed: data.failed,
            dlq: data.dlq,
            clients: data.active_websocket_clients,
            cache:
              data.redis_cache_hit_rate == null
                ? null
                : data.redis_cache_hit_rate * 100,
          });
      } catch {
        if (!controller.signal.aborted) setBackend(null);
      } finally {
        if (!controller.signal.aborted) timer = setTimeout(poll, 2000);
      }
    }
    void poll();
    return () => {
      controller.abort();
      clearTimeout(timer);
    };
  }, []);
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
        p50: percentile(uiLatencies, 0.5),
        p95: percentile(uiLatencies, 0.95),
        p99: percentile(uiLatencies, 0.99),
        browserQueue: percentile(clientStages.browserQueue ?? [], .95),
        render: percentile(clientStages.render ?? [], .95),
        socketTransport: percentile(clientStages.socketTransport ?? [], .95),
        coalesced: (stats.coalesced - previous.coalesced) / seconds,
        alignedP50: serverAlignedNow() === null ? null : percentile(alignedUiLatencies, .5),
        alignedP95: serverAlignedNow() === null ? null : percentile(alignedUiLatencies, .95),
        alignedP99: serverAlignedNow() === null ? null : percentile(alignedUiLatencies, .99),
        clockOffset: serverAlignedNow() === null ? null : clockEstimate.offsetMs,
        clockUncertainty: serverAlignedNow() === null ? null : clockEstimate.uncertaintyMs,
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
    alignedP50: "Clock-aligned event to UI P50 ms",
    alignedP95: "Clock-aligned event to UI P95 ms",
    alignedP99: "Clock-aligned event to UI P99 ms",
    clockOffset: "Estimated server clock offset ms",
    clockUncertainty: "Clock estimate RTT/2 ms",
    events: "Socket events/s",
    flushes: "UI flushes/s",
    merged: "Events/flush",
    renders: "Card commits/s",
    latency: "Receipt → commit ms",
    fps: "Estimated FPS",
    p50: "Raw event to UI P50 ms",
    p95: "Raw event to UI P95 ms",
    p99: "Raw event to UI P99 ms",
    browserQueue: "Browser queue P95 ms",
    render: "Flush to commit P95 ms",
    socketTransport: "Socket transport P95 ms",
    coalesced: "Coalesced events/s",
    simulatorRate: "Simulator target events/s",
    inventoryRate: "Inventory completed/s",
    socketFailures: "Failed socket sends",
    disconnected: "Disconnected clients",
    processedRate: "Completed records/s",
    producedRate: "Broker ingress records/s",
    kafka: "Consumed Kafka records/s",
    pricing: "Pricing records/s",
    websocket: "Fanout sends/s",
    lag: "Committed consumer lag",
    processed: "Processed",
    duplicate: "Duplicates",
    failed: "Failed attempts",
    dlq: "DLQ",
    clients: "Socket clients",
    cache: "Fallback cache hits %",
  };
  return (
    <section className="engineering" aria-label="Engineering metrics">
      <div>
        <p className="eyebrow">UNDER THE HOOD</p>
        <h2>Pipeline and rendering, measured.</h2>
        <p>Browser: 1s · backend: 2s · P50/P95/P99: latest 512 commits</p>
        <p>
          Event latency needs synchronized clocks; coalesced events are not
          render samples. Clock-aligned gauges use an HTTP midpoint estimate;
          RTT/2 shows its timing uncertainty.
        </p>
      </div>
      <dl>
        {Object.entries({
          ...metrics,
          ...(backend ?? {
            kafka: null,
            lag: null,
            pricing: null,
            websocket: null,
            processed: null,
            duplicate: null,
            failed: null,
            dlq: null,
            clients: null,
            cache: null,
          }),
        }).map(([key, value]) => (
          <div key={key}>
            <dt>{labels[key] ?? (key.startsWith("partition ") ? key : key.replace(/(50|95|99)$/, " P$1 ms"))}</dt>
            <dd data-metric={key}>
              {typeof value === "number" && Number.isFinite(value)
                ? value.toFixed(1)
                : "N/A"}
            </dd>
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
