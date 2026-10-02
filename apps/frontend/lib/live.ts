import { create } from "zustand";
export type Product = {
  product_id: string;
  name: string;
  category: string;
  current_price: string;
  stock: number;
  reserved_stock: number;
  version: number;
  status: string;
  sales_velocity: number;
  last_updated: string;
};
export type Mode = "NAIVE" | "ATOMIC" | "MEMOIZED" | "BATCHED";
export type Connection =
  "CONNECTING" | "LIVE" | "RECONNECTING" | "DEGRADED" | "OFFLINE";
export function isProduct(value: unknown): value is Product {
  if (!value || typeof value !== "object") return false;
  const p = value as Record<string, unknown>;
  return (
    [
      "product_id",
      "name",
      "category",
      "current_price",
      "status",
      "last_updated",
    ].every((k) => typeof p[k] === "string" && (p[k] as string).length > 0) &&
    ["ACTIVE", "LOW_STOCK", "SOLD_OUT"].includes(p.status as string) &&
    Number(p.version) >= 1 &&
    Number.isFinite(Number(p.current_price)) &&
    Number(p.current_price) > 0 &&
    Number.isFinite(Date.parse(p.last_updated as string)) &&
    ["stock", "reserved_stock", "version"].every(
      (k) => Number.isSafeInteger(p[k]) && Number(p[k]) >= 0,
    ) &&
    Number(p.reserved_stock) <= Number(p.stock) &&
    typeof p.sales_velocity === "number" &&
    Number.isFinite(p.sales_velocity) &&
    p.sales_velocity >= 0
  );
}
export const stats = {
  events: 0,
  flushes: 0,
  merged: 0,
  renders: 0,
  latency: 0,
  samples: 0,
  frames: 0,
};
export const arrivals = new Map<string, number>();
export const useInventory = create<{
  productsById: Record<string, Product>;
  ids: string[];
  connection: Connection;
  mode: Mode;
}>(() => ({
  productsById: {},
  ids: [],
  connection: "CONNECTING",
  mode: "BATCHED",
}));
export function applyProducts(products: Product[]) {
  useInventory.setState((state) => {
    const next = { ...state.productsById };
    let ids = state.ids,
      changed = false;
    for (const p of products) {
      if (next[p.product_id] && p.version <= next[p.product_id].version)
        continue;
      if (!next[p.product_id]) ids = [...ids, p.product_id];
      next[p.product_id] = p;
      changed = true;
    }
    return changed ? { productsById: next, ids } : state;
  });
}
export function createBuffer(
  flush: (products: Product[]) => void,
  schedule: (callback: FrameRequestCallback) => number,
  cancel: (id: number) => void,
) {
  const pending = new Map<string, Product>();
  let frame: number | undefined,
    count = 0;
  function drain() {
    frame = undefined;
    if (!pending.size) return;
    stats.flushes++;
    stats.merged += count;
    flush([...pending.values()]);
    pending.clear();
    count = 0;
  }
  return {
    push(p: Product) {
      count++;
      if (
        !pending.has(p.product_id) ||
        p.version > pending.get(p.product_id)!.version
      )
        pending.set(p.product_id, p);
      if (frame === undefined) frame = schedule(drain);
    },
    drain() {
      if (frame !== undefined) cancel(frame);
      drain();
    },
    stop() {
      if (frame !== undefined) cancel(frame);
      frame = undefined;
      pending.clear();
      count = 0;
    },
  };
}
export const reconnectDelay = (attempt: number) =>
  Math.min(1000 * 2 ** Math.min(attempt, 5), 30000);
export function startLive() {
  let stopped = false,
    attempt = 0,
    generation = 0;
  let socket: WebSocket | undefined,
    retry: ReturnType<typeof setTimeout> | undefined,
    openTimeout: ReturnType<typeof setTimeout> | undefined,
    heartbeat: ReturnType<typeof setInterval> | undefined,
    controller: AbortController | undefined;
  const buffer = createBuffer(
    applyProducts,
    requestAnimationFrame,
    cancelAnimationFrame,
  );
  const unsubscribe = useInventory.subscribe((state, previous) => {
    if (state.mode !== previous.mode) buffer.drain();
  });
  const status = (connection: Connection) =>
    useInventory.setState({ connection });
  function connect() {
    if (stopped) return;
    if (!navigator.onLine) {
      status("OFFLINE");
      return;
    }
    clearInterval(heartbeat);
    clearTimeout(openTimeout);
    controller?.abort();
    status(generation ? "RECONNECTING" : "CONNECTING");
    const current = ++generation;
    const ws = new WebSocket(
      `${location.protocol === "https:" ? "wss" : "ws"}://${location.hostname}:8000/ws`,
    );
    let lastMessage = performance.now();
    socket = ws;
    openTimeout = setTimeout(() => ws.close(), 10000);
    ws.onopen = async () => {
      if (stopped || current !== generation) return;
      clearTimeout(openTimeout);
      attempt = 0;
      status("LIVE");
      lastMessage = performance.now();
      heartbeat = setInterval(() => {
        if (performance.now() - lastMessage > 45000) ws.close();
        else if (ws.readyState === WebSocket.OPEN) ws.send("pong");
      }, 10000);
      controller = new AbortController();
      try {
        const response = await fetch("/api/products", {
          signal: AbortSignal.any([
            controller.signal,
            AbortSignal.timeout(10000),
          ]),
        });
        const products: unknown = await response.json();
        if (
          !response.ok ||
          !Array.isArray(products) ||
          !products.every(isProduct)
        )
          throw new Error("Invalid catalog");
        if (
          !stopped &&
          current === generation &&
          ws.readyState === WebSocket.OPEN
        )
          applyProducts(products);
      } catch {
        if (
          !stopped &&
          current === generation &&
          ws.readyState === WebSocket.OPEN
        )
          status("DEGRADED");
      }
    };
    ws.onmessage = (event) => {
      if (stopped || current !== generation) return;
      lastMessage = performance.now();
      try {
        const message = JSON.parse(event.data);
        if (message.type === "heartbeat") {
          ws.send("pong");
          return;
        }
        if (message.type !== "product_update" || !isProduct(message.product)) {
          status("DEGRADED");
          return;
        }
        const p: Product = message.product;
        stats.events++;
        if (
          p.version <=
          (useInventory.getState().productsById[p.product_id]?.version ?? -1)
        )
          return;
        arrivals.set(p.product_id, performance.now());
        if (useInventory.getState().mode === "BATCHED") buffer.push(p);
        else {
          stats.flushes++;
          stats.merged++;
          applyProducts([p]);
        }
      } catch {
        status("DEGRADED");
      }
    };
    ws.onclose = () => {
      if (stopped || current !== generation) return;
      clearTimeout(openTimeout);
      clearInterval(heartbeat);
      controller?.abort();
      buffer.drain();
      status(navigator.onLine ? "RECONNECTING" : "OFFLINE");
      if (navigator.onLine)
        retry = setTimeout(connect, reconnectDelay(attempt++));
    };
    ws.onerror = () => ws.close();
  }
  function offline() {
    clearTimeout(retry);
    status("OFFLINE");
    socket?.close();
  }
  function online() {
    clearTimeout(retry);
    if (!socket || socket.readyState >= WebSocket.CLOSING) connect();
  }
  window.addEventListener("offline", offline);
  window.addEventListener("online", online);
  connect();
  return () => {
    stopped = true;
    generation++;
    clearTimeout(retry);
    clearTimeout(openTimeout);
    clearInterval(heartbeat);
    controller?.abort();
    socket?.close();
    buffer.stop();
    unsubscribe();
    window.removeEventListener("offline", offline);
    window.removeEventListener("online", online);
  };
}
