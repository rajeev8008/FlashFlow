"use client";

import { useEffect, useState } from "react";

type Product = {
  product_id: string; name: string; category: string; current_price: string;
  stock: number; reserved_stock: number; version: number; status: string;
};

function isProduct(value: unknown): value is Product {
  if (!value || typeof value !== "object") return false;
  const p = value as Record<string, unknown>;
  return ["product_id", "name", "category", "current_price", "status"].every(key => typeof p[key] === "string")
    && ["stock", "reserved_stock", "version"].every(key => typeof p[key] === "number" && Number.isInteger(p[key]) && (p[key] as number) >= 0)
    && (p.reserved_stock as number) <= (p.stock as number);
}

export default function Home() {
  const [products, setProducts] = useState<Record<string, Product>>({});
  const [status, setStatus] = useState("CONNECTING");
  useEffect(() => {
    let stopped = false;
    let socket: WebSocket;
    let retry: ReturnType<typeof setTimeout>;
    let heartbeat: ReturnType<typeof setInterval>;
    function merge(items: Product[]) {
      setProducts(previous => {
        const next = { ...previous };
        for (const item of items) if (!next[item.product_id] || next[item.product_id].version <= item.version) next[item.product_id] = item;
        return next;
      });
    }
    function connect() {
      socket = new WebSocket(`${location.protocol === "https:" ? "wss" : "ws"}://${location.hostname}:8000/ws`);
      socket.onopen = async () => {
        setStatus("LIVE");
        heartbeat = setInterval(() => { if (socket.readyState === WebSocket.OPEN) socket.send("pong"); }, 10000);
        try {
          const response = await fetch("/api/products");
          if (!response.ok) throw new Error("Catalog unavailable");
          const catalog: unknown = await response.json();
          if (!Array.isArray(catalog) || !catalog.every(isProduct)) throw new Error("Invalid catalog");
          if (!stopped) merge(catalog);
        } catch { if (!stopped) setStatus("CATALOG UNAVAILABLE"); }
      };
      socket.onmessage = event => {
        try {
          const message = JSON.parse(event.data);
          if (message.type === "heartbeat") socket.send("pong");
          else if (message.type === "product_update" && isProduct(message.product)) merge([message.product]);
        } catch { setStatus("INVALID UPDATE"); }
      };
      socket.onclose = () => {
        clearInterval(heartbeat);
        if (!stopped) { setStatus("RECONNECTING"); retry = setTimeout(connect, 2000); }
      };
      socket.onerror = () => socket.close();
    }
    connect();
    return () => { stopped = true; clearTimeout(retry); clearInterval(heartbeat); socket.close(); };
  }, []);
  return <main><p className="eyebrow">FLASHFLOW · LIVE INVENTORY</p><h1>FlashFlow</h1>
    <p role="status" aria-live="polite">{status} · {Object.keys(products).length} products</p>
    <div className="grid">{Object.values(products).map(product => <article key={product.product_id}>
      <small>{product.category}</small><h2>{product.name}</h2><p>${Number(product.current_price).toFixed(2)}</p>
      <p>Available: {product.stock - product.reserved_stock} · Reserved: {product.reserved_stock}</p>
      <small>{product.status} · Version {product.version}</small>
    </article>)}</div></main>;
}
