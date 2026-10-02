export async function GET() {
  const response = await fetch(`${process.env.API_INTERNAL_URL ?? "http://localhost:8000"}/products?limit=500`, { cache: "no-store" });
  if (!response.ok) return Response.json({ error: "Catalog unavailable" }, { status: 503 });
  return Response.json(await response.json());
}
