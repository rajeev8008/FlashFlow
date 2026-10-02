export async function GET(request: Request) {
  const id = new URL(request.url).searchParams.get("product_id");
  if (!id || !/^[0-9a-f-]{36}$/i.test(id))
    return Response.json({ error: "Invalid product ID" }, { status: 400 });
  try {
    const response = await fetch(
      `${process.env.API_INTERNAL_URL ?? "http://localhost:8000"}/pricing/decisions?product_id=${encodeURIComponent(id)}&limit=30`,
      { cache: "no-store", signal: AbortSignal.timeout(8000) },
    );
    return Response.json(await response.json(), { status: response.status });
  } catch {
    return Response.json(
      { error: "Pricing audit unavailable" },
      { status: 503 },
    );
  }
}
