export async function GET() {
  try {
    const response = await fetch(
      `${process.env.API_INTERNAL_URL ?? "http://localhost:8000"}/inventory`,
      { cache: "no-store", signal: AbortSignal.timeout(8000) },
    );
    if (!response.ok) throw new Error("Inventory unavailable");
    return Response.json(await response.json());
  } catch {
    return Response.json({ error: "Catalog unavailable" }, { status: 503 });
  }
}
