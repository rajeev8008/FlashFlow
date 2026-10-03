export async function GET() {
  try {
    const response = await fetch(
      `${process.env.API_INTERNAL_URL ?? "http://localhost:8000"}/metrics`,
      { cache: "no-store", signal: AbortSignal.timeout(3000) },
    );
    if (!response.ok) throw new Error("Metrics unavailable");
    return Response.json(await response.json());
  } catch {
    return Response.json({ error: "Metrics unavailable" }, { status: 503 });
  }
}
