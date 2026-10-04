import { NextRequest } from "next/server";
async function proxy(
  request: NextRequest,
  context: { params: Promise<{ path: string[] }> },
) {
  const path = (await context.params).path.join("/");
  const read =
    /^(overview|recommendations|scenarios|model-health|products\/[a-f0-9-]{36})$/.test(
      path,
    );
  const write =
    /^(scenarios|analyst|recommendations\/[a-f0-9-]{36}\/decision)$/.test(path);
  if (!(request.method === "GET" ? read : write))
    return Response.json({ error: "Not found" }, { status: 404 });
  if (request.method === "POST") {
    const host = request.headers.get("host") ?? "";
    if (
      !/^(localhost|127\.0\.0\.1|\[::1\])(?::\d{1,5})?$/i.test(host) ||
      request.headers.get("origin") !== `${request.nextUrl.protocol}//${host}`
    )
      return Response.json(
        { error: "Local same-origin controls only" },
        { status: 403 },
      );
    if (
      path !== "analyst" &&
      (process.env.APP_ENV !== "development" ||
        process.env.ENABLE_RETAIL_CONTROLS !== "true" ||
        !process.env.CHAOS_TOKEN)
    )
      return Response.json(
        { error: "Retail controls disabled" },
        { status: 404 },
      );
  }
  try {
    const body = request.method === "POST" ? await request.text() : undefined;
    if (body && body.length > 4096)
      return Response.json({ error: "Request too large" }, { status: 413 });
    const response = await fetch(
      `${process.env.API_INTERNAL_URL ?? "http://localhost:8000"}/retail/${path}`,
      {
        method: request.method,
        cache: "no-store",
        signal: AbortSignal.timeout(path === "analyst" ? 45000 : 5000),
        headers: {
          "Content-Type": "application/json",
          ...(path === "analyst"
            ? {}
            : { Authorization: `Bearer ${process.env.CHAOS_TOKEN ?? ""}` }),
        },
        body,
      },
    );
    return Response.json(await response.json(), { status: response.status });
  } catch {
    return Response.json(
      {
        error: "Retail service unavailable; inventory continues independently.",
      },
      { status: 503 },
    );
  }
}
export const GET = proxy;
export const POST = proxy;
