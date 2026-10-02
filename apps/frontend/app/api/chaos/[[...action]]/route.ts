import { NextRequest } from "next/server";
async function proxy(
  request: NextRequest,
  context: { params: Promise<{ action?: string[] }> },
) {
  if (process.env.APP_ENV !== "development" || !process.env.CHAOS_TOKEN)
    return Response.json({ error: "Not found" }, { status: 404 });
  const host = request.headers.get("host") ?? "";
  if (!/^(localhost|127\.0\.0\.1|\[::1\])(?::\d{1,5})?$/i.test(host))
    return Response.json(
      { error: "Local development controls only" },
      { status: 403 },
    );
  const action = (await context.params).action?.join("/") ?? "";
  if (!["", "invalid-event", "disconnect"].includes(action))
    return Response.json({ error: "Not found" }, { status: 404 });
  if (
    request.method === "POST" &&
    request.headers.get("origin") !== `${request.nextUrl.protocol}//${host}`
  )
    return Response.json(
      { error: "Same-origin controls only" },
      { status: 403 },
    );
  try {
    const response = await fetch(
      `${process.env.API_INTERNAL_URL ?? "http://localhost:8000"}/dev/faults${action ? `/${action}` : ""}`,
      {
        method: request.method,
        cache: "no-store",
        signal: AbortSignal.timeout(5000),
        headers: {
          Authorization: `Bearer ${process.env.CHAOS_TOKEN}`,
          "Content-Type": "application/json",
        },
        body:
          request.method === "POST" && !action
            ? await request.text()
            : undefined,
      },
    );
    return Response.json(await response.json(), { status: response.status });
  } catch {
    return Response.json(
      { error: "Development controls unavailable" },
      { status: 503 },
    );
  }
}
export const GET = proxy;
export const POST = proxy;
