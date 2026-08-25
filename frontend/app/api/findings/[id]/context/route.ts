import { NextResponse } from "next/server";
import { apiFetch } from "../../../../findings";

// Proxies the poller API's /findings/{id}/context so the browser never
// talks to it directly — API_BASE_URL/API_AUTH_TOKEN stay server-side,
// same rule as every server-fetched page in this app.
export async function GET(_req: Request, ctx: RouteContext<"/api/findings/[id]/context">) {
  const { id } = await ctx.params;
  const res = await apiFetch(`/findings/${id}/context`);
  if (!res.ok) {
    return NextResponse.json({ error: "finding context request failed" }, { status: res.status });
  }
  return NextResponse.json(await res.json());
}
