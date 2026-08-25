import { NextRequest, NextResponse } from "next/server";
import { apiFetch } from "../../findings";

// Proxies /findings so the client-side live-poll (findings-panel.tsx)
// never needs API_BASE_URL/API_AUTH_TOKEN — same reasoning as every
// other fetch here.
export async function GET(req: NextRequest) {
  const incoming = new URL(req.url).searchParams;
  const params = new URLSearchParams();
  for (const key of ["since", "board", "kind", "limit"]) {
    const value = incoming.get(key);
    if (value) params.set(key, value);
  }

  const res = await apiFetch(`/findings?${params}`);
  if (!res.ok) {
    return NextResponse.json({ error: "findings request failed" }, { status: res.status });
  }
  return NextResponse.json(await res.json());
}
