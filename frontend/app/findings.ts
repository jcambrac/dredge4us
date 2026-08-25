export type Finding = {
  id: number;
  board: string;
  threadNo: number;
  postNo: number;
  postTime: string;
  detector: string;
  kind: string;
  matchedValue: string;
  note: string | null;
  threadSubject: string;
  threadReplies: number;
  foundAt: string;
  // Populated by the LLM classifier (lib/detect/llm.go) starting in a
  // later commit — null until then, and for regex-only findings. The UI
  // falls back to matchedValue/note when these are null.
  headline: string | null;
  rationale: string | null;
  confidence: number | null;
  rule: string | null;
  model: string | null;
};

// FindingContext is /findings/{id}/context's shape — the detail panel's
// post excerpt and sibling findings, fetched client-side on selection
// (see app/api/findings/[id]/context/route.ts).
export type FindingContext = {
  postText: string | null;
  neighbors: Finding[];
};

// archiveBoards maps a board to the FoolFuuka archive it's pulled from —
// boards not listed here are live 4chan. Keep in sync by hand with
// server/internal/config's archiveHosts + the poller's POLLER_BOARDS
// value; there's no shared source of truth between Go and the frontend
// yet. See docs/archive-sources.md.
const archiveBoards: Record<string, string> = {
  his: "https://desuarchive.org",
  k: "https://desuarchive.org",
  int: "https://desuarchive.org",
  news: "https://archive.palanq.win",
};

// threadURL returns the human-facing link for a thread (and, with
// postNo, a specific post within it): the archive's own permalink for a
// board pulled from one, live boards.4chan.org otherwise. Live links
// 404 once 4chan prunes the thread — the whole reason archive-sourced
// boards exist — so this must not always point at boards.4chan.org.
export function threadURL(board: string, threadNo: number, postNo?: number): string {
  const archive = archiveBoards[board];
  if (archive) {
    // /{board}/post/{postNo}/ redirects to the post's thread and
    // highlights it; falls back to the thread root with no post number.
    return postNo ? `${archive}/${board}/post/${postNo}/` : `${archive}/${board}/thread/${threadNo}/`;
  }
  return postNo
    ? `https://boards.4chan.org/${board}/thread/${threadNo}#p${postNo}`
    : `https://boards.4chan.org/${board}/thread/${threadNo}`;
}

// archiveNote is the short provenance note for a finding's actions row.
// Full prune-time estimation for live threads ("prunes in ~5h") needs
// thread age and lands in a later commit — until then this only
// distinguishes archived (permalink stable) from live.
export function archiveNote(board: string): string {
  return archiveBoards[board] ? "archived — permalink stable" : "live thread";
}

// sourceLabel names where a finding's post came from, for the detail
// panel's post attribution line.
export function sourceLabel(board: string): string {
  const archive = archiveBoards[board];
  return archive ? archive.replace(/^https?:\/\//, "") : "live 4chan";
}

export function apiBase(): string {
  const base = process.env.API_BASE_URL;
  if (!base) {
    throw new Error("API_BASE_URL is not set");
  }
  return base;
}

function apiToken(): string {
  const token = process.env.API_AUTH_TOKEN;
  if (!token) {
    throw new Error("API_AUTH_TOKEN is not set");
  }
  return token;
}

// apiFetch calls the poller's API server-side, with the bearer token
// every route requires (see server/internal/api's withAuth).
// API_BASE_URL/API_AUTH_TOKEN are not NEXT_PUBLIC_-prefixed on purpose —
// the browser never talks to that API directly.
export function apiFetch(path: string): Promise<Response> {
  return fetch(`${apiBase()}${path}`, {
    cache: "no-store",
    headers: { Authorization: `Bearer ${apiToken()}` },
  });
}

// getFindings calls the poller's read-only API server-side. board/kind
// are omitted to mean "no filter".
export async function getFindings(board?: string, kind?: string): Promise<Finding[]> {
  const params = new URLSearchParams({ limit: "100" });
  if (board) {
    params.set("board", board);
  }
  if (kind) {
    params.set("kind", kind);
  }

  const res = await apiFetch(`/findings?${params}`);
  if (!res.ok) {
    throw new Error(`findings request failed: ${res.status}`);
  }

  return res.json();
}

// getBoards returns the boards currently watched by the poller.
export async function getBoards(): Promise<string[]> {
  const res = await apiFetch("/boards");
  if (!res.ok) {
    throw new Error(`boards request failed: ${res.status}`);
  }

  return res.json();
}

export type BoardStatus = {
  board: string;
  title: string;
  watched: boolean;
};

// getAllBoards returns every board 4chan currently serves, each tagged
// with whether this poller watches it — unlike getBoards, which only
// lists the watched ones.
export async function getAllBoards(): Promise<BoardStatus[]> {
  const res = await apiFetch("/boards/all");
  if (!res.ok) {
    throw new Error(`all-boards request failed: ${res.status}`);
  }

  return res.json();
}

// getKinds returns the distinct finding kinds present, optionally
// scoped to board — spans every detector (regex kinds like
// "github_url", LLM classes like "ARTIFACT_DROP") and grows as
// detectors are added, so this is a live query, not a hardcoded list.
export async function getKinds(board?: string): Promise<string[]> {
  const params = new URLSearchParams();
  if (board) {
    params.set("board", board);
  }

  const res = await apiFetch(`/kinds?${params}`);
  if (!res.ok) {
    throw new Error(`kinds request failed: ${res.status}`);
  }

  return res.json();
}

export type GeneralLineage = {
  board: string;
  subjectKey: string;
  threadNo: number;
  threadSubject: string;
  replies: number;
  lastSeenAt: string;
  endedAt: string | null;
  instanceCount: number;
  firstSeenAt: string;
  findingKinds: string[];
  // Findings per instance, oldest first, capped at ~24 entries.
  instanceDensity: number[];
};

// getGenerals returns the general-thread lineages tracked for board —
// see lib/general on the Go side for the detection/stitching heuristic.
export async function getGenerals(board: string): Promise<GeneralLineage[]> {
  const res = await apiFetch(`/generals?board=${encodeURIComponent(board)}`);
  if (!res.ok) {
    throw new Error(`generals request failed: ${res.status}`);
  }

  return res.json();
}

export type KindCount = {
  kind: string;
  count: number;
};

export type SummaryWindow = {
  label: string;
  totalFindings: number;
  byKind: KindCount[];
  newGenerals: number;
};

// getSummary returns findings/generals activity across three trailing
// windows (last hour, last 24 hours, last 7 days), optionally scoped to
// board ("" or omitted means every board).
export async function getSummary(board?: string): Promise<SummaryWindow[]> {
  const params = new URLSearchParams();
  if (board) {
    params.set("board", board);
  }

  const res = await apiFetch(`/summary?${params}`);
  if (!res.ok) {
    throw new Error(`summary request failed: ${res.status}`);
  }

  return res.json();
}

export type NarrativeSummary = {
  window: "hour" | "day" | "week";
  periodStart: string;
  periodEnd: string;
  findingCount: number;
  summary: string;
  generatedAt: string;
};

// getNarrativeSummaries returns the latest LLM-generated prose summary
// per window (hour/day/week) — not board-scoped, since generation only
// covers all boards combined (see server/cmd/summarizer). Can return
// fewer than 3 entries if a window hasn't generated yet.
export async function getNarrativeSummaries(): Promise<NarrativeSummary[]> {
  const res = await apiFetch("/summary/narrative");
  if (!res.ok) {
    throw new Error(`narrative summary request failed: ${res.status}`);
  }

  return res.json();
}
