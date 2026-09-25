// Package api implements the poller's HTTP API: findings and the
// board/coverage/summary views built on top of them, plus the raw post
// feed other services pull from (/raw-posts), as JSON. Assumed
// caller is a trusted server-side process (e.g. a Next.js route
// handler) or another app — not a browser directly, there's still no
// CORS. Every route requires a bearer token (see withAuth); there's no
// per-consumer scoping, just the one shared secret.
package api

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jcl80/dredge4us/lib/detect"
	"github.com/jcl80/dredge4us/lib/foolfuuka"
	"github.com/jcl80/dredge4us/lib/fourchan"
	"github.com/jcl80/dredge4us/server/internal/store"
)

const (
	defaultLimit = 100
	maxLimit     = 500
)

// Finder is the read surface this package needs from storage.
type Finder interface {
	ListFindings(ctx context.Context, q store.FindingsQuery) ([]store.FindingRecord, error)
	FindingContext(ctx context.Context, id int64) (*store.FindingContext, error)
	ListGenerals(ctx context.Context, board string) ([]store.GeneralLineage, error)
	ListKinds(ctx context.Context, board string) ([]string, error)
	Summary(ctx context.Context, board string) ([]store.SummaryWindow, error)
	LatestNarrativeSummaries(ctx context.Context) ([]store.NarrativeSummary, error)
	CoverageStats(ctx context.Context, boards []string) ([]store.BoardCoverageStats, error)
	ListRawPosts(ctx context.Context, q store.RawPostsQuery) ([]store.RawPost, error)
}

var _ Finder = (*store.Postgres)(nil)

// DebugStore is the write/read surface the backfill and classify
// handlers need, beyond Finder's read-only routes.
type DebugStore interface {
	SaveRawPosts(ctx context.Context, posts []store.RawPost) error
	UnclassifiedRawPosts(ctx context.Context, board string) ([]store.RawPost, error)
	MarkClassified(ctx context.Context, ids []int64) error
	SaveFindings(ctx context.Context, findings []detect.Finding) error
}

var _ DebugStore = (*store.Postgres)(nil)

// BackfillBoard is one board /debug/backfill pulls, and the archive
// client to pull it through. Client must be shared across every
// BackfillBoard on the same host — one Limiter per host, not per board,
// same rule as the scheduler's Sources. See docs/archive-sources.md.
type BackfillBoard struct {
	Board  string
	Client *foolfuuka.Client
}

// New builds the API's HTTP handler. fc serves the /boards/all board
// index — the only route that talks to 4chan directly instead of Store.
// backfillBoards/detectors feed /debug/backfill and /debug/classify;
// pass nil/empty to disable both. token is required and gates every
// route via withAuth — see New's callers for where it comes from.
func New(finder Finder, debugStore DebugStore, fc *fourchan.Client, backfillBoards []BackfillBoard, detectors []detect.Detector, token string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /findings", findingsHandler(finder))
	mux.HandleFunc("GET /findings/{id}/context", findingContextHandler(finder))
	mux.HandleFunc("GET /boards", boardsHandler())
	mux.HandleFunc("GET /boards/all", allBoardsHandler(fc))
	mux.HandleFunc("GET /coverage", coverageHandler(finder, fc))
	mux.HandleFunc("GET /generals", generalsHandler(finder))
	mux.HandleFunc("GET /kinds", kindsHandler(finder))
	mux.HandleFunc("GET /summary", summaryHandler(finder))
	mux.HandleFunc("GET /summary/narrative", narrativeSummaryHandler(finder))
	mux.HandleFunc("GET /raw-posts", rawPostsHandler(finder))
	// TEMP: verifies desuarchive/palanq are reachable (not Cloudflare-
	// challenged) from this app's real DO egress, vs. the phone hotspot
	// docs/archive-sources.md was drafted against. Remove once checked.
	mux.HandleFunc("GET /debug/archive-check", archiveCheckHandler())
	// TEMP: one-shot bulk pull via the archive search API (full board
	// history, not just the currently bumped catalog) — stores raw post
	// text, doesn't classify. Pair with /debug/classify. Remove when done.
	mux.HandleFunc("GET /debug/backfill", backfillHandler(debugStore, backfillBoards))
	// TEMP: runs detectors over whatever /debug/backfill stored and
	// hasn't been classified yet. ?board= to scope to one board.
	// Re-runnable any time, against detectors added since. Remove when
	// backfilling is done.
	mux.HandleFunc("GET /debug/classify", classifyHandler(debugStore, detectors))
	return withLogging(withAuth(token, mux))
}

// withAuth requires "Authorization: Bearer <token>" on every request,
// including /debug/*: those trigger real work (archive pulls, billed
// LLM calls) and are more dangerous unauthenticated than the read
// routes, not less. token must be non-empty — New's callers own that
// invariant, there's no "auth disabled" mode.
func withAuth(token string, next http.Handler) http.Handler {
	if token == "" {
		panic("api: withAuth called with an empty token")
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || subtle.ConstantTimeCompare([]byte(got), []byte(token)) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func archiveCheckHandler() http.HandlerFunc {
	// board must be one each archive actually carries — an unmapped
	// board 422s at the FoolFuuka app itself, which looks like a
	// failure here but has nothing to do with Cloudflare reachability.
	hosts := []struct{ base, board string }{
		{"https://desuarchive.org", "g"},
		{"https://archive.palanq.win", "news"},
		{"https://archive.4plebs.org", "pol"},
		{"https://archived.moe", "biz"},
	}
	return func(w http.ResponseWriter, r *http.Request) {
		type result struct {
			Host        string `json:"host"`
			Status      int    `json:"status"`
			CFMitigated string `json:"cf_mitigated,omitempty"`
			Error       string `json:"error,omitempty"`
		}

		results := make([]result, 0, len(hosts))
		for _, h := range hosts {
			res := result{Host: h.base}

			req, err := http.NewRequestWithContext(r.Context(), http.MethodGet,
				h.base+"/_/api/chan/index/?board="+h.board+"&page=1", nil)
			if err != nil {
				res.Error = err.Error()
				results = append(results, res)
				continue
			}
			req.Header.Set("User-Agent", foolfuuka.DefaultUserAgent)

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				res.Error = err.Error()
				results = append(results, res)
				continue
			}
			res.Status = resp.StatusCode
			res.CFMitigated = resp.Header.Get("cf-mitigated")
			_ = resp.Body.Close()

			results = append(results, res)
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(results)
	}
}

const (
	backfillDuration = 15 * time.Minute
	// backfillMaxPages caps pagination at the search API's own reachable
	// range for an unfiltered query (25 posts/page, ~5000 max — see
	// Meta.TotalFound in lib/foolfuuka). Requesting past it just returns
	// an empty page, so this is a safety cap, not a real limit hit in
	// practice.
	backfillMaxPages = 200
	// classifyDuration is a safety cap, not a real budget — LLM calls in
	// detect.Detector aren't bounded by the ctx passed in here (the
	// interface takes no context), so a large enough backlog with LLM
	// classification enabled can still run past this if each call is
	// slow; it stops issuing new work at the deadline either way.
	classifyDuration = 15 * time.Minute
)

// backfillHandler triggers a one-shot pull of each board's recent
// history via the archive search API (not just the currently bumped
// catalog) and stores the raw post text — no detection here, see
// classifyHandler for that. A burst for "get more data now", not
// something meant to run continuously. Runs in the background so the
// request returns immediately; a second call while one's in flight is
// rejected rather than running two at once against the same hosts.
// ?board= scopes to one configured board instead of all of them; ?minutes=
// overrides backfillDuration — both for quick, bounded test runs.
func backfillHandler(db DebugStore, boards []BackfillBoard) http.HandlerFunc {
	var running atomic.Bool

	return func(w http.ResponseWriter, r *http.Request) {
		selected := boards
		if want := r.URL.Query().Get("board"); want != "" {
			selected = nil
			for _, b := range boards {
				if b.Board == want {
					selected = append(selected, b)
				}
			}
			if len(selected) == 0 {
				http.Error(w, fmt.Sprintf("board %q not configured for backfill", want), http.StatusBadRequest)
				return
			}
		}
		if len(selected) == 0 {
			http.Error(w, "no backfill boards configured", http.StatusNotImplemented)
			return
		}

		duration := backfillDuration
		if raw := r.URL.Query().Get("minutes"); raw != "" {
			n, err := strconv.Atoi(raw)
			if err != nil || n <= 0 {
				http.Error(w, "minutes must be a positive integer", http.StatusBadRequest)
				return
			}
			duration = time.Duration(n) * time.Minute
		}

		if !running.CompareAndSwap(false, true) {
			http.Error(w, "backfill already running", http.StatusConflict)
			return
		}

		go func() {
			defer running.Store(false)
			ctx, cancel := context.WithTimeout(context.Background(), duration)
			defer cancel()
			runBackfill(ctx, db, selected)
		}()

		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("backfill started — check logs for progress\n"))
	}
}

func runBackfill(ctx context.Context, db DebugStore, boards []BackfillBoard) {
	started := time.Now()
	for _, b := range boards {
		if ctx.Err() != nil {
			slog.Info("backfill deadline hit, stopping early", "remaining_boards", b.Board)
			break
		}
		backfillBoard(ctx, db, b)
	}
	slog.Info("backfill complete", "elapsed", time.Since(started))
}

// backfillBoard pages through board's search results until the archive's
// own result cap, ctx's deadline, or an error stops it, and stores
// every post's full text as-is — grouping into threads and running
// detectors is classifyHandler's job, against whatever's still
// unclassified, whenever it's run.
func backfillBoard(ctx context.Context, db DebugStore, b BackfillBoard) {
	var posts []store.RawPost
	fetched := 0

	for page := 1; page <= backfillMaxPages; page++ {
		if ctx.Err() != nil {
			break
		}

		fcPosts, totalFound, err := b.Client.Search(ctx, b.Board, page)
		if err != nil {
			slog.Error("backfill search failed", "board", b.Board, "page", page, "error", err)
			break
		}
		if len(fcPosts) == 0 {
			break
		}

		for _, p := range fcPosts {
			threadNo := p.Resto
			if threadNo == 0 {
				threadNo = p.No
			}
			posts = append(posts, store.RawPost{
				Board:    b.Board,
				Source:   b.Client.BaseURL,
				ThreadNo: threadNo,
				PostNo:   p.No,
				PostTime: p.PostTime(),
				Sub:      p.Sub,
				Com:      p.Com,
				Sticky:   p.Sticky != 0,
				Closed:   p.Closed != 0,
				Archived: p.Archived != 0,
			})
		}

		fetched += len(fcPosts)
		if fetched >= totalFound {
			break
		}
	}

	// Independent of ctx and its own short timeout: ctx is the overall
	// backfill deadline, and if it expired mid-page-loop above (the
	// common case for whichever board is running when time runs out),
	// reusing it here would drop that board's posts on the floor instead
	// of saving whatever was gathered before the cutoff.
	saveCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := db.SaveRawPosts(saveCtx, posts); err != nil {
		slog.Error("backfill save raw posts failed", "board", b.Board, "error", err)
		return
	}
	slog.Info("backfill board done", "board", b.Board, "posts", len(posts))
}

// classifyHandler runs detectors over whatever /debug/backfill stored
// and no earlier classify pass has touched, optionally scoped to
// ?board=. Re-runnable any time — e.g. after adding a detector — since
// it only ever processes rows still unclassified, and marks them done
// as it goes so a second run without new backfill data is a no-op.
func classifyHandler(db DebugStore, detectors []detect.Detector) http.HandlerFunc {
	var running atomic.Bool

	return func(w http.ResponseWriter, r *http.Request) {
		if len(detectors) == 0 {
			http.Error(w, "no detectors configured", http.StatusNotImplemented)
			return
		}
		if !running.CompareAndSwap(false, true) {
			http.Error(w, "classify already running", http.StatusConflict)
			return
		}

		board := r.URL.Query().Get("board")
		go func() {
			defer running.Store(false)
			ctx, cancel := context.WithTimeout(context.Background(), classifyDuration)
			defer cancel()
			runClassify(ctx, db, detectors, board)
		}()

		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("classify started — check logs for progress\n"))
	}
}

func runClassify(ctx context.Context, db DebugStore, detectors []detect.Detector, board string) {
	raw, err := db.UnclassifiedRawPosts(ctx, board)
	if err != nil {
		slog.Error("classify: list unclassified raw posts failed", "board", board, "error", err)
		return
	}
	if len(raw) == 0 {
		slog.Info("classify: nothing unclassified", "board", board)
		return
	}

	type threadKey struct {
		board    string
		threadNo int
	}
	groups := make(map[threadKey][]store.RawPost)
	for _, p := range raw {
		k := threadKey{p.Board, p.ThreadNo}
		groups[k] = append(groups[k], p)
	}

	var findings []detect.Finding
	ids := make([]int64, 0, len(raw))
	for k, group := range groups {
		if ctx.Err() != nil {
			slog.Info("classify deadline hit, stopping early", "remaining_threads", len(groups))
			break
		}

		sub := ""
		fcPosts := make([]fourchan.Post, 0, len(group))
		for _, p := range group {
			if p.Sub != "" {
				sub = p.Sub
			}
			resto := 0
			if p.PostNo != p.ThreadNo {
				resto = p.ThreadNo
			}
			fcPosts = append(fcPosts, fourchan.Post{
				No:       p.PostNo,
				Resto:    resto,
				Time:     p.PostTime.Unix(),
				Sub:      p.Sub,
				Com:      p.Com,
				Sticky:   boolToFlag(p.Sticky),
				Closed:   boolToFlag(p.Closed),
				Archived: boolToFlag(p.Archived),
			})
			ids = append(ids, p.ID)
		}

		th := fourchan.Thread{No: k.threadNo, Sub: sub, Replies: len(group)}
		for _, d := range detectors {
			findings = append(findings, d.Detect(k.board, th, fcPosts)...)
		}
	}

	// Own short-lived context, not the classify loop's overall deadline —
	// same reason as backfillBoard's save: if time ran out mid-loop above,
	// ctx is already expired right when these two calls need to succeed.
	saveCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := db.SaveFindings(saveCtx, findings); err != nil {
		slog.Error("classify: save findings failed", "error", err)
		return
	}
	if err := db.MarkClassified(saveCtx, ids); err != nil {
		slog.Error("classify: mark classified failed", "error", err)
		return
	}
	slog.Info("classify complete", "posts", len(raw), "threads", len(groups), "findings", len(findings))
}

func boolToFlag(b bool) int {
	if b {
		return 1
	}
	return 0
}

func findingsHandler(finder Finder) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := store.FindingsQuery{
			Board: r.URL.Query().Get("board"),
			Kind:  r.URL.Query().Get("kind"),
			Limit: parseLimit(r.URL.Query().Get("limit")),
		}
		if raw := r.URL.Query().Get("since"); raw != "" {
			since, err := time.Parse(time.RFC3339, raw)
			if err != nil {
				http.Error(w, "since must be RFC3339", http.StatusBadRequest)
				return
			}
			q.Since = &since
		}

		findings, err := finder.ListFindings(r.Context(), q)
		if err != nil {
			slog.Error("list findings failed", "error", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		if findings == nil {
			findings = []store.FindingRecord{}
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(findings); err != nil {
			slog.Error("encode findings response failed", "error", err)
		}
	}
}

func findingContextHandler(finder Finder) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
		if err != nil {
			http.Error(w, "invalid finding id", http.StatusBadRequest)
			return
		}

		fc, err := finder.FindingContext(r.Context(), id)
		if err != nil {
			slog.Error("finding context failed", "id", id, "error", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		if fc == nil {
			http.Error(w, "finding not found", http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(fc); err != nil {
			slog.Error("encode finding context response failed", "error", err)
		}
	}
}

func parseLimit(raw string) int {
	return parseLimitWith(raw, defaultLimit, maxLimit)
}

func parseLimitWith(raw string, def, ceiling int) int {
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return def
	}
	if n > ceiling {
		return ceiling
	}
	return n
}

func withLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		next.ServeHTTP(w, r)
		slog.Info("request", "method", r.Method, "path", r.URL.Path, "duration", time.Since(started))
	})
}
