// Package scheduler runs the poll loop: one ticking goroutine per watched
// board, funnelling thread fetches through a small worker pool. Each
// board's fetches go through the fourchan.Source its config selected —
// live 4chan or an archive — and every board sharing a source shares that
// source's single rate limiter; see Scheduler.Sources. This is
// deliberately an in-process queue, not the Postgres SKIP LOCKED queue
// that's the known eventual destination — Store is the seam that will
// let that swap in later without touching this package.
package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jcl80/dredge4us/lib/detect"
	"github.com/jcl80/dredge4us/lib/diff"
	"github.com/jcl80/dredge4us/lib/fourchan"
	"github.com/jcl80/dredge4us/lib/general"
	libstore "github.com/jcl80/dredge4us/lib/store"
	"github.com/jcl80/dredge4us/server/internal/config"
	pgstore "github.com/jcl80/dredge4us/server/internal/store"
)

// RawPostSaver persists every fetched post's full text. Declared here
// rather than on lib/store.Store because raw posts are a server concern
// — lib's Store is the persistence boundary lib's own packages need,
// and none of them need this.
type RawPostSaver interface {
	SaveRawPosts(ctx context.Context, posts []pgstore.RawPost) error
}

// Scheduler runs one poll loop per watched board.
type Scheduler struct {
	// Sources maps a config.Board.Source value to the fourchan.Source
	// that serves it: "" must map to the live fourchan.Client, and every
	// other key present in config's archiveHosts table must map to the
	// matching lib/foolfuuka.Client. A board whose Source has no entry
	// here is a wiring bug (config accepted a source that Sources never
	// got built for) and is skipped with a loud log rather than started.
	Sources map[string]fourchan.Source
	Store   libstore.Store
	// RawPosts, when non-nil, receives every post of every thread
	// fetched, so consumers can pull full text via the API's /raw-posts.
	// nil keeps posts on the call stack only, as before.
	RawPosts  RawPostSaver
	Detectors []detect.Detector
	Boards    []config.Board
	Workers   int
}

// Run starts one ticking goroutine per board and blocks until ctx is
// cancelled.
func (s *Scheduler) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, b := range s.Boards {
		client, ok := s.Sources[b.Source]
		if !ok {
			slog.Error("no source wired up for board, skipping", "board", b.Name, "source", b.Source)
			continue
		}

		wg.Add(1)
		go func(b config.Board, client fourchan.Source) {
			defer wg.Done()
			s.watchBoard(ctx, b, client)
		}(b, client)
	}
	wg.Wait()
}

// watchBoard runs cycles for a single board on its own ticker, against
// the source (live 4chan or an archive) its config selected. Cycles run
// synchronously within this goroutine, so a slow cycle simply delays the
// next tick rather than overlapping with it.
func (s *Scheduler) watchBoard(ctx context.Context, b config.Board, client fourchan.Source) {
	ticker := time.NewTicker(b.Interval)
	defer ticker.Stop()

	var prev map[int]fourchan.Thread
	prev = s.runCycle(ctx, b, client, prev)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			prev = s.runCycle(ctx, b, client, prev)
		}
	}
}

// sourceBase returns the host to key Store's last-modified cache under
// for a board's source — a cache-key namespace, not a URL actually
// fetched (each Source builds its own request URLs internally). This
// keeps live and archive cache entries for the same board name from
// colliding if a board's source ever changes.
func sourceBase(source string) string {
	if source == "" {
		return "https://a.4cdn.org"
	}
	return source
}

func catalogURL(source, board string) string {
	return fmt.Sprintf("%s/%s/catalog.json", sourceBase(source), board)
}

func threadURL(source, board string, no int) string {
	return fmt.Sprintf("%s/%s/thread/%d.json", sourceBase(source), board, no)
}

// runCycle fetches the board's catalog from client, diffs it against
// prev, fetches and scans whatever's new or changed, persists findings
// and cycle stats, and returns the new snapshot to become next cycle's
// prev.
func (s *Scheduler) runCycle(ctx context.Context, b config.Board, client fourchan.Source, prev map[int]fourchan.Thread) map[int]fourchan.Thread {
	board := b.Name
	stats := libstore.PollCycle{Board: board, StartedAt: time.Now()}

	url := catalogURL(b.Source, board)
	lastMod, _, err := s.Store.LastModified(ctx, url)
	if err != nil {
		slog.Error("lookup catalog last-modified failed", "board", board, "error", err)
	}

	catalog, newLastMod, err := client.FetchCatalog(ctx, board, lastMod)
	stats.Requests++

	switch {
	case errors.Is(err, fourchan.ErrNotModified):
		stats.NotModified++
		s.finishCycle(ctx, stats)
		return prev
	case errors.Is(err, fourchan.ErrGone):
		stats.Errors++
		slog.Error("board catalog gone", "board", board)
		s.finishCycle(ctx, stats)
		return prev
	case err != nil:
		stats.Errors++
		slog.Error("fetch catalog failed", "board", board, "error", err)
		s.finishCycle(ctx, stats)
		return prev
	}

	if err := s.Store.SetLastModified(ctx, url, newLastMod); err != nil {
		slog.Error("persist catalog last-modified failed", "board", board, "error", err)
	}

	curr := diff.Snapshot(catalog)
	change := diff.Compute(prev, curr)

	stats.ThreadsSeen = len(curr)
	stats.ThreadsNew = len(change.New)
	stats.ThreadsChanged = len(change.Changed)
	stats.ThreadsGone = len(change.Gone)

	s.trackGenerals(ctx, board, change, prev, stats.StartedAt)

	toFetch := make([]fourchan.Thread, 0, len(change.New)+len(change.Changed))
	toFetch = append(toFetch, change.New...)
	toFetch = append(toFetch, change.Changed...)

	findings := s.fetchAndDetect(ctx, b, client, toFetch, &stats)

	if err := s.Store.SaveFindings(ctx, findings); err != nil {
		slog.Error("save findings failed", "board", board, "error", err)
		stats.Errors++
	}

	s.finishCycle(ctx, stats)
	return curr
}

// trackGenerals updates general_threads for any new/changed thread
// whose subject matches the general heuristic, and marks tracked
// generals as ended once their thread goes gone. Runs on catalog data
// alone — no extra HTTP requests beyond the catalog fetch already done.
func (s *Scheduler) trackGenerals(ctx context.Context, board string, change diff.Change, prev map[int]fourchan.Thread, seenAt time.Time) {
	candidates := make([]fourchan.Thread, 0, len(change.New)+len(change.Changed))
	candidates = append(candidates, change.New...)
	candidates = append(candidates, change.Changed...)

	for _, t := range candidates {
		if !general.IsGeneral(t.Sub) {
			continue
		}
		g := libstore.GeneralThread{
			Board:         board,
			SubjectKey:    general.NormalizeSubject(t.Sub),
			ThreadNo:      t.No,
			ThreadSubject: t.Sub,
			Replies:       t.Replies,
			SeenAt:        seenAt,
		}
		if err := s.Store.UpsertGeneralThread(ctx, g); err != nil {
			slog.Error("upsert general thread failed", "board", board, "thread", t.No, "error", err)
		}
	}

	for _, no := range change.Gone {
		t, ok := prev[no]
		if !ok || !general.IsGeneral(t.Sub) {
			continue
		}
		if err := s.Store.EndGeneralThread(ctx, board, no, seenAt); err != nil {
			slog.Error("end general thread failed", "board", board, "thread", no, "error", err)
		}
	}
}

// fetchAndDetect fans toFetch out across the worker pool. Every fetch
// still goes through client's single shared rate limiter — the pool only
// lets the scheduler avoid blocking on each fetch in turn.
func (s *Scheduler) fetchAndDetect(ctx context.Context, b config.Board, client fourchan.Source, toFetch []fourchan.Thread, stats *libstore.PollCycle) []detect.Finding {
	workers := s.Workers
	if workers < 1 {
		workers = 1
	}

	jobs := make(chan fourchan.Thread)
	var mu sync.Mutex
	var findings []detect.Finding
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for t := range jobs {
				f, req, notMod, postsSeen, errored := s.fetchOneThread(ctx, b, client, t)
				mu.Lock()
				stats.Requests += req
				stats.NotModified += notMod
				stats.PostsSeen += postsSeen
				if errored {
					stats.Errors++
				}
				findings = append(findings, f...)
				mu.Unlock()
			}
		}()
	}

	for _, t := range toFetch {
		jobs <- t
	}
	close(jobs)
	wg.Wait()

	return findings
}

// fetchOneThread fetches, stores (if s.RawPosts is set), and scans a
// single thread. A changed thread is re-fetched in full, so most of its
// posts are already stored — SaveRawPosts skips those on (board,
// post_no).
func (s *Scheduler) fetchOneThread(ctx context.Context, b config.Board, client fourchan.Source, t fourchan.Thread) (findings []detect.Finding, requests, notModified, postsSeen int, errored bool) {
	board := b.Name
	url := threadURL(b.Source, board, t.No)

	lastMod, _, err := s.Store.LastModified(ctx, url)
	if err != nil {
		slog.Error("lookup thread last-modified failed", "board", board, "thread", t.No, "error", err)
	}

	posts, newLastMod, err := client.FetchThread(ctx, board, t.No, lastMod)
	requests = 1

	switch {
	case errors.Is(err, fourchan.ErrNotModified):
		return nil, requests, 1, 0, false
	case errors.Is(err, fourchan.ErrGone):
		slog.Info("thread gone", "board", board, "thread", t.No)
		return nil, requests, 0, 0, false
	case err != nil:
		slog.Error("fetch thread failed", "board", board, "thread", t.No, "error", err)
		return nil, requests, 0, 0, true
	}

	if err := s.Store.SetLastModified(ctx, url, newLastMod); err != nil {
		slog.Error("persist thread last-modified failed", "board", board, "thread", t.No, "error", err)
	}

	if s.RawPosts != nil {
		if err := s.RawPosts.SaveRawPosts(ctx, toRawPosts(b, t.No, posts)); err != nil {
			slog.Error("save raw posts failed", "board", board, "thread", t.No, "error", err)
			errored = true
		}
	}

	for _, d := range s.Detectors {
		findings = append(findings, d.Detect(board, t, posts)...)
	}

	return findings, requests, 0, len(posts), errored
}

func toRawPosts(b config.Board, threadNo int, posts []fourchan.Post) []pgstore.RawPost {
	source := sourceBase(b.Source)
	out := make([]pgstore.RawPost, 0, len(posts))
	for _, p := range posts {
		out = append(out, pgstore.RawPost{
			Board:    b.Name,
			Source:   source,
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
	return out
}

func (s *Scheduler) finishCycle(ctx context.Context, stats libstore.PollCycle) {
	slog.Info("poll cycle",
		"board", stats.Board,
		"threads_seen", stats.ThreadsSeen,
		"threads_new", stats.ThreadsNew,
		"threads_changed", stats.ThreadsChanged,
		"threads_gone", stats.ThreadsGone,
		"requests", stats.Requests,
		"not_modified", stats.NotModified,
		"posts_seen", stats.PostsSeen,
		"errors", stats.Errors,
	)
	if err := s.Store.SavePollCycle(ctx, stats); err != nil {
		slog.Error("save poll cycle failed", "board", stats.Board, "error", err)
	}
}
