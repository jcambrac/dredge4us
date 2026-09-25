package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/jcl80/dredge4us/server/internal/store"
)

const (
	defaultRawPostsLimit = 500
	maxRawPostsLimit     = 5000
)

type rawPostsResponse struct {
	Posts []store.RawPost `json:"posts"`
	// NextAfterID is the after_id to send on the next request: the last
	// returned post's id, or the request's own after_id when nothing new
	// has settled yet — so a consumer can always just feed it back in.
	NextAfterID int64 `json:"nextAfterId"`
}

// rawPostsHandler serves raw_posts for other services to pull
// incrementally: GET /raw-posts?after_id=N[&board=g][&limit=500]. Start
// at after_id=0 and keep passing back nextAfterId; an empty page means
// caught up for now, not done — poll again later. Rows show up here
// about a minute after they're written; see store.ListRawPosts for why.
func rawPostsHandler(finder Finder) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := store.RawPostsQuery{
			Board: r.URL.Query().Get("board"),
			Limit: parseLimitWith(r.URL.Query().Get("limit"), defaultRawPostsLimit, maxRawPostsLimit),
		}
		if raw := r.URL.Query().Get("after_id"); raw != "" {
			n, err := strconv.ParseInt(raw, 10, 64)
			if err != nil || n < 0 {
				http.Error(w, "after_id must be a non-negative integer", http.StatusBadRequest)
				return
			}
			q.AfterID = n
		}

		posts, err := finder.ListRawPosts(r.Context(), q)
		if err != nil {
			slog.Error("list raw posts failed", "error", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}

		resp := rawPostsResponse{Posts: posts, NextAfterID: q.AfterID}
		if len(posts) > 0 {
			resp.NextAfterID = posts[len(posts)-1].ID
		} else {
			resp.Posts = []store.RawPost{}
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			slog.Error("encode raw posts response failed", "error", err)
		}
	}
}
