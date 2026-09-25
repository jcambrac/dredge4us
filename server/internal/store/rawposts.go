package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// RawPost is one post's full text — the findings table's opposite:
// everything, not just what a detector flagged. Written by the poller
// for every post it fetches, and by the archive backfill. See
// migrations/0005_raw_posts.sql for why this table exists at all. ID and
// FetchedAt are unset on insert (DB-assigned); Source records where the
// post was fetched from (live 4chan's API host, or an archive's base
// URL), informational only.
type RawPost struct {
	ID        int64     `json:"id"`
	Board     string    `json:"board"`
	Source    string    `json:"source"`
	ThreadNo  int       `json:"threadNo"`
	PostNo    int       `json:"postNo"`
	PostTime  time.Time `json:"postTime"`
	Sub       string    `json:"sub"`
	Com       string    `json:"com"`
	Sticky    bool      `json:"sticky"`
	Closed    bool      `json:"closed"`
	Archived  bool      `json:"archived"`
	FetchedAt time.Time `json:"fetchedAt"`
}

// SaveRawPosts inserts posts, skipping any (board, post_no) already
// stored — a re-run of the same backfill is a no-op past the first time.
func (p *Postgres) SaveRawPosts(ctx context.Context, posts []RawPost) error {
	if len(posts) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	for _, rp := range posts {
		batch.Queue(`
			INSERT INTO raw_posts
				(board, source, thread_no, post_no, post_time, sub, com, sticky, closed, archived)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
			ON CONFLICT (board, post_no) DO NOTHING
		`, rp.Board, rp.Source, rp.ThreadNo, rp.PostNo, rp.PostTime, nullableText(rp.Sub), rp.Com, rp.Sticky, rp.Closed, rp.Archived)
	}

	br := p.pool.SendBatch(ctx, batch)
	defer func() { _ = br.Close() }()

	for range posts {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("insert raw_post: %w", err)
		}
	}
	return nil
}

// UnclassifiedRawPosts returns every raw post no classify pass has
// touched yet, ordered so callers can group consecutive rows into
// threads without a second pass. board filters to one board, or every
// board when "".
func (p *Postgres) UnclassifiedRawPosts(ctx context.Context, board string) ([]RawPost, error) {
	rows, err := p.pool.Query(ctx, `
		SELECT id, board, source, thread_no, post_no, post_time, sub, com, sticky, closed, archived
		FROM raw_posts
		WHERE classified_at IS NULL AND ($1 = '' OR board = $1)
		ORDER BY board, thread_no, post_no
	`, board)
	if err != nil {
		return nil, fmt.Errorf("query raw_posts: %w", err)
	}
	defer rows.Close()

	var posts []RawPost
	for rows.Next() {
		var rp RawPost
		var sub *string
		if err := rows.Scan(&rp.ID, &rp.Board, &rp.Source, &rp.ThreadNo, &rp.PostNo, &rp.PostTime,
			&sub, &rp.Com, &rp.Sticky, &rp.Closed, &rp.Archived); err != nil {
			return nil, fmt.Errorf("scan raw_post: %w", err)
		}
		if sub != nil {
			rp.Sub = *sub
		}
		posts = append(posts, rp)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate raw_posts: %w", err)
	}
	return posts, nil
}

// MarkClassified stamps classified_at on every id in ids, so a later
// classify pass doesn't reprocess them.
func (p *Postgres) MarkClassified(ctx context.Context, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := p.pool.Exec(ctx, `UPDATE raw_posts SET classified_at = now() WHERE id = ANY($1)`, ids)
	if err != nil {
		return fmt.Errorf("mark raw_posts classified: %w", err)
	}
	return nil
}

// rawPostsSettle is how old a raw_posts row must be before ListRawPosts
// returns it. id comes from a sequence at INSERT time but only becomes
// visible at COMMIT, so concurrent writers (the poller's worker pool)
// can commit a lower id after a higher one — a reader paging by id
// would already be past it and never see it. fetched_at is the
// transaction's start time (now()), so holding rows back until every
// transaction that could still be open for them has finished closes
// that gap. Each SaveRawPosts batch is one implicit transaction lasting
// well under this.
const rawPostsSettle = time.Minute

// RawPostsQuery pages through raw_posts in id order. AfterID is the
// cursor — the last id the caller already has, 0 to start from the
// beginning. Board filters to one board, or every board when "".
type RawPostsQuery struct {
	Board   string
	AfterID int64
	Limit   int
}

// ListRawPosts returns up to q.Limit raw posts with id > q.AfterID,
// oldest id first, holding back rows younger than rawPostsSettle. A
// caller that feeds the last returned id back in as AfterID sees every
// row exactly once.
func (p *Postgres) ListRawPosts(ctx context.Context, q RawPostsQuery) ([]RawPost, error) {
	rows, err := p.pool.Query(ctx, `
		SELECT id, board, source, thread_no, post_no, post_time, sub, com, sticky, closed, archived, fetched_at
		FROM raw_posts
		WHERE id > $1
		  AND ($2 = '' OR board = $2)
		  AND fetched_at < now() - make_interval(secs => $3)
		ORDER BY id
		LIMIT $4
	`, q.AfterID, q.Board, rawPostsSettle.Seconds(), q.Limit)
	if err != nil {
		return nil, fmt.Errorf("query raw_posts: %w", err)
	}
	defer rows.Close()

	var posts []RawPost
	for rows.Next() {
		var rp RawPost
		var sub *string
		if err := rows.Scan(&rp.ID, &rp.Board, &rp.Source, &rp.ThreadNo, &rp.PostNo, &rp.PostTime,
			&sub, &rp.Com, &rp.Sticky, &rp.Closed, &rp.Archived, &rp.FetchedAt); err != nil {
			return nil, fmt.Errorf("scan raw_post: %w", err)
		}
		if sub != nil {
			rp.Sub = *sub
		}
		posts = append(posts, rp)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate raw_posts: %w", err)
	}
	return posts, nil
}
