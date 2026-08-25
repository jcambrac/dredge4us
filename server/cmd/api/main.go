// Command api serves the poller's findings as read-only JSON. Kept as a
// separate binary from poller so ingestion and querying can restart,
// scale, and fail independently of each other.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jcl80/dredge4us/lib/detect"
	"github.com/jcl80/dredge4us/lib/foolfuuka"
	"github.com/jcl80/dredge4us/lib/fourchan"
	"github.com/jcl80/dredge4us/server/internal/api"
	"github.com/jcl80/dredge4us/server/internal/migrate"
	pgstore "github.com/jcl80/dredge4us/server/internal/store"
	"github.com/jcl80/dredge4us/server/migrations"
)

const shutdownTimeout = 5 * time.Second

func main() {
	if err := run(); err != nil {
		slog.Error("api exited", "error", err)
		os.Exit(1)
	}
}

func run() error {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		return fmt.Errorf("DATABASE_URL is required")
	}
	token := os.Getenv("API_AUTH_TOKEN")
	if token == "" {
		return fmt.Errorf("API_AUTH_TOKEN is required")
	}
	addr := os.Getenv("API_ADDR")
	if addr == "" {
		addr = ":8080"
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pg, err := pgstore.NewPostgres(ctx, dbURL)
	if err != nil {
		return err
	}
	defer pg.Close()

	// api and poller both run every migration on their own startup —
	// migrate.Run is idempotent (tracks applied filenames), so this is
	// just belt-and-suspenders against whichever one happens to deploy
	// first after a schema change, not a sign either one owns the schema.
	if err := migrate.Run(ctx, pg.Pool(), migrations.Files); err != nil {
		return err
	}

	fc := fourchan.NewClient(fourchan.NewLimiter())

	// One archive client (and Limiter) per host, shared by every board
	// mapped to it — matches docs/archive-sources.md, same rule the
	// poller's Sources follows.
	desu := foolfuuka.NewClient("https://desuarchive.org", fourchan.NewLimiter())
	palanq := foolfuuka.NewClient("https://archive.palanq.win", fourchan.NewLimiter())
	backfillBoards := []api.BackfillBoard{
		{Board: "his", Client: desu},
		{Board: "k", Client: desu},
		{Board: "g", Client: desu},
		{Board: "int", Client: desu},
		{Board: "news", Client: palanq},
	}

	detectors := []detect.Detector{detect.NewArtifactDetector()}
	if apiKey := os.Getenv("OPENAI_API_KEY"); apiKey != "" {
		model := os.Getenv("OPENAI_MODEL")
		if model == "" {
			model = "gpt-5.5"
		}
		detectors = append(detectors, detect.NewLLMClassifier(apiKey, model))
		slog.Info("llm classification enabled for backfill", "model", model)
	}

	srv := &http.Server{Addr: addr, Handler: api.New(pg, pg, fc, backfillBoards, detectors, token)}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	slog.Info("api starting", "addr", addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
