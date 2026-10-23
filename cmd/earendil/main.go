package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/CdreKaputt/earendil/internal/config"
	"github.com/CdreKaputt/earendil/internal/consumer"
	"github.com/CdreKaputt/earendil/internal/devqueue"
	"github.com/CdreKaputt/earendil/internal/ingest"
)

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "earendil: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: cfg.LogLevel,
	}))

	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Create db connection pool to pass to goroutines
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("creating database pool: %w", err)
	}
	defer pool.Close()

	// Use Ping to test db connection at startup.
	// Fail fast if connection invalid
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		return fmt.Errorf("connecting to database: %w", err)
	}

	// queue for testing only; replace with Cloudflare queue later
	queue := devqueue.New()
	queue.Push(devqueue.SampleBody("dev-site"))

	cons := consumer.New(queue, ingest.New(pool, logger), logger)

	logger.Info("earendil starting", "config", cfg)

	if err := cons.Run(ctx); err != nil {
		return fmt.Errorf("running consumer: %w", err)
	}

	logger.Info("shutting down")
	return nil
}
