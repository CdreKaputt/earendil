package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	if err := run(context.Background(), logger); err != nil {
		logger.Error("fatal error", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, logger *slog.Logger) error {
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger.Info("earendil starting")

	<-ctx.Done() // wait here until a signal arrives

	logger.Info("shutting down")
	return nil
}
