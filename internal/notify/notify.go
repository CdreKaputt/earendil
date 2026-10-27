// Package notify sends the emails queued in the notifications table
// (the transactional outbox) that ingest.Accept writes.
package notify

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/CdreKaputt/earendil/internal/store"
)

// Email is one message ready to hand to a Sender.
type Email struct {
	To      string
	ReplyTo string // empty if the submission has no usable reply address
	Subject string
	Body    string
}

// Sender delivers an Email
type Sender interface {
	Send(ctx context.Context, e Email) error
}

// Notifier polls for due notifications and sends them
type Notifier struct {
	queries      *store.Queries
	sender       Sender
	logger       *slog.Logger
	batchSize    int
	pollInterval time.Duration
}

// New returns a Notifier that reads from pool and delivers with sender
func New(pool *pgxpool.Pool, sender Sender, logger *slog.Logger) *Notifier {
	return &Notifier{
		queries:      store.New(pool),
		sender:       sender,
		logger:       logger,
		batchSize:    10,
		pollInterval: 5 * time.Second,
	}
}

// Run sends due notifications until ctx is canceled. It returns nil on
// a clean shutdown.
func (n *Notifier) Run(ctx context.Context) error {
	n.logger.Info("notifier started")
	for {
		if err := n.processBatch(ctx); err != nil {
			n.logger.Error("processing notifications", "err", err)
		}

		select {
		case <-ctx.Done():
			n.logger.Info("notifier stopped")
			return nil
		case <-time.After(n.pollInterval):
		}
	}
}

// processBatch sends one batch of due notifications and marks each
// one sent. A failed send is logged and left pending.
func (n *Notifier) processBatch(ctx context.Context) error {
	rows, err := n.queries.ListDueNotifications(ctx, int32(n.batchSize))
	if err != nil {
		return fmt.Errorf("listing due notifications: %w", err)
	}

	for _, row := range rows {
		log := n.logger.With("notification_id", row.ID, "site_id", row.SiteID, "form_id", row.FormID)

		email, err := buildEmail(row)
		if err != nil {
			log.Error("building email", "err", err)
			continue
		}
		if err := n.sender.Send(ctx, email); err != nil {
			log.Warn("sending email", "err", err)
			continue
		}
		if err := n.queries.MarkNotificationSent(ctx, row.ID); err != nil {
			return fmt.Errorf("marking notification %d sent: %w", row.ID, err)
		}
		log.Info("notification sent", "recipient", row.Recipient)
	}
	return nil
}

// LogSender is a stand-in Sender that only logs. It is replaced by real
// SMTP delivery in the next increment.
type LogSender struct {
	Logger *slog.Logger
}

// Send logs e instead of delivering it
func (s LogSender) Send(ctx context.Context, e Email) error {
	s.Logger.Info("email not sent (LogSender)", "to", e.To, "reply_to", e.ReplyTo, "subject", e.Subject)
	return nil
}
