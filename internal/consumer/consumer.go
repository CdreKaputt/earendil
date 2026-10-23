// Package consumer pulls form submissions off a queue and hands them
// to ingest. It acks a message only after the submission is stored.
package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"
	"uuid"

	"github.com/CdreKaputt/earendil/internal/ingest"
)

// Message is one queue message, as returned by Queue.Pull
type Message struct {
	LeaseID  string // used by Cloudflare's pull API
	Body     []byte
	Attempts int
}

// Queue is the subset of a message queue the consumer needs.
type Queue interface {
	Pull(ctx context.Context, batchSize int, visibility time.Duration) ([]Message, error)
	Ack(ctx context.Context, acks, retries []string) error
}

// Accepter stores a submission.
// *ingest.Service satisfies Accepter but mock accepters can be used for testing.
type Accepter interface {
	Accept(ctx context.Context, sub ingest.Submission) (ingest.EntryID, error)
}

// Consumer moves messages from a Queue into an Accepter
type Consumer struct {
	queue      Queue
	accepter   Accepter
	logger     *slog.Logger
	batchSize  int
	visibility time.Duration
}

// New returns a Consumer with default batch settings.
func New(q Queue, a Accepter, logger *slog.Logger) *Consumer {
	return &Consumer{
		queue:      q,
		accepter:   a,
		logger:     logger,
		batchSize:  10,
		visibility: 30 * time.Second,
	}
}

// processBatch pulls one batch, accepts each message, and acks or
// retries it. It returns how many messages were pulled.
func (c *Consumer) processBatch(ctx context.Context) (int, error) {
	msgs, err := c.queue.Pull(ctx, c.batchSize, c.visibility)
	if err != nil {
		return 0, fmt.Errorf("pulling messages: %w", err)
	}
	if len(msgs) == 0 {
		return 0, nil
	}

	var acks, retries []string
	for _, m := range msgs {
		if err := c.handle(ctx, m); err != nil {
			c.logger.Warn("message will be retried",
				"lease_id", m.LeaseID, "attempts", m.Attempts, "err", err)
			retries = append(retries, m.LeaseID)
			continue
		}
		acks = append(acks, m.LeaseID)
	}

	if err := c.queue.Ack(ctx, acks, retries); err != nil {
		return len(msgs), fmt.Errorf("acking messages: %w", err)
	}
	return len(msgs), nil
}

// messageBody is the JSON contract site Workers send.
type messageBody struct {
	SiteID         string          `json:"siteId"`
	FormID         string          `json:"formId"`
	IdempotencyKey uuid.UUID       `json:"idempotencyKey"`
	Fields         json.RawMessage `json:"fields"`
	ReceivedAt     time.Time       `json:"receivedAt"`
}

// handle decodes one message and passes it to the Accepter
func (c *Consumer) handle(ctx context.Context, m Message) error {
	var body messageBody
	if err := json.Unmarshal(m.Body, &body); err != nil {
		return fmt.Errorf("decoding message body: %w", err)
	}

	sub := ingest.Submission{
		SiteID:         body.SiteID,
		FormID:         body.FormID,
		IdempotencyKey: body.IdempotencyKey,
		Fields:         body.Fields,
		ReceivedAt:     body.ReceivedAt,
	}
	id, err := c.accepter.Accept(ctx, sub)
	if err != nil {
		return err
	}

	c.logger.Info("submission accepted", "submission", sub, "entry_id", id)
	return nil
}
