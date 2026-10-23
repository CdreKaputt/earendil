// Package devqueue is an in-memory consumer.Queue for local development.
// Retried messages are redelivered on a later Pull, as a real queue would.
// It is not safe for concurrent use.
package devqueue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
	"uuid"

	"github.com/CdreKaputt/earendil/internal/consumer"
)

// Queue holds messages in memory
type Queue struct {
	pending  []consumer.Message
	inflight map[string]consumer.Message
	nextID   int
}

var _ consumer.Queue = (*Queue)(nil)

// New returns an empty Queue
func New() *Queue {
	return &Queue{inflight: make(map[string]consumer.Message)}
}

// Push adds a message with the given body.
func (q *Queue) Push(body []byte) {
	q.nextID++
	q.pending = append(q.pending, consumer.Message{
		LeaseID: fmt.Sprintf("dev-%d", q.nextID),
		Body:    body,
	})
}

// Pull returns up to batchSize pending messages and holds them until acked
func (q *Queue) Pull(ctx context.Context, batchSize int, visibility time.Duration) ([]consumer.Message, error) {
	n := min(batchSize, len(q.pending))
	msgs := q.pending[:n:n]
	q.pending = q.pending[n:]

	for _, m := range msgs {
		q.inflight[m.LeaseID] = m
	}
	return msgs, nil
}

// Ack drops acked messages and requeues retried ones
func (q *Queue) Ack(ctx context.Context, acks, retries []string) error {
	for _, id := range acks {
		delete(q.inflight, id)
	}
	for _, id := range retries {
		m, ok := q.inflight[id]
		if !ok {
			continue
		}
		delete(q.inflight, id)
		m.Attempts++
		q.pending = append(q.pending, m)
	}
	return nil
}

// SampleBody returns a message body for siteID's "contact" form with a
// fresh idempotency key, shaped like what a site Worker sends.
func SampleBody(siteID string) []byte {
	body, err := json.Marshal(map[string]any{
		"siteId":         siteID,
		"formId":         "contact",
		"idempotencyKey": uuid.New(),
		"fields":         map[string]string{"name": "Ada Lovelace", "email": "ada@example.org", "message": "Hello from devqueue"},
		"receivedAt":     time.Now().UTC(),
	})
	if err != nil {
		panic(err) // only fails for unencodable types, which these are not
	}
	return body
}
