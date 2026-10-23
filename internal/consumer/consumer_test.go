package consumer

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"testing"
	"time"
	"uuid"

	"github.com/CdreKaputt/earendil/internal/ingest"
)

// fakeQueue returns its messages on the first Pull and records acks
type fakeQueue struct {
	messages []Message
	acks     []string
	retries  []string
}

var _ Queue = (*fakeQueue)(nil)

func (q *fakeQueue) Pull(ctx context.Context, batchSize int, visibility time.Duration) ([]Message, error) {
	msgs := q.messages
	q.messages = nil
	return msgs, nil
}

func (q *fakeQueue) Ack(ctx context.Context, acks, retries []string) error {
	q.acks = append(q.acks, acks...)
	q.retries = append(q.retries, retries...)
	return nil
}

// fakeAccepter records submissions and returns err for every call
type fakeAccepter struct {
	got []ingest.Submission
	err error
}

var _ Accepter = (*fakeAccepter)(nil)

func (a *fakeAccepter) Accept(ctx context.Context, sub ingest.Submission) (ingest.EntryID, error) {
	a.got = append(a.got, sub)
	if a.err != nil {
		return 0, a.err
	}
	return ingest.EntryID(len(a.got)), nil
}

const validBody = `{
	"siteId": "test-site",
	"formId": "contact",
	"idempotencyKey": "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
	"fields": {"name":"Ada"},
	"receivedAt": "2026-10-21T15:04:05Z"
}`

func newTestConsumer(t *testing.T, q Queue, a Accepter) *Consumer {
	t.Helper()
	return New(q, a, slog.New(slog.NewTextHandler(t.Output(), nil)))
}

func TestProcessBatchAcksAcceptedMessages(t *testing.T) {
	q := &fakeQueue{messages: []Message{{LeaseID: "lease-1", Body: []byte(validBody)}}}
	a := &fakeAccepter{}
	c := newTestConsumer(t, q, a)

	n, err := c.processBatch(t.Context())
	if err != nil {
		t.Fatalf("processBatch() error = %v", err)
	}
	if n != 1 {
		t.Errorf("processBatch() = %d, want 1", n)
	}
	if len(a.got) != 1 {
		t.Fatalf("Accept called %d times, want 1", len(a.got))
	}

	sub := a.got[0]
	wantTime := time.Date(2026, 10, 21, 15, 4, 5, 0, time.UTC)
	if sub.SiteID != "test-site" || sub.FormID != "contact" ||
		sub.IdempotencyKey != uuid.MustParse("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11") ||
		string(sub.Fields) != `{"name":"Ada"}` || !sub.ReceivedAt.Equal(wantTime) {
		t.Errorf("Accept got %+v, want fields from validBody", sub)
	}

	if !slices.Equal(q.acks, []string{"lease-1"}) {
		t.Errorf("acks = %q, want [lease-1]", q.acks)
	}
	if len(q.retries) != 0 {
		t.Errorf("retries = %q, want none", q.retries)
	}
}

func TestProcessBatchRetriesRejectedMessages(t *testing.T) {
	q := &fakeQueue{messages: []Message{{LeaseID: "lease-1", Body: []byte(validBody)}}}
	a := &fakeAccepter{err: errors.New("database unavailable")}
	c := newTestConsumer(t, q, a)

	if _, err := c.processBatch(t.Context()); err != nil {
		t.Fatalf("processBatch() error = %v", err)
	}

	if len(q.acks) != 0 {
		t.Errorf("acks = %q, want none", q.acks)
	}
	if !slices.Equal(q.retries, []string{"lease-1"}) {
		t.Errorf("retries = %q, want [lease-1]", q.retries)
	}
}

func TestRunProcessesBatchAndStopsOnCancel(t *testing.T) {
	q := &fakeQueue{messages: []Message{{LeaseID: "lease-1", Body: []byte(validBody)}}}
	a := &fakeAccepter{}
	c := newTestConsumer(t, q, a)

	ctx, cancel := context.WithCancel(t.Context())
	cancel() // cancel right away. Run() still makes one cycle

	if err := c.Run(ctx); err != nil {
		t.Fatalf("Run() error = %v, want nil", err)
	}
	if !slices.Equal(q.acks, []string{"lease-1"}) {
		t.Errorf("acks = %q, want [lease-1]", q.acks)
	}
}
