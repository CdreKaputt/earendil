package consumer

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
	"uuid"

	"github.com/CdreKaputt/earendil/internal/ingest"
)

// fakeQueue returns its messages on the first Pull and records acks
type fakeQueue struct {
	pulls    atomic.Int32 // atomic avoids race condition during testing
	messages []Message
	acks     []string
	retries  []string
}

var _ Queue = (*fakeQueue)(nil)

func (q *fakeQueue) Pull(ctx context.Context, batchSize int, visibility time.Duration) ([]Message, error) {
	q.pulls.Add(1)
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

func TestProcessBatchRetriesMalformedMessageWithoutBlockingOthers(t *testing.T) {
	q := &fakeQueue{messages: []Message{
		{LeaseID: "lease-1", Body: []byte(validBody)},
		{LeaseID: "lease-2", Body: []byte(`not json`)},
		{LeaseID: "lease-3", Body: []byte(validBody)},
	}}
	a := &fakeAccepter{}
	c := newTestConsumer(t, q, a)

	n, err := c.processBatch(t.Context())
	if err != nil {
		t.Fatalf("processBatch() error = %v", err)
	}
	if n != 3 {
		t.Errorf("processBatch() = %d, want 3", n)
	}
	if len(a.got) != 2 {
		t.Errorf("Accept called %d times, want 2 (malformed body must not reach Accept)", len(a.got))
	}
	if !slices.Equal(q.acks, []string{"lease-1", "lease-3"}) {
		t.Errorf("acks = %q, want [lease-1 lease-3]", q.acks)
	}
	if !slices.Equal(q.retries, []string{"lease-2"}) {
		t.Errorf("retries = %q, want [lease-2]", q.retries)
	}
}

func TestHandleRejectsUndecodableBodies(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{"empty body", ``},
		{"not json", `not json`},
		{"json array", `[]`},
		{"invalid idempotency key", `{"idempotencyKey": "not-a-uuid"}`},
		{"invalid timestamp", `{"receivedAt": "yesterday"}`},
		{"wrong field type", `{"siteId": 42}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &fakeAccepter{}
			c := newTestConsumer(t, &fakeQueue{}, a)

			err := c.handle(t.Context(), Message{LeaseID: "lease-1", Body: []byte(tt.body)})
			if err == nil {
				t.Errorf("handle() error = nil, want a decoding error")
			}
			if len(a.got) != 0 {
				t.Errorf("Accept alled %d times, want 0", len(a.got))
			}
		})
	}
}

func TestNextWait(t *testing.T) {
	c := newTestConsumer(t, &fakeQueue{}, &fakeAccepter{})
	c.batchSize = 10
	c.minWait = 5 * time.Second
	c.maxWait = 60 * time.Second
	errPull := errors.New("pull failed")

	tests := []struct {
		name   string
		prev   time.Duration
		pulled int
		err    error
		want   time.Duration
	}{
		{"full batch pulls again at once", 20 * time.Second, 10, nil, 0},
		{"partial batch resets to min", 40 * time.Second, 3, nil, 5 * time.Second},
		{"first empty batch waits min", 0, 0, nil, 5 * time.Second},
		{"empty batch doubles", 5 * time.Second, 0, nil, 10 * time.Second},
		{"empty batch is capped", 40 * time.Second, 0, nil, 60 * time.Second},
		{"error backs off like empty", 10 * time.Second, 0, errPull, 20 * time.Second},
		{"error after full batch waits min", 0, 0, errPull, 5 * time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := c.nextWait(tt.prev, tt.pulled, tt.err); got != tt.want {
				t.Errorf("nextWait(%v, %d, %v), = %v, want %v", tt.prev, tt.pulled, tt.err, got, tt.want)
			}
		})
	}
}

func TestRunBacksOffWhileQueueIsEmpty(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := &fakeQueue{}
		c := newTestConsumer(t, q, &fakeAccepter{})

		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error)
		go func() { done <- c.Run(ctx) }()

		// Pulls happen at 0s, then after waits of 5s, 10s, 20s, 40s, 60s, 60s:
		// t = 0, 5, 15, 35, 75, 135, 195.
		checks := []struct {
			at    time.Duration
			pulls int32
		}{
			{0, 1},
			{5 * time.Second, 2},
			{15 * time.Second, 3},
			{35 * time.Second, 4},
			{75 * time.Second, 5},
			{135 * time.Second, 6},
			{195 * time.Second, 7},
		}
		start := time.Now()
		for _, check := range checks {
			time.Sleep(check.at - time.Since(start))
			synctest.Wait()
			if got := q.pulls.Load(); got != check.pulls {
				t.Errorf("after %v: pulls = %d, want %d", check.at, got, check.pulls)
			}
		}

		cancel()
		if err := <-done; err != nil {
			t.Errorf("Run() error = %v, want nil", err)
		}
	})
}
