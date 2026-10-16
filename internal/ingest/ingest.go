// Package ingest turns form submissions into stored entries and
// queued notifications.
package ingest

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"
	"uuid"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/CdreKaputt/earendil/internal/store"
)

// EntryID identifies a stroed entry.
type EntryID int64

// Submission is one form entry as received at the edge
type Submission struct {
	SiteID         string
	FormID         string
	IdempotencyKey uuid.UUID
	Fields         json.RawMessage
	ReceivedAt     time.Time
}

// LogValue omits Feilds, which hold submitters' personal data
func (s Submission) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("site_id", s.SiteID),
		slog.String("form_id", s.FormID),
		slog.String("idempotency_key", s.IdempotencyKey.String()),
	)
}

var _ slog.LogValuer = Submission{}

var (
	ErrInvalidSubmission = errors.New("invalid submission")
	ErrUnknownSite       = errors.New("unknown site")
	ErrInactiveSite      = errors.New("inactive site")
	ErrUnknownForm       = errors.New("unknown form")
)

// Service accepts submissions into the database
type Service struct {
	pool    *pgxpool.Pool
	queries *store.Queries
	logger  *slog.Logger
}

// New returns a Service backed by db pool
func New(pool *pgxpool.Pool, logger *slog.Logger) *Service {
	return &Service{
		pool:    pool,
		queries: store.New(pool),
		logger:  logger,
	}
}

// validate reports every structural problem with submission at once
func (s Submission) validate() error {
	var problems []string
	if s.SiteID == "" {
		problems = append(problems, "missing siteId")
	}
	if s.FormID == "" {
		problems = append(problems, "missing formId")
	}
	if s.IdempotencyKey == uuid.Nil() {
		problems = append(problems, "missing idempotencyKey")
	}
	if s.ReceivedAt.IsZero() {
		problems = append(problems, "missing receivedAt")
	}
	if !isJSONObject(s.Fields) {
		problems = append(problems, "fields must be a JSON object")
	}
	if len(problems) > 0 {
		return fmt.Errorf("%w: %s", ErrInvalidSubmission, strings.Join(problems, "; "))
	}
	return nil
}

func isJSONObject(raw json.RawMessage) bool {
	var m map[string]json.RawMessage
	return json.Unmarshal(raw, &m) == nil && m != nil
}
