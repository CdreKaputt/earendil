package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"
	"uuid"

	"github.com/jackc/pgx/v5/pgxpool"
)

// newTestService connects to DATABASE_URL, or skips the test if it is unset.
func newTestService(t *testing.T) (*Service, *pgxpool.Pool) {
	t.Helper()

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set; skipping integration test")
	}

	pool, err := pgxpool.New(t.Context(), dsn)
	if err != nil {
		t.Fatalf("creating pool: %v", err)
	}
	t.Cleanup(pool.Close)

	logger := slog.New(slog.NewTextHandler(t.Output(), nil))
	return New(pool, logger), pool
}

// createSite inserts an active site allowing example.com, plus a config
// with one "contact" form, and removes both when the test ends.
func createSite(t *testing.T, pool *pgxpool.Pool, recipients ...string) string {
	t.Helper()
	ctx := t.Context()
	siteID := "test-" + uuid.New().String()[:8]
	_, err := pool.Exec(ctx,
		`INSERT INTO sites (id, allowed_recipient_domains) VALUES ($1, '{example.com}')`,
		siteID)
	if err != nil {
		t.Fatalf("inserting site: %v", err)
	}
	// register cleanup right after site insert succeeds. Cleanup runs when tests finish or break
	t.Cleanup(func() {
		ctx := context.Background()
		if _, err := pool.Exec(ctx, `DELETE FROM entries WHERE site_id = $1`, siteID); err != nil {
			t.Errorf("cleanup entries: %v", err)
		}
		if _, err := pool.Exec(ctx, `DELETE FROM sites WHERE id = $1`, siteID); err != nil {
			t.Errorf("cleanup site: %v", err)
		}
	})

	cfg, err := json.Marshal(map[string]any{
		"forms": map[string]any{
			"contact": map[string]any{"recipients": recipients, "subject": "New lead"},
		},
	})
	if err != nil {
		t.Fatalf("encoding config: %v", err)
	}
	_, err = pool.Exec(ctx,
		`INSERT INTO site_config_versions (site_id, version, config, content_hash, source)
		 VALUES ($1, 1, $2, 'test', 'manual')`,
		siteID, cfg)
	if err != nil {
		t.Fatalf("inserting config: %v", err)
	}

	return siteID
}

func newSubmission(siteID, formID string) Submission {
	return Submission{
		SiteID:         siteID,
		FormID:         formID,
		IdempotencyKey: uuid.New(),
		Fields:         json.RawMessage(`{"name":"Ada","email":"ada@example.org"}`),
		ReceivedAt:     time.Now(),
	}
}

func countRows(t *testing.T, pool *pgxpool.Pool, query string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(t.Context(), query, args...).Scan(&n); err != nil {
		t.Fatalf("counting rows: %v", err)
	}
	return n
}

func TestAcceptStoresEntryAndNotifications(t *testing.T) {
	svc, pool := newTestService(t)
	siteID := createSite(t, pool, "owner@example.com", "office@example.com")

	id, err := svc.Accept(t.Context(), newSubmission(siteID, "contact"))
	if err != nil {
		t.Fatalf("Accept() error = %v", err)
	}

	if got := countRows(t, pool, `SELECT count(*) FROM entries WHERE id = $1`, int64(id)); got != 1 {
		t.Errorf("entries with id %d = %d, want 1", id, got)
	}
	if got := countRows(t, pool,
		`SELECT count(*) FROM notifications WHERE entry_id = $1 AND status = 'pending'`, int64(id)); got != 2 {
		t.Errorf("pending notifications = %d, want 2", got)
	}
}

func TestAcceptDuplicateReturnsExistingEntry(t *testing.T) {
	svc, pool := newTestService(t)
	siteID := createSite(t, pool, "owner@example.com")
	sub := newSubmission(siteID, "contact")

	first, err := svc.Accept(t.Context(), sub)
	if err != nil {
		t.Fatalf("first Accept() error = %v", err)
	}
	second, err := svc.Accept(t.Context(), sub)
	if err != nil {
		t.Fatalf("second Accept() error = %v", err)
	}

	if second != first {
		t.Errorf("second Accept() = %d, want existing id %d", second, first)
	}
	if got := countRows(t, pool, `SELECT count(*) FROM entries WHERE site_id = $1`, siteID); got != 1 {
		t.Errorf("entries = %d, want 1", got)
	}
	if got := countRows(t, pool, `SELECT count(*) FROM notifications WHERE entry_id = $1`, int64(first)); got != 1 {
		t.Errorf("notifications = %d, want 1 (duplicate must not queue more)", got)
	}
}

func TestAcceptRejectsUnroutable(t *testing.T) {
	svc, pool := newTestService(t)
	unknownSiteID := "test-" + uuid.New().String()[:8]
	inactiveSiteID := createSite(t, pool, "owner@example.com")
	activeSiteID := createSite(t, pool, "owner@example.com")

	// deactivate inactive site for testing
	ctx := t.Context()
	_, err := pool.Exec(ctx, `UPDATE sites SET active = false WHERE id = $1`, inactiveSiteID)
	if err != nil {
		t.Fatalf("updating inactive site status: %v", err)
	}

	submissionTests := []struct {
		name    string
		sub     Submission
		wantErr error
	}{
		{
			name:    "unknown site",
			sub:     newSubmission(unknownSiteID, "contact"),
			wantErr: ErrUnknownSite,
		},
		{
			name:    "inactive site",
			sub:     newSubmission(inactiveSiteID, "contact"),
			wantErr: ErrInactiveSite,
		},
		{
			name:    "unknown form",
			sub:     newSubmission(activeSiteID, "newsletter"),
			wantErr: ErrUnknownForm,
		},
		{
			name:    "invalid submission",
			sub:     newSubmission("", ""),
			wantErr: ErrInvalidSubmission,
		},
	}

	for _, tt := range submissionTests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := svc.Accept(t.Context(), tt.sub)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("Accept() error = %v, want %v", err, tt.wantErr)
			}
			if got := countRows(t, pool, `SELECT count(*) FROM entries WHERE idempotency_key = $1`, tt.sub.IdempotencyKey); got != 0 {
				t.Errorf("entries count = %d, want 0", got)
			}
		})
	}
}
