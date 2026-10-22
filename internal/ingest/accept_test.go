package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"slices"
	"testing"
	"time"
	"uuid"

	"github.com/CdreKaputt/earendil/internal/store"
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

func TestAcceptRollsBackWhenNotificationInsertFails(t *testing.T) {
	svc, pool := newTestService(t)
	siteID := createSite(t, pool, "owner@example.com", "boom@example.com")
	ctx := t.Context()

	// register PostgreSQL function and trigger to reject boom@example.com
	if _, err := pool.Exec(ctx,
		`CREATE FUNCTION fail_on_boom() RETURNS trigger
		LANGUAGE plpgsql AS $$
		BEGIN
			RAISE EXCEPTION 'forced failure for %', NEW.recipient;
		END
		$$;
		CREATE TRIGGER fail_on_boom
			BEFORE INSERT ON notifications
			FOR EACH ROW
			WHEN (NEW.recipient = 'boom@example.com')
			EXECUTE FUNCTION fail_on_boom();`); err != nil {
		t.Fatalf("creating fail_on_boom function and trigger: %v", err)
	}
	// drop trigger and function after test runs or fails
	t.Cleanup(func() {
		ctx := context.Background()
		_, err := pool.Exec(ctx,
			`DROP TRIGGER fail_on_boom ON notifications;
			DROP FUNCTION fail_on_boom();`)
		if err != nil {
			t.Errorf("dropping fail_on_boom function and trigger: %v", err)
		}
	})

	sub := newSubmission(siteID, "contact")
	if _, err := svc.Accept(ctx, sub); err == nil {
		t.Errorf("Accept() error = nil, want an error")
	}
	// No separate notifications check: notifications.entry_id is a foreign
	// key to entries, so with no entry there can be no notification rows.
	if got := countRows(t, pool, `SELECT count(*) FROM entries WHERE idempotency_key = $1`, sub.IdempotencyKey); got != 0 {
		t.Errorf("entries count = %d, want 0", got)
	}
}

func TestAllowedRecipients(t *testing.T) {
	// testSite may email any address at example.com, up to 3 per entry.
	testSite := store.Site{
		AllowedRecipientDomains: []string{"example.com"},
		MaxRecipients:           3,
	}

	tests := []struct {
		name         string
		site         store.Site
		recipients   []string
		wantAllowed  []string
		wantRejected []string
	}{
		{
			name:        "allowed address is lowercased",
			site:        testSite,
			recipients:  []string{"Owner@Example.COM"},
			wantAllowed: []string{"owner@example.com"},
		},
		{
			// Rejected addresses keep their original form so the
			// warning log matches what is written in the config.
			name:         "disallowed domain is rejected as written",
			site:         testSite,
			recipients:   []string{"owner@example.com", " Lead@Evil.com"},
			wantAllowed:  []string{"owner@example.com"},
			wantRejected: []string{" Lead@Evil.com"},
		},
		{
			name:         "subdomain of an allowed domain is rejected",
			site:         testSite,
			recipients:   []string{"lead@mail.example.com"},
			wantRejected: []string{"lead@mail.example.com"},
		},
		{
			name:         "lookalike domain is rejected",
			site:         testSite,
			recipients:   []string{"lead@example.com.evil.com"},
			wantRejected: []string{"lead@example.com.evil.com"},
		},
		{
			name:        "duplicates differing by case and whitespace collapse to one",
			site:        testSite,
			recipients:  []string{" owner@example.com ", "OWNER@Example.Com", "owner@example.com"},
			wantAllowed: []string{"owner@example.com"},
		},
		{
			name:         "address with no @ or a leading @ is rejected",
			site:         testSite,
			recipients:   []string{"ownerexample.com", "@example.com"},
			wantRejected: []string{"ownerexample.com", "@example.com"},
		},
		{
			name:         "recipients beyond MaxRecipients are rejected",
			site:         testSite,
			recipients:   []string{"a@example.com", "b@example.com", "c@example.com", "d@example.com"},
			wantAllowed:  []string{"a@example.com", "b@example.com", "c@example.com"},
			wantRejected: []string{"d@example.com"},
		},
		{
			// Deduplication runs before the limit check, so a repeat of an
			// address that was already allowed is dropped silently, not
			// counted as rejected.
			name: "duplicate after the limit is dropped, not rejected",
			site: store.Site{
				AllowedRecipientDomains: []string{"example.com"},
				MaxRecipients:           1,
			},
			recipients:   []string{"a@example.com", "b@example.com", "A@example.com"},
			wantAllowed:  []string{"a@example.com"},
			wantRejected: []string{"b@example.com"},
		},
		{
			name: "uppercase allowed domain still matches",
			site: store.Site{
				AllowedRecipientDomains: []string{"EXAMPLE.COM"},
				MaxRecipients:           1,
			},
			recipients:  []string{"owner@example.com"},
			wantAllowed: []string{"owner@example.com"},
		},
		{
			name:       "no recipients returns nothing",
			site:       testSite,
			recipients: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			allowed, rejected := allowedRecipients(tt.recipients, tt.site)
			if !slices.Equal(allowed, tt.wantAllowed) {
				t.Errorf("allowedRecipients() allowed = %q, want %q", allowed, tt.wantAllowed)
			}
			if !slices.Equal(rejected, tt.wantRejected) {
				t.Errorf("allowedRecipients() rejected = %q, want %q", rejected, tt.wantRejected)
			}
		})
	}
}
