package ingest

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/CdreKaputt/earendil/internal/siteconfig"
	"github.com/CdreKaputt/earendil/internal/store"
)

// Accept validates sub and stores it as an entry plus one pending
// notification per allowed recipient, in a single transation.
// A duplicate idempotency key returns the existing entry's ID.
func (s *Service) Accept(ctx context.Context, sub Submission) (EntryID, error) {
	if err := sub.validate(); err != nil {
		return 0, err
	}

	// being transaction
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("beginning transaction: %w", err)
	}
	// transaction is rolled back if err before tx.Commit()
	defer tx.Rollback(ctx)

	q := s.queries.WithTx(tx)

	site, err := q.GetSite(ctx, sub.SiteID)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("%w: %q", ErrUnknownSite, sub.SiteID)
	}
	if err != nil {
		return 0, fmt.Errorf("loading sites: %w", err)
	}
	if !site.Active {
		return 0, fmt.Errorf("%w: %q", ErrInactiveSite, sub.SiteID)
	}

	row, err := q.GetLatestSiteConfig(ctx, sub.SiteID)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("%w: site %q has no config", ErrUnknownForm, sub.SiteID)
	}
	if err != nil {
		return 0, fmt.Errorf("loading site config: %w", err)
	}

	cfg, err := siteconfig.Parse(row.Config)
	if err != nil {
		return 0, err
	}

	form, ok := cfg.Forms[sub.FormID]
	if !ok {
		return 0, fmt.Errorf("%w: %q on site %q", ErrUnknownForm, sub.FormID, sub.SiteID)
	}

	id, err := q.InsertEntry(ctx, store.InsertEntryParams{
		SiteID:         sub.SiteID,
		FormID:         sub.FormID,
		Fields:         sub.Fields,
		IdempotencyKey: sub.IdempotencyKey,
		ConfigVersion:  row.Version,
		ReceivedAt:     sub.ReceivedAt,
	})
	// if entry exists, do nothing and return entry ID
	if errors.Is(err, pgx.ErrNoRows) {
		existing, err := q.GetEntryIDByIdempotencyKey(ctx, store.GetEntryIDByIdempotencyKeyParams{
			SiteID:         sub.SiteID,
			IdempotencyKey: sub.IdempotencyKey,
		})
		if err != nil {
			return 0, fmt.Errorf("loading duplicate entry: %w", err)
		}
		return EntryID(existing), nil
	}
	if err != nil {
		return 0, fmt.Errorf("inserting entry: %w", err)
	}

	allowed, rejected := allowedRecipients(form.Recipients, site)
	for _, r := range rejected {
		s.logger.Warn("recipient blocked by site guardrails", "submission", sub, "recipient", r)
	}
	if len(allowed) == 0 {
		s.logger.Warn("entry stored with no notifications", "submission", sub)
	}
	for _, r := range allowed {
		err := q.InsertNotification(ctx, store.InsertNotificationParams{
			EntryID:   id,
			Recipient: r,
		})
		if err != nil {
			return 0, fmt.Errorf("queuing notification: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("committing: %w", err)
	}
	return EntryID(id), nil
}

// allowedRecipients applies a site's guardrails to a from's recipient
// list. Addresses are lowercaseed and deduplicated, must belong to one
// of the site's allowed domains, and at most site.MaxRecipients are kept.
func allowedRecipients(recipients []string, site store.Site) (allowed, rejected []string) {
	domains := make(map[string]bool, len(site.AllowedRecipientDomains))
	for _, d := range site.AllowedRecipientDomains {
		domains[strings.ToLower(d)] = true
	}

	seen := make(map[string]bool)
	for _, r := range recipients {
		addr := strings.ToLower(strings.TrimSpace(r))
		if seen[addr] {
			continue
		}
		seen[addr] = true

		at := strings.LastIndex(addr, "@")
		switch {
		case at < 1 || !domains[addr[at+1:]]:
			rejected = append(rejected, r)
		case len(allowed) >= int(site.MaxRecipients):
			rejected = append(rejected, r)
		default:
			allowed = append(allowed, addr)
		}
	}
	return allowed, rejected
}
