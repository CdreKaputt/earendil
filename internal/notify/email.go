package notify

import (
	"encoding/json"
	"fmt"
	"maps"
	"net/mail"
	"slices"
	"strings"
	"time"

	"github.com/CdreKaputt/earendil/internal/siteconfig"
	"github.com/CdreKaputt/earendil/internal/store"
)

const defaultSubject = "New form submission"

// buildEmail renders one notification row as a plain-text email
func buildEmail(row store.ListDueNotificationsRow) (Email, error) {
	cfg, err := siteconfig.Parse(row.Config)
	if err != nil {
		return Email{}, err
	}
	form := cfg.Forms[row.FormID] // zero Form if the form was since removed

	var fields map[string]any
	if err := json.Unmarshal(row.Fields, &fields); err != nil {
		return Email{}, fmt.Errorf("decoding entry fields: %w", err)
	}

	subject := form.Subject
	if subject == "" {
		subject = defaultSubject
	}

	var b strings.Builder
	fmt.Fprintf(&b, "New submission to %q on %s\n", row.FormID, row.SiteID)
	fmt.Fprintf(&b, "Received: %s\n\n", row.ReceivedAt.UTC().Format(time.RFC1123))
	for _, key := range orderedKeys(fields, form.FieldOrder) {
		fmt.Fprintf(&b, "%s: %v\n", key, fields[key])
	}

	return Email{
		To:      row.Recipient,
		ReplyTo: replyTo(fields, form.ReplyToField),
		Subject: subject,
		Body:    b.String(),
	}, nil
}

// orderedKeys returns the key of fields: those named in order first,
// then any others alphabetically, so no submitted field is ever dropped.
func orderedKeys(fields map[string]any, order []string) []string {
	keys := make([]string, 0, len(fields))
	seen := make(map[string]bool, len(fields))
	for _, k := range order {
		if _, ok := fields[k]; ok && !seen[k] {
			keys = append(keys, k)
			seen[k] = true
		}
	}
	for _, k := range slices.Sorted(maps.Keys(fields)) {
		if !seen[k] {
			keys = append(keys, k)
		}
	}
	return keys
}

// replyTo returns the submitted address in fields[field], or "" if the
// field is unset, not a string, or not a single valid address. The value
// comes from an unstrested form, so it is parsed rather than used as is.
func replyTo(fields map[string]any, field string) string {
	if field == "" {
		return ""
	}
	s, ok := fields[field].(string)
	if !ok {
		return ""
	}
	addr, err := mail.ParseAddress(s)
	if err != nil {
		return ""
	}
	return addr.Address
}
