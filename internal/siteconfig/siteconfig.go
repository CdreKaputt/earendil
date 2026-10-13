// Package siteconfig defines the per-site configuration document
// stored in site_config_versions.config.
package siteconfig

import (
	"encoding/json"
	"fmt"
)

// Config is one site's notification settings, key by form ID.
type Config struct {
	Forms map[string]Form `json:"forms"`
}

// Form describes how submissions to one form are routed and rendered.
type Form struct {
	Recipients   []string `json:"recipients"`
	Subject      string   `json:"subject"`
	ReplyToField string   `json:"replyToField,omitempty"`
	FieldOrder   []string `json:"fieldOrder,omitempty"`
}

// Parse decodes a stored config document.
func Parse(data []byte) (Config, error) {
	var c Config
	if err := json.Unmarshal(data, &c); err != nil {
		return Config{}, fmt.Errorf("decoding site config: %w", err)
	}
	return c, nil
}
