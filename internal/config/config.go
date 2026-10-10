package config

import (
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
)

// Config holds all runtime settings, read from the environment at startup.
type Config struct {
	DatabaseURL string
	AdminAddr   string
	LogLevel    slog.Level
}

// Load reads Config from environment variables. It reports every
// problem at once rather than stopping at the first.
func Load() (Config, error) {
	cfg := Config{
		AdminAddr: "127.0.0.1:8080",
		LogLevel:  slog.LevelInfo,
	}
	var errs []error

	if v := os.Getenv("DATABASE_URL"); v != "" {
		cfg.DatabaseURL = v
	} else {
		errs = append(errs, errors.New("DATABASE_URL is required"))
	}

	if v := os.Getenv("ADMIN_ADDR"); v != "" {
		cfg.AdminAddr = v
	}

	if v := os.Getenv("LOG_LEVEL"); v != "" {
		if err := cfg.LogLevel.UnmarshalText([]byte(v)); err != nil {
			errs = append(errs, fmt.Errorf("LOG_LEVEL: %w", err))
		}
	}

	if err := errors.Join(errs...); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Check to ensure Config includes LogValue() method
// Config must satisfy slog.LogValuer so logging it never leaks passwords
var _ slog.LogValuer = Config{}

// LogValue implements slog.LogValuer, so logging a Config never
// leaks the database password.
func (c Config) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("database_url", redactURL(c.DatabaseURL)),
		slog.String("admin_addr", c.AdminAddr),
		slog.String("log_level", c.LogLevel.String()),
	)
}

func redactURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" {
		return "[redacted]"
	}
	return u.Redacted()
}
