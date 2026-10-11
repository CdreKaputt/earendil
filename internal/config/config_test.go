package config

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func TestLoad(t *testing.T) {
	const dsn = "postgres://localhost/earendil"
	defaults := Config{
		DatabaseURL: dsn,
		AdminAddr:   "127.0.0.1:8080",
		LogLevel:    slog.LevelInfo,
	}

	tests := []struct {
		name    string
		env     map[string]string
		want    Config
		wantErr []string
	}{
		{
			name: "defaults",
			env:  map[string]string{"DATABASE_URL": dsn},
			want: defaults,
		},
		{
			name: "overrides",
			env: map[string]string{
				"DATABASE_URL": dsn,
				"ADMIN_ADDR":   "127.0.0.1:9090",
				"LOG_LEVEL":    "debug",
			},
			want: Config{
				DatabaseURL: dsn,
				AdminAddr:   "127.0.0.1:9090",
				LogLevel:    slog.LevelDebug,
			},
		},
		{
			name: "empty admin addr falls back to localhost",
			env:  map[string]string{"DATABASE_URL": dsn, "ADMIN_ADDR": ""},
			want: defaults,
		},
		{
			name:    "missing database url",
			env:     map[string]string{},
			wantErr: []string{"DATABASE_URL"},
		},
		{
			name:    "reports every error at once",
			env:     map[string]string{"LOG_LEVEL": "loud"},
			wantErr: []string{"DATABASE_URL", "LOG_LEVEL"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, k := range []string{"DATABASE_URL", "ADMIN_ADDR", "LOG_LEVEL"} {
				t.Setenv(k, "")
			}
			for k, v := range tt.env {
				t.Setenv(k, v)
			}

			got, err := Load()

			if len(tt.wantErr) > 0 {
				if err == nil {
					t.Fatalf("Load() error = nil, want error containing %q", tt.wantErr)
				}
				for _, s := range tt.wantErr {
					if !strings.Contains(err.Error(), s) {
						t.Errorf("Load() error = %q, want it to contain %q", err, s)
					}
				}
				return
			}

			if err != nil {
				t.Fatalf("Load() unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("Load() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestConfigLogValueRedactsPassword(t *testing.T) {
	tests := []struct {
		name string
		dsn  string
	}{
		{"url form", "postgres://earendil:hunter2@localhost:5432/earendil"},
		{"key value form", "host=localhost user=earendil password=hunter2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&buf, nil))

			logger.Info("test", "config", Config{DatabaseURL: tt.dsn})

			if strings.Contains(buf.String(), "hunter2") {
				t.Errorf("password leaked into log output: %s", buf.String())
			}
		})
	}
}
