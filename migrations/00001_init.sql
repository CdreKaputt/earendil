-- +goose Up
CREATE TABLE sites (
    id                          text PRIMARY KEY
                                CHECK (id ~ '^[a-z0-9][a-z0-9-]{0,62}$'),
    active                      boolean NOT NULL DEFAULT true,
    managed_by                  text NOT NULL DEFAULT 'manual'
                                CHECK (managed_by IN ('manual', 'repo')),
    allowed_recipient_domains   text[] NOT NULL DEFAULT '{}',
    max_recipients              integer NOT NULL DEFAULT 5
                                CHECK (max_recipients > 0),
    rate_limit_per_hour         integer NOT NULL DEFAULT 60
                                CHECK (rate_limit_per_hour > 0),
    created_at                  timestamptz NOT NULL DEFAULT now(),
    updated_at                  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE site_config_versions (
    site_id         text NOT NULL REFERENCES sites (id) ON DELETE CASCADE,
    version         integer NOT NULL CHECK (version > 0),
    config          jsonb NOT NULL CHECK (jsonb_typeof(config) = 'object'),
    content_hash    text NOT NULL,
    source          text NOT NULL CHECK (source IN ('manual', 'ci')),
    git_sha         text,
    created_at      timestamptz NOT NULL DEFAULT now(),

    PRIMARY KEY (site_id, version),
    CHECK (git_sha IS NULL OR source = 'ci')
);

CREATE TABLE entries (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    site_id         text NOT NULL REFERENCES sites (id),
    form_id         text NOT NULL,
    fields          jsonb NOT NULL CHECK (jsonb_typeof(fields) = 'object'),
    idempotency_key uuid NOT NULL,
    config_version  integer NOT NULL,
    received_at     timestamptz NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),

    UNIQUE (site_id, idempotency_key),
    FOREIGN KEY (site_id, config_version)
        REFERENCES site_config_versions (site_id, version)
);

CREATE INDEX entries_site_created_idx ON entries (site_id, created_at DESC);

CREATE TABLE notifications (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    entry_id        bigint NOT NULL REFERENCES entries (id) ON DELETE CASCADE,
    recipient       text NOT NULL,
    status          text NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'sent', 'failed')),
    attempts        integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    last_error      text,
    sent_at         timestamptz,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),

    UNIQUE (entry_id, recipient),
    CHECK ((status = 'sent') = (sent_at IS NOT NULL))
);

CREATE INDEX notifications_due_idx ON notifications (next_attempt_at)
    WHERE status = 'pending';

-- +goose Down

DROP TABLE notifications;
DROP TABLE entries;
DROP TABLE site_config_versions;
DROP TABLE sites;
