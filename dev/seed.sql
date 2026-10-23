-- Local development data. Safe to run repeatedly.
INSERT INTO sites (id, allowed_recipient_domains)
VALUES ('dev-site', '{example.com}')
ON CONFLICT (id) DO NOTHING;

INSERT INTO site_config_versions (site_id, version, config, content_hash, source)
VALUES (
    'dev-site', 1,
    '{"forms": {"contact": {"recipients": ["owner@example.com"], "subject": "New contact form submission", "replyToField": "email", "fieldOrder": ["name", "email", "message"]}}}',
    'seed', 'manual'
)
ON CONFLICT (site_id, version) DO NOTHING;
