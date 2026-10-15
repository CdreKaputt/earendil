-- name: InsertEntry :one
INSERT INTO entries (
    site_id, form_id, fields, idempotency_key, config_version, received_at
) VALUES (
    $1, $2, $3, $4, $5, $6
)
ON CONFLICT (site_id, idempotency_key) DO NOTHING
RETURNING id;

-- name: GetEntryIDByIdempotencyKey :one
SELECT id FROM entries
WHERE site_id = $1 AND idempotency_key = $2;
