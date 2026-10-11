-- name: GetSite :one
SELECT * FROM sites
WHERE id = $1;