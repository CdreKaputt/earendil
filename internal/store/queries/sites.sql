-- name: GetSite :one
SELECT * FROM sites
WHERE id = $1;

-- name: GetLatestSiteConfig :one
SELECT * FROM site_config_versions
WHERE site_id = $1
ORDER BY version DESC
LIMIT 1;

