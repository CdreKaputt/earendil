-- name: InsertNotification :exec
INSERT INTO notifications (entry_id, recipient)
VALUES ($1, $2);