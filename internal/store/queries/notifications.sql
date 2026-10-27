-- name: InsertNotification :exec
INSERT INTO notifications (entry_id, recipient)
VALUES ($1, $2);

-- name: ListDueNotifications :many
SELECT n.id, n.recipient, n.attempts,
       e.site_id, e.form_id, e.fields, e.received_at,
       c.config
FROM notifications n
JOIN entries e ON e.id = n.entry_id
JOIN site_config_versions c
  ON c.site_id = e.site_id AND c.version = e.config_version
WHERE n.status = 'pending' AND n.next_attempt_at <= now()
ORDER BY n.next_attempt_at
LIMIT $1;

-- name: MarkNotificationSent :exec
UPDATE notifications
SET status = 'sent', sent_at = now(), updated_at = now()
WHERE id = $1;
