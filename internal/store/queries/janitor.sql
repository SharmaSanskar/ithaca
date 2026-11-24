-- name: DeleteExpiredIdempotencyKeys :execrows
-- Bounded batches: one huge DELETE holds locks and bloats WAL in a single
-- burst. The janitor loops until a batch comes back short.
DELETE FROM idempotency_keys
WHERE (tenant_id, key) IN (
    SELECT ik.tenant_id, ik.key FROM idempotency_keys ik
    WHERE ik.expires_at <= now()
    ORDER BY ik.expires_at
    LIMIT @batch_size
);

-- Retention. Batched DELETEs: this is the honest small-scale substitute for
-- time partitions + DROP TABLE (see ARCHITECTURE.md). Each statement deletes
-- at most @batch_size rows so no single sweep holds locks or bloats WAL.

-- name: DeleteOldAttempts :execrows
DELETE FROM delivery_attempts
WHERE id IN (SELECT a.id FROM delivery_attempts a
             WHERE a.created_at < now() - interval '1 second' * @older_than_seconds::bigint
             LIMIT @batch_size);

-- name: DeleteOldHistory :execrows
-- Dead letters are kept: an operator may still want to replay them.
DELETE FROM deliveries_history
WHERE id IN (SELECT h.id FROM deliveries_history h
             WHERE h.completed_at < now() - interval '1 second' * @older_than_seconds::bigint
               AND (h.final_state <> 'dead' OR h.replayed_at IS NOT NULL)
             LIMIT @batch_size);

-- name: DeleteOldEvents :execrows
-- Never an event that still has a pending delivery (the dispatcher reads its
-- payload at delivery time) or an unreplayed dead letter (replay needs it).
DELETE FROM events
WHERE id IN (SELECT ev.id FROM events ev
             WHERE ev.created_at < now() - interval '1 second' * @older_than_seconds::bigint
               AND NOT EXISTS (SELECT 1 FROM deliveries d WHERE d.event_id = ev.id)
               AND NOT EXISTS (SELECT 1 FROM deliveries_history h
                               WHERE h.event_id = ev.id AND h.final_state = 'dead' AND h.replayed_at IS NULL)
             LIMIT @batch_size);
