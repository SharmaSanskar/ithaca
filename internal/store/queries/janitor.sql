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
