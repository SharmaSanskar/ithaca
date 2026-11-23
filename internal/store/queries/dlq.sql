-- name: ListDeadLetters :many
-- The dead-letter queue, newest first, keyset-paginated on (completed_at, id)
-- so pages stay stable while new dead letters arrive.
SELECT dh.id, dh.endpoint_id, dh.subscription_id, dh.event_id, dh.attempt, dh.dead_reason,
       dh.last_error, dh.completed_at, dh.replayed_at, dh.replayed_as, dh.replay_of,
       ev.event_type,
       -- COALESCE: comparing against a NULL paused_delivery_id yields NULL,
       -- not false, and NULL does not scan into a bool.
       COALESCE(ds.paused_delivery_id = dh.id, false)::bool AS blocks_stream
FROM deliveries_history dh
JOIN events ev ON ev.id = dh.event_id
LEFT JOIN delivery_streams ds ON ds.id = dh.stream_id
WHERE dh.tenant_id = @tenant_id
  AND dh.final_state = 'dead'
  AND (sqlc.narg(endpoint_id)::uuid IS NULL OR dh.endpoint_id = sqlc.narg(endpoint_id))
  AND (sqlc.narg(before_completed_at)::timestamptz IS NULL
       OR (dh.completed_at, dh.id) < (sqlc.narg(before_completed_at), sqlc.narg(before_id)::uuid))
ORDER BY dh.completed_at DESC, dh.id DESC
LIMIT @max_rows;

-- name: ClaimDeadLetters :many
-- Claim dead letters for replay. replayed_at IS NULL is what makes replay
-- idempotent: a second, concurrent request for the same ids blocks on these
-- row locks, then re-checks the predicate and claims nothing.
UPDATE deliveries_history dh
SET replayed_at = now()
FROM (
    SELECT c.id FROM deliveries_history c
    WHERE c.tenant_id = @tenant_id
      AND c.final_state = 'dead'
      AND c.replayed_at IS NULL
      AND (cardinality(@ids::uuid[]) = 0 OR c.id = ANY(@ids::uuid[]))
      AND (sqlc.narg(endpoint_id)::uuid IS NULL OR c.endpoint_id = sqlc.narg(endpoint_id))
    ORDER BY c.completed_at
    LIMIT @max_rows
    FOR UPDATE
) pick
WHERE dh.id = pick.id
RETURNING dh.id, dh.tenant_id, dh.stream_id, dh.subscription_id, dh.endpoint_id, dh.event_id, dh.seq;

-- name: ReplayDelivery :one
-- Re-create one dead letter as a new pending delivery: new delivery id, SAME
-- event id (so a well-behaved subscriber dedupes it by Ithaca-Id).
--
-- Where it lands in the stream:
--   * the stream is PAUSED ON THIS dead letter (an ordered head that died):
--     reuse its ORIGINAL seq. It is still free and lower than everything
--     queued behind it, so original order is restored exactly. Then resume.
--   * otherwise: append at the tail with a fresh seq, like a new event.
WITH old AS (
    SELECT o.id, o.next_seq, (o.paused_delivery_id IS NOT DISTINCT FROM @dead_id::uuid) AS resumes
    FROM delivery_streams o
    WHERE o.id = @stream_id
    FOR UPDATE
),
s AS (
    UPDATE delivery_streams ds
    SET next_seq           = CASE WHEN old.resumes THEN ds.next_seq ELSE ds.next_seq + 1 END,
        paused_delivery_id = CASE WHEN old.resumes THEN NULL ELSE ds.paused_delivery_id END,
        paused_at          = CASE WHEN old.resumes THEN NULL ELSE ds.paused_at END,
        state = CASE WHEN old.resumes OR ds.state = 'idle' THEN 'ready' ELSE ds.state END,
        next_visible_at = CASE
            WHEN old.resumes OR ds.state = 'idle' THEN now() + interval '1 millisecond' * @delay_ms::bigint
            WHEN ds.state = 'ready' THEN LEAST(ds.next_visible_at, now() + interval '1 millisecond' * @delay_ms::bigint)
            ELSE ds.next_visible_at END
    FROM old
    WHERE ds.id = old.id
    RETURNING ds.id, CASE WHEN old.resumes THEN @original_seq::bigint ELSE old.next_seq END AS seq
)
INSERT INTO deliveries (id, tenant_id, stream_id, subscription_id, endpoint_id, event_id, seq,
                        state, attempt, next_attempt_at, replay_of)
SELECT @new_id, @tenant_id, s.id, @subscription_id, @endpoint_id, @event_id, s.seq,
       'pending', 0, now() + interval '1 millisecond' * @delay_ms::bigint, @dead_id
FROM s
RETURNING id, seq;

-- name: SetReplayedAs :exec
UPDATE deliveries_history SET replayed_as = @replayed_as WHERE id = @id;
