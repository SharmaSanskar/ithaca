-- The Postgres queue driver. See docs/design/delivery-engine.md §2.
--
-- Rule for every statement in this file: it commits on its own and is never
-- held open across an outbound HTTP request. Workers hold no connection while
-- a delivery is in flight.

-- name: LeaseStreams :many
-- Lease up to @max_streams runnable streams in ONE statement.
--
-- SKIP LOCKED makes concurrent dispatchers take disjoint sets instead of
-- queueing behind each other. The scan runs on the partial index over
-- state='ready', so it touches only runnable streams however large the
-- backlog.
--
-- The lease length scales with the endpoint's timeout, so a normal delivery
-- never races its own lease: max(min_lease, 2*timeout + padding).
WITH candidate AS (
    SELECT c.id
    FROM delivery_streams c
    WHERE c.state = 'ready' AND c.next_visible_at <= now()
    ORDER BY c.next_visible_at, c.id
    FOR UPDATE SKIP LOCKED
    LIMIT @max_streams
)
UPDATE delivery_streams s
SET state        = 'leased',
    locked_by    = @worker_id,
    lock_token   = gen_random_uuid(),
    locked_until = now() + interval '1 millisecond'
                   * GREATEST(@min_lease_ms::bigint, e.timeout_ms * 2 + @lease_padding_ms::bigint)
FROM candidate, endpoints e
WHERE s.id = candidate.id AND e.id = s.endpoint_id
RETURNING s.id, s.lock_token, s.tenant_id, s.subscription_id, s.endpoint_id, s.ordered, s.stream_key,
          e.url, e.state AS endpoint_state, e.timeout_ms, e.max_concurrency,
          e.signing_key_id, e.signing_secret,
          GREATEST(@min_lease_ms::bigint, e.timeout_ms * 2 + @lease_padding_ms::bigint)::bigint AS lease_ms;

-- name: FetchOrdered :many
-- ORDERED: strict head of stream, skip nothing. The worker stops at the first
-- row that is not yet due, because delivering seq N+1 while N waits would be
-- exactly the reordering this mode exists to prevent. No locks: the stream
-- lease IS the mutual exclusion.
SELECT d.id, d.event_id, d.seq, d.attempt, (d.next_attempt_at <= now())::bool AS due,
       ev.event_type, ev.payload, ev.traceparent, ev.created_at AS event_created_at
FROM deliveries d
JOIN events ev ON ev.id = d.event_id
WHERE d.stream_id = @stream_id
ORDER BY d.seq
LIMIT @max_rows;

-- name: FetchUnordered :many
-- UNORDERED: only rows that are due, so one backing-off delivery never blocks
-- the lane. This WHERE clause is the entire behavioral difference between the
-- two modes.
SELECT d.id, d.event_id, d.seq, d.attempt, true AS due,
       ev.event_type, ev.payload, ev.traceparent, ev.created_at AS event_created_at
FROM deliveries d
JOIN events ev ON ev.id = d.event_id
WHERE d.stream_id = @stream_id AND d.next_attempt_at <= now()
ORDER BY d.next_attempt_at, d.seq
LIMIT @max_rows;

-- name: RenewLease :one
-- Heartbeat. Zero rows means the lease is gone (reaped and possibly re-leased
-- by another worker): the worker must abandon the stream immediately.
UPDATE delivery_streams
SET locked_until = now() + interval '1 millisecond' * @lease_ms::bigint
WHERE id = @id AND lock_token = @lock_token AND state = 'leased'
RETURNING locked_until;

-- name: FenceStream :one
-- The fencing check that opens every ack transaction (Kleppmann's fencing
-- token, in one statement). A worker partitioned from the database for a
-- minute may still be mid-request when the reaper hands its stream to someone
-- else; when it comes back, this returns no row and its ack rolls back. FOR
-- UPDATE is held only for the few milliseconds of the ack transaction.
SELECT id FROM delivery_streams
WHERE id = @id AND lock_token = @lock_token AND state = 'leased' AND locked_until > now()
FOR UPDATE;

-- name: InsertAttempts :exec
-- Every HTTP attempt in an ack batch, as one multi-row insert.
INSERT INTO delivery_attempts (id, tenant_id, delivery_id, endpoint_id, attempt, started_at,
                               duration_ms, outcome, status_code, resp_body, error_class,
                               error_detail, worker_id, trace_id)
SELECT r.id, @tenant_id, r.delivery_id, @endpoint_id, r.attempt, r.started_at,
       r.duration_ms, r.outcome, NULLIF(r.status_code, 0), r.resp_body, NULLIF(r.error_class, ''),
       NULLIF(r.error_details_, ''), @worker_id, NULLIF(r.trace_id, '')
FROM (
    -- Several unnest() calls in one SELECT list advance in lockstep, zipping
    -- the parallel arrays into rows (sqlc does not model the multi-argument
    -- unnest form).
    SELECT unnest(@ids::uuid[])                AS id,
           unnest(@delivery_ids::uuid[])       AS delivery_id,
           unnest(@attempts::int[])            AS attempt,
           unnest(@started_ats::timestamptz[]) AS started_at,
           unnest(@duration_ms::int[])         AS duration_ms,
           unnest(@outcomes::text[])           AS outcome,
           unnest(@status_codes::int[])        AS status_code,
           unnest(@resp_bodies::bytea[])       AS resp_body,
           unnest(@error_classes::text[])      AS error_class,
           unnest(@error_details::text[])      AS error_details_,
           unnest(@trace_ids::text[])          AS trace_id
) r;

-- name: CompleteDeliveries :execrows
-- Terminal deliveries (succeeded or dead) are MOVED, not updated: deleted from
-- the hot table and inserted into history in one statement. `deliveries` stays
-- sized by backlog, so the queue fits in memory however long the system runs.
WITH r AS (
    SELECT unnest(@ids::uuid[])          AS id,
           unnest(@attempts::int[])      AS attempt,
           unnest(@final_states::text[]) AS final_state,
           unnest(@dead_reasons::text[]) AS dead_reason,
           unnest(@last_errors::text[])  AS last_error
),
moved AS (
    DELETE FROM deliveries d
    USING r
    WHERE d.id = r.id AND d.stream_id = @stream_id
    RETURNING d.id, d.tenant_id, d.stream_id, d.subscription_id, d.endpoint_id, d.event_id,
              d.seq, d.replay_of, d.created_at
)
INSERT INTO deliveries_history (id, tenant_id, stream_id, subscription_id, endpoint_id, event_id,
                                seq, attempt, last_error, replay_of, created_at, final_state,
                                dead_reason, completed_at)
SELECT m.id, m.tenant_id, m.stream_id, m.subscription_id, m.endpoint_id, m.event_id,
       m.seq, r.attempt, NULLIF(r.last_error, ''), m.replay_of, m.created_at, r.final_state,
       NULLIF(r.dead_reason, ''), now()
FROM moved m
JOIN r ON r.id = m.id;

-- name: RescheduleDeliveries :execrows
-- Retries (attempt consumed) and deferrals (rate limited: attempt NOT
-- consumed) both land here; the caller decides the attempt count. Due times
-- use the DATABASE clock, so every dispatcher replica agrees on "now".
UPDATE deliveries d
SET attempt         = r.attempt,
    state           = 'retrying',
    next_attempt_at = now() + interval '1 millisecond' * r.delay_ms,
    last_error      = NULLIF(r.last_error, '')
FROM (
    SELECT unnest(@ids::uuid[])         AS id,
           unnest(@attempts::int[])     AS attempt,
           unnest(@delay_ms::bigint[])  AS delay_ms,
           unnest(@last_errors::text[]) AS last_error
) r
WHERE d.id = r.id AND d.stream_id = @stream_id;

-- name: ReleaseStream :execrows
-- Give the stream back and recompute when it is next runnable.
--
--   ordered:   the HEAD's due time (head-of-line semantics)
--   unordered: the earliest due time of anything pending
--   nothing pending: idle (drops out of the dequeue index entirely)
--
-- @not_before_ms pushes visibility out further (endpoint disabled, rate
-- limited). Fenced by lock_token, so a worker that lost its lease cannot
-- release a stream someone else now holds.
--
-- Race with ingest: an enqueue onto this stream either commits before this
-- statement (and is counted below) or blocks on this row and then sees
-- 'idle'/'ready' and makes it ready itself. No wakeup is ever lost.
UPDATE delivery_streams ds
SET state           = CASE WHEN h.t IS NULL THEN 'idle' ELSE 'ready' END,
    next_visible_at = GREATEST(COALESCE(h.t, now()), now() + interval '1 millisecond' * @not_before_ms::bigint),
    locked_by       = NULL,
    lock_token      = NULL,
    locked_until    = NULL
FROM (
    SELECT CASE WHEN @ordered::bool
                THEN (SELECT d.next_attempt_at FROM deliveries d WHERE d.stream_id = @id ORDER BY d.seq LIMIT 1)
                ELSE (SELECT min(d.next_attempt_at) FROM deliveries d WHERE d.stream_id = @id)
           END AS t
) h
WHERE ds.id = @id AND ds.lock_token = @lock_token AND ds.state = 'leased';

-- name: ReapExpiredLeases :many
-- Reclaim leases whose worker died (SIGKILL, OOM, partition). Uses the partial
-- index on state='leased', which holds O(active workers) entries. A nonzero
-- steady-state reap rate means leases are too short or workers keep dying.
WITH expired AS (
    SELECT x.id FROM delivery_streams x
    WHERE x.state = 'leased' AND x.locked_until < now()
    ORDER BY x.locked_until
    FOR UPDATE SKIP LOCKED
    LIMIT @max_streams
)
UPDATE delivery_streams ds
SET state = 'ready', locked_by = NULL, lock_token = NULL, locked_until = NULL, next_visible_at = now()
FROM expired
WHERE ds.id = expired.id
RETURNING ds.id;

-- name: QueueStats :one
-- Gauges for Prometheus, and the same numbers KEDA will scale on.
SELECT
    (SELECT count(*) FROM delivery_streams rs WHERE rs.state = 'ready' AND rs.next_visible_at <= now())::bigint AS runnable_streams,
    (SELECT count(*) FROM delivery_streams ls WHERE ls.state = 'leased')::bigint AS leased_streams,
    (SELECT count(*) FROM deliveries)::bigint AS pending_deliveries,
    (SELECT COALESCE(EXTRACT(EPOCH FROM now() - min(od.next_attempt_at)), 0)
       FROM deliveries od WHERE od.next_attempt_at <= now())::float8 AS oldest_due_seconds;
