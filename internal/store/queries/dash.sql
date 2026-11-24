-- Read-only queries for the live dashboard and the ws hub.

-- name: GetTenantByName :one
SELECT * FROM tenants WHERE name = @name ORDER BY created_at LIMIT 1;

-- name: LiveEndpoints :many
-- What the hub polls once a second per watched tenant: each endpoint's state,
-- circuit and backlog. updated_at is the endpoint's revision: when it moves,
-- the hub emits a delta, which reconciles anything pub/sub dropped.
SELECT e.id, e.description, e.url, e.state, e.circuit_state, e.consecutive_failures,
       e.circuit_next_probe_at, e.updated_at,
       (SELECT count(*) FROM deliveries d WHERE d.endpoint_id = e.id)::bigint AS depth
FROM endpoints e
WHERE e.tenant_id = @tenant_id
ORDER BY e.created_at, e.id;

-- name: OpenDeadLetterCount :one
SELECT count(*)::bigint FROM deliveries_history
WHERE tenant_id = @tenant_id AND final_state = 'dead' AND replayed_at IS NULL;

-- name: PausedStreamCount :one
SELECT count(*)::bigint FROM delivery_streams WHERE tenant_id = @tenant_id AND state = 'paused';

-- name: ListRecentEvents :many
-- The event feed, newest first, keyset-paginated. Delivery status is
-- summarized per event: pending (still in the queue) and terminal counts.
SELECT ev.id, ev.event_type, ev.ordering_key, ev.created_at,
       (SELECT count(*) FROM deliveries d WHERE d.event_id = ev.id)::bigint AS pending,
       (SELECT count(*) FROM deliveries_history h WHERE h.event_id = ev.id AND h.final_state = 'succeeded')::bigint AS succeeded,
       (SELECT count(*) FROM deliveries_history h WHERE h.event_id = ev.id AND h.final_state = 'dead')::bigint AS dead
FROM events ev
WHERE ev.tenant_id = @tenant_id
  AND (sqlc.narg(before_created_at)::timestamptz IS NULL
       OR (ev.created_at, ev.id) < (sqlc.narg(before_created_at), sqlc.narg(before_id)::uuid))
ORDER BY ev.created_at DESC, ev.id DESC
LIMIT @max_rows;

-- name: GetEvent :one
SELECT * FROM events WHERE tenant_id = @tenant_id AND id = @id;

-- name: EventPendingDeliveries :many
-- An event's deliveries still in the queue.
SELECT id, endpoint_id, seq, attempt, state, next_attempt_at, last_error, replay_of
FROM deliveries WHERE event_id = @event_id;

-- name: EventDoneDeliveries :many
-- An event's terminal deliveries.
SELECT id, endpoint_id, seq, attempt, final_state, completed_at, last_error, replay_of, dead_reason
FROM deliveries_history WHERE event_id = @event_id;

-- name: DeliveryAttempts :many
SELECT delivery_id, attempt, started_at, duration_ms, outcome, status_code, error_class, error_detail
FROM delivery_attempts
WHERE delivery_id = ANY(@delivery_ids::uuid[])
ORDER BY delivery_id, attempt, started_at;

-- name: ThroughputSeries :many
-- Attempts per bucket over a recent window, with latency percentiles, for the
-- dashboard's time-series chart. Optionally for one endpoint.
SELECT to_timestamp(floor(EXTRACT(EPOCH FROM a.created_at) / @bucket_seconds::int) * @bucket_seconds::int)::timestamptz AS bucket,
       count(*) FILTER (WHERE a.outcome = 'success')::bigint AS succeeded,
       count(*) FILTER (WHERE a.outcome IN ('retryable', 'timeout', 'permanent'))::bigint AS failed,
       count(*) FILTER (WHERE a.outcome = 'rate_limited')::bigint AS rate_limited,
       COALESCE(percentile_cont(0.5) WITHIN GROUP (ORDER BY a.duration_ms), 0)::float8 AS p50_ms,
       COALESCE(percentile_cont(0.99) WITHIN GROUP (ORDER BY a.duration_ms), 0)::float8 AS p99_ms
FROM delivery_attempts a
WHERE a.tenant_id = @tenant_id
  AND a.created_at > now() - interval '1 second' * @window_seconds::int
  AND (sqlc.narg(endpoint_id)::uuid IS NULL OR a.endpoint_id = sqlc.narg(endpoint_id))
GROUP BY 1
ORDER BY 1;

-- name: IngestSeries :many
SELECT to_timestamp(floor(EXTRACT(EPOCH FROM ev.created_at) / @bucket_seconds::int) * @bucket_seconds::int)::timestamptz AS bucket,
       count(*)::bigint AS ingested
FROM events ev
WHERE ev.tenant_id = @tenant_id AND ev.created_at > now() - interval '1 second' * @window_seconds::int
GROUP BY 1
ORDER BY 1;

-- name: EndpointByID :one
SELECT * FROM endpoints WHERE tenant_id = @tenant_id AND id = @id;
