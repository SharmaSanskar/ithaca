-- Conditions the demo scenarios wait on, and the numbers they narrate.

-- name: DemoEndpointStatus :many
SELECT e.id, e.description, e.state, e.circuit_state,
       (SELECT count(*) FROM deliveries d WHERE d.endpoint_id = e.id)::bigint AS depth,
       (SELECT count(*) FROM deliveries_history h
         WHERE h.endpoint_id = e.id AND h.final_state = 'dead' AND h.replayed_at IS NULL)::bigint AS dlq
FROM endpoints e
WHERE e.tenant_id = @tenant_id;

-- name: RecentAttemptStats :one
SELECT count(*) FILTER (WHERE a.outcome = 'success')::bigint AS succeeded,
       count(*) FILTER (WHERE a.outcome IN ('retryable', 'timeout'))::bigint AS failed,
       count(*) FILTER (WHERE a.outcome = 'permanent')::bigint AS permanent,
       COALESCE(percentile_cont(0.99) WITHIN GROUP (ORDER BY a.duration_ms)
                FILTER (WHERE a.outcome = 'success'), 0)::float8 AS p99_ms
FROM delivery_attempts a
WHERE a.tenant_id = @tenant_id
  AND a.created_at > now() - interval '1 second' * @seconds::int
  AND (sqlc.narg(endpoint_id)::uuid IS NULL OR a.endpoint_id = sqlc.narg(endpoint_id));
