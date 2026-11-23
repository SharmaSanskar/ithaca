-- Circuit breaker. See internal/breaker for the state machine itself; these
-- statements only load and persist it.

-- name: LockEndpointCircuit :one
-- Taken inside the ack transaction, only when the ack carries a failure or a
-- probe result. The healthy path never touches the endpoint row.
SELECT circuit_state, consecutive_failures, circuit_cooldown_ms, circuit_probe_stream_id, timeout_ms
FROM endpoints
WHERE id = @id
FOR UPDATE;

-- name: ResetEndpointFailures :exec
-- Healthy-path reset. The WHERE clause makes it a no-op (and lock-free) when
-- there is nothing to reset, which is the overwhelmingly common case.
UPDATE endpoints SET consecutive_failures = 0
WHERE id = @id AND consecutive_failures > 0 AND circuit_state = 'closed';

-- name: UpdateEndpointCircuit :exec
UPDATE endpoints
SET circuit_state           = @circuit_state,
    consecutive_failures    = @consecutive_failures,
    circuit_cooldown_ms     = @cooldown_ms,
    circuit_next_probe_at   = CASE WHEN @circuit_state = 'open'
                                   THEN now() + interval '1 millisecond' * @next_probe_in_ms::bigint
                                   ELSE NULL END,
    circuit_opened_at       = CASE WHEN @circuit_state = 'closed' THEN NULL
                                   ELSE COALESCE(circuit_opened_at, now()) END,
    circuit_probe_stream_id = NULL,
    circuit_probe_until     = NULL,
    updated_at              = now()
WHERE id = @id;

-- name: UnparkEndpointStreams :execrows
-- The breaker closed. Every parked stream becomes runnable, but NOT all at
-- once: each gets a random delay within @spread_ms. Reopening every stream at
-- the same instant recreates exactly the thundering herd the breaker existed
-- to prevent, aimed at an endpoint that has just recovered.
UPDATE delivery_streams
SET state = 'ready', next_visible_at = now() + interval '1 millisecond' * floor(random() * @spread_ms::bigint)
WHERE endpoint_id = @endpoint_id AND state = 'circuit_open';

-- name: AutoDisableEndpoint :exec
-- 410 Gone means "stop asking". Honor it.
UPDATE endpoints
SET state = 'auto_disabled', disabled_reason = @reason, updated_at = now()
WHERE id = @id AND state = 'enabled';

-- name: ParkOpenEndpointStreams :execrows
-- Breaker loop: park runnable streams of endpoints whose circuit is open. A
-- parked stream leaves the dequeue index, so a dead endpoint's backlog stops
-- competing with healthy traffic in every SKIP LOCKED scan. The half-open
-- probe stream is the one exception.
UPDATE delivery_streams s
SET state = 'circuit_open'
FROM endpoints e
WHERE s.endpoint_id = e.id
  AND s.state = 'ready'
  AND e.circuit_state IN ('open', 'half_open')
  AND e.circuit_probe_stream_id IS DISTINCT FROM s.id;

-- name: ExpireStaleProbes :execrows
-- A probe that never reported back (no due message, worker died) must not
-- wedge the breaker in half_open forever: return to open, probe again now.
UPDATE endpoints
SET circuit_state = 'open', circuit_next_probe_at = now(),
    circuit_probe_stream_id = NULL, circuit_probe_until = NULL
WHERE circuit_state = 'half_open' AND circuit_probe_until < now();

-- name: ClaimDueProbes :many
-- Open circuits whose cooldown has elapsed. SKIP LOCKED + the caller's
-- transaction mean that across any number of dispatchers, each endpoint gets
-- exactly one probe.
SELECT id, timeout_ms
FROM endpoints
WHERE circuit_state = 'open' AND circuit_next_probe_at <= now()
ORDER BY circuit_next_probe_at
FOR UPDATE SKIP LOCKED
LIMIT 100;

-- name: PickProbeStream :one
SELECT id FROM delivery_streams
WHERE endpoint_id = @endpoint_id AND state = 'circuit_open'
ORDER BY next_visible_at, id
LIMIT 1
FOR UPDATE SKIP LOCKED;

-- name: StartProbe :exec
-- half_open: exactly one stream may deliver, and only one message (the worker
-- enforces that). Bounded by probe_until so it cannot stick.
WITH ep AS (
    UPDATE endpoints pe
    SET circuit_state = 'half_open', circuit_probe_stream_id = @stream_id,
        circuit_probe_until = now() + interval '1 millisecond' * (pe.timeout_ms + 10000)
    WHERE pe.id = @endpoint_id AND pe.circuit_state = 'open'
    RETURNING pe.id
)
UPDATE delivery_streams s
SET state = 'ready', next_visible_at = now()
FROM ep
WHERE s.id = @stream_id AND s.state = 'circuit_open';
