-- name: ClaimEndpointsToVerify :many
-- Claim pending endpoints by pushing verification_next_at forward (a short
-- lease), so concurrent verifiers never challenge the same endpoint twice and
-- the HTTP call happens with no transaction open.
UPDATE endpoints e
SET verification_next_at = now() + interval '1 minute'
FROM (
    SELECT c.id FROM endpoints c
    WHERE c.state = 'pending_verification'
      AND (c.verification_next_at IS NULL OR c.verification_next_at <= now())
    ORDER BY c.verification_next_at NULLS FIRST
    FOR UPDATE SKIP LOCKED
    LIMIT @max_endpoints
) due
WHERE e.id = due.id
RETURNING e.id, e.url, e.signing_key_id, e.signing_secret, e.timeout_ms, e.verification_attempts;

-- name: MarkEndpointVerified :exec
UPDATE endpoints
SET state = 'enabled', verified_at = now(), verification_next_at = NULL,
    last_verification_error = NULL, updated_at = now()
WHERE id = @id AND state = 'pending_verification';

-- name: RecordVerificationFailure :exec
UPDATE endpoints
SET verification_attempts   = verification_attempts + 1,
    last_verification_error = @error,
    verification_next_at    = now() + interval '1 millisecond' * @retry_in_ms::bigint,
    state = CASE WHEN verification_attempts + 1 >= @max_attempts::int
                 THEN 'verification_failed' ELSE state END,
    updated_at = now()
WHERE id = @id AND state = 'pending_verification';

-- name: RequestVerification :one
-- (Re-)start verification: after a failed handshake, or to re-enable an
-- endpoint that was disabled or auto-disabled. Also resets the breaker: a
-- freshly verified endpoint starts with a clean slate.
UPDATE endpoints
SET state = 'pending_verification', verification_attempts = 0, verification_next_at = NULL,
    last_verification_error = NULL, disabled_reason = NULL,
    circuit_state = 'closed', consecutive_failures = 0, circuit_cooldown_ms = 0,
    circuit_next_probe_at = NULL, circuit_opened_at = NULL,
    circuit_probe_stream_id = NULL, circuit_probe_until = NULL,
    updated_at = now()
WHERE tenant_id = @tenant_id AND id = @id
  AND state IN ('pending_verification', 'verification_failed', 'disabled', 'auto_disabled')
RETURNING *;
