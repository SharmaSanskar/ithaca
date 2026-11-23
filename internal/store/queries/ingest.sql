-- name: ClaimIdempotencyKey :one
-- Claim a key, or take over one whose 24h window has expired.
--
-- Runs first inside the ingest transaction. A concurrent request with the
-- same key blocks on the unique index until this transaction commits or
-- rolls back, then either sees a live row (returns nothing → caller replays
-- or rejects) or proceeds as the new owner. No 'in_flight' state is ever
-- committed, so there is nothing to recover after a crash.
INSERT INTO idempotency_keys (tenant_id, key, request_hash, response_code, response_body,
                              event_id, created_at, expires_at)
VALUES (@tenant_id, @key, @request_hash, @response_code, @response_body,
        @event_id, @created_at, @expires_at)
ON CONFLICT (tenant_id, key) DO UPDATE
SET request_hash  = EXCLUDED.request_hash,
    response_code = EXCLUDED.response_code,
    response_body = EXCLUDED.response_body,
    event_id      = EXCLUDED.event_id,
    created_at    = EXCLUDED.created_at,
    expires_at    = EXCLUDED.expires_at
WHERE idempotency_keys.expires_at <= now()
RETURNING event_id;

-- name: GetIdempotencyKey :one
SELECT request_hash, response_code, response_body, event_id
FROM idempotency_keys
WHERE tenant_id = @tenant_id AND key = @key AND expires_at > now();

-- name: InsertEvent :exec
INSERT INTO events (id, tenant_id, event_type, ordering_key, payload, idempotency_key,
                    traceparent, created_at)
VALUES (@id, @tenant_id, @event_type, sqlc.narg(ordering_key), @payload,
        sqlc.narg(idempotency_key), sqlc.narg(traceparent), @created_at);

-- name: EnqueueDelivery :one
-- Upsert the stream and append one delivery to it, in ONE statement.
--
-- seq comes from delivery_streams.next_seq, incremented under the row lock
-- that ON CONFLICT DO UPDATE holds until commit. Concurrent enqueues to the
-- same stream therefore serialize, and seq order equals commit order:
-- gapless and monotonic, which a BIGSERIAL cannot guarantee.
--
-- State handling on conflict: never un-pause a paused or circuit-open stream,
-- and never yank a leased stream out from under its worker. An idle stream
-- becomes ready and visible now.
WITH s AS (
    INSERT INTO delivery_streams AS ds
        (tenant_id, subscription_id, endpoint_id, stream_key, ordered,
         state, next_visible_at, next_seq)
    VALUES (@tenant_id, @subscription_id, @endpoint_id, @stream_key, @ordered,
            'ready', now(), 2)
    ON CONFLICT (subscription_id, stream_key) DO UPDATE
    SET next_seq        = ds.next_seq + 1,
        state           = CASE WHEN ds.state IN ('paused', 'circuit_open', 'leased')
                               THEN ds.state ELSE 'ready' END,
        next_visible_at = CASE WHEN ds.state IN ('paused', 'circuit_open')
                               THEN ds.next_visible_at
                               WHEN ds.state = 'idle' THEN now()
                               ELSE LEAST(ds.next_visible_at, now()) END
    RETURNING ds.id, ds.next_seq - 1 AS assigned_seq
)
INSERT INTO deliveries (id, tenant_id, stream_id, subscription_id, endpoint_id,
                        event_id, seq, state, next_attempt_at)
SELECT @delivery_id, @tenant_id, s.id, @subscription_id, @endpoint_id,
       @event_id, s.assigned_seq, 'pending', now()
FROM s
RETURNING id, stream_id, seq;
