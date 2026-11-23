-- +goose Up
-- Phase 3: endpoint verification, the circuit breaker, ordered-stream pausing
-- and dead-letter replay. Forward-only, like every migration here.

-- ── endpoint verification ───────────────────────────────────────────────────
-- An endpoint must echo a signed challenge before it receives anything.
-- Without that, a service that POSTs to any URL a stranger types in is a
-- rentable Layer-7 amplifier. Fanout already requires state = 'enabled', so a
-- pending endpoint gets no delivery rows at all.
ALTER TABLE endpoints DROP CONSTRAINT endpoints_state_check;
ALTER TABLE endpoints ADD CONSTRAINT endpoints_state_check
    CHECK (state IN ('pending_verification', 'verification_failed', 'enabled', 'disabled', 'auto_disabled'));

ALTER TABLE endpoints
    ADD COLUMN verified_at              timestamptz,
    ADD COLUMN verification_attempts    int NOT NULL DEFAULT 0,
    ADD COLUMN verification_next_at     timestamptz,
    ADD COLUMN last_verification_error  text;

-- The verifier's work list: only pending endpoints, so it stays tiny.
CREATE INDEX endpoints_verification_idx ON endpoints (verification_next_at NULLS FIRST)
    WHERE state = 'pending_verification';

-- ── circuit breaker ─────────────────────────────────────────────────────────
-- Breaker state lives on the endpoint row, next to the streams it parks.
-- Postgres, not Redis: the open/close decision is made inside the same
-- transaction as the ack that observed the failure, so it can never disagree
-- with the delivery record.
ALTER TABLE endpoints
    ADD COLUMN circuit_state            text NOT NULL DEFAULT 'closed'
                                          CHECK (circuit_state IN ('closed', 'open', 'half_open')),
    ADD COLUMN consecutive_failures     int NOT NULL DEFAULT 0,
    ADD COLUMN circuit_opened_at        timestamptz,
    ADD COLUMN circuit_next_probe_at    timestamptz,
    ADD COLUMN circuit_cooldown_ms      bigint NOT NULL DEFAULT 0,
    -- In half_open, exactly one stream is allowed to deliver: this one.
    ADD COLUMN circuit_probe_stream_id  bigint,
    ADD COLUMN circuit_probe_until      timestamptz;

-- The breaker loop's work list: only non-closed endpoints.
CREATE INDEX endpoints_circuit_idx ON endpoints (circuit_next_probe_at)
    WHERE circuit_state <> 'closed';

-- Parked streams per endpoint, for unparking on close and picking a probe.
CREATE INDEX delivery_streams_parked_idx ON delivery_streams (endpoint_id, next_visible_at)
    WHERE state = 'circuit_open';

-- ── dead-letter replay ──────────────────────────────────────────────────────
-- A dead letter can be replayed once. Claiming it sets replayed_at in the same
-- transaction that creates the new delivery, which is what makes a
-- double-clicked "Replay" a no-op rather than a duplicate. If the replay dies
-- too, it becomes a NEW dead letter (replay_of points back) and can be
-- replayed again.
ALTER TABLE deliveries_history
    ADD COLUMN replayed_at  timestamptz,
    ADD COLUMN replayed_as  uuid;

-- The DLQ browse index, rebuilt to support keyset pagination with an
-- id tiebreak.
DROP INDEX dh_dlq_idx;
CREATE INDEX dh_dlq_idx ON deliveries_history (tenant_id, completed_at DESC, id DESC)
    WHERE final_state = 'dead';
