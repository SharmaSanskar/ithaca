-- +goose Up
-- Ithaca's initial schema. Forward-only: there is no Down section on purpose.
-- A rollback on a queue that holds customer data is a new forward migration,
-- written deliberately, never a reflexive `goose down`.
--
-- Conventions (see docs/design/delivery-engine.md §1.1):
--   * uuid PKs are UUIDv7, generated in Go. Time-ordered, so B-tree inserts land
--     on the right edge instead of splitting random pages.
--   * timestamptz everywhere.
--   * tenant_id leads every tenant-scoped index (future sharding key).
--
-- Deliberately NOT here (scoped out, documented in ARCHITECTURE.md): time
-- partitioning of events / history / attempts. At demo scale a plain table is
-- the honest choice; partitioning is the documented path when retention by
-- DELETE starts to hurt.

CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- ── control plane ───────────────────────────────────────────────────────────

CREATE TABLE tenants (
    id          uuid PRIMARY KEY,
    name        text NOT NULL CHECK (length(name) BETWEEN 1 AND 128),
    state       text NOT NULL DEFAULT 'active' CHECK (state IN ('active', 'suspended')),
    created_at  timestamptz NOT NULL DEFAULT now()
);

-- API keys are 256-bit random secrets, never human-chosen, so a single SHA-256
-- is enough (no dictionary to attack) and costs ~1µs instead of bcrypt's
-- ~100ms. We store only the hash; lookup is by the public prefix.
CREATE TABLE api_keys (
    id          uuid PRIMARY KEY,
    tenant_id   uuid NOT NULL REFERENCES tenants (id),
    key_prefix  text NOT NULL,
    key_hash    bytea NOT NULL CHECK (length(key_hash) = 32),
    name        text NOT NULL DEFAULT '',
    revoked_at  timestamptz,
    created_at  timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX api_keys_prefix_uk ON api_keys (key_prefix);

CREATE TABLE endpoints (
    id               uuid PRIMARY KEY,
    tenant_id        uuid NOT NULL REFERENCES tenants (id),
    url              text NOT NULL,
    description      text NOT NULL DEFAULT '',
    state            text NOT NULL DEFAULT 'enabled'
                       CHECK (state IN ('enabled', 'disabled', 'auto_disabled')),
    disabled_reason  text,
    timeout_ms       int  NOT NULL DEFAULT 15000 CHECK (timeout_ms BETWEEN 1000 AND 60000),
    -- In-flight cap for unordered streams (ordered streams are serial anyway).
    max_concurrency  int  NOT NULL DEFAULT 8 CHECK (max_concurrency BETWEEN 1 AND 256),
    -- HMAC signing secret. Single active secret; key id is echoed in the
    -- signature header (`v1,k=<key_id>,...`) so rotation can be added later
    -- without a wire-format change.
    signing_key_id   text  NOT NULL,
    signing_secret   bytea NOT NULL,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX endpoints_tenant_idx ON endpoints (tenant_id, id);

-- A subscription is (endpoint × event-type filter): the fanout unit.
CREATE TABLE subscriptions (
    id           uuid PRIMARY KEY,
    tenant_id    uuid NOT NULL REFERENCES tenants (id),
    endpoint_id  uuid NOT NULL REFERENCES endpoints (id) ON DELETE CASCADE,
    -- ['user.*', 'order.created']; '*' matches everything.
    event_types  text[] NOT NULL CHECK (cardinality(event_types) BETWEEN 1 AND 64),
    state        text NOT NULL DEFAULT 'enabled' CHECK (state IN ('enabled', 'disabled')),
    -- The ordering knob (design doc §3). ordered=true → one stream per
    -- event.ordering_key. ordered=false → events hashed across lane_count
    -- synthetic streams, so unordered mode is "ordering over a synthetic key"
    -- and shares every code path with ordered mode.
    ordered      boolean NOT NULL DEFAULT false,
    lane_count   int     NOT NULL DEFAULT 16 CHECK (lane_count BETWEEN 1 AND 1024),
    created_at   timestamptz NOT NULL DEFAULT now()
);
-- Fanout lookup: every enabled subscription for a tenant, in id order. The id
-- order is load-bearing — see ingest.Service for the deadlock argument.
CREATE INDEX subscriptions_fanout_idx ON subscriptions (tenant_id, id) WHERE state = 'enabled';
CREATE INDEX subscriptions_endpoint_idx ON subscriptions (endpoint_id);

-- ── events ──────────────────────────────────────────────────────────────────

CREATE TABLE events (
    id               uuid PRIMARY KEY,
    tenant_id        uuid NOT NULL,
    event_type       text NOT NULL,
    ordering_key     text,
    -- The EXACT bytes received. This is what gets signed and what goes on the
    -- wire. jsonb would normalize key order and whitespace and silently break
    -- every subscriber's HMAC check.
    payload          bytea NOT NULL,
    idempotency_key  text,
    -- W3C traceparent of the ingest request. Delivery starts a NEW root span
    -- that links to this, instead of a child span that could be 6 hours long.
    traceparent      text,
    created_at       timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX events_tenant_created_idx ON events (tenant_id, created_at DESC, id DESC);

-- ── the queue ───────────────────────────────────────────────────────────────

-- A stream (lane) is the unit of leasing. At most one worker holds a stream at
-- a time, which is what makes per-key ordering hold without any per-message
-- locking. See docs/DECISIONS/adr-0003-stream-leasing.md.
CREATE TABLE delivery_streams (
    id                  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tenant_id           uuid NOT NULL,
    subscription_id     uuid NOT NULL REFERENCES subscriptions (id) ON DELETE CASCADE,
    endpoint_id         uuid NOT NULL,
    -- ordered: the customer's ordering_key. unordered: 'lane:<n>'.
    stream_key          text NOT NULL,
    ordered             boolean NOT NULL,

    state               text NOT NULL DEFAULT 'idle'
                          CHECK (state IN ('idle', 'ready', 'leased', 'paused', 'circuit_open')),
    next_visible_at     timestamptz NOT NULL DEFAULT now(),

    -- The lease. lock_token is a FENCING TOKEN: every worker mutation is
    -- conditional on it, so a worker whose lease was reaped cannot ack.
    locked_by           text,
    lock_token          uuid,
    locked_until        timestamptz,

    -- Per-stream sequence counter. seq is assigned from here under the row
    -- lock taken by ON CONFLICT DO UPDATE, so seq order == commit order.
    -- A BIGSERIAL would hand out numbers outside transaction order.
    next_seq            bigint NOT NULL DEFAULT 1,

    paused_at           timestamptz,
    paused_delivery_id  uuid,
    created_at          timestamptz NOT NULL DEFAULT now()
) WITH (
    -- Every lease/release is an UPDATE. Free space in the page lets Postgres
    -- keep new tuple versions on the same page.
    fillfactor = 70,
    -- Tiny table, highest update rate in the system: vacuum early and never
    -- throttle it.
    autovacuum_vacuum_scale_factor = 0.01,
    autovacuum_analyze_scale_factor = 0.02,
    autovacuum_vacuum_cost_delay = 0
);

-- Ingest upsert target and the per-stream serialization point.
CREATE UNIQUE INDEX delivery_streams_uk ON delivery_streams (subscription_id, stream_key);

-- ★ The dequeue index. Partial, so it holds only RUNNABLE streams — hundreds,
-- not millions — and lives permanently in shared_buffers.
CREATE INDEX delivery_streams_runnable_idx ON delivery_streams (next_visible_at, id)
    WHERE state = 'ready';

-- Reaper index. O(active leases) entries.
CREATE INDEX delivery_streams_expiry_idx ON delivery_streams (locked_until)
    WHERE state = 'leased';

-- Pending work only. Terminal rows are MOVED to deliveries_history in the same
-- statement that terminates them, so this table is sized by backlog, not by
-- lifetime volume.
CREATE TABLE deliveries (
    id                uuid PRIMARY KEY,
    tenant_id         uuid NOT NULL,
    stream_id         bigint NOT NULL REFERENCES delivery_streams (id) ON DELETE CASCADE,
    subscription_id   uuid NOT NULL,
    endpoint_id       uuid NOT NULL,
    event_id          uuid NOT NULL,
    seq               bigint NOT NULL,
    state             text NOT NULL DEFAULT 'pending' CHECK (state IN ('pending', 'retrying')),
    attempt           int  NOT NULL DEFAULT 0,
    next_attempt_at   timestamptz NOT NULL DEFAULT now(),
    last_error        text,
    -- Set when this delivery was created by a DLQ replay.
    replay_of         uuid,
    created_at        timestamptz NOT NULL DEFAULT now(),
    UNIQUE (stream_id, seq)
) WITH (
    fillfactor = 80,
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_vacuum_cost_delay = 0
);
-- (stream_id, seq) is already indexed by the UNIQUE constraint: it serves the
-- ordered inner query. This one serves the unordered inner query.
CREATE INDEX deliveries_stream_due_idx ON deliveries (stream_id, next_attempt_at, seq);
CREATE INDEX deliveries_event_idx ON deliveries (event_id);

-- Terminal deliveries. There is no separate DLQ table: a dead letter is a row
-- here with final_state = 'dead', reached through a partial index.
CREATE TABLE deliveries_history (
    id                uuid PRIMARY KEY,
    tenant_id         uuid NOT NULL,
    stream_id         bigint NOT NULL,
    subscription_id   uuid NOT NULL,
    endpoint_id       uuid NOT NULL,
    event_id          uuid NOT NULL,
    seq               bigint NOT NULL,
    attempt           int  NOT NULL,
    last_error        text,
    replay_of         uuid,
    created_at        timestamptz NOT NULL,
    final_state       text NOT NULL CHECK (final_state IN ('succeeded', 'dead', 'discarded')),
    dead_reason       text,
    completed_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX dh_tenant_idx ON deliveries_history (tenant_id, completed_at DESC);
CREATE INDEX dh_event_idx ON deliveries_history (event_id);
-- ★ The DLQ index: only dead rows.
CREATE INDEX dh_dlq_idx ON deliveries_history (tenant_id, endpoint_id, completed_at DESC)
    WHERE final_state = 'dead';

-- One row per HTTP attempt. Highest-volume table in the system.
CREATE TABLE delivery_attempts (
    id            uuid PRIMARY KEY,
    tenant_id     uuid NOT NULL,
    delivery_id   uuid NOT NULL,
    endpoint_id   uuid NOT NULL,
    attempt       int  NOT NULL,
    started_at    timestamptz NOT NULL,
    duration_ms   int,
    outcome       text NOT NULL
                    CHECK (outcome IN ('success', 'retryable', 'permanent', 'timeout', 'blocked', 'rate_limited')),
    status_code   int,
    resp_body     bytea,          -- truncated to 4 KiB
    error_class   text,           -- 'dns', 'tls', 'conn_refused', 'timeout', 'ssrf_blocked', ...
    error_detail  text,
    worker_id     text,
    trace_id      text,
    created_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX da_delivery_idx ON delivery_attempts (delivery_id, attempt);
CREATE INDEX da_endpoint_idx ON delivery_attempts (endpoint_id, created_at DESC);

-- ── ingest idempotency ──────────────────────────────────────────────────────

-- Claimed and completed inside the SAME transaction as the event insert, so
-- the database's unique-index wait is the lock: a concurrent duplicate blocks
-- until the winner commits, then sees a completed row and replays it. That is
-- why there is no committed 'in_flight' state to recover from.
CREATE TABLE idempotency_keys (
    tenant_id      uuid NOT NULL,
    key            text NOT NULL,
    request_hash   bytea NOT NULL CHECK (length(request_hash) = 32),
    response_code  int  NOT NULL,
    response_body  bytea NOT NULL,
    event_id       uuid NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now(),
    expires_at     timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, key)
);
-- The janitor's sweep.
CREATE INDEX idempotency_keys_expiry_idx ON idempotency_keys (expires_at);
