Directory is empty (confirmed greenfield), Go 1.25.5 on darwin/arm64, Docker CLI present but daemon not running, kubectl and terraform installed. No existing code to conform to, so this is a clean-sheet design.

---

# Ithaca — Delivery Engine Design

## 0. The one-paragraph thesis

Postgres is the **system of record and the scheduler**. The queue is not a table of messages that workers race over — it is a table of **streams** (lanes), and workers lease *streams*, not messages. This single decision buys you: correct per-key FIFO with a concurrent worker pool, a dramatically smaller contended row set for `SKIP LOCKED`, a natural place to hang circuit-breaker/rate-limit state, and a clean mapping onto SQS FIFO's `MessageGroupId`. Everything below follows from it.

---

## 1. Data Model

### 1.1 Conventions

- `uuid` PKs generated as **UUIDv7** in Go (time-ordered → B-tree inserts stay at the right edge, no random-IO write amplification). Do not use `gen_random_uuid()` (v4) for high-volume PKs. Use `bigint GENERATED ALWAYS AS IDENTITY` where you never expose the ID.
- `timestamptz` everywhere, never `timestamp`.
- All tenant-scoped tables carry `tenant_id` as the **leading column of every index** — this is your future sharding key and your row-level-security predicate.
- `jsonb` for payloads, but store the raw bytes for signing separately (see §7.1 — you must sign exactly the bytes you send; `jsonb` re-serialization reorders keys and will break HMAC).

### 1.2 Control plane

```sql
CREATE TABLE tenants (
  id            uuid PRIMARY KEY,
  name          text NOT NULL,
  state         text NOT NULL DEFAULT 'active'
                  CHECK (state IN ('active','suspended','deleted')),
  -- quotas, enforced at ingest
  max_events_per_sec  int NOT NULL DEFAULT 500,
  max_endpoints       int NOT NULL DEFAULT 100,
  retention_days      int NOT NULL DEFAULT 30,
  created_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE api_keys (
  id           uuid PRIMARY KEY,
  tenant_id    uuid NOT NULL REFERENCES tenants(id),
  -- NEVER store the key. Store SHA-256 of the full secret.
  -- Lookup is by the public prefix, verification is constant-time on the hash.
  key_prefix   text NOT NULL,              -- e.g. "ith_live_7f3a"  (12 chars, indexed)
  key_hash     bytea NOT NULL,             -- sha256(full_key), 32 bytes
  scopes       text[] NOT NULL DEFAULT '{ingest}',
  last_used_at timestamptz,
  expires_at   timestamptz,
  revoked_at   timestamptz,
  created_at   timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX api_keys_prefix_uk ON api_keys (key_prefix);
```

> **Why `key_prefix` + `key_hash` and not bcrypt?** API keys are 256-bit random, not human-chosen. There is no dictionary to attack, so a single SHA-256 is sufficient and is ~1µs instead of ~100ms. bcrypt on every ingest request would cap you at ~10 rps/core. Verify with `subtle.ConstantTimeCompare`. Cache the lookup in Redis for 30s with negative caching.

```sql
CREATE TABLE endpoints (
  id            uuid PRIMARY KEY,
  tenant_id     uuid NOT NULL REFERENCES tenants(id),
  url           text NOT NULL,
  description   text,
  state         text NOT NULL DEFAULT 'enabled'
                  CHECK (state IN ('enabled','disabled','auto_disabled')),
  disabled_reason text,

  -- delivery tuning (all nullable → fall back to system defaults)
  timeout_ms          int    NOT NULL DEFAULT 15000 CHECK (timeout_ms BETWEEN 1000 AND 60000),
  rate_limit_per_sec  int,                        -- NULL = tenant default
  max_concurrency     int    NOT NULL DEFAULT 8,  -- in-flight cap for unordered mode
  retry_policy        jsonb,                      -- see §6.2
  custom_headers      jsonb  NOT NULL DEFAULT '{}',

  created_at    timestamptz NOT NULL DEFAULT now(),
  updated_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX endpoints_tenant_idx ON endpoints (tenant_id) WHERE state <> 'disabled';

-- Multiple ACTIVE secrets at once = zero-downtime rotation (§7.2)
CREATE TABLE endpoint_secrets (
  id            uuid PRIMARY KEY,
  endpoint_id   uuid NOT NULL REFERENCES endpoints(id) ON DELETE CASCADE,
  key_id        text NOT NULL,           -- short id echoed in the signature header
  secret_enc    bytea NOT NULL,          -- AES-256-GCM, DEK wrapped by KMS/local master key
  dek_wrapped   bytea NOT NULL,
  created_at    timestamptz NOT NULL DEFAULT now(),
  expires_at    timestamptz              -- NULL = primary; set when rotating out
);
CREATE INDEX endpoint_secrets_ep_idx ON endpoint_secrets (endpoint_id)
  WHERE expires_at IS NULL OR expires_at > now();
```

> The partial index predicate uses `now()`, which is **not immutable** — Postgres will reject it. Real form: index on `(endpoint_id, expires_at)` and filter in the query. I'm calling this out because it's exactly the kind of thing that bites you at migration time. Correct version:
> ```sql
> CREATE INDEX endpoint_secrets_ep_idx ON endpoint_secrets (endpoint_id, expires_at NULLS FIRST);
> ```

```sql
-- A subscription is (endpoint × event-type filter). This is the fanout unit.
CREATE TABLE subscriptions (
  id            uuid PRIMARY KEY,
  tenant_id     uuid NOT NULL REFERENCES tenants(id),
  endpoint_id   uuid NOT NULL REFERENCES endpoints(id) ON DELETE CASCADE,
  event_types   text[] NOT NULL,        -- ['user.*','order.created']; '*' = all
  state         text NOT NULL DEFAULT 'enabled',

  -- ORDERING CONFIG — the core knob (§3)
  ordered       boolean NOT NULL DEFAULT false,
  lane_count    int     NOT NULL DEFAULT 16   -- only meaningful when ordered = false
                  CHECK (lane_count BETWEEN 1 AND 1024),
  hol_policy    text    NOT NULL DEFAULT 'pause'
                  CHECK (hol_policy IN ('pause','skip')),

  created_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX subscriptions_fanout_idx
  ON subscriptions (tenant_id) INCLUDE (endpoint_id, event_types)
  WHERE state = 'enabled';
```

### 1.3 Events (append-only, high volume, partitioned)

```sql
CREATE TABLE events (
  id            uuid        NOT NULL,           -- UUIDv7
  tenant_id     uuid        NOT NULL,
  event_type    text        NOT NULL,
  ordering_key  text,                            -- customer-supplied; NULL = unordered
  payload       bytea       NOT NULL,            -- EXACT bytes as received; this is what we sign
  content_type  text        NOT NULL DEFAULT 'application/json',
  headers       jsonb       NOT NULL DEFAULT '{}',
  idempotency_key text,
  traceparent   text,                            -- W3C, for span links (§8.5)
  created_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (created_at, id)                   -- partition key must be in the PK
) PARTITION BY RANGE (created_at);
```

**Why partition `events`:** it is append-only, immutable, and its retention is time-based. Daily partitions + `DETACH`/`DROP` turns retention into an O(1) metadata operation instead of a `DELETE` that generates gigabytes of WAL and dead tuples. This is the single biggest operational win in the schema.

**Why `PRIMARY KEY (created_at, id)` and not `(id)`:** Postgres requires the partition key to be a member of every unique constraint on a partitioned table. Consequence: lookups by bare `id` hit **every partition**. Mitigate by always passing `created_at` (you can extract the timestamp from the UUIDv7 in Go — no DB round-trip) so the planner prunes to one partition. This is a real ergonomic tax of partitioning and you should know it going in.

```sql
CREATE INDEX events_tenant_created_idx ON events (tenant_id, created_at DESC);
CREATE INDEX events_type_idx ON events (tenant_id, event_type, created_at DESC);
```
Both are created on the parent and cascade to partitions. The first serves the dashboard's "recent events for tenant" feed; the second serves filtering.

### 1.4 Streams — the heart of the queue

```sql
CREATE TABLE delivery_streams (
  id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  tenant_id        uuid NOT NULL,
  subscription_id  uuid NOT NULL REFERENCES subscriptions(id) ON DELETE CASCADE,
  endpoint_id      uuid NOT NULL,
  -- ordered subs: the customer's ordering_key. unordered: 'lane:<n>'.
  stream_key       text NOT NULL,
  ordered          boolean NOT NULL,

  state            text NOT NULL DEFAULT 'idle'
                     CHECK (state IN ('idle','ready','leased','paused','circuit_open')),
  next_visible_at  timestamptz NOT NULL DEFAULT now(),

  -- lease
  locked_by        text,             -- worker identity (pod name + pid), for debugging
  lock_token       uuid,             -- FENCING TOKEN. every mutation is conditional on this.
  locked_until     timestamptz,

  -- ordering serialization point (§3.3)
  next_seq         bigint NOT NULL DEFAULT 1,

  paused_at        timestamptz,
  paused_delivery_id uuid,
  created_at       timestamptz NOT NULL DEFAULT now()
) WITH (fillfactor = 70,
        autovacuum_vacuum_scale_factor = 0.01,
        autovacuum_vacuum_cost_delay = 0,
        autovacuum_analyze_scale_factor = 0.02);

CREATE UNIQUE INDEX delivery_streams_uk
  ON delivery_streams (subscription_id, stream_key);

-- ★ THE DEQUEUE INDEX ★
CREATE INDEX delivery_streams_runnable_idx
  ON delivery_streams (next_visible_at, id)
  WHERE state = 'ready';

-- reaper index
CREATE INDEX delivery_streams_expiry_idx
  ON delivery_streams (locked_until)
  WHERE state = 'leased';
```

**Index justification, one by one:**

| Index | Why it exists | What breaks without it |
|---|---|---|
| `delivery_streams_uk (subscription_id, stream_key)` | The ingest-time upsert target. Also the per-stream serialization point that makes `next_seq` correct. | No `ON CONFLICT` target; duplicate streams; ordering guarantee gone. |
| `delivery_streams_runnable_idx` **partial on `state='ready'`** | The dequeue query scans this and nothing else. Because it is partial, it contains only *runnable* streams — typically hundreds to low thousands even when the system holds millions of pending deliveries. It fits in `shared_buffers` permanently. | Dequeue scans a full index over every stream ever created, including millions of idle ones. Latency goes from 50µs to tens of ms and gets worse forever. |
| `delivery_streams_expiry_idx` **partial on `state='leased'`** | The reaper's `locked_until < now()` scan. Leased streams == number of active workers, so this index has O(workers) entries. | Reaper does a seq scan of the whole streams table every 5 seconds. |
| `fillfactor = 70` | Every lease/release is an `UPDATE` of `state`, `locked_until`, `lock_token`. With free space in-page, Postgres uses **HOT updates** — the new tuple version lives in the same page and *no index entries are written*. This is the difference between a stable queue and one that bloats its indexes to death in a week. | Every lease writes 3 index entries + 3 dead index entries. Index bloat compounds. |
| Aggressive autovacuum on this table only | Streams table is tiny (rows = active streams) but has the highest update rate in the system. Default `scale_factor=0.2` means vacuum waits for 20% dead tuples; on a hot 5k-row table that's fine, but `cost_delay` default throttles it. Set it to 0 here. | Bloat + stale visibility map + slow index scans. |

> **Note on HOT:** HOT requires that no *indexed* column changes. `state`, `next_visible_at`, and `locked_until` **are** indexed (in the partial indexes). So updates that change `state` will not be HOT. That's unavoidable — but it's ~5 index entries per stream-lease-cycle, not per *message*, which is the point of leasing streams instead of messages. Batch drain (§2.5) amortizes it further: one lease cycle covers up to 64 deliveries.

### 1.5 Deliveries — the pending work set

```sql
CREATE TABLE deliveries (
  id               uuid PRIMARY KEY,               -- UUIDv7
  tenant_id        uuid NOT NULL,
  stream_id        bigint NOT NULL REFERENCES delivery_streams(id) ON DELETE CASCADE,
  subscription_id  uuid NOT NULL,
  endpoint_id      uuid NOT NULL,

  event_id         uuid NOT NULL,
  event_created_at timestamptz NOT NULL,           -- partition-pruning hint for events lookup

  seq              bigint NOT NULL,                -- monotonic WITHIN stream, gapless, commit-ordered
  state            text NOT NULL DEFAULT 'pending'
                     CHECK (state IN ('pending','retrying')),   -- terminal states live in history
  attempt          int NOT NULL DEFAULT 0,
  next_attempt_at  timestamptz NOT NULL DEFAULT now(),
  last_error       text,
  replay_of        uuid,                            -- set when created by DLQ replay
  created_at       timestamptz NOT NULL DEFAULT now(),

  UNIQUE (stream_id, seq)
) WITH (fillfactor = 80,
        autovacuum_vacuum_scale_factor = 0.02,
        autovacuum_vacuum_cost_delay = 0);

-- ★ THE INNER (per-stream) DEQUEUE INDEX ★
CREATE INDEX deliveries_stream_seq_idx     ON deliveries (stream_id, seq);
CREATE INDEX deliveries_stream_due_idx     ON deliveries (stream_id, next_attempt_at, seq);
CREATE INDEX deliveries_event_idx          ON deliveries (event_id);
```

`deliveries_stream_seq_idx` serves the **ordered** inner query (`ORDER BY seq LIMIT k`). `deliveries_stream_due_idx` serves the **unordered** inner query (`WHERE next_attempt_at <= now() ORDER BY next_attempt_at`). Both are `stream_id`-leading, so each lookup touches one tight index range.

**Crucially: `deliveries` holds only non-terminal rows.** On success or death, the row is *moved* out (§2.6). The table's size is therefore proportional to **backlog**, not to lifetime volume. A healthy system has a `deliveries` table of a few thousand rows that lives entirely in RAM.

### 1.6 History, attempts, dead letters

```sql
CREATE TABLE deliveries_history (
  LIKE deliveries INCLUDING DEFAULTS,
  final_state    text NOT NULL CHECK (final_state IN ('succeeded','dead','discarded')),
  dead_reason    text,
  completed_at   timestamptz NOT NULL DEFAULT now()
) PARTITION BY RANGE (completed_at);
-- PK must include partition key:
ALTER TABLE deliveries_history ADD PRIMARY KEY (completed_at, id);

CREATE INDEX dh_tenant_idx  ON deliveries_history (tenant_id, completed_at DESC);
CREATE INDEX dh_event_idx   ON deliveries_history (event_id);
-- ★ THE DLQ INDEX: partial, so it only covers dead rows ★
CREATE INDEX dh_dlq_idx ON deliveries_history (tenant_id, endpoint_id, completed_at DESC)
  WHERE final_state = 'dead';

CREATE TABLE delivery_attempts (
  id             uuid NOT NULL,
  tenant_id      uuid NOT NULL,
  delivery_id    uuid NOT NULL,
  endpoint_id    uuid NOT NULL,
  attempt        int  NOT NULL,
  started_at     timestamptz NOT NULL,
  duration_ms    int,
  outcome        text NOT NULL   -- 'success'|'retryable'|'permanent'|'timeout'|'blocked'
                   CHECK (outcome IN ('success','retryable','permanent','timeout','blocked','rate_limited')),
  status_code    int,
  resp_headers   jsonb,
  resp_body      bytea,          -- TRUNCATED to 4 KiB
  error_class    text,           -- 'dns','tls','conn_refused','ctx_deadline','ssrf_blocked',...
  error_detail   text,
  worker_id      text,
  trace_id       text,
  created_at     timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (created_at, id)
) PARTITION BY RANGE (created_at);

CREATE INDEX da_delivery_idx ON delivery_attempts (delivery_id, attempt);
CREATE INDEX da_endpoint_idx ON delivery_attempts (endpoint_id, created_at DESC);
```

`delivery_attempts` is your highest-volume table (N attempts per delivery, and delivery counts are already fan-out-multiplied). **Daily partitions, 30-day retention, dropped by a cron.** Never `DELETE` from it.

There is **no separate DLQ table.** A dead letter is `deliveries_history WHERE final_state='dead'`, reached via the partial index `dh_dlq_idx`. A separate table would be a second write path with a second set of consistency bugs for zero benefit.

### 1.7 Idempotency keys

```sql
CREATE TABLE idempotency_keys (
  tenant_id     uuid NOT NULL,
  key           text NOT NULL,
  request_hash  bytea NOT NULL,            -- sha256(method|path|canonical(body))
  state         text NOT NULL CHECK (state IN ('in_flight','completed')),
  response_code int,
  response_body bytea,
  event_id      uuid,
  locked_until  timestamptz,               -- in_flight lease, so a crashed request self-heals
  created_at    timestamptz NOT NULL DEFAULT now(),
  expires_at    timestamptz NOT NULL,
  PRIMARY KEY (created_at, tenant_id, key)
) PARTITION BY RANGE (created_at);

CREATE UNIQUE INDEX idem_uk ON idempotency_keys (tenant_id, key, created_at);
```

Partitioned hourly, dropped after 24h — TTL enforcement becomes `DROP PARTITION` rather than a delete-sweep of millions of rows. **Caveat:** the uniqueness constraint must include `created_at`, so uniqueness is only guaranteed *within a partition*. To make it globally correct, the lookup must scan the last 24h of partitions — which is exactly 24 index probes, cheap, and the planner prunes to them via `created_at > now() - interval '24 hours'`. If you find that unsatisfying, the alternative is a **Redis-first** idempotency check with Postgres as the durable backstop; see §4.1.

### 1.8 Fanout outbox

```sql
CREATE TABLE fanout_tasks (
  id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  tenant_id      uuid NOT NULL,
  event_id       uuid NOT NULL,
  event_created_at timestamptz NOT NULL,
  cursor_sub_id  uuid,                -- resume point for batched expansion
  state          text NOT NULL DEFAULT 'pending',
  locked_until   timestamptz,
  lock_token     uuid,
  created_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX fanout_pending_idx ON fanout_tasks (id) WHERE state = 'pending';
```

**Why an outbox for fanout:** a tenant with 5,000 matching subscriptions would otherwise make ingest write 5,001 rows in one transaction, holding locks and blowing p99 latency. Instead: ingest writes `events` + `fanout_tasks` in **one transaction** (this is the atomicity boundary that guarantees no-loss), returns 202, and a fanout worker expands in batches of 500 with a resumable cursor. **Below a threshold (≤ 32 matching subs, the overwhelming majority) do it inline** and skip the outbox — best of both.

---

## 2. The Postgres Queue Driver

### 2.1 The dequeue query (stream lease)

One statement, one round trip, commits immediately. It **never stays open across a network call.**

```sql
-- $1 = batch size (streams), $2 = worker_id, $3 = lease duration
WITH candidate AS (
    SELECT id
    FROM delivery_streams
    WHERE state = 'ready'
      AND next_visible_at <= now()
    ORDER BY next_visible_at, id
    FOR UPDATE SKIP LOCKED
    LIMIT $1
)
UPDATE delivery_streams ds
SET state        = 'leased',
    locked_by    = $2,
    lock_token   = gen_random_uuid(),
    locked_until = now() + ($3 || ' milliseconds')::interval
FROM candidate c
WHERE ds.id = c.id
RETURNING ds.id, ds.lock_token, ds.locked_until,
          ds.subscription_id, ds.endpoint_id, ds.tenant_id,
          ds.ordered, ds.stream_key;
```

Then, per leased stream — **no locking needed**, because the stream lease *is* the mutual exclusion:

```sql
-- ORDERED: strict head-of-stream, do not skip anything
SELECT d.id, d.event_id, d.event_created_at, d.seq, d.attempt, d.next_attempt_at
FROM deliveries d
WHERE d.stream_id = $1
ORDER BY d.seq
LIMIT $2;              -- drain batch; caller stops at first not-yet-due row

-- UNORDERED: skip rows that are backing off — no head-of-line blocking
SELECT d.id, d.event_id, d.event_created_at, d.seq, d.attempt
FROM deliveries d
WHERE d.stream_id = $1
  AND d.next_attempt_at <= now()
ORDER BY d.next_attempt_at, d.seq
LIMIT $2;
```

That asymmetry in the inner query is the *entire* difference between ordered and unordered mode. Everything else — leasing, renewal, reaping, acking, backoff, breakers — is shared code.

### 2.2 Why lease streams and not messages

The textbook pattern is `SELECT ... FROM jobs WHERE state='pending' ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 10`. It has two problems this design avoids:

1. **It cannot preserve order.** `SKIP LOCKED` is defined by "skip whatever someone else holds." Order is the first casualty.
2. **Contention scales with in-flight messages.** `SKIP LOCKED` does not block, but it does **work**: each worker walks the index and, for every candidate tuple, must fetch the heap page to evaluate visibility and lock status. With W workers each taking a batch of B, the W-th worker skips roughly `(W-1)×B` tuples before it finds free ones. Cost per dequeue is O(W×B) heap fetches. At 64 workers × batch 20 that's ~1,280 wasted buffer accesses per dequeue, per worker, forever.

With stream-leasing, the contended set is `count(runnable streams)`, and each unit of contention yields up to 64 deliveries of work. **Contention per delivered message drops by the batch factor.**

### 2.3 Honest measured behavior of `SKIP LOCKED`

What you should expect on a single mid-size Postgres (8 vCPU, `synchronous_commit=on`, local NVMe):

- Naive per-message lease + ack, 1 worker: ~2–4k msg/s. You are bound by **two WAL commits per message** (lease, ack), i.e. by fsync latency, not by locking.
- Naive per-message, 32 workers: throughput plateaus around 6–10k msg/s and **p99 dequeue latency rises superlinearly**. Blame is split between the O(W×B) skip scan and `LWLock:WALWrite` / `LWLock:BufferContent` contention. `pg_stat_activity.wait_event` will show it.
- Batched stream-lease, 32 workers, batch 32: 25–40k deliveries/s of *queue* throughput, at which point your real bottleneck is autovacuum keeping up with the dead tuples, and then outbound HTTP concurrency long before that.

**Two mitigations worth building because they are cheap:**
- Randomized scan offset. If every worker sorts by `next_visible_at, id` they all pile onto the same index head. Add `AND id > $random_pivot` with wraparound, or bucket workers by `id % N`. This is the single highest-leverage anti-contention trick and it makes a great benchmark graph for a resume.
- `LIMIT $1` where `$1` is derived from free pool slots (§5.3), not a constant.

**Set `synchronous_commit = off` for the ack path only** (`SET LOCAL synchronous_commit = off`). Reasoning: losing the last ~200ms of *acks* on a crash means redelivering a handful of messages, which the system is already designed to tolerate (at-least-once). Losing an *ingest* commit means losing a customer's event, which it is not. Different durability requirements for different transactions — set it per-transaction. Expect a 2–5× improvement on the ack path.

### 2.4 Lease, renewal, reaper, fencing

- **Lease duration** = `endpoint.timeout_ms × 2 + 10s`, floored at 30s. Long enough that a normal delivery never races the lease.
- **Renewal**: a heartbeat goroutine per lease, ticking at `lease/3`.
  ```sql
  UPDATE delivery_streams
  SET locked_until = now() + $3
  WHERE id = $1 AND lock_token = $2 AND state = 'leased'
  RETURNING locked_until;
  ```
  Zero rows returned ⇒ **the lease is lost**. The heartbeat cancels the worker's context. The worker must then abandon: do not ack, do not write history, do write a `delivery_attempts` row with `outcome='lease_lost'` for forensics.
- **Reaper** (runs every 5s, batched):
  ```sql
  WITH expired AS (
      SELECT id FROM delivery_streams
      WHERE state = 'leased' AND locked_until < now()
      ORDER BY locked_until
      FOR UPDATE SKIP LOCKED
      LIMIT 500
  )
  UPDATE delivery_streams ds
  SET state = 'ready', locked_by = NULL, lock_token = NULL, locked_until = NULL,
      next_visible_at = now()
  FROM expired e WHERE ds.id = e.id
  RETURNING ds.id;
  ```
  Emit a metric on every reap. A nonzero steady-state reap rate means your lease is too short or your workers are being OOM-killed.

- **Fencing is mandatory and it is the correctness crux.** A worker that has been network-partitioned from Postgres for 60s may still be mid-HTTP-request. The reaper reassigns its stream. When the partition heals, the zombie worker tries to ack. Without fencing it would ack a delivery the new worker is concurrently retrying — and in ordered mode it could ack out of order. The `lock_token` closes this: **every** mutation the worker makes is inside a transaction that begins with

  ```sql
  SELECT 1 FROM delivery_streams
  WHERE id = $1 AND lock_token = $2 AND state = 'leased' AND locked_until > now()
  FOR UPDATE;
  ```
  and rolls back if it returns nothing. This is Kleppmann's fencing token, implemented in one line. Note the `FOR UPDATE` here is momentary — held only for the duration of the short ack transaction, never across the HTTP call.

### 2.5 Batching and stream drain

A leased stream is drained in a loop by one worker:

```
lease stream  →  fetch up to 64 deliveries  →  deliver them (sequentially if ordered,
                 concurrently up to endpoint.max_concurrency if not)
              →  ack results in ONE batched transaction
              →  fetch again (up to maxDrainRounds or leaseDeadline*0.6)
              →  release stream
```

Amortizing the lease over ≥1 batch is what makes the design fast. Cap the drain at `min(4 rounds, 60% of lease remaining)` so one busy stream cannot starve others and so you always release cleanly before the lease expires.

**Release** recomputes visibility, and this is where ordered vs. unordered diverge again:

```sql
-- ORDERED release: visibility = the HEAD's due time (head-of-line semantics)
UPDATE delivery_streams ds
SET state = CASE WHEN h.next_attempt_at IS NULL THEN 'idle' ELSE 'ready' END,
    next_visible_at = COALESCE(h.next_attempt_at, now()),
    locked_by = NULL, lock_token = NULL, locked_until = NULL
FROM (
    SELECT (SELECT next_attempt_at FROM deliveries
            WHERE stream_id = $1 ORDER BY seq LIMIT 1) AS next_attempt_at
) h
WHERE ds.id = $1 AND ds.lock_token = $2;

-- UNORDERED release: visibility = MIN over all pending (no HOL)
UPDATE delivery_streams ds
SET state = CASE WHEN m.t IS NULL THEN 'idle' ELSE 'ready' END,
    next_visible_at = COALESCE(m.t, now()),
    locked_by = NULL, lock_token = NULL, locked_until = NULL
FROM (SELECT MIN(next_attempt_at) AS t FROM deliveries WHERE stream_id = $1) m
WHERE ds.id = $1 AND ds.lock_token = $2;
```

Because a `state='ready'` stream with `next_visible_at` in the future is not in the partial dequeue index's *useful* range but *is* in the index... note the partial index is on `state='ready'` with `next_visible_at` as the leading key, so future-dated streams sit at the tail of the index and the `next_visible_at <= now()` range scan never touches them. Correct and cheap.

### 2.6 Solving the "queue table is a graveyard"

**Do not** keep terminal rows in `deliveries` with a `state='succeeded'` column and a partial index. That is the standard advice and it is *half* right — the partial index stays small, but the **table** doesn't, and every dequeue's index scan increasingly touches heap pages that are cold, and `VACUUM` must repeatedly scan a table that is 99.99% frozen history. Autovacuum's visibility map helps but does not save you.

**Do:** move terminal rows out of the hot table in the same statement that terminates them.

```sql
-- Batch-complete a drained stream. One statement, one WAL record set, atomic.
WITH moved AS (
    DELETE FROM deliveries
    WHERE id = ANY($1::uuid[])
    RETURNING *
)
INSERT INTO deliveries_history
SELECT m.*, r.final_state, r.dead_reason, now()
FROM moved m
JOIN unnest($1::uuid[], $2::text[], $3::text[]) AS r(id, final_state, dead_reason)
  ON r.id = m.id;
```

Wrapped with the fencing check, plus a batched `INSERT INTO delivery_attempts` (multi-row `unnest`), plus a Redis publish after commit. Three statements, one transaction, one fsync, up to 64 deliveries.

**The result:** `deliveries` stays small (rows ≈ backlog). `deliveries_history` is partitioned by `completed_at`, so retention is `DROP TABLE deliveries_history_2026_06`. `delivery_attempts` likewise. `events` likewise. **Nothing in this system is ever deleted row-by-row.**

The cost is roughly 2× WAL on the completion path (delete + insert instead of update). That is the correct trade: WAL is sequential and cheap; bloat is random and compounding.

**Rejected alternative — partition `deliveries` by `state`:** an `UPDATE` that changes the partition key is implemented as DELETE+INSERT with row movement, and a concurrent `SELECT ... FOR UPDATE` on that row fails with `ERROR: tuple to be locked was already moved to another partition due to concurrent update`. Your dequeue path would randomly error under load. Do not do this.

### 2.7 Wakeup: don't poll, don't `NOTIFY` either

Polling every 100ms across 50 workers = 500 pointless queries/sec and 100ms of added p50 latency. The obvious fix is `LISTEN`/`NOTIFY`, but `NOTIFY` appends to a **cluster-global** queue under a global lock at commit time; at high ingest rates it becomes a serialization point and it's a known scalability wall.

**Use Redis pub/sub as the wakeup hint, Postgres polling as the correctness floor.** Ingest publishes `ithaca:wake` (no payload needed, or a coarse shard id) after commit. Workers subscribe and, on any message, reset their poll timer to fire immediately. Polling continues at a 250ms–2s adaptive floor (exponential backoff up to 2s when idle, snap to 0 on wake). Redis being down degrades latency, never correctness. This gives Redis a real, load-bearing job and demonstrates the pattern properly.

---

## 3. Ordering

### 3.1 The lane abstraction (unifying ordered and unordered)

Every delivery belongs to exactly one **stream** (lane), identified by `(subscription_id, stream_key)`:

- `subscription.ordered = true` → `stream_key = event.ordering_key` (or `"__null__"` if the producer omitted it).
- `subscription.ordered = false` → `stream_key = "lane:" || (hashtext(event_id::text) & (lane_count-1))`.

**A stream is leased by at most one worker at a time.** That is the whole guarantee. Ordering falls out of it for free, and unordered mode is just "ordering over a synthetic key with `lane_count` values," which reuses 100% of the code path. Parallelism for an unordered subscription is bounded by `lane_count`; it's a tunable knob (default 16, max 1024) that also serves as a per-subscription concurrency limit.

This unification is the design's best property: **there is exactly one dequeue path, one lease path, one ack path.** No mode-specific correctness bugs.

### 3.2 Why not "just lock the message and order by seq"

Because `SKIP LOCKED` with `ORDER BY seq` gives you: worker A locks seq=1, worker B skips it and locks seq=2, B's endpoint is fast, A's is slow. B delivers seq=2 first. You have violated the guarantee while appearing to sort. There is no set of hints that fixes this. **Serialization must happen at the lane, not at the query.**

### 3.3 The sequence-assignment subtlety (this is where naive designs break)

You cannot use a global `BIGSERIAL` for `seq`. Sequences are assigned **outside** transaction ordering: transaction A can take seq=5, transaction B take seq=6, and B commits first. A reader between the commits sees 6 without 5; when A commits, 5 appears "in the past." A consumer that has advanced past 6 will either skip 5 or deliver it out of order. This is the same failure that breaks naive CDC-by-autoincrement pipelines, and it is easy to not notice until production.

**Fix:** assign `seq` from `delivery_streams.next_seq` under the stream row's write lock, in the same transaction as the delivery insert:

```sql
-- Ingest/fanout, per (subscription, stream_key). ONE statement.
WITH s AS (
    INSERT INTO delivery_streams
        (tenant_id, subscription_id, endpoint_id, stream_key, ordered,
         state, next_visible_at, next_seq)
    VALUES ($1, $2, $3, $4, $5, 'ready', now(), 2)
    ON CONFLICT (subscription_id, stream_key) DO UPDATE
      SET next_seq        = delivery_streams.next_seq + 1,
          -- never un-pause; never yank a leased stream out from under a worker
          state           = CASE WHEN delivery_streams.state IN ('paused','circuit_open','leased')
                                 THEN delivery_streams.state ELSE 'ready' END,
          next_visible_at = CASE WHEN delivery_streams.state IN ('paused','circuit_open')
                                 THEN delivery_streams.next_visible_at
                                 ELSE LEAST(delivery_streams.next_visible_at, now()) END
    RETURNING id, next_seq - 1 AS assigned_seq
)
INSERT INTO deliveries
    (id, tenant_id, stream_id, subscription_id, endpoint_id,
     event_id, event_created_at, seq, state, next_attempt_at)
SELECT $6, $1, s.id, $2, $3, $7, $8, s.assigned_seq, 'pending', now()
FROM s
RETURNING id, stream_id, seq;
```

Because `ON CONFLICT DO UPDATE` takes a row-level write lock held until commit, concurrent inserts into the same stream **serialize**, and `seq` order is therefore identical to commit order. Gapless, monotonic, correct.

**Own the cost:** this makes the stream row a hot row. Two producers writing to the same `ordering_key` serialize on it — but they *must*, because "ordered" is a request for a serialization point. There is no free lunch here and you should say so in your README. For unordered subscriptions the hot row is a lane row, so contention is divided by `lane_count`; that is why the default is 16 rather than 1, and why `fillfactor=70` on that table matters.

### 3.4 Head-of-line policy

When the head of an *ordered* stream exhausts its retries or hits a permanent error:

**Default = `pause`.** The stream transitions to `state='paused'`, `paused_delivery_id` is set, `next_visible_at` is irrelevant, and the dequeue index no longer sees it. New events keep accumulating in `deliveries` behind it (no data loss, correct order preserved). An alert fires on the WS feed and via the control-plane API. The tenant fixes their endpoint and calls `POST /v1/streams/{id}/resume`, which resets the head's `attempt=0, next_attempt_at=now()` and the stream to `ready`.

**Defend it:** a customer who asked for ordering did so because message N+1 is causally dependent on message N — `order.created` before `order.shipped`. Silently skipping N and delivering N+1 gives the subscriber a state they cannot reconcile and is *worse* than delay. If you would rather have availability than ordering, then you didn't want ordering; set `ordered=false`.

**Opt-in escape hatch = `skip`:** head goes to `deliveries_history` with `final_state='dead'`, stream advances. Choose it per-subscription via `hol_policy`. Do **not** auto-escalate `pause → skip` after a timeout; a silent correctness downgrade at 3am is exactly the behavior nobody wants. Instead: alert, expose `paused_at` and stream depth as first-class dashboard state, and add a `stream_paused_seconds` metric with an SLO.

Second-order rule: **auto-disable the whole endpoint** after N distinct streams pause or after 24h of 100% failure, and email the tenant. That is the humane version of pressure relief, and it's the one Stripe and Svix both use.

Interaction with retries: pausing does **not** discard retry state. The head keeps `attempt` at whatever it reached, so `resume` can either continue the schedule or reset it (API flag `reset_attempts`, default true).

### 3.5 The throughput cost, stated plainly

An ordered stream is a serial channel. Its ceiling is `1 / p50_delivery_latency`. At 100ms p50, **one stream = ~10 deliveries/s**, full stop, regardless of how many workers you run. Drain batching does not help, because within an ordered stream you must await attempt N before starting N+1.

Consequences to put in the docs:
- A subscription with `ordering_key = "global"` is capped at ~10 msg/s. This is not a bug.
- Aggregate throughput = `distinct_ordering_keys × 10/s`, bounded by worker count.
- **Ordering is opt-in (`ordered=false` default) and the API should reject an ordered subscription whose observed `ordering_key` cardinality over the last hour is < 8**, or at minimum warn loudly in the dashboard. Guiding customers to per-entity keys (`user_id`, `order_id`) is the difference between a working product and a support queue.

---

## 4. Idempotency

### 4.1 Ingest side

`POST /v1/events` with header `Idempotency-Key: <opaque, ≤255 chars>`. Optional; when absent, no dedupe.

Algorithm:

1. `req_hash = sha256(method || "\n" || path || "\n" || raw_body)`. Hash the **raw bytes**, not a re-serialized form.
2. Attempt to claim:
   ```sql
   INSERT INTO idempotency_keys
       (tenant_id, key, request_hash, state, locked_until, created_at, expires_at)
   VALUES ($1, $2, $3, 'in_flight', now() + interval '60 seconds', now(), now() + interval '24 hours')
   ON CONFLICT (tenant_id, key, created_at) DO NOTHING
   RETURNING 1;
   ```
   Because of partitioning, first do a bounded lookup (`WHERE tenant_id=$1 AND key=$2 AND created_at > now() - interval '24 hours'`); if it misses, insert. The insert is the authoritative race-resolver. (Pragmatic accelerator: a Redis `SET key val NX EX 86400` in front of this cuts the common-case to one Redis round trip; Postgres remains the source of truth so a Redis flush costs you duplicates, not correctness.)
3. **Claimed** → do the work (event insert + fanout) in the *same transaction*, then `UPDATE ... SET state='completed', response_code=202, response_body=..., event_id=...`. Same transaction as the event insert — otherwise a crash between them leaves a permanently-`in_flight` key.
4. **Conflict** → read the existing row:
   - `request_hash` differs → **`422 Unprocessable Entity`**, body `{"code":"idempotency_key_reuse", ...}`. Per `draft-ietf-httpapi-idempotency-key-header`. Never process it, never return the old response — both are silently wrong.
   - same hash, `state='completed'` → replay the stored response verbatim, plus `Idempotent-Replay: true`. Status code is the original's.
   - same hash, `state='in_flight'` and `locked_until > now()` → **`409 Conflict`** + `Retry-After: 1`. A concurrent duplicate is in progress.
   - same hash, `state='in_flight'` and `locked_until < now()` → the original request's process died. Steal the claim with a conditional `UPDATE ... WHERE state='in_flight' AND locked_until < now()`; if it updates 1 row, proceed as owner.

**TTL: 24h**, enforced by dropping hourly partitions. Document that reuse of a key after 24h is a *new* request. Scope keys to `(tenant_id, key)` — never global, or one tenant can probe another's keyspace.

**Response body storage cap:** 8 KiB. Beyond that, store a pointer and replay a `303`-ish reference. In practice the 202 response is tiny.

### 4.2 Delivery side — this is at-least-once, precisely

**We do not offer exactly-once and we say so in the docs.** Headers we send so the subscriber can dedupe:

```
Ithaca-Id:            evt_01JQ8ZM7... # the EVENT id. STABLE ACROSS ALL RETRIES AND REPLAYS.
Ithaca-Delivery-Id:   dlv_01JQ8ZM9... # this (subscription, event) pair. stable across retries.
Ithaca-Timestamp:     1753996800      # unix seconds, changes per attempt
Ithaca-Attempt:       3               # 0-indexed
Ithaca-Event-Type:    order.created
Ithaca-Signature:     v1,k=sec_a1,<b64> v1,k=sec_b2,<b64>
Ithaca-Webhook-Id:    sub_01JQ...
Idempotency-Key:      evt_01JQ8ZM7...   # RFC-shaped alias of Ithaca-Id, for tooling
User-Agent:           Ithaca/1.0 (+https://…/docs/webhooks)
```

The dedupe contract we publish: *"Store `Ithaca-Id` with a 72-hour TTL. If you have seen it, return 200 and do nothing."* Note `Ithaca-Id` (event) vs `Ithaca-Delivery-Id` (per-subscription): a subscriber with two endpoints registered for the same event legitimately gets the same `Ithaca-Id` twice, once per endpoint. Dedupe on `Ithaca-Delivery-Id` if you route per-endpoint; on `Ithaca-Id` if you converge them. Document both.

### 4.3 Exactly where duplicates arise — the complete list

1. **The canonical one: crash between HTTP 200 and the ack commit.** The subscriber has the event. Our process dies before `DELETE FROM deliveries`. The reaper reassigns the stream. We deliver again. **This window is unclosable** without a distributed transaction with the subscriber, which does not exist over HTTP. Minimize it by acking immediately after the response body is read and by making the ack a single fast statement — do not do Redis publishes, metrics flushes, or trace exports before the ack.
2. **Lease expiry during a slow-but-successful request.** Our HTTP client is 40s into a 60s response; lease renewal fails due to a DB blip; the reaper hands the stream to worker B; B delivers; then A's original request also succeeds on the subscriber. Two deliveries, *and in ordered mode a potential inversion*. Mitigated by: (a) lease renewal at lease/3 with 2 retries, (b) fencing (A cannot ack, so state is consistent even though the subscriber saw both), (c) `endpoint.timeout_ms` capped at 60s and the lease at ≥ 2× that.
3. **Timeout where the subscriber actually committed.** We `context.DeadlineExceeded` at 15s; the subscriber finished at 15.2s. We classify as retryable and retry. Unavoidable; this is why subscribers must dedupe.
4. **Connection reset after the request was fully written.** Same shape.
5. **DLQ replay.** By construction — replay reuses `Ithaca-Id`, so a well-behaved subscriber no-ops. Intentional and safe.
6. **`INSERT ... ON CONFLICT DO NOTHING` losing to a concurrent identical request in a partition edge case at the hour boundary of `idempotency_keys`.** Rare; produces a duplicate *event*, which then produces duplicate deliveries with *different* `Ithaca-Id`s. This one the subscriber cannot dedupe. It's the argument for the Redis-first check with a 24h TTL (no partition boundary). Worth building.

Write these six into `docs/at-least-once.md`. Being able to enumerate them in an interview is more valuable than the code.

---

## 5. The Worker Pool

This is the flagged learning gap, so here is the whole thing, opinionated.

### 5.1 The shape

```
                    ┌──────────────────────────────────────────┐
                    │  Dispatcher (1 goroutine per process)    │
   Redis wake ─────▶│  1. acquire N slots (BLOCKS if full)     │
   poll ticker ────▶│  2. dequeue exactly N streams            │
                    │  3. hand each to the pool                │
                    │  4. release slots we didn't fill         │
                    └───────────────┬──────────────────────────┘
                                    │ chan *Lease  (UNBUFFERED)
                    ┌───────────────▼──────────────────────────┐
                    │  Bounded pool: W worker goroutines        │
                    │  each: drain stream → deliver → ack       │
                    │  each: heartbeat goroutine for the lease  │
                    └──────────────────────────────────────────┘
```

The single most important rule, and the one your SQS-loop-with-unbounded-goroutines background will not have taught you:

> **Acquire capacity *before* dequeuing, and dequeue exactly as much as you acquired.**

Everything else is a variation on that. If you dequeue first and then try to find somewhere to put the work, you have converted a queue-depth problem into a memory problem, and you have taken out leases you cannot service, which will expire and be reaped — a livelock where the system is 100% busy reaping its own abandoned work.

### 5.2 Core types

```go
package worker

// Clock is injected everywhere time is read. See §9.3.
type Clock interface {
	Now() time.Time
	NewTimer(d time.Duration) *time.Timer
	Since(t time.Time) time.Duration
}

// Slots is a counting semaphore that supports partial acquisition.
// This is the piece that makes backpressure work, and it is NOT in the stdlib.
type Slots struct {
	mu    sync.Mutex
	cond  *sync.Cond
	cap   int
	free  int
	closed bool
}

// AcquireUpTo blocks until at least one slot is free, then takes as many as
// are free, capped at max. Returns 0 only if ctx is done or Slots is closed.
// This is the whole backpressure mechanism: it bounds the dequeue batch size
// by real, currently-available processing capacity.
func (s *Slots) AcquireUpTo(ctx context.Context, max int) int
func (s *Slots) Release(n int)
func (s *Slots) InFlight() int   // for metrics + K8s HPA custom metric

type Config struct {
	Workers         int           // size of the goroutine pool, e.g. 64
	MaxStreamsBatch int           // cap on streams per dequeue, e.g. 32
	DrainBatch      int           // deliveries fetched per stream round, e.g. 64
	MaxDrainRounds  int           // e.g. 4
	LeaseTTL        time.Duration // e.g. 60s
	RenewEvery      time.Duration // LeaseTTL / 3
	PollFloor       time.Duration // 250ms
	PollCeiling     time.Duration // 2s
	ShutdownGrace   time.Duration // 30s
}

type Pool struct {
	cfg      Config
	q        queue.Queue
	deliver  *deliver.Client   // §7
	slots    *Slots
	leases   chan *queue.Lease // UNBUFFERED — buffering here re-creates the bug
	wg       sync.WaitGroup
	clock    Clock
	log      *slog.Logger
	metrics  *Metrics
}

func New(cfg Config, q queue.Queue, d *deliver.Client, opts ...Option) *Pool
func (p *Pool) Run(ctx context.Context) error   // blocks until ctx done + drain complete
```

`leases` is deliberately **unbuffered**. A buffered channel is a second, invisible queue that defeats the semaphore's accounting: the dispatcher would report N in-flight while 3N sit in a buffer, and shutdown would have to drain a queue nobody is tracking.

### 5.3 The dispatcher loop

```go
func (p *Pool) dispatch(ctx context.Context) error {
	backoff := p.cfg.PollFloor
	for {
		// (1) BACKPRESSURE. Blocks here when the pool is saturated.
		//     While blocked we issue ZERO queries. Postgres load is
		//     proportional to spare capacity, not to queue depth.
		n := p.slots.AcquireUpTo(ctx, p.cfg.MaxStreamsBatch)
		if n == 0 {
			return ctx.Err()
		}

		// (2) Fetch exactly what we can service.
		leases, err := p.q.LeaseStreams(ctx, queue.LeaseReq{
			Limit: n, WorkerID: p.id, TTL: p.cfg.LeaseTTL,
		})
		if err != nil {
			p.slots.Release(n)
			p.metrics.DequeueErrors.Inc()
			backoff = jitteredBackoff(backoff, p.cfg.PollCeiling)
			if !p.sleep(ctx, backoff) { return ctx.Err() }
			continue
		}

		// (3) Give back what we over-reserved. Forgetting this is the #1 bug
		//     in this pattern: the pool silently shrinks to zero.
		p.slots.Release(n - len(leases))

		if len(leases) == 0 {
			backoff = jitteredBackoff(backoff, p.cfg.PollCeiling)
			select {
			case <-ctx.Done():        return ctx.Err()
			case <-p.wake:            backoff = p.cfg.PollFloor  // Redis pub/sub hint
			case <-p.clock.NewTimer(backoff).C:
			}
			continue
		}
		backoff = p.cfg.PollFloor

		// (4) Hand off. Each worker Releases its own slot when done.
		for _, l := range leases {
			select {
			case p.leases <- l:
			case <-ctx.Done():
				_ = p.q.ReleaseNow(context.WithoutCancel(ctx), l) // don't wait for the reaper
				p.slots.Release(1)
			}
		}
	}
}
```

Two details worth naming: `context.WithoutCancel` (Go 1.21+) for cleanup work that must run *after* the parent context is cancelled — the ack path uses this too. And `p.slots.Release(n - len(leases))`, which is the bug you will write and then spend an afternoon finding, because the symptom is "throughput slowly decays to zero over hours."

### 5.4 The worker

```go
func (p *Pool) worker(ctx context.Context) {
	for l := range p.leases {
		p.handleLease(ctx, l)
		p.slots.Release(1)          // MUST be unconditional. defer it inside handleLease.
	}
}

func (p *Pool) handleLease(ctx context.Context, l *queue.Lease) {
	// Lease-scoped context: cancelled if the heartbeat loses the lease.
	lctx, cancel := context.WithCancel(ctx)
	defer cancel()

	hb := newHeartbeat(p.q, l, p.cfg.RenewEvery, p.clock, cancel)
	go hb.run(lctx)
	defer hb.stop()

	defer func() {
		if r := recover(); r != nil {
			// A panic must not lose the stream. Release it so the next
			// worker picks it up immediately instead of waiting for the reaper.
			p.log.Error("worker panic", "stream", l.StreamID, "panic", r,
				"stack", string(debug.Stack()))
			p.metrics.Panics.Inc()
		}
		_ = p.q.Release(context.WithoutCancel(ctx), l)
	}()

	for round := 0; round < p.cfg.MaxDrainRounds; round++ {
		if p.clock.Since(l.AcquiredAt) > time.Duration(float64(p.cfg.LeaseTTL)*0.6) {
			return // bail early; never risk racing our own lease expiry
		}
		msgs, err := p.q.FetchStream(lctx, l, p.cfg.DrainBatch)
		if err != nil || len(msgs) == 0 { return }

		results := p.deliverBatch(lctx, l, msgs)

		if err := p.q.Ack(context.WithoutCancel(lctx), l, results); err != nil {
			// Fenced out or DB down. Do NOT retry blindly; the reaper will
			// re-lease and everything will be redelivered (at-least-once).
			p.log.Warn("ack failed", "stream", l.StreamID, "err", err)
			return
		}
		if len(msgs) < p.cfg.DrainBatch { return }
	}
}
```

`deliverBatch` is where ordered/unordered diverge for the last time:

```go
func (p *Pool) deliverBatch(ctx context.Context, l *queue.Lease, msgs []queue.Message) []queue.Result {
	if l.Ordered {
		// Strictly sequential. Stop at the first non-success: everything
		// behind it stays pending, in order, untouched.
		out := make([]queue.Result, 0, len(msgs))
		for _, m := range msgs {
			r := p.deliverOne(ctx, l, m)
			out = append(out, r)
			if r.Outcome != queue.OutcomeSuccess {
				break
			}
		}
		return out
	}
	// Unordered: concurrency bounded by the endpoint's own cap, and by the
	// fact that we hold exactly one lane. errgroup.SetLimit is the whole
	// per-endpoint concurrency mechanism inside a lane.
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(l.EndpointMaxConcurrency)
	out := make([]queue.Result, len(msgs))
	for i, m := range msgs {
		g.Go(func() error { out[i] = p.deliverOne(gctx, l, m); return nil })
	}
	_ = g.Wait()
	return out
}
```

(Go 1.22+ per-iteration loop variables mean no `i := i` shadowing is needed. On Go 1.25 you can also use `wg.Go(func(){...})` from the new `sync.WaitGroup.Go` method where an errgroup is overkill.)

### 5.5 Per-endpoint concurrency: two layers, honestly scoped

- **In-lane cap** (`errgroup.SetLimit`) — exact, cheap, in-process.
- **Cross-lane / cross-process cap** — a `map[uuid.UUID]*semaphore.Weighted` guarded by a mutex with refcounted eviction, is exact only *within one process*. Across N dispatcher pods it is N× the intended cap.

**Opinion: don't try to make in-process concurrency globally exact.** Concurrency limiting across processes requires a distributed lock on the hot path and it will cost more than it saves. Instead: (a) set the in-process cap to `endpoint.max_concurrency / replicas` (exposed to the pod via the Downward API + a `REPLICAS` env from the Deployment), accepting drift during rollouts, and (b) enforce the *hard* limit as a **rate** limit in Redis (§7.4), which is cheap to make global and is what customers actually care about ("don't exceed 100 rps at my endpoint"). Concurrency is a resource-protection heuristic; rate is the contract.

### 5.6 Graceful shutdown

Three phases, driven by two contexts:

```go
func (p *Pool) Run(parent context.Context) error {
	// runCtx: cancelled on SIGTERM → stops the dispatcher accepting new work.
	// Workers keep a derived ctx alive through the drain window.
	runCtx, stopAccepting := context.WithCancel(context.Background())
	drainCtx, cancelDrain := context.WithCancel(context.Background())

	go func() {
		<-parent.Done()
		stopAccepting()                                   // phase 1
		t := p.clock.NewTimer(p.cfg.ShutdownGrace)
		defer t.Stop()
		select {
		case <-p.drained:                                 // phase 2 complete
		case <-t.C: cancelDrain()                         // phase 3: hard cut
		}
	}()
	...
}
```

1. **Stop accepting** (immediately on SIGTERM). Dispatcher returns; `close(p.leases)` after it exits. The readiness probe flips to failing *first*, so the ALB/Service stops routing — but this process has no inbound traffic, so what matters is that it stops *dequeuing*.
2. **Drain** (up to `ShutdownGrace`, default 30s). Workers finish their current stream drain round, ack, and release. Because `handleLease` uses `context.WithoutCancel` for the ack, an ack in progress completes even after cancellation.
3. **Hard cut.** `cancelDrain()` kills in-flight HTTP requests. Those deliveries are *not* acked and *are* released explicitly (`ReleaseNow`), so the next worker picks them up in milliseconds rather than after the lease TTL. Duplicates are possible here — that's case #1 from §4.3, and it's exactly why the invariant is at-least-once.

**Set `terminationGracePeriodSeconds` in the K8s Deployment to `ShutdownGrace + 15`.** A mismatch here (grace period shorter than drain) is the classic K8s bug and, given the stated Kubernetes gap, is worth building the `preStop` hook and probe wiring for explicitly.

### 5.7 The DB connection pool sizing rule

Workers must **not** hold a Postgres connection across the HTTP call. They hold one for `LeaseStreams`, release, do network I/O for up to 60s, then take one for `Ack`. Therefore:

`pgxpool.MaxConns ≈ 2 × GOMAXPROCS + 8`, **not** `= Workers`. 64 workers on a 4-core pod runs fine on a pool of 16. Sizing the pool to worker count is how you get `too many connections` at 8 replicas.

---

## 6. Retry & Backoff

### 6.1 Full jitter

```go
// Pure function. No clock, no randomness source hidden inside. Testable in isolation.
func NextBackoff(attempt int, p Policy, r *rand.Rand) time.Duration {
	if attempt <= 0 { return p.Base }
	exp := float64(p.Base) * math.Pow(p.Multiplier, float64(attempt))
	capped := math.Min(exp, float64(p.Cap))
	// FULL JITTER: uniform in [Base/2, capped]. The floor prevents a
	// pathological 3ms retry that hammers a recovering endpoint.
	lo := float64(p.Base) / 2
	if capped < lo { capped = lo }
	return time.Duration(lo + r.Float64()*(capped-lo))
}
```

**Full jitter over "exponential + ±20%":** AWS's *Exponential Backoff and Jitter* measured this directly — full jitter minimizes both total work and completion time under contention. It matters more here than in most systems, because when a downed endpoint comes back, *every* queued delivery for it becomes due; without jitter you deliver a synchronized thundering herd to a service that just finished recovering and knock it back over. Full jitter smears them across the whole window.

**Defaults:** `Base=5s, Multiplier=2.0, Cap=6h, MaxAttempts=12`. Schedule (medians): 5s, 10s, 20s, 40s, 80s, 2.7m, 5.3m, 10.7m, 21m, 43m, 1.4h, 2.8h → ~6h to exhaustion at the median, ~12h worst case. Long enough to survive a business-hours outage, short enough that the DLQ isn't a week stale.

**Per-endpoint override** via `endpoints.retry_policy jsonb`, validated on write:
```json
{"base_ms": 5000, "multiplier": 2.0, "cap_ms": 21600000, "max_attempts": 12}
```
Bounds enforced server-side: `base ≥ 1s`, `max_attempts ≤ 25`, `cap ≤ 24h`. Also support an explicit `"schedule_ms": [5000, 300000, 1800000, ...]` fixed-list form — some customers want predictability more than optimality, and it's four lines of code.

### 6.2 Classification

```go
type Outcome int
const (
	OutcomeSuccess Outcome = iota
	OutcomeRetryable
	OutcomePermanent
	OutcomeRateLimited   // does NOT increment attempt
	OutcomeBlocked       // SSRF / circuit open — does NOT increment attempt
)

func Classify(resp *http.Response, err error) (Outcome, time.Duration /*hint*/, string /*class*/)
```

| Signal | Outcome | Notes |
|---|---|---|
| 2xx | success | Body read (capped at 64 KiB) and discarded; **must** be read before `Close()` or you leak the keep-alive conn. |
| 3xx | **permanent** | We disable redirects. Error: "endpoint returned a redirect; register the final URL." Following redirects is an SSRF hole and an ordering hazard. |
| 400, 401, 403, 404, 405, 422 | permanent | Config error on their side. |
| 410 Gone | permanent **+ auto-disable endpoint** | The RFC meaning is "stop asking." Honor it. |
| 408, 425 | retryable | |
| 429 | **rate_limited** | Honor `Retry-After` (both delta-seconds and HTTP-date forms), clamp to `[1s, 1h]`. **Does not consume an attempt** — otherwise a rate-limiting subscriber burns your whole retry budget in 12 seconds. Re-schedule the *stream*, not just the delivery. |
| 5xx | retryable | 501 Not Implemented is arguably permanent; treat as retryable, it's rare and cheap. |
| `context.DeadlineExceeded` | retryable, class `timeout` | |
| `syscall.ECONNREFUSED` / `ECONNRESET` / `EPIPE` | retryable, class `conn` | |
| `*net.DNSError` with `IsNotFound` | permanent after 3 attempts | NXDOMAIN that persists is a config error. `IsTemporary` → retryable. |
| `tls.CertificateVerificationError` | retryable, `max_attempts=4` | Expired certs get renewed; permanently failing here is user-hostile. But cap it — don't retry a bad cert for 12 hours. |
| SSRF blocked, invalid scheme, unparseable URL | **permanent** + surface prominently | Never retry; it will never change without a config edit. |
| circuit open | **blocked** | Stream is parked (§7.5). Not an attempt, no `delivery_attempts` row. |

Key discipline: **`rate_limited` and `blocked` never increment `attempt`.** Only genuine delivery failures consume the budget. Getting this wrong means healthy-but-throttled endpoints DLQ their traffic.

### 6.3 DLQ and replay

Death path (single transaction): move to `deliveries_history` with `final_state='dead'`, `dead_reason` = last error class + status; write the final `delivery_attempts` row; publish to Redis (dashboard alert); if ordered and `hol_policy='pause'`, set the stream to `paused` *instead* of moving the row (a paused head is not dead — nothing to replay, you `resume`).

Replay (`POST /v1/dlq/replay`, filters: endpoint, time range, error class, explicit ids):

```sql
WITH src AS (
    SELECT * FROM deliveries_history
    WHERE tenant_id = $1 AND final_state = 'dead'
      AND endpoint_id = $2 AND completed_at BETWEEN $3 AND $4
    ORDER BY completed_at
    LIMIT 1000
)
INSERT INTO deliveries (id, tenant_id, stream_id, subscription_id, endpoint_id,
                        event_id, event_created_at, seq, state, attempt,
                        next_attempt_at, replay_of)
SELECT uuidv7(), src.tenant_id, src.stream_id, src.subscription_id, src.endpoint_id,
       src.event_id, src.event_created_at,
       nextval_stream_seq(src.stream_id),   -- SECURITY DEFINER fn doing the row-locked bump
       'pending', 0, now(), src.id
FROM src;
```

Four rules:
1. **New `delivery.id`, same `event_id`.** `Ithaca-Id` is preserved so subscribers dedupe replays for free. `replay_of` gives you the audit chain.
2. **New `seq` from the tail.** A replay appends; it does not re-inject at its original position. Document loudly: *replaying into an ordered stream restores delivery but not original position.* (This is why `hol_policy='pause'` + `resume` is the right primitive for ordered subscriptions, and DLQ replay is mostly for `hol_policy='skip'` and unordered.)
3. **Idempotent replay of the replay.** `POST /v1/dlq/replay` takes an `Idempotency-Key` and reuses §4.1 machinery. Someone will double-click the button.
4. **Rate-controlled.** A replay of 500k dead letters must not be one transaction and must not saturate the endpoint. Replay writes a `replay_jobs` row and a background worker inserts in 1,000-row batches with a token bucket, so the endpoint sees a controlled ramp. Expose progress on the WS feed.

---

## 7. Endpoint Protection & Security

### 7.1 HMAC signing

```
Ithaca-Timestamp: 1753996800
Ithaca-Id:        evt_01JQ8ZM7XKQ2H3
Ithaca-Signature: v1,k=sec_a1b2,<base64(hmac)> v1,k=sec_c3d4,<base64(hmac)>
```

Signed content is **exactly**:
```
signed = event_id + "." + timestamp + "." + raw_body
sig    = base64_std(HMAC_SHA256(secret_bytes, signed))
```

Non-negotiables:
- **Sign the bytes you transmit.** Store `events.payload` as `bytea` and write those exact bytes to the wire. Never `json.Marshal` a decoded map on the delivery path — Go randomizes map iteration and `jsonb` normalizes key order and whitespace; either will produce a signature the customer cannot reproduce, and you'll get a bug report a month later that takes a day to diagnose.
- **The signature covers the timestamp and the id**, not just the body. Body-only signatures are trivially replayable and are the most common webhook-signing mistake in the wild.
- Secret material is a 32-byte random value, presented to the customer base64'd with a `whsec_` prefix. HMAC uses the **decoded bytes**.
- **Replay window: ±5 minutes**, published in the docs with sample verification code in Go, Python, Node, and Ruby. Customers *will not* implement the timestamp check unless you hand them the code.
- Constant-time compare on their side; on ours, nothing to compare.

### 7.2 Rotation with multiple active secrets

`POST /v1/endpoints/{id}/secrets/rotate` inserts a new `endpoint_secrets` row and sets `expires_at = now() + 24h` on the current one. **While ≥1 unexpired secret exists, every attempt is signed with all of them**, space-separated in the one header. The customer verifies against old-or-new and passes if either matches; they deploy the new secret at leisure; expired secrets stop being used automatically. Zero downtime, no coordination.

Cap active secrets at 3 (otherwise a customer who rotates in a loop makes you compute 40 HMACs per delivery). `k=<key_id>` in each signature component lets a sophisticated subscriber skip the ones they don't hold.

**At rest:** envelope encryption. Per-secret DEK, `AES-256-GCM`, DEK wrapped by AWS KMS in the cloud path and by a local master key from env in dev. Keep an in-process LRU of decrypted secrets (5 min TTL) so you don't KMS-call per delivery; that's the difference between a $12/mo KMS bill and a $12,000 one.

### 7.3 SSRF defense — the DNS-rebinding-safe dialer

Validate-then-connect is **broken**, and it is worth understanding why: you resolve `evil.com` → `1.2.3.4`, approve it, hand the URL to `http.Client`, which resolves *again* → `169.254.169.254`, and connects to the EC2 IMDS. That's a TOCTOU with a 1-second TTL. Every "we check the IP first" implementation has this hole.

The fix is `net.Dialer.Control`, which the runtime invokes **after** resolution and **before** `connect(2)`, with the literal address about to be dialed. There is no window.

```go
func safeControl(network, address string, _ syscall.RawConn) error {
	if network != "tcp4" && network != "tcp6" {
		return fmt.Errorf("ssrf: network %q not allowed", network)
	}
	host, _, err := net.SplitHostPort(address)
	if err != nil { return err }
	addr, err := netip.ParseAddr(host)
	if err != nil { return fmt.Errorf("ssrf: unparseable address %q", host) }
	// CRITICAL: ::ffff:127.0.0.1 is loopback but IsLoopback() on the
	// unmapped form is what you want; normalize first.
	if addr.Is4In6() { addr = addr.Unmap() }
	if blocked(addr) {
		return fmt.Errorf("ssrf: destination %s is in a blocked range", addr)
	}
	return nil
}

var blockedPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),        // "this network"
	netip.MustParsePrefix("10.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"),    // CGNAT
	netip.MustParsePrefix("127.0.0.0/8"),
	netip.MustParsePrefix("169.254.0.0/16"),   // ★ link-local: AWS/GCP/Azure IMDS lives here
	netip.MustParsePrefix("172.16.0.0/12"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("192.168.0.0/16"),
	netip.MustParsePrefix("198.18.0.0/15"),    // benchmarking
	netip.MustParsePrefix("224.0.0.0/4"),      // multicast
	netip.MustParsePrefix("240.0.0.0/4"),      // reserved + 255.255.255.255
	netip.MustParsePrefix("::1/128"),
	netip.MustParsePrefix("fc00::/7"),         // ULA
	netip.MustParsePrefix("fe80::/10"),        // link-local
	netip.MustParsePrefix("ff00::/8"),         // multicast
	netip.MustParsePrefix("64:ff9b::/96"),     // NAT64 → can encode 169.254.169.254
	netip.MustParsePrefix("2002::/16"),        // 6to4 → same trick
}
```

The `64:ff9b::/96` and `2002::/16` entries are the ones every hand-rolled blocklist misses; both let an attacker encode an IPv4 private address inside a "public-looking" IPv6 address.

The rest of the outbound client:

```go
func NewDeliveryTransport(allowPrivate bool) *http.Transport {
	d := &net.Dialer{
		Timeout:   5 * time.Second,   // connect timeout, separate from request timeout
		KeepAlive: 30 * time.Second,
		Control:   safeControl,       // nil if allowPrivate (local dev ONLY, guarded by env)
	}
	return &http.Transport{
		DialContext:           d.DialContext,
		TLSHandshakeTimeout:   5 * time.Second,
		ResponseHeaderTimeout: 15 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		MaxIdleConns:          512,
		MaxIdleConnsPerHost:   8,     // default is 2 — the default is a real throughput bug here
		IdleConnTimeout:       90 * time.Second,
		ForceAttemptHTTP2:     true,
		DisableCompression:    false,
	}
}

client := &http.Client{
	Transport: NewDeliveryTransport(cfg.AllowPrivate),
	Timeout:   0,  // per-request via context, since it's per-endpoint
	CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse   // never follow. classify 3xx as permanent.
	},
}
```

Plus, at registration and at delivery time: scheme must be `https` (allow `http` only when `ALLOW_INSECURE_ENDPOINTS=true`, off in prod); no userinfo in the URL; no non-standard ports below 1024 other than 443; response body read through `io.LimitReader(resp.Body, 64<<10)` then `io.Copy(io.Discard, ...)` to drain, then `Close()`.

**One `http.Transport` for the whole process, not one per delivery.** Constructing a transport per request is the classic Go leak — it defeats connection pooling and eventually exhausts file descriptors. Consider two transports: one for large tenants, one shared, so a single tenant's idle conns can't monopolize the pool.

### 7.4 Per-endpoint rate limiting (Redis, GCRA)

Token bucket via a Lua script so check-and-consume is atomic (`EVALSHA`, one RTT):

```lua
-- KEYS[1]=bucket key  ARGV[1]=rate/sec ARGV[2]=burst ARGV[3]=now_ms ARGV[4]=cost
local rate, burst, now, cost = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3]), tonumber(ARGV[4])
local st = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(st[1]) or burst
local ts     = tonumber(st[2]) or now
tokens = math.min(burst, tokens + (now - ts) * rate / 1000.0)
local allowed, wait_ms = 0, 0
if tokens >= cost then
  tokens, allowed = tokens - cost, 1
else
  wait_ms = math.ceil((cost - tokens) * 1000.0 / rate)
end
redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now)
redis.call('PEXPIRE', KEYS[1], math.ceil(burst * 1000.0 / rate) + 1000)
return {allowed, wait_ms}
```

**How it interacts with the queue — this is the important part.** On `allowed=0`, do **not** deliver-and-fail and do **not** busy-wait. Push the *stream* forward:

```sql
UPDATE delivery_streams
SET state = 'ready', next_visible_at = now() + ($3 || ' milliseconds')::interval,
    locked_by = NULL, lock_token = NULL, locked_until = NULL
WHERE id = $1 AND lock_token = $2;
```

with `$3 = wait_ms`. Zero attempts consumed, zero HTTP requests made, the worker slot is freed instantly, and the stream reappears exactly when tokens are available. **Rate limiting becomes a scheduling decision, not a failure.** That is the single most elegant consequence of leasing streams instead of messages, and it's the sentence to lead with when explaining this design.

Check the bucket **before** taking the endpoint's HTTP slot, and once per delivery, not once per lease.

### 7.5 Circuit breaker

Per-endpoint state in Redis (hash `cb:{endpoint_id}`), shared across dispatcher replicas:

- **Closed → Open** when, over a 60s sliding window with ≥ 20 samples, `failures/total > 0.5`; **or** on 10 consecutive `conn`-class errors (a refused connection needs no statistics). Sliding window as two 30s buckets — good enough, one `HINCRBY` per outcome, no sorted sets.
- **Open** for `min(30s × 2^consecutive_opens, 15m)`. On open, write `state=open, until=<ts>` and **park every ready stream for that endpoint**:
  ```sql
  UPDATE delivery_streams
  SET state = 'circuit_open', next_visible_at = $2
  WHERE endpoint_id = $1 AND state = 'ready';
  ```
  (Batched with `LIMIT`+loop for endpoints with thousands of lanes.)
- **Open → Half-open** at `until`. Exactly one probe cluster-wide, gated by `SET cb:{id}:probe 1 NX PX 30000`. The winner leases one stream and delivers one message. Success → close, reset counters, `UPDATE ... SET state='ready', next_visible_at=now() WHERE state='circuit_open'` (**with jitter** — `now() + random()*5s` — or you re-create the thundering herd you just avoided). Failure → back to open with a longer window.

**Park, do not slow-drain.** Slow-draining a dead endpoint means every parked delivery still costs a lease, a DNS lookup, a TCP SYN, a 5s connect timeout, an attempt row, and a WAL commit. At 10,000 queued deliveries for one dead endpoint that's 10,000 useless leases per cycle competing with healthy traffic in the same `SKIP LOCKED` scan. Parking removes them from the dequeue index entirely, and the cost of recovery is one `UPDATE ... WHERE endpoint_id = $1` on an index you already have.

**Be honest about the breaker's consistency.** Redis-backed counters race across replicas; two pods can both decide to open, or both miss the threshold by one. That is fine — a circuit breaker is a *throttle*, not a correctness mechanism. The `SET NX` on the probe is the only part that must be exclusive, and it is.

Add `endpoint_state_transitions` (endpoint_id, from, to, reason, at) as an audit table — it is the single most-requested thing on a webhook dashboard.

---

## 8. Service Decomposition & Repo Layout

### 8.1 Processes

| Binary | Responsibility | Why separate | Scaling signal |
|---|---|---|---|
| `ithaca-ingest` | `POST /v1/events`. API-key auth, quota check, idempotency, event insert + inline fanout (≤32 subs) or outbox row. **Zero outbound network.** Returns 202 in <10ms p99. | Its scaling axis is *inbound request rate*, and it must never be starved by delivery work. It is also the only component whose unavailability loses customer data, so it gets the strictest SLO, the simplest code, and the most conservative deploys. | HPA on RPS / p99 latency |
| `ithaca-dispatcher` | Leases streams, delivers, retries, breaks circuits, acks. The worker pool from §5. | Owns all long-blocking outbound I/O and all the interesting concurrency. Isolating it means an endpoint that 60-second-timeouts cannot consume ingest capacity. **This is the K8s learning vehicle**: KEDA `ScaledObject` on a Postgres query returning runnable-stream depth is a far better demo than CPU-based HPA. | KEDA on queue depth + oldest-pending age |
| `ithaca-api` | Control plane: tenants, endpoints, subscriptions, secrets, DLQ browse/replay, attempt history. Session/JWT auth, not API-key auth. | Different auth boundary (a compromised ingest key must not rotate secrets), different traffic shape (low volume, expensive queries), different deploy cadence. Also the only service that decrypts secrets for display. | HPA on CPU |
| `ithaca-ws` | WebSocket hub. Subscribes Redis pub/sub, fans out live delivery events to dashboard clients. | **Stateful** (long-lived connections). Needs long LB idle timeouts, graceful-drain-on-deploy semantics that differ from stateless services, and it is memory/FD-bound rather than CPU-bound. Co-locating it with `ithaca-api` means every API deploy drops every dashboard connection. | HPA on connection count |
| `ithaca-worker` (utility) | Fanout expander, lease reaper, partition manager (create tomorrow's, drop expired), idempotency janitor, DLQ replay ramp, daily rollups. | Singleton-ish periodic work. Runs as a `Deployment` with `replicas: 1` for the reaper (it's idempotent, so 2 is safe) plus `CronJob`s for the partition manager — another concrete K8s primitive to learn. | fixed |

Five binaries, one module, one image (multi-binary `Dockerfile` with a `CMD` per stage, or one image + `ENTRYPOINT ["/ithaca"]` with a subcommand). **One image with subcommands** — it halves the CI matrix and guarantees version consistency across services.

### 8.2 Repo layout

```
ithaca/
├── cmd/
│   └── ithaca/main.go            # single binary, subcommands: ingest|dispatcher|api|ws|worker
├── internal/
│   ├── config/                   # env → struct, validated at boot, fail fast
│   ├── model/                    # domain types. NO db/http tags leaking in.
│   ├── store/                    # pgx repositories, sqlc-generated
│   │   ├── queries/*.sql         # sqlc source of truth
│   │   └── gen/                  # sqlc output (committed)
│   ├── queue/
│   │   ├── queue.go              # ★ the Queue interface — driver boundary
│   │   ├── pg/                   # SELECT ... FOR UPDATE SKIP LOCKED driver
│   │   └── sqsfifo/              # AWS SQS FIFO driver
│   ├── worker/                   # Pool, Slots, dispatcher, heartbeat, drain
│   ├── deliver/                  # http client, signing, ssrf dialer, classify
│   ├── retry/                    # Policy, NextBackoff (pure)
│   ├── ratelimit/                # redis GCRA
│   ├── breaker/                  # redis circuit breaker
│   ├── idem/                     # idempotency keys
│   ├── ingest/                   # http handlers + fanout
│   ├── api/                      # control plane handlers
│   ├── wshub/                    # hub, client, redis subscriber
│   ├── obs/                      # slog handler, otel setup, prom metrics
│   └── clock/                    # Clock iface + real + fake
├── migrations/                   # goose, numbered, forward-only
├── test/
│   ├── integration/              # testcontainers
│   ├── receiver/                 # programmable fake subscriber
│   └── chaos/                    # jepsen-lite
├── deploy/{k8s,terraform,docker}/
└── docs/{architecture,at-least-once,ordering,benchmarks}.md
```

**Tooling opinions:** `pgx/v5` (not `database/sql` — you want `CopyFrom`, native types, and `pgxpool`); **`sqlc`** for query codegen (compile-time-checked SQL, real Go structs, no ORM, and it forces you to actually write the SQL — which is the point of this project); `goose` for migrations; `chi` for HTTP (you know it, and `gin`'s middleware model fights `context`); `gorilla/websocket` (you know it); `slog` stdlib.

### 8.3 The `Queue` interface (driver boundary)

```go
package queue

type Queue interface {
	// Enqueue is transactional: pass a tx so ingest can write event + deliveries atomically.
	Enqueue(ctx context.Context, tx Tx, items []NewDelivery) error

	LeaseStreams(ctx context.Context, req LeaseReq) ([]*Lease, error)
	FetchStream(ctx context.Context, l *Lease, limit int) ([]Message, error)
	Renew(ctx context.Context, l *Lease, ttl time.Duration) (time.Time, error)
	Ack(ctx context.Context, l *Lease, results []Result) error
	Release(ctx context.Context, l *Lease) error
	ReleaseUntil(ctx context.Context, l *Lease, until time.Time) error  // rate-limit deferral

	Stats(ctx context.Context) (Depth, error)   // for KEDA / dashboard
}
```

### 8.4 SQS FIFO driver — the honest architectural finding

Mapping: `MessageGroupId = stream_key` (SQS FIFO already serializes per group and already blocks the group on a non-deleted message — **identical semantics to our stream lease and to `hol_policy='pause'`**, which is a satisfying convergence). `MessageDeduplicationId = delivery_id`. Visibility timeout = lease.

But SQS FIFO cannot be the whole queue, for concrete, checkable reasons:

1. **FIFO queues do not support per-message `DelaySeconds` at all** (only a queue-level delay), and the max delay is 900s regardless. Our retry schedule goes to 6 hours. There is no way to express it.
2. Max **visibility timeout is 12h**, max **retention is 14 days** — retention is survivable, but the delay limit is not.
3. Throughput: 300 API calls/s (3,000 messages/s batched) per FIFO queue; high-throughput mode raises this substantially but changes dedup scoping to per-message-group. Verify current regional limits before quoting a number.

**Therefore: Postgres remains the system of record and the *scheduler*; SQS FIFO carries only immediately-runnable work.** A scheduler component moves deliveries whose `next_attempt_at <= now()` from Postgres into SQS; workers consume SQS; results are written back to Postgres; long retries never enter SQS. This is not a workaround — it's the correct decomposition, and it is exactly how you'd describe it in an interview: *"the managed queue is a transport, not a scheduler; the scheduler needs arbitrary-horizon delayed visibility and durable state that SQS doesn't provide."* It also keeps the two drivers genuinely comparable rather than one being a toy.

### 8.5 Observability (the `log.Printf` gap)

- `slog` with `JSONHandler` in prod, `TextHandler` locally. A custom handler that pulls `trace_id`/`span_id` off the context so every line correlates. Canonical log line per delivery attempt: one wide event with ~20 fields, not 8 scattered lines.
- OTel tracing, OTLP → Jaeger locally, ADOT → X-Ray in the cloud week. **Trace propagation across the queue must use span *links*, not parent-child**: a delivery can happen six hours after ingest, and a six-hour span is meaningless in every trace UI and blows the sampling model. Store `traceparent` on the event row; the worker starts a *new* root span with `trace.LinkFromContext(ingestCtx)`. This is a genuinely non-obvious detail and it's the right answer.
- Prometheus: `ithaca_deliveries_total{outcome,status_class}`, `ithaca_delivery_duration_seconds` (histogram, native if you can), `ithaca_queue_depth{state}`, `ithaca_queue_oldest_pending_seconds` (**your best single health metric**), `ithaca_pool_slots_in_flight`, `ithaca_lease_reaped_total`, `ithaca_breaker_state`, `ithaca_stream_paused_total`.

### 8.6 WebSocket hub, briefly

`dispatcher` publishes to Redis `ithaca:tenant:{id}:events` after ack commit. Hub keeps one Redis subscription per *connected* tenant (refcounted, unsubscribed at zero). Each client has a **bounded** send buffer (`chan []byte`, cap 256); on overflow, drop and send a `{"type":"lagged","dropped":N}` marker rather than blocking the hub goroutine or growing unboundedly. Backpressure, again — the same lesson in a second context, which is why it's worth building. Write deadlines on every write, ping every 30s, `ReadLimit` set, and drop the connection on pong timeout.

---

## 9. Testing Strategy

Written for someone with one test file total. The goal is not coverage; it's **the ability to change this system without fear**.

### 9.1 Table-driven unit tests — the base layer

```go
func TestClassify(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		status   int
		hdr      http.Header
		err      error
		want     Outcome
		wantHint time.Duration
	}{
		{"200 ok",            200, nil, nil, OutcomeSuccess, 0},
		{"500 retryable",     500, nil, nil, OutcomeRetryable, 0},
		{"410 permanent",     410, nil, nil, OutcomePermanent, 0},
		{"302 permanent",     302, nil, nil, OutcomePermanent, 0},
		{"429 delta-seconds", 429, http.Header{"Retry-After": {"120"}}, nil, OutcomeRateLimited, 2 * time.Minute},
		{"429 http-date",     429, http.Header{"Retry-After": {"Wed, 31 Jul 2026 12:00:00 GMT"}}, nil, OutcomeRateLimited, 0},
		{"429 absurd clamped",429, http.Header{"Retry-After": {"999999"}}, nil, OutcomeRateLimited, time.Hour},
		{"timeout",           0, nil, context.DeadlineExceeded, OutcomeRetryable, 0},
		{"conn refused",      0, nil, syscall.ECONNREFUSED, OutcomeRetryable, 0},
		{"nxdomain",          0, nil, &net.DNSError{IsNotFound: true}, OutcomePermanent, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, hint, _ := Classify(mkResp(tt.status, tt.hdr), tt.err)
			if got != tt.want { t.Errorf("outcome = %v, want %v", got, tt.want) }
			if tt.wantHint != 0 && hint != tt.wantHint {
				t.Errorf("hint = %v, want %v", hint, tt.wantHint)
			}
		})
	}
}
```

Rules: name the field `want`/`got`; `t.Parallel()` in both the outer and inner function; one `t.Run` per case so failures name themselves; **never** `assert` libraries at first — use `if got != want { t.Errorf(...) }` until you feel the pain, then reach for `google/go-cmp`'s `cmp.Diff`.

Best first targets, because they're pure: `retry.NextBackoff`, `deliver.Classify`, `deliver.Sign`, `ssrf.blocked`, `stream_key` derivation, `Retry-After` parsing.

### 9.2 Integration tests with testcontainers-go

```go
var testDB *pgxpool.Pool

func TestMain(m *testing.M) {
	ctx := context.Background()
	pg, err := postgres.Run(ctx, "postgres:17-alpine",
		postgres.WithDatabase("ithaca_tmpl"),
		postgres.WithUsername("ithaca"), postgres.WithPassword("ithaca"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(60*time.Second)),
	)
	if err != nil { log.Fatal(err) }
	defer testcontainers.CleanupContainer(nil, pg)
	dsn, _ := pg.ConnectionString(ctx, "sslmode=disable")
	migrateUp(dsn)              // run goose ONCE against the template
	testDB = mustPool(dsn)
	os.Exit(m.Run())
}

// ★ The template-database trick: per-test isolation in ~20ms instead of
//   re-running every migration (~2s) per test.
func newTestDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	name := "t_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:16]
	mustExec(t, adminPool, `CREATE DATABASE `+name+` TEMPLATE ithaca_tmpl`)
	t.Cleanup(func() { mustExec(t, adminPool, `DROP DATABASE `+name+` WITH (FORCE)`) })
	return mustPool(dsnFor(name))
}
```

Why a real Postgres and not sqlmock: **every interesting behavior in this system is Postgres behavior.** `SKIP LOCKED`, `ON CONFLICT DO UPDATE` row locking, partial index selection, partition pruning, row-level lock ordering. A mock tests that you called a function; you need to test that Postgres does what you believe it does. `WITH (FORCE)` on drop (PG 13+) avoids "database is being accessed by other users" flakes.

Gate with a build tag (`//go:build integration`) so `go test ./...` stays fast; CI runs both.

### 9.3 Deterministic time

**Two techniques, used for different things.**

**(a) Make time a parameter, not a dependency.** `NextBackoff(attempt, policy, *rand.Rand)` is pure — seed the RNG and assert exact values. If a function reads the clock internally, you cannot test it; if it takes the time as an argument, the test is one line. Push clock reads to the edges. This is 80% of the benefit for 5% of the machinery.

**(b) `testing/synctest` — new and GA in Go 1.25, and it is made for exactly this.**

```go
func TestHeartbeatRenewsAndCancelsOnLoss(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := &fakeQueue{renewOK: true}
		ctx, cancel := context.WithCancel(t.Context())
		hb := newHeartbeat(q, lease, 20*time.Second, realClock{}, cancel)
		go hb.run(ctx)

		time.Sleep(65 * time.Second)  // INSTANT — fake clock inside the bubble
		synctest.Wait()               // let all bubble goroutines settle
		if got := q.renewCount(); got != 3 {
			t.Fatalf("renewals = %d, want 3", got)
		}

		q.setRenewOK(false)           // simulate losing the lease
		time.Sleep(25 * time.Second)
		synctest.Wait()
		if ctx.Err() == nil {
			t.Fatal("expected lease loss to cancel the delivery context")
		}
	})
}
```

Inside a `synctest` bubble, `time` is virtual and advances instantly once all bubbled goroutines are durably blocked — so a test of a 6-hour backoff schedule runs in microseconds, with no sleeps and no flakes. The constraint: a goroutine blocked on **real** I/O (a Postgres query, a real socket) is never "durably blocked," so the bubble stalls. Therefore: **`synctest` for the scheduler/heartbeat/pool timing logic against in-memory fakes; injected `Clock` + real time for integration tests that touch Postgres.** Keep the `Clock` interface anyway — it's how you fast-forward the *database's* notion of due time (or just write `next_attempt_at` directly in test setup).

### 9.4 The programmable fake receiver

The most valuable test asset in the repo. Build it first.

```go
package receiver

type Script struct {
	Status   int
	Delay    time.Duration
	Body     string
	Hang     bool          // never respond → exercises client timeout + lease renewal
	Close    bool          // hijack + RST → exercises conn-reset classification
	Headers  http.Header   // e.g. Retry-After
}

type Receiver struct {
	*httptest.Server
	mu       sync.Mutex
	script   []Script      // consumed in order; last entry repeats
	received []Received    // full record of every request
	secret   []byte        // if set, HMAC is verified and a bad sig 401s
}

func New(t *testing.T, opts ...Option) *Receiver

// Assertions the tests actually call:
func (r *Receiver) Requests() []Received
func (r *Receiver) IDs() []string                       // Ithaca-Id, in arrival order
func (r *Receiver) OrderFor(key string) []int64         // seq numbers seen, in arrival order
func (r *Receiver) AssertAllSignaturesValid(t *testing.T)
func (r *Receiver) AssertNoDuplicates(t *testing.T)
```

Modes worth building on day one: `AlwaysOK`, `FailNThenOK(n, status)`, `Flaky(seed, failRate)` (seeded → reproducible), `Slow(d)`, `Hang`, `RateLimit(after, retryAfter)`, `RejectBadSignature`.

`Hang` is the one that finds real bugs: it proves the client honors `endpoint.timeout_ms`, that the heartbeat renews the lease during a long request, and that shutdown actually cancels in-flight requests.

### 9.5 Testing the ordering guarantee

```go
//go:build integration

func TestOrderedStreamNeverReorders(t *testing.T) {
	db := newTestDB(t)
	rcv := receiver.New(t, receiver.Flaky(42, 0.30), receiver.Slow(5*time.Millisecond))
	sub := seedSubscription(t, db, rcv.URL, withOrdered(true))

	const keys, perKey = 8, 200
	for k := 0; k < keys; k++ {
		for i := 0; i < perKey; i++ {
			ingest(t, db, sub, fmt.Sprintf("key-%d", k), payload{Seq: i})
		}
	}

	// Real concurrency. If ordering only holds at Workers=1, the design is wrong.
	pool := worker.New(worker.Config{Workers: 32, MaxStreamsBatch: 16, DrainBatch: 64}, ...)
	runUntilDrained(t, pool, db, 90*time.Second)

	for k := 0; k < keys; k++ {
		seen := rcv.OrderFor(fmt.Sprintf("key-%d", k))
		// AT-LEAST-ONCE: duplicates are legal, REGRESSION is not.
		// The subsequence with duplicates removed must be exactly 0..perKey-1.
		var maxSeen int64 = -1
		for _, s := range seen {
			if s < maxSeen {
				t.Fatalf("key-%d: reordering — saw seq %d after %d; full order: %v", k, s, maxSeen, seen)
			}
			maxSeen = s
		}
		if maxSeen != perKey-1 {
			t.Fatalf("key-%d: incomplete, highest seq = %d", k, maxSeen)
		}
	}
}
```

Non-negotiables for this test: **32 workers** (concurrency is the thing under test), **30% induced failures** (retries are where reordering hides), **multiple keys** (proves lanes are independent), and the assertion is *monotonic non-decreasing*, not *strictly equal to the input list* — because duplicates are allowed by the contract and asserting equality gives you a flaky test that is "correct" by an invariant you never promised.

Run the whole suite with `-race`. Then run this specific test with `-count=20`.

### 9.6 Testing idempotency

```go
func TestIngestIdempotencyUnderConcurrency(t *testing.T) {
	db := newTestDB(t)
	srv := ingest.New(db, ...)
	body := []byte(`{"type":"order.created","data":{"id":"o_1"}}`)

	var g errgroup.Group
	codes := make([]int, 64)
	ids   := make([]string, 64)
	for i := range codes {
		g.Go(func() error {
			resp := post(srv, body, hdr("Idempotency-Key", "k-1"))
			codes[i], ids[i] = resp.Code, resp.EventID
			return nil
		})
	}
	_ = g.Wait()

	// EXACTLY ONE event row, no matter how many concurrent requests.
	if n := countRows(t, db, `SELECT count(*) FROM events`); n != 1 {
		t.Fatalf("events = %d, want 1", n)
	}
	// Every caller got either the created response or a well-formed replay/409.
	for i := range codes {
		if codes[i] != 202 && codes[i] != 409 {
			t.Fatalf("call %d: status %d", i, codes[i])
		}
		if codes[i] == 202 && ids[i] != ids[0] && ids[0] != "" {
			t.Fatalf("call %d: divergent event id %s vs %s", i, ids[i], ids[0])
		}
	}
}

func TestIdempotencyKeyReuseWithDifferentBody(t *testing.T) {
	// first POST → 202; second POST, same key, different body → 422, and
	// crucially: still exactly 1 event row.
}
```

### 9.7 Chaos / jepsen-lite: the no-loss check

A single long-running integration test, `//go:build chaos`, run nightly in CI and on demand:

```go
func TestNoEventIsEverLost(t *testing.T) {
	db  := newTestDB(t)
	rcv := receiver.New(t, receiver.Flaky(seed, 0.25))
	subs := seedSubscriptions(t, db, rcv.URL, 5 /*endpoints*/)

	produced := produceContinuously(t, db, subs, 20_000 /*events*/, 500 /*eps*/)

	// Run 6 worker pools as independent "processes". Randomly kill and
	// restart them mid-flight — an abrupt cancel with NO drain is the
	// closest in-process analogue of SIGKILL, and it exercises exactly
	// the crash-between-200-and-ack window.
	sup := newSupervisor(db, 6)
	go sup.chaos(t, chaosOpts{
		KillEveryMean: 3 * time.Second,
		PauseReceiver: pauseSpec{Every: 20 * time.Second, For: 5 * time.Second},
		DBBlackout:    blackoutSpec{Every: 45 * time.Second, For: 3 * time.Second},
		ClockSkew:     true,   // reaper races: set locked_until into the past
	})

	waitForQuiescence(t, db, 5*time.Minute)
	sup.stopAll()

	// ── INVARIANTS ──────────────────────────────────────────────────────
	// I1 NO LOSS: every (event, subscription) pair reached a terminal state.
	//    Nothing is stuck 'pending' and nothing vanished.
	assertZero(t, db, `SELECT count(*) FROM deliveries`)
	assertEq(t, len(produced)*len(subs),
		countRows(t, db, `SELECT count(*) FROM deliveries_history`))

	// I2 AT-LEAST-ONCE: everything marked succeeded was ACTUALLY received.
	//    ★ This is the invariant that catches a premature ack — the single
	//    worst bug this system can have, and the one no unit test finds.
	seen := rcv.DeliveryIDSet()
	for _, id := range queryIDs(t, db,
		`SELECT id FROM deliveries_history WHERE final_state='succeeded'`) {
		if !seen.Has(id) {
			t.Fatalf("PHANTOM ACK: delivery %s marked succeeded but the receiver never saw it", id)
		}
	}

	// I3 NO PHANTOM DELIVERY: nothing the receiver saw is absent from history.
	for id := range seen {
		if !existsInHistory(t, db, id) {
			t.Fatalf("delivery %s was sent but has no history row", id)
		}
	}

	// I4 ORDER: per ordering_key, arrival order is non-decreasing (dupes ok).
	assertPerKeyMonotonic(t, rcv)

	// I5 NO ORPHANED LEASE: reaper cleaned up after every killed worker.
	assertZero(t, db, `SELECT count(*) FROM delivery_streams WHERE state='leased'`)

	// I6 BOUNDED DUPLICATION: at-least-once is not a license for 10x.
	//    Duplicates should be a small fraction of total, proportional to kills.
	if dupRate := rcv.DuplicateRate(); dupRate > 0.05 {
		t.Fatalf("duplicate rate %.3f exceeds 5%% — the ack window is too wide", dupRate)
	}
}
```

I2 is the whole point. Every other invariant can be satisfied by a system that lies about success. And I6 turns "at-least-once" from an excuse into a **measurable budget** — if a refactor pushes duplication from 0.4% to 8%, the test tells you, which is precisely the kind of result that makes a good README graph.

### 9.8 CI (the zero-CI gap)

GitHub Actions, three workflows:

- **`pr.yml`** (< 5 min): `go vet`, `golangci-lint` (`errcheck, govet, staticcheck, gosec, bodyclose, sqlclosecheck, rowserrcheck, contextcheck, noctx`), `go test -race -count=1 -shuffle=on ./...` (unit only), `go build ./...`. `-shuffle=on` catches inter-test coupling, which you *will* write.
- **`integration.yml`**: the tagged integration suite via testcontainers (works on GH runners out of the box — that's the reason to prefer testcontainers over a `services:` block, since the same tests then run identically on your laptop), plus `sqlc diff` and a migration up/down/up round-trip.
- **`nightly.yml`**: chaos suite, `go test -race -count=10` on the ordering and idempotency tests, `govulncheck`, Trivy on the image, and a k6/Locust throughput run whose numbers land in `docs/benchmarks.md`.

Add a `benchmarks` job that plots **dequeue throughput vs. worker count** for both the naive per-message `SKIP LOCKED` design and the stream-lease design. That graph, checked into the repo with the methodology, is worth more on a resume than another feature — it's evidence you measured a distributed-systems claim instead of repeating it.

---

## 10. Six-to-eight week sequencing

| Wk | Deliverable | Gap closed |
|---|---|---|
| 1 | Module, config, migrations, `store` + sqlc, `events`/`deliveries`/`delivery_streams`, ingest handler with idempotency. **First test file: `retry.NextBackoff` table-driven.** | testing |
| 2 | `queue/pg` driver end-to-end: lease/fetch/renew/ack/release + reaper. Fake receiver. First testcontainers integration test. | testing, PG queues |
| 3 | **Worker pool + Slots + backpressure + graceful shutdown.** `synctest` tests for heartbeat/dispatcher. | worker pools |
| 4 | Delivery client: HMAC, rotation, SSRF dialer, classification, retry/DLQ. Ordering test at 32 workers. | security |
| 5 | Redis: rate limit, circuit breaker, wake pub/sub. WS hub with bounded buffers. | Redis |
| 6 | `slog` + OTel with span links + Prometheus. CI (all three workflows). Chaos suite. | logging/tracing, CI |
| 7 | Kubernetes: kind cluster, Deployments, HPA/KEDA on queue depth, CronJob partition manager, probes + `terminationGracePeriodSeconds`. Helm or Kustomize. | **Kubernetes** |
| 8 | `queue/sqsfifo` driver + the scheduler bridge. Terraform for the demo week. Benchmarks doc. Deploy, run load, tear down. | cloud path |

Kubernetes lands in week 7 deliberately: by then the services have real readiness/liveness semantics, real drain behavior, and a real custom scaling metric, so K8s teaches you something instead of being `kubectl apply` on a hello-world.

---

## Critical Files for Implementation

- `/Users/jay/Documents/Programming/ithaca/internal/queue/queue.go` — the `Queue` interface; the driver boundary that makes the Postgres/SQS-FIFO comparison real.
- `/Users/jay/Documents/Programming/ithaca/internal/queue/pg/lease.go` — the `SKIP LOCKED` stream-lease, fetch, fencing-token ack, and archive-on-complete statements from §2.
- `/Users/jay/Documents/Programming/ithaca/internal/worker/pool.go` — `Slots`, the dispatcher backpressure loop, drain, heartbeat, graceful shutdown (§5).
- `/Users/jay/Documents/Programming/ithaca/internal/deliver/client.go` — SSRF `Dialer.Control`, shared `http.Transport`, no-redirect policy, HMAC signing, `Classify` (§6.2, §7).
- `/Users/jay/Documents/Programming/ithaca/migrations/0001_init.sql` — the DDL, especially the two partial indexes and the `fillfactor`/autovacuum settings that keep the queue from bloating.
- `/Users/jay/Documents/Programming/ithaca/test/receiver/receiver.go` — the programmable fake subscriber that every meaningful test depends on; build it before the first delivery test.