# Architecture

How Ithaca is put together, and why. Deeper treatments live in
[`design/delivery-engine.md`](design/delivery-engine.md) and
[`design/dashboard.md`](design/dashboard.md); decisions live in [`DECISIONS/`](DECISIONS/).

## The shape of the problem

A webhook platform is an availability translator. Producers need a write path that always succeeds
in single-digit milliseconds; subscribers are third-party HTTP servers that are slow, flaky,
rate-limited, occasionally gone forever, and sometimes not who they claim to be. Everything in the
design follows from keeping those two facts apart.

That translation is why the queue is not an implementation detail. It is the product.

## Processes

```
                       ┌──────────────┐
   POST /v1/events ───▶│    ingest    │──┐   202 in <10ms
                       └──────────────┘  │   makes no outbound calls, ever
                                         ▼
                              ┌─────────────────────┐
                              │      Postgres       │  system of record
                              │  streams · queue    │  AND scheduler
                              └──────────┬──────────┘
                        lease a STREAM   │
                                         ▼
                       ┌──────────────────────────────┐
                       │         dispatcher           │──▶ subscriber
                       │  bounded pool · backpressure │◀── retry · breaker · DLQ
                       └──────────────┬───────────────┘
                                      │ publish after ack
                                      ▼
                       ┌───────┐   ┌──────┐   ┌────────────┐
                       │ redis │──▶│  ws  │──▶│ dashboard  │
                       └───────┘   └──────┘   └────────────┘

                       ┌──────────────┐
                       │    worker    │  janitor · lease reaper · DLQ replay
                       └──────────────┘
```

All five run from one binary; see [ADR-0001](DECISIONS/adr-0001-single-binary-subcommands.md).

### Why these boundaries

**ingest is separated because it is the only component whose unavailability loses data.** Everything
else can be down for an hour and the system recovers; if ingest rejects a write, the customer's
event is simply gone. So it gets the strictest SLO, the simplest code, the most conservative
deploys, and — critically — no outbound network calls at all. A subscriber that takes 60 seconds to
time out cannot consume ingest capacity, because ingest never talks to subscribers.

**dispatcher is separated because it owns all long-blocking I/O and all the interesting
concurrency.** It is also the only service that scales on a signal other than CPU or request rate:
queue depth. That makes it the KEDA target and the most interesting thing in the cluster.

**ws is separated because it is stateful.** Long-lived connections need different load-balancer idle
timeouts, a different drain story, and they are memory- and file-descriptor-bound rather than
CPU-bound. Co-locating it with the API would mean every control-plane deploy drops every open
dashboard.

## Storage

**Postgres is the system of record *and* the scheduler.** Both roles matter. Being the scheduler is
what lets a retry be dated six hours into the future — something no managed queue offers, and the
reason SQS is a transport in this design rather than the queue itself.

Three principles govern the schema:

1. **The queue table holds only pending work.** Terminal deliveries are *moved* out of
   `deliveries` into `deliveries_history` in the same statement that terminates them, so the hot
   table stays proportional to backlog rather than to lifetime volume, and VACUUM never re-scans
   finished history.
2. **The dequeue index is partial.** It covers only runnable streams — hundreds of rows even when
   the system holds millions of pending deliveries — so it stays resident in `shared_buffers`.
3. **Primary keys are UUIDv7.** Time-ordered, so B-tree inserts land at the right edge instead of
   scattering random I/O across the index the way v4 does.

**Ingest is one transaction.** Idempotency claim, event insert and fanout (one delivery per matching
subscription) commit together or not at all. That transaction is the no-loss boundary: a `202` means
every delivery row exists. A delivery's `seq` comes from its stream row's `next_seq`, incremented
under the row lock that `INSERT … ON CONFLICT DO UPDATE` holds until commit, so `seq` order equals
commit order and has no gaps. A `BIGSERIAL` would hand out numbers outside transaction order.

**Idempotency without an `in_flight` state.** The key is claimed first, inside the ingest
transaction. A concurrent duplicate blocks on the primary-key index until the winner commits, then
reads the completed row and replays it byte-for-byte (`Idempotent-Replay: true`). The same key with
a different body gets `422`. Since nothing half-finished is ever committed, a crash leaves nothing
to recover.

**What is deliberately not here yet: time partitioning.** At this project's scale, `events`,
`deliveries_history`, `delivery_attempts` and `idempotency_keys` are plain tables, and retention is a
batched `DELETE` (the worker's janitor). The scale-up path is range partitioning by time, with
retention as `DROP TABLE`, an O(1) metadata operation. Switch when WAL volume from retention deletes
or vacuum churn on these tables shows up on a graph. The cost is known up front: the partition key
must join every unique constraint, so idempotency uniqueness becomes per-partition and lookups by
bare id fan out across partitions. `deliveries` must **never** be partitioned by `state`: an UPDATE
that moves a row across partitions makes a concurrent `SELECT … FOR UPDATE` fail with "tuple to be
locked was already moved to another partition".

**Redis is off the delivery path entirely.** Circuit-breaker state lives in Postgres next to the
streams it parks, and the dispatcher finds work by adaptive polling of the partial dequeue index.
That leaves one fewer moving part to fail, deploy and explain. Redis is used only to fan out live
dashboard updates, where losing it costs a stale graph, never a delivery.

## The delivery engine

The full treatment is in [`design/delivery-engine.md`](design/delivery-engine.md). The load-bearing
ideas:

**Streams, not messages** ([ADR-0003](DECISIONS/adr-0003-stream-leasing.md)). A stream is leased by
at most one worker at a time, which is what makes per-key ordering compatible with a concurrent
pool.

**Fencing tokens.** A worker partitioned from Postgres may still be mid-request when the reaper
reassigns its stream. Every mutation is conditional on the lease token, so the zombie cannot ack work
another worker is retrying.

**Acquire capacity before dequeuing.** The dispatcher takes worker slots *first*, then leases exactly
that many streams. The inverse — dequeue, then find somewhere to put it — converts a queue-depth
problem into a memory problem and takes out leases that expire unserviced, producing a livelock in
which the system is fully occupied reaping its own abandoned work.

**Rate limits and open circuits are scheduling decisions.** A `429` re-dates the delivery
(honouring `Retry-After`) without consuming a retry attempt, so a throttling subscriber cannot burn
its own retry budget. An open circuit *parks* every stream for that endpoint rather than slow-draining it, because
slow-draining 10,000 queued deliveries to a dead endpoint costs 10,000 useless leases competing with
healthy traffic in the same index scan.

### Measured, not assumed

Four things the tests measured that changed the implementation:

- **Ack batching sets the duplicate rate.** Acking once per 64-message drain batch meant a crashed
  dispatcher threw away the acks for everything the subscriber had already accepted in that batch:
  **17% duplicates** in `TestKilledDispatcherLosesNothing`. Fetching 64 but acking in chunks of the
  lane's concurrency (8) cut that to **~5%**, bounded by one chunk per lease held, at the cost of
  ~25% peak throughput (14.7k → 10.9k deliveries/s under the race detector). For a webhook system
  that is the right trade: duplicates cost every subscriber, raw throughput is rarely the limit.
- **Crash recovery time equals the lease TTL.** A `kill -9`'d dispatcher's streams sit leased until
  their lease expires (30s minimum) and the reaper reclaims them. Shorter leases recover faster but
  cost more heartbeat writes; the reaper runs inside every dispatcher, so there is no separate
  process to forget to deploy.
- **The retry schedule.** Quoting a backoff schedule by its window ceilings and then applying full
  jitter roughly halves the real duration. A 5s base that "takes ~6h" actually dead-letters after
  ~1.4h. The default is a 30s base, which simulates to ~7h.
- **Panics on errgroup goroutines.** `recover()` only catches panics on its own goroutine, so a
  `recover` around a drain loop never sees a panic in a concurrent delivery. One bad delivery would
  have crashed the whole dispatcher. The recover lives on the goroutine that runs the attempt.

## Guarantees

**At-least-once, stated plainly.** Exactly-once over HTTP would need a distributed transaction with
the subscriber, which does not exist. The unclosable window is the crash between reading the
subscriber's `200` and committing the ack. It is minimised — the ack is a single fast statement with
no Redis publish, metric flush or trace export in front of it — and then documented, with every
delivery carrying a stable `Ithaca-Id` so subscribers can deduplicate.

Four invariants are enforced as tests:

| | |
|---|---|
| **No loss** | after quiescence every (event × subscription) pair is terminal |
| **No phantom acks** | nothing marked delivered that the subscriber did not receive |
| **No reordering** | within an ordering key, the observed sequence is monotonic |
| **Ingest never blocks** | a total subscriber outage does not affect acceptance |

## Security

**Outbound SSRF defence uses `net.Dialer.Control`.** Validate-then-connect is broken: you resolve
`evil.com` to a public IP, approve it, and `http.Client` resolves again — to `169.254.169.254` — and
connects to the instance metadata service. `Control` runs after resolution and before `connect(2)`,
with the literal address, so there is no window. The blocklist includes `64:ff9b::/96` (NAT64) and
`2002::/16` (6to4), which encode private IPv4 addresses inside public-looking IPv6 and which most
hand-rolled blocklists miss.

**Endpoints must complete a challenge handshake before receiving anything.** Without it, a fanout
service that will POST to any URL a stranger types in is a rentable Layer 7 amplifier. Fanout writes
no delivery rows at all for an unverified endpoint.

**Signatures cover the exact bytes transmitted**, plus the timestamp and event ID — not the body
alone, which is trivially replayable. Payloads are stored as `bytea` and written to the wire
unchanged; re-serialising from `jsonb` would reorder keys and produce a signature the customer
cannot reproduce.

## Observability

Structured `slog` with context-carried request, tenant and trace IDs, so one delivery attempt
produces one wide event rather than eight lines to correlate by hand.

**Trace context crosses the queue hop as a span *link*, not a parent-child edge.** A delivery can
happen six hours after ingest; a six-hour span is meaningless in every trace UI and destroys the
sampling model. The `traceparent` is stored on the event row and the worker starts a new root span
linked to it.

The single most useful health metric is `ithaca_queue_oldest_pending_seconds`. Throughput can look
fine while the oldest item quietly ages.

## Deployment

Local development runs the dependencies in compose and the services on the host. Schema changes are
an explicit `ithaca migrate` step (a Job in Kubernetes), never run implicitly at boot, so six
replicas never race a migration on a rolling deploy. The public demo runs the compose stack on a
single VM. Kubernetes runs on k3d locally from a Kustomize base that is cloud-agnostic.

Two lifecycle details that are easy to get wrong and expensive to get wrong:

**Liveness never touches a dependency.** A liveness probe that pings Postgres turns a 30-second RDS
blip into a fleet-wide simultaneous restart, cold connection pools and a thundering herd on
recovery — a real outage manufactured from a non-event. Liveness answers "is this process wedged";
readiness answers "should traffic come here."

**Readiness fails before the listener stops.** Shutdown marks the pod unready, then keeps serving for
a drain delay while load balancers notice, and only then stops accepting. Skipping that window is
why "graceful" shutdowns still return 502s. `terminationGracePeriodSeconds` must exceed the drain
delay plus the grace budget, or the kubelet SIGKILLs the process mid-drain.
