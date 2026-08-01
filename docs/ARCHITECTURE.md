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
                       │    worker    │  fanout · reaper · partitions · replay
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

1. **Nothing is deleted row by row.** Terminal deliveries are *moved* out of the hot table in the
   same statement that terminates them, so the queue table stays proportional to backlog rather than
   to lifetime volume. `events`, `deliveries_history`, `delivery_attempts` and `idempotency_keys`
   are range-partitioned by time; retention is `DROP TABLE`, an O(1) metadata operation instead of a
   `DELETE` that generates gigabytes of WAL and dead tuples.
2. **The dequeue index is partial.** It covers only runnable streams — hundreds of rows even when
   the system holds millions of pending deliveries — so it stays resident in `shared_buffers`.
3. **Primary keys are UUIDv7.** Time-ordered, so B-tree inserts land at the right edge instead of
   scattering random I/O across the index the way v4 does.

**Redis is never the source of truth.** It carries queue wakeup hints, rate-limit buckets, circuit
breaker state and dashboard sessions. Losing it must cost latency, never correctness — a wakeup hint
that never arrives just means the poller finds the work 250ms later.

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

**Rate limits and open circuits are scheduling decisions.** A throttled endpoint re-dates its stream
and releases the worker slot immediately: no HTTP request, no consumed retry attempt, no wasted
lease. An open circuit *parks* every stream for that endpoint rather than slow-draining it, because
slow-draining 10,000 queued deliveries to a dead endpoint costs 10,000 useless leases competing with
healthy traffic in the same index scan.

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

Local development runs the dependencies in compose and the services on the host. Kubernetes runs on
k3d locally and EKS during the demo week, from the same Kustomize base.

Two lifecycle details that are easy to get wrong and expensive to get wrong:

**Liveness never touches a dependency.** A liveness probe that pings Postgres turns a 30-second RDS
blip into a fleet-wide simultaneous restart, cold connection pools and a thundering herd on
recovery — a real outage manufactured from a non-event. Liveness answers "is this process wedged";
readiness answers "should traffic come here."

**Readiness fails before the listener stops.** Shutdown marks the pod unready, then keeps serving for
a drain delay while load balancers notice, and only then stops accepting. Skipping that window is
why "graceful" shutdowns still return 502s. `terminationGracePeriodSeconds` must exceed the drain
delay plus the grace budget, or the kubelet SIGKILLs the process mid-drain.
