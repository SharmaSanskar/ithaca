# Benchmark results

Every number here was measured, on one laptop, with production durability. Percentiles only,
warm-up excluded. Delivery latencies are **exact**: computed in SQL from the attempt log
(`bench/latency.sql`), not estimated from histogram buckets. Rerun any experiment with
`make bench-up && make bench E=e3`.

```
machine   Apple M4, 10 cores, 16 GB · Docker Desktop: 10 CPUs, 7.7 GB
postgres  17, fsync=on, synchronous_commit=on (a separate durable instance, port 5433;
          the ack path opts into synchronous_commit=off per transaction, by design)
load      k6 2.4.0 in Docker, open model (constant arrival rate: a slow server cannot
          slow the load down, which is what hides latency in closed-model tests)
receiver  ithaca receiver-sim on the host, fixed latency per sink
```

Everything runs on one machine, so the load generator, Postgres, and Ithaca compete for the same
cores, and Docker Desktop's VM adds a network hop to every query. Treat the absolute numbers as
a floor; the shapes and ratios are the result.

## Summary

| | |
|---|---|
| Ingest, 100% accepted | up to **3,000 events/s** sustained (p99 < 50 ms reliably up to ~1,000/s) |
| Drain, one endpoint, 64 lanes | **13,560 deliveries/s** at 64 workers, 20 ms receiver |
| End-to-end latency at 200 events/s | **p50 38 ms · p99 54 ms**, of which 21 ms is the receiver itself |
| Noisy neighbor (30% failing endpoint) | healthy endpoint's p99 **53.8 → 53.4 ms** (no change) |
| Endpoint down 60 s at 150 events/s | ingest **100% accepted**, p99 8 ms; 14.5k deliveries parked, 0 lost |
| `kill -9` a dispatcher every 20 s | **0 lost, 0 phantom acks**, 3 duplicates in 24,002 (0.01%) |

Benchmarks found two real bugs and the reason for one ceiling. All three are covered under
[What the benchmarks found](#what-the-benchmarks-found).

---

## E1 · Ingest ceiling

`POST /v1/events`, dispatcher off, one subscription (fanout 1, 16 lanes), payload ~120 B,
stepped 20 s per rate. Three runs; p99 in ms:

| target/s | achieved/s (worst run) | accepted | p50 | p99 run 1 | p99 run 2 | p99 run 3 |
|---:|---:|---:|---:|---:|---:|---:|
| 250 | 250 | 100% | 1.9 | 20.0 | 8.5 | 6.6 |
| 500 | 500 | 100% | 1.3 | 5.0 | 79.0 | 4.0 |
| 1,000 | 1,000 | 100% | 1.1 | 34.6 | 30.5 | 73.7 |
| 2,000 | 2,000 | 100% | 2.9–3.8 | 138 | 398 | 91 |
| 3,000 | 2,915 | 100% | 8–408 | 798 | 1,115 | 604 |
| 4,000 | 2,510 | 100% | ~1,000+ | 3,100 | 3,056 | 2,490 |
| 6,000 | 1,774 | 100% | ~2,000 | 4,000 | 4,138 | 3,086 |

**Reading it.** Nothing was ever rejected or dropped: past the knee, ingest queues instead of
failing. The knee sits between 2,000 and 3,000/s, and p99 under 50 ms holds through ~1,000/s
except for one-off spikes (single steps, different in every run: this is a shared laptop).

**Why it saturates.** Sampling Postgres at a held 3,000/s (it keeps up there, with p99 at 325 ms):

- *Not CPU.* Postgres used ~2 of 10 cores and the ingest process under 1.
- *Row locks on the stream.* `EnqueueDelivery` averaged **1.10 ms** against 0.05 ms for the plain
  event insert, with 2–5 backends waiting on a `transactionid` lock at any moment. Gapless
  per-stream `seq` is assigned under the stream row's lock, which is held **through the commit's
  fsync** (`WALWrite`/`WalSync` waits). With 16 lanes, at most 16 enqueues commit concurrently.
- *Round trips.* 11–17 backends sat `idle in transaction`, waiting on the client. Each event is
  five statements (begin, fanout lookup, insert event, enqueue, commit), each crossing Docker's VM.

This is the price of the ordering guarantee, and it is per stream rather than global: more lanes
spread the lock. The next step would be collapsing the transaction into one statement (or
pipelining it), which removes four round trips while the lock is held. Not done: 1,000/s at
p99 < 50 ms on one laptop is well past what this project needs, and the fix belongs to the day it
is needed.

## E2 · Drain rate vs worker count

Pre-loaded backlog, then one dispatcher started with W workers; time until nothing is pending or
leased. Receiver 20 ms, `max_concurrency` 8. Unordered backlogs are 40,000 deliveries, because a
10,000 one drains in ~2 s at the top end, too short to time reliably.

| workers | 16 lanes (del/s) | 64 lanes (del/s) | ordered, 256 keys (del/s) |
|---:|---:|---:|---:|
| 1 | 326 | 332 | 46 |
| 2 | 642 | 645 | 92 |
| 4 | 1,225 | 1,258 | 182 |
| 8 | 2,420 | 2,429 | 364 |
| 16 | 4,734 | 4,814 | 713 |
| 32 | 4,603 | 8,547 | 1,347 |
| 64 | 4,364 | **13,560** | 2,501 |

**Reading it.** A leased stream sends up to `max_concurrency` requests at once, so one worker is
worth ~330 deliveries/s here (8 in flight ÷ ~24 ms). Scaling is linear until it hits a limit that
is *meant* to be there:

- **16 lanes flattens at 16 workers.** Sixteen lanes × 8 in flight = 128 requests, ≈ 4,700/s at
  ~27 ms each. Extra workers find nothing to lease. That ceiling is the subscriber's own
  concurrency limit, which exists to protect the subscriber. To go faster, the subscription
  needs more lanes, not the dispatcher more workers.
- **64 lanes keeps climbing** to 13.5k/s at 64 workers (4.1× from 16 workers; 64 × 8 = 512
  in flight).
- **Ordered streams scale linearly**: one in-flight delivery per key, so ~39/s per worker, until
  workers outnumber keys.

## E3 · End-to-end latency, decomposed

Fanout 2, receivers 20 ms, 64 workers, 70 s runs with the first 10 s excluded. *Queue wait* is
from enqueue to the first attempt starting; *delivery* is the attempt itself.

| rate | ingest p99 | queue wait p50 / p99 | delivery p50 / p99 | e2e p50 / p95 / p99 / max |
|---:|---:|---:|---:|---:|
| 200 events/s (24,070 deliveries) | 8.2 ms | 16.8 / 32.7 | 21 / 22 | **38.0 / 50.7 / 54.1** / 81 |
| 20 events/s (2,408 deliveries) | 12.4 ms | 34.3 / 101.0 | 22 / 22 | 55.5 / 114.4 / 122.8 / 132 |

**Reading it.** Delivery is the receiver's 20 ms plus ~1 ms of overhead. Everything else is
queue wait, which is **polling**: there is no wake signal from ingest to the dispatcher (cut from
the plan to keep Redis off the critical path), so new work waits for the next poll. An idle
dispatcher backs off from 50 ms toward 500 ms, which is why the *quieter* run waits longer: at
20 events/s it is often mid-backoff when an event lands. A pub/sub nudge from ingest would cut
queue wait to near zero; adaptive polling is the deliberate, dependency-free trade.

Before the two fixes below, the same 200 events/s run measured queue wait p50 42.4 ms and e2e
p99 140.8 ms. Delivery time didn't change, so the improvement is all queue wait. I didn't run the
fixes separately, so I don't claim how much each one contributed.

## E4 · Does a failing endpoint hurt a healthy one?

Same load as E3, retry base 1 s. Baseline: both endpoints healthy. Neighbor: one endpoint fails
30% of requests with 503.

| endpoint | baseline e2e p50 / p99 | with failing neighbor e2e p50 / p99 | delivery p99 |
|---|---:|---:|---:|
| healthy | 37.0 / 53.8 ms | 36.6 / **53.4 ms** | 22 → 23 ms |
| flaky | 37.3 / 54.0 ms | 43.4 / 5,726 ms (max 64 s) | 22 → 23 ms |

The flaky endpoint averaged **1.429 attempts per delivered event**, matching the theoretical
1 / 0.7 = 1.4286 for independent 30% failures. Its tail is the retry backoff working as designed.
The healthy endpoint did not move. Retries are scheduled (`next_attempt_at` on the stream), never
slept on by a worker, so a failing endpoint costs the healthy one nothing.

## E5 · One endpoint down for 60 s

150 events/s for 150 s, fanout 2 (`steady` and `outage`). At t=30 s `outage` answers 503 to
everything; at t=90 s it recovers. Default breaker: 10 consecutive failures, 30 s cooldown,
doubling.

| phase | accepted | ingest p50 | ingest p99 |
|---|---:|---:|---:|
| before (0–30 s) | 100% | 2.18 ms | 8.58 ms |
| during outage (30–90 s) | 100% | 2.21 ms | 8.17 ms |
| after (90–150 s) | 100% | 2.17 ms | 7.96 ms |

- The breaker opened at t=30 s. The backlog for `outage` peaked at **14,516 deliveries**, parked
  (streams in `circuit_open`, no requests sent, no attempts consumed). None were lost and none
  dead-lettered.
- The circuit closed at t=127 s, 37 s after recovery: a probe at ~t=60 s failed, so the cooldown
  doubled, and the next probe succeeded. 99% of the backlog drained within **3 s** of the close.
- Every last delivery was done 0.13 s after load stopped: **45,006 of 45,006 delivered.**

Ingest never noticed. The outage was absorbed entirely as parked rows.

## E8 · Chaos: `kill -9` under load

Two dispatchers × 32 workers, 100 events/s × 120 s, fanout 2 (one endpoint fails 20% with 503),
retry base 1 s, 30 s leases. A dispatcher is `kill -9`'d every 20 s and restarted. Afterwards, the
receiver's own log of every 2xx it returned is compared with the database.

```
kills: 5 · quiescent 35.9 s after load stopped (orphaned leases expire, the reaper reclaims them)
events ingested 12,001 × 2 = 24,002 expected deliveries
terminal 24,002 (succeeded 24,002 + dead 0) · pending 0 · leases held 0
CONSERVATION   exact ✓    ingested × fanout == delivered + dead-lettered
PHANTOM ACKS   0 ✓        marked succeeded without the receiver ever returning 2xx
UNRECORDED     0          receiver returned 2xx, database does not say succeeded
DUPLICATES     3 of 24,002 (0.01%), the at-least-once cost of five crashes
```

Across earlier runs, duplicates ranged from 0 to 18 (0.07%). They are bounded by acking in
chunks of the lane's concurrency (see ARCHITECTURE.md).

## Autoscaling (Kubernetes)

Measured on the k3d cluster rather than compose, so it lives with the manifests: a 60k-event
burst against a slow ordered consumer drained in 152 s with KEDA scaling the dispatchers 2 → 10,
against 411 s pinned at 2 (peak backlog 5.5k vs 90k). Full table, method and findings:
[`deploy/k8s/README.md`](../deploy/k8s/README.md#autoscaling).

---

## What the benchmarks found

### 1. The connection pool was narrower than the concurrency (bug, fixed)

The first E2 runs on a 40,000-delivery backlog flattened at ~4,500/s regardless of lanes, and
got *worse* with more workers. Sampling sockets during a drain:

```
before fix:  0.3s pending=38585 time_wait=962    →  2.7s pending=7169 time_wait=16428  (stalls)
after fix:   0.3s pending=38185 time_wait=2      →  3.2s pending=0    time_wait=2
```

The delivery HTTP client kept **16** idle connections per host, while one dispatcher sends up to
512 requests at once to a busy host. Every request above 16 closed its connection afterwards, so
a sustained drain opened a new connection for almost every delivery. Within two seconds 16k
sockets sat in TIME_WAIT, the ephemeral port range (16,384 on macOS) was exhausted, and a
14k/s drain collapsed to ~2k/s. Short benchmarks hid it, because they finished before the ports
ran out.

The fix sets the per-host idle cap to the pool's maximum in flight (512).
`TestConcurrentDeliveriesToOneHostReuseConnections` counts connections opened over five rounds of
64 concurrent requests. It requires 64, and the old setting opened 256. The 64-lane drain at 64
workers went from 4,220/s to **13,560/s (3.2×)**.

### 2. A lost wakeup (bug, fixed)

E5 finished with **1 of 45,004** deliveries still pending in the first run, and **2 of 45,006** in
a later one, on the *healthy* endpoint. They had never been attempted and were long overdue, on a
stream in state `idle`. Nothing leases an idle stream, so those deliveries would have waited
forever.

The cause is in READ COMMITTED semantics. An enqueue onto a *leased* stream leaves it leased
(it must not take the stream away from its worker) and relies on the worker's release to notice
the new delivery. When the release blocked on the enqueue's row lock, Postgres re-checked the
stream row at its latest version once the enqueue committed. But the release's "anything
pending?" subquery kept its original snapshot, which could not see the delivery, so the release
concluded the stream was empty.

The fix: the release reads, from its own snapshot, the two columns that every enqueue path
changes (`next_seq` and `paused_delivery_id`) and compares them with the latest row. If either
moved, the stream goes back to `ready`. `TestEnqueueRacingTheFinalReleaseIsNotLost` holds an enqueue
open, waits until Postgres reports the release blocked on its lock, then commits. It reproduced
the bug on every run before the fix. After it, E5 delivers **45,006 of 45,006**.

The race needs an event to land within milliseconds of a stream's final release, which is why
it showed up as roughly one delivery in 45,000. That rate is too rare for a functional test to
catch by chance, but it would happen every day under production volume.

### 3. Polling sets the latency floor (by design)

See E3. Queue wait, not delivery, dominates end-to-end latency, and it is highest when the
system is *quiet*. This is the cost of keeping Redis off the critical path. The upgrade is a
best-effort wake hint on enqueue (losing one only costs a poll interval), and the Grafana
dashboard tracks the cost directly in its queue-wait panel.

### Also observed

- **Redis is not on the critical path.** The first full run happened, by accident, with Redis
  down. The dispatcher logged "live publish failed" (the dashboard feed) hundreds of times. Ingest
  and delivery numbers matched the later runs with Redis up, as designed.
- **Measurement hygiene.** The harness's first timeouts counted loop iterations instead of
  seconds (each check is a ~150 ms `docker exec`). One full run was lost to the laptop sleeping;
  runs now go under `caffeinate`.

## Observability

The same metrics are on the Grafana dashboard (`make up`, then http://localhost:3000). It is
generated from `deploy/compose/grafana/dashboards.py` (dashboards as code; `make dashboards`) and
covers ingest rate and latency, queue depth and oldest pending, queue wait vs delivery time, pool
occupancy (peak per window), lease and ack behavior, breaker transitions, and the DLQ. Alert
rules live in `deploy/compose/prometheus/rules/ithaca.yml`, and promtool validates them.
