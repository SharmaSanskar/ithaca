# ADR-0006: Autoscale the dispatchers with KEDA on worker demand, not CPU or queue depth

**Status:** accepted · **Date:** 2026-11-26 · **Phase:** 9

## Context

Dispatchers spend almost all their time waiting on subscribers' HTTP responses. A pod with all
32 workers busy against a 1 s endpoint uses a few percent of a core. CPU says it's idle while
the queue grows without bound. An HPA on CPU therefore scales on the wrong signal.

Scaling on queue depth is the obvious alternative, and it is wrong in three ways. Two are
visible on paper, and the third only showed up under load:

- **Depth that can't be parallelized.** 100k deliveries on one ordered key are delivered strictly
  one at a time by one worker. Depth says 50 pods; the number of pods that can do anything is 1.
- **Depth that isn't work yet.** 14k deliveries parked behind an open circuit breaker (bench E5),
  or waiting out a retry backoff, need no workers until they are due. Depth says to scale up
  into an outage.
- **Depth measures work waiting, not work.** This is the one found by measurement. The first
  version of the scaler counted due deliveries (with the first two problems handled). Under a
  sustained burst it scaled 2 → 9 pods and drained the queue, and then the queue was short, so
  the metric said "1 pod". After the stabilization window it scaled 9 → 3 *while the load
  continued*, the backlog regrew to 6.4k, and it scaled back out to 7. A healthy, caught-up
  system and an idle one look the same to a backlog metric.

## Decision

KEDA's PostgreSQL scaler computes worker demand directly from the stream table:

```sql
SELECT CEIL(count(*) / 32.0) FROM delivery_streams
WHERE state = 'leased' OR (state = 'ready' AND next_visible_at <= now())
```

One worker holds one stream, so the number of workers needed right now is the number of streams
being delivered (`leased`) plus the number waiting for a worker (`ready` and visible). Divided
by `ITHACA_DISPATCH_WORKERS` (32), that's pods. Utilization is measured in the currency that
limits throughput, worker slots, which CPU doesn't measure.

- **In-flight work counts.** A caught-up system under load keeps its pods, because its streams are
  leased.
- **Parked, paused and backing-off streams don't count.** They aren't `leased` and aren't visible.
- **One hot key is one stream**, so one worker and one pod, however deep it is.
- **It reads one row per stream, never the deliveries table**, so its cost doesn't grow with the
  backlog.

The query returns pods directly (`AverageValue`, target 1). Bounds are 2 to 20: the floor is for
availability (the PDB's `maxUnavailable: 1` needs two). Scale-up has no stabilization window
and allows up to +6 pods or +100% per 15 s. Scale-down waits for 60 s of agreement, then removes
at most half the pods per 30 s.

## Consequences

**Good.**

- It scales on the thing users feel, deliveries that need a worker, and does nothing during an
  endpoint outage, which is when a naive autoscaler does the most damage.
- No new moving parts. The scaler reads the table the dispatchers lease from, with no exporter
  and no Prometheus in the scaling path.
- Scaling is safe by construction. Dispatchers coordinate only through leases on Postgres rows,
  so adding or removing a pod mid-drain loses nothing. A pod removed by scale-down drains through
  the same shutdown path as a rollout, which `scripts/k8s-rollout-check.sh` shows losing nothing.

**Bad, and accepted.**

- The query couples to the worker count: the `32` must match `ITHACA_DISPATCH_WORKERS`, which the
  autoscaling component sets right next to it, with a comment to change both together.
- A `leased` stream counts as busy even in the moment after its work finished and before its
  release. Leases are released right after a drain, so this overstates demand by at most a
  poll interval's worth of streams.
- KEDA needs network access to Postgres, so it has its own NetworkPolicy, and it uses the app's
  credentials. A production setup would give it a read-only role.
- Scale-up latency is the HPA sync (15 s) plus pod start (a no-op migration, then the startup
  probe). Bursts shorter than that are absorbed by the existing pods and the queue, which is
  the queue's job.
