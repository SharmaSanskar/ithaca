# ADR-0003: Lease streams, not messages

**Status:** accepted · **Date:** 2026-07-31 · **Phase:** 2 (decided in 0)

## Context

Ithaca needs a durable queue that supports per-key FIFO delivery, arbitrary-horizon delayed retries
(up to six hours), and a concurrent worker pool.

The textbook Postgres-as-a-queue pattern is:

```sql
SELECT * FROM jobs
WHERE state = 'pending'
ORDER BY id
FOR UPDATE SKIP LOCKED
LIMIT 10;
```

It has two problems that are fatal for this workload.

**It cannot preserve order.** `SKIP LOCKED` is *defined* as "skip whatever another transaction
holds." Worker A locks seq 1; worker B skips it and takes seq 2; B's endpoint responds faster; B
delivers first. The `ORDER BY` makes the query look ordered while the outcome is not, and no
combination of hints fixes it. Serialisation has to happen at the lane, not in the query.

**Contention scales with the in-flight set.** `SKIP LOCKED` does not block, but it does work: each
worker walks the index and, for every candidate, fetches the heap page to evaluate visibility and
lock status. With W workers each taking B rows, the W-th worker discards roughly (W−1)×B tuples
before finding free ones — at 64 workers and a batch of 20, about 1,280 wasted buffer accesses per
dequeue, per worker, forever.

## Decision

Workers lease **streams** (lanes), not individual messages.

Every delivery belongs to exactly one stream, keyed `(subscription_id, stream_key)`:

- ordered subscription → `stream_key` is the customer's `ordering_key`
- unordered subscription → `stream_key` is `lane:<hash(event_id) mod lane_count>`, default 16 lanes

**A stream is leased by at most one worker at a time.** That single invariant is the whole
guarantee. Ordering falls out of it. Unordered delivery is the same code over a synthetic key, so
there is one dequeue path, one lease path and one ack path — not two, and therefore no mode-specific
correctness bugs.

Within a leased stream the only difference between the modes is the inner query: ordered mode reads
strictly from the head and stops at the first row that is not due; unordered mode skips rows that
are still backing off.

## Consequences

**Good.**

- Per-key FIFO with a genuinely concurrent pool, rather than "ordered as long as you run one worker."
- The contended set is the number of *runnable streams* — hundreds — instead of the number of
  pending messages, which can be millions. Each unit of contention yields up to 64 deliveries, so
  contention per delivered message drops by the batch factor.
- The dequeue index is partial (`WHERE state = 'ready'`), so it holds only runnable lanes and stays
  resident in `shared_buffers` permanently.
- Rate limiting and circuit breaking become *scheduling* rather than failure: a throttled endpoint
  re-dates its stream and frees the worker slot instantly, with no HTTP request, no consumed retry
  attempt and no wasted lease. This is the most elegant consequence and it is only available because
  the lease is held at the lane.
- It maps cleanly onto SQS FIFO, where `MessageGroupId` already provides per-group serialisation and
  head-of-line blocking — the same semantics, which is a satisfying convergence rather than a
  coincidence.

**Bad, and accepted.**

- `seq` cannot come from a sequence. Sequence values are handed out *outside* transaction ordering,
  so transaction A can take 5, B take 6, and B commit first — a reader between the commits sees 6
  without 5. `seq` is therefore assigned from `delivery_streams.next_seq` under the stream row's
  write lock, in the same transaction as the delivery insert.
- That makes the stream row hot. Two producers writing the same ordering key serialise on it — but
  they must, because "ordered" is a request for a serialisation point. For unordered subscriptions
  the contention divides by `lane_count`, which is why the default is 16 rather than 1.
- An ordered stream is a serial channel capped at `1 / p50_latency` — roughly 10 deliveries per
  second at 100ms, regardless of worker count. This is documented rather than hidden, and ordering
  is opt-in with a default of off.
- Head-of-line blocking is real. When the head of an ordered stream exhausts its retries the stream
  pauses rather than skipping, because a customer who asked for ordering did so precisely because
  message N+1 depends on message N.

## Alternatives considered

**Message-level `SKIP LOCKED` with an ordering column.** Rejected: it does not actually order, per
the mechanism above.

**One queue per ordering key.** Correct semantics, unbounded object count — customers with a key per
user would need millions of queues.

**SQS FIFO as the whole queue.** Rejected on a checkable limitation: FIFO queues do not support
per-message `DelaySeconds` at all, and the queue-level maximum is 900 seconds against a retry
schedule that runs to six hours. Postgres therefore stays the scheduler and SQS carries only
immediately-runnable work — the managed queue is a transport, not a scheduler.
