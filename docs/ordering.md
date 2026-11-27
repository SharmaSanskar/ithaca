# Ordering

Most webhooks do not need ordering, and paying for it everywhere would make everything slow.
Ithaca makes it a per-subscription choice and guarantees it per key:

> On an **ordered** subscription, events with the same `ordering_key` are delivered in the order
> they were accepted. Event N+1 is not attempted until event N has been delivered or replayed.

Everything else is **unordered**: spread across lanes and delivered concurrently.

## One mechanism, two modes

Every delivery belongs to a **stream**, and a stream is the unit of everything: leasing, retries,
the circuit breaker. Which stream an event joins is decided by one function
(`ingest.StreamKey`):

| subscription | event has `ordering_key` | stream | effect |
|---|---|---|---|
| ordered | yes | `key:<ordering_key>` | all events for `order_123` go through one stream, strictly in sequence |
| ordered | no | `lane:<hash(event id) % lanes>` | nothing to order it against, so it is spread like unordered |
| unordered | either | `lane:<hash(event id) % lanes>` | spread across `lane_count` lanes (default 16) |

An ordered subscription that receives events *without* a key does not funnel them into one
global stream. That would silently cap it at one delivery per round trip.

## How order is kept

**A gapless sequence, assigned at commit order.** Each stream has a `next_seq`, and an enqueue
takes its number with `INSERT … ON CONFLICT DO UPDATE SET next_seq = next_seq + 1 RETURNING`.
The row lock that statement takes is held until the transaction commits, so concurrent enqueues
to one stream are serialized and sequence order equals commit order. A `BIGSERIAL` would not do
this: it hands out numbers at statement time, so a slow transaction can commit `seq 5` after
`seq 6` is already visible, and a reader would deliver 6 first.

That lock is also the cost. The ingest benchmark shows enqueues waiting on it at ~3,000
events/s (bench E1). It is per stream, not global, so more keys or more lanes spread it.

**One worker per stream.** A stream is leased by exactly one worker at a time (`FOR UPDATE SKIP
LOCKED` on the stream row, plus a fencing token checked on every write). Within an ordered
stream, the worker delivers strictly one at a time, head first.

**The head blocks.** If the head delivery fails, it is scheduled for retry and *nothing behind it
moves*. The worker stops at the first message that is not due yet, because delivering seq N+1
while N waits is exactly the reordering this mode prevents.

**A dead head pauses the stream.** When the head exhausts its retries, it goes to the
dead-letter queue and the stream pauses. Skipping it would deliver "shipped" before "created".
Replaying the dead letter puts it back at its **original** sequence number, which is still free
and lower than everything queued behind it, and resumes the stream. Order is restored exactly.

## What ordering does not promise

- **Across keys:** none. `order_1` and `order_2` are independent streams, delivered concurrently.
- **Against duplicates:** delivery is at-least-once (see [at-least-once.md](at-least-once.md)).
  A crash after the subscriber accepted 1, 2, 3 but before the acks for 2 and 3 were written
  replays from the first unacked: the subscriber sees `1, 2, 3, 2, 3`. The *first* delivery of
  each event is always in order, and every repeat carries the same `Ithaca-Id`, so deduping
  restores `1, 2, 3`. Ordered streams ack every 8 deliveries, which bounds the repeat.
- **Throughput:** an ordered key delivers one event per round trip, ~39/s per key against a
  20 ms subscriber (bench E2). Throughput comes from having many keys, not from one hot key.

## Tested

`TestOrderedStreamNeverReorders` runs 32 workers against a receiver failing 30% of requests and
checks that every key's deliveries arrive in sequence, under the race detector.
`TestDeadOrderedHeadPausesStreamAndReplayRestoresOrder` kills a head, checks that nothing behind
it is delivered, then replays it and checks the original order. `TestRetryOnOrderedStreamDoesNotPause`
checks that a retry only blocks the head and does not pause the stream.
