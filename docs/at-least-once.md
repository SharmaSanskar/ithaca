# At-least-once delivery, and where duplicates come from

Ithaca guarantees that every event reaches every matching subscription **at least once**: it is
delivered (2xx), or it ends up in the dead-letter queue with the reason, where it can be
replayed. It never guarantees *exactly* once, because no webhook system can. The subscriber's
2xx and Ithaca's record of it are two separate facts on two machines, and something can always
fail between them.

So the contract is: **duplicates are possible, and every one carries the same `Ithaca-Id`.**
A subscriber that remembers the `Ithaca-Id`s it has processed (a unique index is enough) gets
effectively-once processing. That covers every duplicate below, including the ones from replay.

| header | stays the same across… | use it for |
|---|---|---|
| `Ithaca-Id` | every retry, every crash redelivery, every DLQ replay | **dedupe** |
| `Ithaca-Delivery-Id` | retries of one delivery; a replay gets a new one | debugging, support tickets |
| `Ithaca-Attempt` | nothing (1, 2, 3, …) | logging |
| `Ithaca-Signature` | nothing (fresh timestamp each attempt) | verifying the request is from Ithaca |

## The duplicate sources

Each of these is a real path in the code, and most have a test that provokes it on purpose.

**1. The subscriber returned 2xx, then the dispatcher died before acking.**
Deliveries are fetched in batches of 64 but acked in chunks of the endpoint's concurrency (8),
in one transaction per chunk. A crash loses the acks for at most the chunk in flight. Those
deliveries are still `pending` in Postgres, so when the dead dispatcher's lease expires (30 s
minimum), the reaper hands the stream to another worker, which delivers them again. Acking once
per 64-batch measured 17% duplicates under `kill -9`; chunking by concurrency measured ~5% in
`TestKilledDispatcherLosesNothing`, and 0.01–0.07% in bench E8 (five kills in two minutes).

**2. A worker lost its lease but was still sending.**
A stalled process (GC pause, a frozen VM, a partition) can outlive its lease. The reaper gives
the stream to someone else, and both deliver the same message. The stall cannot cause *wrong
state*: every ack, renewal and release is fenced by the lease's `lock_token`, so the zombie's
writes are rejected (`TestZombieWorkerIsFencedOut`). But its HTTP request already went out.
Heartbeats renew the lease while a worker drains (`TestLeaseRenewsDuringALongDrain`), so this
needs a real stall, not just slow work.

**3. The subscriber processed the request but answered too late.**
Each endpoint has a timeout (default 15 s). A subscriber that does the work and responds after
it gets a timeout recorded and a retry. This is the most common duplicate in practice, and the
reason handlers should do slow work asynchronously and return 2xx fast.

**4. The connection broke after the subscriber had the request.**
A reset or EOF while reading the response is classified as transient and retried. Ithaca cannot
tell whether the request was processed before the connection died.

**5. The ack transaction failed.**
If the database rejects the ack (failover, a dropped connection), the attempt is not recorded,
and the delivery is retried later, even though the subscriber returned 2xx.

**6. A dead letter is replayed.**
A delivery can be dead-lettered after timeouts the subscriber actually processed (see 3). Replay
creates a new delivery (new `Ithaca-Delivery-Id`) for the **same** event (same `Ithaca-Id`), so
a subscriber that dedupes is safe to replay into. Replay itself is idempotent: a double-clicked
"Replay" claims the dead letter once (`replayed_at`, set in the same transaction), so it never
creates two deliveries (`TestReplayIsIdempotentUnderConcurrentDoubleClick`).

## Duplicates on the way *in*

At-least-once also applies to the publisher. If a client times out on `POST /v1/events` and
retries, it may create the event twice, as two *different* events with different `Ithaca-Id`s,
which no subscriber can dedupe. The fix is on the publisher's side: send an `Idempotency-Key`.

- Same key and same body within 24 h → the original response is replayed and no new event is
  created.
- Same key with a different body → `422`. That is a client bug, and it must not be silently
  accepted.
- Two requests with the same key at the same moment: the second blocks on the first's
  uncommitted claim, then receives the first's response. One event, two identical answers.

## What is never lost

The other half of the guarantee, checked by invariant tests and by bench E8 under `kill -9`:

- **Conservation:** after quiescence, events × matching subscriptions = delivered + dead-lettered,
  with nothing pending. A delivery row is only ever *moved* (to history, in the same statement as
  its ack). The only deletion is the opt-in retention janitor, which the public demo uses to
  stay small.
- **No phantom acks:** nothing is marked delivered unless the subscriber returned 2xx. E8 checks
  this against the receiver's own log of every 2xx it sent.
- **No stranded work:** a stream with pending deliveries is always leasable eventually. The one
  bug found here (a lost wakeup under READ COMMITTED, see ARCHITECTURE.md) is covered by
  `TestEnqueueRacingTheFinalReleaseIsNotLost`.
