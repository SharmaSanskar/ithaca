package worker

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/SharmaSanskar/ithaca/internal/queue"
)

// ── Slots ────────────────────────────────────────────────────────────────────

func TestSlotsPartialAcquisition(t *testing.T) {
	s := NewSlots(5)
	if n := s.AcquireUpTo(context.Background(), 3); n != 3 {
		t.Fatalf("got %d, want 3", n)
	}
	if n := s.AcquireUpTo(context.Background(), 10); n != 2 {
		t.Fatalf("got %d, want the remaining 2", n)
	}
	if s.InFlight() != 5 {
		t.Fatalf("in flight = %d", s.InFlight())
	}
	s.Release(5)
	if s.InFlight() != 0 {
		t.Fatalf("in flight after release = %d", s.InFlight())
	}
}

func TestSlotsBlockUntilReleasedOrCanceled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := NewSlots(1)
		s.AcquireUpTo(context.Background(), 1)

		got := make(chan int, 1)
		go func() { got <- s.AcquireUpTo(context.Background(), 4) }()
		synctest.Wait()
		select {
		case <-got:
			t.Fatal("acquired from a full semaphore")
		default:
		}
		s.Release(1)
		if n := <-got; n != 1 {
			t.Fatalf("got %d after one release, want 1", n)
		}

		ctx, cancel := context.WithCancel(context.Background())
		go func() { got <- s.AcquireUpTo(ctx, 1) }()
		synctest.Wait()
		cancel()
		if n := <-got; n != 0 {
			t.Fatalf("canceled acquire returned %d, want 0", n)
		}
	})
}

func TestSlotsOverReleasePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("releasing a slot that was never acquired must panic, not inflate capacity")
		}
	}()
	NewSlots(1).Release(1)
}

// ── the pool ─────────────────────────────────────────────────────────────────

// runPool starts p and returns a func that shuts it down gracefully.
func runPool(t *testing.T, p *Pool) func() {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- p.Run(context.Background()) }()
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := p.Shutdown(ctx); err != nil {
			t.Errorf("shutdown: %v", err)
		}
		if err := <-done; err != nil {
			t.Errorf("run: %v", err)
		}
	}
}

// The backpressure test: the pool never leases more than it can process, and
// every slot comes back. shortOne makes the queue hand out fewer streams than
// asked for on every call, which is exactly the case where a missing
// Release(n - len(leases)) leaks capacity until the pool stalls.
func TestPoolBoundsWorkAndNeverLeaksSlots(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := newFakeQueue()
		q.shortOne = true
		for range 100 {
			q.add(false, 1)
		}
		e := &fakeExec{fn: sleepThen(time.Second)}
		p, slots, _ := newTestPool(testConfig(4), q, e)
		stop := runPool(t, p)

		for q.succeeded() < 100 {
			time.Sleep(100 * time.Millisecond)
		}
		stop()

		if e.maxRun > 4 {
			t.Errorf("%d deliveries ran at once with 4 workers", e.maxRun)
		}
		if q.maxInFlight > 4 {
			t.Errorf("%d streams were leased at once with 4 workers: dequeued beyond capacity", q.maxInFlight)
		}
		for i, m := range q.leaseMaxes {
			if m > 3 {
				t.Fatalf("lease call %d asked for %d streams, above MaxStreamsBatch", i, m)
			}
		}
		if n := slots.InFlight(); n != 0 {
			t.Errorf("%d slots still taken after shutdown: capacity leaked", n)
		}
		// 100 one-second deliveries over 4 workers should take ~25s, not
		// 100s: the pool must actually run in parallel.
	})
}

func TestPoolIssuesNoQueriesWhileSaturated(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := newFakeQueue()
		for range 2 {
			q.add(false, 1)
		}
		e := &fakeExec{fn: sleepThen(time.Hour)}
		p, _, _ := newTestPool(testConfig(2), q, e)
		stop := runPool(t, p)

		time.Sleep(time.Second)
		synctest.Wait()
		q.mu.Lock()
		before := len(q.leaseMaxes)
		q.mu.Unlock()

		time.Sleep(10 * time.Minute) // both workers busy the whole time
		synctest.Wait()
		q.mu.Lock()
		after := len(q.leaseMaxes)
		q.mu.Unlock()
		if after != before {
			t.Errorf("dispatcher polled the queue %d times while every slot was busy", after-before)
		}
		stop()
	})
}

func TestHeartbeatRenewsThenAbandonsOnLostLease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := newFakeQueue()
		s := q.add(false, 1)
		q.renewFunc = func(n int) error {
			if n <= 2 {
				return nil
			}
			return queue.ErrLeaseLost
		}
		canceled := make(chan struct{})
		e := &fakeExec{fn: func(ctx context.Context, _ *queue.Lease, _ queue.Message) (queue.Result, bool) {
			<-ctx.Done() // a delivery that takes "forever"
			close(canceled)
			return queue.Result{}, false
		}}
		p, _, m := newTestPool(testConfig(1), q, e)
		stop := runPool(t, p)

		// TTL 30s → renew every 10s. Renewals 1 and 2 succeed; 3 (at 30s)
		// reports the lease lost, which must cancel the delivery.
		time.Sleep(35 * time.Second)
		synctest.Wait()
		select {
		case <-canceled:
		default:
			t.Fatal("losing the lease did not cancel the in-flight delivery")
		}
		if got := testutil.ToFloat64(m.LeasesLost); got != 1 {
			t.Errorf("leases_lost_total = %v, want 1", got)
		}
		q.mu.Lock()
		acks, rels := len(q.acks[s.lease.StreamID]), len(q.releases[s.lease.StreamID])
		q.mu.Unlock()
		if acks != 0 || rels != 0 {
			t.Errorf("after losing the lease: %d acks, %d releases; want neither (someone else may own the stream now)", acks, rels)
		}
		stop()
	})
}

func TestShutdownDrainsInFlightWork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := newFakeQueue()
		s := q.add(false, 1)
		p, _, _ := newTestPool(testConfig(1), q, &fakeExec{fn: sleepThen(10 * time.Second)})
		done := make(chan error, 1)
		go func() { done <- p.Run(context.Background()) }()

		time.Sleep(time.Second)
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		start := time.Now()
		if err := p.Shutdown(ctx); err != nil {
			t.Fatal(err)
		}
		<-done
		if took := time.Since(start); took < 8*time.Second || took > 10*time.Second {
			t.Errorf("shutdown took %s; it should wait out the ~9s of remaining work", took)
		}
		if q.succeeded() != 1 {
			t.Error("the in-flight delivery was not acked during the drain")
		}
		if len(q.releases[s.lease.StreamID]) != 1 {
			t.Error("stream not released after draining")
		}
	})
}

func TestShutdownHardCutsWorkThatOutlivesTheBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := newFakeQueue()
		s := q.add(false, 1)
		p, slots, _ := newTestPool(testConfig(1), q, &fakeExec{fn: sleepThen(time.Hour)})
		done := make(chan error, 1)
		go func() { done <- p.Run(context.Background()) }()

		time.Sleep(time.Second)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		start := time.Now()
		if err := p.Shutdown(ctx); err != nil {
			t.Fatal(err)
		}
		<-done
		// Budget 10s, cleanup margin 3s → the hard cut lands at 7s.
		if took := time.Since(start); took != 7*time.Second {
			t.Errorf("hard cut after %s, want 7s (budget minus cleanup margin)", took)
		}
		if n := len(q.acks[s.lease.StreamID]); n != 0 {
			t.Errorf("an abandoned delivery was acked (%d results)", n)
		}
		if len(q.releases[s.lease.StreamID]) != 1 {
			t.Error("abandoned stream was not released for immediate redelivery")
		}
		if slots.InFlight() != 0 {
			t.Error("slots leaked through the hard cut")
		}
	})
}

func TestOrderedStreamStopsAtFirstNonSuccess(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := newFakeQueue()
		s := q.add(true, 3)
		e := &fakeExec{fn: func(_ context.Context, _ *queue.Lease, m queue.Message) (queue.Result, bool) {
			d := queue.Succeeded
			if m.Seq == 2 {
				d = queue.Retry
			}
			return queue.Result{DeliveryID: m.DeliveryID, Disposition: d, Delay: time.Minute}, true
		}}
		p, _, _ := newTestPool(testConfig(1), q, e)
		stop := runPool(t, p)
		time.Sleep(time.Second)
		synctest.Wait()
		stop()

		acks := q.acks[s.lease.StreamID]
		if len(acks) != 2 || acks[0].Disposition != queue.Succeeded || acks[1].Disposition != queue.Retry {
			t.Fatalf("acks = %+v; want seq1 succeeded, seq2 retry, seq3 untouched", acks)
		}
		if e.execCount != 2 {
			t.Errorf("executed %d messages; seq 3 must wait behind the failed seq 2", e.execCount)
		}
	})
}

func TestUnorderedLaneIsNotBlockedByOneFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := newFakeQueue()
		s := q.add(false, 3)
		e := &fakeExec{fn: func(_ context.Context, _ *queue.Lease, m queue.Message) (queue.Result, bool) {
			d := queue.Succeeded
			if m.Seq == 1 {
				d = queue.Retry
			}
			return queue.Result{DeliveryID: m.DeliveryID, Disposition: d, Delay: time.Minute}, true
		}}
		p, _, _ := newTestPool(testConfig(1), q, e)
		stop := runPool(t, p)
		time.Sleep(time.Second)
		synctest.Wait()
		stop()
		if got := len(q.acks[s.lease.StreamID]); got < 3 {
			t.Errorf("acked %d results; in unordered mode a failure must not block the rest", got)
		}
	})
}

func TestPanicInDeliveryReleasesStreamAndPoolSurvives(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := newFakeQueue()
		bad := q.add(false, 1)
		q.add(false, 1)
		first := true
		e := &fakeExec{fn: func(_ context.Context, _ *queue.Lease, m queue.Message) (queue.Result, bool) {
			if first {
				first = false
				panic("boom")
			}
			return queue.Result{DeliveryID: m.DeliveryID, Disposition: queue.Succeeded}, true
		}}
		p, slots, m := newTestPool(testConfig(1), q, e)
		stop := runPool(t, p)
		time.Sleep(time.Second)
		synctest.Wait()
		stop()

		if got := testutil.ToFloat64(m.Panics); got != 1 {
			t.Errorf("panics = %v, want 1", got)
		}
		if len(q.releases[bad.lease.StreamID]) != 1 {
			t.Error("the panicking stream was not released")
		}
		if q.succeeded() != 1 {
			t.Error("the pool stopped working after a panic")
		}
		if slots.InFlight() != 0 {
			t.Error("a panic leaked a slot")
		}
	})
}

func TestDisabledEndpointIsDeferredNotDelivered(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := newFakeQueue()
		s := q.add(false, 1)
		s.lease.Endpoint.State = "disabled"
		e := &fakeExec{fn: sleepThen(0)}
		p, _, _ := newTestPool(testConfig(1), q, e)
		stop := runPool(t, p)
		time.Sleep(time.Second)
		synctest.Wait()
		stop()
		if e.execCount != 0 {
			t.Error("delivered to a disabled endpoint")
		}
		if rel := q.releases[s.lease.StreamID]; len(rel) != 1 || rel[0] != time.Minute {
			t.Errorf("releases = %v, want one deferral of DisabledRecheck", rel)
		}
	})
}

func TestCancelingRunIsAnImmediateHardStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := newFakeQueue()
		q.add(false, 1)
		p, _, _ := newTestPool(testConfig(1), q, &fakeExec{fn: sleepThen(time.Hour)})
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- p.Run(ctx) }()
		time.Sleep(time.Second)
		start := time.Now()
		cancel()
		if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		if took := time.Since(start); took > 0 {
			t.Errorf("Run took %s to stop after cancel; want immediate", took)
		}
	})
}
