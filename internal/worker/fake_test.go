package worker

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/SharmaSanskar/ithaca/internal/platform/clock"
	"github.com/SharmaSanskar/ithaca/internal/queue"
)

// fakeQueue is an in-memory queue.Queue. No real I/O, so goroutines blocked
// on it count as "durably blocked" inside a synctest bubble.
type fakeQueue struct {
	mu sync.Mutex

	ready    []*fakeStream
	byID     map[int64]*fakeStream
	shortOne bool // return at most one lease per call, to exercise Release(n - len)

	leaseMaxes  []int
	maxInFlight int // most streams leased at once
	inFlight    int

	acks      map[int64][]queue.Result
	releases  map[int64][]time.Duration
	renewals  int
	renewFunc func(n int) error // nil = always succeed
}

type fakeStream struct {
	lease queue.Lease
	msgs  []queue.Message
}

func newFakeQueue() *fakeQueue {
	return &fakeQueue{byID: map[int64]*fakeStream{}, acks: map[int64][]queue.Result{}, releases: map[int64][]time.Duration{}}
}

func (f *fakeQueue) add(ordered bool, nmsgs int) *fakeStream {
	f.mu.Lock()
	defer f.mu.Unlock()
	id := int64(len(f.byID) + 1)
	s := &fakeStream{lease: queue.Lease{
		StreamID: id, Token: uuid.New(), Ordered: ordered, TTL: 30 * time.Second,
		Endpoint: queue.Endpoint{State: "enabled", Circuit: "closed", MaxConcurrency: 4, Timeout: 10 * time.Second},
	}}
	for i := range nmsgs {
		s.msgs = append(s.msgs, queue.Message{DeliveryID: uuid.New(), Seq: int64(i + 1), Due: true})
	}
	f.byID[id] = s
	f.ready = append(f.ready, s)
	return s
}

func (f *fakeQueue) Lease(_ context.Context, req queue.LeaseRequest) ([]*queue.Lease, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.leaseMaxes = append(f.leaseMaxes, req.Max)
	n := min(req.Max, len(f.ready))
	if f.shortOne {
		n = min(n, 1)
	}
	out := make([]*queue.Lease, 0, n)
	for _, s := range f.ready[:n] {
		l := s.lease
		l.AcquiredAt = time.Now()
		out = append(out, &l)
	}
	f.ready = f.ready[n:]
	f.inFlight += n
	f.maxInFlight = max(f.maxInFlight, f.inFlight)
	return out, nil
}

func (f *fakeQueue) Fetch(_ context.Context, l *queue.Lease, limit int) ([]queue.Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	msgs := f.byID[l.StreamID].msgs
	return append([]queue.Message(nil), msgs[:min(limit, len(msgs))]...), nil
}

func (f *fakeQueue) Renew(context.Context, *queue.Lease) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.renewals++
	if f.renewFunc != nil {
		return f.renewFunc(f.renewals)
	}
	return nil
}

func (f *fakeQueue) Ack(_ context.Context, l *queue.Lease, rs []queue.Result) (queue.AckOutcome, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.acks[l.StreamID] = append(f.acks[l.StreamID], rs...)
	done := map[uuid.UUID]bool{}
	for _, r := range rs {
		if r.Disposition == queue.Succeeded || r.Disposition == queue.Dead {
			done[r.DeliveryID] = true
		}
	}
	s := f.byID[l.StreamID]
	kept := s.msgs[:0]
	for _, m := range s.msgs {
		if !done[m.DeliveryID] {
			kept = append(kept, m)
		}
	}
	s.msgs = kept
	return queue.AckOutcome{}, nil
}

func (f *fakeQueue) Release(_ context.Context, l *queue.Lease, notBefore time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.releases[l.StreamID] = append(f.releases[l.StreamID], notBefore)
	f.inFlight--
	return nil
}

func (f *fakeQueue) Reap(context.Context, int) (int, error)     { return 0, nil }
func (f *fakeQueue) Stats(context.Context) (queue.Stats, error) { return queue.Stats{}, nil }

func (f *fakeQueue) succeeded() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, rs := range f.acks {
		for _, r := range rs {
			if r.Disposition == queue.Succeeded {
				n++
			}
		}
	}
	return n
}

// fakeExec runs fn for every message.
type fakeExec struct {
	fn func(ctx context.Context, l *queue.Lease, m queue.Message) (queue.Result, bool)

	mu        sync.Mutex
	running   int
	maxRun    int
	execCount int
}

func (e *fakeExec) Execute(ctx context.Context, l *queue.Lease, m queue.Message) (queue.Result, bool) {
	e.mu.Lock()
	e.running++
	e.execCount++
	e.maxRun = max(e.maxRun, e.running)
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		e.running--
		e.mu.Unlock()
	}()
	return e.fn(ctx, l, m)
}

// sleepThen succeeds after d, or abandons if ctx ends first.
func sleepThen(d time.Duration) func(context.Context, *queue.Lease, queue.Message) (queue.Result, bool) {
	return func(ctx context.Context, _ *queue.Lease, m queue.Message) (queue.Result, bool) {
		select {
		case <-time.After(d):
			return queue.Result{DeliveryID: m.DeliveryID, Disposition: queue.Succeeded, Attempts: 1}, true
		case <-ctx.Done():
			return queue.Result{}, false
		}
	}
}

func testConfig(workers int) Config {
	return Config{
		WorkerID: "test", Workers: workers, MaxStreamsBatch: 3, DrainBatch: 8, MaxDrainRounds: 4,
		PollFloor: 10 * time.Millisecond, PollCeiling: 100 * time.Millisecond, DisabledRecheck: time.Minute,
	}
}

func newTestPool(cfg Config, q queue.Queue, e Executor) (*Pool, *Slots, *Metrics) {
	slots := NewSlots(cfg.Workers)
	m := NewMetrics(prometheus.NewRegistry(), slots)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return New(cfg, q, e, slots, log, m, clock.Real{}), slots, m
}
