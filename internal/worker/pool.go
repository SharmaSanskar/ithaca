// Package worker is the dispatcher's bounded worker pool.
//
//	poll ticker ──▶┌ dispatcher (1 goroutine) ───────────────────┐
//	               │ 1. acquire N slots      (BLOCKS if saturated) │
//	               │ 2. lease EXACTLY N streams                    │
//	               │ 3. hand off over an UNBUFFERED channel        │
//	               │ 4. Release(N - len(leases))                   │
//	               └──────────────┬───────────────────────────────┘
//	               ┌──────────────▼───────────────────────────────┐
//	               │ W workers: drain stream → deliver → ack,      │
//	               │ heartbeat renews the lease meanwhile          │
//	               └───────────────────────────────────────────────┘
//
// The one rule: ACQUIRE CAPACITY BEFORE DEQUEUING, and dequeue exactly as much
// as you acquired. Dequeue first and you have turned a queue-depth problem
// into a memory problem, and taken leases nobody can service. Those expire
// and get reaped, which is a livelock where the system is 100% busy reclaiming
// its own abandoned work.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/SharmaSanskar/ithaca/internal/platform/clock"
	"github.com/SharmaSanskar/ithaca/internal/queue"
)

// Executor performs one delivery attempt and decides what happens next. It
// returns ok=false when the attempt was abandoned (shutdown, lost lease): no
// verdict on the endpoint, so nothing to ack.
type Executor interface {
	Execute(ctx context.Context, l *queue.Lease, m queue.Message) (r queue.Result, ok bool)
}

// Observer is told about every ack AFTER it commits: what was delivered,
// retried or dead-lettered, and any endpoint side effects. It must not block.
// Observing after the commit (never before) keeps the at-least-once window
// minimal: nothing — not even a dashboard update — sits between an HTTP 200
// and the ack.
type Observer interface {
	Acked(l *queue.Lease, results []queue.Result, o queue.AckOutcome)
}

// Config tunes the pool.
type Config struct {
	WorkerID        string
	Workers         int
	MaxStreamsBatch int
	DrainBatch      int
	MaxDrainRounds  int
	MinLease        time.Duration
	LeasePadding    time.Duration
	PollFloor       time.Duration
	PollCeiling     time.Duration
	// DisabledRecheck is how long a stream for a disabled endpoint is pushed
	// out before being looked at again.
	DisabledRecheck time.Duration
}

// cleanupTimeout bounds the ack and release calls that must run after
// cancellation (via context.WithoutCancel), so a dead database cannot wedge
// shutdown.
const cleanupTimeout = 5 * time.Second

// hardCutMargin is reserved at the end of the shutdown budget for cleanup
// after in-flight requests are canceled.
const hardCutMargin = 3 * time.Second

// Pool is the bounded worker pool.
type Pool struct {
	cfg   Config
	q     queue.Queue
	exec  Executor
	slots *Slots
	log   *slog.Logger
	m     *Metrics
	clock clock.Clock
	obs   Observer

	// leases is UNBUFFERED, on purpose. A buffer is a second, invisible queue:
	// the semaphore would say N in flight while more sit in the buffer, and
	// shutdown would have to drain work nobody is tracking.
	leases chan *queue.Lease

	stopAccepting chan struct{} // closed by Shutdown: phase 1
	stopOnce      sync.Once
	hardCutCh     chan struct{} // closed for the hard cut: phase 3
	hardCutOnce   sync.Once
	done          chan struct{} // closed when Run has fully returned
}

// New builds a pool. slots may be shared with NewMetrics for the gauges.
func New(cfg Config, q queue.Queue, exec Executor, slots *Slots, log *slog.Logger, m *Metrics, clk clock.Clock) *Pool {
	return &Pool{
		cfg: cfg, q: q, exec: exec, slots: slots, log: log, m: m, clock: clk,
		leases:        make(chan *queue.Lease),
		stopAccepting: make(chan struct{}),
		hardCutCh:     make(chan struct{}),
		done:          make(chan struct{}),
	}
}

// SetObserver installs a post-ack observer (the live dashboard feed).
func (p *Pool) SetObserver(o Observer) { p.obs = o }

// Run starts the dispatcher and workers and blocks until every worker has
// exited. Canceling ctx is an immediate hard stop (in-flight requests are
// canceled); a graceful stop is Shutdown.
func (p *Pool) Run(ctx context.Context) error {
	defer close(p.done)

	// workCtx carries ctx's values but has its own lifetime: it ends only at
	// the hard cut, so a graceful Shutdown lets in-flight work finish.
	workCtx, cancelWork := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelWork()
	go func() {
		select {
		case <-p.hardCutCh:
			cancelWork()
		case <-workCtx.Done():
		}
	}()

	// A canceled ctx means "stop now": skip straight to the hard cut.
	stopWatch := context.AfterFunc(ctx, func() { p.hardCut(); p.stop() })
	defer stopWatch()

	var wg sync.WaitGroup
	for range p.cfg.Workers {
		wg.Go(func() {
			for l := range p.leases {
				p.handle(workCtx, l)
				// Unconditional, after handle returns however it returns
				// (handle recovers its own panics). A skipped Release here
				// leaks capacity permanently.
				p.slots.Release(1)
			}
		})
	}

	p.dispatch(workCtx)
	close(p.leases) // the dispatcher was the only sender
	wg.Wait()
	return nil
}

// Shutdown stops the pool gracefully, in three phases:
//
//  1. stop accepting: the dispatcher leases nothing more.
//  2. drain: workers finish their current stream round, ack and release.
//  3. hard cut: if the drain outlives the budget, in-flight HTTP requests are
//     canceled. Completed attempts are still acked (acks run on a
//     WithoutCancel context); abandoned ones are released for immediate
//     redelivery rather than waiting for the lease to expire.
//
// The budget is ctx's deadline minus a small margin for that cleanup.
// Kubernetes' terminationGracePeriodSeconds must exceed the whole thing, or
// the kubelet SIGKILLs the process mid-drain.
func (p *Pool) Shutdown(ctx context.Context) error {
	p.stop()

	var hardCutAt <-chan time.Time
	if dl, ok := ctx.Deadline(); ok {
		t := time.NewTimer(max(time.Until(dl)-hardCutMargin, 0))
		defer t.Stop()
		hardCutAt = t.C
	}

	select {
	case <-p.done:
		return nil
	case <-hardCutAt:
		p.log.Warn("drain budget exhausted; canceling in-flight deliveries")
		p.hardCut()
	case <-ctx.Done():
		p.hardCut()
	}

	select {
	case <-p.done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("worker pool did not stop: %w", ctx.Err())
	}
}

func (p *Pool) stop()    { p.stopOnce.Do(func() { close(p.stopAccepting) }) }
func (p *Pool) hardCut() { p.hardCutOnce.Do(func() { close(p.hardCutCh) }) }

// dispatch is the single goroutine that leases work.
func (p *Pool) dispatch(workCtx context.Context) {
	ctx, cancel := context.WithCancel(workCtx)
	defer cancel()
	go func() {
		select {
		case <-p.stopAccepting:
			cancel()
		case <-ctx.Done():
		}
	}()

	backoff := p.cfg.PollFloor
	for {
		// (1) BACKPRESSURE. Blocks while the pool is saturated, issuing no
		// queries at all.
		n := p.slots.AcquireUpTo(ctx, p.cfg.MaxStreamsBatch)
		if n == 0 {
			return
		}

		// (2) Lease exactly what we can service.
		leases, err := p.q.Lease(ctx, queue.LeaseRequest{
			WorkerID: p.cfg.WorkerID, Max: n, MinLease: p.cfg.MinLease, LeasePadding: p.cfg.LeasePadding,
		})
		if err != nil {
			p.slots.Release(n)
			if ctx.Err() != nil {
				return
			}
			p.m.DequeueErrors.Inc()
			p.log.Warn("lease failed", "error", err)
			backoff = min(backoff*2, p.cfg.PollCeiling)
			if !sleep(ctx, jitter(backoff)) {
				return
			}
			continue
		}

		// (3) Give back what we over-reserved. Forgetting this is THE bug in
		// this pattern: capacity leaks a few slots at a time and throughput
		// decays to zero over hours.
		p.slots.Release(n - len(leases))
		p.m.LeaseBatch.Observe(float64(len(leases)))

		if len(leases) == 0 {
			// Adaptive polling: back off toward the ceiling while idle, snap
			// to the floor as soon as there is work.
			backoff = min(backoff*2, p.cfg.PollCeiling)
			if !sleep(ctx, jitter(backoff)) {
				return
			}
			continue
		}
		backoff = p.cfg.PollFloor

		// (4) Hand off. Each worker releases its own slot when done.
		for i, l := range leases {
			select {
			case p.leases <- l:
			case <-ctx.Done():
				// Stopping with leases in hand: give them straight back so
				// another dispatcher picks them up now, not after the TTL.
				for _, rest := range leases[i:] {
					p.release(ctx, rest, 0)
					p.slots.Release(1)
				}
				return
			}
		}
	}
}

// handle drains one leased stream.
func (p *Pool) handle(ctx context.Context, l *queue.Lease) {
	lctx, cancel := context.WithCancel(ctx)
	var lost atomic.Bool
	hbDone := make(chan struct{})
	go p.heartbeat(lctx, l, &lost, cancel, hbDone)

	notBefore := time.Duration(0)
	defer func() {
		if r := recover(); r != nil {
			// A panic must not strand the stream. Fall through to release so
			// the next worker can take it immediately.
			p.m.Panics.Inc()
			p.log.Error("worker panic", "stream", l.StreamID, "panic", r, "stack", string(debug.Stack()))
		}
		cancel()
		<-hbDone // no renewals racing the release below
		if !lost.Load() {
			p.release(ctx, l, notBefore)
		}
	}()

	switch {
	case l.Endpoint.State != "enabled":
		notBefore = p.cfg.DisabledRecheck
		return
	case l.Endpoint.Circuit == "open", l.Endpoint.Circuit == "half_open" && !l.IsProbe:
		// The endpoint is presumed down. Release without delivering: the
		// release parks the stream (state circuit_open) until the breaker
		// closes. No HTTP request, no attempt consumed, slot freed at once.
		return
	}

	// Hold the stream for at most 60% of the lease across rounds, so one busy
	// stream cannot starve the rest and we always release well before expiry.
	maxHold := l.TTL * 6 / 10
	for round := range p.cfg.MaxDrainRounds {
		if round > 0 && p.clock.Now().Sub(l.AcquiredAt) > maxHold {
			return
		}
		msgs, err := p.q.Fetch(lctx, l, p.cfg.DrainBatch)
		if err != nil {
			if lctx.Err() == nil {
				p.log.Warn("fetch failed", "stream", l.StreamID, "error", err)
			}
			return
		}
		if len(msgs) == 0 {
			return
		}

		// Fetch big (one query per DrainBatch), ack small (one transaction
		// per chunk). The ack chunk IS the crash window: deliveries the
		// subscriber has accepted but we have not yet acked are redelivered
		// if this process dies. Acking once per 64-message drain batch
		// measured 17% duplicates in TestKilledDispatcherLosesNothing;
		// chunking by the lane's concurrency bounds it to one chunk per lane.
		chunk := ackChunk(l)
		if l.IsProbe {
			// Half-open: exactly ONE request decides the endpoint's fate.
			// Sending a batch would be the thundering herd the breaker exists
			// to prevent, aimed at an endpoint that may be barely alive.
			msgs, chunk = msgs[:1], 1
		}
		for start := 0; start < len(msgs); start += chunk {
			part := msgs[start:min(start+chunk, len(msgs))]
			results, stoppedEarly := p.deliverBatch(lctx, l, part)
			if lost.Load() {
				return // fenced out: anything we did will be redelivered
			}
			if len(results) > 0 && !p.ack(ctx, l, results, &lost) {
				return
			}
			if stoppedEarly || lctx.Err() != nil {
				return
			}
		}
		if l.IsProbe || len(msgs) < p.cfg.DrainBatch {
			return
		}
	}
}

// orderedAckChunk is how many sequential deliveries an ordered stream makes
// between acks. An ordered stream's throughput is bounded by endpoint latency
// anyway, so a small ack every few deliveries costs almost nothing.
const orderedAckChunk = 8

func ackChunk(l *queue.Lease) int {
	if l.Ordered {
		return orderedAckChunk
	}
	return max(1, l.Endpoint.MaxConcurrency)
}

// deliverBatch is where ordered and unordered diverge for the last time.
func (p *Pool) deliverBatch(ctx context.Context, l *queue.Lease, msgs []queue.Message) (out []queue.Result, stoppedEarly bool) {
	if l.Ordered {
		// Strictly sequential. Stop at the first message that is not due or
		// did not complete: everything behind it waits, in order.
		out = make([]queue.Result, 0, len(msgs))
		for _, m := range msgs {
			if !m.Due || ctx.Err() != nil {
				return out, true
			}
			r, ok := p.execute(ctx, l, m)
			if !ok {
				return out, true
			}
			out = append(out, r)
			if r.Disposition != queue.Succeeded {
				// A retry or deferral blocks the head; a dead head pauses the
				// whole stream (see PauseStream). Either way nothing behind
				// it may go first.
				return out, true
			}
		}
		return out, false
	}

	// Unordered: concurrent within the lane, capped by the endpoint's own
	// max_concurrency.
	var g errgroup.Group
	g.SetLimit(max(1, l.Endpoint.MaxConcurrency))
	all := make([]queue.Result, len(msgs))
	okv := make([]bool, len(msgs))
	for i, m := range msgs {
		g.Go(func() error {
			if ctx.Err() == nil {
				all[i], okv[i] = p.execute(ctx, l, m)
			}
			return nil
		})
	}
	_ = g.Wait()
	out = make([]queue.Result, 0, len(msgs))
	for i := range all {
		if okv[i] {
			out = append(out, all[i])
		}
	}
	return out, len(out) < len(msgs)
}

// execute runs one attempt and turns a panic into an abandoned attempt.
//
// The recover has to live HERE, on the goroutine that runs the attempt.
// Unordered deliveries run on errgroup goroutines, and recover() only catches
// panics on its own goroutine: the recover in handle() would never see them,
// and one bad delivery would crash the whole dispatcher.
func (p *Pool) execute(ctx context.Context, l *queue.Lease, m queue.Message) (r queue.Result, ok bool) {
	defer func() {
		if rec := recover(); rec != nil {
			p.m.Panics.Inc()
			p.log.Error("delivery panic", "stream", l.StreamID, "delivery", m.DeliveryID,
				"panic", rec, "stack", string(debug.Stack()))
			r, ok = queue.Result{}, false
		}
	}()
	return p.exec.Execute(ctx, l, m)
}

// ack persists results. It runs on a context detached from cancellation
// (context.WithoutCancel keeps ctx's values but ignores its cancellation):
// work that already happened at the endpoint must be recorded even during the
// hard cut, or it is redelivered for no reason.
func (p *Pool) ack(ctx context.Context, l *queue.Lease, results []queue.Result, lost *atomic.Bool) bool {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
	defer cancel()
	start := time.Now()
	outcome, err := p.q.Ack(ctx, l, results)
	p.m.AckDuration.Observe(time.Since(start).Seconds())
	switch {
	case err == nil:
		p.reportAckOutcome(l, outcome)
		if p.obs != nil {
			p.obs.Acked(l, results, outcome)
		}
		for _, r := range results {
			switch r.Disposition {
			case queue.Succeeded:
				p.m.Completed.WithLabelValues("succeeded").Inc()
			case queue.Dead:
				p.m.Completed.WithLabelValues("dead").Inc()
			}
		}
		return true
	case errors.Is(err, queue.ErrLeaseLost):
		lost.Store(true)
		p.m.LeasesLost.Inc()
		p.log.Warn("ack fenced out: lease lost", "stream", l.StreamID)
		return false
	default:
		// Database trouble. Do not retry blindly: release (or let the lease
		// expire) and everything unacked is redelivered.
		p.m.AckErrors.Inc()
		p.log.Error("ack failed", "stream", l.StreamID, "error", err)
		return false
	}
}

// release must also survive cancellation: a stream released during shutdown is
// redelivered by another dispatcher in milliseconds instead of after the TTL.
func (p *Pool) reportAckOutcome(l *queue.Lease, o queue.AckOutcome) {
	if o.Breaker != "" {
		p.m.BreakerTransitions.WithLabelValues(o.Breaker).Inc()
		p.log.Warn("circuit breaker "+o.Breaker, "endpoint", l.EndpointID, "stream", l.StreamID)
	}
	if o.AutoDisabled {
		p.m.AutoDisabled.Inc()
		p.log.Warn("endpoint auto-disabled: 410 Gone", "endpoint", l.EndpointID)
	}
	if o.Paused {
		p.m.StreamsPaused.Inc()
		p.log.Warn("ordered stream paused: its head was dead-lettered; replay it to resume",
			"endpoint", l.EndpointID, "stream", l.StreamID, "stream_key", l.StreamKey)
	}
}

func (p *Pool) release(ctx context.Context, l *queue.Lease, notBefore time.Duration) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
	defer cancel()
	if err := p.q.Release(ctx, l, notBefore); err != nil && !errors.Is(err, queue.ErrLeaseLost) {
		p.log.Warn("release failed; the reaper will reclaim the stream", "stream", l.StreamID, "error", err)
	}
}

// heartbeat renews the lease every TTL/3 until ctx ends. On a definitive loss
// it cancels the stream's context, which aborts any in-flight request.
func (p *Pool) heartbeat(ctx context.Context, l *queue.Lease, lost *atomic.Bool, cancel context.CancelFunc, done chan<- struct{}) {
	defer close(done)
	every := l.TTL / 3
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		rctx, rcancel := context.WithTimeout(ctx, every)
		err := p.q.Renew(rctx, l)
		rcancel()
		switch {
		case err == nil:
		case errors.Is(err, queue.ErrLeaseLost):
			lost.Store(true)
			p.m.LeasesLost.Inc()
			p.log.Warn("lease lost during processing; abandoning stream", "stream", l.StreamID)
			cancel()
			return
		case ctx.Err() == nil:
			// Transient. Try again next tick. If the database stays away, the
			// lease expires, and the fenced ack refuses our stale write.
			p.log.Warn("lease renewal failed", "stream", l.StreamID, "error", err)
		}
	}
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// jitter spreads idle polls over [d/2, d] so replicas do not poll in lockstep.
func jitter(d time.Duration) time.Duration {
	half := d / 2
	if half <= 0 {
		return d
	}
	return half + rand.N(half+1) //nolint:gosec // scheduling jitter, not security
}
