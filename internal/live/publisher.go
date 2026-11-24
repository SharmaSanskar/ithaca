package live

import (
	"context"
	"encoding/json"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/SharmaSanskar/ithaca/internal/platform/metrics"
)

// Bus carries encoded envelopes between processes. *RedisBus implements it.
type Bus interface {
	Publish(ctx context.Context, msg []byte) error
}

// Window sizes and caps. Everything is bounded: a burst of 50,000 events in
// one window produces the same size batch as a burst of 500.
const (
	window          = 100 * time.Millisecond
	maxPlainSamples = 6  // successes/ingests kept per tenant per window (reservoir)
	maxPrioSamples  = 24 // failures kept per tenant per window (always admitted, up to this cap)
	maxLatencies    = 64 // latency samples per endpoint per window (reservoir)
	outboxSize      = 256
)

// Publisher pre-aggregates live activity in-process and publishes one batch
// per tenant per 100ms window.
//
// Recording is a mutex and a few increments: no I/O, no allocation in the
// steady state, safe to call on the ingest and ack paths. Publishing happens
// on a separate goroutine, and when Redis is slow or down the outbox fills
// and batches are DROPPED — the dashboard goes stale, deliveries never wait.
type Publisher struct {
	bus Bus
	log *slog.Logger

	mu   sync.Mutex
	cur  map[uuid.UUID]*accum
	rng  *rand.Rand
	out  chan []byte
	drop prometheus.Counter

	// pool, when set, reports this process's worker-pool occupancy with
	// every flush (dispatchers only).
	pool    func() (workers, busy int)
	process string
}

// ReportPool makes every flush include this process's worker-pool
// occupancy, so the dashboard can draw the bounded pool as it really is.
func (p *Publisher) ReportPool(process string, f func() (workers, busy int)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.process, p.pool = process, f
}

type accum struct {
	b         Batch
	plainSeen int
}

// NewPublisher builds a publisher. Call Run to start it.
func NewPublisher(bus Bus, log *slog.Logger, reg prometheus.Registerer) *Publisher {
	p := &Publisher{
		bus: bus, log: log,
		cur: map[uuid.UUID]*accum{},
		rng: rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64())), //nolint:gosec // sampling, not security
		out: make(chan []byte, outboxSize),
		drop: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metrics.Namespace, Subsystem: "live", Name: "publish_dropped_total",
			Help: "Live batches dropped because the bus was slow or down. Costs dashboard freshness, never a delivery.",
		}),
	}
	reg.MustRegister(p.drop)
	return p
}

// Ingested records an accepted event.
func (p *Publisher) Ingested(tenant, eventID uuid.UUID, eventType string) {
	now := time.Now().UnixMilli()
	p.mu.Lock()
	defer p.mu.Unlock()
	a := p.accum(tenant)
	a.b.Ingested++
	p.sample(a, Sample{EventID: eventID, Type: eventType, Outcome: Ingested, At: now})
}

// Attempt records one acked delivery attempt.
func (p *Publisher) Attempt(tenant, endpoint, eventID, deliveryID uuid.UUID, eventType, outcome string, attempt int, latency time.Duration) {
	now := time.Now().UnixMilli()
	ms := int32(min(latency.Milliseconds(), 1<<30)) //nolint:gosec // clamped
	p.mu.Lock()
	defer p.mu.Unlock()
	a := p.accum(tenant)
	if a.b.Endpoints == nil {
		a.b.Endpoints = map[uuid.UUID]*EndpointWindow{}
	}
	w := a.b.Endpoints[endpoint]
	if w == nil {
		w = &EndpointWindow{}
		a.b.Endpoints[endpoint] = w
	}
	switch outcome {
	case Delivered:
		w.OK++
	case Retrying:
		w.Retry++
	case DeadLetter:
		w.Dead++
	case RateLimited:
		w.RateLimited++
	}
	if latency > 0 {
		w.latSeen++
		if len(w.Latencies) < maxLatencies {
			w.Latencies = append(w.Latencies, ms)
		} else if j := p.rng.IntN(w.latSeen); j < maxLatencies {
			w.Latencies[j] = ms
		}
	}
	ep, del := endpoint, deliveryID
	p.sample(a, Sample{EventID: eventID, DeliveryID: &del, EndpointID: &ep, Type: eventType,
		Outcome: outcome, Attempt: attempt, LatencyMS: int(ms), At: now})
}

// sample admits s with priority sampling: failures always (up to a cap),
// everything else by reservoir sampling (Algorithm R) over the window, so each
// success has an equal chance of being the one shown. Caller holds p.mu.
func (p *Publisher) sample(a *accum, s Sample) {
	if s.priority() {
		if countPrio(a.b.Samples) < maxPrioSamples {
			a.b.Samples = append(a.b.Samples, s)
		}
		return
	}
	a.plainSeen++
	plain := len(a.b.Samples) - countPrio(a.b.Samples)
	if plain < maxPlainSamples {
		a.b.Samples = append(a.b.Samples, s)
		return
	}
	if j := p.rng.IntN(a.plainSeen); j < maxPlainSamples {
		// Replace the j-th plain sample.
		n := 0
		for i := range a.b.Samples {
			if !a.b.Samples[i].priority() {
				if n == j {
					a.b.Samples[i] = s
					return
				}
				n++
			}
		}
	}
}

func countPrio(ss []Sample) int {
	n := 0
	for _, s := range ss {
		if s.priority() {
			n++
		}
	}
	return n
}

func (p *Publisher) accum(tenant uuid.UUID) *accum {
	a := p.cur[tenant]
	if a == nil {
		a = &accum{}
		p.cur[tenant] = a
	}
	return a
}

// Delta publishes an entity change immediately (low volume).
func (p *Publisher) Delta(tenant uuid.UUID, d Delta) {
	p.enqueue(Envelope{Kind: "delta", Tenant: tenant, Delta: &d})
}

// Narrate publishes a narration line immediately.
func (p *Publisher) Narrate(tenant uuid.UUID, n Narration) {
	if n.At.IsZero() {
		n.At = time.Now()
	}
	p.enqueue(Envelope{Kind: "narration", Tenant: tenant, Narration: &n})
}

func (p *Publisher) enqueue(e Envelope) {
	b, err := json.Marshal(e)
	if err != nil {
		p.log.Error("encode live message", "error", err)
		return
	}
	select {
	case p.out <- b:
	default:
		p.drop.Inc()
	}
}

// Flush encodes and enqueues the current windows. Exported for tests; Run
// calls it every 100ms.
func (p *Publisher) Flush() {
	p.mu.Lock()
	cur := p.cur
	p.cur = make(map[uuid.UUID]*accum, len(cur))
	pool, process := p.pool, p.process
	p.mu.Unlock()
	for tenant, a := range cur {
		b := a.b
		p.enqueue(Envelope{Kind: "batch", Tenant: tenant, Batch: &b})
	}
	if pool != nil {
		w, busy := pool()
		p.enqueue(Envelope{Kind: "pool", Pool: &PoolStat{Process: process, Workers: w, Busy: busy}})
	}
}

// Run flushes every window and publishes until ctx is done.
func (p *Publisher) Run(ctx context.Context) error {
	go func() {
		t := time.NewTicker(window)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				p.Flush()
			}
		}
	}()
	var lastErr time.Time
	for {
		select {
		case <-ctx.Done():
			return nil
		case b := <-p.out:
			pctx, cancel := context.WithTimeout(ctx, time.Second)
			err := p.bus.Publish(pctx, b)
			cancel()
			if err != nil && ctx.Err() == nil {
				p.drop.Inc()
				if time.Since(lastErr) > 10*time.Second { // don't flood the log during an outage
					p.log.Warn("live publish failed; dashboard will be stale", "error", err)
					lastErr = time.Now()
				}
			}
		}
	}
}
