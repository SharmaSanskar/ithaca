package demo

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/SharmaSanskar/ithaca/internal/ingest"
)

// Generator produces realistic traffic through the REAL ingest transaction
// (in-process, so no HTTP hop, but the same code path and the same database
// work as POST /v1/events). It measures its own ingest latency, which is what
// lets a scenario prove "ingest never slowed down" with a number.
type Generator struct {
	Ingest *ingest.Service
	Tenant uuid.UUID

	rate atomic.Int64 // events per second

	mu   sync.Mutex
	lat  []time.Duration // ring buffer of recent ingest latencies
	next int
	rng  *rand.Rand
	sent atomic.Int64
}

// NewGenerator builds a generator at the given rate.
func NewGenerator(svc *ingest.Service, tenant uuid.UUID, rate int) *Generator {
	g := &Generator{Ingest: svc, Tenant: tenant, lat: make([]time.Duration, 0, 2048),
		rng: rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))} //nolint:gosec // demo data
	g.SetRate(rate)
	return g
}

// MaxRate caps demo traffic: the public demo runs on one small VM.
const MaxRate = 200

// SetRate changes events per second (0 pauses).
func (g *Generator) SetRate(r int) { g.rate.Store(int64(min(max(r, 0), MaxRate))) }

// Rate returns events per second.
func (g *Generator) Rate() int { return int(g.rate.Load()) }

// Sent returns the total events generated.
func (g *Generator) Sent() int64 { return g.sent.Load() }

// Run generates until ctx is done. Ticks every 50ms and sends rate/20 events
// per tick, carrying the fractional remainder so low rates are accurate.
func (g *Generator) Run(ctx context.Context) error {
	t := time.NewTicker(50 * time.Millisecond)
	defer t.Stop()
	carry := 0.0
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
		carry += float64(g.Rate()) / 20
		n := int(carry)
		carry -= float64(n)
		var wg sync.WaitGroup
		for range n {
			wg.Go(func() { g.one(ctx) })
		}
		wg.Wait()
	}
}

func (g *Generator) one(ctx context.Context) {
	typ, key, payload := g.event()
	body, _ := json.Marshal(map[string]any{"type": typ, "ordering_key": key, "payload": payload})
	start := time.Now()
	if _, err := g.Ingest.Ingest(ctx, ingest.Request{TenantID: g.Tenant, Method: "POST", Path: "/v1/events", Body: body}); err != nil {
		return
	}
	g.record(time.Since(start))
	g.sent.Add(1)
}

func (g *Generator) event() (typ, key string, payload map[string]any) {
	g.mu.Lock()
	r := g.rng.IntN(100)
	order := g.rng.IntN(300)
	amount := 5 + g.rng.IntN(495)
	g.mu.Unlock()
	switch {
	case r < 40:
		typ = []string{"order.created", "order.paid", "order.shipped"}[order%3]
		key = fmt.Sprintf("order_%d", order)
		payload = map[string]any{"order_id": key, "total": amount, "currency": "USD"}
	case r < 60:
		typ = "invoice.paid"
		payload = map[string]any{"invoice_id": fmt.Sprintf("inv_%d", order), "amount": amount}
	case r < 75:
		typ = "payment.succeeded"
		payload = map[string]any{"payment_id": fmt.Sprintf("pay_%d", order), "amount": amount}
	default:
		typ = []string{"user.signup", "user.updated"}[order%2]
		key = fmt.Sprintf("user_%d", order)
		payload = map[string]any{"user_id": key, "plan": []string{"free", "pro", "team"}[order%3]}
	}
	return typ, key, payload
}

func (g *Generator) record(d time.Duration) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.lat) < cap(g.lat) {
		g.lat = append(g.lat, d)
		return
	}
	g.lat[g.next] = d
	g.next = (g.next + 1) % len(g.lat)
}

// IngestP99 is the p99 of recent ingest latencies.
func (g *Generator) IngestP99() time.Duration {
	g.mu.Lock()
	s := slices.Clone(g.lat)
	g.mu.Unlock()
	if len(s) == 0 {
		return 0
	}
	slices.Sort(s)
	return s[min(len(s)-1, len(s)*99/100)]
}
