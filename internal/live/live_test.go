package live

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// ── outbox: the per-connection backpressure policy ───────────────────────────

func TestOutboxNewestFrameSupersedesUnsentOlderOne(t *testing.T) {
	o := newOutbox()
	for i := range 5 {
		o.offerFrame("flow", []byte(fmt.Sprintf("frame-%d", i)))
	}
	msgs, superseded, _ := o.take()
	if len(msgs) != 1 || string(msgs[0]) != "frame-4" {
		t.Fatalf("got %q; a slow client must get only the newest frame", msgs)
	}
	if superseded != 4 {
		t.Errorf("superseded = %d, want 4", superseded)
	}
}

func TestOutboxKeepsOneFramePerChannel(t *testing.T) {
	o := newOutbox()
	o.offerFrame("flow", []byte("a"))
	o.offerFrame("endpoint:x", []byte("b"))
	if msgs, _, _ := o.take(); len(msgs) != 2 {
		t.Fatalf("got %d messages, want one per channel", len(msgs))
	}
}

func TestOutboxDeltasAreNeverDroppedAndComeFirst(t *testing.T) {
	o := newOutbox()
	o.offerFrame("flow", []byte("frame"))
	for i := range 10 {
		if !o.offerDelta([]byte(fmt.Sprintf("delta-%d", i))) {
			t.Fatal("delta refused below the cap")
		}
	}
	msgs, _, overflow := o.take()
	if overflow || len(msgs) != 11 {
		t.Fatalf("got %d messages (overflow=%v)", len(msgs), overflow)
	}
	for i := range 10 {
		if string(msgs[i]) != fmt.Sprintf("delta-%d", i) {
			t.Fatalf("deltas out of order or not first: %q", msgs)
		}
	}
}

func TestOutboxOverflowMeansDisconnect(t *testing.T) {
	o := newOutbox()
	for range maxPendingDeltas {
		o.offerDelta([]byte("d"))
	}
	if o.offerDelta([]byte("one too many")) {
		t.Fatal("a hopelessly slow client's delta queue must overflow, not grow forever")
	}
	if _, _, overflow := o.take(); !overflow {
		t.Error("overflow not reported to the writer")
	}
}

// ── publisher: bounded windows, priority sampling ────────────────────────────

type captureBus struct {
	mu   sync.Mutex
	msgs []Envelope
}

func (c *captureBus) Publish(_ context.Context, b []byte) error {
	var e Envelope
	_ = json.Unmarshal(b, &e)
	c.mu.Lock()
	c.msgs = append(c.msgs, e)
	c.mu.Unlock()
	return nil
}

func drain(p *Publisher) []Envelope {
	var out []Envelope
	for {
		select {
		case b := <-p.out:
			var e Envelope
			_ = json.Unmarshal(b, &e)
			out = append(out, e)
		default:
			return out
		}
	}
}

func TestPublisherBatchesAreBoundedAndFailuresAlwaysSampled(t *testing.T) {
	p := NewPublisher(&captureBus{}, quiet(), prometheus.NewRegistry())
	tenant, ep := uuid.New(), uuid.New()

	// A burst: 10,000 successes and 3 failures in one window.
	for range 10_000 {
		p.Attempt(tenant, ep, uuid.New(), uuid.New(), "t", Delivered, 1, 20*time.Millisecond)
	}
	for range 3 {
		p.Attempt(tenant, ep, uuid.New(), uuid.New(), "t", DeadLetter, 6, 5*time.Millisecond)
	}
	p.Flush()
	env := drain(p)
	if len(env) != 1 {
		t.Fatalf("%d envelopes, want 1 batch", len(env))
	}
	b := env[0].Batch
	w := b.Endpoints[ep]
	if w.OK != 10_000 || w.Dead != 3 {
		t.Errorf("counts ok=%d dead=%d: counts must be exact even when samples are not", w.OK, w.Dead)
	}
	if len(w.Latencies) > maxLatencies {
		t.Errorf("%d latency samples, cap is %d", len(w.Latencies), maxLatencies)
	}
	dead, plain := 0, 0
	for _, s := range b.Samples {
		if s.Outcome == DeadLetter {
			dead++
		} else {
			plain++
		}
	}
	if dead != 3 {
		t.Errorf("%d of 3 failures sampled: a rare failure must never be sampled out", dead)
	}
	if plain > maxPlainSamples {
		t.Errorf("%d success samples, cap is %d", plain, maxPlainSamples)
	}
}

func TestPublisherDropsInsteadOfBlocking(t *testing.T) {
	p := NewPublisher(&captureBus{}, quiet(), prometheus.NewRegistry())
	// Nobody is draining p.out (Run is not started): fill it past capacity.
	done := make(chan struct{})
	go func() {
		for range outboxSize * 3 {
			p.Delta(uuid.New(), Delta{Kind: DeltaEndpoint, ID: "x"})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("publishing blocked when the bus was not keeping up; it must drop instead")
	}
}

// ── hub: merge, frames, deltas, isolation ───────────────────────────────────

type fakeSnap struct {
	mu sync.Mutex
	s  map[uuid.UUID]Snapshot
}

func (f *fakeSnap) Snapshot(_ context.Context, t uuid.UUID) (Snapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.s[t], nil
}

func (f *fakeSnap) set(t uuid.UUID, s Snapshot) {
	f.mu.Lock()
	f.s[t] = s
	f.mu.Unlock()
}

func newTestHub() (*Hub, *fakeSnap) {
	fs := &fakeSnap{s: map[uuid.UUID]Snapshot{}}
	return NewHub(fs, quiet(), prometheus.NewRegistry()), fs
}

func attach(h *Hub, tenant uuid.UUID, chans ...string) *client {
	c := &client{tenant: tenant, box: newOutbox(), subs: map[string]bool{}, closed: func() {}}
	h.register(c)
	for _, ch := range chans {
		h.subscribe(context.Background(), c, ch)
	}
	return c
}

func messages(t *testing.T, c *client) []map[string]any {
	t.Helper()
	msgs, _, _ := c.box.take()
	out := make([]map[string]any, 0, len(msgs))
	for _, m := range msgs {
		var v map[string]any
		if err := json.Unmarshal(m, &v); err != nil {
			t.Fatal(err)
		}
		out = append(out, v)
	}
	return out
}

func envelope(t uuid.UUID, b Batch) []byte {
	m, _ := json.Marshal(Envelope{Kind: "batch", Tenant: t, Batch: &b})
	return m
}

func TestHubMergesBatchesFromEveryProcessIntoOneFrame(t *testing.T) {
	h, fs := newTestHub()
	tenant, ep := uuid.New(), uuid.New()
	fs.set(tenant, Snapshot{Endpoints: []SnapshotEndpoint{{ID: ep, Name: "billing", State: "enabled", Circuit: "closed", Depth: 7}}, DLQ: 2})
	c := attach(h, tenant, ChanFlow)

	// Two processes (an ingest and a dispatcher) report in the same window.
	h.ingest(envelope(tenant, Batch{Ingested: 5}))
	h.ingest(envelope(tenant, Batch{Endpoints: map[uuid.UUID]*EndpointWindow{ep: {OK: 3, Retry: 1, Latencies: []int32{10, 20, 30, 400}}}}))
	h.emitFrames()

	f := messages(t, c)
	if len(f) != 1 || f[0]["type"] != "frame" {
		t.Fatalf("got %v", f)
	}
	if f[0]["ingested"] != float64(5) {
		t.Errorf("ingested = %v", f[0]["ingested"])
	}
	e := f[0]["endpoints"].([]any)[0].(map[string]any)
	if e["ok"] != float64(3) || e["retry"] != float64(1) || e["depth"] != float64(7) || e["p99_ms"] != float64(400) {
		t.Errorf("endpoint slice = %v", e)
	}
	if f[0]["totals"].(map[string]any)["dlq"] != float64(2) {
		t.Errorf("totals = %v", f[0]["totals"])
	}

	// The next frame starts a fresh window: counts are per window, not cumulative.
	h.emitFrames()
	f = messages(t, c)
	if f[0]["ingested"] != float64(0) {
		t.Errorf("second frame ingested = %v; windows must reset", f[0]["ingested"])
	}
}

func TestEndpointChannelNeverReceivesTheFirehose(t *testing.T) {
	h, fs := newTestHub()
	tenant, a, b := uuid.New(), uuid.New(), uuid.New()
	fs.set(tenant, Snapshot{Endpoints: []SnapshotEndpoint{{ID: a, Name: "a"}, {ID: b, Name: "b"}}})
	c := attach(h, tenant, chanEndpoint+a.String())

	h.ingest(envelope(tenant, Batch{Samples: []Sample{
		{EventID: uuid.New(), EndpointID: &a, Outcome: Delivered},
		{EventID: uuid.New(), EndpointID: &b, Outcome: Delivered},
	}}))
	h.emitFrames()
	f := messages(t, c)[0]
	eps := f["endpoints"].([]any)
	if len(eps) != 1 || eps[0].(map[string]any)["name"] != "a" {
		t.Errorf("endpoint channel got endpoints %v", eps)
	}
	if n := len(f["samples"].([]any)); n != 1 {
		t.Errorf("endpoint channel got %d samples, want only its own", n)
	}
}

func TestEndpointDeltasComeFromPostgresRevisions(t *testing.T) {
	h, fs := newTestHub()
	tenant, ep := uuid.New(), uuid.New()
	t0 := time.Now()
	fs.set(tenant, Snapshot{Endpoints: []SnapshotEndpoint{{ID: ep, Circuit: "closed", UpdatedAt: t0, Data: map[string]string{"circuit": "closed"}}}})
	c := attach(h, tenant, ChanFlow) // first snapshot: baseline, no delta
	_ = messages(t, c)

	h.refresh(context.Background(), tenant) // unchanged revision: no delta
	if m := messages(t, c); len(m) != 0 {
		t.Fatalf("delta emitted with no change: %v", m)
	}

	// The breaker opened. Even if the pub/sub delta was LOST, the next
	// once-a-second refresh sees the new revision and emits the delta.
	fs.set(tenant, Snapshot{Endpoints: []SnapshotEndpoint{{ID: ep, Circuit: "open", UpdatedAt: t0.Add(time.Second), Data: map[string]string{"circuit": "open"}}}})
	h.refresh(context.Background(), tenant)
	m := messages(t, c)
	if len(m) != 1 || m[0]["type"] != "delta" || m[0]["kind"] != "endpoint" || m[0]["data"].(map[string]any)["circuit"] != "open" {
		t.Fatalf("got %v, want one endpoint delta carrying the new state", m)
	}
}

func TestTenantsAreIsolated(t *testing.T) {
	h, fs := newTestHub()
	a, b := uuid.New(), uuid.New()
	fs.set(a, Snapshot{})
	fs.set(b, Snapshot{})
	ca := attach(h, a, ChanFlow, ChanDemo)
	cb := attach(h, b, ChanFlow, ChanDemo)

	h.ingest(envelope(a, Batch{Ingested: 9}))
	n, _ := json.Marshal(Envelope{Kind: "narration", Tenant: a, Narration: &Narration{Text: "for A"}})
	h.ingest(n)
	h.emitFrames()

	for _, m := range messages(t, cb) {
		if m["type"] == "narration" || m["ingested"] == float64(9) {
			t.Fatalf("tenant B received tenant A's data: %v", m)
		}
	}
	got := map[string]bool{}
	for _, m := range messages(t, ca) {
		got[m["type"].(string)] = true
	}
	if !got["frame"] || !got["narration"] {
		t.Errorf("tenant A got %v", got)
	}
}

func TestNarrationOnlyReachesDemoSubscribers(t *testing.T) {
	h, fs := newTestHub()
	tenant := uuid.New()
	fs.set(tenant, Snapshot{})
	flowOnly := attach(h, tenant, ChanFlow)
	n, _ := json.Marshal(Envelope{Kind: "narration", Tenant: tenant, Narration: &Narration{Text: "hi"}})
	h.ingest(n)
	for _, m := range messages(t, flowOnly) {
		if m["type"] == "narration" {
			t.Fatal("narration sent to a client not subscribed to the demo channel")
		}
	}
}

func TestNoFramesForUnwatchedTenants(t *testing.T) {
	h, fs := newTestHub()
	tenant := uuid.New()
	fs.set(tenant, Snapshot{})
	c := attach(h, tenant, ChanDemo) // narration only: not watching frames
	h.ingest(envelope(tenant, Batch{Ingested: 1}))
	h.emitFrames()
	if m := messages(t, c); len(m) != 0 {
		t.Errorf("frames built for a tenant nobody watches: %v", m)
	}
}

// ── WebSocket handler ────────────────────────────────────────────────────────

func TestWebSocketRejectsCrossOriginBrowsers(t *testing.T) {
	h, _ := newTestHub()
	srv := httptest.NewServer(&Handler{Hub: h, Resolve: func(*http.Request) (uuid.UUID, bool) { return uuid.New(), true }})
	defer srv.Close()

	for origin, want := range map[string]int{
		"https://evil.example": http.StatusForbidden,
		"":                     http.StatusSwitchingProtocols, // non-browser clients
	} {
		req, _ := http.NewRequest(http.MethodGet, srv.URL, nil)
		req.Header.Set("Connection", "Upgrade")
		req.Header.Set("Upgrade", "websocket")
		req.Header.Set("Sec-WebSocket-Version", "13")
		req.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != want {
			t.Errorf("origin %q: status %d, want %d", origin, resp.StatusCode, want)
		}
	}
}

func TestValidChannel(t *testing.T) {
	for _, ch := range []string{"flow", "demo", "endpoint:" + uuid.NewString()} {
		if !ValidChannel(ch) {
			t.Errorf("%q rejected", ch)
		}
	}
	for _, ch := range []string{"", "firehose", "endpoint:", "endpoint:not-a-uuid", strings.Repeat("x", 100)} {
		if ValidChannel(ch) {
			t.Errorf("%q accepted", ch)
		}
	}
}

// A newly connected client's first frame must already have its endpoints.
func TestNoFrameBeforeTheFirstSnapshot(t *testing.T) {
	h, _ := newTestHub()
	tenant := uuid.New()
	// Register a watcher WITHOUT the snapshot having loaded yet.
	c := &client{tenant: tenant, box: newOutbox(), subs: map[string]bool{ChanFlow: true}, closed: func() {}}
	h.register(c)
	h.mu.Lock()
	h.tenant(tenant).watchers++
	h.mu.Unlock()
	h.emitFrames()
	if m := messages(t, c); len(m) != 0 {
		t.Fatalf("emitted %v before the snapshot loaded; the dashboard would flash an empty graph", m)
	}
}
