//go:build integration

package live_test

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
	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/SharmaSanskar/ithaca/internal/auth"
	"github.com/SharmaSanskar/ithaca/internal/breaker"
	"github.com/SharmaSanskar/ithaca/internal/deliver"
	"github.com/SharmaSanskar/ithaca/internal/ingest"
	"github.com/SharmaSanskar/ithaca/internal/live"
	"github.com/SharmaSanskar/ithaca/internal/platform/clock"
	"github.com/SharmaSanskar/ithaca/internal/queue/pg"
	"github.com/SharmaSanskar/ithaca/internal/receiver"
	"github.com/SharmaSanskar/ithaca/internal/retry"
	"github.com/SharmaSanskar/ithaca/internal/store/gen"
	"github.com/SharmaSanskar/ithaca/internal/testdb"
	"github.com/SharmaSanskar/ithaca/internal/testredis"
	"github.com/SharmaSanskar/ithaca/internal/worker"
)

// wsClient collects everything the server sends.
type wsClient struct {
	mu   sync.Mutex
	msgs []map[string]any
	conn *websocket.Conn
}

func dial(t *testing.T, url string, channels ...string) *wsClient {
	t.Helper()
	conn, resp, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(url, "http")+"/ws", nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	resp.Body.Close()
	c := &wsClient{conn: conn}
	t.Cleanup(func() { _ = conn.Close() })
	go func() {
		for {
			_, b, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var m map[string]any
			_ = json.Unmarshal(b, &m)
			c.mu.Lock()
			c.msgs = append(c.msgs, m)
			c.mu.Unlock()
		}
	}()
	for _, ch := range channels {
		if err := conn.WriteJSON(map[string]string{"op": "subscribe", "ch": ch}); err != nil {
			t.Fatal(err)
		}
	}
	return c
}

func (c *wsClient) all() []map[string]any {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]map[string]any(nil), c.msgs...)
}

func (c *wsClient) waitFor(t *testing.T, what string, timeout time.Duration, match func(map[string]any) bool) map[string]any {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, m := range c.all() {
			if match(m) {
				return m
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, what)
	return nil
}

// The whole live path, nothing faked: real ingest → real dispatcher → a real
// (failing) HTTP endpoint → post-commit observers → publisher → Redis pub/sub
// → hub (+ Postgres reconciliation) → a real WebSocket client.
func TestLivePipelineEndToEnd(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	s := testdb.New(t)
	rdb := testredis.New(t)

	p, err := auth.ProvisionTenant(ctx, s, "live")
	if err != nil {
		t.Fatal(err)
	}
	rcv := receiver.New(1, false)
	rcv.SetSink("down", receiver.Behavior{FailRate: 1, FailStatus: 503})
	sinks := httptest.NewServer(rcv.Handler())
	defer sinks.Close()
	ep, err := s.CreateEndpoint(ctx, gen.CreateEndpointParams{
		ID: uuid.Must(uuid.NewV7()), TenantID: p.TenantID, Url: sinks.URL + "/sink/down", Description: "down",
		State: "enabled", TimeoutMs: 1000, MaxConcurrency: 4, SigningKeyID: "k", SigningSecret: []byte("s"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateSubscription(ctx, gen.CreateSubscriptionParams{
		ID: uuid.Must(uuid.NewV7()), TenantID: p.TenantID, EndpointID: ep.ID, EventTypes: []string{"*"}, LaneCount: 4,
	}); err != nil {
		t.Fatal(err)
	}

	// Producers: ingest and the dispatcher, both publishing post-commit.
	bus := live.NewRedisBus(rdb)
	pub := live.NewPublisher(bus, log, prometheus.NewRegistry())
	go func() { _ = pub.Run(ctx) }()
	svc := ingest.NewService(s, nil)
	svc.SetObserver(live.IngestObserver{P: pub})

	q := pg.New(s, pg.WithBreaker(breaker.Policy{Threshold: 5, BaseCooldown: time.Minute, MaxCooldown: time.Minute}))
	guard, _ := deliver.NewGuard([]string{"127.0.0.0/8"})
	slots := worker.NewSlots(8)
	m := worker.NewMetrics(prometheus.NewRegistry(), slots)
	pool := worker.New(worker.Config{WorkerID: "w", Workers: 8, MaxStreamsBatch: 8, DrainBatch: 16, MaxDrainRounds: 2,
		MinLease: 5 * time.Second, PollFloor: 5 * time.Millisecond, PollCeiling: 50 * time.Millisecond, DisabledRecheck: time.Minute},
		q, worker.NewExecutor(deliver.NewClient(guard), retry.Policy{Base: 5 * time.Millisecond, Multiplier: 2, Cap: 20 * time.Millisecond, MaxAttempts: 100}, m),
		slots, log, m, clock.Real{})
	pool.SetObserver(live.DispatchObserver{P: pub})
	go func() { _ = pool.Run(ctx) }()
	go func() { _ = (&worker.BreakerLoop{B: q, Every: 50 * time.Millisecond, Log: log, M: m}).Run(ctx) }()

	// Consumer: the hub behind a real WebSocket endpoint.
	hub := live.NewHub(live.PGSnapshotter{Store: s}, log, prometheus.NewRegistry())
	go func() { _ = hub.Run(ctx, bus.Subscribe(ctx)) }()
	time.Sleep(200 * time.Millisecond) // let the Redis subscription establish (pub/sub has no backlog)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		(&live.Handler{Hub: hub, Resolve: func(*http.Request) (uuid.UUID, bool) { return p.TenantID, true }}).ServeHTTP(w, r)
	}))
	defer srv.Close()
	client := dial(t, srv.URL, live.ChanFlow)
	client.waitFor(t, "hello", 5*time.Second, func(m map[string]any) bool { return m["type"] == "hello" })

	// Traffic.
	for i := range 40 {
		body := fmt.Sprintf(`{"type":"order.created","payload":{"i":%d}}`, i)
		if _, err := svc.Ingest(ctx, ingest.Request{TenantID: p.TenantID, Method: "POST", Path: "/v1/events", Body: []byte(body)}); err != nil {
			t.Fatal(err)
		}
	}

	// 1. Frames report what actually happened.
	client.waitFor(t, "a frame showing retries at the failing endpoint", 10*time.Second, func(m map[string]any) bool {
		if m["type"] != "frame" {
			return false
		}
		for _, e := range m["endpoints"].([]any) {
			if e.(map[string]any)["retry"].(float64) > 0 {
				return true
			}
		}
		return false
	})
	// Counts are eventually exact: a publisher's batch can land a window or
	// two after the event. Sum across frames until it converges; a total
	// that never reaches 40 would mean the pipeline lost data.
	sum := func() float64 {
		n := 0.0
		for _, m := range client.all() {
			if m["type"] == "frame" {
				n += m["ingested"].(float64)
			}
		}
		return n
	}
	deadline := time.Now().Add(5 * time.Second)
	for sum() < 40 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if got := sum(); got != 40 {
		t.Errorf("frames reported %v events ingested in total, want exactly 40", got)
	}

	// 2. The breaker trip arrives as a delta carrying the new state.
	d := client.waitFor(t, "an endpoint delta with the circuit open", 10*time.Second, func(m map[string]any) bool {
		if m["type"] != "delta" || m["kind"] != "endpoint" {
			return false
		}
		return m["data"].(map[string]any)["circuit"] == "open"
	})
	if d["rev"].(float64) <= 0 {
		t.Error("endpoint delta has no revision")
	}

	// 3. Frames keep flowing at ~10Hz, and carry the parked backlog as depth.
	before := countFrames(client.all())
	time.Sleep(time.Second)
	rate := countFrames(client.all()) - before
	if rate < 7 || rate > 13 {
		t.Errorf("%d frames in 1s, want ~10", rate)
	}
	last := lastFrame(client.all())
	e := last["endpoints"].([]any)[0].(map[string]any)
	if e["circuit"] != "open" || e["depth"].(float64) <= 0 {
		t.Errorf("latest frame endpoint = %v; want circuit open with a parked backlog", e)
	}
}

func countFrames(ms []map[string]any) int {
	n := 0
	for _, m := range ms {
		if m["type"] == "frame" {
			n++
		}
	}
	return n
}

func lastFrame(ms []map[string]any) map[string]any {
	for i := len(ms) - 1; i >= 0; i-- {
		if ms[i]["type"] == "frame" {
			return ms[i]
		}
	}
	return nil
}
