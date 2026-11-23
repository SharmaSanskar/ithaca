//go:build integration

package worker_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/SharmaSanskar/ithaca/internal/deliver"
	"github.com/SharmaSanskar/ithaca/internal/dlq"
	"github.com/SharmaSanskar/ithaca/internal/receiver"
	"github.com/SharmaSanskar/ithaca/internal/retry"
	"github.com/SharmaSanskar/ithaca/internal/store/gen"
	"github.com/SharmaSanskar/ithaca/internal/verify"
	"github.com/SharmaSanskar/ithaca/internal/worker"
)

func (h *harness) deadLetterIDs() []uuid.UUID {
	h.t.Helper()
	rows, err := h.store.Pool.Query(context.Background(),
		"SELECT id FROM deliveries_history WHERE final_state='dead' AND replayed_at IS NULL ORDER BY completed_at")
	if err != nil {
		h.t.Fatal(err)
	}
	defer rows.Close()
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		_ = rows.Scan(&id)
		ids = append(ids, id)
	}
	return ids
}

func (h *harness) replayer() *dlq.Replayer {
	return &dlq.Replayer{Store: h.store, RatePerEndpoint: 1000}
}

// receivedSeqs returns the payload seq of every 2xx the sink answered, in
// arrival order.
func (h *harness) receivedSeqs(sink string) []int {
	var out []int
	for _, r := range h.rcv.Received() {
		if r.Sink != sink || r.Status != 200 {
			continue
		}
		var p payload
		_ = json.Unmarshal(r.Body, &p)
		out = append(out, p.Seq)
	}
	return out
}

func (h *harness) waitFor(what string, timeout time.Duration, cond func() bool) {
	h.t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			h.t.Fatalf("timed out after %s waiting for: %s", timeout, what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// An ordered stream whose head dies must STOP, not skip: delivering
// "order.shipped" after "order.created" was dead-lettered hands the
// subscriber a state it cannot reconcile. Replaying the dead head puts it back
// at its original position and the stream resumes in exact order.
func TestDeadOrderedHeadPausesStreamAndReplayRestoresOrder(t *testing.T) {
	h := newHarness(t)
	h.subscribe(h.endpoint("ordered", time.Second), true)
	// The very first request fails permanently: seq 0 dies on its first attempt.
	h.rcv.SetSink("ordered", receiver.Behavior{FailRequests: 1, FailStatus: 404})
	for i := range 5 {
		h.publish("order_1", i)
	}

	stop := h.startPool(poolOpts{workers: 4})
	h.waitFor("stream to pause", 10*time.Second, func() bool {
		return h.count("SELECT count(*) FROM delivery_streams WHERE state='paused'") == 1
	})
	time.Sleep(300 * time.Millisecond) // give a broken implementation time to skip ahead

	if got := len(h.receivedSeqs("ordered")); got != 0 {
		t.Fatalf("%d events were delivered past the dead head; the stream must pause", got)
	}
	if got := h.count("SELECT count(*) FROM deliveries"); got != 4 {
		t.Fatalf("pending = %d, want 4 waiting behind the dead head", got)
	}
	dead := h.deadLetterIDs()
	if len(dead) != 1 {
		t.Fatalf("dead letters = %d, want 1", len(dead))
	}
	var blocks bool
	_ = h.store.Pool.QueryRow(context.Background(),
		"SELECT paused_delivery_id = $1 FROM delivery_streams", dead[0]).Scan(&blocks)
	if !blocks {
		t.Fatal("the paused stream does not point at its dead head")
	}

	res, err := h.replayer().Replay(context.Background(), dlq.Request{TenantID: h.tenant, IDs: dead})
	if err != nil {
		t.Fatal(err)
	}
	if res.Replayed != 1 || res.Resumed != 1 {
		t.Fatalf("replay result %+v, want 1 replayed that resumed the stream", res)
	}
	h.waitDrained(10 * time.Second)
	_ = stop(5 * time.Second)

	got := h.receivedSeqs("ordered")
	want := []int{0, 1, 2, 3, 4}
	if len(got) != len(want) {
		t.Fatalf("received %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("received %v, want exact original order %v", got, want)
		}
	}
	// Same event, new delivery: the subscriber can dedupe by Ithaca-Id.
	var sameEvent bool
	_ = h.store.Pool.QueryRow(context.Background(), `
		SELECT r.event_id = d.event_id AND r.id <> d.id
		FROM deliveries_history r JOIN deliveries_history d ON r.replay_of = d.id`).Scan(&sameEvent)
	if !sameEvent {
		t.Error("a replay must keep the event id and get a new delivery id")
	}
}

// Someone will double-click "Replay". Two concurrent replays of the same dead
// letters must create each replay exactly once.
func TestReplayIsIdempotentUnderConcurrentDoubleClick(t *testing.T) {
	h := newHarness(t)
	h.subscribe(h.endpoint("gone", time.Second), false)
	h.rcv.SetSink("gone", receiver.Behavior{FailRate: 1, FailStatus: 400})
	for i := range 20 {
		h.publish("k", i)
	}
	stop := h.startPool(poolOpts{workers: 8})
	h.waitDrained(10 * time.Second)
	_ = stop(5 * time.Second)
	dead := h.deadLetterIDs()
	if len(dead) != 20 {
		t.Fatalf("dead letters = %d, want 20", len(dead))
	}

	var wg sync.WaitGroup
	results := make([]dlq.Result, 2)
	for i := range 2 {
		wg.Go(func() {
			r, err := h.replayer().Replay(context.Background(), dlq.Request{TenantID: h.tenant, IDs: dead})
			if err != nil {
				t.Error(err)
			}
			results[i] = r
		})
	}
	wg.Wait()

	if total := results[0].Replayed + results[1].Replayed; total != 20 {
		t.Errorf("two concurrent replays created %d deliveries (%+v), want exactly 20", total, results)
	}
	if got := h.count("SELECT count(*) FROM deliveries"); got != 20 {
		t.Errorf("pending = %d, want 20", got)
	}
	if got := h.count("SELECT count(*) FROM deliveries_history WHERE replayed_at IS NOT NULL AND replayed_as IS NOT NULL"); got != 20 {
		t.Errorf("dead letters marked replayed = %d, want 20", got)
	}

	// Replaying again is a no-op...
	if r, _ := h.replayer().Replay(context.Background(), dlq.Request{TenantID: h.tenant, IDs: dead}); r.Replayed != 0 {
		t.Errorf("third replay created %d deliveries", r.Replayed)
	}
	// ...and once the endpoint is fixed, every replay is delivered.
	h.rcv.SetSink("gone", receiver.Behavior{})
	h.startPool(poolOpts{workers: 8})
	h.waitDrained(10 * time.Second)
	if got := h.count("SELECT count(*) FROM deliveries_history WHERE final_state='succeeded' AND replay_of IS NOT NULL"); got != 20 {
		t.Errorf("succeeded replays = %d, want 20", got)
	}
}

// Replaying a backlog to an endpoint that just recovered must ramp, not arrive
// in one burst.
func TestReplayRampsAtTheConfiguredRate(t *testing.T) {
	h := newHarness(t)
	h.subscribe(h.endpoint("ramp", time.Second), false)
	h.rcv.SetSink("ramp", receiver.Behavior{FailRate: 1, FailStatus: 400})
	h.publishMany(100)
	stop := h.startPool(poolOpts{workers: 8})
	h.waitDrained(10 * time.Second)
	_ = stop(5 * time.Second)

	r := &dlq.Replayer{Store: h.store, RatePerEndpoint: 50}
	ep := uuid.UUID{}
	_ = h.store.Pool.QueryRow(context.Background(), "SELECT id FROM endpoints").Scan(&ep)
	res, err := r.Replay(context.Background(), dlq.Request{TenantID: h.tenant, EndpointID: &ep})
	if err != nil || res.Replayed != 100 {
		t.Fatalf("replay: %+v %v", res, err)
	}
	var spread float64
	_ = h.store.Pool.QueryRow(context.Background(),
		"SELECT EXTRACT(EPOCH FROM max(next_attempt_at) - min(next_attempt_at)) FROM deliveries").Scan(&spread)
	// 100 deliveries at 50/s: the last is due ~1.98s after the first.
	if spread < 1.9 || spread > 2.1 {
		t.Errorf("replays spread over %.2fs, want ~1.98s (100 at 50/s)", spread)
	}
}

// The breaker, end to end, with TWO dispatchers competing:
//
//	dead endpoint → circuit opens → its streams are parked (no HTTP, no
//	attempts while open) → exactly one probe at a time across both
//	dispatchers → endpoint recovers → probe succeeds → circuit closes →
//	backlog drains.
func TestBreakerParksStreamsProbesOnceAndRecovers(t *testing.T) {
	h := newHarness(t)
	h.subscribe(h.endpoint("down", time.Second), false)
	h.rcv.SetSink("down", receiver.Behavior{FailRate: 1, FailStatus: 503, LatencyMS: 5})
	h.publishMany(300)

	var m1, m2 *worker.Metrics
	h.startPool(poolOpts{workers: 16, metricsInto: &m1})
	h.startPool(poolOpts{workers: 16, metricsInto: &m2})

	h.waitFor("circuit to open", 10*time.Second, func() bool {
		return h.count("SELECT count(*) FROM endpoints WHERE circuit_state <> 'closed'") == 1
	})
	h.waitFor("streams to be parked", 10*time.Second, func() bool {
		return h.count("SELECT count(*) FROM delivery_streams WHERE state = 'circuit_open'") >= 8
	})

	// While open, the endpoint sees only probes: one request at a time,
	// never a burst. Measure over a window of several cooldowns.
	time.Sleep(200 * time.Millisecond) // let requests in flight at trip time finish
	h.rcv.ResetMaxConcurrent("down")
	before := len(h.rcv.Received())
	probesBefore := testutil.ToFloat64(m1.BreakerProbes) + testutil.ToFloat64(m2.BreakerProbes)
	time.Sleep(1500 * time.Millisecond)
	during := len(h.rcv.Received()) - before
	probes := testutil.ToFloat64(m1.BreakerProbes) + testutil.ToFloat64(m2.BreakerProbes) - probesBefore

	t.Logf("while open for 1.5s: %d requests reached the endpoint, %v probes started", during, probes)
	if probes < 1 {
		t.Fatal("no probe was started while the circuit was open")
	}
	if float64(during) > probes+1 {
		t.Errorf("%d requests reached a dead endpoint during %v probes: parked streams leaked traffic", during, probes)
	}
	if mc := h.rcv.MaxConcurrent("down"); mc > 1 {
		t.Errorf("%d concurrent requests while open: half-open must send exactly ONE probe, across all dispatchers", mc)
	}

	// The endpoint recovers. The next probe succeeds and the backlog drains.
	h.rcv.SetSink("down", receiver.Behavior{})
	h.waitDrained(30 * time.Second)
	if got := h.count("SELECT count(*) FROM endpoints WHERE circuit_state = 'closed'"); got != 1 {
		t.Error("circuit did not close after recovery")
	}
	closed := testutil.ToFloat64(m1.BreakerTransitions.WithLabelValues("closed")) +
		testutil.ToFloat64(m2.BreakerTransitions.WithLabelValues("closed"))
	if closed < 1 {
		t.Error("no 'closed' transition was recorded")
	}
	// Nothing was dead-lettered: an open circuit consumes no attempts.
	if got := h.count("SELECT count(*) FROM deliveries_history WHERE final_state = 'succeeded'"); got != 300 {
		t.Errorf("succeeded = %d of 300", got)
	}
}

func TestGoneEndpointIsAutoDisabled(t *testing.T) {
	h := newHarness(t)
	h.subscribe(h.endpoint("gone", time.Second), false)
	h.rcv.SetSink("gone", receiver.Behavior{FailRate: 1, FailStatus: 410})
	h.publish("k", 1)
	h.startPool(poolOpts{workers: 2})
	h.waitDrained(10 * time.Second)

	var state, reason string
	_ = h.store.Pool.QueryRow(context.Background(), "SELECT state, disabled_reason FROM endpoints").Scan(&state, &reason)
	if state != "auto_disabled" {
		t.Fatalf("endpoint state = %q after a 410, want auto_disabled", state)
	}
	t.Logf("disabled_reason: %s", reason)
	// A disabled endpoint gets no new deliveries at all.
	h.publish("k", 2)
	if got := h.count("SELECT count(*) FROM deliveries"); got != 0 {
		t.Errorf("fanout created %d deliveries for an auto-disabled endpoint", got)
	}
}

// The verification handshake: nothing is delivered to an endpoint until it
// proves it wants the traffic by echoing a signed challenge.
func TestEndpointVerificationHandshake(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	guard, _ := deliver.NewGuard([]string{"127.0.0.0/8"})
	v := verify.New(h.store, deliver.NewClient(guard), slog.New(slog.NewTextHandler(io.Discard, nil)), prometheus.NewRegistry())
	v.Schedule = []time.Duration{0, 0} // two attempts, no waiting

	// A server that answers 200 to everything, without reading the request:
	// exactly the kind of URL an attacker would register to aim traffic at.
	lazy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) }))
	defer lazy.Close()

	mk := func(url string) uuid.UUID {
		ep, err := h.store.CreateEndpoint(ctx, gen.CreateEndpointParams{
			ID: uuid.Must(uuid.NewV7()), TenantID: h.tenant, Url: url, State: "pending_verification",
			TimeoutMs: 2000, MaxConcurrency: 4, SigningKeyID: "k", SigningSecret: []byte("s"),
		})
		if err != nil {
			t.Fatal(err)
		}
		h.subscribe(ep.ID, false)
		return ep.ID
	}
	good := mk(h.srv.URL + "/sink/echo") // the receiver echoes challenges
	bad := mk(lazy.URL + "/hook")

	// Events published before verification go nowhere.
	h.publish("k", 1)
	if got := h.count("SELECT count(*) FROM deliveries"); got != 0 {
		t.Fatalf("%d deliveries were created for unverified endpoints", got)
	}

	v.Tick(ctx)
	v.Tick(ctx)
	state := func(id uuid.UUID) string {
		var s string
		_ = h.store.Pool.QueryRow(ctx, "SELECT state FROM endpoints WHERE id=$1", id).Scan(&s)
		return s
	}
	if s := state(good); s != "enabled" {
		t.Errorf("echoing endpoint: state %q, want enabled", s)
	}
	if s := state(bad); s != "verification_failed" {
		t.Errorf("endpoint that 200s without echoing: state %q, want verification_failed", s)
	}

	// Only the verified endpoint receives traffic from now on.
	h.publish("k", 2)
	if got := h.count("SELECT count(*) FROM deliveries WHERE endpoint_id=$1", good); got != 1 {
		t.Errorf("verified endpoint deliveries = %d, want 1", got)
	}
	if got := h.count("SELECT count(*) FROM deliveries WHERE endpoint_id=$1", bad); got != 0 {
		t.Errorf("unverified endpoint deliveries = %d, want 0", got)
	}
}

// A retryable failure on an ORDERED stream blocks the head but must not pause
// the stream: only death pauses.
func TestRetryOnOrderedStreamDoesNotPause(t *testing.T) {
	h := newHarness(t)
	h.subscribe(h.endpoint("blip", time.Second), true)
	h.rcv.SetSink("blip", receiver.Behavior{FailRequests: 2, FailStatus: 503})
	for i := range 3 {
		h.publish("k", i)
	}
	h.startPool(poolOpts{workers: 2, policy: retry.Policy{Base: time.Millisecond, Multiplier: 2, Cap: 5 * time.Millisecond, MaxAttempts: 10}})
	h.waitDrained(10 * time.Second)
	if got := h.receivedSeqs("blip"); len(got) != 3 || got[0] != 0 || got[2] != 2 {
		t.Errorf("received %v, want [0 1 2]", got)
	}
	if got := h.count("SELECT count(*) FROM delivery_streams WHERE state='paused'"); got != 0 {
		t.Error("a retryable failure paused the stream")
	}
}

// Defense in depth: a stream can be leased in the gap between its circuit
// opening and the breaker loop parking it. The worker must check the circuit
// it was handed at lease time and deliver nothing.
func TestOpenCircuitIsHonoredAtLeaseTime(t *testing.T) {
	h := newHarness(t)
	ep := h.endpoint("open", time.Second)
	h.subscribe(ep, false)
	h.publishMany(50) // runnable streams, NOT yet parked

	// Open the circuit directly, with no probe due for an hour.
	if _, err := h.store.Pool.Exec(context.Background(), `
		UPDATE endpoints SET circuit_state = 'open', circuit_cooldown_ms = 3600000,
		       circuit_next_probe_at = now() + interval '1 hour', circuit_opened_at = now()
		WHERE id = $1`, ep); err != nil {
		t.Fatal(err)
	}
	h.startPool(poolOpts{workers: 16})
	h.waitFor("every stream to be parked", 10*time.Second, func() bool {
		return h.count("SELECT count(*) FROM delivery_streams WHERE state IN ('ready','leased')") == 0
	})
	time.Sleep(200 * time.Millisecond)
	if got := len(h.rcv.Received()); got != 0 {
		t.Errorf("%d requests reached an endpoint whose circuit was open", got)
	}
	if got := h.count("SELECT count(*) FROM delivery_attempts"); got != 0 {
		t.Errorf("%d attempts recorded against an open circuit; parking must cost no attempts", got)
	}
}
