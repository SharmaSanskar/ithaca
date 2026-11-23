//go:build integration

package worker_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/SharmaSanskar/ithaca/internal/auth"
	"github.com/SharmaSanskar/ithaca/internal/breaker"
	"github.com/SharmaSanskar/ithaca/internal/deliver"
	"github.com/SharmaSanskar/ithaca/internal/ingest"
	"github.com/SharmaSanskar/ithaca/internal/platform/clock"
	"github.com/SharmaSanskar/ithaca/internal/queue"
	"github.com/SharmaSanskar/ithaca/internal/queue/pg"
	"github.com/SharmaSanskar/ithaca/internal/receiver"
	"github.com/SharmaSanskar/ithaca/internal/retry"
	"github.com/SharmaSanskar/ithaca/internal/store"
	"github.com/SharmaSanskar/ithaca/internal/store/gen"
	"github.com/SharmaSanskar/ithaca/internal/testdb"
	"github.com/SharmaSanskar/ithaca/internal/worker"
)

// ── harness ──────────────────────────────────────────────────────────────────

var testBreaker = breaker.Policy{Threshold: 5, BaseCooldown: 50 * time.Millisecond, MaxCooldown: 400 * time.Millisecond}

type harness struct {
	t      *testing.T
	store  *store.Store
	q      *pg.Driver
	rcv    *receiver.Receiver
	srv    *httptest.Server
	tenant uuid.UUID
	ingest *ingest.Service
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	s := testdb.New(t)
	p, err := auth.ProvisionTenant(context.Background(), s, "test")
	if err != nil {
		t.Fatal(err)
	}
	rcv := receiver.New(42, true)
	srv := httptest.NewServer(rcv.Handler())
	t.Cleanup(srv.Close)
	// A breaker with test-speed cooldowns: real enough that flaky receivers
	// trip it (so parking and probing run under every test), fast enough not
	// to dominate test time.
	q := pg.New(s, pg.WithBreaker(testBreaker), pg.WithUnparkSpread(20*time.Millisecond))
	return &harness{t: t, store: s, q: q, rcv: rcv, srv: srv, tenant: p.TenantID, ingest: ingest.NewService(s, nil)}
}

// endpoint registers a sink with a known secret, so the receiver verifies
// every signature it sees.
func (h *harness) endpoint(sink string, timeout time.Duration) uuid.UUID {
	h.t.Helper()
	secret := []byte("secret-" + sink)
	ep, err := h.store.CreateEndpoint(context.Background(), gen.CreateEndpointParams{
		State: "enabled", // already verified: these tests are not about the handshake
		ID:    uuid.Must(uuid.NewV7()), TenantID: h.tenant, Url: h.srv.URL + "/sink/" + sink,
		TimeoutMs: int32(timeout.Milliseconds()), MaxConcurrency: 8, SigningKeyID: "k_" + sink, SigningSecret: secret,
	})
	if err != nil {
		h.t.Fatal(err)
	}
	h.rcv.SetSecret(sink, secret)
	return ep.ID
}

func (h *harness) subscribe(ep uuid.UUID, ordered bool) {
	h.t.Helper()
	if _, err := h.store.CreateSubscription(context.Background(), gen.CreateSubscriptionParams{
		ID: uuid.Must(uuid.NewV7()), TenantID: h.tenant, EndpointID: ep, EventTypes: []string{"*"},
		Ordered: ordered, LaneCount: 16,
	}); err != nil {
		h.t.Fatal(err)
	}
}

type payload struct {
	Key string `json:"key"`
	Seq int    `json:"seq"`
}

func (h *harness) publish(key string, seq int) {
	h.t.Helper()
	b, _ := json.Marshal(map[string]any{"type": "test.event", "ordering_key": key, "payload": payload{key, seq}})
	if _, err := h.ingest.Ingest(context.Background(), ingest.Request{TenantID: h.tenant, Method: "POST", Path: "/v1/events", Body: b}); err != nil {
		h.t.Fatal(err)
	}
}

// publishMany ingests concurrently: 10k sequential round trips would dominate
// the test's runtime.
func (h *harness) publishMany(n int) {
	h.t.Helper()
	var wg sync.WaitGroup
	sem := make(chan struct{}, 16)
	for i := range n {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			h.publish(fmt.Sprintf("k%d", i%64), i)
		}()
	}
	wg.Wait()
}

func (h *harness) count(q string, args ...any) int {
	h.t.Helper()
	var n int
	if err := h.store.Pool.QueryRow(context.Background(), q, args...).Scan(&n); err != nil {
		h.t.Fatalf("%s: %v", q, err)
	}
	return n
}

type poolOpts struct {
	workers     int
	policy      retry.Policy
	minLease    time.Duration
	padding     time.Duration
	q           queue.Queue
	metricsInto **worker.Metrics
}

// startPool runs a pool plus a fast reaper; the returned func shuts both down.
func (h *harness) startPool(o poolOpts) (stop func(grace time.Duration) error) {
	h.t.Helper()
	if o.workers == 0 {
		o.workers = 32
	}
	if o.policy.MaxAttempts == 0 {
		o.policy = retry.Policy{Base: 2 * time.Millisecond, Multiplier: 2, Cap: 50 * time.Millisecond, MaxAttempts: 1000}
	}
	if o.minLease == 0 {
		o.minLease = 5 * time.Second
	}
	var q queue.Queue = h.q
	if o.q != nil {
		q = o.q
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	guard, _ := deliver.NewGuard([]string{"127.0.0.0/8", "::1/128"})
	slots := worker.NewSlots(o.workers)
	m := worker.NewMetrics(prometheus.NewRegistry(), slots)
	if o.metricsInto != nil {
		*o.metricsInto = m
	}
	p := worker.New(worker.Config{
		WorkerID: "w-" + uuid.NewString()[:8], Workers: o.workers, MaxStreamsBatch: 16, DrainBatch: 64,
		MaxDrainRounds: 4, MinLease: o.minLease, LeasePadding: o.padding,
		PollFloor: 5 * time.Millisecond, PollCeiling: 50 * time.Millisecond, DisabledRecheck: time.Minute,
	}, q, worker.NewExecutor(deliver.NewClient(guard), o.policy, m), slots, log, m, clock.Real{})

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- p.Run(ctx) }()
	reaper := &worker.Reaper{Q: h.q, Every: 100 * time.Millisecond, Log: log, M: m}
	bl := &worker.BreakerLoop{B: h.q, Every: 20 * time.Millisecond, Log: log, M: m}
	reaperDone := make(chan struct{})
	go func() {
		var wg sync.WaitGroup
		wg.Go(func() { _ = reaper.Run(ctx) })
		wg.Go(func() { _ = bl.Run(ctx) })
		wg.Wait()
		close(reaperDone)
	}()

	var once sync.Once
	stop = func(grace time.Duration) error {
		var err error
		once.Do(func() {
			sctx, scancel := context.WithTimeout(context.Background(), grace)
			defer scancel()
			err = p.Shutdown(sctx)
			cancel()
			<-runDone
			<-reaperDone
		})
		return err
	}
	h.t.Cleanup(func() { _ = stop(10 * time.Second) })
	return stop
}

// waitDrained waits until no delivery is pending and no stream is leased.
func (h *harness) waitDrained(timeout time.Duration) {
	h.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if h.count("SELECT count(*) FROM deliveries") == 0 &&
			h.count("SELECT count(*) FROM delivery_streams WHERE state = 'leased'") == 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	h.t.Fatalf("not drained after %s: %d pending, %d leased", timeout,
		h.count("SELECT count(*) FROM deliveries"),
		h.count("SELECT count(*) FROM delivery_streams WHERE state = 'leased'"))
}

// assertNoLossNoPhantomAcks checks the two invariants that matter most.
//
//	NO LOSS:         every (event × subscription) pair reached history.
//	NO PHANTOM ACKS: every delivery marked succeeded was actually received
//	                 with a 2xx. This catches a premature ack — the worst bug
//	                 this system can have, and one no unit test finds.
func (h *harness) assertNoLossNoPhantomAcks(expected int) {
	h.t.Helper()
	if got := h.count("SELECT count(*) FROM deliveries_history"); got != expected {
		h.t.Errorf("NO LOSS violated: history has %d deliveries, want %d", got, expected)
	}
	rows, err := h.store.Pool.Query(context.Background(),
		"SELECT id FROM deliveries_history WHERE final_state = 'succeeded'")
	if err != nil {
		h.t.Fatal(err)
	}
	defer rows.Close()
	seen := h.rcv.Delivered()
	for rows.Next() {
		var id uuid.UUID
		_ = rows.Scan(&id)
		if seen[id.String()] == 0 {
			h.t.Fatalf("PHANTOM ACK: delivery %s marked succeeded but the receiver never returned 2xx for it", id)
		}
	}
	for _, r := range h.rcv.Received() {
		if r.SignatureValid != nil && !*r.SignatureValid {
			h.t.Fatalf("delivery %s arrived with an invalid signature", r.DeliveryID)
		}
	}
}

// ── tests ────────────────────────────────────────────────────────────────────

// Phase 2 gate: a burst of 10k events fully delivers.
func TestBurstOf10kEventsFullyDelivers(t *testing.T) {
	if testing.Short() {
		t.Skip("slow")
	}
	h := newHarness(t)
	h.subscribe(h.endpoint("burst", 5*time.Second), false)

	const n = 10_000
	start := time.Now()
	h.publishMany(n)
	t.Logf("ingested %d events in %s", n, time.Since(start).Round(time.Millisecond))

	start = time.Now()
	h.startPool(poolOpts{workers: 32})
	h.waitDrained(2 * time.Minute)
	took := time.Since(start)
	t.Logf("delivered %d in %s (%.0f deliveries/s, 32 workers, -race)", n, took.Round(time.Millisecond), n/took.Seconds())

	h.assertNoLossNoPhantomAcks(n)
	if got := h.count("SELECT count(*) FROM deliveries_history WHERE final_state = 'succeeded'"); got != n {
		t.Errorf("succeeded = %d, want %d", got, n)
	}
	// The payload on the wire is the exact stored bytes.
	for _, r := range h.rcv.Received()[:10] {
		var p payload
		if err := json.Unmarshal(r.Body, &p); err != nil || p.Key == "" {
			t.Fatalf("receiver got a body that is not the event payload: %s", r.Body)
		}
	}
}

// The ordering guarantee, under the conditions where it breaks if it is
// wrong: 32 workers (concurrency is the thing under test), 30% induced
// failures (retries are where reordering hides) and 8 keys (lanes must be
// independent). The assertion is monotonic NON-DECREASING per key, not
// equality with the input: duplicates are legal under at-least-once,
// regressions are not.
func TestOrderedStreamNeverReorders(t *testing.T) {
	h := newHarness(t)
	h.subscribe(h.endpoint("ordered", 5*time.Second), true)
	h.rcv.SetSink("ordered", receiver.Behavior{FailRate: 0.30, LatencyMS: 1})

	const keys, perKey = 8, 200
	for i := range perKey {
		for k := range keys {
			h.publish(fmt.Sprintf("key-%d", k), i)
		}
	}

	h.startPool(poolOpts{workers: 32})
	h.waitDrained(2 * time.Minute)

	order := map[string][]int{}
	for _, r := range h.rcv.Received() {
		if r.Status != 200 {
			continue
		}
		var p payload
		if err := json.Unmarshal(r.Body, &p); err != nil {
			t.Fatal(err)
		}
		order[p.Key] = append(order[p.Key], p.Seq)
	}
	failures := 0
	for _, r := range h.rcv.Received() {
		if r.Status != 200 {
			failures++
		}
	}
	t.Logf("%d failed attempts were retried", failures)
	if failures < keys*perKey/10 {
		t.Fatalf("only %d induced failures; the test is not exercising retries", failures)
	}

	for k := range keys {
		key := fmt.Sprintf("key-%d", k)
		seen := order[key]
		maxSeen := -1
		for _, s := range seen {
			if s < maxSeen {
				t.Fatalf("%s: REORDERED — seq %d delivered after %d", key, s, maxSeen)
			}
			maxSeen = s
		}
		if maxSeen != perKey-1 {
			t.Fatalf("%s: incomplete, highest seq delivered = %d", key, maxSeen)
		}
	}
	h.assertNoLossNoPhantomAcks(keys * perKey)
}

// killableQueue simulates a process being SIGKILLed: after kill(), every call
// fails, so nothing is acked, released or renewed. Its leases are left
// dangling in the database exactly as a dead process would leave them.
type killableQueue struct {
	queue.Queue
	dead atomic.Bool
}

var errKilled = errors.New("process killed")

func (k *killableQueue) Lease(ctx context.Context, r queue.LeaseRequest) ([]*queue.Lease, error) {
	if k.dead.Load() {
		return nil, errKilled
	}
	return k.Queue.Lease(ctx, r)
}

func (k *killableQueue) Fetch(ctx context.Context, l *queue.Lease, n int) ([]queue.Message, error) {
	if k.dead.Load() {
		return nil, errKilled
	}
	return k.Queue.Fetch(ctx, l, n)
}

func (k *killableQueue) Renew(ctx context.Context, l *queue.Lease) error {
	if k.dead.Load() {
		return errKilled
	}
	return k.Queue.Renew(ctx, l)
}

func (k *killableQueue) Ack(ctx context.Context, l *queue.Lease, rs []queue.Result) (queue.AckOutcome, error) {
	if k.dead.Load() {
		return queue.AckOutcome{}, errKilled
	}
	return k.Queue.Ack(ctx, l, rs)
}

func (k *killableQueue) Release(ctx context.Context, l *queue.Lease, d time.Duration) error {
	if k.dead.Load() {
		return errKilled
	}
	return k.Queue.Release(ctx, l, d)
}

// Phase 2 gate: kill -9 a dispatcher mid-flight and every event still arrives.
func TestKilledDispatcherLosesNothing(t *testing.T) {
	h := newHarness(t)
	h.subscribe(h.endpoint("crash", time.Second), false)
	// Slow enough that A is guaranteed to be mid-flight, holding leases with
	// requests on the wire, at the moment it dies.
	h.rcv.SetSink("crash", receiver.Behavior{LatencyMS: 150})
	const n = 1500
	h.publishMany(n)

	// Dispatcher A: 2-second leases (2 × 1s timeout, no padding).
	victim := &killableQueue{Queue: h.q}
	stopA := h.startPool(poolOpts{workers: 16, q: victim, minLease: time.Second})
	for len(h.rcv.Received()) < 200 { // well into the run, nowhere near the end
		time.Sleep(10 * time.Millisecond)
	}
	victim.dead.Store(true) // SIGKILL: from now on it can touch nothing
	_ = stopA(time.Millisecond)
	leased := h.count("SELECT count(*) FROM delivery_streams WHERE state = 'leased'")
	t.Logf("dispatcher A killed holding %d leases", leased)
	if leased == 0 {
		t.Fatal("A held no leases when killed; the test did not hit the crash window")
	}

	// Dispatcher B must pick everything up, including A's orphaned streams
	// once the reaper reclaims their expired leases.
	var mB *worker.Metrics
	h.startPool(poolOpts{workers: 16, minLease: time.Second, metricsInto: &mB})
	h.waitDrained(time.Minute)

	h.assertNoLossNoPhantomAcks(n)
	if got := testutil.ToFloat64(mB.Reaped); got == 0 {
		t.Error("the reaper reclaimed nothing; A's leases were not exercised")
	}
	// At-least-once is not a license for unbounded duplication. The crash
	// window is one ack chunk per lease A held (max_concurrency = 8), so the
	// duplicates must stay within that bound.
	dupes := 0
	for _, c := range h.rcv.Delivered() {
		dupes += c - 1
	}
	t.Logf("duplicates from the crash: %d of %d (%.1f%%); bound = %d leases × 8",
		dupes, n, 100*float64(dupes)/n, leased)
	if dupes > leased*8 {
		t.Errorf("%d duplicates exceeds the ack-window bound of %d: acks are batched too widely", dupes, leased*8)
	}
}

// Fencing: a zombie whose lease was reaped and re-leased cannot ack or release.
func TestZombieWorkerIsFencedOut(t *testing.T) {
	h := newHarness(t)
	h.subscribe(h.endpoint("fence", time.Second), true)
	h.publish("k", 1)
	ctx := context.Background()

	zombie, err := h.q.Lease(ctx, queue.LeaseRequest{WorkerID: "zombie", Max: 1, MinLease: time.Second})
	if err != nil || len(zombie) != 1 {
		t.Fatalf("lease: %v %d", err, len(zombie))
	}
	msgs, _ := h.q.Fetch(ctx, zombie[0], 10)

	// The zombie stalls past its lease; the reaper hands the stream on.
	if _, err := h.store.Pool.Exec(ctx, "UPDATE delivery_streams SET locked_until = now() - interval '1 second'"); err != nil {
		t.Fatal(err)
	}
	if n, _ := h.q.Reap(ctx, 10); n != 1 {
		t.Fatalf("reaped %d, want 1", n)
	}
	fresh, _ := h.q.Lease(ctx, queue.LeaseRequest{WorkerID: "fresh", Max: 1, MinLease: time.Minute})
	if len(fresh) != 1 {
		t.Fatal("the reaped stream was not leasable")
	}

	// The zombie wakes up and tries to finish its work.
	res := []queue.Result{{DeliveryID: msgs[0].DeliveryID, Disposition: queue.Succeeded, Attempts: 1}}
	if _, err := h.q.Ack(ctx, zombie[0], res); !errors.Is(err, queue.ErrLeaseLost) {
		t.Fatalf("zombie ack: %v, want ErrLeaseLost", err)
	}
	if err := h.q.Release(ctx, zombie[0], 0); !errors.Is(err, queue.ErrLeaseLost) {
		t.Fatalf("zombie release: %v, want ErrLeaseLost", err)
	}
	if err := h.q.Renew(ctx, zombie[0]); !errors.Is(err, queue.ErrLeaseLost) {
		t.Fatalf("zombie renew: %v, want ErrLeaseLost", err)
	}
	if got := h.count("SELECT count(*) FROM deliveries"); got != 1 {
		t.Errorf("the zombie's ack changed state: %d pending, want 1", got)
	}
	var holder string
	_ = h.store.Pool.QueryRow(ctx, "SELECT locked_by FROM delivery_streams").Scan(&holder)
	if holder != "fresh" {
		t.Errorf("stream held by %q after the zombie's attempts, want fresh", holder)
	}
	// And the rightful holder can still ack.
	if _, err := h.q.Ack(ctx, fresh[0], res); err != nil {
		t.Errorf("rightful ack: %v", err)
	}
}

// Phase 2 gate: the lease renews during a long drain. Five 900ms deliveries on
// one ordered stream take ~4.5s against a 2s lease, with a reaper sweeping
// every 100ms. Without heartbeats the reaper would steal the stream mid-drain
// and the events would be delivered twice.
func TestLeaseRenewsDuringALongDrain(t *testing.T) {
	h := newHarness(t)
	h.subscribe(h.endpoint("slow", time.Second), true) // lease = 2 × 1s timeout
	h.rcv.SetSink("slow", receiver.Behavior{LatencyMS: 900})
	for i := range 5 {
		h.publish("k", i)
	}

	var m *worker.Metrics
	h.startPool(poolOpts{workers: 4, minLease: time.Millisecond, metricsInto: &m})
	h.waitDrained(30 * time.Second)

	if got := len(h.rcv.Received()); got != 5 {
		t.Errorf("receiver saw %d requests for 5 events: the lease was lost mid-drain", got)
	}
	if testutil.ToFloat64(m.Reaped) != 0 || testutil.ToFloat64(m.LeasesLost) != 0 {
		t.Error("a lease was reaped or lost despite heartbeats")
	}
	h.assertNoLossNoPhantomAcks(5)
}

// Phase 2 gate: shutdown cancels a hung request instead of waiting for it.
func TestShutdownCancelsAHungDelivery(t *testing.T) {
	h := newHarness(t)
	h.subscribe(h.endpoint("hang", 60*time.Second), false)
	h.rcv.SetSink("hang", receiver.Behavior{Hang: true})
	h.publish("k", 1)

	stop := h.startPool(poolOpts{workers: 2})
	deadline := time.Now().Add(10 * time.Second)
	for len(h.rcv.Received()) == 0 && h.count("SELECT count(*) FROM delivery_streams WHERE state='leased'") == 0 {
		if time.Now().After(deadline) {
			t.Fatal("delivery never started")
		}
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)

	start := time.Now()
	if err := stop(4 * time.Second); err != nil { // hard cut at 4s − 3s margin = 1s
		t.Fatalf("shutdown: %v", err)
	}
	if took := time.Since(start); took > 3*time.Second {
		t.Errorf("shutdown took %s with a hung request; the hard cut did not cancel it", took)
	}
	if got := h.count("SELECT count(*) FROM delivery_attempts"); got != 0 {
		t.Errorf("an abandoned attempt was recorded (%d rows): our own cancellation is not the endpoint's fault", got)
	}
	if got := h.count("SELECT attempt FROM deliveries"); got != 0 {
		t.Errorf("attempt count = %d, want 0", got)
	}
	if got := h.count("SELECT count(*) FROM delivery_streams WHERE state = 'leased'"); got != 0 {
		t.Error("the stream was left leased instead of released for immediate redelivery")
	}
}

func TestRateLimitingDoesNotConsumeAttempts(t *testing.T) {
	h := newHarness(t)
	h.subscribe(h.endpoint("limited", time.Second), false)
	h.rcv.SetSink("limited", receiver.Behavior{FailFirst: 2, FailStatus: 429, RetryAfter: 1})
	h.publish("k", 1)

	h.startPool(poolOpts{workers: 2, policy: retry.Policy{Base: time.Millisecond, Multiplier: 2, Cap: time.Millisecond, MaxAttempts: 2}})
	h.waitDrained(20 * time.Second)

	// MaxAttempts is 2, yet after two 429s the delivery still succeeded: the
	// 429s did not spend the budget.
	if got := h.count("SELECT count(*) FROM deliveries_history WHERE final_state = 'succeeded' AND attempt = 1"); got != 1 {
		t.Errorf("expected one success recorded at attempt 1, got %d", got)
	}
	if got := h.count("SELECT count(*) FROM delivery_attempts WHERE outcome = 'rate_limited'"); got != 2 {
		t.Errorf("rate_limited attempt rows = %d, want 2", got)
	}
}

func TestFailuresDeadLetterWithReasons(t *testing.T) {
	h := newHarness(t)
	h.subscribe(h.endpoint("gone", time.Second), false)
	h.subscribe(h.endpoint("flaky", time.Second), false)
	h.rcv.SetSink("gone", receiver.Behavior{FailRate: 1, FailStatus: 404})
	h.rcv.SetSink("flaky", receiver.Behavior{FailRate: 1, FailStatus: 503})
	h.publish("k", 1)

	h.startPool(poolOpts{workers: 4, policy: retry.Policy{Base: time.Millisecond, Multiplier: 2, Cap: 5 * time.Millisecond, MaxAttempts: 3}})
	h.waitDrained(20 * time.Second)

	var reasons []string
	rows, _ := h.store.Pool.Query(context.Background(), "SELECT dead_reason || '@' || attempt FROM deliveries_history WHERE final_state='dead' ORDER BY 1")
	for rows.Next() {
		var r string
		_ = rows.Scan(&r)
		reasons = append(reasons, r)
	}
	rows.Close()
	want := []string{"http_4xx@1", "max_attempts:http_5xx@3"}
	if fmt.Sprint(reasons) != fmt.Sprint(want) {
		t.Errorf("dead letters = %v, want %v (404 dies at once; 503 after the budget)", reasons, want)
	}
}
