//go:build integration

package demo_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/SharmaSanskar/ithaca/internal/breaker"
	"github.com/SharmaSanskar/ithaca/internal/deliver"
	"github.com/SharmaSanskar/ithaca/internal/demo"
	"github.com/SharmaSanskar/ithaca/internal/ingest"
	"github.com/SharmaSanskar/ithaca/internal/live"
	"github.com/SharmaSanskar/ithaca/internal/platform/clock"
	"github.com/SharmaSanskar/ithaca/internal/queue/pg"
	"github.com/SharmaSanskar/ithaca/internal/receiver"
	"github.com/SharmaSanskar/ithaca/internal/retry"
	"github.com/SharmaSanskar/ithaca/internal/testdb"
	"github.com/SharmaSanskar/ithaca/internal/testredis"
	"github.com/SharmaSanskar/ithaca/internal/verify"
	"github.com/SharmaSanskar/ithaca/internal/worker"
)

// The demo, end to end: seed → real verification → ambient traffic → four
// scenarios driven over the real Redis command channel, against the real
// dispatcher and real HTTP sinks. Each scenario must reach the end of its
// script; the narration (read off the live bus, exactly as the dashboard
// receives it) is the assertion surface.
func TestDemoScenariosRunEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("about a minute")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	s := testdb.New(t)
	rdb := testredis.New(t)

	rcv := receiver.New(7, false)
	sinks := httptest.NewServer(rcv.Handler())
	defer sinks.Close()

	res, err := demo.Seed(ctx, s, sinks.URL)
	if err != nil || !res.Created {
		t.Fatalf("seed: %+v %v", res, err)
	}
	if again, _ := demo.Seed(ctx, s, sinks.URL); again.Created {
		t.Fatal("seed is not idempotent")
	}

	// Verification: the seeded endpoints start pending; the receiver echoes.
	guard, _ := deliver.NewGuard([]string{"127.0.0.0/8"})
	client := deliver.NewClient(guard)
	v := verify.New(s, client, log, prometheus.NewRegistry())
	v.Tick(ctx)
	var enabled int
	_ = s.Pool.QueryRow(ctx, "SELECT count(*) FROM endpoints WHERE state='enabled'").Scan(&enabled)
	if enabled != len(demo.Sinks) {
		t.Fatalf("%d of %d demo endpoints verified", enabled, len(demo.Sinks))
	}

	// The delivery engine, with demo-speed policies.
	q := pg.New(s, pg.WithBreaker(breaker.Policy{Threshold: 10, BaseCooldown: time.Second, MaxCooldown: 2 * time.Second}),
		pg.WithUnparkSpread(200*time.Millisecond))
	slots := worker.NewSlots(32)
	m := worker.NewMetrics(prometheus.NewRegistry(), slots)
	pool := worker.New(worker.Config{WorkerID: "w", Workers: 32, MaxStreamsBatch: 16, DrainBatch: 32, MaxDrainRounds: 4,
		MinLease: 10 * time.Second, PollFloor: 5 * time.Millisecond, PollCeiling: 50 * time.Millisecond, DisabledRecheck: time.Minute},
		q, worker.NewExecutor(client, retry.Policy{Base: 100 * time.Millisecond, Multiplier: 2, Cap: time.Second, MaxAttempts: 6}, m),
		slots, log, m, clock.Real{})
	bus := live.NewRedisBus(rdb)
	pub := live.NewPublisher(bus, log, prometheus.NewRegistry())
	pool.SetObserver(live.DispatchObserver{P: pub})
	go func() { _ = pub.Run(ctx) }()
	go func() { _ = pool.Run(ctx) }()
	go func() { _ = (&worker.BreakerLoop{B: q, Every: 100 * time.Millisecond, Log: log, M: m}).Run(ctx) }()
	go func() { _ = (&worker.Reaper{Q: q, Every: time.Second, Log: log, M: m}).Run(ctx) }()

	// Narration, read off the live bus.
	var mu sync.Mutex
	var lines []live.Narration
	go func() {
		for b := range bus.Subscribe(ctx) {
			var e live.Envelope
			if json.Unmarshal(b, &e) == nil && e.Kind == "narration" && e.Tenant == res.TenantID {
				mu.Lock()
				lines = append(lines, *e.Narration)
				mu.Unlock()
			}
		}
	}()

	svc := ingest.NewService(s, nil)
	svc.SetObserver(live.IngestObserver{P: pub})
	rt := &demo.Runtime{Store: s, Redis: rdb, Publisher: pub, Ingest: svc, SinkBaseURL: sinks.URL,
		AmbientRate: 10, ReplayRate: 200, Log: log}
	go func() { _ = rt.Run(ctx) }()

	// Wait for the runtime to become leader (it publishes state).
	deadline := time.Now().Add(15 * time.Second)
	for rdb.Exists(ctx, demo.StateKey).Val() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("demo runtime never became leader")
		}
		time.Sleep(100 * time.Millisecond)
	}

	run := func(name string, timeout time.Duration) []live.Narration {
		t.Helper()
		mu.Lock()
		start := len(lines)
		mu.Unlock()
		cmd, _ := json.Marshal(demo.Command{Op: "scenario", Name: name})
		if n := rdb.Publish(ctx, demo.CommandChannel, cmd).Val(); n == 0 {
			t.Fatal("no runtime subscribed to the command channel")
		}
		deadline := time.Now().Add(timeout)
		for time.Now().Before(deadline) {
			mu.Lock()
			got := append([]live.Narration(nil), lines[start:]...)
			mu.Unlock()
			for _, l := range got {
				if l.Scenario == name && l.Done {
					for _, x := range got {
						t.Logf("  [%s %d/%d %s] %s", x.Scenario, x.Step, x.Total, x.Level, x.Text)
					}
					if l.Level != "success" {
						t.Fatalf("%s ended with %q: %s", name, l.Level, l.Text)
					}
					return got
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatalf("%s did not finish within %s", name, timeout)
		return nil
	}

	run("happy_path", 40*time.Second)
	run("endpoint_down", 60*time.Second)
	var open string
	_ = s.Pool.QueryRow(ctx, "SELECT circuit_state FROM endpoints WHERE description='billing'").Scan(&open)
	if open == "closed" {
		t.Fatal("endpoint_down finished but billing's circuit is closed")
	}

	run("bad_deploy", 60*time.Second)
	var dlq int
	_ = s.Pool.QueryRow(ctx, "SELECT count(*) FROM deliveries_history WHERE final_state='dead' AND replayed_at IS NULL").Scan(&dlq)
	if dlq == 0 {
		t.Fatal("bad_deploy finished with an empty dead-letter queue")
	}

	recovery := run("recovery", 120*time.Second)
	if !strings.Contains(recovery[len(recovery)-2].Text, "✓ Queue depth stable") {
		t.Errorf("recovery's closing line = %q", recovery[len(recovery)-2].Text)
	}
	var notClosed, openDLQ int
	_ = s.Pool.QueryRow(ctx, "SELECT count(*) FROM endpoints WHERE circuit_state <> 'closed'").Scan(&notClosed)
	_ = s.Pool.QueryRow(ctx, "SELECT count(*) FROM deliveries_history WHERE final_state='dead' AND replayed_at IS NULL").Scan(&openDLQ)
	if notClosed != 0 || openDLQ != 0 {
		t.Errorf("after recovery: %d circuits not closed, %d dead letters not replayed", notClosed, openDLQ)
	}
}
