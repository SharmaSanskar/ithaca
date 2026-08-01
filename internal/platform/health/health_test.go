package health

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func newStarted() *Registry {
	r := New()
	r.MarkStarted()
	return r
}

func TestNotReadyBeforeStart(t *testing.T) {
	r := New()
	if got := r.Ready(context.Background()); got.Status != StatusDown {
		t.Errorf("Status = %q, want %q before MarkStarted", got.Status, StatusDown)
	}
}

func TestReadyWithNoChecks(t *testing.T) {
	r := newStarted()
	if got := r.Ready(context.Background()); got.Status != StatusUp {
		t.Errorf("Status = %q, want %q", got.Status, StatusUp)
	}
}

func TestCriticalCheckFailureMakesUnready(t *testing.T) {
	r := newStarted()
	r.Register("postgres", true, time.Second, func(context.Context) error {
		return errors.New("connection refused")
	})

	rep := r.Ready(context.Background())
	if rep.Status != StatusDown {
		t.Fatalf("Status = %q, want %q", rep.Status, StatusDown)
	}
	if rep.Reason != "postgres is down" {
		t.Errorf("Reason = %q, want %q", rep.Reason, "postgres is down")
	}
}

func TestNonCriticalCheckFailureIsReported0ButStaysReady(t *testing.T) {
	r := newStarted()
	r.Register("jaeger", false, time.Second, func(context.Context) error {
		return errors.New("collector unreachable")
	})

	rep := r.Ready(context.Background())
	if rep.Status != StatusUp {
		t.Fatalf("Status = %q, want %q — a non-critical dependency must not pull the pod out of rotation", rep.Status, StatusUp)
	}
	if len(rep.Checks) != 1 || rep.Checks[0].Status != StatusDown {
		t.Errorf("the failure should still be visible in the report: %+v", rep.Checks)
	}
}

func TestCheckTimeoutIsEnforced(t *testing.T) {
	r := newStarted()
	r.Register("slow", true, 20*time.Millisecond, func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})

	start := time.Now()
	rep := r.Ready(context.Background())
	elapsed := time.Since(start)

	if rep.Status != StatusDown {
		t.Errorf("Status = %q, want %q", rep.Status, StatusDown)
	}
	// A hung dependency must not hang the probe; the kubelet has its own
	// timeout and a probe that never answers looks identical to a dead pod.
	if elapsed > time.Second {
		t.Errorf("Ready took %v, want the per-check timeout to bound it", elapsed)
	}
}

func TestShutdownMakesUnreadyImmediately(t *testing.T) {
	r := newStarted()
	// Prime the cache with a healthy result...
	if got := r.Ready(context.Background()); got.Status != StatusUp {
		t.Fatalf("precondition failed: %q", got.Status)
	}
	// ...then flip. This must take effect at once, not after the cache TTL:
	// the whole point is to fail the probe before the listener stops.
	r.MarkShuttingDown()

	rep := r.Ready(context.Background())
	if rep.Status != StatusDown {
		t.Fatalf("Status = %q, want %q", rep.Status, StatusDown)
	}
	if rep.Reason != ErrShuttingDown.Error() {
		t.Errorf("Reason = %q, want %q", rep.Reason, ErrShuttingDown.Error())
	}
}

func TestResultsAreCachedToProtectDependencies(t *testing.T) {
	r := newStarted()
	var calls atomic.Int64
	r.Register("db", true, time.Second, func(context.Context) error {
		calls.Add(1)
		return nil
	})

	for range 10 {
		r.Ready(context.Background())
	}

	// The kubelet probes every few seconds, per pod. Without caching, a
	// readiness endpoint becomes a self-inflicted load generator.
	if n := calls.Load(); n != 1 {
		t.Errorf("check ran %d times across 10 probes, want 1 (cached)", n)
	}
}

func TestLivenessNeverConsultsDependencies(t *testing.T) {
	// This is the guard for the single most common self-inflicted Kubernetes
	// outage: a liveness probe that pings the database, so one RDS blip
	// restarts every pod in the fleet at the same moment.
	r := newStarted()
	var called atomic.Bool
	r.Register("postgres", true, time.Second, func(context.Context) error {
		called.Store(true)
		return errors.New("database is on fire")
	})

	rec := httptest.NewRecorder()
	r.LiveHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if rec.Code != http.StatusOK {
		t.Errorf("liveness returned %d with a failing dependency; it must only report process health", rec.Code)
	}
	if called.Load() {
		t.Error("liveness ran a dependency check — it must never do that")
	}
}

func TestReadyHandlerStatusCodes(t *testing.T) {
	tests := []struct {
		name     string
		setup    func(*Registry)
		wantCode int
	}{
		{
			name:     "healthy",
			setup:    func(r *Registry) { r.MarkStarted() },
			wantCode: http.StatusOK,
		},
		{
			name:     "still starting",
			setup:    func(*Registry) {},
			wantCode: http.StatusServiceUnavailable,
		},
		{
			name: "critical dependency down",
			setup: func(r *Registry) {
				r.MarkStarted()
				r.Register("db", true, time.Second, func(context.Context) error { return errors.New("nope") })
			},
			wantCode: http.StatusServiceUnavailable,
		},
		{
			name: "shutting down",
			setup: func(r *Registry) {
				r.MarkStarted()
				r.MarkShuttingDown()
			},
			wantCode: http.StatusServiceUnavailable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := New()
			tt.setup(r)

			rec := httptest.NewRecorder()
			r.ReadyHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))

			if rec.Code != tt.wantCode {
				t.Errorf("status = %d, want %d (body: %s)", rec.Code, tt.wantCode, rec.Body.String())
			}
			var rep Report
			if err := json.Unmarshal(rec.Body.Bytes(), &rep); err != nil {
				t.Errorf("body is not valid JSON: %v", err)
			}
		})
	}
}

func TestChecksRunConcurrently(t *testing.T) {
	r := newStarted()
	const n = 8
	const each = 50 * time.Millisecond
	for i := range n {
		r.Register(string(rune('a'+i)), true, time.Second, func(context.Context) error {
			time.Sleep(each)
			return nil
		})
	}

	start := time.Now()
	r.Ready(context.Background())
	elapsed := time.Since(start)

	// Serially this would be 400ms. Allow generous slack for a loaded CI box
	// while still failing if the checks were run one after another.
	if elapsed > n*each/2 {
		t.Errorf("Ready took %v for %d checks of %v each; they should run concurrently", elapsed, n, each)
	}
}
