// Package health implements the liveness / readiness split.
//
// The distinction matters more than it looks, and getting it wrong is the most
// common self-inflicted Kubernetes outage there is:
//
//	LIVENESS  answers "is this process wedged?" — and NOTHING ELSE. It must
//	          never touch Postgres, Redis, or any other dependency. If it did,
//	          a 30-second RDS blip would fail the probe on every pod at once,
//	          the kubelet would restart all of them simultaneously, and a
//	          non-event would become a real outage with cold connection pools
//	          and a thundering herd on recovery.
//
//	READINESS answers "should traffic be routed here right now?" — and that
//	          legitimately depends on dependencies. A pod that cannot reach
//	          Postgres should be pulled from the Service endpoints, not killed.
//
//	STARTUP   answers "has boot finished?" — it exists so slow starts
//	          (migrations, cache warm) do not trip an aggressive liveness probe.
package health

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// CheckFunc reports whether one dependency is usable. It must respect ctx and
// return quickly; the registry applies its own timeout regardless.
type CheckFunc func(ctx context.Context) error

// Status is the outcome of a check or of the registry as a whole.
type Status string

const (
	StatusUp   Status = "up"
	StatusDown Status = "down"
)

// ErrShuttingDown is reported by readiness once shutdown has begun.
var ErrShuttingDown = errors.New("shutting down")

type check struct {
	name     string
	fn       CheckFunc
	critical bool
	timeout  time.Duration
}

// Registry holds the readiness checks for one process.
type Registry struct {
	mu     sync.RWMutex
	checks []check

	started      atomic.Bool
	shuttingDown atomic.Bool

	// cache prevents a probe storm (kubelet every 5s × N checks) from turning
	// into real dependency load. Readiness is allowed to be slightly stale.
	cacheTTL time.Duration
	cacheMu  sync.Mutex
	cached   *Report
	cachedAt time.Time
}

// New creates a registry. Until MarkStarted is called, readiness reports down.
func New() *Registry {
	return &Registry{cacheTTL: time.Second}
}

// Register adds a readiness check.
//
// critical=false means a failure is reported but does not make the process
// unready — use it for things the service can genuinely run without (a metrics
// sink, say), and be honest about which those are.
func (r *Registry) Register(name string, critical bool, timeout time.Duration, fn CheckFunc) {
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.checks = append(r.checks, check{name: name, fn: fn, critical: critical, timeout: timeout})
}

// MarkStarted flips the startup gate. Call it once boot is complete.
func (r *Registry) MarkStarted() { r.started.Store(true) }

// MarkShuttingDown makes readiness fail immediately. This is deliberately the
// FIRST thing shutdown does: the load balancer needs time to observe the
// failing probe and stop sending new requests before the server actually stops
// accepting them. Skipping this is why "graceful" shutdowns still drop requests.
func (r *Registry) MarkShuttingDown() {
	r.shuttingDown.Store(true)
	r.cacheMu.Lock()
	r.cached = nil
	r.cacheMu.Unlock()
}

// Started reports whether boot completed.
func (r *Registry) Started() bool { return r.started.Load() }

// CheckResult is one dependency's outcome.
type CheckResult struct {
	Name     string `json:"name"`
	Status   Status `json:"status"`
	Error    string `json:"error,omitempty"`
	Critical bool   `json:"critical"`
	TookMS   int64  `json:"took_ms"`
}

// Report is the full readiness snapshot.
type Report struct {
	Status Status        `json:"status"`
	Checks []CheckResult `json:"checks,omitempty"`
	Reason string        `json:"reason,omitempty"`
}

// Ready runs every check (concurrently) and returns the aggregate.
func (r *Registry) Ready(ctx context.Context) Report {
	if r.shuttingDown.Load() {
		return Report{Status: StatusDown, Reason: ErrShuttingDown.Error()}
	}
	if !r.started.Load() {
		return Report{Status: StatusDown, Reason: "starting"}
	}

	r.cacheMu.Lock()
	if r.cached != nil && time.Since(r.cachedAt) < r.cacheTTL {
		rep := *r.cached
		r.cacheMu.Unlock()
		return rep
	}
	r.cacheMu.Unlock()

	r.mu.RLock()
	checks := make([]check, len(r.checks))
	copy(checks, r.checks)
	r.mu.RUnlock()

	results := make([]CheckResult, len(checks))
	var wg sync.WaitGroup
	for i, c := range checks {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cctx, cancel := context.WithTimeout(ctx, c.timeout)
			defer cancel()

			start := time.Now()
			err := c.fn(cctx)
			res := CheckResult{
				Name:     c.name,
				Status:   StatusUp,
				Critical: c.critical,
				TookMS:   time.Since(start).Milliseconds(),
			}
			if err != nil {
				res.Status = StatusDown
				res.Error = err.Error()
			}
			results[i] = res
		}()
	}
	wg.Wait()

	sort.Slice(results, func(i, j int) bool { return results[i].Name < results[j].Name })

	rep := Report{Status: StatusUp, Checks: results}
	for _, res := range results {
		if res.Status == StatusDown && res.Critical {
			rep.Status = StatusDown
			rep.Reason = res.Name + " is down"
			break
		}
	}

	r.cacheMu.Lock()
	cp := rep
	r.cached, r.cachedAt = &cp, time.Now()
	r.cacheMu.Unlock()

	return rep
}

// ── HTTP handlers ────────────────────────────────────────────────────────────

// LiveHandler serves /healthz. It intentionally checks nothing but the process
// itself. If this handler can run, the process is not wedged. See the package
// comment for why adding a database ping here is a bad idea.
func (r *Registry) LiveHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, Report{Status: StatusUp})
	}
}

// ReadyHandler serves /readyz.
func (r *Registry) ReadyHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		rep := r.Ready(req.Context())
		code := http.StatusOK
		if rep.Status != StatusUp {
			code = http.StatusServiceUnavailable
		}
		writeJSON(w, code, rep)
	}
}

// StartupHandler serves /startupz.
func (r *Registry) StartupHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		if !r.started.Load() {
			writeJSON(w, http.StatusServiceUnavailable, Report{Status: StatusDown, Reason: "starting"})
			return
		}
		writeJSON(w, http.StatusOK, Report{Status: StatusUp})
	}
}

func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}
