// Package breaker is the per-endpoint circuit breaker's state machine.
//
//	          threshold consecutive failures
//	closed ─────────────────────────────────▶ open ──┐
//	  ▲                                        ▲     │ cooldown elapses:
//	  │ probe succeeds                         │     │ ONE stream unparked,
//	  │                          probe fails   │     ▼ it may send ONE request
//	  └──────────────────── half_open ─────────┘
//	                        (cooldown doubles, capped, jittered)
//
// Next is a pure function. The storage side (internal/queue/pg) loads the
// endpoint row FOR UPDATE inside the ack transaction, calls Next, and writes
// the result back, so a breaker decision can never disagree with the delivery
// record that caused it.
//
// Why a breaker at all, when retries already back off per delivery: backoff
// is per DELIVERY. A dead endpoint with 10,000 queued deliveries still gets
// 10,000 independent retry schedules, each costing a lease, a connect timeout,
// an attempt row and a WAL commit, and all competing with healthy traffic in
// the same dequeue scan. The breaker turns that into one probe per cooldown.
package breaker

import (
	"math/rand/v2"
	"time"
)

// Circuit is the breaker state.
type Circuit string

const (
	Closed   Circuit = "closed"
	Open     Circuit = "open"
	HalfOpen Circuit = "half_open"
)

// Policy configures the breaker.
type Policy struct {
	// Threshold is how many consecutive failures open the circuit.
	Threshold int
	// BaseCooldown is the wait before the first probe; it doubles after each
	// failed probe, up to MaxCooldown.
	BaseCooldown time.Duration
	MaxCooldown  time.Duration
}

// Default: 10 consecutive failures; probe after 30s, then 1m, 2m … up to 10m.
//
// The threshold is consecutive failures, so it is really a statement about
// failure RATE at volume. At 5, an endpoint that is merely 30% flaky trips it
// regularly (0.3^5 ≈ 1 in 400 windows, which a busy endpoint hits in
// seconds); at 10 that is 1 in 170,000, while a dead endpoint still trips
// within its first ten requests.
var Default = Policy{Threshold: 10, BaseCooldown: 30 * time.Second, MaxCooldown: 10 * time.Minute}

// State is the persisted breaker state for one endpoint.
type State struct {
	Circuit     Circuit
	Failures    int           // consecutive
	Cooldown    time.Duration // current cooldown (0 when closed)
	ProbeStream *int64        // the one stream allowed to deliver while half_open
}

// Observation is what one ack learned about the endpoint.
type Observation struct {
	// Healthy: the endpoint answered with a response below 500 (other than
	// 429). A 404 is a config problem, but it proves the endpoint is up.
	Healthy int
	// Failed: 5xx, timeouts, connection errors. The endpoint is not well.
	Failed int
	// Stream is the stream whose ack this is (to recognize the probe).
	Stream int64
}

// Transition names what changed, for logs, metrics and the live dashboard.
type Transition string

const (
	None      Transition = ""
	Opened    Transition = "opened"   // closed → open
	Reopened  Transition = "reopened" // half_open → open (probe failed)
	Recovered Transition = "closed"   // half_open → closed (probe succeeded)
)

// Next applies an observation. nextProbeIn is meaningful when the result is
// Open: how long until the next probe, already jittered.
func Next(s State, o Observation, p Policy, r *rand.Rand) (out State, t Transition, nextProbeIn time.Duration) {
	out = s
	isProbe := s.Circuit == HalfOpen && s.ProbeStream != nil && *s.ProbeStream == o.Stream

	switch {
	case isProbe && o.Healthy > 0:
		return State{Circuit: Closed}, Recovered, 0

	case isProbe && o.Failed > 0:
		cd := min(s.Cooldown*2, p.MaxCooldown)
		if cd <= 0 {
			cd = p.BaseCooldown
		}
		out = State{Circuit: Open, Failures: s.Failures + o.Failed, Cooldown: cd}
		return out, Reopened, jitter(cd, r)

	case s.Circuit != Closed:
		// Open or half_open, and this is not the probe: results from requests
		// that were already in flight when the circuit opened. They neither
		// close nor re-open it; only the probe decides.
		return s, None, 0

	case o.Healthy > 0:
		out.Failures = 0
		return out, None, 0

	case o.Failed > 0:
		out.Failures = s.Failures + o.Failed
		if out.Failures >= p.Threshold {
			out.Circuit = Open
			out.Cooldown = p.BaseCooldown
			return out, Opened, jitter(p.BaseCooldown, r)
		}
		return out, None, 0
	}
	return s, None, 0
}

// jitter spreads a cooldown over [0.8d, 1.2d]. Without it, every endpoint that
// tripped in the same incident (a shared upstream outage) would be probed in
// the same second, forever.
func jitter(d time.Duration, r *rand.Rand) time.Duration {
	return time.Duration(float64(d) * (0.8 + 0.4*r.Float64()))
}
