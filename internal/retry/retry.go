// Package retry computes the backoff schedule.
//
// NextBackoff is a pure function: no clock and no hidden randomness source. The
// caller passes the attempt number and a *rand.Rand, so a test seeds the RNG
// and asserts exact values, and a 6-hour schedule is tested in microseconds.
package retry

import (
	"math"
	"math/rand/v2"
	"time"
)

// Policy is a retry schedule.
type Policy struct {
	Base        time.Duration // delay ceiling after the first failure
	Multiplier  float64       // growth factor per attempt
	Cap         time.Duration // upper bound on any single delay
	MaxAttempts int           // total attempts (first try included) before dead-lettering
}

// Default is the production schedule: 30s doubling to a 6h cap, 12 attempts.
//
// Window ceilings: 30s 1m 2m 4m 8m 16m 32m 64m 2.1h 4.3h 6h. With full jitter
// the EXPECTED wait is about half of each ceiling, so expected time to
// exhaustion is ~7h (TestDefaultScheduleExhaustsInHours pins this). That is
// long enough to survive a business-hours outage and short enough that the
// dead-letter queue is not a week stale.
//
// A trap worth naming: quoting a schedule by its ceilings ("5s, 10s, 20s ...
// about 6h") and then applying full jitter roughly halves the real duration.
// The first draft of this policy (base 5s) made exactly that mistake and would
// have dead-lettered after ~1.4h.
var Default = Policy{Base: 30 * time.Second, Multiplier: 2, Cap: 6 * time.Hour, MaxAttempts: 12}

// NextBackoff returns how long to wait after the given number of failed
// attempts (failed = 1 after the first failure).
//
// FULL JITTER: uniform in [Base/2, min(Cap, Base*Multiplier^(failed-1))].
//
// Jitter is not optional here. When a downed endpoint recovers, every delivery
// queued for it becomes due at once; without jitter they arrive as one
// synchronized herd and knock the endpoint back over. Full jitter spreads them
// across the whole window. (AWS, "Exponential Backoff and Jitter", measured
// full jitter as best for both total work and completion time.) The Base/2
// floor stops a pathological 3ms retry from hammering a recovering endpoint.
func NextBackoff(failed int, p Policy, r *rand.Rand) time.Duration {
	if failed < 1 {
		failed = 1
	}
	ceil := float64(p.Base) * math.Pow(p.Multiplier, float64(failed-1))
	if ceil > float64(p.Cap) || math.IsInf(ceil, 0) {
		ceil = float64(p.Cap)
	}
	lo := float64(p.Base) / 2
	if ceil < lo {
		ceil = lo
	}
	return time.Duration(lo + r.Float64()*(ceil-lo))
}

// Exhausted reports whether an attempt number used up the budget.
func (p Policy) Exhausted(attempt int) bool { return attempt >= p.MaxAttempts }
