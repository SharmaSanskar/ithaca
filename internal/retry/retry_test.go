package retry

import (
	"math/rand/v2"
	"testing"
	"time"
)

func TestNextBackoffStaysWithinTheJitterWindow(t *testing.T) {
	p := Default
	r := rand.New(rand.NewPCG(1, 2))
	for failed := 1; failed <= 20; failed++ {
		ceil := p.Base << (failed - 1)
		if ceil > p.Cap || ceil <= 0 {
			ceil = p.Cap
		}
		for range 1000 {
			d := NextBackoff(failed, p, r)
			if d < p.Base/2 || d > ceil {
				t.Fatalf("failed=%d: backoff %s outside [%s, %s]", failed, d, p.Base/2, ceil)
			}
		}
	}
}

func TestNextBackoffGrowsExponentially(t *testing.T) {
	// The median doubles each step until the cap. Use many samples so the
	// median is stable, and a fixed seed so the test is deterministic.
	p := Policy{Base: time.Second, Multiplier: 2, Cap: time.Hour, MaxAttempts: 12}
	r := rand.New(rand.NewPCG(42, 42))
	median := func(failed int) time.Duration {
		var sum time.Duration
		const n = 4000
		for range n {
			sum += NextBackoff(failed, p, r)
		}
		return sum / n
	}
	prev := median(1)
	for failed := 2; failed <= 8; failed++ {
		cur := median(failed)
		if ratio := float64(cur) / float64(prev); ratio < 1.6 || ratio > 2.4 {
			t.Errorf("failed=%d: mean %s is %.2fx the previous %s, want ~2x", failed, cur, ratio, prev)
		}
		prev = cur
	}
}

func TestNextBackoffNeverExceedsCapEvenWhenTheExponentOverflows(t *testing.T) {
	p := Default
	r := rand.New(rand.NewPCG(7, 7))
	for _, failed := range []int{50, 500, 5000} {
		if d := NextBackoff(failed, p, r); d > p.Cap || d < 0 {
			t.Errorf("failed=%d: %s", failed, d)
		}
	}
}

func TestNextBackoffIsDeterministicForASeed(t *testing.T) {
	a := rand.New(rand.NewPCG(9, 9))
	b := rand.New(rand.NewPCG(9, 9))
	for failed := 1; failed < 12; failed++ {
		if NextBackoff(failed, Default, a) != NextBackoff(failed, Default, b) {
			t.Fatal("same seed produced different schedules")
		}
	}
}

func TestDefaultScheduleExhaustsInHours(t *testing.T) {
	// Expected total wait across the 11 gaps between 12 attempts, measured by
	// simulation rather than by quoting window ceilings (which overstates it
	// by ~2x under full jitter).
	r := rand.New(rand.NewPCG(3, 3))
	const runs = 2000
	var sum time.Duration
	for range runs {
		for failed := 1; failed < Default.MaxAttempts; failed++ {
			sum += NextBackoff(failed, Default, r)
		}
	}
	mean := sum / runs
	t.Logf("expected time from first attempt to dead letter: %s", mean.Round(time.Minute))
	if mean < 4*time.Hour || mean > 10*time.Hour {
		t.Errorf("expected time to exhaustion = %s, want 4-10h", mean)
	}
}
