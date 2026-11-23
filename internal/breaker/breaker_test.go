package breaker

import (
	"math/rand/v2"
	"testing"
	"time"
)

var p = Policy{Threshold: 3, BaseCooldown: 10 * time.Second, MaxCooldown: 60 * time.Second}

func rng() *rand.Rand { return rand.New(rand.NewPCG(1, 1)) }

func ptr(v int64) *int64 { return &v }

func TestOpensAfterThresholdConsecutiveFailures(t *testing.T) {
	s := State{Circuit: Closed}
	var tr Transition
	for i := range 3 {
		s, tr, _ = Next(s, Observation{Failed: 1, Stream: 1}, p, rng())
		if i < 2 && tr != None {
			t.Fatalf("opened after %d failures, threshold is 3", i+1)
		}
	}
	if s.Circuit != Open || tr != Opened || s.Cooldown != p.BaseCooldown {
		t.Fatalf("after 3 failures: %+v %q", s, tr)
	}
}

func TestASuccessResetsTheCount(t *testing.T) {
	s := State{Circuit: Closed, Failures: 2}
	s, _, _ = Next(s, Observation{Healthy: 1}, p, rng())
	s, tr, _ := Next(s, Observation{Failed: 1}, p, rng())
	if s.Circuit != Closed || s.Failures != 1 || tr != None {
		t.Fatalf("failures must be CONSECUTIVE: %+v", s)
	}
}

func TestOnlyTheProbeDecidesWhileHalfOpen(t *testing.T) {
	s := State{Circuit: HalfOpen, Cooldown: 10 * time.Second, ProbeStream: ptr(7)}

	// Stragglers from before the circuit opened: ignored either way.
	for _, o := range []Observation{{Healthy: 5, Stream: 8}, {Failed: 5, Stream: 9}} {
		if got, tr, _ := Next(s, o, p, rng()); got.Circuit != HalfOpen || tr != None {
			t.Errorf("non-probe observation %+v changed the circuit: %+v %q", o, got, tr)
		}
	}

	if got, tr, _ := Next(s, Observation{Healthy: 1, Stream: 7}, p, rng()); got.Circuit != Closed || tr != Recovered || got.Failures != 0 {
		t.Errorf("successful probe: %+v %q", got, tr)
	}
}

func TestFailedProbesDoubleTheCooldownUpToTheCap(t *testing.T) {
	s := State{Circuit: HalfOpen, Cooldown: 10 * time.Second, ProbeStream: ptr(7)}
	want := []time.Duration{20 * time.Second, 40 * time.Second, 60 * time.Second, 60 * time.Second}
	for i, w := range want {
		var tr Transition
		var wait time.Duration
		s, tr, wait = Next(s, Observation{Failed: 1, Stream: 7}, p, rng())
		if s.Circuit != Open || tr != Reopened || s.Cooldown != w {
			t.Fatalf("failed probe %d: %+v %q, want cooldown %s", i+1, s, tr, w)
		}
		if wait < w*8/10 || wait > w*12/10 {
			t.Fatalf("next probe in %s, outside the ±20%% jitter around %s", wait, w)
		}
		s.Circuit, s.ProbeStream = HalfOpen, ptr(7) // the breaker loop starts the next probe
	}
}

func TestOpenCircuitIgnoresInFlightResults(t *testing.T) {
	s := State{Circuit: Open, Failures: 3, Cooldown: 10 * time.Second}
	got, tr, _ := Next(s, Observation{Healthy: 10, Stream: 1}, p, rng())
	if got != s || tr != None {
		t.Errorf("results that were already in flight when the circuit opened changed it: %+v", got)
	}
}

func TestNeutralObservationsChangeNothing(t *testing.T) {
	// e.g. a batch of only 429s or SSRF blocks: neither healthy nor failed.
	s := State{Circuit: Closed, Failures: 2}
	if got, tr, _ := Next(s, Observation{}, p, rng()); got != s || tr != None {
		t.Errorf("empty observation changed state: %+v", got)
	}
}
