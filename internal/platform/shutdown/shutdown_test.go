package shutdown

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError + 1}))
}

// blocker is a component that runs until its context is canceled and records
// what happened, which is all these tests need to assert on.
type blocker struct {
	started  atomic.Bool
	finished atomic.Bool
	stopped  atomic.Bool
	stopAt   atomic.Int64
	runFor   time.Duration
}

func (b *blocker) component(name string) Component {
	return Component{
		Name: name,
		Run: func(ctx context.Context) error {
			b.started.Store(true)
			defer b.finished.Store(true)
			if b.runFor > 0 {
				select {
				case <-time.After(b.runFor):
				case <-ctx.Done():
				}
				return nil
			}
			<-ctx.Done()
			return nil
		},
		Stop: func(context.Context) error {
			b.stopped.Store(true)
			b.stopAt.Store(time.Now().UnixNano())
			return nil
		},
	}
}

func TestRunStopsOnSignal(t *testing.T) {
	r := &Runner{Log: quietLogger()}
	b := &blocker{}
	r.Add(b.component("worker"))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx, 2*time.Second) }()

	waitFor(t, func() bool { return b.started.Load() }, "component to start")
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() error = %v, want nil", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}

	if !b.stopped.Load() {
		t.Error("Stop was never called")
	}
	if !b.finished.Load() {
		t.Error("Run never returned")
	}
}

func TestPreStopHooksRunBeforeComponentsStop(t *testing.T) {
	// This ordering is the entire point of the pre-stop phase: readiness has
	// to start failing while the listener is still accepting, or in-flight
	// requests get dropped by whatever is load-balancing to us.
	r := &Runner{Log: quietLogger()}

	var hookAt atomic.Int64
	r.PreStop("mark-unready", func(context.Context) error {
		hookAt.Store(time.Now().UnixNano())
		return nil
	})

	b := &blocker{}
	r.Add(b.component("listener"))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx, 2*time.Second) }()

	waitFor(t, func() bool { return b.started.Load() }, "component to start")
	cancel()
	<-done

	if hookAt.Load() == 0 {
		t.Fatal("pre-stop hook never ran")
	}
	if b.stopAt.Load() == 0 {
		t.Fatal("component Stop never ran")
	}
	if hookAt.Load() > b.stopAt.Load() {
		t.Error("pre-stop hook ran after the component stopped; readiness must fail first")
	}
}

func TestDrainDelayKeepsServingBeforeStopping(t *testing.T) {
	const delay = 120 * time.Millisecond
	r := &Runner{Log: quietLogger(), DrainDelay: delay}

	var hookAt atomic.Int64
	r.PreStop("mark-unready", func(context.Context) error {
		hookAt.Store(time.Now().UnixNano())
		return nil
	})

	b := &blocker{}
	r.Add(b.component("listener"))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx, 3*time.Second) }()

	waitFor(t, func() bool { return b.started.Load() }, "component to start")
	cancel()
	<-done

	gap := time.Duration(b.stopAt.Load() - hookAt.Load())
	if gap < delay/2 {
		t.Errorf("gap between readiness failing and the listener stopping was %v, want at least ~%v", gap, delay)
	}
}

func TestComponentsStopInReverseOrder(t *testing.T) {
	// Registered first = stopped last, so a dependency registered early is
	// still usable while the components that need it are draining.
	r := &Runner{Log: quietLogger()}

	var order []string
	record := func(name string) Component {
		return Component{
			Name: name,
			Run:  func(ctx context.Context) error { <-ctx.Done(); return nil },
			Stop: func(context.Context) error {
				order = append(order, name)
				return nil
			},
		}
	}
	r.Add(record("db"))
	r.Add(record("api"))
	r.Add(record("ws"))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx, 2*time.Second) }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	<-done

	want := []string{"ws", "api", "db"}
	if len(order) != len(want) {
		t.Fatalf("stop order = %v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("stop order = %v, want %v", order, want)
		}
	}
}

func TestFailingComponentShutsDownTheProcess(t *testing.T) {
	r := &Runner{Log: quietLogger()}
	sentinel := errors.New("database exploded")

	r.Add(Component{
		Name: "bad",
		Run:  func(context.Context) error { return sentinel },
	})
	other := &blocker{}
	r.Add(other.component("good"))

	err := r.Run(context.Background(), 2*time.Second)
	if !errors.Is(err, sentinel) {
		t.Fatalf("Run() error = %v, want it to wrap %v", err, sentinel)
	}
	if !other.finished.Load() {
		t.Error("the healthy component was not shut down alongside the failing one")
	}
}

func TestPanicInComponentIsContained(t *testing.T) {
	r := &Runner{Log: quietLogger()}
	r.Add(Component{
		Name: "panicky",
		Run:  func(context.Context) error { panic("boom") },
	})

	err := r.Run(context.Background(), 2*time.Second)
	if err == nil {
		t.Fatal("Run() error = nil, want the panic surfaced as an error")
	}
}

func TestGraceExpiryForcesExit(t *testing.T) {
	// A component that ignores its context must not be able to hold the
	// process open past the grace budget — the kubelet would SIGKILL us and
	// we would learn nothing about why.
	r := &Runner{Log: quietLogger()}
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	r.Add(Component{
		Name: "stubborn",
		Run:  func(context.Context) error { <-release; return nil },
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx, 150*time.Millisecond) }()
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Run() error = nil, want a timeout error")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not give up after the grace period")
	}
}

func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
