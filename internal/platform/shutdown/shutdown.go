// Package shutdown coordinates process lifecycle: start N long-running
// components, then stop them in a defined order when a signal arrives.
//
// The shape here is the one Kubernetes actually needs, and it has four phases
// rather than the usual two:
//
//  1. SIGNAL      SIGTERM arrives. The kubelet has *already* begun removing
//     this pod from Service endpoints, but that removal is
//     eventually consistent — kube-proxy on other nodes may still
//     be sending traffic here for a second or more.
//  2. PRE-STOP    Readiness starts failing, then we deliberately WAIT
//     (DrainDelay) while still serving. This is the window that
//     makes shutdown genuinely graceful instead of merely orderly.
//     Skipping it is why "graceful" shutdowns still 502.
//  3. DRAIN       Components are told to stop accepting new work and finish
//     what they hold, bounded by the grace budget.
//  4. HARD CUT    Anything still running is canceled outright.
//
// terminationGracePeriodSeconds must exceed DrainDelay + grace, or the kubelet
// SIGKILLs the process in the middle of phase 3.
package shutdown

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// Component is one long-running part of a process.
type Component struct {
	// Name appears in logs.
	Name string
	// Run blocks until the component finishes or ctx is canceled. Returning
	// nil means "finished normally"; returning an error triggers shutdown of
	// the whole process.
	Run func(ctx context.Context) error
	// Stop asks the component to wind down gracefully. It is called with a
	// context carrying the drain deadline. Optional: components that watch
	// their Run context can leave it nil.
	Stop func(ctx context.Context) error
}

// Runner supervises a set of components.
type Runner struct {
	Log *slog.Logger

	// DrainDelay is how long to keep serving after readiness starts failing,
	// so load balancers notice before the listener closes. 0 in dev.
	DrainDelay time.Duration

	// PreStop hooks run first, synchronously, before DrainDelay. The canonical
	// hook is health.Registry.MarkShuttingDown.
	preStop    []namedHook
	components []Component
}

type namedHook struct {
	name string
	fn   func(context.Context) error
}

// Add registers a component. Order matters only for Stop, which runs in
// reverse registration order (last added stops first), so dependencies
// registered early are still available while later components drain.
func (r *Runner) Add(c Component) { r.components = append(r.components, c) }

// PreStop registers a hook to run at the very start of shutdown.
func (r *Runner) PreStop(name string, fn func(context.Context) error) {
	r.preStop = append(r.preStop, namedHook{name: name, fn: fn})
}

// Signals returns a context canceled on SIGINT or SIGTERM.
//
// The returned stop function restores default signal handling, which means a
// second Ctrl-C kills the process immediately — the behavior you want when a
// drain is taking longer than you are willing to wait.
func Signals(parent context.Context) (context.Context, context.CancelFunc) {
	return signal.NotifyContext(parent, syscall.SIGINT, syscall.SIGTERM)
}

// Run starts every component and blocks until shutdown completes.
//
// It returns the first non-nil error reported by a component, or nil if
// everything stopped cleanly. A component returning early (even with nil) is
// treated as a reason to shut the process down: in this system every component
// is supposed to run for the lifetime of the process, so one exiting on its own
// means something is wrong.
func (r *Runner) Run(ctx context.Context, grace time.Duration) error {
	log := r.Log
	if log == nil {
		log = slog.Default()
	}

	// runCtx is what components actually observe. It is NOT the parent: we
	// want to control precisely when components are told to stop, rather than
	// having every one of them canceled the instant SIGTERM lands.
	runCtx, cancelRun := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelRun()

	type exit struct {
		name string
		err  error
	}
	exits := make(chan exit, len(r.components))

	var wg sync.WaitGroup
	for _, c := range r.components {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if p := recover(); p != nil {
					exits <- exit{name: c.Name, err: fmt.Errorf("panic: %v", p)}
				}
			}()
			log.Info("component started", "component", c.Name)
			exits <- exit{name: c.Name, err: c.Run(runCtx)}
		}()
	}

	var firstErr error

	select {
	case <-ctx.Done():
		log.Info("shutdown signal received", "grace", grace, "drain_delay", r.DrainDelay)
	case e := <-exits:
		if e.err != nil && !errors.Is(e.err, context.Canceled) {
			firstErr = fmt.Errorf("component %s: %w", e.name, e.err)
			log.Error("component failed, shutting down", "component", e.name, "error", e.err)
		} else {
			log.Warn("component exited unexpectedly, shutting down", "component", e.name)
		}
	}

	// ── Phase 2: pre-stop hooks, then the drain delay ────────────────────────
	stopCtx, cancelStop := context.WithTimeout(context.WithoutCancel(ctx), grace)
	defer cancelStop()

	for _, h := range r.preStop {
		if err := h.fn(stopCtx); err != nil {
			log.Warn("pre-stop hook failed", "hook", h.name, "error", err)
		} else {
			log.Info("pre-stop hook done", "hook", h.name)
		}
	}

	if r.DrainDelay > 0 {
		log.Info("draining: still serving while load balancers observe the failing readiness probe",
			"drain_delay", r.DrainDelay)
		select {
		case <-time.After(r.DrainDelay):
		case <-stopCtx.Done():
		}
	}

	// ── Phase 3: stop components in reverse order ────────────────────────────
	for i := len(r.components) - 1; i >= 0; i-- {
		c := r.components[i]
		if c.Stop == nil {
			continue
		}
		log.Info("stopping component", "component", c.Name)
		if err := c.Stop(stopCtx); err != nil && !errors.Is(err, context.Canceled) {
			log.Warn("component stop returned an error", "component", c.Name, "error", err)
			if firstErr == nil {
				firstErr = fmt.Errorf("stop %s: %w", c.Name, err)
			}
		}
	}

	// Now cancel the run contexts so anything still blocked unwinds.
	cancelRun()

	// ── Phase 4: wait for the drain, then hard cut ───────────────────────────
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
		log.Info("shutdown complete")
	case <-stopCtx.Done():
		log.Error("shutdown grace period expired with components still running; forcing exit",
			"grace", grace)
		if firstErr == nil {
			firstErr = errors.New("shutdown timed out")
		}
	}

	// Drain the exit channel so component errors surfaced during shutdown are
	// not silently lost.
	for {
		select {
		case e := <-exits:
			if e.err != nil && !errors.Is(e.err, context.Canceled) && firstErr == nil {
				firstErr = fmt.Errorf("component %s: %w", e.name, e.err)
			}
		default:
			return firstErr
		}
	}
}
