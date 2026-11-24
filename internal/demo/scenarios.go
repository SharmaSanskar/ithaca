package demo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/SharmaSanskar/ithaca/internal/receiver"
	"github.com/SharmaSanskar/ithaca/internal/store/gen"
)

// Scenario is one server-driven demo script.
type Scenario struct {
	Name        string `json:"name"`
	Title       string `json:"title"`
	Description string `json:"description"`
	steps       int
	run         func(ctx context.Context, r *run) error
}

// Scenarios are the five demo scripts, in the order the UI shows them.
var Scenarios = []Scenario{
	{
		Name: "happy_path", Title: "Happy path", steps: 4,
		Description: "Healthy endpoints, steady traffic: watch events fan out and deliver.",
		run:         happyPath,
	},
	{
		Name: "endpoint_down", Title: "Endpoint goes down", steps: 5,
		Description: "billing starts returning 503. Retries back off, the circuit breaker opens, and its backlog waits safely while everything else keeps flowing.",
		run:         endpointDown,
	},
	{
		Name: "slow_consumer", Title: "Slow consumer", steps: 5,
		Description: "analytics slows to 1.5s per request. Its queue absorbs the difference; ingest never slows down.",
		run:         slowConsumer,
	},
	{
		Name: "bad_deploy", Title: "Bad deploy → dead letters", steps: 4,
		Description: "crm ships a broken release that rejects every request with 400. Permanent failures go straight to the dead-letter queue.",
		run:         badDeploy,
	},
	{
		Name: "recovery", Title: "Recovery + replay", steps: 6,
		Description: "Every endpoint recovers. The breaker closes after one probe, backlogs drain, and the dead-letter queue is replayed at a controlled rate.",
		run:         recovery,
	},
}

// ScenarioByName finds a scenario.
func ScenarioByName(name string) (Scenario, bool) {
	for _, s := range Scenarios {
		if s.Name == name {
			return s, true
		}
	}
	return Scenario{}, false
}

// ── scripts ──────────────────────────────────────────────────────────────────

func happyPath(ctx context.Context, r *run) error {
	r.healAll(ctx)
	r.traffic(25)
	r.say("info", "Traffic is flowing at ~25 events/s. Each event is written to Postgres together with one delivery per subscribed endpoint, in a single transaction, before ingest answers 202.")
	if !r.sleep(ctx, 6*time.Second) {
		return ctx.Err()
	}
	r.say("info", "The dispatcher leases whole streams, not single messages: orders-service is ordered per order id, so each order's events arrive in sequence while different orders deliver in parallel.")
	if !r.sleep(ctx, 6*time.Second) {
		return ctx.Err()
	}
	r.say("info", "analytics subscribes to everything, billing only to invoice.* and payment.*: one event fans out to every matching endpoint.")
	if !r.sleep(ctx, 4*time.Second) {
		return ctx.Err()
	}
	st, err := r.attempts(ctx, 10, nil)
	if err != nil {
		return err
	}
	r.say("success", fmt.Sprintf("✓ %d deliveries in the last 10s · delivery p99 %dms · ingest p99 %s",
		st.Succeeded, int(st.P99Ms), r.ingestP99()))
	return nil
}

func endpointDown(ctx context.Context, r *run) error {
	r.traffic(30)
	billing := r.endpoint(ctx, "billing")
	r.setSink(ctx, "billing", receiver.Behavior{Status: 200, LatencyMS: 40, FailRate: 1, FailStatus: 503})
	r.say("warn", "billing goes down: every request now returns 503.")

	if err := r.waitFor(ctx, 30*time.Second, func() (bool, error) {
		st, err := r.attempts(ctx, 5, &billing)
		return st.Failed > 0, err
	}); err != nil {
		return err
	}
	r.say("warn", "Failed deliveries are retried with exponential backoff and full jitter, so billing is not hammered while it is down.")

	if err := r.waitFor(ctx, 60*time.Second, func() (bool, error) {
		e, err := r.status(ctx, "billing")
		return e.CircuitState != "closed", err
	}); err != nil {
		return err
	}
	r.say("warn", "After 10 consecutive failures the circuit breaker OPENS. billing's streams are parked: no more requests, no retry attempts used up, and no wasted work competing with healthy traffic.")
	if !r.sleep(ctx, 8*time.Second) {
		return ctx.Err()
	}
	e, err := r.status(ctx, "billing")
	if err != nil {
		return err
	}
	r.say("info", fmt.Sprintf("Every few seconds exactly ONE probe request tests billing. While it keeps failing, the backlog waits: %d deliveries parked, none lost.", e.Depth))
	others, err := r.othersP99(ctx, billing)
	if err != nil {
		return err
	}
	r.say("success", fmt.Sprintf("✓ The other endpoints are unaffected: delivery p99 %dms. Run “Recovery + replay” to bring billing back.", others))
	return nil
}

func slowConsumer(ctx context.Context, r *run) error {
	r.traffic(60)
	r.setSink(ctx, "analytics", receiver.Behavior{Status: 200, LatencyMS: 1500})
	r.say("warn", "analytics slows down: every request now takes 1.5s, while traffic rises to 60 events/s.")
	if !r.sleep(ctx, 8*time.Second) {
		return ctx.Err()
	}
	peak := int64(0)
	e, err := r.status(ctx, "analytics")
	if err != nil {
		return err
	}
	peak = max(peak, e.Depth)
	r.say("info", fmt.Sprintf("analytics' queue is growing (%d waiting): the dispatcher can only deliver as fast as analytics answers, so the backlog absorbs the difference.", e.Depth))
	if !r.sleep(ctx, 6*time.Second) {
		return ctx.Err()
	}
	r.say("success", fmt.Sprintf("✓ Ingest is unaffected: p99 %s. Accepting an event never waits on any subscriber. That decoupling is the point of a queue.", r.ingestP99()))
	r.setSink(ctx, "analytics", Healthy)
	r.traffic(25)
	r.say("info", "analytics recovers. Watch its backlog drain.")
	if err := r.waitFor(ctx, 90*time.Second, func() (bool, error) {
		e, err := r.status(ctx, "analytics")
		peak = max(peak, e.Depth)
		return e.Depth < 20, err
	}); err != nil {
		return err
	}
	r.say("success", fmt.Sprintf("✓ Backlog peaked at %d and drained. Ingest p99 stayed at %s throughout.", peak, r.ingestP99()))
	return nil
}

func badDeploy(ctx context.Context, r *run) error {
	r.traffic(30)
	crm := r.endpoint(ctx, "crm")
	r.setSink(ctx, "crm", receiver.Behavior{Status: 200, LatencyMS: 40, FailRate: 1, FailStatus: 400})
	r.say("warn", "crm ships a broken release: every request is rejected with 400 Bad Request.")
	if err := r.waitFor(ctx, 30*time.Second, func() (bool, error) {
		e, err := r.status(ctx, "crm")
		return e.Dlq > 0, err
	}); err != nil {
		return err
	}
	r.say("warn", "A 4xx is a permanent failure: retrying cannot fix a bad request, so those deliveries go straight to the dead-letter queue instead of wasting retries.")
	if !r.sleep(ctx, 8*time.Second) {
		return ctx.Err()
	}
	r.setSink(ctx, "crm", Healthy)
	e, err := r.status(ctx, "crm")
	if err != nil {
		return err
	}
	st, _ := r.attempts(ctx, 15, &crm)
	r.say("info", fmt.Sprintf("crm rolls back. %d dead letters are waiting, %d permanent failures recorded, nothing lost.", e.Dlq, st.Permanent))
	r.say("success", "✓ Dead letters keep their original event id, so they can be replayed safely. Run “Recovery + replay”.")
	return nil
}

func recovery(ctx context.Context, r *run) error {
	r.traffic(25)
	r.healAll(ctx)
	r.say("info", "Every endpoint is healthy again.")

	if err := r.waitFor(ctx, 90*time.Second, func() (bool, error) {
		all, err := r.statuses(ctx)
		for _, e := range all {
			if e.CircuitState != "closed" {
				return false, err
			}
		}
		return true, err
	}); err != nil {
		return err
	}
	r.say("success", "The next probe succeeded and every circuit is CLOSED. Parked streams reopen staggered over a few seconds, not all at once, so a recovering endpoint is not knocked straight back over.")

	if err := r.waitFor(ctx, 120*time.Second, func() (bool, error) {
		d, err := r.totalDepth(ctx)
		return d < 50, err
	}); err != nil {
		return err
	}
	r.say("info", "Backlogs have drained.")

	dlq, err := r.totalDLQ(ctx)
	if err != nil {
		return err
	}
	if dlq == 0 {
		r.say("info", "The dead-letter queue is empty: nothing to replay.")
	} else {
		res, rerr := r.rt.replay(ctx)
		if rerr != nil {
			return rerr
		}
		r.say("info", fmt.Sprintf("Replaying %d dead letters at %d/s per endpoint. Each keeps its event id, so a subscriber that already processed one ignores the duplicate.", res, r.rt.replayRate))
		if werr := r.waitFor(ctx, 120*time.Second, func() (bool, error) {
			left, e1 := r.totalDLQ(ctx)
			depth, e2 := r.totalDepth(ctx)
			return left == 0 && depth < 50, errors.Join(e1, e2)
		}); werr != nil {
			return werr
		}
		r.say("success", "Replayed deliveries have landed.")
	}
	d, err := r.totalDepth(ctx)
	if err != nil {
		return err
	}
	r.say("success", fmt.Sprintf("✓ Queue depth stable at %d. Ingest never slowed down (p99 %s).", d, r.ingestP99()))
	return nil
}

// ── the script runtime ───────────────────────────────────────────────────────

// run is one scenario execution: helpers for actions, waits and narration.
type run struct {
	rt       *Runtime
	scenario Scenario
	step     int
}

func (r *run) say(level, text string) {
	r.step++
	r.rt.narrate(r.scenario, r.step, level, text, false)
}

func (r *run) traffic(rate int) { r.rt.gen.SetRate(rate) }

func (r *run) setSink(ctx context.Context, sink string, b receiver.Behavior) {
	if err := r.rt.sinks.Set(ctx, sink, b); err != nil {
		r.rt.log.Warn("demo: set sink failed", "sink", sink, "error", err)
	}
}

func (r *run) healAll(ctx context.Context) {
	for _, s := range Sinks {
		r.setSink(ctx, s.ID, Healthy)
	}
}

func (r *run) sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// errTimeout is a condition that never came true: narrated, not fatal.
var errTimeout = errors.New("timed out")

func (r *run) waitFor(ctx context.Context, timeout time.Duration, cond func() (bool, error)) error {
	deadline := time.Now().Add(timeout)
	for {
		ok, err := cond()
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		if time.Now().After(deadline) {
			return errTimeout
		}
		if !r.sleep(ctx, 250*time.Millisecond) {
			return ctx.Err()
		}
	}
}

func (r *run) ingestP99() string {
	d := r.rt.gen.IngestP99()
	if d < time.Millisecond {
		return fmt.Sprintf("%dµs", d.Microseconds())
	}
	return fmt.Sprintf("%.1fms", float64(d.Microseconds())/1000)
}

func (r *run) statuses(ctx context.Context) ([]gen.DemoEndpointStatusRow, error) {
	return r.rt.store.DemoEndpointStatus(ctx, r.rt.tenant)
}

func (r *run) status(ctx context.Context, sink string) (gen.DemoEndpointStatusRow, error) {
	all, err := r.statuses(ctx)
	if err != nil {
		return gen.DemoEndpointStatusRow{}, err
	}
	name := sinkName(sink)
	for _, e := range all {
		if e.Description == name {
			return e, nil
		}
	}
	return gen.DemoEndpointStatusRow{}, fmt.Errorf("demo endpoint %q not found (is the tenant seeded?)", name)
}

func (r *run) endpoint(ctx context.Context, sink string) uuid.UUID {
	e, err := r.status(ctx, sink)
	if err != nil {
		return uuid.Nil
	}
	return e.ID
}

func (r *run) totalDepth(ctx context.Context) (int64, error) {
	all, err := r.statuses(ctx)
	var n int64
	for _, e := range all {
		n += e.Depth
	}
	return n, err
}

func (r *run) totalDLQ(ctx context.Context) (int64, error) {
	all, err := r.statuses(ctx)
	var n int64
	for _, e := range all {
		n += e.Dlq
	}
	return n, err
}

func (r *run) attempts(ctx context.Context, seconds int, endpoint *uuid.UUID) (gen.RecentAttemptStatsRow, error) {
	return r.rt.store.RecentAttemptStats(ctx, gen.RecentAttemptStatsParams{
		TenantID: r.rt.tenant, Seconds: int32(seconds), EndpointID: endpoint, //nolint:gosec // small
	})
}

// othersP99 is the worst delivery p99 among endpoints other than except.
func (r *run) othersP99(ctx context.Context, except uuid.UUID) (int, error) {
	all, err := r.statuses(ctx)
	if err != nil {
		return 0, err
	}
	worst := 0
	for _, e := range all {
		if e.ID == except {
			continue
		}
		id := e.ID
		st, err := r.attempts(ctx, 10, &id)
		if err != nil {
			return 0, err
		}
		worst = max(worst, int(st.P99Ms))
	}
	return worst, nil
}

func sinkName(sink string) string {
	for _, s := range Sinks {
		if s.ID == sink {
			return s.Name
		}
	}
	return sink
}
