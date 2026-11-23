// Package pg is the Postgres queue driver: `FOR UPDATE SKIP LOCKED` stream
// leasing, fenced acks, and move-to-history completion. The SQL lives in
// internal/store/queries/queue.sql.
package pg

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/SharmaSanskar/ithaca/internal/breaker"
	"github.com/SharmaSanskar/ithaca/internal/queue"
	"github.com/SharmaSanskar/ithaca/internal/store"
	"github.com/SharmaSanskar/ithaca/internal/store/gen"
)

// Driver implements queue.Queue on Postgres.
type Driver struct {
	s       *store.Store
	now     func() time.Time
	breaker breaker.Policy
	// unparkSpread is the window over which a recovered endpoint's parked
	// streams are reopened (see UnparkEndpointStreams).
	unparkSpread time.Duration

	mu  sync.Mutex
	rng *rand.Rand
}

// Option configures a Driver.
type Option func(*Driver)

// WithBreaker sets the circuit-breaker policy.
func WithBreaker(p breaker.Policy) Option { return func(d *Driver) { d.breaker = p } }

// WithUnparkSpread sets how widely reopened streams are staggered.
func WithUnparkSpread(spread time.Duration) Option {
	return func(d *Driver) { d.unparkSpread = spread }
}

// New returns a driver over the given store.
func New(s *store.Store, opts ...Option) *Driver {
	d := &Driver{
		s: s, now: time.Now, breaker: breaker.Default, unparkSpread: 5 * time.Second,
		rng: rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64())), //nolint:gosec // jitter, not security
	}
	for _, o := range opts {
		o(d)
	}
	return d
}

var _ queue.Queue = (*Driver)(nil)

// Lease implements queue.Queue.
func (d *Driver) Lease(ctx context.Context, req queue.LeaseRequest) ([]*queue.Lease, error) {
	worker := req.WorkerID
	rows, err := d.s.LeaseStreams(ctx, gen.LeaseStreamsParams{
		MaxStreams:     int32(req.Max), //nolint:gosec // bounded by pool size
		WorkerID:       &worker,
		MinLeaseMs:     req.MinLease.Milliseconds(),
		LeasePaddingMs: req.LeasePadding.Milliseconds(),
	})
	if err != nil {
		return nil, fmt.Errorf("lease streams: %w", err)
	}
	now := d.now()
	out := make([]*queue.Lease, 0, len(rows))
	for _, r := range rows {
		out = append(out, &queue.Lease{
			StreamID:       r.ID,
			Token:          *r.LockToken,
			TenantID:       r.TenantID,
			SubscriptionID: r.SubscriptionID,
			EndpointID:     r.EndpointID,
			Ordered:        r.Ordered,
			StreamKey:      r.StreamKey,
			Endpoint: queue.Endpoint{
				URL:            r.Url,
				State:          r.EndpointState,
				Timeout:        time.Duration(r.TimeoutMs) * time.Millisecond,
				MaxConcurrency: int(r.MaxConcurrency),
				KeyID:          r.SigningKeyID,
				Secret:         r.SigningSecret,
				Circuit:        r.CircuitState,
			},
			WorkerID:   req.WorkerID,
			TTL:        time.Duration(r.LeaseMs) * time.Millisecond,
			AcquiredAt: now,
			IsProbe:    r.CircuitState == string(breaker.HalfOpen) && r.CircuitProbeStreamID != nil && *r.CircuitProbeStreamID == r.ID,
		})
	}
	return out, nil
}

// Fetch implements queue.Queue.
func (d *Driver) Fetch(ctx context.Context, l *queue.Lease, limit int) ([]queue.Message, error) {
	max := int32(limit) //nolint:gosec // bounded by config
	// A half-open probe takes the stream's first pending message whether or
	// not it is due yet. Waiting out a delivery's own backoff would stall the
	// probe, and with it the whole breaker: the probe IS the scheduling
	// decision.
	if l.Ordered || l.IsProbe {
		rows, err := d.s.FetchOrdered(ctx, gen.FetchOrderedParams{StreamID: l.StreamID, MaxRows: max})
		if err != nil {
			return nil, fmt.Errorf("fetch ordered: %w", err)
		}
		out := make([]queue.Message, len(rows))
		for i, r := range rows {
			out[i] = queue.Message{
				DeliveryID: r.ID, EventID: r.EventID, Seq: r.Seq, Attempt: r.Attempt, Due: r.Due || l.IsProbe,
				EventType: r.EventType, Payload: r.Payload, Traceparent: deref(r.Traceparent),
				EventCreatedAt: r.EventCreatedAt,
			}
		}
		return out, nil
	}
	rows, err := d.s.FetchUnordered(ctx, gen.FetchUnorderedParams{StreamID: l.StreamID, MaxRows: max})
	if err != nil {
		return nil, fmt.Errorf("fetch unordered: %w", err)
	}
	out := make([]queue.Message, len(rows))
	for i, r := range rows {
		out[i] = queue.Message{
			DeliveryID: r.ID, EventID: r.EventID, Seq: r.Seq, Attempt: r.Attempt, Due: r.Due,
			EventType: r.EventType, Payload: r.Payload, Traceparent: deref(r.Traceparent),
			EventCreatedAt: r.EventCreatedAt,
		}
	}
	return out, nil
}

// Renew implements queue.Queue.
func (d *Driver) Renew(ctx context.Context, l *queue.Lease) error {
	token := l.Token
	_, err := d.s.RenewLease(ctx, gen.RenewLeaseParams{
		ID: l.StreamID, LockToken: &token, LeaseMs: l.TTL.Milliseconds(),
	})
	if store.IsNoRows(err) {
		return queue.ErrLeaseLost
	}
	if err != nil {
		return fmt.Errorf("renew lease: %w", err)
	}
	return nil
}

// Ack implements queue.Queue. One transaction:
//
//  1. fence (abort if the lease is no longer ours)
//  2. insert every attempt row
//  3. move terminal deliveries to history
//  4. reschedule the rest
//  5. ordered stream whose head died → pause it
//  6. 410 Gone → auto-disable the endpoint
//  7. feed the circuit breaker
//
// Steps 5-7 happen in the same transaction as the ack that observed them, so
// endpoint state can never disagree with the delivery record.
//
// synchronous_commit is off for this transaction only. Losing the last few
// hundred milliseconds of acks in a database crash means redelivering a
// handful of messages, which at-least-once already permits. Ingest keeps full
// durability: losing an ingest commit would lose a customer's event.
func (d *Driver) Ack(ctx context.Context, l *queue.Lease, results []queue.Result) (queue.AckOutcome, error) {
	var out queue.AckOutcome
	if len(results) == 0 {
		return out, nil
	}
	var att attemptCols
	var done completeCols
	var resched rescheduleCols
	var obs breaker.Observation
	obs.Stream = l.StreamID
	var deadHead *uuid.UUID
	gone := false
	for _, r := range results {
		if r.Record != nil {
			att.add(r.DeliveryID, r.Record)
			switch health(r.Record) {
			case healthy:
				obs.Healthy++
			case failed:
				obs.Failed++
			}
			if r.Record.StatusCode == http.StatusGone {
				gone = true
			}
		}
		switch r.Disposition {
		case queue.Succeeded:
			done.add(r, "succeeded")
		case queue.Dead:
			done.add(r, "dead")
			if l.Ordered && deadHead == nil {
				id := r.DeliveryID
				deadHead = &id
			}
		case queue.Retry, queue.Defer:
			resched.add(r)
		default:
			return out, fmt.Errorf("ack: delivery %s has no disposition", r.DeliveryID)
		}
	}

	token := l.Token
	err := pgx.BeginFunc(ctx, d.s.Pool, func(tx pgx.Tx) error {
		q := d.s.WithTx(tx)
		if _, err := tx.Exec(ctx, "SET LOCAL synchronous_commit = off"); err != nil {
			return err
		}
		if _, err := q.FenceStream(ctx, gen.FenceStreamParams{ID: l.StreamID, LockToken: &token}); err != nil {
			if store.IsNoRows(err) {
				return queue.ErrLeaseLost
			}
			return fmt.Errorf("fence: %w", err)
		}
		if len(att.ids) > 0 {
			if err := q.InsertAttempts(ctx, att.params(l)); err != nil {
				return fmt.Errorf("insert attempts: %w", err)
			}
		}
		if len(done.ids) > 0 {
			if _, err := q.CompleteDeliveries(ctx, done.params(l.StreamID)); err != nil {
				return fmt.Errorf("complete deliveries: %w", err)
			}
		}
		if len(resched.ids) > 0 {
			if _, err := q.RescheduleDeliveries(ctx, resched.params(l.StreamID)); err != nil {
				return fmt.Errorf("reschedule deliveries: %w", err)
			}
		}
		if deadHead != nil {
			if err := q.PauseStream(ctx, gen.PauseStreamParams{ID: l.StreamID, LockToken: &token, DeliveryID: deadHead}); err != nil {
				return fmt.Errorf("pause stream: %w", err)
			}
			out.Paused = true
		}
		if gone {
			reason := "endpoint returned 410 Gone"
			if err := q.AutoDisableEndpoint(ctx, gen.AutoDisableEndpointParams{ID: l.EndpointID, Reason: &reason}); err != nil {
				return fmt.Errorf("auto-disable: %w", err)
			}
			out.AutoDisabled = true
		}
		t, err := d.observe(ctx, q, l, obs)
		out.Breaker = string(t)
		return err
	})
	return out, err
}

// observe feeds one ack's observation to the endpoint's breaker.
//
// The healthy path — a closed circuit and nothing but successes — must stay
// cheap at thousands of acks per second, so it never locks the endpoint row:
// ResetEndpointFailures matches nothing (and locks nothing) unless there is
// actually a failure count to clear. Only failures and probe results take the
// row lock and run the state machine.
func (d *Driver) observe(ctx context.Context, q *gen.Queries, l *queue.Lease, o breaker.Observation) (breaker.Transition, error) {
	if o.Healthy == 0 && o.Failed == 0 {
		return breaker.None, nil
	}
	if o.Failed == 0 && !l.IsProbe && l.Endpoint.Circuit == string(breaker.Closed) {
		return breaker.None, q.ResetEndpointFailures(ctx, l.EndpointID)
	}

	row, err := q.LockEndpointCircuit(ctx, l.EndpointID)
	if err != nil {
		return breaker.None, fmt.Errorf("lock endpoint circuit: %w", err)
	}
	cur := breaker.State{
		Circuit:     breaker.Circuit(row.CircuitState),
		Failures:    int(row.ConsecutiveFailures),
		Cooldown:    time.Duration(row.CircuitCooldownMs) * time.Millisecond,
		ProbeStream: row.CircuitProbeStreamID,
	}
	d.mu.Lock()
	next, t, probeIn := breaker.Next(cur, o, d.breaker, d.rng)
	d.mu.Unlock()
	if next == cur {
		return breaker.None, nil
	}
	if err := q.UpdateEndpointCircuit(ctx, gen.UpdateEndpointCircuitParams{
		ID:                  l.EndpointID,
		CircuitState:        string(next.Circuit),
		ConsecutiveFailures: int32(min(next.Failures, 1<<30)), //nolint:gosec // clamped
		CooldownMs:          next.Cooldown.Milliseconds(),
		NextProbeInMs:       probeIn.Milliseconds(),
	}); err != nil {
		return breaker.None, fmt.Errorf("update endpoint circuit: %w", err)
	}
	if t == breaker.Recovered {
		if _, err := q.UnparkEndpointStreams(ctx, gen.UnparkEndpointStreamsParams{
			EndpointID: l.EndpointID, SpreadMs: d.unparkSpread.Milliseconds(),
		}); err != nil {
			return breaker.None, fmt.Errorf("unpark streams: %w", err)
		}
	}
	return t, nil
}

type endpointHealth int

const (
	neutral endpointHealth = iota
	healthy
	failed
)

// health classifies one attempt for the breaker. Any HTTP answer below 500
// proves the endpoint is up (a 404 is a config problem, not an outage),
// except 429, which is the endpoint asking for less traffic, not failing.
// No answer at all (timeout, refused, reset) is a failure; an SSRF block or a
// bad URL says nothing about the endpoint's health.
func health(r *queue.AttemptRecord) endpointHealth {
	switch {
	case r.StatusCode == http.StatusTooManyRequests:
		return neutral
	case r.StatusCode >= 500:
		return failed
	case r.StatusCode > 0:
		return healthy
	case r.Outcome == "retryable" || r.Outcome == "timeout":
		return failed
	default:
		return neutral
	}
}

// BreakerTick runs the breaker's periodic work. Safe to run in every
// dispatcher concurrently.
//
//  1. park runnable streams of open endpoints
//  2. send half-open probes that never reported back back to open
//  3. for each open endpoint whose cooldown elapsed, unpark exactly one
//     stream and mark it the probe (SKIP LOCKED: one probe per endpoint
//     across all dispatchers)
func (d *Driver) BreakerTick(ctx context.Context) (parked, expired, probes int64, err error) {
	if parked, err = d.s.ParkOpenEndpointStreams(ctx); err != nil {
		return 0, 0, 0, fmt.Errorf("park streams: %w", err)
	}
	if expired, err = d.s.ExpireStaleProbes(ctx); err != nil {
		return parked, 0, 0, fmt.Errorf("expire probes: %w", err)
	}
	err = pgx.BeginFunc(ctx, d.s.Pool, func(tx pgx.Tx) error {
		q := d.s.WithTx(tx)
		due, cerr := q.ClaimDueProbes(ctx)
		if cerr != nil {
			return fmt.Errorf("claim probes: %w", cerr)
		}
		for _, ep := range due {
			stream, perr := q.PickProbeStream(ctx, ep.ID)
			if store.IsNoRows(perr) {
				continue // nothing queued for it; stay open until work arrives
			}
			if perr != nil {
				return fmt.Errorf("pick probe stream: %w", perr)
			}
			if serr := q.StartProbe(ctx, gen.StartProbeParams{EndpointID: ep.ID, StreamID: stream}); serr != nil {
				return fmt.Errorf("start probe: %w", serr)
			}
			probes++
		}
		return nil
	})
	return parked, expired, probes, err
}

// Release implements queue.Queue.
func (d *Driver) Release(ctx context.Context, l *queue.Lease, notBefore time.Duration) error {
	token := l.Token
	n, err := d.s.ReleaseStream(ctx, gen.ReleaseStreamParams{
		ID: l.StreamID, LockToken: &token, Ordered: l.Ordered, NotBeforeMs: notBefore.Milliseconds(),
	})
	if err != nil {
		return fmt.Errorf("release stream: %w", err)
	}
	if n == 0 {
		return queue.ErrLeaseLost
	}
	return nil
}

// Reap implements queue.Queue.
func (d *Driver) Reap(ctx context.Context, limit int) (int, error) {
	ids, err := d.s.ReapExpiredLeases(ctx, int32(limit)) //nolint:gosec // small constant
	if err != nil {
		return 0, fmt.Errorf("reap: %w", err)
	}
	return len(ids), nil
}

// Stats implements queue.Queue.
func (d *Driver) Stats(ctx context.Context) (queue.Stats, error) {
	r, err := d.s.QueueStats(ctx)
	if err != nil {
		return queue.Stats{}, fmt.Errorf("queue stats: %w", err)
	}
	return queue.Stats{
		RunnableStreams:   r.RunnableStreams,
		LeasedStreams:     r.LeasedStreams,
		PendingDeliveries: r.PendingDeliveries,
		OldestDue:         time.Duration(r.OldestDueSeconds * float64(time.Second)),
	}, nil
}

// ── column builders for the batched statements ──────────────────────────────

type attemptCols struct {
	ids, deliveryIDs                 []uuid.UUID
	attempts, durations, statuses    []int32
	startedAts                       []time.Time
	outcomes, classes, details, tids []string
	bodies                           [][]byte
}

func (c *attemptCols) add(deliveryID uuid.UUID, r *queue.AttemptRecord) {
	c.ids = append(c.ids, r.ID)
	c.deliveryIDs = append(c.deliveryIDs, deliveryID)
	c.attempts = append(c.attempts, r.Attempt)
	c.startedAts = append(c.startedAts, r.StartedAt)
	c.durations = append(c.durations, int32(r.Duration.Milliseconds())) //nolint:gosec // <= 60s
	c.outcomes = append(c.outcomes, r.Outcome)
	c.statuses = append(c.statuses, int32(r.StatusCode)) //nolint:gosec // HTTP status
	if r.RespBody == nil {
		r.RespBody = []byte{}
	}
	c.bodies = append(c.bodies, r.RespBody)
	c.classes = append(c.classes, r.ErrorClass)
	c.details = append(c.details, truncate(r.ErrorDetail, 500))
	c.tids = append(c.tids, r.TraceID)
}

func (c *attemptCols) params(l *queue.Lease) gen.InsertAttemptsParams {
	worker := l.WorkerID
	return gen.InsertAttemptsParams{
		TenantID: l.TenantID, EndpointID: l.EndpointID, WorkerID: &worker,
		Ids: c.ids, DeliveryIds: c.deliveryIDs, Attempts: c.attempts, StartedAts: c.startedAts,
		DurationMs: c.durations, Outcomes: c.outcomes, StatusCodes: c.statuses, RespBodies: c.bodies,
		ErrorClasses: c.classes, ErrorDetails: c.details, TraceIds: c.tids,
	}
}

type completeCols struct {
	ids                         []uuid.UUID
	attempts                    []int32
	states, reasons, lastErrors []string
}

func (c *completeCols) add(r queue.Result, state string) {
	c.ids = append(c.ids, r.DeliveryID)
	c.attempts = append(c.attempts, r.Attempts)
	c.states = append(c.states, state)
	c.reasons = append(c.reasons, r.DeadReason)
	c.lastErrors = append(c.lastErrors, truncate(r.LastError, 500))
}

func (c *completeCols) params(stream int64) gen.CompleteDeliveriesParams {
	return gen.CompleteDeliveriesParams{
		StreamID: stream, Ids: c.ids, Attempts: c.attempts, FinalStates: c.states,
		DeadReasons: c.reasons, LastErrors: c.lastErrors,
	}
}

type rescheduleCols struct {
	ids        []uuid.UUID
	attempts   []int32
	delays     []int64
	lastErrors []string
}

func (c *rescheduleCols) add(r queue.Result) {
	c.ids = append(c.ids, r.DeliveryID)
	c.attempts = append(c.attempts, r.Attempts)
	c.delays = append(c.delays, r.Delay.Milliseconds())
	c.lastErrors = append(c.lastErrors, truncate(r.LastError, 500))
}

func (c *rescheduleCols) params(stream int64) gen.RescheduleDeliveriesParams {
	return gen.RescheduleDeliveriesParams{
		StreamID: stream, Ids: c.ids, Attempts: c.attempts, DelayMs: c.delays, LastErrors: c.lastErrors,
	}
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
