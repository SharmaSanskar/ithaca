// Package pg is the Postgres queue driver: `FOR UPDATE SKIP LOCKED` stream
// leasing, fenced acks, and move-to-history completion. The SQL lives in
// internal/store/queries/queue.sql.
package pg

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/SharmaSanskar/ithaca/internal/queue"
	"github.com/SharmaSanskar/ithaca/internal/store"
	"github.com/SharmaSanskar/ithaca/internal/store/gen"
)

// Driver implements queue.Queue on Postgres.
type Driver struct {
	s   *store.Store
	now func() time.Time
}

// New returns a driver over the given store.
func New(s *store.Store) *Driver { return &Driver{s: s, now: time.Now} }

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
			},
			WorkerID:   req.WorkerID,
			TTL:        time.Duration(r.LeaseMs) * time.Millisecond,
			AcquiredAt: now,
		})
	}
	return out, nil
}

// Fetch implements queue.Queue.
func (d *Driver) Fetch(ctx context.Context, l *queue.Lease, limit int) ([]queue.Message, error) {
	max := int32(limit) //nolint:gosec // bounded by config
	if l.Ordered {
		rows, err := d.s.FetchOrdered(ctx, gen.FetchOrderedParams{StreamID: l.StreamID, MaxRows: max})
		if err != nil {
			return nil, fmt.Errorf("fetch ordered: %w", err)
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
//
// synchronous_commit is off for this transaction only. Losing the last few
// hundred milliseconds of acks in a database crash means redelivering a
// handful of messages, which at-least-once already permits. Ingest keeps full
// durability: losing an ingest commit would lose a customer's event.
func (d *Driver) Ack(ctx context.Context, l *queue.Lease, results []queue.Result) error {
	if len(results) == 0 {
		return nil
	}
	var att attemptCols
	var done completeCols
	var resched rescheduleCols
	for _, r := range results {
		if r.Record != nil {
			att.add(r.DeliveryID, r.Record)
		}
		switch r.Disposition {
		case queue.Succeeded:
			done.add(r, "succeeded")
		case queue.Dead:
			done.add(r, "dead")
		case queue.Retry, queue.Defer:
			resched.add(r)
		default:
			return fmt.Errorf("ack: delivery %s has no disposition", r.DeliveryID)
		}
	}

	token := l.Token
	return pgx.BeginFunc(ctx, d.s.Pool, func(tx pgx.Tx) error {
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
		return nil
	})
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
