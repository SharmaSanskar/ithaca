// Package queue is the driver boundary between the worker pool and whatever
// stores pending deliveries.
//
// The unit of work handed out is a STREAM (a lane), not a message: a lease
// grants exclusive use of one (subscription, stream_key) lane, and the holder
// drains it in batches. Ordering falls out of that exclusivity; see
// docs/DECISIONS/adr-0003-stream-leasing.md.
//
// Postgres (package queue/pg) is the only driver. The interface exists so the
// worker pool is testable against an in-memory fake, and so a transport such
// as SQS could carry runnable work later while Postgres stays the scheduler.
package queue

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
)

// ErrLeaseLost means the lease expired and was reaped, and possibly handed to
// another worker. The holder must stop touching the stream immediately: no
// ack, no release. Whatever it was doing will be redelivered, which is the
// at-least-once contract working as intended.
var ErrLeaseLost = errors.New("queue: lease lost")

// Endpoint is the delivery target, snapshotted at lease time.
type Endpoint struct {
	URL            string
	State          string // "enabled" | "disabled" | "auto_disabled"
	Timeout        time.Duration
	MaxConcurrency int
	KeyID          string
	Secret         []byte
	// Circuit is the breaker state at lease time: "closed", "open" or
	// "half_open".
	Circuit string
}

// Lease is exclusive, time-bounded ownership of one stream.
type Lease struct {
	StreamID       int64
	Token          uuid.UUID // the fencing token
	TenantID       uuid.UUID
	SubscriptionID uuid.UUID
	EndpointID     uuid.UUID
	Ordered        bool
	StreamKey      string
	Endpoint       Endpoint
	WorkerID       string
	TTL            time.Duration
	AcquiredAt     time.Time
	// IsProbe: the endpoint's circuit is half-open and this is the one stream
	// allowed to test it. The holder delivers exactly one message.
	IsProbe bool
}

// LeaseRequest asks for up to Max streams.
type LeaseRequest struct {
	WorkerID     string
	Max          int
	MinLease     time.Duration
	LeasePadding time.Duration
}

// Message is one pending delivery, joined with its event.
type Message struct {
	DeliveryID     uuid.UUID
	EventID        uuid.UUID
	Seq            int64
	Attempt        int32 // attempts made so far
	Due            bool  // next_attempt_at <= now(), by the database's clock
	EventType      string
	Payload        []byte
	Traceparent    string
	EventCreatedAt time.Time
}

// Disposition is what happens to a delivery after an attempt.
type Disposition int

const (
	// Succeeded: move to history as succeeded.
	Succeeded Disposition = iota + 1
	// Dead: move to history as dead (the dead-letter queue).
	Dead
	// Retry: stay pending; Attempt is consumed; due again after Delay.
	Retry
	// Defer: stay pending WITHOUT consuming an attempt (rate limited).
	Defer
)

// AttemptRecord is one row for delivery_attempts.
type AttemptRecord struct {
	ID          uuid.UUID
	Attempt     int32
	StartedAt   time.Time
	Duration    time.Duration
	Outcome     string
	StatusCode  int
	RespBody    []byte
	ErrorClass  string
	ErrorDetail string
	TraceID     string
}

// Result is the outcome of processing one message, ready to ack.
type Result struct {
	DeliveryID  uuid.UUID
	Disposition Disposition
	// Attempts is the delivery's attempt count AFTER this result.
	Attempts   int32
	Delay      time.Duration // Retry / Defer
	DeadReason string
	LastError  string
	Record     *AttemptRecord // nil = nothing to log
}

// AckOutcome reports side effects of an ack beyond the deliveries themselves.
type AckOutcome struct {
	// Breaker is the circuit transition this ack caused: "", "opened",
	// "reopened" or "closed".
	Breaker string
	// AutoDisabled: the endpoint answered 410 Gone and was disabled.
	AutoDisabled bool
	// Paused: an ordered stream's head died and the stream stopped.
	Paused bool
}

// Stats are queue-wide gauges.
type Stats struct {
	RunnableStreams   int64
	LeasedStreams     int64
	PendingDeliveries int64
	OldestDue         time.Duration
}

// Queue is the driver interface. Every method is short and commits on its
// own; none is ever held open across an outbound HTTP call.
type Queue interface {
	// Lease takes up to req.Max runnable streams.
	Lease(ctx context.Context, req LeaseRequest) ([]*Lease, error)
	// Fetch returns up to limit pending messages from a leased stream.
	Fetch(ctx context.Context, l *Lease, limit int) ([]Message, error)
	// Renew extends a lease. ErrLeaseLost if it is gone.
	Renew(ctx context.Context, l *Lease) error
	// Ack records results in one fenced transaction. ErrLeaseLost if fenced out.
	Ack(ctx context.Context, l *Lease, results []Result) (AckOutcome, error)
	// Release returns the stream, invisible for at least notBefore.
	Release(ctx context.Context, l *Lease, notBefore time.Duration) error
	// Reap reclaims up to limit expired leases and returns how many.
	Reap(ctx context.Context, limit int) (int, error)
	// Stats reports queue-wide gauges.
	Stats(ctx context.Context) (Stats, error)
}
