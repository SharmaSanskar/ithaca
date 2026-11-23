package worker

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/SharmaSanskar/ithaca/internal/deliver"
	"github.com/SharmaSanskar/ithaca/internal/platform/otelx"
	"github.com/SharmaSanskar/ithaca/internal/queue"
	"github.com/SharmaSanskar/ithaca/internal/retry"
)

// Sender is the HTTP side of an attempt; *deliver.Client satisfies it.
type Sender interface {
	Deliver(ctx context.Context, r deliver.Request) deliver.Response
}

// HTTPExecutor turns one HTTP attempt into a queue.Result: it owns the retry
// policy and the classification rules that depend on attempt history.
type HTTPExecutor struct {
	send   Sender
	policy retry.Policy
	m      *Metrics
	tracer trace.Tracer
	now    func() time.Time

	mu  sync.Mutex
	rng *rand.Rand
}

// NewExecutor builds an executor.
func NewExecutor(send Sender, policy retry.Policy, m *Metrics) *HTTPExecutor {
	return &HTTPExecutor{
		send: send, policy: policy, m: m,
		tracer: otel.Tracer("github.com/SharmaSanskar/ithaca/worker"),
		now:    time.Now,
		rng:    rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64())), //nolint:gosec // jitter, not security
	}
}

// Some error classes are retryable at first but should give up early: a DNS
// name that still does not resolve after a few tries is a config error, and an
// invalid certificate is worth a few retries (certs get renewed) but not 12h.
var earlyGiveUp = map[string]int{"dns": 3, "tls": 4}

// Execute implements Executor.
func (e *HTTPExecutor) Execute(ctx context.Context, l *queue.Lease, m queue.Message) (queue.Result, bool) {
	attempt := int(m.Attempt) + 1

	opts := []trace.SpanStartOption{
		trace.WithNewRoot(), // a new trace per attempt; see otelx.LinkFromTraceparent
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("ithaca.event_id", m.EventID.String()),
			attribute.String("ithaca.delivery_id", m.DeliveryID.String()),
			attribute.String("ithaca.endpoint_id", l.EndpointID.String()),
			attribute.String("ithaca.event_type", m.EventType),
			attribute.Int("ithaca.attempt", attempt),
			attribute.Int64("ithaca.seq", m.Seq),
			attribute.Bool("ithaca.ordered", l.Ordered),
		),
	}
	if link, ok := otelx.LinkFromTraceparent(ctx, m.Traceparent); ok {
		opts = append(opts, trace.WithLinks(link))
	}
	ctx, span := e.tracer.Start(ctx, "webhook.deliver", opts...)
	defer span.End()

	started := e.now()
	resp := e.send.Deliver(ctx, deliver.Request{
		URL:         l.Endpoint.URL,
		Timeout:     l.Endpoint.Timeout,
		EventID:     m.EventID,
		DeliveryID:  m.DeliveryID,
		EventType:   m.EventType,
		Attempt:     attempt,
		Body:        m.Payload,
		KeyID:       l.Endpoint.KeyID,
		Secret:      l.Endpoint.Secret,
		Traceparent: otelx.Traceparent(ctx),
	})

	span.SetAttributes(
		attribute.String("ithaca.outcome", string(resp.Outcome)),
		attribute.Int("http.response.status_code", resp.StatusCode),
	)
	if resp.Outcome == deliver.Canceled {
		span.SetStatus(codes.Unset, "canceled")
		return queue.Result{}, false
	}
	e.m.Attempts.WithLabelValues(string(resp.Outcome)).Inc()
	e.m.AttemptLatency.Observe(resp.Duration.Seconds())

	rec := &queue.AttemptRecord{
		ID:          uuid.Must(uuid.NewV7()),
		Attempt:     int32(attempt), //nolint:gosec // bounded by MaxAttempts
		StartedAt:   started,
		Duration:    resp.Duration,
		Outcome:     string(resp.Outcome),
		StatusCode:  resp.StatusCode,
		RespBody:    resp.Body,
		ErrorClass:  resp.Class,
		ErrorDetail: errString(resp.Err),
		TraceID:     traceID(span),
	}
	r := queue.Result{DeliveryID: m.DeliveryID, Record: rec, LastError: describe(resp)}

	switch resp.Outcome {
	case deliver.Success:
		r.Disposition = queue.Succeeded
		r.Attempts = int32(attempt) //nolint:gosec // bounded
		e.m.EndToEnd.Observe(e.now().Sub(m.EventCreatedAt).Seconds())

	case deliver.RateLimited:
		// A scheduling decision, not a failure: the attempt is NOT consumed,
		// or a throttling subscriber would burn the whole retry budget in
		// seconds and dead-letter perfectly good traffic.
		r.Disposition = queue.Defer
		r.Attempts = m.Attempt
		r.Delay = resp.RetryAfter
		if r.Delay == 0 {
			r.Delay = e.backoff(attempt)
		}

	case deliver.Permanent:
		r.Disposition = queue.Dead
		r.Attempts = int32(attempt) //nolint:gosec // bounded
		r.DeadReason = resp.Class

	default: // Retryable, Timeout
		r.Attempts = int32(attempt) //nolint:gosec // bounded
		if limit, ok := earlyGiveUp[resp.Class]; ok && attempt >= limit {
			r.Disposition = queue.Dead
			r.DeadReason = resp.Class
		} else if e.policy.Exhausted(attempt) {
			r.Disposition = queue.Dead
			r.DeadReason = "max_attempts:" + resp.Class
		} else {
			r.Disposition = queue.Retry
			r.Delay = e.backoff(attempt)
		}
	}

	if r.Disposition != queue.Succeeded {
		span.SetStatus(codes.Error, r.LastError)
	}
	return r, true
}

func (e *HTTPExecutor) backoff(failed int) time.Duration {
	e.mu.Lock()
	defer e.mu.Unlock()
	return retry.NextBackoff(failed, e.policy, e.rng)
}

func describe(r deliver.Response) string {
	switch {
	case r.StatusCode != 0:
		return fmt.Sprintf("HTTP %d", r.StatusCode)
	case r.Err != nil:
		return r.Class + ": " + r.Err.Error()
	default:
		return r.Class
	}
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func traceID(span trace.Span) string {
	sc := span.SpanContext()
	if !sc.IsValid() {
		return ""
	}
	return sc.TraceID().String()
}
