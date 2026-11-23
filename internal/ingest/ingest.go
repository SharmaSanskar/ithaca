// Package ingest is the write path: POST /v1/events.
//
// Its contract is narrow and strict. Ingest durably records the event, fans it
// out into per-subscription deliveries and returns 202, all inside ONE
// transaction, and it makes zero outbound network calls. The transaction is
// the no-loss boundary: once a client sees 202, every matching subscription has
// a delivery row. Making no outbound calls is what keeps ingest available
// while every subscriber is down.
package ingest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/SharmaSanskar/ithaca/internal/store"
	"github.com/SharmaSanskar/ithaca/internal/store/gen"
)

// IdempotencyTTL is how long a key is remembered. Reusing a key after this is
// a new request, and the API docs say so.
const IdempotencyTTL = 24 * time.Hour

// ErrIdempotencyMismatch means the key was already used with a different body.
// Per draft-ietf-httpapi-idempotency-key-header that is a 422: we must neither
// process the new body nor replay the old response, because both are silently
// wrong for the caller.
var ErrIdempotencyMismatch = errors.New("idempotency key reused with a different request")

// Request is one ingest call, already authenticated.
type Request struct {
	TenantID       uuid.UUID
	Method, Path   string
	Body           []byte // raw request body, exactly as received
	IdempotencyKey string // "" = no dedupe
	Traceparent    string
}

// Result is what to send back.
type Result struct {
	Status     int
	Body       []byte
	EventID    uuid.UUID
	Replayed   bool // true: this is a stored response for an earlier request
	Deliveries int  // deliveries created (0 on replay)
}

// Accepted is the 202 response body.
type Accepted struct {
	ID        uuid.UUID `json:"id"`
	Type      string    `json:"type"`
	CreatedAt time.Time `json:"created_at"`
}

// Service runs the ingest transaction.
type Service struct {
	store   *store.Store
	metrics *Metrics
	now     func() time.Time
}

// NewService builds the ingest service. metrics may be nil in tests.
func NewService(s *store.Store, m *Metrics) *Service {
	return &Service{store: s, metrics: m, now: time.Now}
}

// errKeyTaken aborts the transaction when another request owns the key.
var errKeyTaken = errors.New("idempotency key already claimed")

// Ingest validates, stores and fans out one event.
func (s *Service) Ingest(ctx context.Context, req Request) (Result, error) {
	env, perr := ParseEnvelope(req.Body)
	if perr != nil {
		s.count("invalid")
		return Result{}, perr
	}

	// Everything in the response is decided before the transaction starts, so
	// the idempotency row can be written complete in its first INSERT. Postgres
	// stores microseconds; truncating here means the replayed created_at is
	// byte-identical to the original.
	now := s.now().UTC().Truncate(time.Microsecond)
	eventID := uuid.Must(uuid.NewV7())
	respBody, merr := json.Marshal(Accepted{ID: eventID, Type: env.Type, CreatedAt: now})
	if merr != nil {
		return Result{}, merr
	}

	var reqHash []byte
	if req.IdempotencyKey != "" {
		reqHash = RequestHash(req.Method, req.Path, req.Body)
	}

	// The loop only repeats in one narrow race: the key we collided with
	// expired between our claim attempt and our read of it.
	for range 3 {
		deliveries := 0
		start := time.Now()
		txErr := s.store.InTx(ctx, func(q *gen.Queries) error {
			if req.IdempotencyKey != "" {
				_, err := q.ClaimIdempotencyKey(ctx, gen.ClaimIdempotencyKeyParams{
					TenantID:     req.TenantID,
					Key:          req.IdempotencyKey,
					RequestHash:  reqHash,
					ResponseCode: 202,
					ResponseBody: respBody,
					EventID:      eventID,
					CreatedAt:    now,
					ExpiresAt:    now.Add(IdempotencyTTL),
				})
				if store.IsNoRows(err) {
					return errKeyTaken
				}
				if err != nil {
					return fmt.Errorf("claim idempotency key: %w", err)
				}
			}

			if err := q.InsertEvent(ctx, gen.InsertEventParams{
				ID:             eventID,
				TenantID:       req.TenantID,
				EventType:      env.Type,
				OrderingKey:    nonEmpty(env.OrderingKey),
				Payload:        env.Payload,
				IdempotencyKey: nonEmpty(req.IdempotencyKey),
				Traceparent:    nonEmpty(req.Traceparent),
				CreatedAt:      now,
			}); err != nil {
				return fmt.Errorf("insert event: %w", err)
			}

			n, err := fanout(ctx, q, req.TenantID, eventID, env)
			deliveries = n
			return err
		})
		if s.metrics != nil {
			s.metrics.TxDuration.Observe(time.Since(start).Seconds())
		}

		if errors.Is(txErr, errKeyTaken) {
			prev, gerr := s.store.GetIdempotencyKey(ctx, gen.GetIdempotencyKeyParams{
				TenantID: req.TenantID, Key: req.IdempotencyKey,
			})
			if store.IsNoRows(gerr) {
				continue // expired in between: claim again
			}
			if gerr != nil {
				return Result{}, fmt.Errorf("read idempotency key: %w", gerr)
			}
			if !bytes.Equal(prev.RequestHash, reqHash) {
				s.count("conflict")
				return Result{}, ErrIdempotencyMismatch
			}
			s.count("replayed")
			return Result{Status: int(prev.ResponseCode), Body: prev.ResponseBody, EventID: prev.EventID, Replayed: true}, nil
		}
		if txErr != nil {
			return Result{}, txErr
		}

		s.count("accepted")
		if s.metrics != nil {
			s.metrics.Deliveries.Add(float64(deliveries))
			s.metrics.Fanout.Observe(float64(deliveries))
		}
		return Result{Status: 202, Body: respBody, EventID: eventID, Deliveries: deliveries}, nil
	}
	return Result{}, errors.New("idempotency key churned repeatedly; retry the request")
}

// fanout creates one delivery per matching subscription.
//
// Subscriptions come back ordered by id and each one touches exactly one
// stream row, so every concurrent ingest transaction takes its stream locks in
// the same global order. That consistent order is what makes concurrent
// fanouts deadlock-free.
//
// Fanout is inline. The full design moves tenants with hundreds of matching
// subscriptions to an outbox expanded by the worker; at the scale this project
// runs, inline keeps ingest a single transaction with no second moving part.
func fanout(ctx context.Context, q *gen.Queries, tenant, eventID uuid.UUID, env Envelope) (int, error) {
	subs, err := q.ListFanoutSubscriptions(ctx, tenant)
	if err != nil {
		return 0, fmt.Errorf("list subscriptions: %w", err)
	}
	n := 0
	for _, sub := range subs {
		if !MatchesEventType(sub.EventTypes, env.Type) {
			continue
		}
		if _, err := q.EnqueueDelivery(ctx, gen.EnqueueDeliveryParams{
			DeliveryID:     uuid.Must(uuid.NewV7()),
			TenantID:       tenant,
			SubscriptionID: sub.ID,
			EndpointID:     sub.EndpointID,
			EventID:        eventID,
			StreamKey:      StreamKey(sub.Ordered, sub.LaneCount, env.OrderingKey, eventID),
			Ordered:        sub.Ordered,
		}); err != nil {
			return n, fmt.Errorf("enqueue delivery for subscription %s: %w", sub.ID, err)
		}
		n++
	}
	return n, nil
}

// RequestHash fingerprints a request for idempotency: method, path and the raw
// body bytes. Hashing the raw bytes (not a re-serialized form) means two
// semantically equal bodies with different whitespace count as different
// requests — strict on purpose, since a client retrying sends identical bytes.
func RequestHash(method, path string, body []byte) []byte {
	h := sha256.New()
	h.Write([]byte(method))
	h.Write([]byte{'\n'})
	h.Write([]byte(path))
	h.Write([]byte{'\n'})
	h.Write(body)
	return h.Sum(nil)
}

func (s *Service) count(result string) {
	if s.metrics != nil {
		s.metrics.Events.WithLabelValues(result).Inc()
	}
}

func nonEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
