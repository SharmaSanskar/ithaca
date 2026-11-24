package live

import (
	"encoding/json"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/SharmaSanskar/ithaca/internal/queue"
)

// IngestObserver adapts the publisher to ingest.Observer.
type IngestObserver struct{ P *Publisher }

// Ingested implements ingest.Observer.
func (o IngestObserver) Ingested(tenant, eventID uuid.UUID, eventType string) {
	o.P.Ingested(tenant, eventID, eventType)
}

// DispatchObserver adapts the publisher to worker.Observer.
type DispatchObserver struct{ P *Publisher }

// Acked implements worker.Observer.
func (o DispatchObserver) Acked(l *queue.Lease, results []queue.Result, out queue.AckOutcome) {
	for _, r := range results {
		outcome := ""
		switch r.Disposition {
		case queue.Succeeded:
			outcome = Delivered
		case queue.Retry:
			outcome = Retrying
		case queue.Defer:
			outcome = RateLimited
		case queue.Dead:
			outcome = DeadLetter
		}
		attempt, latency := int(r.Attempts), int64(0)
		if r.Record != nil {
			attempt = int(r.Record.Attempt)
			latency = r.Record.Duration.Nanoseconds()
		}
		o.P.Attempt(l.TenantID, l.EndpointID, r.EventID, r.DeliveryID, r.EventType, outcome, attempt, time.Duration(latency))

		if r.Disposition == queue.Dead {
			data, _ := json.Marshal(map[string]any{
				"id": r.DeliveryID, "event_id": r.EventID, "event_type": r.EventType,
				"endpoint_id": l.EndpointID, "reason": r.DeadReason, "attempts": r.Attempts,
				"blocks_stream": out.Paused,
			})
			o.P.Delta(l.TenantID, Delta{Kind: DeltaDeadLetter, ID: r.DeliveryID.String(), Data: data})
		}
	}
	if out.Breaker != "" || out.AutoDisabled {
		// No payload: the hub re-reads the endpoint from Postgres.
		o.P.Delta(l.TenantID, Delta{Kind: DeltaEndpoint, ID: l.EndpointID.String()})
	}
	if out.Paused {
		data, _ := json.Marshal(map[string]any{
			"stream_id": l.StreamID, "endpoint_id": l.EndpointID, "stream_key": l.StreamKey, "state": "paused",
		})
		o.P.Delta(l.TenantID, Delta{Kind: DeltaStream, ID: strconv.FormatInt(l.StreamID, 10), Data: data})
	}
}
