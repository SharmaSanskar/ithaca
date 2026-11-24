package live

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/SharmaSanskar/ithaca/internal/store"
)

// EndpointJSON is the endpoint shape sent in endpoint deltas (and served by
// the dashboard API), so the browser can replace its cached copy wholesale.
type EndpointJSON struct {
	ID                  uuid.UUID  `json:"id"`
	Name                string     `json:"name"`
	URL                 string     `json:"url"`
	State               string     `json:"state"`
	Circuit             string     `json:"circuit"`
	ConsecutiveFailures int32      `json:"consecutive_failures"`
	NextProbeAt         *time.Time `json:"circuit_next_probe_at,omitempty"`
	Depth               int64      `json:"depth"`
	Rev                 int64      `json:"rev"`
}

// PGSnapshotter reads tenant state from Postgres.
type PGSnapshotter struct{ Store *store.Store }

// Snapshot implements Snapshotter: three indexed queries, run once a second
// per watched tenant.
func (p PGSnapshotter) Snapshot(ctx context.Context, tenant uuid.UUID) (Snapshot, error) {
	rows, err := p.Store.LiveEndpoints(ctx, tenant)
	if err != nil {
		return Snapshot{}, err
	}
	dlq, err := p.Store.OpenDeadLetterCount(ctx, tenant)
	if err != nil {
		return Snapshot{}, err
	}
	paused, err := p.Store.PausedStreamCount(ctx, tenant)
	if err != nil {
		return Snapshot{}, err
	}
	s := Snapshot{DLQ: dlq, PausedStreams: paused, Endpoints: make([]SnapshotEndpoint, 0, len(rows))}
	for _, r := range rows {
		name := r.Description
		if name == "" {
			name = r.Url
		}
		s.Endpoints = append(s.Endpoints, SnapshotEndpoint{
			ID: r.ID, Name: name, URL: r.Url, State: r.State, Circuit: r.CircuitState, Depth: r.Depth, UpdatedAt: r.UpdatedAt,
			Data: EndpointJSON{
				ID: r.ID, Name: name, URL: r.Url, State: r.State, Circuit: r.CircuitState,
				ConsecutiveFailures: r.ConsecutiveFailures, NextProbeAt: r.CircuitNextProbeAt,
				Depth: r.Depth, Rev: r.UpdatedAt.UnixMicro(),
			},
		})
	}
	return s, nil
}
