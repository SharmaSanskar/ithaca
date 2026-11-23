// Package dlq replays dead letters.
//
// A dead letter is a deliveries_history row with final_state = 'dead'; there
// is no separate DLQ table to drift out of sync. Replay re-creates it as a new
// pending delivery: NEW delivery id, SAME event id. The subscriber sees the
// same Ithaca-Id and can dedupe a replay it already processed.
package dlq

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/SharmaSanskar/ithaca/internal/store"
	"github.com/SharmaSanskar/ithaca/internal/store/gen"
)

// MaxBatch caps one replay call. Bigger replays are several calls: one
// enormous transaction would hold locks and bloat WAL in a single burst.
const MaxBatch = 1000

// Request selects dead letters to replay. Empty IDs means "the oldest dead
// letters" (optionally for one endpoint), up to Limit.
type Request struct {
	TenantID   uuid.UUID
	IDs        []uuid.UUID
	EndpointID *uuid.UUID
	Limit      int
}

// Result reports what happened.
type Result struct {
	Replayed int // new deliveries created
	Resumed  int // of which: put back at their original position in a paused ordered stream
	Skipped  int // the stream no longer exists (its subscription was deleted)
}

// Replayer creates replay deliveries.
type Replayer struct {
	Store *store.Store
	// RatePerEndpoint spaces replayed deliveries for one endpoint this many
	// per second. Replaying 500 dead letters to an endpoint that just
	// recovered must ramp, not arrive as one burst that knocks it over again.
	RatePerEndpoint float64
}

// Replay claims and re-creates dead letters in one transaction.
//
// Idempotent under a double-click: the claim sets replayed_at and only
// matches rows where it is NULL. A concurrent second request blocks on those
// row locks, re-checks after the first commits, and claims nothing.
func (r *Replayer) Replay(ctx context.Context, req Request) (Result, error) {
	var res Result
	limit := req.Limit
	if limit <= 0 || limit > MaxBatch {
		limit = MaxBatch
	}
	if len(req.IDs) > MaxBatch {
		return res, fmt.Errorf("at most %d ids per replay", MaxBatch)
	}
	ids := req.IDs
	if ids == nil {
		ids = []uuid.UUID{}
	}
	spacing := time.Duration(float64(time.Second) / max(r.RatePerEndpoint, 0.001))

	err := r.Store.InTx(ctx, func(q *gen.Queries) error {
		res = Result{}
		rows, err := q.ClaimDeadLetters(ctx, gen.ClaimDeadLettersParams{
			TenantID: req.TenantID, Ids: ids, EndpointID: req.EndpointID, MaxRows: int32(limit), //nolint:gosec // <= MaxBatch
		})
		if err != nil {
			return fmt.Errorf("claim dead letters: %w", err)
		}
		perEndpoint := map[uuid.UUID]int{}
		for _, d := range rows {
			n := perEndpoint[d.EndpointID]
			perEndpoint[d.EndpointID]++
			newID := uuid.Must(uuid.NewV7())
			row, err := q.ReplayDelivery(ctx, gen.ReplayDeliveryParams{
				DeadID: &d.ID, StreamID: d.StreamID, DelayMs: (time.Duration(n) * spacing).Milliseconds(),
				OriginalSeq: d.Seq, NewID: newID, TenantID: d.TenantID,
				SubscriptionID: d.SubscriptionID, EndpointID: d.EndpointID, EventID: d.EventID,
			})
			if store.IsNoRows(err) {
				res.Skipped++
				continue
			}
			if err != nil {
				return fmt.Errorf("replay %s: %w", d.ID, err)
			}
			if err := q.SetReplayedAs(ctx, gen.SetReplayedAsParams{ID: d.ID, ReplayedAs: &newID}); err != nil {
				return fmt.Errorf("record replay of %s: %w", d.ID, err)
			}
			res.Replayed++
			// Back at its original seq means it resumed a paused ordered
			// stream; an append always gets a higher seq than it had.
			if row.Seq == d.Seq {
				res.Resumed++
			}
		}
		return nil
	})
	if err != nil {
		return Result{}, err
	}
	return res, nil
}
