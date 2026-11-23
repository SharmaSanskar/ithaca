// Package janitor holds the worker service's periodic cleanup jobs.
//
// Today that is one job: expiring idempotency keys. It is the honest
// small-scale substitute for the full design's hourly partitions, where expiry
// is a DROP TABLE. A batched DELETE is fine until the table is large enough
// that WAL volume and vacuum churn show up on a graph. ARCHITECTURE.md
// documents that as the trigger for partitioning.
package janitor

import (
	"context"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/SharmaSanskar/ithaca/internal/platform/metrics"
	"github.com/SharmaSanskar/ithaca/internal/store"
)

// Janitor periodically deletes expired idempotency keys.
type Janitor struct {
	store     *store.Store
	log       *slog.Logger
	interval  time.Duration
	batchSize int32
	deleted   prometheus.Counter
}

// New builds a janitor.
func New(s *store.Store, log *slog.Logger, reg prometheus.Registerer) *Janitor {
	j := &Janitor{
		store: s, log: log, interval: 5 * time.Minute, batchSize: 1000,
		deleted: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metrics.Namespace, Subsystem: "janitor", Name: "idempotency_keys_deleted_total",
			Help: "Expired idempotency keys removed.",
		}),
	}
	reg.MustRegister(j.deleted)
	return j
}

// Run sweeps once immediately and then on every tick until ctx is done.
func (j *Janitor) Run(ctx context.Context) error {
	t := time.NewTicker(j.interval)
	defer t.Stop()
	for {
		j.sweep(ctx)
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

func (j *Janitor) sweep(ctx context.Context) {
	total := int64(0)
	for ctx.Err() == nil {
		n, err := j.store.DeleteExpiredIdempotencyKeys(ctx, j.batchSize)
		if err != nil {
			if ctx.Err() == nil {
				j.log.Warn("idempotency sweep failed", "error", err)
			}
			return
		}
		total += n
		j.deleted.Add(float64(n))
		if n < int64(j.batchSize) {
			break
		}
	}
	if total > 0 {
		j.log.Info("expired idempotency keys deleted", "count", total)
	}
}
