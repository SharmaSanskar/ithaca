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
	"github.com/SharmaSanskar/ithaca/internal/store/gen"
)

// Janitor periodically deletes expired idempotency keys.
type Janitor struct {
	store     *store.Store
	log       *slog.Logger
	interval  time.Duration
	batchSize int32
	deleted   prometheus.Counter
	// Retention, when positive, deletes delivery history, attempts and events
	// older than this. Zero keeps everything.
	Retention time.Duration
	retained  *prometheus.CounterVec
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
	j.retained = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metrics.Namespace, Subsystem: "janitor", Name: "retention_deleted_total",
		Help: "Rows removed by the retention sweep.",
	}, []string{"table"})
	reg.MustRegister(j.deleted, j.retained)
	return j
}

// Run sweeps once immediately and then on every tick until ctx is done.
func (j *Janitor) Run(ctx context.Context) error {
	t := time.NewTicker(j.interval)
	defer t.Stop()
	for {
		j.sweep(ctx)
		j.retain(ctx)
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

// retain applies the retention policy, table by table, in batches. Attempts
// and history go first so events are no longer referenced when their turn
// comes.
func (j *Janitor) retain(ctx context.Context) {
	if j.Retention <= 0 {
		return
	}
	age := int64(j.Retention.Seconds())
	steps := []struct {
		table string
		del   func() (int64, error)
	}{
		{"delivery_attempts", func() (int64, error) {
			return j.store.DeleteOldAttempts(ctx, gen.DeleteOldAttemptsParams{OlderThanSeconds: age, BatchSize: j.batchSize})
		}},
		{"deliveries_history", func() (int64, error) {
			return j.store.DeleteOldHistory(ctx, gen.DeleteOldHistoryParams{OlderThanSeconds: age, BatchSize: j.batchSize})
		}},
		{"events", func() (int64, error) {
			return j.store.DeleteOldEvents(ctx, gen.DeleteOldEventsParams{OlderThanSeconds: age, BatchSize: j.batchSize})
		}},
	}
	for _, st := range steps {
		total := int64(0)
		for ctx.Err() == nil {
			n, err := st.del()
			if err != nil {
				if ctx.Err() == nil {
					j.log.Warn("retention sweep failed", "table", st.table, "error", err)
				}
				break
			}
			total += n
			if n < int64(j.batchSize) {
				break
			}
		}
		if total > 0 {
			j.retained.WithLabelValues(st.table).Add(float64(total))
			j.log.Info("retention: deleted old rows", "table", st.table, "count", total)
		}
	}
}
