package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/SharmaSanskar/ithaca/internal/queue"
)

// Reaper reclaims leases whose holder died without releasing them (SIGKILL,
// OOM kill, network partition).
//
// It runs inside every dispatcher rather than as a separate singleton. It is
// cheap (a partial-index scan over O(active leases) rows) and safe to run
// concurrently (SKIP LOCKED), and it means a cluster that has dispatchers
// always has a reaper — there is no "the reaper pod was down" failure mode.
type Reaper struct {
	Q     queue.Queue
	Every time.Duration
	Log   *slog.Logger
	M     *Metrics
}

// Run reaps until ctx is done.
func (r *Reaper) Run(ctx context.Context) error {
	t := time.NewTicker(r.Every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
		for {
			n, err := r.Q.Reap(ctx, 500)
			if err != nil {
				if ctx.Err() == nil {
					r.Log.Warn("reap failed", "error", err)
				}
				break
			}
			if n > 0 {
				r.M.Reaped.Add(float64(n))
				r.Log.Warn("reclaimed expired leases", "count", n)
			}
			if n < 500 {
				break
			}
		}
	}
}

// StatsLoop publishes queue-wide gauges. Every replica reports the same
// global numbers; dashboards take max() across replicas.
type StatsLoop struct {
	Q     queue.Queue
	Every time.Duration
	Log   *slog.Logger
	M     *Metrics
}

// Run samples until ctx is done.
func (s *StatsLoop) Run(ctx context.Context) error {
	t := time.NewTicker(s.Every)
	defer t.Stop()
	for {
		st, err := s.Q.Stats(ctx)
		switch {
		case err == nil:
			s.M.RunnableStreams.Set(float64(st.RunnableStreams))
			s.M.LeasedStreams.Set(float64(st.LeasedStreams))
			s.M.PendingDeliveries.Set(float64(st.PendingDeliveries))
			s.M.OldestPending.Set(st.OldestDue.Seconds())
		case ctx.Err() == nil:
			s.Log.Warn("queue stats failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}
