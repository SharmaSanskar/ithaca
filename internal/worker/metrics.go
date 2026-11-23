package worker

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/SharmaSanskar/ithaca/internal/platform/metrics"
)

// Metrics instruments the pool, the queue and delivery.
type Metrics struct {
	LeaseBatch     prometheus.Histogram
	DequeueErrors  prometheus.Counter
	LeasesLost     prometheus.Counter
	Reaped         prometheus.Counter
	Panics         prometheus.Counter
	AckDuration    prometheus.Histogram
	AckErrors      prometheus.Counter
	Attempts       *prometheus.CounterVec
	Completed      *prometheus.CounterVec
	AttemptLatency prometheus.Histogram
	EndToEnd       prometheus.Histogram

	RunnableStreams   prometheus.Gauge
	LeasedStreams     prometheus.Gauge
	PendingDeliveries prometheus.Gauge
	OldestPending     prometheus.Gauge
}

// NewMetrics registers the instruments. slots may be nil (no pool in this
// process); otherwise its occupancy is exported as gauges read at scrape time.
func NewMetrics(reg prometheus.Registerer, slots *Slots) *Metrics {
	ns := metrics.Namespace
	m := &Metrics{
		LeaseBatch: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: ns, Subsystem: "queue", Name: "lease_batch_streams",
			Help:    "Streams returned per lease query.",
			Buckets: []float64{0, 1, 2, 4, 8, 16, 32, 64},
		}),
		DequeueErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: ns, Subsystem: "queue", Name: "dequeue_errors_total", Help: "Failed lease queries.",
		}),
		LeasesLost: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: ns, Subsystem: "queue", Name: "leases_lost_total",
			Help: "Leases lost mid-processing (renewal or fenced ack failed). Nonzero means leases are too short or the database is struggling.",
		}),
		Reaped: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: ns, Subsystem: "queue", Name: "leases_reaped_total",
			Help: "Expired leases reclaimed by the reaper. Nonzero in steady state means workers are dying.",
		}),
		Panics: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: ns, Subsystem: "pool", Name: "worker_panics_total", Help: "Recovered panics in workers.",
		}),
		AckDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: ns, Subsystem: "queue", Name: "ack_duration_seconds",
			Help:    "Duration of the fenced ack transaction.",
			Buckets: []float64{.0005, .001, .002, .004, .008, .016, .032, .064, .128, .256},
		}),
		AckErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: ns, Subsystem: "queue", Name: "ack_errors_total", Help: "Ack transactions that failed for reasons other than a lost lease.",
		}),
		Attempts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Subsystem: "delivery", Name: "attempts_total",
			Help: "HTTP delivery attempts by outcome.",
		}, []string{"outcome"}),
		Completed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Subsystem: "delivery", Name: "completed_total",
			Help: "Deliveries that reached a terminal state.",
		}, []string{"final_state"}),
		AttemptLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: ns, Subsystem: "delivery", Name: "attempt_duration_seconds",
			Help:    "Duration of one outbound HTTP attempt.",
			Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60},
		}),
		EndToEnd: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: ns, Subsystem: "delivery", Name: "end_to_end_seconds",
			Help:    "From event ingest to successful delivery (includes queue wait and retries).",
			Buckets: []float64{.01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 300, 1800, 7200},
		}),
		RunnableStreams: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: ns, Subsystem: "queue", Name: "runnable_streams", Help: "Streams ready to lease now.",
		}),
		LeasedStreams: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: ns, Subsystem: "queue", Name: "leased_streams", Help: "Streams currently leased.",
		}),
		PendingDeliveries: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: ns, Subsystem: "queue", Name: "pending_deliveries", Help: "Deliveries not yet terminal (the backlog).",
		}),
		OldestPending: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: ns, Subsystem: "queue", Name: "oldest_pending_seconds",
			Help: "Age of the oldest delivery that is due but not done. THE health signal: throughput can look fine while this climbs.",
		}),
	}
	reg.MustRegister(m.LeaseBatch, m.DequeueErrors, m.LeasesLost, m.Reaped, m.Panics, m.AckDuration,
		m.AckErrors, m.Attempts, m.Completed, m.AttemptLatency, m.EndToEnd,
		m.RunnableStreams, m.LeasedStreams, m.PendingDeliveries, m.OldestPending)
	if slots != nil {
		reg.MustRegister(
			prometheus.NewGaugeFunc(prometheus.GaugeOpts{
				Namespace: ns, Subsystem: "pool", Name: "slots_in_flight", Help: "Worker slots in use.",
			}, func() float64 { return float64(slots.InFlight()) }),
			prometheus.NewGaugeFunc(prometheus.GaugeOpts{
				Namespace: ns, Subsystem: "pool", Name: "slots_capacity", Help: "Total worker slots.",
			}, func() float64 { return float64(slots.Cap()) }),
		)
	}
	return m
}
