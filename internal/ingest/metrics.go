package ingest

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/SharmaSanskar/ithaca/internal/platform/metrics"
)

// Metrics are the ingest path's own instruments, on top of the generic HTTP
// ones every listener already has.
type Metrics struct {
	Events     *prometheus.CounterVec
	Deliveries prometheus.Counter
	Fanout     prometheus.Histogram
	TxDuration prometheus.Histogram
}

// NewMetrics registers the ingest instruments.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		Events: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metrics.Namespace, Subsystem: "ingest", Name: "events_total",
			Help: "Ingest requests by result: accepted, replayed (idempotent replay), conflict (key reused with a different body), invalid.",
		}, []string{"result"}),
		Deliveries: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metrics.Namespace, Subsystem: "ingest", Name: "deliveries_enqueued_total",
			Help: "Deliveries created by fanout.",
		}),
		Fanout: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: metrics.Namespace, Subsystem: "ingest", Name: "fanout_subscriptions",
			Help:    "Matching subscriptions per accepted event.",
			Buckets: []float64{0, 1, 2, 4, 8, 16, 32, 64},
		}),
		TxDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: metrics.Namespace, Subsystem: "ingest", Name: "tx_duration_seconds",
			Help:    "Time spent in the ingest transaction (idempotency claim + event insert + fanout).",
			Buckets: []float64{.0005, .001, .002, .004, .008, .016, .032, .064, .128, .256},
		}),
	}
	reg.MustRegister(m.Events, m.Deliveries, m.Fanout, m.TxDuration)
	return m
}
