// Package metrics owns the Prometheus registry.
//
// Deliberately a private registry rather than prometheus.DefaultRegisterer:
// a shared global means any dependency can register a collector with a
// conflicting name and panic the process at init, and it makes tests that
// assert on metric values order-dependent.
//
// Naming follows the Prometheus conventions that actually matter:
//   - `ithaca_` prefix on everything so a mixed scrape target is greppable
//   - base units, always: seconds and bytes, never milliseconds or megabytes
//   - `_total` suffix on counters, `_seconds` on durations
//   - low-cardinality labels only. Never an endpoint URL, a tenant name, or a
//     delivery ID in a label — that is how you take down your own Prometheus.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Namespace prefixes every metric in the system.
const Namespace = "ithaca"

// Registry wraps a Prometheus registry with the collectors every service wants.
type Registry struct {
	reg *prometheus.Registry

	// HTTP server metrics, shared by every inbound listener.
	HTTPRequests *prometheus.CounterVec
	HTTPDuration *prometheus.HistogramVec
	HTTPInFlight *prometheus.GaugeVec
	HTTPReqSize  *prometheus.HistogramVec
	HTTPRespSize *prometheus.HistogramVec

	// BuildInfo is the standard "always 1, labels carry the payload" gauge.
	BuildInfo *prometheus.GaugeVec
}

// New creates a registry preloaded with Go runtime and process collectors.
func New(service string) *Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(
			collectors.WithGoCollectorRuntimeMetrics(collectors.MetricsScheduler, collectors.MetricsGC),
		),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	constLabels := prometheus.Labels{"service": service}

	m := &Registry{
		reg: reg,

		HTTPRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace:   Namespace,
			Subsystem:   "http",
			Name:        "requests_total",
			Help:        "Total inbound HTTP requests.",
			ConstLabels: constLabels,
		}, []string{"listener", "method", "route", "code"}),

		HTTPDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: Namespace,
			Subsystem: "http",
			Name:      "request_duration_seconds",
			Help:      "Inbound HTTP request latency.",
			// Tuned for a service whose p99 target is single-digit
			// milliseconds. The default buckets start at 5ms, which would put
			// every ingest request in the first bucket and tell us nothing.
			Buckets:     []float64{.0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
			ConstLabels: constLabels,
		}, []string{"listener", "method", "route"}),

		HTTPInFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace:   Namespace,
			Subsystem:   "http",
			Name:        "requests_in_flight",
			Help:        "Inbound HTTP requests currently being served.",
			ConstLabels: constLabels,
		}, []string{"listener"}),

		HTTPReqSize: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace:   Namespace,
			Subsystem:   "http",
			Name:        "request_size_bytes",
			Help:        "Inbound HTTP request body size.",
			Buckets:     prometheus.ExponentialBuckets(64, 4, 8), // 64B → 1MiB
			ConstLabels: constLabels,
		}, []string{"listener", "route"}),

		HTTPRespSize: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace:   Namespace,
			Subsystem:   "http",
			Name:        "response_size_bytes",
			Help:        "Inbound HTTP response body size.",
			Buckets:     prometheus.ExponentialBuckets(64, 4, 8),
			ConstLabels: constLabels,
		}, []string{"listener", "route"}),

		BuildInfo: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace:   Namespace,
			Name:        "build_info",
			Help:        "Build metadata; the value is always 1.",
			ConstLabels: constLabels,
		}, []string{"version", "revision", "go_version"}),
	}

	reg.MustRegister(
		m.HTTPRequests, m.HTTPDuration, m.HTTPInFlight,
		m.HTTPReqSize, m.HTTPRespSize, m.BuildInfo,
	)
	return m
}

// Registerer exposes the underlying registry so feature packages can add their
// own collectors without importing Prometheus globals.
func (r *Registry) Registerer() prometheus.Registerer { return r.reg }

// Gatherer exposes the registry for tests and for the scrape handler.
func (r *Registry) Gatherer() prometheus.Gatherer { return r.reg }

// SetBuildInfo records build metadata as a labeled gauge.
func (r *Registry) SetBuildInfo(version, revision, goVersion string) {
	r.BuildInfo.WithLabelValues(version, revision, goVersion).Set(1)
}

// Handler serves the scrape endpoint.
func (r *Registry) Handler() http.Handler {
	return promhttp.HandlerFor(r.reg, promhttp.HandlerOpts{
		// A broken collector should degrade the scrape, not 500 the whole
		// endpoint and blind us to every other metric.
		ErrorHandling:       promhttp.ContinueOnError,
		EnableOpenMetrics:   true,
		MaxRequestsInFlight: 4,
	})
}
