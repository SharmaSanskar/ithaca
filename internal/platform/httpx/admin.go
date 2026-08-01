package httpx

import (
	"net/http"
	"net/http/pprof"

	"github.com/go-chi/chi/v5"

	"github.com/SharmaSanskar/ithaca/internal/platform/health"
	"github.com/SharmaSanskar/ithaca/internal/platform/metrics"
	"github.com/SharmaSanskar/ithaca/internal/platform/version"
)

// AdminRouter builds the internal listener.
//
// This port is never routed through the Ingress. Probes are node-local, the
// scrape comes from inside the cluster, and pprof exposes heap contents — all
// three would be a problem on a public listener and are fine on this one.
func AdminRouter(h *health.Registry, m *metrics.Registry, enablePprof bool) http.Handler {
	r := chi.NewRouter()

	r.Get("/healthz", h.LiveHandler())
	r.Get("/readyz", h.ReadyHandler())
	r.Get("/startupz", h.StartupHandler())
	r.Handle("/metrics", m.Handler())

	r.Get("/version", func(w http.ResponseWriter, _ *http.Request) {
		WriteJSON(w, http.StatusOK, version.Info())
	})

	if enablePprof {
		r.Route("/debug/pprof", func(r chi.Router) {
			r.HandleFunc("/", pprof.Index)
			r.HandleFunc("/cmdline", pprof.Cmdline)
			r.HandleFunc("/profile", pprof.Profile)
			r.HandleFunc("/symbol", pprof.Symbol)
			r.HandleFunc("/trace", pprof.Trace)
			// The named profiles (heap, goroutine, allocs, block, mutex) are
			// all served by Index's fallthrough handler.
			r.HandleFunc("/{name}", pprof.Index)
		})
	}

	return r
}

// PublicHealth mounts a shallow liveness endpoint on a public listener.
//
// This exists because cloud load balancers health-check the traffic port, not
// a side port. It reports process liveness only — the detailed readiness
// report, which names every dependency, stays on the admin listener.
func PublicHealth(r chi.Router, h *health.Registry) {
	r.Get("/healthz", h.LiveHandler())
}
