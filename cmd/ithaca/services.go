package main

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/SharmaSanskar/ithaca/internal/platform/config"
	"github.com/SharmaSanskar/ithaca/internal/platform/httpx"
	"github.com/SharmaSanskar/ithaca/internal/platform/shutdown"
	"github.com/SharmaSanskar/ithaca/internal/web"
)

// wire assembles the components for whichever service this process is.
//
// Phase 0 deliberately stops at the skeleton: every service boots, binds its
// ports, serves probes and metrics, and shuts down gracefully. The delivery
// engine, schema and dashboard arrive in later phases and slot into these
// same seams.
func (a *app) wire() error {
	if err := a.addAdminListener(); err != nil {
		return err
	}

	switch a.cfg.Service {
	case config.ServiceIngest:
		return a.wireIngest()
	case config.ServiceDispatcher:
		return a.wireDispatcher()
	case config.ServiceAPI:
		return a.wireAPI()
	case config.ServiceWS:
		return a.wireWS()
	case config.ServiceWorker:
		return a.wireWorker()
	case config.ServiceReceiverSim:
		return a.wireReceiverSim()
	default:
		return fmt.Errorf("no wiring for service %q", a.cfg.Service)
	}
}

// ── ingest ───────────────────────────────────────────────────────────────────

// The write path. Its only job is to durably record an event and return 202
// fast; it makes no outbound calls, which is what keeps ingest availability
// independent of every subscriber's health.
func (a *app) wireIngest() error {
	return a.addPublicListener("ingest", func(r chi.Router) {
		r.Get("/", serviceInfo(config.ServiceIngest))
		r.Route("/v1", func(r chi.Router) {
			// 1 MiB matches the documented maximum event payload. Scoped to
			// /v1 so it never applies to the health endpoint.
			r.Use(httpx.MaxBodyBytes(1 << 20))
			r.Post("/events", notImplemented("event ingestion", "Phase 1"))
		})
	})
}

// ── dispatcher ───────────────────────────────────────────────────────────────

// The delivery engine: leases streams, delivers, retries, acks. No inbound
// listener at all — it is a pure consumer, so there is nothing to route to it
// and nothing for an Ingress to expose.
func (a *app) wireDispatcher() error {
	a.runner.Add(shutdown.Component{
		Name: "dispatcher",
		Run:  idleUntilDone("dispatcher pool arrives in Phase 2"),
	})
	return nil
}

// ── api (control plane + dashboard) ──────────────────────────────────────────

func (a *app) wireAPI() error {
	return a.addPublicListener("api", func(r chi.Router) {
		r.Route("/api/v1", func(r chi.Router) {
			r.Use(httpx.NoStore)
			r.Use(httpx.MaxBodyBytes(1 << 20))
			r.Get("/", serviceInfo(config.ServiceAPI))
			r.Get("/me", notImplemented("session identity", "Phase 4"))
		})

		// The SPA is mounted last and catches everything else, so client-side
		// routes like /events/evt_123 deep-link correctly.
		r.NotFound(web.Handler("http://localhost:5173").ServeHTTP)
	})
}

// ── websocket hub ────────────────────────────────────────────────────────────

// Split from the API because it is stateful. Long-lived connections need
// different LB idle timeouts, a different drain story (every deploy drops every
// socket), and it scales on connection count rather than CPU. Co-locating it
// would mean every control-plane deploy kills every dashboard.
func (a *app) wireWS() error {
	return a.addPublicListener("ws", func(r chi.Router) {
		r.Get("/", serviceInfo(config.ServiceWS))
		r.Get("/ws", notImplemented("live event feed", "Phase 4"))
	})
}

// ── worker (periodic) ────────────────────────────────────────────────────────

// Fanout expansion, the lease reaper, partition management, the idempotency
// janitor and the DLQ replay ramp. Low-volume, singleton-ish work that must not
// compete with delivery for the dispatcher's worker slots.
func (a *app) wireWorker() error {
	a.runner.Add(shutdown.Component{
		Name: "worker",
		Run:  idleUntilDone("periodic jobs arrive in Phase 1"),
	})
	return nil
}

// ── receiver simulator ───────────────────────────────────────────────────────

// A programmable subscriber: configurable latency, status codes, failure rates,
// hangs and connection resets. It is the most valuable test asset in the repo,
// because every interesting delivery behavior needs a badly-behaved endpoint
// to provoke it. Runs in-cluster during load tests so deliveries never leave
// the AZ (external URLs would mean paying NAT egress per delivery body).
func (a *app) wireReceiverSim() error {
	return a.addPublicListener("receiver-sim", func(r chi.Router) {
		r.Get("/", serviceInfo(config.ServiceReceiverSim))
		r.Post("/sink/{id}", notImplemented("programmable sink", "Phase 2"))
	})
}

// ── helpers ──────────────────────────────────────────────────────────────────

// notImplemented is an honest 501 rather than a 404. A route that exists but is
// unbuilt should say so; pretending it does not exist makes the skeleton
// impossible to smoke-test.
func notImplemented(what, phase string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		httpx.WriteError(w, r, httpx.NewError(
			http.StatusNotImplemented,
			"not_implemented",
			fmt.Sprintf("%s is not built yet (%s).", what, phase),
		))
	}
}

// idleUntilDone keeps a listener-less service alive so its probes, metrics and
// shutdown path are all real and testable before the real work exists.
func idleUntilDone(note string) func(context.Context) error {
	return func(ctx context.Context) error {
		_ = note
		t := time.NewTicker(time.Hour)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-t.C:
			}
		}
	}
}
