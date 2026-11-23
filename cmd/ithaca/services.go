package main

import (
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/SharmaSanskar/ithaca/internal/api"
	"github.com/SharmaSanskar/ithaca/internal/auth"
	"github.com/SharmaSanskar/ithaca/internal/deliver"
	"github.com/SharmaSanskar/ithaca/internal/ingest"
	"github.com/SharmaSanskar/ithaca/internal/janitor"
	"github.com/SharmaSanskar/ithaca/internal/platform/clock"
	"github.com/SharmaSanskar/ithaca/internal/platform/config"
	"github.com/SharmaSanskar/ithaca/internal/platform/httpx"
	"github.com/SharmaSanskar/ithaca/internal/platform/shutdown"
	"github.com/SharmaSanskar/ithaca/internal/queue/pg"
	"github.com/SharmaSanskar/ithaca/internal/receiver"
	"github.com/SharmaSanskar/ithaca/internal/retry"
	"github.com/SharmaSanskar/ithaca/internal/web"
	"github.com/SharmaSanskar/ithaca/internal/worker"
)

// wire assembles the components for whichever service this process is.
//
// Phase 0 deliberately stops at the skeleton: every service boots, binds its
// ports, serves probes and metrics, and shuts down gracefully. The delivery
// engine, schema and dashboard arrive in later phases and slot into these
// same seams.
func (a *app) wire() error {
	if err := a.setupTelemetry(); err != nil {
		return err
	}
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
	if err := a.openStore(); err != nil {
		return err
	}
	svc := ingest.NewService(a.store, ingest.NewMetrics(a.metrics.Registerer()))
	authn := auth.NewAuthenticator(a.store, apiKeyCacheTTL)

	return a.addPublicListener("ingest", func(r chi.Router) {
		r.Get("/", serviceInfo(config.ServiceIngest))
		r.Route("/v1", func(r chi.Router) {
			// 1 MiB matches the documented maximum event payload. Scoped to
			// /v1 so it never applies to the health endpoint.
			r.Use(httpx.MaxBodyBytes(1 << 20))
			r.Use(authn.Middleware)
			ingest.NewHandler(svc).Routes(r)
		})
	})
}

// apiKeyCacheTTL bounds how long a revoked key keeps working. 30s is the
// documented trade: auth becomes a map lookup on the hot path.
const apiKeyCacheTTL = 30 * time.Second

// ── dispatcher ───────────────────────────────────────────────────────────────

// The delivery engine: leases streams, delivers, retries, acks. No inbound
// listener at all — it is a pure consumer, so there is nothing to route to it
// and nothing for an Ingress to expose.
func (a *app) wireDispatcher() error {
	if err := a.openStore(); err != nil {
		return err
	}
	guard, err := deliver.NewGuard(a.cfg.Delivery.AllowCIDRs)
	if err != nil {
		return err
	}
	if len(a.cfg.Delivery.AllowCIDRs) > 0 {
		a.log.Warn("SSRF blocklist has exemptions; never do this in production",
			"allow_cidrs", a.cfg.Delivery.AllowCIDRs)
	}

	d := a.cfg.Dispatch
	slots := worker.NewSlots(d.Workers)
	m := worker.NewMetrics(a.metrics.Registerer(), slots)
	q := pg.New(a.store)
	exec := worker.NewExecutor(deliver.NewClient(guard), retry.Policy{
		Base:        a.cfg.Delivery.RetryBase,
		Multiplier:  2,
		Cap:         a.cfg.Delivery.RetryCap,
		MaxAttempts: a.cfg.Delivery.MaxAttempts,
	}, m)

	pool := worker.New(worker.Config{
		WorkerID:        workerID(),
		Workers:         d.Workers,
		MaxStreamsBatch: d.MaxStreamsBatch,
		DrainBatch:      d.DrainBatch,
		MaxDrainRounds:  d.MaxDrainRounds,
		MinLease:        d.MinLease,
		LeasePadding:    d.LeasePadding,
		PollFloor:       d.PollFloor,
		PollCeiling:     d.PollCeiling,
		DisabledRecheck: time.Minute,
	}, q, exec, slots, a.log.With("component", "pool"), m, clock.Real{})

	// Registration order matters for shutdown: components stop in REVERSE
	// order, and only the pool has a Stop. The reaper and stats loop keep
	// running until the pool has drained.
	a.runner.Add(shutdown.Component{Name: "pool", Run: pool.Run, Stop: pool.Shutdown})
	a.runner.Add(shutdown.Component{Name: "reaper", Run: (&worker.Reaper{
		Q: q, Every: d.ReapEvery, Log: a.log.With("component", "reaper"), M: m,
	}).Run})
	a.runner.Add(shutdown.Component{Name: "queue-stats", Run: (&worker.StatsLoop{
		Q: q, Every: 5 * time.Second, Log: a.log.With("component", "queue-stats"), M: m,
	}).Run})
	return nil
}

// workerID names this process in lease rows (locked_by) and attempt rows, so
// "who was holding this stream?" has an answer. In Kubernetes HOSTNAME is the
// pod name.
func workerID() string {
	host, _ := os.Hostname()
	return fmt.Sprintf("%s-%d", host, os.Getpid())
}

// ── api (control plane + dashboard) ──────────────────────────────────────────

func (a *app) wireAPI() error {
	if err := a.openStore(); err != nil {
		return err
	}
	authn := auth.NewAuthenticator(a.store, apiKeyCacheTTL)
	cp := api.NewControlPlane(a.store)

	return a.addPublicListener("api", func(r chi.Router) {
		r.Route("/api/v1", func(r chi.Router) {
			r.Use(httpx.NoStore)
			r.Use(httpx.MaxBodyBytes(1 << 20))
			r.Get("/", serviceInfo(config.ServiceAPI))

			// Tenant-scoped management API, authenticated by API key. The
			// dashboard's public read-only API arrives in Phase 4.
			r.Group(func(r chi.Router) {
				r.Use(authn.Middleware)
				cp.Routes(r)
			})
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

// The idempotency janitor (and the DLQ replay ramp in Phase 3). Low-volume
// periodic work that must not compete with delivery for the dispatcher's
// worker slots. The lease reaper lives in the dispatcher itself.
func (a *app) wireWorker() error {
	if err := a.openStore(); err != nil {
		return err
	}
	j := janitor.New(a.store, a.log.With("component", "janitor"), a.metrics.Registerer())
	a.runner.Add(shutdown.Component{Name: "janitor", Run: j.Run})
	return nil
}

// ── receiver simulator ───────────────────────────────────────────────────────

// A programmable subscriber: configurable latency, status codes, failure rates,
// hangs and connection resets. It is the most valuable test asset in the repo,
// because every interesting delivery behavior needs a badly-behaved endpoint
// to provoke it. Runs in-cluster during load tests so deliveries never leave
// the AZ (external URLs would mean paying NAT egress per delivery body).
func (a *app) wireReceiverSim() error {
	// No request log: this process may take millions of requests in a load
	// test. Per-sink counters are kept instead (GET /sink/{id}/stats).
	rcv := receiver.New(uint64(time.Now().UnixNano()), false) //nolint:gosec // seed only
	return a.addPublicListener("receiver-sim", func(r chi.Router) {
		r.Get("/", serviceInfo(config.ServiceReceiverSim))
		rcv.Mount(r)
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
