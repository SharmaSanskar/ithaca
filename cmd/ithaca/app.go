package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"runtime"
	"runtime/debug"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/SharmaSanskar/ithaca/internal/platform/config"
	"github.com/SharmaSanskar/ithaca/internal/platform/health"
	"github.com/SharmaSanskar/ithaca/internal/platform/httpx"
	"github.com/SharmaSanskar/ithaca/internal/platform/logging"
	"github.com/SharmaSanskar/ithaca/internal/platform/metrics"
	"github.com/SharmaSanskar/ithaca/internal/platform/shutdown"
	"github.com/SharmaSanskar/ithaca/internal/platform/version"
)

// app is the shared skeleton every service is built on: config, logger,
// metrics, health, an admin listener, and a shutdown runner.
type app struct {
	cfg     *config.Config
	log     *slog.Logger
	metrics *metrics.Registry
	health  *health.Registry
	runner  *shutdown.Runner
}

func newApp(cfg *config.Config) *app {
	log := logging.New(logging.Options{
		Level:     cfg.Log.Level,
		Format:    cfg.Log.Format,
		AddSource: cfg.Log.AddSource,
		Service:   string(cfg.Service),
		Version:   version.Version,
	})

	// GOMAXPROCS is not container-aware: in a pod with `cpu: 500m` the Go
	// runtime still sees every core on the node, which means far too many
	// OS threads, heavy context switching, and a GC that assumes parallelism
	// it does not have. Go 1.25 finally reads the cgroup CPU limit itself, so
	// this is now handled — but log the effective value so it is visible when
	// a latency graph looks wrong.
	log.LogAttrs(context.Background(), slog.LevelInfo, "starting",
		append(cfg.Redacted(),
			slog.String("revision", version.Revision()),
			slog.String("go", runtime.Version()),
			slog.Int("gomaxprocs", runtime.GOMAXPROCS(0)),
			slog.Int("numcpu", runtime.NumCPU()),
		)...)

	m := metrics.New(string(cfg.Service))
	m.SetBuildInfo(version.Version, version.Revision(), runtime.Version())

	h := health.New()

	// DrainDelay only matters behind a load balancer. In dev it is pure
	// waiting, so Ctrl-C stays instant.
	drainDelay := 5 * time.Second
	if cfg.Env == "dev" {
		drainDelay = 0
	}

	a := &app{
		cfg:     cfg,
		log:     log,
		metrics: m,
		health:  h,
		runner: &shutdown.Runner{
			Log:        log,
			DrainDelay: drainDelay,
		},
	}

	// Readiness must start failing before anything stops accepting. This is
	// the single most important ordering in the whole shutdown sequence.
	a.runner.PreStop("mark-unready", func(context.Context) error {
		h.MarkShuttingDown()
		return nil
	})

	return a
}

// addAdminListener binds the internal port: probes, metrics, pprof, version.
func (a *app) addAdminListener() error {
	if a.cfg.Admin.Addr == "" {
		return nil
	}
	srv, err := httpx.NewServer(httpx.ServerConfig{
		Name:    "admin",
		Addr:    a.cfg.Admin.Addr,
		Handler: httpx.AdminRouter(a.health, a.metrics, a.cfg.Admin.Pprof),
		Log:     a.log,
		Timeouts: config.HTTP{
			ReadHeaderTimeout: 5 * time.Second,
			// No WriteTimeout: a 30-second CPU profile is a legitimate
			// request on this listener and a write deadline would truncate it.
			IdleTimeout:    60 * time.Second,
			MaxHeaderBytes: 1 << 16,
		},
	})
	if err != nil {
		return err
	}
	a.runner.Add(srv.Component())
	return nil
}

// addPublicListener binds the traffic port with the standard middleware stack.
func (a *app) addPublicListener(name string, mount func(chi.Router)) error {
	if a.cfg.HTTP.Addr == "" {
		return fmt.Errorf("service %s needs a public listener but ITHACA_HTTP_ADDR is empty", a.cfg.Service)
	}

	r := chi.NewRouter()

	// Order matters. RequestID first so every later middleware — including
	// the panic handler — can log a correlation ID. Recoverer before Metrics
	// so a panicking handler still records a 500 rather than vanishing.
	r.Use(httpx.RequestID())
	r.Use(httpx.RealIP(a.cfg.Env != "dev"))
	r.Use(httpx.Recoverer(a.log))
	r.Use(httpx.Metrics(a.metrics, name))
	r.Use(httpx.AccessLog(a.log, name))

	// The service mounts its own middleware and routes next. chi panics on a
	// Use() call that follows any route registration, so nothing may be
	// registered on this mux before mount runs.
	mount(r)

	// Cloud load balancers health-check the traffic port, not the admin port,
	// so a shallow liveness endpoint lives here too — registered last, for the
	// ordering reason above.
	httpx.PublicHealth(r, a.health)

	srv, err := httpx.NewServer(httpx.ServerConfig{
		Name:     name,
		Addr:     a.cfg.HTTP.Addr,
		Handler:  r,
		Log:      a.log,
		Timeouts: a.cfg.HTTP,
	})
	if err != nil {
		return err
	}
	a.runner.Add(srv.Component())
	return nil
}

// run starts everything and blocks until shutdown finishes.
func (a *app) run(ctx context.Context) error {
	defer func() {
		if p := recover(); p != nil {
			a.log.Error("fatal panic", "panic", p, "stack", string(debug.Stack()))
			panic(p)
		}
	}()

	a.health.MarkStarted()
	a.log.Info("ready")

	err := a.runner.Run(ctx, a.cfg.ShutdownGrace)
	if err != nil {
		a.log.Error("stopped with error", "error", err)
		return err
	}
	a.log.Info("stopped cleanly")
	return nil
}

// serviceInfo is the small identity document every listener serves at its root
// when there is nothing more interesting to show.
func serviceInfo(svc config.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		httpx.WriteJSON(w, http.StatusOK, map[string]any{
			"service": string(svc),
			"build":   version.Info(),
		})
	}
}
