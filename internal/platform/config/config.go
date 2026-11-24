// Package config loads all runtime configuration from the environment exactly
// once, at boot, and validates it before anything else starts. Nothing in
// Ithaca reads os.Getenv outside this package.
//
// Two rules make this worth having:
//   - Fail fast and fail completely. A misconfigured process must die at boot
//     with every problem listed, not serve traffic and fall over at 3am.
//   - Never log a Config directly. Use Redacted().
package config

import (
	"fmt"
	"log/slog"
	"net/netip"
	"net/url"
	"strings"
	"time"
)

// Service identifies which subcommand this process is running as.
type Service string

const (
	ServiceIngest      Service = "ingest"
	ServiceDispatcher  Service = "dispatcher"
	ServiceAPI         Service = "api"
	ServiceWS          Service = "ws"
	ServiceWorker      Service = "worker"
	ServiceReceiverSim Service = "receiver-sim"
)

// ServiceCLI is the identity used by one-shot commands (migrate, tenant). It is
// not a long-running service and is deliberately absent from Services.
const ServiceCLI Service = "cli"

// Services is every valid subcommand, in the order they appear in help output.
var Services = []Service{
	ServiceIngest, ServiceDispatcher, ServiceAPI, ServiceWS, ServiceWorker, ServiceReceiverSim,
}

// ParseService validates a subcommand name.
func ParseService(s string) (Service, bool) {
	for _, v := range Services {
		if string(v) == s {
			return v, true
		}
	}
	return "", false
}

// defaults holds the per-service listen addresses. They differ so the whole
// stack can run on one laptop without port collisions.
var defaults = map[Service]struct{ http, admin string }{
	ServiceIngest:     {":8081", ":9081"},
	ServiceDispatcher: {"", ":9083"}, // no public listener: dispatcher makes calls, it never accepts them
	ServiceAPI:        {":8080", ":9080"},
	ServiceWS:         {":8082", ":9082"},
	ServiceWorker:     {"", ":9084"},
	// 9085, not 9090: Prometheus publishes 9090 on the host during local
	// development, and a collision there is confusing in exactly the wrong way
	// — the scrape succeeds against the wrong process.
	ServiceReceiverSim: {":8090", ":9085"},
}

// Config is the fully-resolved runtime configuration for one process.
type Config struct {
	Service Service
	Env     string // "dev" | "staging" | "prod"

	Log       Log
	HTTP      HTTP
	Admin     Admin
	Postgres  Postgres
	Redis     Redis
	Telemetry Telemetry
	Dispatch  Dispatch
	Delivery  Delivery
	Demo      Demo
	WS        WS

	// ShutdownGrace is how long in-flight work gets to finish after SIGTERM.
	// Kubernetes terminationGracePeriodSeconds MUST be larger than this
	// (plan target: grace + 15s) or the kubelet SIGKILLs mid-drain.
	ShutdownGrace time.Duration
}

// Log configures the slog handler.
type Log struct {
	Level  slog.Level
	Format string // "json" | "text"
	// AddSource costs ~1µs per line; on by default in dev only.
	AddSource bool
}

// HTTP is the public listener. Zero Addr means the service has no public port.
type HTTP struct {
	Addr              string
	ReadHeaderTimeout time.Duration
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
	MaxHeaderBytes    int
}

// Admin is the internal listener: health probes, metrics, pprof. It is never
// exposed through the Ingress — probes are node-local and metrics are scraped
// from inside the cluster.
type Admin struct {
	Addr  string
	Pprof bool
}

// Postgres is the primary datastore. Optional until Phase 1.
type Postgres struct {
	URL string
	// MaxConns follows the plan's rule: ~2*GOMAXPROCS+8, NOT the worker count.
	// Workers hold no connection across the outbound HTTP call, so sizing the
	// pool to the pool size is how you get "too many connections" at 8 replicas.
	MaxConns          int32
	MinConns          int32
	MaxConnLifetime   time.Duration
	MaxConnIdleTime   time.Duration
	ConnectTimeout    time.Duration
	StatementCacheOff bool
}

// Redis fans out live dashboard updates (Phase 4). It is off the delivery
// path: losing it costs a stale graph, never a delivery.
type Redis struct {
	URL string
}

// Dispatch tunes the dispatcher's worker pool. See internal/worker.
type Dispatch struct {
	// Workers is the number of streams leased and processed concurrently.
	Workers int
	// MaxStreamsBatch caps the streams leased per dequeue round trip.
	MaxStreamsBatch int
	// DrainBatch is how many deliveries are fetched per stream per round.
	DrainBatch int
	// MaxDrainRounds bounds how long one busy stream is held, so it cannot
	// starve the others.
	MaxDrainRounds int
	// MinLease floors the lease. The actual lease is
	// max(MinLease, 2*endpoint.timeout + LeasePadding).
	MinLease     time.Duration
	LeasePadding time.Duration
	// Adaptive polling: snap to PollFloor after finding work, back off toward
	// PollCeiling while idle.
	PollFloor   time.Duration
	PollCeiling time.Duration
	// ReapEvery is how often expired leases are reclaimed.
	ReapEvery time.Duration
}

// Delivery configures outbound HTTP and the retry schedule.
type Delivery struct {
	// AllowCIDRs are exempted from the SSRF blocklist. Empty in production.
	// Dev allows loopback so the receiver simulator on localhost works.
	AllowCIDRs []string
	RetryBase  time.Duration
	RetryCap   time.Duration
	// MaxAttempts is the total number of attempts before dead-lettering.
	MaxAttempts int

	// Circuit breaker: open after BreakerThreshold consecutive failures,
	// probe after BreakerCooldown, doubling to BreakerMaxCooldown.
	BreakerThreshold   int
	BreakerCooldown    time.Duration
	BreakerMaxCooldown time.Duration

	// ReplayRate is replayed deliveries per second per endpoint.
	ReplayRate int
}

// Demo configures the public demo: the seeded tenant, its sinks, ambient
// traffic, and the guard rails on its public controls.
type Demo struct {
	// Enabled turns on the demo runtime (worker), the public dashboard API
	// and demo controls (api), and restricts endpoint URLs to the sinks.
	Enabled     bool
	SinkBaseURL string // where receiver-sim is reachable from the dispatcher
	AmbientRate int    // events/s when no scenario is running
	// ActionsPerMinute is the per-IP limit on demo buttons.
	ActionsPerMinute int
	// Retention deletes delivery history, attempts and events older than
	// this (0 = keep everything). The always-on demo needs it; a real
	// deployment would use time partitions instead.
	Retention time.Duration
}

// WS configures the WebSocket hub.
type WS struct {
	// AllowedOrigins are accepted in addition to same-origin (e.g. the Vite
	// dev server).
	AllowedOrigins []string
	MaxConns       int
}

// Telemetry configures OTel export.
type Telemetry struct {
	Enabled      bool
	OTLPEndpoint string
	SampleRatio  float64
	ServiceName  string
}

// Load reads and validates the environment for the given service.
func Load(svc Service) (*Config, error) {
	l := &loader{}
	def := defaults[svc]

	cfg := &Config{
		Service: svc,
		Env:     l.oneOf("ITHACA_ENV", "dev", "dev", "staging", "prod"),
		Log: Log{
			Format:    l.oneOf("ITHACA_LOG_FORMAT", "", "", "json", "text"),
			AddSource: l.bool("ITHACA_LOG_SOURCE", false),
		},
		HTTP: HTTP{
			Addr:              l.str("ITHACA_HTTP_ADDR", def.http),
			ReadHeaderTimeout: l.dur("ITHACA_HTTP_READ_HEADER_TIMEOUT", 5*time.Second),
			ReadTimeout:       l.dur("ITHACA_HTTP_READ_TIMEOUT", 30*time.Second),
			WriteTimeout:      l.dur("ITHACA_HTTP_WRITE_TIMEOUT", 30*time.Second),
			IdleTimeout:       l.dur("ITHACA_HTTP_IDLE_TIMEOUT", 120*time.Second),
			MaxHeaderBytes:    l.int("ITHACA_HTTP_MAX_HEADER_BYTES", 1<<20),
		},
		Admin: Admin{
			Addr:  l.str("ITHACA_ADMIN_ADDR", def.admin),
			Pprof: l.bool("ITHACA_PPROF", false),
		},
		Postgres: Postgres{
			URL:             l.str("ITHACA_DATABASE_URL", ""),
			MaxConns:        int32(l.int("ITHACA_DB_MAX_CONNS", 0)), //nolint:gosec // bounded below
			MinConns:        int32(l.int("ITHACA_DB_MIN_CONNS", 2)), //nolint:gosec // bounded below
			MaxConnLifetime: l.dur("ITHACA_DB_MAX_CONN_LIFETIME", time.Hour),
			MaxConnIdleTime: l.dur("ITHACA_DB_MAX_CONN_IDLE_TIME", 30*time.Minute),
			ConnectTimeout:  l.dur("ITHACA_DB_CONNECT_TIMEOUT", 10*time.Second),
		},
		Redis: Redis{
			URL: l.str("ITHACA_REDIS_URL", ""),
		},
		Telemetry: Telemetry{
			Enabled:      l.bool("ITHACA_OTEL_ENABLED", false),
			OTLPEndpoint: l.str("ITHACA_OTEL_ENDPOINT", "localhost:4317"),
			SampleRatio:  float64(l.int("ITHACA_OTEL_SAMPLE_PERCENT", 100)) / 100,
			ServiceName:  l.str("ITHACA_OTEL_SERVICE_NAME", "ithaca-"+string(svc)),
		},
		Dispatch: Dispatch{
			Workers:         l.int("ITHACA_DISPATCH_WORKERS", 64),
			MaxStreamsBatch: l.int("ITHACA_DISPATCH_MAX_STREAMS_BATCH", 32),
			DrainBatch:      l.int("ITHACA_DISPATCH_DRAIN_BATCH", 64),
			MaxDrainRounds:  l.int("ITHACA_DISPATCH_MAX_DRAIN_ROUNDS", 4),
			MinLease:        l.dur("ITHACA_DISPATCH_MIN_LEASE", 30*time.Second),
			LeasePadding:    l.dur("ITHACA_DISPATCH_LEASE_PADDING", 10*time.Second),
			PollFloor:       l.dur("ITHACA_DISPATCH_POLL_FLOOR", 50*time.Millisecond),
			PollCeiling:     l.dur("ITHACA_DISPATCH_POLL_CEILING", 500*time.Millisecond),
			ReapEvery:       l.dur("ITHACA_DISPATCH_REAP_EVERY", 5*time.Second),
		},
		Delivery: Delivery{
			RetryBase:   l.dur("ITHACA_RETRY_BASE", 30*time.Second),
			RetryCap:    l.dur("ITHACA_RETRY_CAP", 6*time.Hour),
			MaxAttempts: l.int("ITHACA_RETRY_MAX_ATTEMPTS", 12),

			BreakerThreshold:   l.int("ITHACA_BREAKER_THRESHOLD", 10),
			BreakerCooldown:    l.dur("ITHACA_BREAKER_COOLDOWN", 30*time.Second),
			BreakerMaxCooldown: l.dur("ITHACA_BREAKER_MAX_COOLDOWN", 10*time.Minute),

			ReplayRate: l.int("ITHACA_REPLAY_RATE", 50),
		},
		Demo: Demo{
			Enabled:          l.bool("ITHACA_DEMO_MODE", false),
			SinkBaseURL:      strings.TrimRight(l.str("ITHACA_DEMO_SINK_URL", "http://localhost:8090"), "/"),
			AmbientRate:      l.int("ITHACA_DEMO_AMBIENT_RATE", 5),
			ActionsPerMinute: l.int("ITHACA_DEMO_ACTIONS_PER_MINUTE", 30),
			Retention:        l.dur("ITHACA_RETENTION", 0),
		},
		WS: WS{
			MaxConns: l.int("ITHACA_WS_MAX_CONNS", 2000),
		},
		ShutdownGrace: l.dur("ITHACA_SHUTDOWN_GRACE", 30*time.Second),
	}
	defOrigins := ""
	if cfg.Env == "dev" {
		defOrigins = "http://localhost:5173,http://localhost:8080"
	}
	for _, o := range strings.Split(l.str("ITHACA_WS_ALLOWED_ORIGINS", defOrigins), ",") {
		if o = strings.TrimSpace(o); o != "" {
			cfg.WS.AllowedOrigins = append(cfg.WS.AllowedOrigins, o)
		}
	}

	// Loopback is allowed by default in dev only, so a local receiver works.
	// Everywhere else the SSRF blocklist is absolute unless explicitly opened.
	defAllow := ""
	if cfg.Env == "dev" {
		defAllow = "127.0.0.0/8,::1/128"
	}
	for _, c := range strings.Split(l.str("ITHACA_DELIVERY_ALLOW_CIDRS", defAllow), ",") {
		if c = strings.TrimSpace(c); c != "" {
			cfg.Delivery.AllowCIDRs = append(cfg.Delivery.AllowCIDRs, c)
		}
	}

	// Log level: accept the usual names, default by environment.
	defLevel := "info"
	if cfg.Env == "dev" {
		defLevel = "debug"
	}
	switch strings.ToLower(l.str("ITHACA_LOG_LEVEL", defLevel)) {
	case "debug":
		cfg.Log.Level = slog.LevelDebug
	case "info":
		cfg.Log.Level = slog.LevelInfo
	case "warn", "warning":
		cfg.Log.Level = slog.LevelWarn
	case "error":
		cfg.Log.Level = slog.LevelError
	default:
		l.fail("ITHACA_LOG_LEVEL", fmt.Errorf("must be one of debug, info, warn, error"))
	}

	// Human-readable logs in dev, machine-readable everywhere else.
	if cfg.Log.Format == "" {
		if cfg.Env == "dev" {
			cfg.Log.Format = "text"
		} else {
			cfg.Log.Format = "json"
		}
	}

	if cfg.Postgres.MaxConns == 0 {
		cfg.Postgres.MaxConns = int32(defaultMaxConns()) //nolint:gosec // small positive
	}

	if err := l.err(); err != nil {
		return nil, err
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func (c *Config) validate() error {
	var errs []string

	if c.Postgres.URL != "" {
		if _, err := url.Parse(c.Postgres.URL); err != nil {
			errs = append(errs, "ITHACA_DATABASE_URL is not a valid URL")
		}
	}
	if c.Redis.URL != "" {
		if _, err := url.Parse(c.Redis.URL); err != nil {
			errs = append(errs, "ITHACA_REDIS_URL is not a valid URL")
		}
	}
	if c.Postgres.MaxConns < c.Postgres.MinConns {
		errs = append(errs, "ITHACA_DB_MAX_CONNS must be >= ITHACA_DB_MIN_CONNS")
	}
	if c.ShutdownGrace <= 0 {
		errs = append(errs, "ITHACA_SHUTDOWN_GRACE must be positive")
	}
	if c.Telemetry.SampleRatio < 0 || c.Telemetry.SampleRatio > 1 {
		errs = append(errs, "ITHACA_OTEL_SAMPLE_PERCENT must be between 0 and 100")
	}
	for _, cidr := range c.Delivery.AllowCIDRs {
		if _, err := netip.ParsePrefix(cidr); err != nil {
			errs = append(errs, fmt.Sprintf("ITHACA_DELIVERY_ALLOW_CIDRS: %q is not a CIDR prefix", cidr))
		}
	}
	d := c.Dispatch
	if d.Workers < 1 || d.MaxStreamsBatch < 1 || d.DrainBatch < 1 || d.MaxDrainRounds < 1 {
		errs = append(errs, "ITHACA_DISPATCH_{WORKERS,MAX_STREAMS_BATCH,DRAIN_BATCH,MAX_DRAIN_ROUNDS} must all be >= 1")
	}
	if d.PollFloor <= 0 || d.PollCeiling < d.PollFloor {
		errs = append(errs, "ITHACA_DISPATCH_POLL_FLOOR must be > 0 and <= ITHACA_DISPATCH_POLL_CEILING")
	}
	if d.MinLease <= 0 || d.ReapEvery <= 0 {
		errs = append(errs, "ITHACA_DISPATCH_MIN_LEASE and ITHACA_DISPATCH_REAP_EVERY must be positive")
	}
	if c.Delivery.RetryBase <= 0 || c.Delivery.RetryCap < c.Delivery.RetryBase || c.Delivery.MaxAttempts < 1 {
		errs = append(errs, "retry policy invalid: need RETRY_BASE > 0, RETRY_CAP >= RETRY_BASE, RETRY_MAX_ATTEMPTS >= 1")
	}
	if c.Delivery.BreakerThreshold < 1 || c.Delivery.BreakerCooldown <= 0 || c.Delivery.BreakerMaxCooldown < c.Delivery.BreakerCooldown {
		errs = append(errs, "breaker invalid: need BREAKER_THRESHOLD >= 1, BREAKER_COOLDOWN > 0, BREAKER_MAX_COOLDOWN >= BREAKER_COOLDOWN")
	}
	if c.Delivery.ReplayRate < 1 {
		errs = append(errs, "ITHACA_REPLAY_RATE must be >= 1")
	}
	if c.Admin.Pprof && c.Env == "prod" {
		// Not fatal — pprof on the admin port is not internet-reachable — but
		// it should be a deliberate choice, so make it visible.
		errs = append(errs, "ITHACA_PPROF is enabled in prod; unset it unless you are actively profiling")
	}

	if len(errs) > 0 {
		return fmt.Errorf("invalid configuration:\n  - %s", strings.Join(errs, "\n  - "))
	}
	return nil
}

// Redacted returns log-safe attributes. Connection strings carry passwords, so
// they are reduced to scheme://host/db before they ever reach a log line.
func (c *Config) Redacted() []slog.Attr {
	// No "service" attribute here: the root logger already carries it, and a
	// duplicate key in a JSON log line is silently dropped by every parser —
	// which is the sort of thing you only discover while debugging something
	// else at 3am.
	return []slog.Attr{
		slog.String("env", c.Env),
		slog.String("log_level", c.Log.Level.String()),
		slog.String("log_format", c.Log.Format),
		slog.String("http_addr", orNone(c.HTTP.Addr)),
		slog.String("admin_addr", orNone(c.Admin.Addr)),
		slog.String("postgres", redactURL(c.Postgres.URL)),
		slog.String("redis", redactURL(c.Redis.URL)),
		slog.Bool("otel_enabled", c.Telemetry.Enabled),
		slog.Duration("shutdown_grace", c.ShutdownGrace),
	}
}

func orNone(s string) string {
	if s == "" {
		return "(none)"
	}
	return s
}

func redactURL(raw string) string {
	if raw == "" {
		return "(unset)"
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "(unparseable)"
	}
	if u.User != nil {
		if _, hasPassword := u.User.Password(); hasPassword {
			u.User = url.UserPassword(u.User.Username(), "xxxxx")
		}
	}
	u.RawQuery = ""
	return u.String()
}
