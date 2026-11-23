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

// Redis backs pub/sub wakeups, rate-limit buckets, breaker state and sessions.
// It is deliberately never the source of truth: losing Redis must degrade
// latency, never correctness.
type Redis struct {
	URL string
}

// Telemetry configures OTel export. Wired properly in Phase 2.
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
		ShutdownGrace: l.dur("ITHACA_SHUTDOWN_GRACE", 30*time.Second),
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
