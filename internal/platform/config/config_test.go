package config

import (
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	// t.Setenv makes the test non-parallel but restores the environment for
	// us, which is what you want when the thing under test reads os.Getenv.
	t.Setenv("ITHACA_ENV", "dev")

	cfg, err := Load(ServiceIngest)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if got, want := cfg.HTTP.Addr, ":8081"; got != want {
		t.Errorf("HTTP.Addr = %q, want %q", got, want)
	}
	if got, want := cfg.Admin.Addr, ":9081"; got != want {
		t.Errorf("Admin.Addr = %q, want %q", got, want)
	}
	// dev defaults: debug level, human-readable output.
	if got, want := cfg.Log.Level, slog.LevelDebug; got != want {
		t.Errorf("Log.Level = %v, want %v", got, want)
	}
	if got, want := cfg.Log.Format, "text"; got != want {
		t.Errorf("Log.Format = %q, want %q", got, want)
	}
	if cfg.Postgres.MaxConns < cfg.Postgres.MinConns {
		t.Errorf("MaxConns %d < MinConns %d", cfg.Postgres.MaxConns, cfg.Postgres.MinConns)
	}
}

func TestLoadProdDefaults(t *testing.T) {
	t.Setenv("ITHACA_ENV", "prod")

	cfg, err := Load(ServiceAPI)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got, want := cfg.Log.Level, slog.LevelInfo; got != want {
		t.Errorf("Log.Level = %v, want %v", got, want)
	}
	if got, want := cfg.Log.Format, "json"; got != want {
		t.Errorf("Log.Format = %q, want %q", got, want)
	}
}

func TestDispatcherHasNoPublicListener(t *testing.T) {
	// The dispatcher only makes outbound calls. If it ever grows a public
	// address by accident, that is a routing surface nobody asked for.
	cfg, err := Load(ServiceDispatcher)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.HTTP.Addr != "" {
		t.Errorf("dispatcher HTTP.Addr = %q, want empty", cfg.HTTP.Addr)
	}
	if cfg.Admin.Addr == "" {
		t.Error("dispatcher must still expose an admin listener for probes and metrics")
	}
}

func TestLoadValidationErrors(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		wantSub string
	}{
		{
			name:    "bad log level",
			env:     map[string]string{"ITHACA_LOG_LEVEL": "chatty"},
			wantSub: "ITHACA_LOG_LEVEL",
		},
		{
			name:    "bad duration",
			env:     map[string]string{"ITHACA_SHUTDOWN_GRACE": "30 seconds"},
			wantSub: "ITHACA_SHUTDOWN_GRACE",
		},
		{
			name:    "bad integer",
			env:     map[string]string{"ITHACA_DB_MAX_CONNS": "lots"},
			wantSub: "ITHACA_DB_MAX_CONNS",
		},
		{
			name:    "bad bool",
			env:     map[string]string{"ITHACA_PPROF": "yes-please"},
			wantSub: "ITHACA_PPROF",
		},
		{
			name:    "unknown env",
			env:     map[string]string{"ITHACA_ENV": "qa"},
			wantSub: "ITHACA_ENV",
		},
		{
			name: "max conns below min conns",
			env: map[string]string{
				"ITHACA_DB_MAX_CONNS": "2",
				"ITHACA_DB_MIN_CONNS": "10",
			},
			wantSub: "ITHACA_DB_MAX_CONNS",
		},
		{
			name: "pprof in prod",
			env: map[string]string{
				"ITHACA_ENV":   "prod",
				"ITHACA_PPROF": "true",
			},
			wantSub: "ITHACA_PPROF",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for k, v := range tt.env {
				t.Setenv(k, v)
			}
			_, err := Load(ServiceAPI)
			if err == nil {
				t.Fatal("Load() error = nil, want an error")
			}
			if !strings.Contains(err.Error(), tt.wantSub) {
				t.Errorf("Load() error = %q, want it to mention %q", err, tt.wantSub)
			}
		})
	}
}

func TestLoadReportsEveryErrorAtOnce(t *testing.T) {
	// Fixing config one boot at a time is miserable. All problems, one message.
	t.Setenv("ITHACA_LOG_LEVEL", "loud")
	t.Setenv("ITHACA_SHUTDOWN_GRACE", "ages")
	t.Setenv("ITHACA_DB_MAX_CONNS", "many")

	_, err := Load(ServiceAPI)
	if err == nil {
		t.Fatal("Load() error = nil, want an error")
	}
	for _, want := range []string{"ITHACA_LOG_LEVEL", "ITHACA_SHUTDOWN_GRACE", "ITHACA_DB_MAX_CONNS"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q is missing %q", err, want)
		}
	}
}

func TestOverrides(t *testing.T) {
	t.Setenv("ITHACA_HTTP_ADDR", "127.0.0.1:1234")
	t.Setenv("ITHACA_LOG_LEVEL", "warn")
	t.Setenv("ITHACA_LOG_FORMAT", "json")
	t.Setenv("ITHACA_SHUTDOWN_GRACE", "45s")

	cfg, err := Load(ServiceIngest)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got, want := cfg.HTTP.Addr, "127.0.0.1:1234"; got != want {
		t.Errorf("HTTP.Addr = %q, want %q", got, want)
	}
	if got, want := cfg.Log.Level, slog.LevelWarn; got != want {
		t.Errorf("Log.Level = %v, want %v", got, want)
	}
	if got, want := cfg.ShutdownGrace, 45*time.Second; got != want {
		t.Errorf("ShutdownGrace = %v, want %v", got, want)
	}
}

func TestRedactedNeverLeaksCredentials(t *testing.T) {
	// A password reaching a log line is a security incident that no amount of
	// log retention policy undoes. This test is the guard.
	const secret = "sup3rs3cret"
	t.Setenv("ITHACA_DATABASE_URL", "postgres://ithaca:"+secret+"@db.internal:5432/ithaca?sslmode=require")
	t.Setenv("ITHACA_REDIS_URL", "redis://:"+secret+"@cache.internal:6379/0")

	cfg, err := Load(ServiceAPI)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	var rendered strings.Builder
	for _, a := range cfg.Redacted() {
		rendered.WriteString(a.Key)
		rendered.WriteString("=")
		rendered.WriteString(a.Value.String())
		rendered.WriteString(" ")
	}

	if strings.Contains(rendered.String(), secret) {
		t.Fatalf("Redacted() leaked the password: %s", rendered.String())
	}
	// Sanity: it should still be useful for debugging.
	if !strings.Contains(rendered.String(), "db.internal:5432") {
		t.Errorf("Redacted() dropped the host, which makes it useless: %s", rendered.String())
	}
}

func TestParseService(t *testing.T) {
	for _, svc := range Services {
		if got, ok := ParseService(string(svc)); !ok || got != svc {
			t.Errorf("ParseService(%q) = %q, %v", svc, got, ok)
		}
	}
	if _, ok := ParseService("nope"); ok {
		t.Error(`ParseService("nope") reported ok`)
	}
}

func TestRedactURL(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"empty", "", "(unset)"},
		{"no credentials", "postgres://localhost:5432/ithaca", "postgres://localhost:5432/ithaca"},
		{"password", "postgres://u:p@h:5432/d", "postgres://u:xxxxx@h:5432/d"},
		{"user only", "postgres://u@h:5432/d", "postgres://u@h:5432/d"},
		{"query stripped", "postgres://u:p@h/d?sslmode=require", "postgres://u:xxxxx@h/d"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := redactURL(tt.in); got != tt.want {
				t.Errorf("redactURL(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestDefaultMaxConnsIsBounded(t *testing.T) {
	// The rule is 2*GOMAXPROCS+8 clamped to [8,64]. The clamp is what stops a
	// 64-core CI runner from opening 136 connections per replica.
	n := defaultMaxConns()
	if n < 8 || n > 64 {
		t.Errorf("defaultMaxConns() = %d, want it within [8,64]", n)
	}
}

// TestDefaultPortsAreUnique guards the "runs on one laptop" property.
//
// Every service has to be startable simultaneously during development, so no
// two may default to the same port — and the admin ports additionally have to
// dodge the dependency stack, which publishes Prometheus on 9090 and Grafana
// on 3000.
func TestDefaultPortsAreUnique(t *testing.T) {
	// Ports the compose stack publishes on the host. A service defaulting to
	// one of these produces the worst kind of failure: the scrape succeeds
	// against the wrong process.
	reserved := map[string]string{
		":5432":  "postgres",
		":6379":  "redis",
		":3000":  "grafana",
		":9090":  "prometheus",
		":16686": "jaeger",
		":4317":  "otel-collector (gRPC)",
		":4318":  "otel-collector (HTTP)",
		":4566":  "localstack",
		":8889":  "otel-collector (prom)",
		":5173":  "vite dev server",
	}

	seen := map[string]string{}
	for svc, d := range defaults {
		for _, addr := range []string{d.http, d.admin} {
			if addr == "" {
				continue
			}
			if owner, ok := reserved[addr]; ok {
				t.Errorf("%s defaults to %s, which the local stack already uses for %s", svc, addr, owner)
			}
			if prev, ok := seen[addr]; ok {
				t.Errorf("%s and %s both default to %s; they cannot run side by side", prev, svc, addr)
			}
			seen[addr] = string(svc)
		}
	}
}

// TestDispatcherAndWorkerHaveNoPublicListener is a design constraint, not a
// preference: neither accepts inbound traffic, so neither should ever appear in
// a Service or an Ingress. If a default address shows up here, something has
// grown an API that was not supposed to have one.
func TestDispatcherAndWorkerHaveNoPublicListener(t *testing.T) {
	for _, svc := range []Service{ServiceDispatcher, ServiceWorker} {
		if addr := defaults[svc].http; addr != "" {
			t.Errorf("%s has a default public listener (%s); it is a pure consumer", svc, addr)
		}
		if defaults[svc].admin == "" {
			t.Errorf("%s has no admin listener, so it cannot be probed or scraped", svc)
		}
	}
}
