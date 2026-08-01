package httpx

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/SharmaSanskar/ithaca/internal/platform/config"
	"github.com/SharmaSanskar/ithaca/internal/platform/shutdown"
)

// Server wraps http.Server with the lifecycle Ithaca needs.
type Server struct {
	name string
	srv  *http.Server
	ln   net.Listener
	log  *slog.Logger
}

// ServerConfig describes one listener.
type ServerConfig struct {
	Name     string
	Addr     string
	Handler  http.Handler
	Log      *slog.Logger
	Timeouts config.HTTP
}

// NewServer binds the listener immediately.
//
// Binding here rather than inside Run is deliberate: a port conflict or a bad
// bind address is a configuration error, and configuration errors should kill
// the process during startup with a clear message — not 200ms later, from a
// goroutine, after the process has already reported itself healthy.
func NewServer(cfg ServerConfig) (*Server, error) {
	if cfg.Addr == "" {
		return nil, fmt.Errorf("httpx: %s listener has no address", cfg.Name)
	}
	log := cfg.Log
	if log == nil {
		log = slog.Default()
	}

	t := cfg.Timeouts
	if t.ReadHeaderTimeout == 0 {
		t.ReadHeaderTimeout = 5 * time.Second
	}
	if t.IdleTimeout == 0 {
		t.IdleTimeout = 120 * time.Second
	}
	if t.MaxHeaderBytes == 0 {
		t.MaxHeaderBytes = 1 << 20
	}

	srv := &http.Server{
		Addr:    cfg.Addr,
		Handler: cfg.Handler,
		// ReadHeaderTimeout is the one that is never optional: without it a
		// single slowloris connection can hold a goroutine open indefinitely.
		ReadHeaderTimeout: t.ReadHeaderTimeout,
		ReadTimeout:       t.ReadTimeout,
		WriteTimeout:      t.WriteTimeout,
		IdleTimeout:       t.IdleTimeout,
		MaxHeaderBytes:    t.MaxHeaderBytes,
		ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelWarn),
		BaseContext: func(net.Listener) context.Context {
			return context.Background()
		},
	}

	lc := net.ListenConfig{}
	ln, err := lc.Listen(context.Background(), "tcp", cfg.Addr)
	if err != nil {
		return nil, fmt.Errorf("httpx: bind %s listener on %s: %w", cfg.Name, cfg.Addr, err)
	}

	return &Server{name: cfg.Name, srv: srv, ln: ln, log: log}, nil
}

// Addr returns the actual bound address, which matters when the config asked
// for port 0 (tests).
func (s *Server) Addr() string { return s.ln.Addr().String() }

// Component adapts the server to the shutdown runner.
func (s *Server) Component() shutdown.Component {
	return shutdown.Component{
		Name: s.name,
		Run: func(ctx context.Context) error {
			s.log.Info("http listener ready", "listener", s.name, "addr", s.Addr())
			err := s.srv.Serve(s.ln)
			if errors.Is(err, http.ErrServerClosed) {
				return nil
			}
			return err
		},
		Stop: func(ctx context.Context) error {
			// Shutdown stops accepting, then waits for in-flight handlers.
			// Idle keep-alive connections are closed immediately.
			err := s.srv.Shutdown(ctx)
			if err != nil {
				// The deadline expired with handlers still running. Close
				// forcibly so the process can exit rather than hanging until
				// the kubelet SIGKILLs us.
				s.log.Warn("listener did not drain in time, closing forcibly",
					"listener", s.name, "error", err)
				return errors.Join(err, s.srv.Close())
			}
			s.log.Info("http listener stopped", "listener", s.name)
			return nil
		},
	}
}
