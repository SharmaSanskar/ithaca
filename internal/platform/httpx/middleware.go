package httpx

import (
	"log/slog"
	"net"
	"net/http"
	"runtime/debug"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	chimw "github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"

	"github.com/SharmaSanskar/ithaca/internal/platform/logging"
	"github.com/SharmaSanskar/ithaca/internal/platform/metrics"
)

// HeaderRequestID is the correlation header. Accepted on the way in, always
// echoed on the way out.
const HeaderRequestID = "X-Request-Id"

// RequestID attaches a correlation ID to the request context and the response.
//
// An inbound ID is trusted but sanitized — a caller that already has a trace
// running should be able to correlate with us, but not inject 8KB of junk or
// newlines into every log line we write about them.
func RequestID() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			id := sanitizeRequestID(r.Header.Get(HeaderRequestID))
			if id == "" {
				// UUIDv7: time-ordered, so IDs sort chronologically in a log
				// aggregator. Same choice as the database primary keys.
				if v7, err := uuid.NewV7(); err == nil {
					id = v7.String()
				} else {
					id = uuid.NewString()
				}
			}
			w.Header().Set(HeaderRequestID, id)
			ctx := logging.With(r.Context(), slog.String(logging.KeyRequestID, id))
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func sanitizeRequestID(s string) string {
	if len(s) > 64 {
		s = s[:64]
	}
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9',
			r == '-', r == '_', r == '.', r == ':':
			b.WriteRune(r)
		}
	}
	return b.String()
}

// RealIP resolves the client address.
//
// trustProxy MUST be false unless the process genuinely sits behind a proxy
// that overwrites X-Forwarded-For. Trusting the header on a directly-exposed
// listener lets any caller forge their own source IP, which silently defeats
// per-IP rate limiting and poisons audit logs.
func RealIP(trustProxy bool) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if trustProxy {
				if ip := firstForwardedFor(r.Header.Get("X-Forwarded-For")); ip != "" {
					r.RemoteAddr = net.JoinHostPort(ip, "0")
				} else if ip := strings.TrimSpace(r.Header.Get("X-Real-Ip")); ip != "" {
					r.RemoteAddr = net.JoinHostPort(ip, "0")
				}
			}
			next.ServeHTTP(w, r)
		})
	}
}

func firstForwardedFor(v string) string {
	if v == "" {
		return ""
	}
	// The left-most entry is the original client. Everything after it was
	// appended by intermediaries.
	head, _, _ := strings.Cut(v, ",")
	head = strings.TrimSpace(head)
	if net.ParseIP(head) == nil {
		return ""
	}
	return head
}

// Recoverer turns a panic into a 500 instead of killing the whole process.
//
// http.ErrAbortHandler is re-panicked deliberately: net/http uses it as the
// documented way to abort a connection (and httputil.ReverseProxy relies on
// it), so swallowing it would break streaming and WebSocket handling.
func Recoverer(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// The deferred closure runs on the handler's own goroutine and uses
			// r.Context() throughout, so there is no context to thread that it
			// does not already have.
			defer func() { //nolint:contextcheck // recovery path uses r.Context()
				rec := recover()
				if rec == nil {
					return
				}
				if rec == http.ErrAbortHandler { //nolint:errorlint // sentinel compared by identity, as net/http does
					panic(rec)
				}
				log.ErrorContext(r.Context(), "panic serving request",
					"panic", rec,
					"method", r.Method,
					"path", r.URL.Path,
					"stack", string(debug.Stack()),
				)
				WriteError(w, r, ErrInternal)
			}()
			next.ServeHTTP(w, r)
		})
	}
}

// AccessLog emits one canonical wide event per request.
//
// One record with every field beats five narrow records you have to stitch
// together: it is a single row in any log tool, it can be aggregated without a
// join, and it makes "show me every 5xx for tenant X in the last hour" a
// one-line query.
func AccessLog(log *slog.Logger, listener string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Probes and scrapes would drown out real traffic at 1 line every
			// 5 seconds per pod, forever.
			if isNoise(r.URL.Path) {
				next.ServeHTTP(w, r)
				return
			}

			start := time.Now()
			ww := chimw.NewWrapResponseWriter(w, r.ProtoMajor)
			next.ServeHTTP(ww, r)

			dur := time.Since(start)
			status := ww.Status()
			if status == 0 {
				status = http.StatusOK
			}

			attrs := []any{
				"method", r.Method,
				"path", r.URL.Path,
				"route", routePattern(r),
				"status", status,
				logging.KeyDuration, dur,
				"bytes", ww.BytesWritten(),
				"remote", clientIP(r.RemoteAddr),
				"listener", listener,
			}
			if ua := r.UserAgent(); ua != "" {
				attrs = append(attrs, "user_agent", ua)
			}
			if q := r.URL.RawQuery; q != "" && len(q) < 512 {
				attrs = append(attrs, "query", q)
			}

			switch {
			case status >= 500:
				log.ErrorContext(r.Context(), "http request", attrs...)
			case status >= 400:
				log.WarnContext(r.Context(), "http request", attrs...)
			default:
				log.InfoContext(r.Context(), "http request", attrs...)
			}
		})
	}
}

func isNoise(path string) bool {
	switch path {
	case "/healthz", "/readyz", "/startupz", "/metrics":
		return true
	}
	return false
}

func clientIP(remoteAddr string) string {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return remoteAddr
	}
	return host
}

// Metrics instruments a listener.
//
// The `route` label is chi's route PATTERN, never the raw path. Using the path
// would put a distinct time series behind every event ID we ever serve, which
// is the classic way to make your own monitoring fall over.
func Metrics(m *metrics.Registry, listener string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/metrics" {
				next.ServeHTTP(w, r)
				return
			}

			inFlight := m.HTTPInFlight.WithLabelValues(listener)
			inFlight.Inc()
			defer inFlight.Dec()

			start := time.Now()
			ww := chimw.NewWrapResponseWriter(w, r.ProtoMajor)
			next.ServeHTTP(ww, r)

			route := routePattern(r)
			status := ww.Status()
			if status == 0 {
				status = http.StatusOK
			}

			m.HTTPDuration.WithLabelValues(listener, r.Method, route).Observe(time.Since(start).Seconds())
			m.HTTPRequests.WithLabelValues(listener, r.Method, route, statusClass(status)).Inc()
			m.HTTPRespSize.WithLabelValues(listener, route).Observe(float64(ww.BytesWritten()))
			if r.ContentLength > 0 {
				m.HTTPReqSize.WithLabelValues(listener, route).Observe(float64(r.ContentLength))
			}
		})
	}
}

// statusClass collapses 200/201/204 into "2xx". Exact codes belong in logs and
// traces; metrics only need the class, and collapsing keeps cardinality flat.
func statusClass(code int) string {
	switch {
	case code < 200:
		return "1xx"
	case code < 300:
		return "2xx"
	case code < 400:
		return "3xx"
	case code < 500:
		return "4xx"
	default:
		return "5xx"
	}
}

func routePattern(r *http.Request) string {
	if rctx := chi.RouteContext(r.Context()); rctx != nil {
		if p := rctx.RoutePattern(); p != "" {
			return p
		}
	}
	// Unmatched requests all share one label rather than one per 404 path,
	// which is exactly the scanner-traffic cardinality bomb we are avoiding.
	return "unmatched"
}

// MaxBodyBytes caps request bodies so a single caller cannot exhaust memory.
func MaxBodyBytes(n int64) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			r.Body = http.MaxBytesReader(w, r.Body, n)
			next.ServeHTTP(w, r)
		})
	}
}

// NoStore marks responses uncacheable. Applied to the whole API: a control
// plane serving live delivery state must never be cached by an intermediary.
func NoStore(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		next.ServeHTTP(w, r)
	})
}
