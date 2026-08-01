// Package logging sets up structured logging on log/slog.
//
// The design goal is one wide, canonical event per unit of work rather than
// several narrow lines scattered through a call path — a single delivery
// attempt should produce one log record with ~20 fields, not eight records you
// have to correlate by hand at 3am.
//
// The mechanism that makes that practical is the ContextHandler: request IDs,
// tenant IDs and (from Phase 2) trace IDs are attached to the context once, at
// the edge, and every record logged with that context carries them
// automatically. Call sites never thread a logger through their signatures.
package logging

import (
	"context"
	"io"
	"log/slog"
	"os"
	"slices"
	"strings"
	"sync"
)

// Standard attribute keys. Using constants keeps the field names stable across
// services, which is what makes a log query like `request_id=X` work at all.
const (
	KeyRequestID = "request_id"
	KeyTenantID  = "tenant_id"
	KeyTraceID   = "trace_id"
	KeySpanID    = "span_id"
	KeyService   = "service"
	KeyEndpoint  = "endpoint_id"
	KeyDelivery  = "delivery_id"
	KeyEvent     = "event_id"
	KeyStream    = "stream_id"
	KeyAttempt   = "attempt"
	KeyOutcome   = "outcome"
	KeyDuration  = "duration_ms"
)

// Options configures the root logger.
type Options struct {
	Level     slog.Level
	Format    string // "json" | "text"
	AddSource bool
	Service   string
	Version   string
	Writer    io.Writer // defaults to os.Stdout
}

// New builds the root logger. It is installed as the slog default so that any
// library reaching for slog.Info lands in the same stream.
func New(opts Options) *slog.Logger {
	w := opts.Writer
	if w == nil {
		w = os.Stdout
	}

	ho := &slog.HandlerOptions{
		Level:       opts.Level,
		AddSource:   opts.AddSource,
		ReplaceAttr: replaceAttr,
	}

	var h slog.Handler
	if strings.EqualFold(opts.Format, "text") {
		h = slog.NewTextHandler(w, ho)
	} else {
		h = slog.NewJSONHandler(w, ho)
	}

	h = &contextHandler{Handler: h}

	base := []slog.Attr{}
	if opts.Service != "" {
		base = append(base, slog.String(KeyService, opts.Service))
	}
	if opts.Version != "" {
		base = append(base, slog.String("version", opts.Version))
	}
	if len(base) > 0 {
		h = h.WithAttrs(base)
	}

	l := slog.New(h)
	slog.SetDefault(l)
	return l
}

// replaceAttr normalises a couple of things that otherwise make logs annoying
// to query: slog's default "time"/"msg"/"level" keys are fine, but durations
// render as "1.5s" strings which no aggregator can do arithmetic on.
func replaceAttr(groups []string, a slog.Attr) slog.Attr {
	if len(groups) > 0 {
		return a
	}
	if a.Value.Kind() == slog.KindDuration {
		return slog.Float64(a.Key, float64(a.Value.Duration().Microseconds())/1000)
	}
	return a
}

// ── context-carried attributes ───────────────────────────────────────────────

type ctxKey struct{}

type ctxAttrs struct {
	attrs []slog.Attr
}

// With returns a context carrying additional attributes. Every record logged
// with the returned context (or a descendant) includes them.
//
// It copies rather than appends in place: contexts are shared across
// goroutines, and appending to a slice that another goroutine also holds is a
// data race that only shows up under load.
func With(ctx context.Context, attrs ...slog.Attr) context.Context {
	if len(attrs) == 0 {
		return ctx
	}
	var existing []slog.Attr
	if cur, ok := ctx.Value(ctxKey{}).(*ctxAttrs); ok {
		existing = cur.attrs
	}
	merged := make([]slog.Attr, 0, len(existing)+len(attrs))
	merged = append(merged, existing...)
	merged = append(merged, attrs...)
	return context.WithValue(ctx, ctxKey{}, &ctxAttrs{attrs: merged})
}

// Attrs returns the attributes carried by ctx. The result must not be mutated.
func Attrs(ctx context.Context) []slog.Attr {
	if cur, ok := ctx.Value(ctxKey{}).(*ctxAttrs); ok {
		return cur.attrs
	}
	return nil
}

// contextHandler injects context-carried attributes into every record.
type contextHandler struct {
	slog.Handler
}

func (h *contextHandler) Handle(ctx context.Context, r slog.Record) error {
	if attrs := Attrs(ctx); len(attrs) > 0 {
		r.AddAttrs(attrs...)
	}
	return h.Handler.Handle(ctx, r)
}

func (h *contextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &contextHandler{Handler: h.Handler.WithAttrs(attrs)}
}

func (h *contextHandler) WithGroup(name string) slog.Handler {
	return &contextHandler{Handler: h.Handler.WithGroup(name)}
}

// ── logger-in-context (for the rare case a component needs its own) ──────────

type loggerKey struct{}

// IntoContext stores a logger in ctx.
func IntoContext(ctx context.Context, l *slog.Logger) context.Context {
	return context.WithValue(ctx, loggerKey{}, l)
}

// FromContext returns the logger stored in ctx, or the default logger.
func FromContext(ctx context.Context) *slog.Logger {
	if l, ok := ctx.Value(loggerKey{}).(*slog.Logger); ok {
		return l
	}
	return slog.Default()
}

// ── test helper ──────────────────────────────────────────────────────────────

// Discard is a logger that drops everything, for tests that do not assert on
// output.
var Discard = sync.OnceValue(func() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError + 1}))
})

// SortedKeys is a small helper used by tests to make attribute assertions
// order-independent.
func SortedKeys(attrs []slog.Attr) []string {
	keys := make([]string, 0, len(attrs))
	for _, a := range attrs {
		keys = append(keys, a.Key)
	}
	slices.Sort(keys)
	return keys
}
