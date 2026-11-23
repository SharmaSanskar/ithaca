package otelx

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/SharmaSanskar/ithaca/internal/platform/config"
)

// Booting with tracing ENABLED must work. The first version failed at startup
// with "conflicting Schema URL", and every other test ran with tracing off,
// so nothing caught it until a real process started.
func TestSetupWithTracingEnabled(t *testing.T) {
	shutdown, err := Setup(context.Background(), config.Telemetry{
		Enabled: true, OTLPEndpoint: "127.0.0.1:1", SampleRatio: 1, ServiceName: "ithaca-test",
	}, "test")
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // nothing is listening; don't wait for an export
	_ = shutdown(ctx)
}

func TestLinkRoundTripsThroughATraceparent(t *testing.T) {
	ctx, span := otel.GetTracerProvider().Tracer("t").Start(context.Background(), "ingest")
	defer span.End()
	// With the global no-op provider there is no valid span; build one.
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{1, 2, 3}, SpanID: trace.SpanID{4, 5, 6}, TraceFlags: trace.FlagsSampled,
	})
	tp := Traceparent(trace.ContextWithSpanContext(ctx, sc))
	if tp == "" {
		t.Fatal("no traceparent rendered")
	}
	link, ok := LinkFromTraceparent(context.Background(), tp)
	if !ok || link.SpanContext.TraceID() != sc.TraceID() || link.SpanContext.SpanID() != sc.SpanID() {
		t.Fatalf("link %+v does not point back at the stored span %s", link.SpanContext, tp)
	}
	if _, ok := LinkFromTraceparent(context.Background(), "garbage"); ok {
		t.Error("a garbage traceparent produced a link")
	}
}
