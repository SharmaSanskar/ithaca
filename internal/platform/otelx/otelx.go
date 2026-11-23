// Package otelx sets up OpenTelemetry tracing.
//
// When telemetry is disabled nothing is installed and the global tracer stays
// the no-op one, so instrumented code costs almost nothing and needs no
// branches.
package otelx

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/SharmaSanskar/ithaca/internal/platform/config"
)

// Setup installs the global tracer provider and W3C propagator. The returned
// function flushes and shuts the exporter down; call it on exit.
func Setup(ctx context.Context, cfg config.Telemetry, version string) (func(context.Context) error, error) {
	// The propagator is installed even when tracing is off, so an incoming
	// traceparent is still carried through and stored on the event.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))
	if !cfg.Enabled {
		return func(context.Context) error { return nil }, nil
	}

	exp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(cfg.OTLPEndpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("otlp exporter: %w", err)
	}
	// Schemaless on purpose. resource.Default() carries the semconv schema
	// URL of whatever SDK version is installed; merging a resource built
	// against a different semconv version fails at boot with "conflicting
	// Schema URL" — which is exactly how this first broke, and it would break
	// again on every SDK upgrade that bumps the schema.
	res, err := resource.Merge(resource.Default(), resource.NewSchemaless(
		attribute.String("service.name", cfg.ServiceName),
		attribute.String("service.version", version),
	))
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		// Batching matters for the at-least-once story: span export must
		// never sit between an HTTP 200 and the ack commit. End() only
		// enqueues; export happens on the batcher's own goroutine.
		sdktrace.WithBatcher(exp, sdktrace.WithBatchTimeout(2*time.Second)),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.SampleRatio))),
	)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}

// LinkFromTraceparent turns a stored W3C traceparent into a span link.
//
// Why a LINK and not a parent: a delivery can run hours after ingest (that is
// what retries are). As a child span it would make the ingest trace six hours
// long, which every trace UI renders as nonsense, and it would chain the
// sampling decision for the whole retry ladder to one coin flip at ingest. A
// new root span that links back keeps each trace short and still lets you
// click from a delivery to the request that caused it.
func LinkFromTraceparent(ctx context.Context, traceparent string) (trace.Link, bool) {
	if traceparent == "" {
		return trace.Link{}, false
	}
	carrier := propagation.MapCarrier{"traceparent": traceparent}
	// Extract into a context with NO current span, so the result is the
	// stored span and never whatever span ctx happens to carry.
	sc := trace.SpanContextFromContext(propagation.TraceContext{}.Extract(trace.ContextWithSpanContext(ctx, trace.SpanContext{}), carrier))
	if !sc.IsValid() {
		return trace.Link{}, false
	}
	return trace.Link{SpanContext: sc}, true
}

// Traceparent renders ctx's span as a W3C traceparent ("" if none).
func Traceparent(ctx context.Context) string {
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	return carrier["traceparent"]
}
