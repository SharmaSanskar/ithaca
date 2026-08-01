package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"testing"
)

func newTestLogger(t *testing.T) (*slog.Logger, *bytes.Buffer) {
	t.Helper()
	var buf bytes.Buffer
	l := slog.New(&contextHandler{
		Handler: slog.NewJSONHandler(&buf, &slog.HandlerOptions{
			Level:       slog.LevelDebug,
			ReplaceAttr: replaceAttr,
		}),
	})
	return l, &buf
}

func decode(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(buf.Bytes(), &m); err != nil {
		t.Fatalf("log line is not valid JSON: %v\nline: %s", err, buf.String())
	}
	return m
}

func TestContextAttrsAreInjected(t *testing.T) {
	l, buf := newTestLogger(t)

	ctx := With(context.Background(),
		slog.String(KeyRequestID, "req_1"),
		slog.String(KeyTenantID, "ten_1"),
	)
	l.InfoContext(ctx, "hello")

	got := decode(t, buf)
	if got[KeyRequestID] != "req_1" {
		t.Errorf("%s = %v, want req_1", KeyRequestID, got[KeyRequestID])
	}
	if got[KeyTenantID] != "ten_1" {
		t.Errorf("%s = %v, want ten_1", KeyTenantID, got[KeyTenantID])
	}
}

func TestContextAttrsAccumulate(t *testing.T) {
	l, buf := newTestLogger(t)

	ctx := With(context.Background(), slog.String(KeyRequestID, "req_1"))
	ctx = With(ctx, slog.String(KeyEndpoint, "ep_1"))
	l.InfoContext(ctx, "hello")

	got := decode(t, buf)
	if got[KeyRequestID] != "req_1" || got[KeyEndpoint] != "ep_1" {
		t.Errorf("expected both attrs, got %v", got)
	}
}

func TestWithDoesNotMutateParentContext(t *testing.T) {
	// The bug this guards against: appending to the parent's slice in place.
	// Two goroutines deriving from one request context would then race, and
	// the symptom is attributes from one request appearing on another — which
	// is both a correctness bug and a tenant-isolation bug.
	parent := With(context.Background(), slog.String("a", "1"))

	childB := With(parent, slog.String("b", "2"))
	childC := With(parent, slog.String("c", "3"))

	if keys := SortedKeys(Attrs(parent)); len(keys) != 1 || keys[0] != "a" {
		t.Errorf("parent context was mutated: %v", keys)
	}

	for name, ctx := range map[string]context.Context{"b": childB, "c": childC} {
		keys := SortedKeys(Attrs(ctx))
		if len(keys) != 2 {
			t.Errorf("child %s: got %v, want exactly 2 attrs", name, keys)
		}
	}
	// childB must not have picked up childC's attribute.
	for _, k := range SortedKeys(Attrs(childB)) {
		if k == "c" {
			t.Error("childB leaked an attribute from childC")
		}
	}
}

func TestWithIsRaceFree(t *testing.T) {
	// Run with -race; the point is the concurrent derivation, not the output.
	parent := With(context.Background(), slog.String("root", "1"))
	l, _ := newTestLogger(t)

	var wg sync.WaitGroup
	for i := range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := With(parent, slog.Int("i", i))
			l.InfoContext(ctx, "concurrent")
			_ = Attrs(ctx)
		}()
	}
	wg.Wait()
}

func TestDurationsAreRenderedAsMilliseconds(t *testing.T) {
	// "1.5s" is a string no aggregator can average. A number can be graphed.
	l, buf := newTestLogger(t)
	l.Info("timed", slog.Duration(KeyDuration, 1500*1000*1000))

	got := decode(t, buf)
	v, ok := got[KeyDuration].(float64)
	if !ok {
		t.Fatalf("%s = %#v, want a number", KeyDuration, got[KeyDuration])
	}
	if v != 1500 {
		t.Errorf("%s = %v, want 1500", KeyDuration, v)
	}
}

func TestWithNoAttrsReturnsSameContext(t *testing.T) {
	ctx := context.Background()
	if With(ctx) != ctx {
		t.Error("With() with no attrs should return the context unchanged")
	}
}

func TestFromContextFallsBackToDefault(t *testing.T) {
	if FromContext(context.Background()) == nil {
		t.Fatal("FromContext returned nil")
	}
	custom, _ := newTestLogger(t)
	ctx := IntoContext(context.Background(), custom)
	if FromContext(ctx) != custom {
		t.Error("FromContext did not return the stored logger")
	}
}
