package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/SharmaSanskar/ithaca/internal/platform/config"
)

// TestEveryServiceWires boots the wiring for all six services.
//
// This exists because the first version of addPublicListener registered the
// health route before calling mount, and chi panics on a Use() that follows any
// route registration — so `ithaca ingest` died at startup while `ithaca api`
// (which adds its middleware inside a sub-router) came up fine. A unit test
// suite that never constructs a router cannot catch that; only actually wiring
// every service can.
func TestEveryServiceWires(t *testing.T) {
	for _, svc := range config.Services {
		t.Run(string(svc), func(t *testing.T) {
			// Port 0 lets the kernel pick, so the test never collides with a
			// developer's running stack or with a parallel test.
			t.Setenv("ITHACA_ENV", "dev")
			t.Setenv("ITHACA_HTTP_ADDR", "127.0.0.1:0")
			t.Setenv("ITHACA_ADMIN_ADDR", "127.0.0.1:0")

			cfg, err := config.Load(svc)
			if err != nil {
				t.Fatalf("config.Load(%s) error = %v", svc, err)
			}

			a := newApp(cfg)
			if err := a.wire(); err != nil {
				t.Fatalf("wire(%s) error = %v", svc, err)
			}

			// Bring it all the way up and back down: binding, serving and
			// draining are as much a part of "does this service work" as
			// construction is.
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- a.run(ctx) }()

			time.Sleep(75 * time.Millisecond)
			cancel()

			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("run(%s) error = %v", svc, err)
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("run(%s) did not shut down", svc)
			}
		})
	}
}

// TestUnbuiltRoutesAnswerHonestly checks that stubs return 501 rather than 404.
// A 404 on a route that exists but is unfinished is indistinguishable from a
// typo, which makes the skeleton impossible to smoke-test.
func TestUnbuiltRoutesAnswerHonestly(t *testing.T) {
	h := notImplemented("event ingestion", "Phase 1")

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/events", nil)
	h(rec, req)

	if rec.Code != http.StatusNotImplemented {
		t.Errorf("status = %d, want 501", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "Phase 1") {
		t.Errorf("body should name the phase that will build it: %s", rec.Body.String())
	}
}
