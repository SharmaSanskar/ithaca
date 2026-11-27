package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestProbeSucceedsOnlyOn2xx(t *testing.T) {
	status := http.StatusOK
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(status) }))
	defer srv.Close()

	if err := runProbe([]string{srv.URL}); err != nil {
		t.Errorf("200: %v, want success", err)
	}
	status = http.StatusServiceUnavailable // a not-ready service must fail its health check
	if err := runProbe([]string{srv.URL}); err == nil {
		t.Error("503: succeeded, want failure")
	}
	srv.Close()
	if err := runProbe([]string{srv.URL}); err == nil {
		t.Error("connection refused: succeeded, want failure")
	}
	if err := runProbe([]string{"http://example.com/readyz"}); err == nil {
		t.Error("non-loopback host: succeeded, want refusal")
	}
	if err := runProbe(nil); err == nil {
		t.Error("no URL: succeeded, want a usage error")
	}
}
