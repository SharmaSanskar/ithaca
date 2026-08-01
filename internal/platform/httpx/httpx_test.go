package httpx

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"

	"github.com/SharmaSanskar/ithaca/internal/platform/logging"
	"github.com/SharmaSanskar/ithaca/internal/platform/metrics"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError + 1}))
}

func TestRequestIDGeneratedWhenAbsent(t *testing.T) {
	var seen string
	h := RequestID()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, a := range logging.Attrs(r.Context()) {
			if a.Key == logging.KeyRequestID {
				seen = a.Value.String()
			}
		}
	}))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	if seen == "" {
		t.Fatal("no request id was attached to the context")
	}
	if got := rec.Header().Get(HeaderRequestID); got != seen {
		t.Errorf("response header = %q, context = %q; they must match", got, seen)
	}
}

func TestRequestIDIsSanitized(t *testing.T) {
	// An inbound ID is trusted for correlation but never verbatim: newlines
	// forge log entries, and 8KB of junk lands in every line about the caller.
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"clean uuid", "0198a1f0-1111-7000-8000-000000000001", "0198a1f0-1111-7000-8000-000000000001"},
		{"strips newlines", "abc\ndef", "abcdef"},
		{"strips spaces and quotes", `a b"c`, "abc"},
		{"keeps trace-ish separators", "trace:01-02.03_04", "trace:01-02.03_04"},
		{"truncates", strings.Repeat("a", 200), strings.Repeat("a", 64)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sanitizeRequestID(tt.in); got != tt.want {
				t.Errorf("sanitizeRequestID(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestRequestIDIsPropagatedFromCaller(t *testing.T) {
	h := RequestID()(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set(HeaderRequestID, "caller-supplied-1")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if got := rec.Header().Get(HeaderRequestID); got != "caller-supplied-1" {
		t.Errorf("request id = %q, want the caller's value echoed back", got)
	}
}

func TestRealIPIgnoresForwardedHeaderWhenProxyIsUntrusted(t *testing.T) {
	// Trusting X-Forwarded-For on a directly-exposed listener lets any caller
	// forge their source IP, which silently defeats per-IP rate limiting.
	var got string
	h := RealIP(false)(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		got = r.RemoteAddr
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "10.1.2.3:5555"
	req.Header.Set("X-Forwarded-For", "1.2.3.4")
	h.ServeHTTP(httptest.NewRecorder(), req)

	if got != "10.1.2.3:5555" {
		t.Errorf("RemoteAddr = %q, want the real socket address", got)
	}
}

func TestRealIPUsesLeftmostForwardedEntryWhenTrusted(t *testing.T) {
	var got string
	h := RealIP(true)(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		got = r.RemoteAddr
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "10.1.2.3:5555"
	req.Header.Set("X-Forwarded-For", "203.0.113.7, 10.0.0.1, 10.0.0.2")
	h.ServeHTTP(httptest.NewRecorder(), req)

	if !strings.HasPrefix(got, "203.0.113.7") {
		t.Errorf("RemoteAddr = %q, want the left-most (original client) entry", got)
	}
}

func TestFirstForwardedForRejectsGarbage(t *testing.T) {
	for _, in := range []string{"", "not-an-ip", "not-an-ip, 1.2.3.4", "  "} {
		if got := firstForwardedFor(in); got != "" {
			t.Errorf("firstForwardedFor(%q) = %q, want empty", in, got)
		}
	}
}

func TestRecovererTurnsPanicIntoFiveHundred(t *testing.T) {
	h := RequestID()(Recoverer(quietLogger())(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("boom")
	})))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}

	var body struct {
		Error Error `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("body is not the standard error envelope: %v (%s)", err, rec.Body.String())
	}
	if body.Error.Code != "internal_error" {
		t.Errorf("code = %q, want internal_error", body.Error.Code)
	}
	if strings.Contains(rec.Body.String(), "boom") {
		t.Error("the panic value leaked into the response body")
	}
	if body.Error.RequestID == "" {
		t.Error("the error response should carry the request id")
	}
}

func TestRecovererRepanicsAbortHandler(t *testing.T) {
	// net/http documents ErrAbortHandler as the way to abort a connection, and
	// httputil.ReverseProxy relies on it. Swallowing it breaks streaming.
	h := Recoverer(quietLogger())(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic(http.ErrAbortHandler)
	}))

	defer func() {
		if rec := recover(); rec != http.ErrAbortHandler { //nolint:errorlint // identity check, as net/http does
			t.Errorf("recovered %v, want ErrAbortHandler to propagate", rec)
		}
	}()
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
}

func TestWriteErrorHidesInternalCauses(t *testing.T) {
	// The boundary that stops a wrapped pgx error from telling the internet
	// your column names.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	WriteError(rec, req, errors.New(`pq: column "api_key_hash" does not exist`))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	if strings.Contains(rec.Body.String(), "api_key_hash") {
		t.Errorf("internal error text leaked to the client: %s", rec.Body.String())
	}
}

func TestWriteErrorPreservesAPIErrors(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	WriteError(rec, req, ErrNotFound.WithMessage("No such endpoint."))

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "No such endpoint.") {
		t.Errorf("message was dropped: %s", rec.Body.String())
	}
}

func TestErrorHelpersDoNotMutateTheSharedSentinel(t *testing.T) {
	// ErrNotFound is a package-level value. If WithMessage mutated it in
	// place, one handler's message would appear on every 404 in the process.
	custom := ErrNotFound.WithMessage("gone fishing")
	if ErrNotFound.Message == "gone fishing" {
		t.Fatal("WithMessage mutated the shared sentinel error")
	}
	if custom.Message != "gone fishing" {
		t.Errorf("custom.Message = %q", custom.Message)
	}
	if custom.Status() != http.StatusNotFound {
		t.Errorf("custom.Status() = %d, want 404", custom.Status())
	}
}

func TestDecodeJSON(t *testing.T) {
	type payload struct {
		Type string `json:"type"`
	}

	tests := []struct {
		name        string
		body        string
		contentType string
		wantErr     bool
		wantCode    string
	}{
		{name: "valid", body: `{"type":"order.created"}`, contentType: "application/json"},
		{name: "valid with charset", body: `{"type":"x"}`, contentType: "application/json; charset=utf-8"},
		{name: "no content type is allowed", body: `{"type":"x"}`},
		{name: "wrong content type", body: `{"type":"x"}`, contentType: "text/plain", wantErr: true, wantCode: "bad_request"},
		{name: "malformed", body: `{"type":`, contentType: "application/json", wantErr: true, wantCode: "bad_request"},
		{name: "wrong field type", body: `{"type":123}`, contentType: "application/json", wantErr: true, wantCode: "bad_request"},
		{name: "unknown field", body: `{"typo":"x"}`, contentType: "application/json", wantErr: true, wantCode: "bad_request"},
		{name: "trailing value", body: `{"type":"a"}{"type":"b"}`, contentType: "application/json", wantErr: true, wantCode: "bad_request"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
			if tt.contentType != "" {
				req.Header.Set("Content-Type", tt.contentType)
			}

			var dst payload
			err := DecodeJSON(httptest.NewRecorder(), req, &dst)

			if tt.wantErr {
				if err == nil {
					t.Fatal("DecodeJSON() error = nil, want an error")
				}
				var apiErr *Error
				if !errors.As(err, &apiErr) {
					t.Fatalf("error is not an *Error: %v", err)
				}
				if apiErr.Code != tt.wantCode {
					t.Errorf("code = %q, want %q", apiErr.Code, tt.wantCode)
				}
				return
			}
			if err != nil {
				t.Fatalf("DecodeJSON() error = %v", err)
			}
		})
	}
}

func TestMaxBodyBytesProducesPayloadTooLarge(t *testing.T) {
	var decodeErr error
	h := MaxBodyBytes(16)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var dst map[string]any
		decodeErr = DecodeJSON(w, r, &dst)
	}))

	body := `{"payload":"` + strings.Repeat("x", 512) + `"}`
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	h.ServeHTTP(httptest.NewRecorder(), req)

	var apiErr *Error
	if !errors.As(decodeErr, &apiErr) || apiErr.Code != "payload_too_large" {
		t.Fatalf("error = %v, want payload_too_large", decodeErr)
	}
}

func TestMetricsUsesRoutePatternNotPath(t *testing.T) {
	// Labeling by raw path would create one time series per event ID — the
	// classic way to take down your own Prometheus.
	m := metrics.New("test")
	r := chi.NewRouter()
	r.Use(Metrics(m, "api"))
	r.Get("/v1/events/{id}", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })

	for _, id := range []string{"evt_1", "evt_2", "evt_3"} {
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/events/"+id, nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d", rec.Code)
		}
	}

	families, err := m.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}

	routes := map[string]bool{}
	for _, f := range families {
		if f.GetName() != "ithaca_http_requests_total" {
			continue
		}
		for _, metric := range f.GetMetric() {
			for _, lbl := range metric.GetLabel() {
				if lbl.GetName() == "route" {
					routes[lbl.GetValue()] = true
				}
			}
		}
	}

	if len(routes) != 1 {
		t.Fatalf("route labels = %v, want exactly one (the pattern)", routes)
	}
	if !routes["/v1/events/{id}"] {
		t.Errorf("route label = %v, want the chi pattern", routes)
	}
}

func TestMetricsCollapsesStatusIntoClasses(t *testing.T) {
	tests := []struct {
		code int
		want string
	}{
		{100, "1xx"}, {200, "2xx"}, {201, "2xx"}, {301, "3xx"},
		{404, "4xx"}, {422, "4xx"}, {500, "5xx"}, {503, "5xx"},
	}
	for _, tt := range tests {
		if got := statusClass(tt.code); got != tt.want {
			t.Errorf("statusClass(%d) = %q, want %q", tt.code, got, tt.want)
		}
	}
}

func TestUnmatchedRoutesShareOneLabel(t *testing.T) {
	// Scanner traffic hits thousands of distinct 404 paths. They must all
	// collapse to a single series.
	r := chi.NewRouter()
	r.Get("/known", func(w http.ResponseWriter, _ *http.Request) {})

	var got []string
	r.NotFound(func(_ http.ResponseWriter, req *http.Request) {
		got = append(got, routePattern(req))
	})

	for _, p := range []string{"/wp-admin", "/.env", "/phpmyadmin"} {
		r.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, p, nil))
	}

	for _, label := range got {
		if label != "unmatched" {
			t.Errorf("route label = %q, want %q", label, "unmatched")
		}
	}
}

func TestNewServerFailsFastOnBadAddress(t *testing.T) {
	// A bind failure is a config error and must kill the process at boot with
	// a clear message, not 200ms later from a goroutine.
	_, err := NewServer(ServerConfig{
		Name:    "api",
		Addr:    "256.256.256.256:99999",
		Handler: http.NewServeMux(),
		Log:     quietLogger(),
	})
	if err == nil {
		t.Fatal("NewServer() error = nil, want a bind error")
	}
}

func TestServerLifecycle(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/ping", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("pong"))
	})

	srv, err := NewServer(ServerConfig{
		Name: "test", Addr: "127.0.0.1:0", Handler: mux, Log: quietLogger(),
	})
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	comp := srv.Component()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- comp.Run(ctx) }()

	resp, err := http.Get("http://" + srv.Addr() + "/ping") //nolint:noctx // short-lived test request
	if err != nil {
		t.Fatalf("GET error = %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if string(body) != "pong" {
		t.Errorf("body = %q, want pong", body)
	}

	if err := comp.Stop(context.Background()); err != nil {
		t.Errorf("Stop() error = %v", err)
	}
	if err := <-errCh; err != nil {
		t.Errorf("Run() error = %v, want nil after a clean shutdown", err)
	}
}
