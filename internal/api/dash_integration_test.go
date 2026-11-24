//go:build integration

package api_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/SharmaSanskar/ithaca/internal/api"
	"github.com/SharmaSanskar/ithaca/internal/auth"
	"github.com/SharmaSanskar/ithaca/internal/demo"
	"github.com/SharmaSanskar/ithaca/internal/ingest"
	"github.com/SharmaSanskar/ithaca/internal/testdb"
	"github.com/SharmaSanskar/ithaca/internal/testredis"
)

func TestDashboardAndDemoControlsOverHTTP(t *testing.T) {
	ctx := context.Background()
	s := testdb.New(t)
	rdb := testredis.New(t)
	seed, err := demo.Seed(ctx, s, "http://sinks.internal:8090")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Pool.Exec(ctx, "UPDATE endpoints SET state='enabled'"); err != nil {
		t.Fatal(err)
	}
	svc := ingest.NewService(s, nil)
	for i := range 12 {
		body := fmt.Sprintf(`{"type":"order.created","ordering_key":"order_%d","payload":{"i":%d}}`, i%3, i)
		if _, err := svc.Ingest(ctx, ingest.Request{TenantID: seed.TenantID, Method: "POST", Path: "/v1/events", Body: []byte(body)}); err != nil {
			t.Fatal(err)
		}
	}

	cp := api.NewControlPlane(s, 50)
	cp.RestrictURLs("http://sinks.internal:8090/sink/")
	dash := api.NewDashboard(s, cp)
	controls := &api.DemoControls{Redis: rdb, PerMinute: 5}
	r := chi.NewRouter()
	r.Route("/api/v1", func(r chi.Router) {
		r.Group(func(r chi.Router) {
			r.Use(auth.NewAuthenticator(s, time.Minute).Middleware)
			cp.Routes(r)
		})
		dash.Routes(r, controls.RateLimit)
		controls.Routes(r)
	})
	srv := httptest.NewServer(r)
	defer srv.Close()

	get := func(path string) (int, map[string]any) {
		t.Helper()
		resp, err := http.Get(srv.URL + "/api/v1" + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		out := map[string]any{}
		_ = json.NewDecoder(resp.Body).Decode(&out)
		return resp.StatusCode, out
	}
	send := func(method, path, body string, header ...string) (int, map[string]any) {
		t.Helper()
		req, _ := http.NewRequest(method, srv.URL+"/api/v1"+path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		for i := 0; i+1 < len(header); i += 2 {
			req.Header.Set(header[i], header[i+1])
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		out := map[string]any{}
		_ = json.Unmarshal(b, &out)
		return resp.StatusCode, out
	}

	// ── read-only dashboard: no credentials needed ──
	code, ov := get("/dash/overview")
	if code != 200 || len(ov["endpoints"].([]any)) != len(demo.Sinks) {
		t.Fatalf("overview: %d %v", code, ov)
	}
	if ov["totals"].(map[string]any)["pending"].(float64) == 0 {
		t.Error("overview reports no pending deliveries")
	}

	code, evs := get("/dash/events?limit=5")
	if code != 200 || len(evs["data"].([]any)) != 5 || evs["next_cursor"] == nil {
		t.Fatalf("events page 1: %d %v", code, evs)
	}
	first := evs["data"].([]any)[0].(map[string]any)
	if first["pending"].(float64) < 1 {
		t.Errorf("event row has no delivery status: %v", first)
	}

	code, ev := get("/dash/events/" + first["id"].(string))
	if code != 200 || ev["payload"] == nil || len(ev["deliveries"].([]any)) == 0 {
		t.Fatalf("event detail: %d %v", code, ev)
	}
	d := ev["deliveries"].([]any)[0].(map[string]any)
	if d["status"] != "pending" || d["history"] == nil {
		t.Errorf("delivery = %v", d)
	}

	code, series := get("/dash/series?window=300&bucket=10")
	if code != 200 {
		t.Fatalf("series: %d", code)
	}
	pts := series["data"].([]any)
	if len(pts) < 30 || len(pts) > 32 {
		t.Errorf("series has %d points for 300s/10s; it must be dense (zeros included)", len(pts))
	}
	ingested := 0.0
	for _, p := range pts {
		ingested += p.(map[string]any)["ingested"].(float64)
	}
	if ingested != 12 {
		t.Errorf("series counted %v events ingested, want 12", ingested)
	}

	// The public dashboard cannot write anything except through demo controls.
	if c, _ := send("POST", "/dash/endpoints", `{"url":"https://evil.example"}`); c != http.StatusMethodNotAllowed && c != http.StatusNotFound {
		t.Errorf("POST to the public dashboard: %d; it must be read-only", c)
	}

	// ── demo mode: endpoints may only target the built-in sinks ──
	key := "Bearer " + seed.APIKey
	if c, out := send("POST", "/endpoints", `{"url":"https://evil.example/hook"}`, "Authorization", key); c != http.StatusBadRequest {
		t.Errorf("demo mode accepted an external URL: %d %v", c, out)
	}
	if c, _ := send("POST", "/endpoints", `{"url":"http://sinks.internal:8090/sink/extra"}`, "Authorization", key); c != http.StatusCreated {
		t.Errorf("demo mode rejected a built-in sink: %d", c)
	}

	// ── demo controls ──
	code, st := get("/demo/state")
	if code != 200 || len(st["scenarios"].([]any)) != len(demo.Scenarios) {
		t.Fatalf("state with no runtime: %d %v", code, st)
	}
	if c, _ := send("POST", "/demo/scenarios/happy_path", ""); c != http.StatusServiceUnavailable {
		t.Errorf("command with no runtime listening: %d, want 503", c)
	}

	// With a "runtime" listening, commands are published, validated and clamped.
	sub := rdb.Subscribe(ctx, demo.CommandChannel)
	defer sub.Close()
	if _, err := sub.Receive(ctx); err != nil {
		t.Fatal(err)
	}
	ch := sub.Channel()
	// A fresh rate-limit window. Delete only our own keys: Redis is shared
	// with every other package's tests in CI, so FLUSHDB would be sabotage.
	if keys, _ := rdb.Keys(ctx, "ithaca:rl:demo:*").Result(); len(keys) > 0 {
		rdb.Del(ctx, keys...)
	}
	if c, _ := send("PUT", "/demo/sinks/billing", `{"latency_ms":999999,"fail_rate":7,"hang":true}`); c != http.StatusAccepted {
		t.Fatalf("set sink: %d", c)
	}
	var cmd demo.Command
	select {
	case m := <-ch:
		_ = json.Unmarshal([]byte(m.Payload), &cmd)
	case <-time.After(3 * time.Second):
		t.Fatal("no command published")
	}
	if cmd.Behavior.LatencyMS != 3000 || cmd.Behavior.FailRate != 1 || cmd.Behavior.Hang {
		t.Errorf("unclamped demo command reached the runtime: %+v", cmd.Behavior)
	}
	if c, _ := send("POST", "/demo/scenarios/drop_tables", ""); c != http.StatusNotFound {
		t.Errorf("unknown scenario: %d, want 404", c)
	}
	if c, _ := send("PUT", "/demo/sinks/../../etc", `{}`); c == http.StatusAccepted {
		t.Error("unknown sink accepted")
	}

	// Rate limit: 5 actions per minute per IP (3 used above).
	codes := []int{}
	for range 4 {
		c, _ := send("POST", "/demo/traffic", `{"rate":10}`)
		codes = append(codes, c)
	}
	if codes[len(codes)-1] != http.StatusTooManyRequests {
		t.Errorf("rate limit never kicked in: %v", codes)
	}
}
