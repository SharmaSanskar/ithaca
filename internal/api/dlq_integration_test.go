//go:build integration

package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/SharmaSanskar/ithaca/internal/api"
	"github.com/SharmaSanskar/ithaca/internal/auth"
	"github.com/SharmaSanskar/ithaca/internal/store"
	"github.com/SharmaSanskar/ithaca/internal/testdb"
)

// seedDeadLetters creates n dead letters for a tenant directly in SQL (the
// delivery engine's own tests cover how they get there).
func seedDeadLetters(t *testing.T, s *store.Store, tenant uuid.UUID, n int) uuid.UUID {
	t.Helper()
	ctx := context.Background()
	ep := uuid.Must(uuid.NewV7())
	sub := uuid.Must(uuid.NewV7())
	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := s.Pool.Exec(ctx, q, args...); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	exec(`INSERT INTO endpoints (id, tenant_id, url, state, signing_key_id, signing_secret)
	      VALUES ($1, $2, 'https://example.com', 'enabled', 'k', '\x00')`, ep, tenant)
	exec(`INSERT INTO subscriptions (id, tenant_id, endpoint_id, event_types) VALUES ($1, $2, $3, '{*}')`, sub, tenant, ep)
	exec(`INSERT INTO delivery_streams (tenant_id, subscription_id, endpoint_id, stream_key, ordered, state, next_seq)
	      VALUES ($1, $2, $3, 'lane:0', false, 'idle', $4)`, tenant, sub, ep, n+1)
	for i := range n {
		ev := uuid.Must(uuid.NewV7())
		exec(`INSERT INTO events (id, tenant_id, event_type, payload) VALUES ($1, $2, 'order.created', '{}')`, ev, tenant)
		exec(`INSERT INTO deliveries_history (id, tenant_id, stream_id, subscription_id, endpoint_id, event_id, seq,
		        attempt, created_at, final_state, dead_reason, completed_at)
		      SELECT $1, $2, id, $3, $4, $5, $6::bigint, 12, now(), 'dead', 'max_attempts:http_5xx', now() - make_interval(secs => $6::int)
		      FROM delivery_streams WHERE subscription_id = $3`,
			uuid.Must(uuid.NewV7()), tenant, sub, ep, ev, i+1)
	}
	return ep
}

func TestDLQBrowseReplayAndVerifyOverHTTP(t *testing.T) {
	s := testdb.New(t)
	ctx := context.Background()
	a, _ := auth.ProvisionTenant(ctx, s, "a")
	b, _ := auth.ProvisionTenant(ctx, s, "b")
	seedDeadLetters(t, s, a.TenantID, 7)

	r := chi.NewRouter()
	r.Route("/api/v1", func(r chi.Router) {
		r.Use(auth.NewAuthenticator(s, time.Minute).Middleware)
		api.NewControlPlane(s, 1000).Routes(r)
	})
	srv := httptest.NewServer(r)
	defer srv.Close()

	call := func(key, method, path, body string) (int, map[string]any) {
		t.Helper()
		req, _ := http.NewRequest(method, srv.URL+"/api/v1"+path, strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var buf bytes.Buffer
		_, _ = buf.ReadFrom(resp.Body)
		out := map[string]any{}
		_ = json.Unmarshal(buf.Bytes(), &out)
		return resp.StatusCode, out
	}

	// Keyset pagination: 3 + 3 + 1, newest first, no overlap.
	seen := map[string]bool{}
	path, pages := "/dlq?limit=3", 0
	for {
		code, page := call(a.APIKey, "GET", path, "")
		if code != 200 {
			t.Fatalf("list: %d %v", code, page)
		}
		pages++
		for _, d := range page["data"].([]any) {
			id := d.(map[string]any)["id"].(string)
			if seen[id] {
				t.Fatalf("dead letter %s appeared on two pages", id)
			}
			seen[id] = true
		}
		next, ok := page["next_cursor"].(string)
		if !ok {
			break
		}
		path = "/dlq?limit=3&cursor=" + next
	}
	if len(seen) != 7 || pages != 3 {
		t.Errorf("paged through %d dead letters in %d pages, want 7 in 3", len(seen), pages)
	}

	// Tenant isolation: B sees none of A's dead letters and cannot replay them.
	if _, page := call(b.APIKey, "GET", "/dlq", ""); len(page["data"].([]any)) != 0 {
		t.Error("tenant B can see tenant A's dead letters")
	}
	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, `"`+id+`"`)
	}
	body := `{"ids":[` + strings.Join(ids, ",") + `]}`
	if _, res := call(b.APIKey, "POST", "/dlq/replay", body); res["replayed"] != float64(0) {
		t.Errorf("tenant B replayed tenant A's dead letters: %v", res)
	}

	// Replay, then the double-click.
	if code, res := call(a.APIKey, "POST", "/dlq/replay", body); code != 200 || res["replayed"] != float64(7) {
		t.Fatalf("replay: %d %v", code, res)
	}
	if _, res := call(a.APIKey, "POST", "/dlq/replay", body); res["replayed"] != float64(0) {
		t.Errorf("second click replayed %v more", res["replayed"])
	}
	if c, _ := call(a.APIKey, "POST", "/dlq/replay", `{}`); c != http.StatusBadRequest {
		t.Errorf("replay with no selector: %d, want 400", c)
	}
	if _, res := call(a.APIKey, "POST", "/dlq/replay", `{"all":true}`); res["replayed"] != float64(0) {
		t.Errorf("replay all after everything was replayed: %v, want 0", res)
	}
	if code, _ := call(a.APIKey, "GET", "/dlq?cursor=garbage", ""); code != http.StatusBadRequest {
		t.Errorf("garbage cursor: %d, want 400", code)
	}

	// New endpoints start unverified; /verify restarts the handshake.
	code, ep := call(a.APIKey, "POST", "/endpoints", `{"url":"https://example.com/hook"}`)
	if code != 201 || ep["state"] != "pending_verification" {
		t.Fatalf("new endpoint: %d state=%v, want pending_verification", code, ep["state"])
	}
	epID := ep["id"].(string)
	if _, err := s.Pool.Exec(ctx, "UPDATE endpoints SET state='auto_disabled' WHERE id=$1", epID); err != nil {
		t.Fatal(err)
	}
	if code, ep := call(a.APIKey, "POST", "/endpoints/"+epID+"/verify", ""); code != 202 || ep["state"] != "pending_verification" {
		t.Errorf("re-verify: %d state=%v", code, ep["state"])
	}
	if code, _ := call(b.APIKey, "POST", "/endpoints/"+epID+"/verify", ""); code != http.StatusConflict {
		t.Errorf("tenant B re-verifying A's endpoint: %d, want 409", code)
	}
}
