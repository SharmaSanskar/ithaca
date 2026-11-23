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

	"github.com/SharmaSanskar/ithaca/internal/api"
	"github.com/SharmaSanskar/ithaca/internal/auth"
	"github.com/SharmaSanskar/ithaca/internal/testdb"
)

func TestControlPlaneLifecycle(t *testing.T) {
	s := testdb.New(t)
	ctx := context.Background()
	a, err := auth.ProvisionTenant(ctx, s, "a")
	if err != nil {
		t.Fatal(err)
	}
	b, err := auth.ProvisionTenant(ctx, s, "b")
	if err != nil {
		t.Fatal(err)
	}

	r := chi.NewRouter()
	r.Route("/api/v1", func(r chi.Router) {
		r.Use(auth.NewAuthenticator(s, time.Minute).Middleware)
		api.NewControlPlane(s).Routes(r)
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

	// Create: the secret is returned exactly once.
	code, ep := call(a.APIKey, "POST", "/endpoints", `{"url":"https://example.com/hook","description":"orders"}`)
	if code != http.StatusCreated {
		t.Fatalf("create endpoint: %d %v", code, ep)
	}
	if secret, _ := ep["signing_secret"].(string); !strings.HasPrefix(secret, "whsec_") {
		t.Errorf("create response missing signing_secret: %v", ep)
	}
	epID := ep["id"].(string)

	code, got := call(a.APIKey, "GET", "/endpoints/"+epID, "")
	if code != http.StatusOK {
		t.Fatalf("get endpoint: %d", code)
	}
	if _, leaked := got["signing_secret"]; leaked {
		t.Error("GET leaked the signing secret")
	}

	// Validation.
	if c, _ := call(a.APIKey, "POST", "/endpoints", `{"url":"ftp://x"}`); c != http.StatusBadRequest {
		t.Errorf("bad url: %d, want 400", c)
	}
	if c, _ := call(a.APIKey, "POST", "/subscriptions", `{"endpoint_id":"`+epID+`","event_types":["or*der"]}`); c != http.StatusBadRequest {
		t.Errorf("bad pattern: %d, want 400", c)
	}

	// Tenant isolation: B can neither see A's endpoint nor subscribe to it.
	if c, _ := call(b.APIKey, "GET", "/endpoints/"+epID, ""); c != http.StatusNotFound {
		t.Errorf("cross-tenant GET: %d, want 404", c)
	}
	if c, _ := call(b.APIKey, "POST", "/subscriptions", `{"endpoint_id":"`+epID+`","event_types":["*"]}`); c != http.StatusUnprocessableEntity {
		t.Errorf("cross-tenant subscribe: %d, want 422", c)
	}

	code, sub := call(a.APIKey, "POST", "/subscriptions", `{"endpoint_id":"`+epID+`","event_types":["order.*"],"ordered":true}`)
	if code != http.StatusCreated {
		t.Fatalf("create subscription: %d %v", code, sub)
	}
	if sub["ordered"] != true || sub["lane_count"] != float64(16) {
		t.Errorf("subscription = %v", sub)
	}

	_, list := call(a.APIKey, "GET", "/subscriptions", "")
	if n := len(list["data"].([]any)); n != 1 {
		t.Errorf("A lists %d subscriptions, want 1", n)
	}
	_, list = call(b.APIKey, "GET", "/subscriptions", "")
	if n := len(list["data"].([]any)); n != 0 {
		t.Errorf("B lists %d subscriptions, want 0", n)
	}

	code, del := call(a.APIKey, "DELETE", "/endpoints/"+epID, "")
	if code != http.StatusOK || del["state"] != "disabled" {
		t.Errorf("disable: %d %v", code, del)
	}
	if c, _ := call(a.APIKey, "DELETE", "/endpoints/not-a-uuid", ""); c != http.StatusNotFound {
		t.Errorf("malformed id: %d, want 404", c)
	}
}
