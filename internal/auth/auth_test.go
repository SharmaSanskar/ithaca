package auth

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/SharmaSanskar/ithaca/internal/store/gen"
)

func TestNewKeyRoundTrips(t *testing.T) {
	key, prefix, hash := NewKey()

	got, ok := SplitKey(key)
	if !ok {
		t.Fatalf("SplitKey(%q) rejected a freshly generated key", key)
	}
	if got != prefix {
		t.Errorf("prefix = %q, want %q", got, prefix)
	}
	if !Verify(key, hash[:]) {
		t.Error("Verify rejected the key's own hash")
	}
	if Verify(key+"x", hash[:]) {
		t.Error("Verify accepted a different key")
	}
}

func TestNewKeyIsUnique(t *testing.T) {
	seen := map[string]bool{}
	for range 1000 {
		k, p, _ := NewKey()
		if seen[k] || seen[p] {
			t.Fatal("NewKey produced a duplicate key or prefix")
		}
		seen[k], seen[p] = true, true
	}
}

func TestSplitKeyRejectsMalformed(t *testing.T) {
	good, _, _ := NewKey()
	cases := map[string]string{
		"empty":         "",
		"wrong scheme":  "abc" + good[3:],
		"truncated":     good[:len(good)-1],
		"too long":      good + "A",
		"non-hex id":    good[:4] + "zzzzzzzzzzzz" + good[16:],
		"bad separator": good[:16] + "-" + good[17:],
		"bad secret":    good[:17] + strings.Repeat("!", 43),
	}
	for name, k := range cases {
		t.Run(name, func(t *testing.T) {
			if _, ok := SplitKey(k); ok {
				t.Errorf("SplitKey(%q) accepted a malformed key", k)
			}
		})
	}
}

// fakeKeys counts lookups so the tests can assert on caching.
type fakeKeys struct {
	rows  map[string]gen.GetAPIKeyByPrefixRow
	err   error
	calls atomic.Int32
}

func (f *fakeKeys) GetAPIKeyByPrefix(_ context.Context, prefix string) (gen.GetAPIKeyByPrefixRow, error) {
	f.calls.Add(1)
	if f.err != nil {
		return gen.GetAPIKeyByPrefixRow{}, f.err
	}
	r, ok := f.rows[prefix]
	if !ok {
		return r, pgx.ErrNoRows
	}
	return r, nil
}

func setup(t *testing.T) (key string, tenant uuid.UUID, keys *fakeKeys) {
	t.Helper()
	key, prefix, hash := NewKey()
	tenant = uuid.Must(uuid.NewV7())
	keys = &fakeKeys{rows: map[string]gen.GetAPIKeyByPrefixRow{
		prefix: {TenantID: tenant, KeyHash: hash[:], TenantState: "active"},
	}}
	return key, tenant, keys
}

func TestAuthenticateValidKeyIsCached(t *testing.T) {
	key, tenant, keys := setup(t)
	a := NewAuthenticator(keys, time.Minute)

	for range 5 {
		got, err := a.Authenticate(context.Background(), key)
		if err != nil {
			t.Fatalf("Authenticate error = %v", err)
		}
		if got != tenant {
			t.Fatalf("tenant = %s, want %s", got, tenant)
		}
	}
	if n := keys.calls.Load(); n != 1 {
		t.Errorf("database lookups = %d, want 1 (cache hit after the first)", n)
	}
}

func TestAuthenticateCacheExpires(t *testing.T) {
	key, _, keys := setup(t)
	a := NewAuthenticator(keys, time.Second)
	now := time.Now()
	a.now = func() time.Time { return now }

	_, _ = a.Authenticate(context.Background(), key)
	now = now.Add(2 * time.Second)
	_, _ = a.Authenticate(context.Background(), key)

	if n := keys.calls.Load(); n != 2 {
		t.Errorf("database lookups = %d, want 2 (entry expired)", n)
	}
}

func TestAuthenticateRejections(t *testing.T) {
	key, prefix, hash := NewKey()
	revoked := time.Now()
	cases := map[string]gen.GetAPIKeyByPrefixRow{
		"revoked key":      {KeyHash: hash[:], TenantState: "active", RevokedAt: &revoked},
		"suspended tenant": {KeyHash: hash[:], TenantState: "suspended"},
		"hash mismatch":    {KeyHash: make([]byte, 32), TenantState: "active"},
	}
	for name, row := range cases {
		t.Run(name, func(t *testing.T) {
			keys := &fakeKeys{rows: map[string]gen.GetAPIKeyByPrefixRow{prefix: row}}
			_, err := NewAuthenticator(keys, time.Minute).Authenticate(context.Background(), key)
			if !errors.Is(err, ErrInvalidKey) {
				t.Errorf("error = %v, want ErrInvalidKey", err)
			}
		})
	}
}

func TestAuthenticateNegativeCaching(t *testing.T) {
	keys := &fakeKeys{rows: map[string]gen.GetAPIKeyByPrefixRow{}}
	a := NewAuthenticator(keys, time.Minute)
	unknown, _, _ := NewKey()
	for range 10 {
		_, _ = a.Authenticate(context.Background(), unknown)
	}
	if n := keys.calls.Load(); n != 1 {
		t.Errorf("database lookups = %d, want 1: misses must be cached too", n)
	}
}

func TestAuthenticateDatabaseErrorIsNotInvalidKey(t *testing.T) {
	key, _, keys := setup(t)
	keys.err = errors.New("connection refused")
	a := NewAuthenticator(keys, time.Minute)

	_, err := a.Authenticate(context.Background(), key)
	if errors.Is(err, ErrInvalidKey) {
		t.Fatal("a database failure must not be reported as a bad key")
	}

	// And it must not be cached: once the database is back, the key works.
	keys.err = nil
	if _, err := a.Authenticate(context.Background(), key); err != nil {
		t.Errorf("after recovery: error = %v", err)
	}
}

func TestMiddleware(t *testing.T) {
	key, tenant, keys := setup(t)
	a := NewAuthenticator(keys, time.Minute)

	var seen uuid.UUID
	h := a.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = TenantFrom(r.Context())
	}))

	cases := []struct {
		name   string
		header string
		want   int
	}{
		{"valid", "Bearer " + key, http.StatusOK},
		{"lowercase scheme", "bearer " + key, http.StatusOK},
		{"missing", "", http.StatusUnauthorized},
		{"wrong scheme", "Basic " + key, http.StatusUnauthorized},
		{"garbage", "Bearer nope", http.StatusUnauthorized},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			seen = uuid.Nil
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			if tc.header != "" {
				req.Header.Set("Authorization", tc.header)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != tc.want {
				t.Fatalf("status = %d, want %d", rec.Code, tc.want)
			}
			if tc.want == http.StatusOK && seen != tenant {
				t.Errorf("handler saw tenant %s, want %s", seen, tenant)
			}
			if tc.want == http.StatusUnauthorized && rec.Header().Get("WWW-Authenticate") == "" {
				t.Error("401 without a WWW-Authenticate header")
			}
		})
	}
}
