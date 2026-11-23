package auth

import (
	"context"
	"crypto/sha256"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/SharmaSanskar/ithaca/internal/platform/httpx"
	"github.com/SharmaSanskar/ithaca/internal/platform/logging"
	"github.com/SharmaSanskar/ithaca/internal/store"
	"github.com/SharmaSanskar/ithaca/internal/store/gen"
)

// KeyLookup is the one query the authenticator needs; *store.Store satisfies it.
type KeyLookup interface {
	GetAPIKeyByPrefix(ctx context.Context, prefix string) (gen.GetAPIKeyByPrefixRow, error)
}

// Authenticator resolves an API key to a tenant, with a short in-process cache.
//
// The cache turns ingest auth into a map lookup on the hot path. The cost is
// that revocation takes up to TTL to propagate — a deliberate, documented
// trade. Negative results are cached too, so a client hammering with a bad key
// cannot turn every request into a database query.
type Authenticator struct {
	keys KeyLookup
	ttl  time.Duration
	now  func() time.Time

	mu    sync.Mutex
	cache map[string]cacheEntry // keyed by the full SHA-256 of the presented key
}

type cacheEntry struct {
	tenant  uuid.UUID
	ok      bool
	expires time.Time
}

// maxCacheEntries bounds memory against a flood of distinct bogus keys.
const maxCacheEntries = 10_000

// NewAuthenticator builds an authenticator with the given cache TTL.
func NewAuthenticator(keys KeyLookup, ttl time.Duration) *Authenticator {
	return &Authenticator{keys: keys, ttl: ttl, now: time.Now, cache: make(map[string]cacheEntry)}
}

// ErrInvalidKey is returned for any authentication failure. Deliberately one
// error for every cause (unknown, revoked, malformed, suspended tenant): a
// distinguishable error is an oracle for key enumeration.
var ErrInvalidKey = httpx.ErrUnauthorized.WithMessage("Invalid or missing API key.")

// Authenticate resolves a presented key to its tenant.
func (a *Authenticator) Authenticate(ctx context.Context, key string) (uuid.UUID, error) {
	prefix, ok := SplitKey(key)
	if !ok {
		return uuid.Nil, ErrInvalidKey
	}

	sum := sha256.Sum256([]byte(key))
	cacheKey := string(sum[:])
	now := a.now()

	a.mu.Lock()
	if e, hit := a.cache[cacheKey]; hit && now.Before(e.expires) {
		a.mu.Unlock()
		if !e.ok {
			return uuid.Nil, ErrInvalidKey
		}
		return e.tenant, nil
	}
	a.mu.Unlock()

	row, err := a.keys.GetAPIKeyByPrefix(ctx, prefix)
	var tenant uuid.UUID
	valid := false
	switch {
	case err == nil:
		valid = row.RevokedAt == nil && row.TenantState == "active" && Verify(key, row.KeyHash)
		tenant = row.TenantID
	case store.IsNoRows(err):
		// fall through: cache the miss
	default:
		// A database error is not "invalid key". Do not cache it, and do not
		// tell the client their key is wrong when it might be fine.
		return uuid.Nil, httpx.ErrUnavailable.WithCause(err)
	}

	a.mu.Lock()
	if len(a.cache) >= maxCacheEntries {
		clear(a.cache) // crude but bounded; a real deployment would use an LRU
	}
	a.cache[cacheKey] = cacheEntry{tenant: tenant, ok: valid, expires: now.Add(a.ttl)}
	a.mu.Unlock()

	if !valid {
		return uuid.Nil, ErrInvalidKey
	}
	return tenant, nil
}

type tenantKey struct{}

// TenantFrom returns the authenticated tenant. It panics if the auth
// middleware did not run, because that is a wiring bug, not a runtime state.
func TenantFrom(ctx context.Context) uuid.UUID {
	t, ok := ctx.Value(tenantKey{}).(uuid.UUID)
	if !ok {
		panic("auth: TenantFrom called on a route without auth.Middleware")
	}
	return t
}

// WithTenant stores a tenant in the context (used by tests and the middleware).
func WithTenant(ctx context.Context, tenant uuid.UUID) context.Context {
	return context.WithValue(ctx, tenantKey{}, tenant)
}

// Middleware authenticates `Authorization: Bearer <key>`.
func (a *Authenticator) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key, ok := bearer(r.Header.Get("Authorization"))
		if !ok {
			w.Header().Set("WWW-Authenticate", `Bearer realm="ithaca"`)
			httpx.WriteError(w, r, ErrInvalidKey)
			return
		}
		tenant, err := a.Authenticate(r.Context(), key)
		if err != nil {
			if err == ErrInvalidKey { //nolint:errorlint // sentinel identity is intended
				w.Header().Set("WWW-Authenticate", `Bearer realm="ithaca", error="invalid_token"`)
			}
			httpx.WriteError(w, r, err)
			return
		}
		ctx := WithTenant(r.Context(), tenant)
		ctx = logging.With(ctx, slog.String("tenant_id", tenant.String()))
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func bearer(h string) (string, bool) {
	const p = "Bearer "
	if len(h) <= len(p) || !strings.EqualFold(h[:len(p)], p) {
		return "", false
	}
	return strings.TrimSpace(h[len(p):]), true
}
