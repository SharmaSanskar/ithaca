// Package api is the control plane: endpoints and subscriptions now, and the
// dashboard's read API in Phase 4.
//
// It is deliberately thin. Every interesting behavior in Ithaca lives in the
// delivery engine; this package validates input and calls sqlc queries.
package api

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/SharmaSanskar/ithaca/internal/auth"
	"github.com/SharmaSanskar/ithaca/internal/dlq"
	"github.com/SharmaSanskar/ithaca/internal/ingest"
	"github.com/SharmaSanskar/ithaca/internal/platform/httpx"
	"github.com/SharmaSanskar/ithaca/internal/store"
	"github.com/SharmaSanskar/ithaca/internal/store/gen"
)

// ControlPlane serves /api/v1/{endpoints,subscriptions,dlq}.
type ControlPlane struct {
	store    *store.Store
	replayer *dlq.Replayer
	// urlPrefix, when set, is the only URL prefix endpoints may use.
	urlPrefix string
}

// RestrictURLs limits new endpoints to URLs under prefix (the public demo's
// built-in sinks).
func (c *ControlPlane) RestrictURLs(prefix string) { c.urlPrefix = prefix }

// NewControlPlane builds the handler set. replayRate is replayed deliveries
// per second per endpoint.
func NewControlPlane(s *store.Store, replayRate float64) *ControlPlane {
	return &ControlPlane{store: s, replayer: &dlq.Replayer{Store: s, RatePerEndpoint: replayRate}}
}

// Routes mounts the control-plane routes. The caller supplies auth.
func (c *ControlPlane) Routes(r chi.Router) {
	r.Route("/endpoints", func(r chi.Router) {
		r.Post("/", c.createEndpoint)
		r.Get("/", c.listEndpoints)
		r.Get("/{id}", c.getEndpoint)
		r.Delete("/{id}", c.disableEndpoint)
		r.Post("/{id}/verify", c.verifyEndpoint)
	})
	r.Route("/subscriptions", func(r chi.Router) {
		r.Post("/", c.createSubscription)
		r.Get("/", c.listSubscriptions)
		r.Delete("/{id}", c.disableSubscription)
	})
	c.DLQRoutes(r)
}

// ── endpoints ───────────────────────────────────────────────────────────────

type endpointJSON struct {
	ID             uuid.UUID `json:"id"`
	URL            string    `json:"url"`
	Description    string    `json:"description"`
	State          string    `json:"state"`
	DisabledReason *string   `json:"disabled_reason,omitempty"`
	TimeoutMS      int32     `json:"timeout_ms"`
	MaxConcurrency int32     `json:"max_concurrency"`
	SigningKeyID   string    `json:"signing_key_id"`
	// Verification handshake and circuit breaker.
	VerifiedAt            *time.Time `json:"verified_at,omitempty"`
	LastVerificationError *string    `json:"last_verification_error,omitempty"`
	Circuit               string     `json:"circuit"`
	ConsecutiveFailures   int32      `json:"consecutive_failures"`
	CircuitNextProbeAt    *time.Time `json:"circuit_next_probe_at,omitempty"`
	// Only present in the create response: the secret is shown exactly once.
	SigningSecret string    `json:"signing_secret,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
}

func toEndpointJSON(e gen.Endpoint) endpointJSON {
	return endpointJSON{
		ID: e.ID, URL: e.Url, Description: e.Description, State: e.State,
		DisabledReason: e.DisabledReason, TimeoutMS: e.TimeoutMs, MaxConcurrency: e.MaxConcurrency,
		SigningKeyID: e.SigningKeyID, CreatedAt: e.CreatedAt,
		VerifiedAt: e.VerifiedAt, LastVerificationError: e.LastVerificationError,
		Circuit: e.CircuitState, ConsecutiveFailures: e.ConsecutiveFailures, CircuitNextProbeAt: e.CircuitNextProbeAt,
	}
}

type createEndpointReq struct {
	URL            string `json:"url"`
	Description    string `json:"description"`
	TimeoutMS      *int32 `json:"timeout_ms"`
	MaxConcurrency *int32 `json:"max_concurrency"`
}

func (c *ControlPlane) createEndpoint(w http.ResponseWriter, r *http.Request) {
	var req createEndpointReq
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, r, err)
		return
	}

	details := map[string]string{}
	if msg := validateEndpointURL(req.URL); msg != "" {
		details["url"] = msg
	} else if c.urlPrefix != "" && !strings.HasPrefix(req.URL, c.urlPrefix) {
		details["url"] = "this is a public demo: endpoints must point at a built-in sink (" + c.urlPrefix + "<id>)"
	}
	if len(req.Description) > 500 {
		details["description"] = "must be at most 500 characters"
	}
	timeout := int32(15000)
	if req.TimeoutMS != nil {
		timeout = *req.TimeoutMS
		if timeout < 1000 || timeout > 60000 {
			details["timeout_ms"] = "must be between 1000 and 60000"
		}
	}
	maxConc := int32(8)
	if req.MaxConcurrency != nil {
		maxConc = *req.MaxConcurrency
		if maxConc < 1 || maxConc > 256 {
			details["max_concurrency"] = "must be between 1 and 256"
		}
	}
	if len(details) > 0 {
		httpx.WriteError(w, r, httpx.ErrBadRequest.WithMessage("The endpoint is invalid.").WithDetails(details))
		return
	}

	secret := make([]byte, 32)
	_, _ = rand.Read(secret)
	kid := make([]byte, 4)
	_, _ = rand.Read(kid)

	e, err := c.store.CreateEndpoint(r.Context(), gen.CreateEndpointParams{
		ID:          uuid.Must(uuid.NewV7()),
		TenantID:    auth.TenantFrom(r.Context()),
		Url:         req.URL,
		Description: req.Description,
		// Nothing is delivered until the endpoint echoes a signed challenge;
		// see internal/verify.
		State:          "pending_verification",
		TimeoutMs:      timeout,
		MaxConcurrency: maxConc,
		SigningKeyID:   "k_" + hex.EncodeToString(kid),
		SigningSecret:  secret,
	})
	if err != nil {
		httpx.WriteError(w, r, err)
		return
	}
	out := toEndpointJSON(e)
	out.SigningSecret = "whsec_" + base64.StdEncoding.EncodeToString(secret)
	httpx.WriteJSON(w, http.StatusCreated, out)
}

func (c *ControlPlane) listEndpoints(w http.ResponseWriter, r *http.Request) {
	rows, err := c.store.ListEndpoints(r.Context(), auth.TenantFrom(r.Context()))
	if err != nil {
		httpx.WriteError(w, r, err)
		return
	}
	out := make([]endpointJSON, 0, len(rows))
	for _, e := range rows {
		out = append(out, toEndpointJSON(e))
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"data": out})
}

func (c *ControlPlane) getEndpoint(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	e, err := c.store.GetEndpoint(r.Context(), gen.GetEndpointParams{TenantID: auth.TenantFrom(r.Context()), ID: id})
	if store.IsNoRows(err) {
		httpx.WriteError(w, r, httpx.ErrNotFound)
		return
	}
	if err != nil {
		httpx.WriteError(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, toEndpointJSON(e))
}

// disableEndpoint is a soft delete. Pending deliveries stay in the queue and
// are not fanned out to again; history and attempts remain queryable.
func (c *ControlPlane) disableEndpoint(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	reason := "disabled by user"
	e, err := c.store.SetEndpointState(r.Context(), gen.SetEndpointStateParams{
		TenantID: auth.TenantFrom(r.Context()), ID: id, State: "disabled", DisabledReason: &reason,
	})
	if store.IsNoRows(err) {
		httpx.WriteError(w, r, httpx.ErrNotFound)
		return
	}
	if err != nil {
		httpx.WriteError(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, toEndpointJSON(e))
}

// verifyEndpoint (re)starts the verification handshake: after a failed one, or
// to re-enable a disabled endpoint. Delivery resumes once it passes.
func (c *ControlPlane) verifyEndpoint(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	e, err := c.store.RequestVerification(r.Context(), gen.RequestVerificationParams{
		TenantID: auth.TenantFrom(r.Context()), ID: id,
	})
	if store.IsNoRows(err) {
		httpx.WriteError(w, r, httpx.ErrConflict.WithMessage("Endpoint not found, or already verified and enabled."))
		return
	}
	if err != nil {
		httpx.WriteError(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusAccepted, toEndpointJSON(e))
}

// validateEndpointURL is a SYNTAX check only. It deliberately does not resolve
// the host: an address checked at registration can resolve somewhere else at
// delivery time (DNS rebinding). The real SSRF defense runs inside the
// delivery dialer, after resolution and before connect (Phase 2).
func validateEndpointURL(raw string) string {
	if raw == "" {
		return "is required"
	}
	if len(raw) > 2048 {
		return "must be at most 2048 characters"
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "is not a valid URL"
	}
	switch {
	case u.Scheme != "https" && u.Scheme != "http":
		return "scheme must be http or https"
	case u.Host == "" || u.Hostname() == "":
		return "must include a host"
	case u.User != nil:
		return "must not contain credentials"
	case u.Fragment != "":
		return "must not contain a fragment"
	}
	return ""
}

// ── subscriptions ───────────────────────────────────────────────────────────

type subscriptionJSON struct {
	ID         uuid.UUID `json:"id"`
	EndpointID uuid.UUID `json:"endpoint_id"`
	EventTypes []string  `json:"event_types"`
	State      string    `json:"state"`
	Ordered    bool      `json:"ordered"`
	LaneCount  int32     `json:"lane_count"`
	CreatedAt  time.Time `json:"created_at"`
}

func toSubscriptionJSON(s gen.Subscription) subscriptionJSON {
	return subscriptionJSON{
		ID: s.ID, EndpointID: s.EndpointID, EventTypes: s.EventTypes, State: s.State,
		Ordered: s.Ordered, LaneCount: s.LaneCount, CreatedAt: s.CreatedAt,
	}
}

type createSubscriptionReq struct {
	EndpointID uuid.UUID `json:"endpoint_id"`
	EventTypes []string  `json:"event_types"`
	Ordered    bool      `json:"ordered"`
	LaneCount  *int32    `json:"lane_count"`
}

func (c *ControlPlane) createSubscription(w http.ResponseWriter, r *http.Request) {
	var req createSubscriptionReq
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, r, err)
		return
	}
	tenant := auth.TenantFrom(r.Context())

	details := map[string]string{}
	if req.EndpointID == uuid.Nil {
		details["endpoint_id"] = "is required"
	}
	if len(req.EventTypes) == 0 || len(req.EventTypes) > 64 {
		details["event_types"] = "must contain 1-64 patterns"
	}
	for _, p := range req.EventTypes {
		if !ingest.ValidPattern(p) {
			details["event_types"] = "invalid pattern " + quote(p) + `; use "*", "order.created" or "order.*"`
			break
		}
	}
	lanes := int32(16)
	if req.LaneCount != nil {
		lanes = *req.LaneCount
		if lanes < 1 || lanes > 1024 {
			details["lane_count"] = "must be between 1 and 1024"
		}
	}
	if len(details) > 0 {
		httpx.WriteError(w, r, httpx.ErrBadRequest.WithMessage("The subscription is invalid.").WithDetails(details))
		return
	}

	// Tenant-scoped existence check: without it, a tenant could subscribe to
	// another tenant's endpoint id (the FK alone does not know about tenants).
	if _, err := c.store.GetEndpoint(r.Context(), gen.GetEndpointParams{TenantID: tenant, ID: req.EndpointID}); err != nil {
		if store.IsNoRows(err) {
			httpx.WriteError(w, r, httpx.ErrUnprocessable.WithMessage("Unknown endpoint_id.").
				WithDetails(map[string]string{"endpoint_id": "no such endpoint"}))
			return
		}
		httpx.WriteError(w, r, err)
		return
	}

	s, err := c.store.CreateSubscription(r.Context(), gen.CreateSubscriptionParams{
		ID: uuid.Must(uuid.NewV7()), TenantID: tenant, EndpointID: req.EndpointID,
		EventTypes: req.EventTypes, Ordered: req.Ordered, LaneCount: lanes,
	})
	if err != nil {
		httpx.WriteError(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusCreated, toSubscriptionJSON(s))
}

func (c *ControlPlane) listSubscriptions(w http.ResponseWriter, r *http.Request) {
	rows, err := c.store.ListSubscriptions(r.Context(), auth.TenantFrom(r.Context()))
	if err != nil {
		httpx.WriteError(w, r, err)
		return
	}
	out := make([]subscriptionJSON, 0, len(rows))
	for _, s := range rows {
		out = append(out, toSubscriptionJSON(s))
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"data": out})
}

func (c *ControlPlane) disableSubscription(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	s, err := c.store.SetSubscriptionState(r.Context(), gen.SetSubscriptionStateParams{
		TenantID: auth.TenantFrom(r.Context()), ID: id, State: "disabled",
	})
	if store.IsNoRows(err) {
		httpx.WriteError(w, r, httpx.ErrNotFound)
		return
	}
	if err != nil {
		httpx.WriteError(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, toSubscriptionJSON(s))
}

// ── helpers ─────────────────────────────────────────────────────────────────

func pathID(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		// Not a 400: a malformed id names no resource, same as an unknown one.
		httpx.WriteError(w, r, httpx.ErrNotFound)
		return uuid.Nil, false
	}
	return id, true
}

func quote(s string) string {
	if len(s) > 64 {
		s = s[:64] + "…"
	}
	return `"` + s + `"`
}
