// Package demo is the self-running demo behind the public dashboard: a seeded
// tenant, ambient traffic, controllable sinks, and five server-driven
// scenarios narrated in plain English.
//
// Nothing here is simulated. The demo drives the real system — events go
// through the real ingest transaction, deliveries go through the real
// dispatcher to real HTTP sinks (receiver-sim) — and the dashboard only
// observes. A scenario is a script of actions on the SINKS and the TRAFFIC,
// plus waits on conditions read from Postgres.
package demo

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"github.com/SharmaSanskar/ithaca/internal/auth"
	"github.com/SharmaSanskar/ithaca/internal/store"
	"github.com/SharmaSanskar/ithaca/internal/store/gen"
)

// TenantName is the seeded demo tenant.
const TenantName = "demo"

// Sink is one demo endpoint, served by receiver-sim at /sink/<ID>.
type Sink struct {
	ID       string   // receiver-sim sink id
	Name     string   // shown on the dashboard
	Patterns []string // subscription event-type filter
	Ordered  bool
}

// Sinks is the demo topology. analytics takes everything (fanout), orders is
// ORDERED per order id (so pausing and per-key ordering are visible).
var Sinks = []Sink{
	{ID: "orders", Name: "orders-service", Patterns: []string{"order.*"}, Ordered: true},
	{ID: "billing", Name: "billing", Patterns: []string{"invoice.*", "payment.*"}},
	{ID: "crm", Name: "crm", Patterns: []string{"user.*"}},
	{ID: "analytics", Name: "analytics", Patterns: []string{"*"}},
}

// SeedResult describes the demo tenant.
type SeedResult struct {
	TenantID uuid.UUID
	APIKey   string // only set when the tenant was created now
	Created  bool
}

// Seed creates the demo tenant and its endpoints if they do not exist. It is
// idempotent. Endpoints start pending_verification like any other: the
// verifier challenges them and receiver-sim echoes, which takes a couple of
// seconds and exercises the real handshake.
func Seed(ctx context.Context, s *store.Store, sinkBaseURL string) (SeedResult, error) {
	t, err := s.GetTenantByName(ctx, TenantName)
	if err == nil {
		return SeedResult{TenantID: t.ID}, nil
	}
	if !store.IsNoRows(err) {
		return SeedResult{}, err
	}
	p, err := auth.ProvisionTenant(ctx, s, TenantName)
	if err != nil {
		return SeedResult{}, err
	}
	for _, sk := range Sinks {
		secret := make([]byte, 32)
		_, _ = rand.Read(secret)
		ep, err := s.CreateEndpoint(ctx, gen.CreateEndpointParams{
			ID: uuid.Must(uuid.NewV7()), TenantID: p.TenantID, Url: sinkBaseURL + "/sink/" + sk.ID,
			Description: sk.Name, State: "pending_verification", TimeoutMs: 5000, MaxConcurrency: 8,
			SigningKeyID: "k_demo_" + sk.ID, SigningSecret: secret,
		})
		if err != nil {
			return SeedResult{}, fmt.Errorf("create endpoint %s: %w", sk.ID, err)
		}
		if _, err := s.CreateSubscription(ctx, gen.CreateSubscriptionParams{
			ID: uuid.Must(uuid.NewV7()), TenantID: p.TenantID, EndpointID: ep.ID,
			EventTypes: sk.Patterns, Ordered: sk.Ordered, LaneCount: 16,
		}); err != nil {
			return SeedResult{}, fmt.Errorf("create subscription %s: %w", sk.ID, err)
		}
	}
	return SeedResult{TenantID: p.TenantID, APIKey: p.APIKey, Created: true}, nil
}

// ErrNotSeeded means the demo tenant does not exist yet.
var ErrNotSeeded = errors.New("demo tenant not seeded: run `ithaca demo seed`")

// TenantID looks the demo tenant up.
func TenantID(ctx context.Context, s *store.Store) (uuid.UUID, error) {
	t, err := s.GetTenantByName(ctx, TenantName)
	if store.IsNoRows(err) {
		return uuid.Nil, ErrNotSeeded
	}
	return t.ID, err
}
