package auth

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/SharmaSanskar/ithaca/internal/store"
	"github.com/SharmaSanskar/ithaca/internal/store/gen"
)

// Provisioned is a new tenant and its first API key. The key is only ever
// available here; afterwards only its hash exists.
type Provisioned struct {
	TenantID uuid.UUID
	APIKey   string
}

// ProvisionTenant creates a tenant and an API key in one transaction.
func ProvisionTenant(ctx context.Context, s *store.Store, name string) (Provisioned, error) {
	var out Provisioned
	err := s.InTx(ctx, func(q *gen.Queries) error {
		t, err := q.CreateTenant(ctx, gen.CreateTenantParams{ID: uuid.Must(uuid.NewV7()), Name: name})
		if err != nil {
			return fmt.Errorf("create tenant: %w", err)
		}
		key, prefix, hash := NewKey()
		if _, err := q.CreateAPIKey(ctx, gen.CreateAPIKeyParams{
			ID: uuid.Must(uuid.NewV7()), TenantID: t.ID, KeyPrefix: prefix, KeyHash: hash[:], Name: "default",
		}); err != nil {
			return fmt.Errorf("create api key: %w", err)
		}
		out = Provisioned{TenantID: t.ID, APIKey: key}
		return nil
	})
	return out, err
}

// IssueKey creates an additional API key for an existing tenant, looked up by
// name. Keys are independent: issuing one never invalidates another, which is
// what lets a client rotate (issue new, deploy it, then revoke old) with no
// window in which neither works. Like ProvisionTenant, the plaintext key
// exists only in the return value.
func IssueKey(ctx context.Context, s *store.Store, tenantName, keyName string) (Provisioned, error) {
	t, err := s.GetTenantByName(ctx, tenantName)
	if err != nil {
		return Provisioned{}, fmt.Errorf("tenant %q: %w", tenantName, err)
	}
	key, prefix, hash := NewKey()
	if _, err := s.CreateAPIKey(ctx, gen.CreateAPIKeyParams{
		ID: uuid.Must(uuid.NewV7()), TenantID: t.ID, KeyPrefix: prefix, KeyHash: hash[:], Name: keyName,
	}); err != nil {
		return Provisioned{}, fmt.Errorf("create api key: %w", err)
	}
	return Provisioned{TenantID: t.ID, APIKey: key}, nil
}
