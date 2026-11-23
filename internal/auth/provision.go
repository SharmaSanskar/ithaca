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
