//go:build integration

package auth_test

import (
	"context"
	"testing"
	"time"

	"github.com/SharmaSanskar/ithaca/internal/auth"
	"github.com/SharmaSanskar/ithaca/internal/testdb"
)

// An issued key authenticates as the same tenant, and the existing key keeps
// working: rotation needs both valid at once.
func TestIssuedKeyWorksAlongsideTheOriginal(t *testing.T) {
	s := testdb.New(t)
	ctx := context.Background()
	p, err := auth.ProvisionTenant(ctx, s, "acme")
	if err != nil {
		t.Fatal(err)
	}
	issued, err := auth.IssueKey(ctx, s, "acme", "ci")
	if err != nil {
		t.Fatal(err)
	}
	if issued.APIKey == p.APIKey {
		t.Fatal("issued key equals the original")
	}
	a := auth.NewAuthenticator(s, time.Minute)
	for name, key := range map[string]string{"original": p.APIKey, "issued": issued.APIKey} {
		got, err := a.Authenticate(ctx, key)
		if err != nil || got != p.TenantID {
			t.Errorf("%s key: tenant %v, err %v; want %v", name, got, err, p.TenantID)
		}
	}
	if _, err := auth.IssueKey(ctx, s, "no-such-tenant", "ci"); err == nil {
		t.Error("issuing a key for a missing tenant succeeded")
	}
}
