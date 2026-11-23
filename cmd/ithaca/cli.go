package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/SharmaSanskar/ithaca/internal/auth"
	"github.com/SharmaSanskar/ithaca/internal/platform/config"
	"github.com/SharmaSanskar/ithaca/internal/platform/logging"
	"github.com/SharmaSanskar/ithaca/internal/store"
)

// runMigrate: `ithaca migrate [up|status]`.
//
// Migrations are an explicit step, never run implicitly on service boot. Six
// replicas racing to migrate on a rolling deploy is how a schema change takes
// a lock in the middle of peak traffic. In Kubernetes this is a Job that runs
// before the Deployments roll.
func runMigrate(args []string) error {
	cfg, err := config.Load(config.ServiceCLI)
	if err != nil {
		return err
	}
	if cfg.Postgres.URL == "" {
		return errors.New("ITHACA_DATABASE_URL is required")
	}
	log := logging.New(logging.Options{Level: cfg.Log.Level, Format: cfg.Log.Format, Service: "migrate"})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	sub := "up"
	if len(args) > 0 {
		sub = args[0]
	}
	switch sub {
	case "up":
		return store.Migrate(ctx, cfg.Postgres.URL, log)
	case "status":
		return store.MigrationStatus(ctx, cfg.Postgres.URL, os.Stdout)
	default:
		return fmt.Errorf("unknown migrate command %q (want up or status)", sub)
	}
}

// runTenant: `ithaca tenant create <name>` — prints the tenant id and its API
// key. The key is shown exactly once; only its SHA-256 is stored.
func runTenant(args []string) error {
	if len(args) < 2 || args[0] != "create" {
		return errors.New("usage: ithaca tenant create <name>")
	}
	name := strings.Join(args[1:], " ")

	cfg, err := config.Load(config.ServiceCLI)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s, err := store.Open(ctx, cfg.Postgres, nil)
	if err != nil {
		return err
	}
	defer s.Close()

	p, err := auth.ProvisionTenant(ctx, s, name)
	if err != nil {
		return err
	}
	fmt.Printf("tenant_id=%s\napi_key=%s\n", p.TenantID, p.APIKey)
	fmt.Fprintln(os.Stderr, "\nStore the API key now; it cannot be shown again.")
	return nil
}
