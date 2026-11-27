package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/SharmaSanskar/ithaca/internal/auth"
	"github.com/SharmaSanskar/ithaca/internal/demo"
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
	if len(args) < 2 || (args[0] != "create" && args[0] != "key") {
		return errors.New("usage: ithaca tenant create <name> | ithaca tenant key <name>")
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

	provision := auth.ProvisionTenant
	if args[0] == "key" {
		provision = func(ctx context.Context, s *store.Store, name string) (auth.Provisioned, error) {
			return auth.IssueKey(ctx, s, name, "cli-"+time.Now().UTC().Format("20060102-150405"))
		}
	}
	p, err := provision(ctx, s, name)
	if err != nil {
		return err
	}
	fmt.Printf("tenant_id=%s\napi_key=%s\n", p.TenantID, p.APIKey)
	fmt.Fprintln(os.Stderr, "\nStore the API key now; it cannot be shown again.")
	return nil
}

// runDemo: `ithaca demo seed` — create the demo tenant and its sink endpoints
// (idempotent). The endpoints verify themselves within seconds once the
// dispatcher and receiver-sim are running.
func runDemo(args []string) error {
	if len(args) < 1 || args[0] != "seed" {
		return errors.New("usage: ithaca demo seed")
	}
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
	res, err := demo.Seed(ctx, s, cfg.Demo.SinkBaseURL)
	if err != nil {
		return err
	}
	if !res.Created {
		fmt.Printf("demo tenant already exists: %s\n", res.TenantID)
		return nil
	}
	fmt.Printf("tenant_id=%s\napi_key=%s\n", res.TenantID, res.APIKey)
	fmt.Fprintf(os.Stderr, "\nSeeded %d endpoints pointing at %s/sink/<id>.\n", len(demo.Sinks), cfg.Demo.SinkBaseURL)
	return nil
}
