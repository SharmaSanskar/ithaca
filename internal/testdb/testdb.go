//go:build integration

// Package testdb gives every integration test its own freshly-migrated
// Postgres database in ~20ms.
//
// The template-database trick: migrations run ONCE per test binary into
// `ithaca_tmpl`. Each test then does
//
//	CREATE DATABASE t_<random> TEMPLATE ithaca_tmpl
//
// which is a file-level copy, not a replay of every migration. Tests get
// complete isolation (no shared rows, no ordering dependence, safe under
// t.Parallel) without paying migration cost per test.
//
// Real Postgres, not sqlmock: every interesting behavior in this system —
// SKIP LOCKED, ON CONFLICT row locks, unique-index waits — IS Postgres
// behavior, and a mock would only test our assumptions about it.
//
// Server selection:
//   - ITHACA_TEST_DATABASE_URL set → use that server (CI service container,
//     or `make up` locally for speed). It must allow CREATE DATABASE.
//   - otherwise → start postgres:17-alpine with testcontainers. Ryuk removes
//     the container when the test binary exits.
package testdb

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/SharmaSanskar/ithaca/internal/store"
)

const templateDB = "ithaca_tmpl"

var (
	once     sync.Once
	adminURL string // connection string to the server's maintenance database
	initErr  error

	// CREATE DATABASE ... TEMPLATE fails if another session is connected to
	// the template, and concurrent clones contend on it. Serializing clones
	// costs ~20ms per test and removes a flaky failure mode.
	cloneMu sync.Mutex
)

// New returns a Store on a brand-new, fully-migrated database that is dropped
// when the test ends.
func New(t testing.TB) *store.Store {
	t.Helper()
	pool := NewPool(t)
	return store.New(pool)
}

// NewPool is New without the Store wrapper.
func NewPool(t testing.TB) *pgxpool.Pool {
	t.Helper()
	dbURL := NewURL(t)
	pool, err := pgxpool.New(context.Background(), dbURL)
	if err != nil {
		t.Fatalf("testdb: connect: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// NewURL creates a fresh database and returns its connection string. The
// database is dropped at test cleanup.
func NewURL(t testing.TB) string {
	t.Helper()
	once.Do(func() { initErr = setup() })
	if initErr != nil {
		t.Fatalf("testdb: setup: %v", initErr)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	name := "t_" + randomHex(8)
	admin, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		t.Fatalf("testdb: connect admin: %v", err)
	}
	defer admin.Close(ctx)

	cloneMu.Lock()
	_, err = admin.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s TEMPLATE %s", name, templateDB))
	cloneMu.Unlock()
	if err != nil {
		t.Fatalf("testdb: clone template: %v", err)
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		c, err := pgx.Connect(ctx, adminURL)
		if err != nil {
			return
		}
		defer c.Close(ctx)
		// FORCE terminates connections a leaky test left open, so cleanup
		// never fails on "database is being accessed by other users".
		_, _ = c.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE)", name))
	})

	return withDatabase(adminURL, name)
}

func setup() error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	adminURL = os.Getenv("ITHACA_TEST_DATABASE_URL")
	if adminURL == "" {
		c, err := tcpostgres.Run(ctx, "postgres:17-alpine",
			tcpostgres.WithDatabase("postgres"),
			tcpostgres.WithUsername("ithaca"),
			tcpostgres.WithPassword("ithaca"),
			tcpostgres.BasicWaitStrategies(),
			// Disposable database: durability off makes the suite several
			// times faster. Never do this anywhere that matters.
			testcontainers.WithCmd("postgres",
				"-c", "fsync=off",
				"-c", "synchronous_commit=off",
				"-c", "full_page_writes=off",
				"-c", "max_connections=500",
			),
		)
		if err != nil {
			return fmt.Errorf("start postgres container: %w", err)
		}
		adminURL, err = c.ConnectionString(ctx, "sslmode=disable")
		if err != nil {
			return err
		}
	}

	admin, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer admin.Close(ctx)

	// Rebuild the template on every run so it always reflects the current
	// migrations, even against a long-lived local server.
	if _, err := admin.Exec(ctx, "DROP DATABASE IF EXISTS "+templateDB+" WITH (FORCE)"); err != nil {
		return err
	}
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+templateDB); err != nil {
		return err
	}
	// Migrate closes its connection before returning, which matters: a
	// template with a live session cannot be cloned.
	return store.Migrate(ctx, withDatabase(adminURL, templateDB), nil)
}

func withDatabase(raw, db string) string {
	u, err := url.Parse(raw)
	if err != nil {
		panic(err)
	}
	u.Path = "/" + db
	return u.String()
}

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
