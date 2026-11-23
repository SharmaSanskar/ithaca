//go:build integration

// Package testdb gives every integration test its own freshly-migrated
// Postgres database in ~20ms.
//
// The template-database trick: migrations run ONCE into a template database
// named after a hash of the migration files (`ithaca_tmpl_<hash>`). Each test
// then does
//
//	CREATE DATABASE t_<random> TEMPLATE ithaca_tmpl_<hash>
//
// which is a file-level copy, not a replay of every migration. Tests get
// complete isolation (no shared rows, no ordering dependence, safe under
// t.Parallel) without paying migration cost per test.
//
// Real Postgres, not sqlmock: every interesting behavior in this system —
// SKIP LOCKED, ON CONFLICT row locks, unique-index waits — IS Postgres
// behavior, and a mock would only test our assumptions about it.
//
// Many test binaries can share one server: `go test ./...` runs each
// package's binary in parallel, and in CI they all point at the same service
// container. So the template is built under a cross-process advisory lock,
// into a temporary name that is renamed only once fully migrated, and reused
// by every binary (and every later run) whose migrations hash the same.
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
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/SharmaSanskar/ithaca/internal/store"
	"github.com/SharmaSanskar/ithaca/migrations"
)

// templateLockID is an arbitrary constant for pg_advisory_lock. Every test
// binary on the server agrees on it, which is what serializes template builds.
const templateLockID = 0x17ac_a7e5

var (
	once       sync.Once
	adminURL   string // connection string to the server's maintenance database
	templateDB string // ithaca_tmpl_<migrations hash>
	initErr    error

	// Serializes clones within one binary. Across binaries, cloneTemplate
	// retries instead, because a mutex cannot span processes.
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
	err = cloneTemplate(ctx, admin, name)
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

	// Assign, don't declare: templateDB is the package-level name every clone
	// uses.
	var err error
	if templateDB, err = templateName(); err != nil {
		return err
	}
	return ensureTemplate(ctx, templateDB)
}

// templateName hashes every migration file's name and contents, so editing a
// migration produces a new template instead of silently reusing a stale one.
func templateName() (string, error) {
	h := sha256.New()
	err := fs.WalkDir(migrations.FS, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, err := migrations.FS.ReadFile(path)
		if err != nil {
			return err
		}
		fmt.Fprintf(h, "%s\x00%d\x00", path, len(b))
		h.Write(b)
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("hash migrations: %w", err)
	}
	return "ithaca_tmpl_" + hex.EncodeToString(h.Sum(nil))[:12], nil
}

// ensureTemplate builds the template exactly once per server, however many
// test binaries race to do it.
func ensureTemplate(ctx context.Context, name string) error {
	admin, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer admin.Close(ctx)

	// A session-level advisory lock: the first binary builds, the others
	// block here and then find the template already present.
	if _, err = admin.Exec(ctx, "SELECT pg_advisory_lock($1)", templateLockID); err != nil {
		return fmt.Errorf("advisory lock: %w", err)
	}
	defer func() { _, _ = admin.Exec(context.WithoutCancel(ctx), "SELECT pg_advisory_unlock($1)", templateLockID) }()

	var exists bool
	if qerr := admin.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)", name).Scan(&exists); qerr != nil {
		return qerr
	}
	if exists {
		return nil
	}

	// Build under a temporary name and rename at the end. A run that dies
	// mid-migration leaves only a *_building database behind, never a
	// half-migrated template that later runs would clone.
	building := name + "_building"
	if _, err = admin.Exec(ctx, "DROP DATABASE IF EXISTS "+building+" WITH (FORCE)"); err != nil {
		return err
	}
	if _, err = admin.Exec(ctx, "CREATE DATABASE "+building); err != nil {
		return err
	}
	// Migrate closes its connection before returning, which matters: a
	// database with a live session can be neither renamed nor cloned.
	if err = store.Migrate(ctx, withDatabase(adminURL, building), nil); err != nil {
		_, _ = admin.Exec(ctx, "DROP DATABASE IF EXISTS "+building+" WITH (FORCE)")
		return err
	}
	if _, err = admin.Exec(ctx, "ALTER DATABASE "+building+" RENAME TO "+name); err != nil {
		return err
	}

	// Templates from older migration sets are dead weight on a long-lived
	// local server; drop them while we hold the lock.
	rows, err := admin.Query(ctx,
		"SELECT datname FROM pg_database WHERE datname LIKE 'ithaca\\_tmpl\\_%' AND datname <> $1", name)
	if err != nil {
		return err
	}
	stale, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return err
	}
	for _, db := range stale {
		_, _ = admin.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %q WITH (FORCE)", db))
	}
	return nil
}

// cloneTemplate creates a test database from the template. Postgres refuses
// to copy a database that another session is touching (SQLSTATE 55006), which
// can happen briefly when several test binaries clone at once; that is
// transient, so retry it.
func cloneTemplate(ctx context.Context, admin *pgx.Conn, name string) error {
	var err error
	for attempt := range 20 {
		_, err = admin.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s TEMPLATE %s", name, templateDB))
		var pgErr *pgconn.PgError
		if err == nil || !errors.As(err, &pgErr) || pgErr.Code != "55006" {
			return err
		}
		time.Sleep(time.Duration(10*(attempt+1)) * time.Millisecond)
	}
	return err
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
