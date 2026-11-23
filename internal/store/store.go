// Package store owns the Postgres connection pool and transaction handling.
// Queries live in internal/store/queries/*.sql and are compiled by sqlc into
// internal/store/gen; nothing in Ithaca hand-writes SQL strings in Go.
package store

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/SharmaSanskar/ithaca/internal/platform/config"
	"github.com/SharmaSanskar/ithaca/internal/store/gen"
)

// Store is the pool plus the generated query set bound to it.
type Store struct {
	Pool *pgxpool.Pool
	*gen.Queries
}

// Open builds the pool. It does not wait for a connection: a service must be
// able to boot (and report not-ready) while Postgres is still coming up, rather
// than crash-looping. Readiness is what gates traffic; see Ping.
func Open(ctx context.Context, cfg config.Postgres, log *slog.Logger) (*Store, error) {
	if cfg.URL == "" {
		return nil, errors.New("ITHACA_DATABASE_URL is required for this service")
	}
	pc, err := pgxpool.ParseConfig(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}

	// Sized by 2*GOMAXPROCS+8, not by worker count: see config.defaultMaxConns.
	pc.MaxConns = cfg.MaxConns
	pc.MinConns = cfg.MinConns
	pc.MaxConnLifetime = cfg.MaxConnLifetime
	// Jitter stops every connection in every replica recycling in the same
	// second an hour after a fleet-wide deploy.
	pc.MaxConnLifetimeJitter = cfg.MaxConnLifetime / 10
	pc.MaxConnIdleTime = cfg.MaxConnIdleTime
	pc.ConnConfig.ConnectTimeout = cfg.ConnectTimeout

	// Every connection gets hard ceilings. A statement that runs for 30s on the
	// hot path is a bug, and an idle-in-transaction session holds locks and the
	// xmin horizon hostage (which stops vacuum on the queue tables).
	pc.ConnConfig.RuntimeParams["statement_timeout"] = "30000"
	pc.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] = "60000"
	pc.ConnConfig.RuntimeParams["application_name"] = "ithaca"

	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	if log != nil {
		log.Info("postgres pool created", "max_conns", pc.MaxConns, "min_conns", pc.MinConns)
	}
	return New(pool), nil
}

// New wraps an existing pool (tests use this with a per-test database).
func New(pool *pgxpool.Pool) *Store {
	return &Store{Pool: pool, Queries: gen.New(pool)}
}

// Close releases every connection.
func (s *Store) Close() { s.Pool.Close() }

// Ping is the readiness check. Cheap: one round trip on an idle connection.
func (s *Store) Ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return s.Pool.Ping(ctx)
}

// InTx runs fn inside a transaction. It commits if fn returns nil and rolls
// back otherwise (including on panic, via pgx.BeginFunc's deferred rollback).
func (s *Store) InTx(ctx context.Context, fn func(q *gen.Queries) error) error {
	return pgx.BeginFunc(ctx, s.Pool, func(tx pgx.Tx) error {
		return fn(s.WithTx(tx))
	})
}

// IsNoRows reports whether err means "the query matched nothing".
func IsNoRows(err error) bool { return errors.Is(err, pgx.ErrNoRows) }

// IsUniqueViolation reports a unique-constraint failure (SQLSTATE 23505).
func IsUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

// IsForeignKeyViolation reports a foreign-key failure (SQLSTATE 23503).
func IsForeignKeyViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23503"
}
