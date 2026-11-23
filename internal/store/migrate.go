package store

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"

	"github.com/SharmaSanskar/ithaca/migrations"
)

// Migrate applies every pending migration. Goose takes a Postgres advisory
// lock for the duration, so two replicas racing to migrate on boot serialize
// instead of corrupting the version table.
func Migrate(ctx context.Context, dsn string, log *slog.Logger) error {
	return withProvider(dsn, func(p *goose.Provider) error {
		results, err := p.Up(ctx)
		for _, r := range results {
			if log != nil {
				log.Info("migration applied", "version", r.Source.Version, "file", r.Source.Path, "took", r.Duration)
			}
		}
		if err != nil {
			return fmt.Errorf("migrate up: %w", err)
		}
		if len(results) == 0 && log != nil {
			log.Info("schema already up to date")
		}
		return nil
	})
}

// MigrationStatus writes one line per migration to w.
func MigrationStatus(ctx context.Context, dsn string, w io.Writer) error {
	return withProvider(dsn, func(p *goose.Provider) error {
		statuses, err := p.Status(ctx)
		if err != nil {
			return err
		}
		for _, s := range statuses {
			applied := "pending"
			if s.State == goose.StateApplied {
				applied = "applied " + s.AppliedAt.Format("2006-01-02 15:04:05")
			}
			fmt.Fprintf(w, "%05d  %-28s  %s\n", s.Source.Version, s.Source.Path, applied)
		}
		return nil
	})
}

func withProvider(dsn string, fn func(*goose.Provider) error) error {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	locker, err := lock.NewPostgresSessionLocker()
	if err != nil {
		return err
	}
	p, err := goose.NewProvider(goose.DialectPostgres, db, migrations.FS,
		goose.WithSessionLocker(locker),
	)
	if err != nil {
		return fmt.Errorf("goose provider: %w", err)
	}
	return fn(p)
}
