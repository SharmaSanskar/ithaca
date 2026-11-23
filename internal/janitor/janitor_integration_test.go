//go:build integration

package janitor

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/SharmaSanskar/ithaca/internal/testdb"
)

func TestSweepDeletesOnlyExpiredKeysInBatches(t *testing.T) {
	s := testdb.New(t)
	ctx := context.Background()

	// 2,500 expired keys (spanning three batches) and 10 live ones.
	_, err := s.Pool.Exec(ctx, `
		INSERT INTO idempotency_keys (tenant_id, key, request_hash, response_code, response_body, event_id, expires_at)
		SELECT gen_random_uuid(), 'k' || g, decode(repeat('00', 32), 'hex'), 202, '{}', gen_random_uuid(),
		       CASE WHEN g <= 2500 THEN now() - interval '1 hour' ELSE now() + interval '1 hour' END
		FROM generate_series(1, 2510) g`)
	if err != nil {
		t.Fatal(err)
	}

	j := New(s, slog.New(slog.NewTextHandler(io.Discard, nil)), prometheus.NewRegistry())
	j.sweep(ctx)

	var left int
	_ = s.Pool.QueryRow(ctx, "SELECT count(*) FROM idempotency_keys").Scan(&left)
	if left != 10 {
		t.Errorf("%d keys remain, want the 10 unexpired ones", left)
	}
}
