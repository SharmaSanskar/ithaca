//go:build integration

package janitor

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

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

// Retention must never delete an event something still needs: one with a
// pending delivery (the dispatcher reads its payload at delivery time) or an
// unreplayed dead letter (replay needs it). Everything else old goes.
func TestRetentionKeepsWhatIsStillNeeded(t *testing.T) {
	s := testdb.New(t)
	ctx := context.Background()
	exec := func(q string) {
		t.Helper()
		if _, err := s.Pool.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	exec(`INSERT INTO tenants (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 't')`)
	exec(`INSERT INTO endpoints (id, tenant_id, url, state, signing_key_id, signing_secret)
	      VALUES ('00000000-0000-0000-0000-0000000000e1', '00000000-0000-0000-0000-000000000001', 'https://x', 'enabled', 'k', '\x00')`)
	exec(`INSERT INTO subscriptions (id, tenant_id, endpoint_id, event_types)
	      VALUES ('00000000-0000-0000-0000-0000000000b1', '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-0000000000e1', '{*}')`)
	exec(`INSERT INTO delivery_streams (tenant_id, subscription_id, endpoint_id, stream_key, ordered)
	      VALUES ('00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-0000000000b1', '00000000-0000-0000-0000-0000000000e1', 'lane:0', false)`)
	// Four OLD events: done, still pending, dead (unreplayed), dead (replayed).
	for i, kind := range []string{"done", "pending", "dead", "replayed"} {
		ev := fmt.Sprintf("00000000-0000-0000-0000-00000000000%d", i+1)
		exec(`INSERT INTO events (id, tenant_id, event_type, payload, created_at)
		      VALUES ('` + ev + `', '00000000-0000-0000-0000-000000000001', '` + kind + `', '{}', now() - interval '2 days')`)
		dl := fmt.Sprintf("00000000-0000-0000-0000-0000000000d%d", i+1)
		switch kind {
		case "pending":
			exec(`INSERT INTO deliveries (id, tenant_id, stream_id, subscription_id, endpoint_id, event_id, seq)
			      SELECT '` + dl + `', tenant_id, id, subscription_id, endpoint_id, '` + ev + `', 1 FROM delivery_streams`)
		default:
			final, replayed := "succeeded", "NULL"
			if kind == "dead" || kind == "replayed" {
				final = "dead"
			}
			if kind == "replayed" {
				replayed = "now()"
			}
			exec(`INSERT INTO deliveries_history (id, tenant_id, stream_id, subscription_id, endpoint_id, event_id, seq,
			        attempt, created_at, final_state, completed_at, replayed_at)
			      SELECT '` + dl + `', tenant_id, id, subscription_id, endpoint_id, '` + ev + `', ` + fmt.Sprint(i+1) + `, 1,
			        now() - interval '2 days', '` + final + `', now() - interval '2 days', ` + replayed + ` FROM delivery_streams`)
		}
	}
	exec(`INSERT INTO events (id, tenant_id, event_type, payload) VALUES
	      ('00000000-0000-0000-0000-0000000000f1', '00000000-0000-0000-0000-000000000001', 'recent', '{}')`)

	j := New(s, slog.New(slog.NewTextHandler(io.Discard, nil)), prometheus.NewRegistry())
	j.Retention = 24 * time.Hour
	j.retain(ctx)

	rows, _ := s.Pool.Query(ctx, "SELECT event_type FROM events ORDER BY event_type")
	var left []string
	for rows.Next() {
		var k string
		_ = rows.Scan(&k)
		left = append(left, k)
	}
	rows.Close()
	want := []string{"dead", "pending", "recent"}
	if fmt.Sprint(left) != fmt.Sprint(want) {
		t.Errorf("events left = %v, want %v (pending and unreplayed-dead events must survive)", left, want)
	}
}
