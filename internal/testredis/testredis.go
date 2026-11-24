//go:build integration

// Package testredis gives integration tests a Redis client.
//
//   - ITHACA_TEST_REDIS_URL set → use it (CI service container, or `make up`).
//   - otherwise → start redis:8-alpine with testcontainers, once per binary.
//
// Each test gets its own logical database number where possible and a
// FLUSHDB at cleanup, so tests do not see each other's keys. Pub/sub is
// server-wide, so tests that use it pick unique channel/tenant ids.
package testredis

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
)

var (
	once    sync.Once
	baseURL string
	initErr error
)

// New returns a client on a flushed database, closed at cleanup.
func New(t testing.TB) *redis.Client {
	t.Helper()
	once.Do(func() {
		baseURL = os.Getenv("ITHACA_TEST_REDIS_URL")
		if baseURL != "" {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		c, err := tcredis.Run(ctx, "redis:8-alpine")
		if err != nil {
			initErr = err
			return
		}
		baseURL, initErr = c.ConnectionString(ctx)
	})
	if initErr != nil {
		t.Fatalf("testredis: %v", initErr)
	}
	opts, err := redis.ParseURL(baseURL)
	if err != nil {
		t.Fatal(err)
	}
	c := redis.NewClient(opts)
	t.Cleanup(func() { _ = c.Close() })
	return c
}
