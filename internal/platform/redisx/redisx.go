// Package redisx opens the Redis client.
//
// Redis carries only the live dashboard feed. Services that deliver webhooks
// register it as a NON-critical readiness dependency: a Redis outage makes the
// dashboard stale and must never take a dispatcher out of rotation.
package redisx

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Open parses the URL and builds a client. Like the Postgres pool it does not
// connect eagerly.
func Open(url string) (*redis.Client, error) {
	if url == "" {
		return nil, errors.New("ITHACA_REDIS_URL is required for the live feed")
	}
	opts, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("parse redis url: %w", err)
	}
	opts.DialTimeout = 2 * time.Second
	opts.ReadTimeout = time.Second
	opts.WriteTimeout = time.Second
	return redis.NewClient(opts), nil
}

// Ping is a readiness check.
func Ping(c *redis.Client) func(context.Context) error {
	return func(ctx context.Context) error { return c.Ping(ctx).Err() }
}
