package live

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// RedisBus is the Bus on Redis pub/sub.
//
// Pub/sub is fire-and-forget: a subscriber that is disconnected misses
// messages. That is acceptable here precisely because nothing on this bus is
// the source of truth: frames are superseded every 100ms and endpoint deltas
// are reconciled from Postgres every second.
type RedisBus struct {
	c *redis.Client
}

// NewRedisBus wraps a client.
func NewRedisBus(c *redis.Client) *RedisBus { return &RedisBus{c: c} }

// Publish implements Bus.
func (b *RedisBus) Publish(ctx context.Context, msg []byte) error {
	return b.c.Publish(ctx, Channel, msg).Err()
}

// Subscribe streams messages until ctx is done. go-redis reconnects and
// resubscribes on its own after a Redis restart.
func (b *RedisBus) Subscribe(ctx context.Context) <-chan []byte {
	ps := b.c.Subscribe(ctx, Channel)
	out := make(chan []byte, 1024)
	go func() {
		defer close(out)
		defer func() { _ = ps.Close() }()
		ch := ps.Channel()
		for {
			select {
			case <-ctx.Done():
				return
			case m, ok := <-ch:
				if !ok {
					return
				}
				select {
				case out <- []byte(m.Payload):
				default: // the hub is behind; frames are lossy anyway
				}
			}
		}
	}()
	return out
}
