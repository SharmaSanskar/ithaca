package live

import (
	"sync"
)

// maxPendingDeltas bounds one connection's delta queue. Deltas are never
// dropped, so a client that cannot keep up must eventually be disconnected
// instead; it reconnects (with jittered backoff) and refetches.
const maxPendingDeltas = 256

// outbox is one connection's send queue, and the whole per-connection
// backpressure policy:
//
//   - frames: at most ONE pending per channel. A newer frame replaces an
//     unsent older one, because a frame is a snapshot and the newer one is
//     strictly more useful. A slow client sees fewer frames, never stale ones.
//   - deltas: queued in order, never dropped. Overflow means the client is
//     hopelessly behind; the connection is closed.
//
// The hub only ever calls offer*, which never block, so one slow client can
// never stall the broadcast to everyone else.
type outbox struct {
	mu         sync.Mutex
	frames     map[string][]byte
	frameOrder []string
	deltas     [][]byte
	superseded int
	overflow   bool
	wake       chan struct{}
}

func newOutbox() *outbox {
	return &outbox{frames: map[string][]byte{}, wake: make(chan struct{}, 1)}
}

func (o *outbox) signal() {
	select {
	case o.wake <- struct{}{}:
	default:
	}
}

// offerFrame queues a frame, replacing any unsent frame for the same channel.
// It reports whether an older frame was superseded.
func (o *outbox) offerFrame(ch string, b []byte) (superseded bool) {
	o.mu.Lock()
	if _, ok := o.frames[ch]; ok {
		o.superseded++
		superseded = true
	} else {
		o.frameOrder = append(o.frameOrder, ch)
	}
	o.frames[ch] = b
	o.mu.Unlock()
	o.signal()
	return superseded
}

// offerDelta queues a delta. It returns false if the queue overflowed, in
// which case the connection must be closed.
func (o *outbox) offerDelta(b []byte) bool {
	o.mu.Lock()
	if len(o.deltas) >= maxPendingDeltas {
		o.overflow = true
		o.mu.Unlock()
		o.signal()
		return false
	}
	o.deltas = append(o.deltas, b)
	o.mu.Unlock()
	o.signal()
	return true
}

// take drains everything pending: deltas first (in order), then frames.
func (o *outbox) take() (msgs [][]byte, superseded int, overflow bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	msgs = append(msgs, o.deltas...)
	o.deltas = o.deltas[:0]
	for _, ch := range o.frameOrder {
		msgs = append(msgs, o.frames[ch])
		delete(o.frames, ch)
	}
	o.frameOrder = o.frameOrder[:0]
	superseded, o.superseded = o.superseded, 0
	return msgs, superseded, o.overflow
}
