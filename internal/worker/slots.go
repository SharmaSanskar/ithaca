package worker

import "context"

// Slots is a counting semaphore with PARTIAL acquisition. It is the whole
// backpressure mechanism, and it is not in the standard library.
//
// The dispatcher asks for "up to N" slots, blocks until at least one is free,
// and then leases exactly as many streams as it got. Postgres load therefore
// tracks spare processing capacity, never queue depth: while every worker is
// busy the dispatcher sits in AcquireUpTo and issues no queries at all.
//
// Implemented as a channel of tokens rather than sync.Cond because acquisition
// must be cancellable by a context, which a Cond cannot do.
type Slots struct {
	tokens chan struct{}
}

// NewSlots creates a semaphore with n slots, all free.
func NewSlots(n int) *Slots {
	s := &Slots{tokens: make(chan struct{}, n)}
	for range n {
		s.tokens <- struct{}{}
	}
	return s
}

// AcquireUpTo blocks until at least one slot is free (or ctx is done), then
// takes as many free slots as it can, up to max. It returns 0 only when ctx
// is done.
func (s *Slots) AcquireUpTo(ctx context.Context, max int) int {
	if max < 1 {
		return 0
	}
	select {
	case <-s.tokens:
	case <-ctx.Done():
		return 0
	}
	n := 1
	for n < max {
		select {
		case <-s.tokens:
			n++
		default:
			return n
		}
	}
	return n
}

// Release returns n slots.
//
// Releasing more than was acquired is a bug, and it panics rather than
// silently inflating capacity. The opposite bug — forgetting
// Release(n - len(leases)) after a short dequeue — cannot be detected here;
// its symptom is throughput decaying to zero over hours as slots leak. The
// pool tests assert InFlight returns to zero to catch it.
func (s *Slots) Release(n int) {
	for range n {
		select {
		case s.tokens <- struct{}{}:
		default:
			panic("worker: Slots.Release without a matching acquire")
		}
	}
}

// InFlight returns how many slots are taken.
func (s *Slots) InFlight() int { return cap(s.tokens) - len(s.tokens) }

// Cap returns the total number of slots.
func (s *Slots) Cap() int { return cap(s.tokens) }
