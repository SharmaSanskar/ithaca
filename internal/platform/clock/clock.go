// Package clock makes "now" injectable.
//
// Two techniques cover time in tests, and this package is the fallback one:
//
//   - testing/synctest (Go 1.25+) virtualizes time.Now, timers and sleeps for
//     every goroutine in its bubble. The pool's heartbeat, polling and
//     shutdown timing tests use it, against in-memory fakes.
//   - synctest cannot help once a goroutine blocks on real I/O (a Postgres
//     query, a socket), because such a goroutine is never "durably blocked".
//     Code that must be timed against real I/O reads time through Clock, so a
//     test can hold it still or jump it forward.
//
// Note what is NOT read from this clock: anything that schedules work in the
// database. Retry and visibility times are computed as `now() + interval` in
// SQL, so every dispatcher replica agrees on one clock (Postgres's), and a
// replica with a skewed system clock cannot make work due early or late.
package clock

import (
	"sync"
	"time"
)

// Clock is the only way time-sensitive code reads the current time.
type Clock interface {
	Now() time.Time
}

// Real is the system clock.
type Real struct{}

// Now returns time.Now().
func (Real) Now() time.Time { return time.Now() }

// Fake is a manually-driven clock for tests. Safe for concurrent use.
type Fake struct {
	mu  sync.Mutex
	now time.Time
}

// NewFake returns a fake clock starting at t.
func NewFake(t time.Time) *Fake { return &Fake{now: t} }

// Now returns the fake's current time.
func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// Advance moves the fake clock forward.
func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
}

// Set jumps the fake clock to t.
func (f *Fake) Set(t time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = t
}
