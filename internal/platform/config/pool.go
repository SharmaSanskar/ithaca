package config

import "runtime"

// defaultMaxConns implements the sizing rule from the design docs:
//
//	MaxConns ≈ 2*GOMAXPROCS + 8   — NOT the worker-pool size.
//
// Delivery workers hold a connection only for the lease and the ack; the
// outbound HTTP call (which can run for tens of seconds) happens with no
// connection checked out. Sizing the pool to the number of worker goroutines
// therefore buys nothing and multiplies across replicas until Postgres starts
// refusing connections.
func defaultMaxConns() int {
	n := 2*runtime.GOMAXPROCS(0) + 8
	if n < 8 {
		n = 8
	}
	if n > 64 {
		n = 64
	}
	return n
}
