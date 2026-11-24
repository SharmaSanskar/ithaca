// Package live is the real-time layer behind the dashboard.
//
//	ingest ─┐ per-process 100ms         Redis            ws hub              browser
//	        ├─ pre-aggregation ──▶ PUBLISH ithaca:live ──▶ merge per tenant ──▶ 10Hz frame
//	dispatch┘ (counts, samples)    (best effort)          + 1s Postgres poll    + deltas
//
// Two message classes go to the browser:
//
//   - FRAME (10Hz, lossy by design): a snapshot of the last 100ms — counts per
//     endpoint, latency percentiles, a few sampled events, queue depth. A newer
//     frame supersedes an unsent older one, so a slow client just sees fewer
//     frames, never a backlog.
//   - DELTA (entity changes, never dropped by the hub): an endpoint changed
//     state, a delivery was dead-lettered, a stream paused. Endpoint deltas are
//     re-derived from Postgres every second (updated_at is the revision), so a
//     message lost in pub/sub is corrected within a second.
//
// The rule the whole package follows: the frontend never simulates. Every
// number on screen is a count of something that happened.
package live

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Channel is the Redis pub/sub channel every live message travels on.
const Channel = "ithaca:live"

// ── between processes (Redis) ────────────────────────────────────────────────

// Envelope is one Redis message.
type Envelope struct {
	Kind      string     `json:"k"` // "batch" | "delta" | "narration"
	Tenant    uuid.UUID  `json:"tn"`
	Batch     *Batch     `json:"b,omitempty"`
	Delta     *Delta     `json:"d,omitempty"`
	Narration *Narration `json:"n,omitempty"`
}

// Batch is one process's 100ms window for one tenant.
type Batch struct {
	Ingested  int                           `json:"in,omitempty"`
	Endpoints map[uuid.UUID]*EndpointWindow `json:"ep,omitempty"`
	Samples   []Sample                      `json:"s,omitempty"`
}

// EndpointWindow counts delivery outcomes for one endpoint in one window.
type EndpointWindow struct {
	OK          int     `json:"ok,omitempty"`
	Retry       int     `json:"re,omitempty"`
	Dead        int     `json:"de,omitempty"`
	RateLimited int     `json:"rl,omitempty"`
	Latencies   []int32 `json:"lat,omitempty"` // ms, a reservoir sample
	latSeen     int
}

// Sample outcomes.
const (
	Ingested    = "ingested"
	Delivered   = "delivered"
	Retrying    = "retry"
	DeadLetter  = "dead"
	RateLimited = "rate_limited"
)

// Sample is one concrete event, for the feed and the canvas particles.
type Sample struct {
	EventID    uuid.UUID  `json:"event_id"`
	DeliveryID *uuid.UUID `json:"delivery_id,omitempty"`
	EndpointID *uuid.UUID `json:"endpoint_id,omitempty"`
	Type       string     `json:"type"`
	Outcome    string     `json:"outcome"`
	Attempt    int        `json:"attempt,omitempty"`
	LatencyMS  int        `json:"latency_ms,omitempty"`
	At         int64      `json:"at"` // unix ms
}

// priority: failures are the interesting events. A 0.5% failure rate must
// still show up on screen, so failures are never sampled out.
func (s Sample) priority() bool {
	return s.Outcome == Retrying || s.Outcome == DeadLetter || s.Outcome == RateLimited
}

// Delta kinds.
const (
	DeltaEndpoint   = "endpoint"    // breaker transition, state change; re-read from Postgres
	DeltaDeadLetter = "dead_letter" // a delivery was dead-lettered
	DeltaStream     = "stream"      // an ordered stream paused
)

// Delta is an entity change.
type Delta struct {
	Kind string          `json:"kind"`
	ID   string          `json:"id"`
	Data json.RawMessage `json:"data,omitempty"`
}

// Narration is one line of the demo's plain-English commentary.
type Narration struct {
	Scenario string    `json:"scenario"`
	Step     int       `json:"step"`
	Total    int       `json:"total"`
	Text     string    `json:"text"`
	Level    string    `json:"level"` // "info" | "warn" | "success"
	Done     bool      `json:"done,omitempty"`
	At       time.Time `json:"at"`
}

// ── to the browser ───────────────────────────────────────────────────────────

// Frame is the 10Hz snapshot.
type Frame struct {
	Type      string          `json:"type"` // "frame"
	Ch        string          `json:"ch"`
	Seq       uint64          `json:"seq"`
	T         int64           `json:"t"` // unix ms
	WindowMS  int             `json:"window_ms"`
	Ingested  int             `json:"ingested"`
	Endpoints []FrameEndpoint `json:"endpoints"`
	Totals    Totals          `json:"totals"`
	Samples   []Sample        `json:"samples"`
}

// FrameEndpoint is one endpoint's slice of a frame.
type FrameEndpoint struct {
	ID          uuid.UUID `json:"id"`
	Name        string    `json:"name"`
	State       string    `json:"state"`
	Circuit     string    `json:"circuit"`
	Depth       int64     `json:"depth"`
	OK          int       `json:"ok"`
	Retry       int       `json:"retry"`
	Dead        int       `json:"dead"`
	RateLimited int       `json:"rate_limited"`
	P50MS       int       `json:"p50_ms"`
	P99MS       int       `json:"p99_ms"`
}

// Totals are tenant-wide gauges, refreshed once a second.
type Totals struct {
	Pending       int64 `json:"pending"`
	DLQ           int64 `json:"dlq"`
	PausedStreams int64 `json:"paused_streams"`
}

// DeltaMsg is a delta as sent to the browser.
type DeltaMsg struct {
	Type string `json:"type"` // "delta"
	Ch   string `json:"ch"`
	Kind string `json:"kind"`
	ID   string `json:"id"`
	Rev  int64  `json:"rev,omitempty"` // endpoint: updated_at in unix micros
	Data any    `json:"data"`
}

// NarrationMsg is a narration line as sent to the browser.
type NarrationMsg struct {
	Type string `json:"type"` // "narration"
	Ch   string `json:"ch"`
	Narration
}
