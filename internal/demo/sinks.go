package demo

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/SharmaSanskar/ithaca/internal/receiver"
)

// SinkClient controls receiver-sim's sinks. receiver-sim is internal-only:
// the public dashboard changes sinks through the demo API, which is
// rate-limited and validated, never by talking to receiver-sim directly.
type SinkClient struct {
	BaseURL string
	HTTP    *http.Client
}

// NewSinkClient builds a client.
func NewSinkClient(base string) *SinkClient {
	return &SinkClient{BaseURL: base, HTTP: &http.Client{Timeout: 3 * time.Second}}
}

// Set replaces a sink's behavior.
func (c *SinkClient) Set(ctx context.Context, sink string, b receiver.Behavior) error {
	body, _ := json.Marshal(b)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.BaseURL+"/sink/"+sink+"/behavior", bytes.NewReader(body))
	if err != nil {
		return err
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("set sink %s: HTTP %d", sink, resp.StatusCode)
	}
	return nil
}

// Get reads a sink's behavior.
func (c *SinkClient) Get(ctx context.Context, sink string) (receiver.Behavior, error) {
	var b receiver.Behavior
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+"/sink/"+sink+"/behavior", nil)
	if err != nil {
		return b, err
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return b, err
	}
	defer resp.Body.Close()
	err = json.NewDecoder(resp.Body).Decode(&b)
	return b, err
}

// Healthy is a sink behaving like a decent production endpoint.
var Healthy = receiver.Behavior{Status: 200, LatencyMS: 40}

// Clamp bounds what the public demo controls may ask for, so nobody can hang
// every sink or set a 10-minute latency on a shared demo.
func Clamp(b receiver.Behavior) receiver.Behavior {
	b.LatencyMS = min(max(b.LatencyMS, 0), 3000)
	b.FailRate = min(max(b.FailRate, 0), 1)
	b.Hang = false
	b.FailFirst, b.FailRequests = 0, 0
	b.RetryAfter = min(max(b.RetryAfter, 0), 30)
	if b.Status != 0 && (b.Status < 200 || b.Status > 299) {
		b.Status = 200
	}
	if b.FailStatus != 0 && (b.FailStatus < 400 || b.FailStatus > 599) {
		b.FailStatus = 503
	}
	return b
}
