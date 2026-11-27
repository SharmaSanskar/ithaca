// Package deliver makes one outbound webhook request: sign it, send it through
// the SSRF-guarded transport, and classify what came back. It knows nothing
// about queues, leases or retries; the worker owns all of that.
package deliver

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
)

// Response body handling: read at most maxRead bytes (so keep-alive works for
// normal bodies), keep the first maxKeep for the attempt log.
const (
	maxRead = 64 << 10
	maxKeep = 4 << 10
)

// UserAgent identifies Ithaca to subscribers.
const UserAgent = "Ithaca-Webhooks/1.0"

// Request is one delivery attempt.
type Request struct {
	URL        string
	Timeout    time.Duration
	EventID    uuid.UUID
	DeliveryID uuid.UUID
	EventType  string
	Attempt    int // 1-based
	Body       []byte
	KeyID      string
	Secret     []byte
	// Traceparent, if set, is forwarded so a subscriber can continue the trace.
	Traceparent string
}

// Response is what happened.
type Response struct {
	Classification
	StatusCode int
	Body       []byte // first 4 KiB of the response
	Duration   time.Duration
	Err        error
}

// Client sends webhook requests.
type Client struct {
	http *http.Client
	now  func() time.Time
}

// NewClient builds the process-wide delivery client.
//
// ONE transport for the whole process. A transport per request defeats
// connection pooling and eventually exhausts file descriptors — the classic Go
// HTTP-client leak.
func NewClient(g *Guard) *Client {
	d := &net.Dialer{
		Timeout:   5 * time.Second, // connect timeout, separate from the request timeout
		KeepAlive: 30 * time.Second,
		Control:   g.Control,
	}
	t := &http.Transport{
		// No Proxy: an environment proxy would route around the SSRF guard,
		// because Control would then vet the proxy's address, not the target.
		Proxy:                 nil,
		DialContext:           d.DialContext,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: time.Second,
		// One dispatcher at the defaults has 64 workers × max_concurrency 8 =
		// 512 requests in flight, all possibly to ONE busy host. The per-host
		// idle cap must cover that, or every request above the cap closes its
		// connection after use and the next one dials a new one. Measured
		// (bench E2): with a per-host cap of 16, a 14k deliveries/s drain
		// piled up 16k sockets in TIME_WAIT within two seconds, exhausted the
		// ephemeral port range, and collapsed to ~2k/s. Go's default of 2 is
		// the same bug, sooner.
		MaxIdleConns:        512,
		MaxIdleConnsPerHost: 512,
		IdleConnTimeout:     90 * time.Second,
		ForceAttemptHTTP2:   true,
	}
	return &Client{
		http: &http.Client{
			Transport: t,
			// The timeout is per request via context, since it is per endpoint.
			CheckRedirect: func(*http.Request, []*http.Request) error {
				// Never follow. A redirect is classified permanent: following
				// it is an SSRF hole and silently changes the destination.
				return http.ErrUseLastResponse
			},
		},
		now: time.Now,
	}
}

// Deliver performs one attempt. It never returns an error: every failure is
// a classified Response, because every failure needs a scheduling decision.
func (c *Client) Deliver(ctx context.Context, r Request) Response {
	start := time.Now()
	rctx, cancel := context.WithTimeout(ctx, r.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(rctx, http.MethodPost, r.URL, bytes.NewReader(r.Body))
	if err != nil {
		return Response{Classification: Classification{Outcome: Permanent, Class: "bad_url"}, Err: err}
	}

	ts := c.now()
	eventID := r.EventID.String()
	h := req.Header
	h.Set("Content-Type", "application/json")
	h.Set("User-Agent", UserAgent)
	h.Set("Ithaca-Id", eventID)
	h.Set("Ithaca-Delivery-Id", r.DeliveryID.String())
	h.Set("Ithaca-Event-Type", r.EventType)
	h.Set("Ithaca-Attempt", strconv.Itoa(r.Attempt))
	h.Set("Ithaca-Timestamp", strconv.FormatInt(ts.Unix(), 10))
	// The exact bytes in r.Body are what is signed AND what is sent.
	h.Set("Ithaca-Signature", Sign(r.Secret, r.KeyID, eventID, ts, r.Body))
	if r.Traceparent != "" {
		h.Set("traceparent", r.Traceparent)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return Response{
			Classification: Classify(nil, err, ctx.Err() != nil),
			Duration:       time.Since(start),
			Err:            err,
		}
	}

	// Read (bounded) and close. Reading before Close is what returns the
	// connection to the keep-alive pool; skipping it leaks a connection per
	// request.
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxRead))
	_ = resp.Body.Close()
	if len(body) > maxKeep {
		body = body[:maxKeep]
	}

	out := Response{
		Classification: Classify(resp, nil, false),
		StatusCode:     resp.StatusCode,
		Body:           body,
		Duration:       time.Since(start),
	}
	// A 2xx whose body we failed to read is still a 2xx: the subscriber
	// accepted it. Don't turn a success into a retry over a truncated body.
	if readErr != nil && out.Outcome != Success {
		out.Err = readErr
	}
	return out
}
