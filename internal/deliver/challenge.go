package deliver

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// ChallengeEventType is the event type of the verification request.
const ChallengeEventType = "ithaca.endpoint.verification"

// ChallengeRequest is one verification handshake.
type ChallengeRequest struct {
	URL        string
	EndpointID uuid.UUID
	KeyID      string
	Secret     []byte
	Timeout    time.Duration
}

// ErrChallengeFailed wraps every reason an endpoint did not pass.
var ErrChallengeFailed = errors.New("endpoint verification failed")

// Challenge proves the URL's owner wants these webhooks.
//
// Ithaca POSTs a signed body containing a random challenge, also sent in the
// Ithaca-Challenge header. The endpoint passes by answering 2xx with the
// challenge echoed back, either as {"challenge": "<value>"} or as the raw
// value. A server that merely returns 200 to everything does NOT pass: it has
// to have read the request. That is what stops Ithaca being pointed at a
// stranger's API as a traffic amplifier.
//
// It goes through the same SSRF-guarded transport as deliveries.
func (c *Client) Challenge(ctx context.Context, r ChallengeRequest) error {
	raw := make([]byte, 24)
	_, _ = rand.Read(raw)
	challenge := base64.RawURLEncoding.EncodeToString(raw)
	body, _ := json.Marshal(map[string]string{"type": ChallengeEventType, "challenge": challenge})

	rctx, cancel := context.WithTimeout(ctx, r.Timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(rctx, http.MethodPost, r.URL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("%w: %v", ErrChallengeFailed, err) //nolint:errorlint // one wrapped sentinel is intended
	}
	ts := c.now()
	id := "verify_" + r.EndpointID.String()
	h := req.Header
	h.Set("Content-Type", "application/json")
	h.Set("User-Agent", UserAgent)
	h.Set("Ithaca-Id", id)
	h.Set("Ithaca-Event-Type", ChallengeEventType)
	h.Set("Ithaca-Challenge", challenge)
	h.Set("Ithaca-Timestamp", strconv.FormatInt(ts.Unix(), 10))
	h.Set("Ithaca-Signature", Sign(r.Secret, r.KeyID, id, ts, body))

	resp, err := c.http.Do(req)
	if err != nil {
		cl := Classify(nil, err, ctx.Err() != nil)
		return fmt.Errorf("%w: %s: %v", ErrChallengeFailed, cl.Class, err) //nolint:errorlint // one wrapped sentinel is intended
	}
	got, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
	_ = resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("%w: HTTP %d", ErrChallengeFailed, resp.StatusCode)
	}
	if echoed(got, challenge) {
		return nil
	}
	return fmt.Errorf("%w: HTTP %d but the response did not echo the challenge", ErrChallengeFailed, resp.StatusCode)
}

func echoed(body []byte, challenge string) bool {
	var v struct {
		Challenge string `json:"challenge"`
	}
	if json.Unmarshal(body, &v) == nil && v.Challenge == challenge {
		return true
	}
	return strings.TrimSpace(string(body)) == challenge
}
