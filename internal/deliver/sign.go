package deliver

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"strconv"
	"strings"
	"time"
)

// Signature header format:
//
//	Ithaca-Signature: v1,k=<key_id>,<base64(hmac)>
//
// Several space-separated components may appear (one per active secret), which
// is what makes zero-downtime secret rotation possible later without a
// wire-format change. Subscribers accept if ANY component verifies.
//
// The signed content is exactly:
//
//	<event_id> "." <unix_timestamp> "." <raw_body>
//
// Covering the id and timestamp, not just the body, is the point: a body-only
// signature can be replayed forever by anyone who captured one request.
const sigVersion = "v1"

// Sign returns one signature component for the given secret.
func Sign(secret []byte, keyID, eventID string, ts time.Time, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(eventID))
	mac.Write([]byte{'.'})
	mac.Write([]byte(strconv.FormatInt(ts.Unix(), 10)))
	mac.Write([]byte{'.'})
	mac.Write(body)
	return sigVersion + ",k=" + keyID + "," + base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

// DefaultTolerance is the replay window subscribers should enforce.
const DefaultTolerance = 5 * time.Minute

// Verify is the subscriber-side check: what a customer's webhook handler
// does. It lives here so the receiver simulator and the tests verify exactly
// the way the docs tell customers to.
func Verify(secret []byte, header, eventID, tsHeader string, body []byte, now time.Time, tolerance time.Duration) bool {
	sec, err := strconv.ParseInt(tsHeader, 10, 64)
	if err != nil {
		return false
	}
	ts := time.Unix(sec, 0)
	if d := now.Sub(ts); d > tolerance || d < -tolerance {
		return false
	}
	want := Sign(secret, "", eventID, ts, body)
	wantMAC := want[strings.LastIndexByte(want, ',')+1:]
	for _, comp := range strings.Fields(header) {
		parts := strings.Split(comp, ",")
		if len(parts) != 3 || parts[0] != sigVersion {
			continue
		}
		if hmac.Equal([]byte(parts[2]), []byte(wantMAC)) {
			return true
		}
	}
	return false
}
