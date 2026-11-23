package deliver

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// ── signing ──────────────────────────────────────────────────────────────────

func TestSignVerifyRoundTrip(t *testing.T) {
	secret := []byte("0123456789abcdef0123456789abcdef")
	now := time.Unix(1_800_000_000, 0)
	body := []byte(`{"b":2,"a":1}`)
	header := Sign(secret, "k_1", "evt_1", now, body)
	ts := strconv.FormatInt(now.Unix(), 10)

	if !strings.HasPrefix(header, "v1,k=k_1,") {
		t.Fatalf("header = %q", header)
	}
	if !Verify(secret, header, "evt_1", ts, body, now, DefaultTolerance) {
		t.Fatal("valid signature rejected")
	}

	tampered := map[string]func() bool{
		"body": func() bool {
			return Verify(secret, header, "evt_1", ts, []byte(`{"a":1,"b":2}`), now, DefaultTolerance)
		},
		"event id": func() bool { return Verify(secret, header, "evt_2", ts, body, now, DefaultTolerance) },
		"timestamp": func() bool {
			return Verify(secret, header, "evt_1", strconv.FormatInt(now.Unix()+1, 10), body, now, DefaultTolerance)
		},
		"secret": func() bool { return Verify([]byte("wrong"), header, "evt_1", ts, body, now, DefaultTolerance) },
		"stale": func() bool {
			return Verify(secret, header, "evt_1", ts, body, now.Add(6*time.Minute), DefaultTolerance)
		},
	}
	for name, check := range tampered {
		if check() {
			t.Errorf("tampered %s still verified", name)
		}
	}
}

func TestVerifyAcceptsAnyMatchingComponent(t *testing.T) {
	// Rotation: the header carries one signature per active secret, and a
	// subscriber holding either one must accept.
	now := time.Now()
	oldSecret, newSecret := []byte("old-secret"), []byte("new-secret")
	body := []byte(`{}`)
	header := Sign(oldSecret, "k_old", "e", now, body) + " " + Sign(newSecret, "k_new", "e", now, body)
	ts := strconv.FormatInt(now.Unix(), 10)
	for _, s := range [][]byte{oldSecret, newSecret} {
		if !Verify(s, header, "e", ts, body, now, DefaultTolerance) {
			t.Errorf("secret %q rejected a multi-signature header", s)
		}
	}
}

// ── SSRF ─────────────────────────────────────────────────────────────────────

func TestGuardBlocksPrivateAndDisguisedAddresses(t *testing.T) {
	g, _ := NewGuard(nil)
	blocked := []string{
		"127.0.0.1", "10.1.2.3", "172.16.0.1", "192.168.1.1", "100.64.0.1", "0.0.0.0",
		"169.254.169.254", // cloud metadata
		"::1", "fe80::1", "fd00::1", "ff02::1", "::",
		"::ffff:169.254.169.254", // IPv4-mapped IPv6: must be unmapped before checking
		"64:ff9b::a9fe:a9fe",     // NAT64 wrapping 169.254.169.254
		"2002:a9fe:a9fe::1",      // 6to4 wrapping 169.254.169.254
		"224.0.0.1", "255.255.255.255",
	}
	for _, a := range blocked {
		if err := g.Check(netip.MustParseAddr(a)); !errors.Is(err, ErrBlocked) {
			t.Errorf("%s: allowed, want blocked", a)
		}
	}
	for _, a := range []string{"8.8.8.8", "1.1.1.1", "2606:4700:4700::1111"} {
		if err := g.Check(netip.MustParseAddr(a)); err != nil {
			t.Errorf("%s: blocked, want allowed: %v", a, err)
		}
	}
}

func TestGuardExemptionsAreExact(t *testing.T) {
	g, err := NewGuard([]string{"127.0.0.1/32"})
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Check(netip.MustParseAddr("127.0.0.1")); err != nil {
		t.Errorf("exempted address blocked: %v", err)
	}
	if err := g.Check(netip.MustParseAddr("169.254.169.254")); err == nil {
		t.Error("an exemption for loopback opened the metadata service")
	}
}

func TestClientRefusesToConnectToABlockedAddress(t *testing.T) {
	// The real dialer path: a local server exists and is reachable, but the
	// guard (with no exemptions) must refuse to connect to it.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("the request reached the server; the SSRF guard did not fire")
	}))
	defer srv.Close()

	g, _ := NewGuard(nil)
	resp := NewClient(g).Deliver(context.Background(), req(srv.URL))
	if resp.Outcome != Permanent || resp.Class != "ssrf_blocked" {
		t.Fatalf("outcome = %s/%s, want permanent/ssrf_blocked (err %v)", resp.Outcome, resp.Class, resp.Err)
	}
}

// ── classification ───────────────────────────────────────────────────────────

func TestClassifyStatusCodes(t *testing.T) {
	cases := []struct {
		code int
		want Outcome
	}{
		{200, Success}, {201, Success}, {204, Success},
		{301, Permanent}, {302, Permanent}, {307, Permanent},
		{400, Permanent}, {401, Permanent}, {404, Permanent}, {410, Permanent}, {422, Permanent},
		{408, Retryable}, {425, Retryable},
		{429, RateLimited},
		{500, Retryable}, {502, Retryable}, {503, Retryable}, {504, Retryable},
	}
	for _, tc := range cases {
		got := Classify(&http.Response{StatusCode: tc.code, Header: http.Header{}}, nil, false)
		if got.Outcome != tc.want {
			t.Errorf("%d: %s, want %s", tc.code, got.Outcome, tc.want)
		}
	}
}

func TestClassifyErrors(t *testing.T) {
	cases := []struct {
		name       string
		err        error
		parentDone bool
		want       Outcome
		class      string
	}{
		{"we canceled", context.Canceled, true, Canceled, "canceled"},
		{"endpoint timeout", context.DeadlineExceeded, false, Timeout, "timeout"},
		{"ssrf", &net.OpError{Op: "dial", Err: ErrBlocked}, false, Permanent, "ssrf_blocked"},
		{"dns", &net.DNSError{Err: "no such host", IsNotFound: true}, false, Retryable, "dns"},
		{"other", errors.New("weird"), false, Retryable, "network"},
	}
	for _, tc := range cases {
		got := Classify(nil, tc.err, tc.parentDone)
		if got.Outcome != tc.want || got.Class != tc.class {
			t.Errorf("%s: %s/%s, want %s/%s", tc.name, got.Outcome, got.Class, tc.want, tc.class)
		}
	}
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cases := map[string]time.Duration{
		"":        0,
		"30":      30 * time.Second,
		"0":       time.Second, // clamped up: never a hot loop
		"-5":      time.Second,
		"9999999": time.Hour, // clamped down: never wedge a stream for a week
		now.Add(2 * time.Minute).Format(http.TimeFormat): 2 * time.Minute,
		"garbage": 0,
	}
	for in, want := range cases {
		if got := parseRetryAfter(in, now); got != want {
			t.Errorf("parseRetryAfter(%q) = %s, want %s", in, got, want)
		}
	}
}

// ── the client end to end ────────────────────────────────────────────────────

func loopback() *Guard {
	g, _ := NewGuard([]string{"127.0.0.0/8", "::1/128"})
	return g
}

func req(url string) Request {
	return Request{
		URL: url, Timeout: 2 * time.Second, EventID: uuid.Must(uuid.NewV7()), DeliveryID: uuid.Must(uuid.NewV7()),
		EventType: "order.created", Attempt: 3, Body: []byte(`{"z":1,"a":2}`), KeyID: "k_1", Secret: []byte("s3cret"),
	}
}

func TestDeliverSendsExactBytesAndValidHeaders(t *testing.T) {
	r := req("")
	var got *http.Request
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, hr *http.Request) {
		got = hr
		gotBody, _ = io.ReadAll(hr.Body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	r.URL = srv.URL

	resp := NewClient(loopback()).Deliver(context.Background(), r)
	if resp.Outcome != Success {
		t.Fatalf("outcome = %s (%v)", resp.Outcome, resp.Err)
	}
	if !bytes.Equal(gotBody, r.Body) {
		t.Errorf("body on the wire = %s, want the exact stored bytes %s", gotBody, r.Body)
	}
	h := got.Header
	if h.Get("Ithaca-Id") != r.EventID.String() || h.Get("Ithaca-Delivery-Id") != r.DeliveryID.String() ||
		h.Get("Ithaca-Attempt") != "3" || h.Get("Ithaca-Event-Type") != "order.created" {
		t.Errorf("missing or wrong Ithaca-* headers: %v", h)
	}
	if !Verify(r.Secret, h.Get("Ithaca-Signature"), h.Get("Ithaca-Id"), h.Get("Ithaca-Timestamp"), gotBody, time.Now(), DefaultTolerance) {
		t.Error("the signature on the wire does not verify against the bytes on the wire")
	}
}

func TestDeliverDoesNotFollowRedirects(t *testing.T) {
	var hits int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, hr *http.Request) {
		hits++
		http.Redirect(w, hr, "/elsewhere", http.StatusFound)
	}))
	defer srv.Close()

	resp := NewClient(loopback()).Deliver(context.Background(), req(srv.URL))
	if resp.Outcome != Permanent || resp.Class != "redirect" || hits != 1 {
		t.Errorf("outcome %s/%s after %d hits; want permanent/redirect after exactly 1", resp.Outcome, resp.Class, hits)
	}
}

func TestDeliverTimeoutVersusCancel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, hr *http.Request) {
		// Consume the body first: net/http only watches for the client
		// hanging up (and cancels hr.Context()) once the body is read.
		_, _ = io.Copy(io.Discard, hr.Body)
		<-hr.Context().Done()
	}))
	defer srv.Close()
	c := NewClient(loopback())

	r := req(srv.URL)
	r.Timeout = 50 * time.Millisecond
	if resp := c.Deliver(context.Background(), r); resp.Outcome != Timeout {
		t.Errorf("endpoint too slow: outcome %s, want timeout", resp.Outcome)
	}

	// Our own cancellation (shutdown, lost lease) is not the endpoint's fault
	// and must not be recorded against it.
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)
	r.Timeout = 5 * time.Second
	if resp := c.Deliver(ctx, r); resp.Outcome != Canceled {
		t.Errorf("we canceled: outcome %s, want canceled", resp.Outcome)
	}
}

func TestDeliverHonorsRetryAfter(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, hr *http.Request) {
		w.Header().Set("Retry-After", "7")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()
	resp := NewClient(loopback()).Deliver(context.Background(), req(srv.URL))
	if resp.Outcome != RateLimited || resp.RetryAfter != 7*time.Second {
		t.Errorf("got %s retry-after %s", resp.Outcome, resp.RetryAfter)
	}
}
