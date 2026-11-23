package deliver

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"net/http"
	"strconv"
	"syscall"
	"time"
)

// Outcome is the verdict on one HTTP attempt. The string values are what is
// stored in delivery_attempts.outcome.
type Outcome string

const (
	// Success: 2xx. The delivery is done.
	Success Outcome = "success"
	// Retryable: worth trying again later (5xx, connection errors, ...).
	Retryable Outcome = "retryable"
	// Timeout: the endpoint did not answer within its timeout. Retryable.
	Timeout Outcome = "timeout"
	// Permanent: retrying cannot help (4xx config errors, redirects, SSRF).
	Permanent Outcome = "permanent"
	// RateLimited: 429. Rescheduled WITHOUT consuming an attempt; otherwise a
	// throttling subscriber burns the whole retry budget in seconds.
	RateLimited Outcome = "rate_limited"
	// Canceled: we abandoned the request (shutdown, lost lease). Not a verdict
	// on the endpoint at all, so it is never recorded or acked.
	Canceled Outcome = "canceled"
)

// Classification is Classify's result.
type Classification struct {
	Outcome    Outcome
	Class      string        // short machine-readable cause: "http_5xx", "timeout", "dns", ...
	RetryAfter time.Duration // from Retry-After on a 429, clamped; 0 if absent
}

// Classify maps an HTTP result to an outcome. parentDone reports whether the
// caller's context (not the per-request timeout) was canceled.
//
//	2xx                      success
//	3xx                      permanent  (redirects are never followed: SSRF + ordering hazard)
//	408, 425                 retryable
//	429                      rate_limited (honors Retry-After, consumes no attempt)
//	other 4xx                permanent  (a config problem on their side)
//	5xx                      retryable
//	timeout                  timeout    (retryable)
//	refused / reset / DNS    retryable  (DNS and TLS get attempt caps in the executor)
//	SSRF blocked             permanent
//	we canceled              canceled
func Classify(resp *http.Response, err error, parentDone bool) Classification {
	if err != nil {
		return classifyErr(err, parentDone)
	}
	code := resp.StatusCode
	switch {
	case code >= 200 && code < 300:
		return Classification{Outcome: Success}
	case code >= 300 && code < 400:
		return Classification{Outcome: Permanent, Class: "redirect"}
	case code == http.StatusTooManyRequests:
		return Classification{Outcome: RateLimited, Class: "http_429", RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After"), time.Now())}
	case code == http.StatusRequestTimeout || code == http.StatusTooEarly:
		return Classification{Outcome: Retryable, Class: "http_" + strconv.Itoa(code)}
	case code == http.StatusGone:
		return Classification{Outcome: Permanent, Class: "http_410"}
	case code >= 400 && code < 500:
		return Classification{Outcome: Permanent, Class: "http_4xx"}
	default:
		return Classification{Outcome: Retryable, Class: "http_5xx"}
	}
}

func classifyErr(err error, parentDone bool) Classification {
	if parentDone {
		return Classification{Outcome: Canceled, Class: "canceled"}
	}
	if errors.Is(err, ErrBlocked) {
		return Classification{Outcome: Permanent, Class: "ssrf_blocked"}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return Classification{Outcome: Timeout, Class: "timeout"}
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return Classification{Outcome: Timeout, Class: "timeout"}
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return Classification{Outcome: Retryable, Class: "dns"}
	}
	var certErr *tls.CertificateVerificationError
	var unknownAuth x509.UnknownAuthorityError
	var hostErr x509.HostnameError
	var invalid x509.CertificateInvalidError
	if errors.As(err, &certErr) || errors.As(err, &unknownAuth) || errors.As(err, &hostErr) || errors.As(err, &invalid) {
		return Classification{Outcome: Retryable, Class: "tls"}
	}
	switch {
	case errors.Is(err, syscall.ECONNREFUSED):
		return Classification{Outcome: Retryable, Class: "conn_refused"}
	case errors.Is(err, syscall.ECONNRESET), errors.Is(err, syscall.EPIPE):
		return Classification{Outcome: Retryable, Class: "conn_reset"}
	}
	return Classification{Outcome: Retryable, Class: "network"}
}

// Retry-After is clamped: a subscriber saying "wait 0s" must not cause a hot
// loop, and one saying "wait a week" must not wedge a stream.
const (
	minRetryAfter = time.Second
	maxRetryAfter = time.Hour
)

// parseRetryAfter accepts both forms the RFC allows: delta-seconds and an
// HTTP-date.
func parseRetryAfter(v string, now time.Time) time.Duration {
	if v == "" {
		return 0
	}
	var d time.Duration
	if secs, err := strconv.Atoi(v); err == nil {
		d = time.Duration(secs) * time.Second
	} else if t, err := http.ParseTime(v); err == nil {
		d = t.Sub(now)
	} else {
		return 0
	}
	return min(max(d, minRetryAfter), maxRetryAfter)
}
