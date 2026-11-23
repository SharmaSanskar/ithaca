package worker

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/SharmaSanskar/ithaca/internal/deliver"
	"github.com/SharmaSanskar/ithaca/internal/queue"
	"github.com/SharmaSanskar/ithaca/internal/retry"
)

type fakeSender struct{ resp deliver.Response }

func (f fakeSender) Deliver(context.Context, deliver.Request) deliver.Response { return f.resp }

func execOnce(t *testing.T, resp deliver.Response, priorAttempts int32) (queue.Result, bool) {
	t.Helper()
	policy := retry.Policy{Base: time.Second, Multiplier: 2, Cap: time.Minute, MaxAttempts: 5}
	e := NewExecutor(fakeSender{resp}, policy, NewMetrics(prometheus.NewRegistry(), nil))
	l := &queue.Lease{EndpointID: uuid.New(), Endpoint: queue.Endpoint{Timeout: time.Second}}
	m := queue.Message{DeliveryID: uuid.New(), EventID: uuid.New(), Attempt: priorAttempts, EventCreatedAt: time.Now()}
	return e.Execute(context.Background(), l, m)
}

func outcome(o deliver.Outcome, class string, status int) deliver.Response {
	return deliver.Response{Classification: deliver.Classification{Outcome: o, Class: class}, StatusCode: status}
}

func TestExecutorDecisions(t *testing.T) {
	cases := []struct {
		name         string
		resp         deliver.Response
		prior        int32
		disposition  queue.Disposition
		attemptsLeft int32 // expected Result.Attempts
		deadReason   string
	}{
		{"success", outcome(deliver.Success, "", 200), 0, queue.Succeeded, 1, ""},
		{"5xx retries", outcome(deliver.Retryable, "http_5xx", 503), 0, queue.Retry, 1, ""},
		{"timeout retries", outcome(deliver.Timeout, "timeout", 0), 2, queue.Retry, 3, ""},
		{"5xx on the last attempt dies", outcome(deliver.Retryable, "http_5xx", 503), 4, queue.Dead, 5, "max_attempts:http_5xx"},
		{"4xx dies immediately", outcome(deliver.Permanent, "http_4xx", 404), 0, queue.Dead, 1, "http_4xx"},
		{"ssrf dies immediately", outcome(deliver.Permanent, "ssrf_blocked", 0), 0, queue.Dead, 1, "ssrf_blocked"},
		{"dns gives up after 3", outcome(deliver.Retryable, "dns", 0), 2, queue.Dead, 3, "dns"},
		{"dns retries before 3", outcome(deliver.Retryable, "dns", 0), 1, queue.Retry, 2, ""},
		{"tls gives up after 4", outcome(deliver.Retryable, "tls", 0), 3, queue.Dead, 4, "tls"},
		// The key rule: rate limiting does NOT consume an attempt.
		{"429 defers without consuming", outcome(deliver.RateLimited, "http_429", 429), 2, queue.Defer, 2, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, ok := execOnce(t, tc.resp, tc.prior)
			if !ok {
				t.Fatal("abandoned")
			}
			if r.Disposition != tc.disposition || r.Attempts != tc.attemptsLeft || r.DeadReason != tc.deadReason {
				t.Errorf("got disposition %v attempts %d reason %q; want %v %d %q",
					r.Disposition, r.Attempts, r.DeadReason, tc.disposition, tc.attemptsLeft, tc.deadReason)
			}
			if r.Record == nil || r.Record.Attempt != tc.prior+1 {
				t.Errorf("attempt record = %+v, want attempt %d", r.Record, tc.prior+1)
			}
		})
	}
}

func TestExecutorRetryDelayFollowsPolicy(t *testing.T) {
	r, _ := execOnce(t, outcome(deliver.Retryable, "http_5xx", 500), 2) // third attempt fails
	// policy base 1s ×2: after 3 failures the window is [0.5s, 4s].
	if r.Delay < 500*time.Millisecond || r.Delay > 4*time.Second {
		t.Errorf("delay %s outside the jitter window [0.5s, 4s]", r.Delay)
	}
}

func TestExecutorUsesRetryAfterWhenGiven(t *testing.T) {
	resp := outcome(deliver.RateLimited, "http_429", 429)
	resp.RetryAfter = 42 * time.Second
	r, _ := execOnce(t, resp, 0)
	if r.Delay != 42*time.Second {
		t.Errorf("delay = %s, want the subscriber's Retry-After of 42s", r.Delay)
	}
}

func TestExecutorCanceledIsNotAVerdict(t *testing.T) {
	if _, ok := execOnce(t, outcome(deliver.Canceled, "canceled", 0), 0); ok {
		t.Error("a canceled attempt produced a result to ack")
	}
}
