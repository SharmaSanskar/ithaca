// Package verify runs the endpoint verification handshake for endpoints in
// state pending_verification (see deliver.Challenge for the protocol).
package verify

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/SharmaSanskar/ithaca/internal/deliver"
	"github.com/SharmaSanskar/ithaca/internal/platform/metrics"
	"github.com/SharmaSanskar/ithaca/internal/store"
	"github.com/SharmaSanskar/ithaca/internal/store/gen"
)

// Schedule is the wait after each failed attempt; its length is the attempt
// budget. A typo'd URL fails fast, a receiver deployed a few minutes after the
// endpoint was registered still gets verified.
var Schedule = []time.Duration{10 * time.Second, 30 * time.Second, 2 * time.Minute, 10 * time.Minute, 30 * time.Minute}

// Challenger is the HTTP side; *deliver.Client satisfies it.
type Challenger interface {
	Challenge(ctx context.Context, r deliver.ChallengeRequest) error
}

// Verifier is the periodic verification loop.
type Verifier struct {
	Store    *store.Store
	Client   Challenger
	Log      *slog.Logger
	Every    time.Duration
	Schedule []time.Duration

	results *prometheus.CounterVec
}

// New builds a verifier and registers its metric.
func New(s *store.Store, c Challenger, log *slog.Logger, reg prometheus.Registerer) *Verifier {
	v := &Verifier{
		Store: s, Client: c, Log: log, Every: 2 * time.Second, Schedule: Schedule,
		results: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metrics.Namespace, Subsystem: "endpoint", Name: "verifications_total",
			Help: "Endpoint verification attempts by result.",
		}, []string{"result"}),
	}
	reg.MustRegister(v.results)
	return v
}

// Run verifies pending endpoints until ctx is done.
func (v *Verifier) Run(ctx context.Context) error {
	t := time.NewTicker(v.Every)
	defer t.Stop()
	for {
		v.Tick(ctx)
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

// Tick claims due endpoints and challenges them concurrently. Exported for
// tests.
func (v *Verifier) Tick(ctx context.Context) {
	due, err := v.Store.ClaimEndpointsToVerify(ctx, 16)
	if err != nil {
		if ctx.Err() == nil {
			v.Log.Warn("claim endpoints to verify failed", "error", err)
		}
		return
	}
	var wg sync.WaitGroup
	for _, ep := range due {
		wg.Go(func() { v.verifyOne(ctx, ep) })
	}
	wg.Wait()
}

func (v *Verifier) verifyOne(ctx context.Context, ep gen.ClaimEndpointsToVerifyRow) {
	err := v.Client.Challenge(ctx, deliver.ChallengeRequest{
		URL: ep.Url, EndpointID: ep.ID, KeyID: ep.SigningKeyID, Secret: ep.SigningSecret,
		Timeout: time.Duration(ep.TimeoutMs) * time.Millisecond,
	})
	if err == nil {
		if merr := v.Store.MarkEndpointVerified(ctx, ep.ID); merr != nil {
			v.Log.Error("mark endpoint verified", "endpoint", ep.ID, "error", merr)
			return
		}
		v.results.WithLabelValues("verified").Inc()
		v.Log.Info("endpoint verified", "endpoint", ep.ID)
		return
	}
	if ctx.Err() != nil {
		return // shutting down; the claim lapses and someone retries later
	}

	attempt := int(ep.VerificationAttempts)
	retryIn := v.Schedule[min(attempt, len(v.Schedule)-1)]
	msg := err.Error()
	if rerr := v.Store.RecordVerificationFailure(ctx, gen.RecordVerificationFailureParams{
		ID: ep.ID, Error: &msg, RetryInMs: retryIn.Milliseconds(), MaxAttempts: int32(len(v.Schedule)), //nolint:gosec // small
	}); rerr != nil {
		v.Log.Error("record verification failure", "endpoint", ep.ID, "error", rerr)
		return
	}
	result := "retrying"
	if attempt+1 >= len(v.Schedule) {
		result = "failed"
	}
	v.results.WithLabelValues(result).Inc()
	v.Log.Warn("endpoint verification failed", "endpoint", ep.ID, "attempt", attempt+1, "result", result, "error", msg)
}
