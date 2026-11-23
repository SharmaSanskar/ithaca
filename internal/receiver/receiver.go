// Package receiver is a programmable webhook subscriber: the most valuable
// test asset in the repo. Every interesting delivery behavior needs a
// badly-behaved endpoint to provoke it.
//
// Each sink (/sink/{id}) has a Behavior: latency, failure rate, fail-the-
// first-N-attempts, hang, rate limiting. The receiver records every request
// and verifies signatures when it knows the sink's secret.
//
// The same type backs the in-process test receiver and the `receiver-sim`
// service used by the demo and load tests.
package receiver

import (
	"context"
	"encoding/json"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/SharmaSanskar/ithaca/internal/deliver"
)

// Behavior scripts how a sink responds.
type Behavior struct {
	Status     int     `json:"status"`      // success status, default 200
	LatencyMS  int     `json:"latency_ms"`  // delay before responding
	FailRate   float64 `json:"fail_rate"`   // probability [0,1] of answering FailStatus
	FailStatus int     `json:"fail_status"` // default 503
	FailFirst  int     `json:"fail_first"`  // fail the first N attempts of EVERY delivery
	Hang       bool    `json:"hang"`        // never answer (until the client gives up)
	RetryAfter int     `json:"retry_after"` // seconds; sent with a 429 FailStatus
}

// Received is one recorded request.
type Received struct {
	At             time.Time
	Sink           string
	EventID        string
	DeliveryID     string
	Attempt        int
	Body           []byte
	Status         int   // what we answered; 0 = client went away first
	SignatureValid *bool // nil when the sink has no known secret
}

// Receiver is safe for concurrent use.
type Receiver struct {
	mu       sync.Mutex
	def      Behavior
	sinks    map[string]Behavior
	secrets  map[string][]byte
	attempts map[string]int // per delivery id, for FailFirst
	rng      *rand.Rand
	log      []Received
	keepLog  bool
	counts   map[string]*SinkStats
}

// SinkStats is a per-sink running tally (kept even when the log is off).
type SinkStats struct {
	Requests  int64 `json:"requests"`
	Succeeded int64 `json:"succeeded"`
	Failed    int64 `json:"failed"`
}

// New creates a receiver. The seed makes failure injection reproducible: a
// flaky test that fails can be re-run with exactly the same failures.
func New(seed uint64, keepLog bool) *Receiver {
	return &Receiver{
		def:      Behavior{Status: http.StatusOK},
		sinks:    map[string]Behavior{},
		secrets:  map[string][]byte{},
		attempts: map[string]int{},
		rng:      rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)), //nolint:gosec // test fixture
		keepLog:  keepLog,
		counts:   map[string]*SinkStats{},
	}
}

// SetDefault sets the behavior for sinks without their own.
func (r *Receiver) SetDefault(b Behavior) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.def = b
}

// SetSink sets one sink's behavior.
func (r *Receiver) SetSink(id string, b Behavior) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sinks[id] = b
}

// SetSecret enables signature verification for a sink.
func (r *Receiver) SetSecret(id string, secret []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.secrets[id] = secret
}

// Received returns a copy of the request log.
func (r *Receiver) Received() []Received {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Received(nil), r.log...)
}

// Delivered returns how many 2xx answers each delivery id got. A delivery the
// database marks succeeded MUST appear here, or it is a phantom ack.
func (r *Receiver) Delivered() map[string]int {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := map[string]int{}
	for _, rec := range r.log {
		if rec.Status >= 200 && rec.Status < 300 {
			out[rec.DeliveryID]++
		}
	}
	return out
}

// Handler serves the sink and control routes.
//
//	POST /sink/{id}            the webhook endpoint
//	GET  /sink/{id}/behavior   current script
//	PUT  /sink/{id}/behavior   replace the script
//	GET  /sink/{id}/stats      running tally
func (r *Receiver) Handler() http.Handler {
	mux := chi.NewRouter()
	r.Mount(mux)
	return mux
}

// Mount registers the routes on an existing router.
func (r *Receiver) Mount(mux chi.Router) {
	mux.Post("/sink/{id}", r.serveSink)
	mux.Get("/sink/{id}/behavior", func(w http.ResponseWriter, req *http.Request) {
		writeJSON(w, http.StatusOK, r.behavior(chi.URLParam(req, "id")))
	})
	mux.Put("/sink/{id}/behavior", func(w http.ResponseWriter, req *http.Request) {
		var b Behavior
		if err := json.NewDecoder(io.LimitReader(req.Body, 1<<16)).Decode(&b); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		r.SetSink(chi.URLParam(req, "id"), b)
		writeJSON(w, http.StatusOK, b)
	})
	mux.Get("/sink/{id}/stats", func(w http.ResponseWriter, req *http.Request) {
		r.mu.Lock()
		st := SinkStats{}
		if c := r.counts[chi.URLParam(req, "id")]; c != nil {
			st = *c
		}
		r.mu.Unlock()
		writeJSON(w, http.StatusOK, st)
	})
}

func (r *Receiver) behavior(id string) Behavior {
	r.mu.Lock()
	defer r.mu.Unlock()
	if b, ok := r.sinks[id]; ok {
		return b
	}
	return r.def
}

func (r *Receiver) serveSink(w http.ResponseWriter, req *http.Request) {
	sink := chi.URLParam(req, "id")
	body, _ := io.ReadAll(io.LimitReader(req.Body, 2<<20))
	rec := Received{
		At:         time.Now(),
		Sink:       sink,
		EventID:    req.Header.Get("Ithaca-Id"),
		DeliveryID: req.Header.Get("Ithaca-Delivery-Id"),
		Body:       body,
	}
	rec.Attempt, _ = strconv.Atoi(req.Header.Get("Ithaca-Attempt"))

	// Decide the response under the lock (rng and counters are shared), then
	// sleep / hang outside it.
	r.mu.Lock()
	b, ok := r.sinks[sink]
	if !ok {
		b = r.def
	}
	if secret, ok := r.secrets[sink]; ok {
		valid := deliver.Verify(secret, req.Header.Get("Ithaca-Signature"), rec.EventID,
			req.Header.Get("Ithaca-Timestamp"), body, time.Now(), deliver.DefaultTolerance)
		rec.SignatureValid = &valid
	}
	r.attempts[rec.DeliveryID]++
	seen := r.attempts[rec.DeliveryID]
	status := b.Status
	if status == 0 {
		status = http.StatusOK
	}
	failStatus := b.FailStatus
	if failStatus == 0 {
		failStatus = http.StatusServiceUnavailable
	}
	if seen <= b.FailFirst || (b.FailRate > 0 && r.rng.Float64() < b.FailRate) {
		status = failStatus
	}
	if rec.SignatureValid != nil && !*rec.SignatureValid {
		status = http.StatusUnauthorized // what a correct subscriber does
	}
	r.mu.Unlock()

	switch {
	case b.Hang:
		<-req.Context().Done()
		status = 0
	case b.LatencyMS > 0:
		if !sleepCtx(req.Context(), time.Duration(b.LatencyMS)*time.Millisecond) {
			status = 0
		}
	}

	if status != 0 {
		if status == http.StatusTooManyRequests && b.RetryAfter > 0 {
			w.Header().Set("Retry-After", strconv.Itoa(b.RetryAfter))
		}
		w.WriteHeader(status)
	}

	rec.Status = status
	r.mu.Lock()
	st := r.counts[sink]
	if st == nil {
		st = &SinkStats{}
		r.counts[sink] = st
	}
	st.Requests++
	if status >= 200 && status < 300 {
		st.Succeeded++
	} else {
		st.Failed++
	}
	if r.keepLog {
		r.log = append(r.log, rec)
	}
	r.mu.Unlock()
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
