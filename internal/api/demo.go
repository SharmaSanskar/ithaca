package api

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/redis/go-redis/v9"

	"github.com/SharmaSanskar/ithaca/internal/demo"
	"github.com/SharmaSanskar/ithaca/internal/platform/httpx"
	"github.com/SharmaSanskar/ithaca/internal/receiver"
)

// DemoControls serves /demo: the public demo's buttons. Every action becomes
// a command for the demo runtime (in the worker service); the API itself
// never touches sinks or generates traffic.
type DemoControls struct {
	Redis     *redis.Client
	PerMinute int
	// Namespace must match the runtime's (demo.Keys). "" in production.
	Namespace string
}

// Routes mounts /demo.
func (d *DemoControls) Routes(r chi.Router) {
	r.Route("/demo", func(r chi.Router) {
		r.Get("/state", d.state)
		r.Group(func(r chi.Router) {
			r.Use(d.RateLimit)
			r.Post("/scenarios/{name}", d.scenario)
			r.Post("/stop", d.stop)
			r.Post("/traffic", d.traffic)
			r.Put("/sinks/{sink}", d.sink)
		})
	})
}

var errRateLimited = httpx.ErrRateLimited.WithMessage("Slow down: the public demo allows a limited number of actions per minute.")

// RateLimit is a fixed-window limiter per client IP in Redis: shared by every
// API replica, one INCR per request. (A fixed window can admit up to 2x the
// limit across a window boundary; for demo buttons that is fine, and it is far
// simpler than a sliding log.)
func (d *DemoControls) RateLimit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			ip = r.RemoteAddr
		}
		now := time.Now()
		key := fmt.Sprintf("ithaca:rl:demo:%s:%s:%d", d.Namespace, ip, now.Unix()/60)
		n, err := d.Redis.Incr(r.Context(), key).Result()
		if err != nil {
			// Fail CLOSED for a public write endpoint: no limiter, no actions.
			httpx.WriteError(w, r, httpx.ErrUnavailable.WithMessage("Demo controls are temporarily unavailable."))
			return
		}
		if n == 1 {
			d.Redis.Expire(r.Context(), key, 70*time.Second)
		}
		if n > int64(d.PerMinute) {
			w.Header().Set("Retry-After", strconv.Itoa(int(60-now.Unix()%60)))
			httpx.WriteError(w, r, errRateLimited)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (d *DemoControls) state(w http.ResponseWriter, r *http.Request) {
	b, err := d.Redis.Get(r.Context(), demo.Keys(d.Namespace).State).Bytes()
	if err == redis.Nil { //nolint:errorlint // sentinel from go-redis
		httpx.WriteJSON(w, http.StatusOK, demo.State{Scenarios: demo.Scenarios, Sinks: map[string]receiver.Behavior{}})
		return
	}
	if err != nil {
		httpx.WriteError(w, r, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(b)
}

func (d *DemoControls) send(w http.ResponseWriter, r *http.Request, c demo.Command) {
	b, _ := json.Marshal(c)
	n, err := d.Redis.Publish(r.Context(), demo.Keys(d.Namespace).Command, b).Result()
	if err != nil {
		httpx.WriteError(w, r, err)
		return
	}
	if n == 0 {
		httpx.WriteError(w, r, httpx.ErrUnavailable.WithMessage("The demo runtime is not running (is the worker service up?)."))
		return
	}
	httpx.WriteJSON(w, http.StatusAccepted, map[string]any{"accepted": c})
}

func (d *DemoControls) scenario(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	if _, ok := demo.ScenarioByName(name); !ok {
		httpx.WriteError(w, r, httpx.ErrNotFound.WithMessage("Unknown scenario."))
		return
	}
	d.send(w, r, demo.Command{Op: "scenario", Name: name})
}

func (d *DemoControls) stop(w http.ResponseWriter, r *http.Request) {
	d.send(w, r, demo.Command{Op: "stop"})
}

func (d *DemoControls) traffic(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Rate int `json:"rate"`
	}
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, r, err)
		return
	}
	if req.Rate < 0 || req.Rate > demo.MaxRate {
		httpx.WriteError(w, r, httpx.ErrBadRequest.WithMessage(fmt.Sprintf("rate must be 0-%d events/s.", demo.MaxRate)))
		return
	}
	d.send(w, r, demo.Command{Op: "traffic", Rate: req.Rate})
}

func (d *DemoControls) sink(w http.ResponseWriter, r *http.Request) {
	sink := chi.URLParam(r, "sink")
	known := false
	for _, s := range demo.Sinks {
		known = known || s.ID == sink
	}
	if !known {
		httpx.WriteError(w, r, httpx.ErrNotFound.WithMessage("Unknown sink."))
		return
	}
	var b receiver.Behavior
	if err := httpx.DecodeJSON(w, r, &b); err != nil {
		httpx.WriteError(w, r, err)
		return
	}
	b = demo.Clamp(b)
	d.send(w, r, demo.Command{Op: "sink", Sink: sink, Behavior: &b})
}
