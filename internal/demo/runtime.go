package demo

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/SharmaSanskar/ithaca/internal/dlq"
	"github.com/SharmaSanskar/ithaca/internal/ingest"
	"github.com/SharmaSanskar/ithaca/internal/live"
	"github.com/SharmaSanskar/ithaca/internal/receiver"
	"github.com/SharmaSanskar/ithaca/internal/store"
)

// Redis keys. Commands go API → runtime over pub/sub; the runtime publishes
// its state to a key with a TTL, so a dead leader's state disappears.
const (
	CommandChannel = "ithaca:demo:cmd"
	StateKey       = "ithaca:demo:state"
	stateTTL       = 10 * time.Second
	// leaderLockID is the Postgres advisory lock that makes exactly one worker
	// process the demo driver, however many replicas run.
	leaderLockID = 0x1_7aca_de30
)

// Command is what the demo API sends.
type Command struct {
	Op       string             `json:"op"` // "scenario" | "stop" | "traffic" | "sink"
	Name     string             `json:"name,omitempty"`
	Rate     int                `json:"rate,omitempty"`
	Sink     string             `json:"sink,omitempty"`
	Behavior *receiver.Behavior `json:"behavior,omitempty"`
}

// State is what the API serves at GET /demo/state.
type State struct {
	Leader      bool                         `json:"leader"`
	Scenario    *ActiveScenario              `json:"scenario"`
	TrafficRate int                          `json:"traffic_rate"`
	Sinks       map[string]receiver.Behavior `json:"sinks"`
	Scenarios   []Scenario                   `json:"scenarios"`
	UpdatedAt   time.Time                    `json:"updated_at"`
}

// ActiveScenario is the scenario currently running.
type ActiveScenario struct {
	Name      string    `json:"name"`
	Title     string    `json:"title"`
	Step      int       `json:"step"`
	Total     int       `json:"total"`
	StartedAt time.Time `json:"started_at"`
}

// Runtime drives the demo.
type Runtime struct {
	Store       *store.Store
	Redis       *redis.Client
	Publisher   *live.Publisher
	Ingest      *ingest.Service
	SinkBaseURL string
	AmbientRate int
	ReplayRate  int
	Log         *slog.Logger

	store      *store.Store
	sinks      *SinkClient
	gen        *Generator
	tenant     uuid.UUID
	log        *slog.Logger
	replayRate int

	mu     sync.Mutex
	active *ActiveScenario
	cancel context.CancelFunc
}

// Run elects a leader, then drives the demo until ctx is done. Non-leaders
// wait and retry, so if the leader dies another worker takes over within
// seconds.
func (rt *Runtime) Run(ctx context.Context) error {
	rt.store, rt.log, rt.replayRate = rt.Store, rt.Log, rt.ReplayRate
	rt.sinks = NewSinkClient(rt.SinkBaseURL)
	for {
		err := rt.lead(ctx)
		if ctx.Err() != nil {
			return nil //nolint:nilerr // shutting down: a lead() error caused by cancellation is not a failure
		}
		if err != nil {
			rt.log.Warn("demo runtime stopped; retrying", "error", err)
		}
		if !sleepCtx(ctx, 5*time.Second) {
			return nil
		}
	}
}

func (rt *Runtime) lead(ctx context.Context) error {
	conn, err := rt.Store.Pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	var got bool
	if qerr := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", leaderLockID).Scan(&got); qerr != nil {
		return qerr
	}
	if !got {
		return nil // someone else is driving
	}
	defer func() {
		_, _ = conn.Exec(context.WithoutCancel(ctx), "SELECT pg_advisory_unlock($1)", leaderLockID)
	}()

	tenant, err := TenantID(ctx, rt.Store)
	if err != nil {
		return err
	}
	rt.tenant = tenant
	// Start from a known state: every sink healthy with realistic latency.
	// receiver-sim's own default (instant 200s) would make the demo narrate
	// "p99 0ms", which is accurate on localhost and unbelievable everywhere.
	for _, sk := range Sinks {
		if err := rt.sinks.Set(ctx, sk.ID, Healthy); err != nil {
			rt.log.Warn("demo: could not reset sink (is receiver-sim up?)", "sink", sk.ID, "error", err)
		}
	}
	rt.gen = NewGenerator(rt.Ingest, tenant, rt.AmbientRate)
	rt.log.Info("demo runtime is the leader", "tenant", tenant, "ambient_rate", rt.AmbientRate)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = rt.gen.Run(ctx) }()

	cmds := rt.Redis.Subscribe(ctx, CommandChannel)
	defer func() { _ = cmds.Close() }()
	ch := cmds.Channel()
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	rt.publishState(ctx)
	for {
		select {
		case <-ctx.Done():
			rt.stopScenario()
			return nil
		case m, ok := <-ch:
			if !ok {
				return errors.New("command subscription closed")
			}
			var c Command
			if err := json.Unmarshal([]byte(m.Payload), &c); err != nil {
				rt.log.Warn("bad demo command", "error", err)
				continue
			}
			rt.handle(ctx, c)
			rt.publishState(ctx)
		case <-tick.C:
			rt.publishState(ctx)
		}
	}
}

func (rt *Runtime) handle(ctx context.Context, c Command) {
	switch c.Op {
	case "scenario":
		sc, ok := ScenarioByName(c.Name)
		if !ok {
			return
		}
		rt.startScenario(ctx, sc)
	case "stop":
		rt.stopScenario()
		rt.gen.SetRate(rt.AmbientRate)
	case "traffic":
		rt.gen.SetRate(c.Rate)
	case "sink":
		if c.Behavior == nil || !knownSink(c.Sink) {
			return
		}
		if err := rt.sinks.Set(ctx, c.Sink, Clamp(*c.Behavior)); err != nil {
			rt.log.Warn("demo: set sink failed", "sink", c.Sink, "error", err)
		}
	}
}

// startScenario runs one scenario at a time: starting a new one cancels the
// one in progress.
func (rt *Runtime) startScenario(parent context.Context, sc Scenario) {
	rt.stopScenario()
	ctx, cancel := context.WithCancel(parent)
	rt.mu.Lock()
	rt.active = &ActiveScenario{Name: sc.Name, Title: sc.Title, Total: sc.steps, StartedAt: time.Now()}
	rt.cancel = cancel
	rt.mu.Unlock()
	rt.log.Info("demo scenario started", "scenario", sc.Name)

	go func() {
		defer cancel()
		r := &run{rt: rt, scenario: sc}
		err := sc.run(ctx, r)
		switch {
		case errors.Is(err, context.Canceled):
			return // superseded or stopped; the next scenario narrates itself
		case errors.Is(err, errTimeout):
			rt.narrate(sc, r.step+1, "warn", "This step did not finish in time on this run; the system is still working, the demo just stopped waiting.", true)
		case err != nil:
			rt.log.Warn("demo scenario failed", "scenario", sc.Name, "error", err)
			rt.narrate(sc, r.step+1, "warn", "The scenario stopped early: "+err.Error(), true)
		default:
			rt.narrate(sc, r.step, "success", "", true)
		}
		rt.mu.Lock()
		if rt.active != nil && rt.active.Name == sc.Name {
			rt.active, rt.cancel = nil, nil
		}
		rt.mu.Unlock()
		rt.publishState(context.WithoutCancel(parent))
	}()
}

func (rt *Runtime) stopScenario() {
	rt.mu.Lock()
	cancel := rt.cancel
	rt.active, rt.cancel = nil, nil
	rt.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (rt *Runtime) narrate(sc Scenario, step int, level, text string, done bool) {
	rt.mu.Lock()
	if rt.active != nil && rt.active.Name == sc.Name {
		rt.active.Step = step
	}
	rt.mu.Unlock()
	if rt.Publisher != nil {
		rt.Publisher.Narrate(rt.tenant, live.Narration{
			Scenario: sc.Name, Step: step, Total: sc.steps, Text: text, Level: level, Done: done,
		})
	}
}

func (rt *Runtime) replay(ctx context.Context) (int, error) {
	r := &dlq.Replayer{Store: rt.Store, RatePerEndpoint: float64(rt.ReplayRate)}
	total := 0
	for {
		res, err := r.Replay(ctx, dlq.Request{TenantID: rt.tenant, Limit: dlq.MaxBatch})
		if err != nil {
			return total, err
		}
		total += res.Replayed
		if res.Replayed < dlq.MaxBatch {
			return total, nil
		}
	}
}

func (rt *Runtime) publishState(ctx context.Context) {
	st := State{Leader: true, TrafficRate: rt.gen.Rate(), Scenarios: Scenarios, Sinks: map[string]receiver.Behavior{}, UpdatedAt: time.Now()}
	rt.mu.Lock()
	if rt.active != nil {
		a := *rt.active
		st.Scenario = &a
	}
	rt.mu.Unlock()
	for _, s := range Sinks {
		if b, err := rt.sinks.Get(ctx, s.ID); err == nil {
			st.Sinks[s.ID] = b
		}
	}
	b, _ := json.Marshal(st)
	if err := rt.Redis.Set(ctx, StateKey, b, stateTTL).Err(); err != nil && ctx.Err() == nil {
		rt.log.Warn("publish demo state failed", "error", err)
	}
}

func knownSink(id string) bool {
	for _, s := range Sinks {
		if s.ID == id {
			return true
		}
	}
	return false
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
