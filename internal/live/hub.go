package live

import (
	"context"
	"encoding/json"
	"log/slog"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/SharmaSanskar/ithaca/internal/platform/metrics"
)

// SnapshotEndpoint is one endpoint as the hub sees it from Postgres.
type SnapshotEndpoint struct {
	ID        uuid.UUID
	Name      string
	URL       string
	State     string
	Circuit   string
	Depth     int64
	UpdatedAt time.Time
	Data      any // the full JSON shape sent in endpoint deltas
}

// Snapshot is the database-derived state of one tenant.
type Snapshot struct {
	Endpoints     []SnapshotEndpoint
	DLQ           int64
	PausedStreams int64
	Retrying      int64
}

// Snapshotter reads a tenant's current state from the source of truth.
type Snapshotter interface {
	Snapshot(ctx context.Context, tenant uuid.UUID) (Snapshot, error)
}

// Channels a client can subscribe to.
const (
	ChanFlow     = "flow"      // tenant frames + deltas
	ChanDemo     = "demo"      // narration
	chanEndpoint = "endpoint:" // + endpoint id: one endpoint's frames + deltas
)

// ValidChannel reports whether ch is subscribable.
func ValidChannel(ch string) bool {
	if ch == ChanFlow || ch == ChanDemo {
		return true
	}
	if id, ok := strings.CutPrefix(ch, chanEndpoint); ok {
		_, err := uuid.Parse(id)
		return err == nil
	}
	return false
}

// Hub merges live batches from every process and fans frames and deltas out
// to WebSocket clients.
type Hub struct {
	snap      Snapshotter
	log       *slog.Logger
	m         *hubMetrics
	frameTick time.Duration
	snapTick  time.Duration

	mu      sync.Mutex
	tenants map[uuid.UUID]*tenantState
	clients map[*client]struct{}
	seq     uint64
	rng     *rand.Rand
	pools   map[string]poolEntry // per dispatcher process
}

type poolEntry struct {
	stat PoolStat
	at   time.Time
}

// poolTTL drops a dispatcher that stopped reporting (scaled down, crashed),
// so the worker strip shrinks instead of showing ghosts.
const poolTTL = 3 * time.Second

type tenantState struct {
	win      Batch
	plain    int
	snap     Snapshot
	revs     map[uuid.UUID]int64
	watchers int
	// loaded: the first snapshot has arrived. No frame is emitted before it,
	// or a newly connected dashboard renders one empty graph (no endpoints)
	// before the real one: a visible flicker on every connect.
	loaded bool
	dirty  bool // an endpoint delta arrived: refresh the snapshot now
}

type client struct {
	tenant uuid.UUID
	box    *outbox
	subs   map[string]bool
	closed func() // closes the connection (slow consumer)
}

// NewHub builds a hub.
func NewHub(snap Snapshotter, log *slog.Logger, reg prometheus.Registerer) *Hub {
	return &Hub{
		snap: snap, log: log, m: newHubMetrics(reg),
		frameTick: window, snapTick: time.Second,
		tenants: map[uuid.UUID]*tenantState{},
		clients: map[*client]struct{}{},
		pools:   map[string]poolEntry{},
		rng:     rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64())), //nolint:gosec // sampling
	}
}

// Run consumes the bus and emits frames until ctx is done.
func (h *Hub) Run(ctx context.Context, in <-chan []byte) error {
	frames := time.NewTicker(h.frameTick)
	defer frames.Stop()
	snaps := time.NewTicker(h.snapTick)
	defer snaps.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-in:
			if !ok {
				return nil
			}
			h.ingest(msg)
		case <-frames.C:
			h.emitFrames()
			h.refreshDirty(ctx)
		case <-snaps.C:
			h.refreshAll(ctx)
		}
	}
}

// ingest merges one bus message.
func (h *Hub) ingest(msg []byte) {
	var e Envelope
	if err := json.Unmarshal(msg, &e); err != nil {
		h.log.Warn("bad live message", "error", err)
		return
	}
	h.m.received.WithLabelValues(e.Kind).Inc()
	h.mu.Lock()
	defer h.mu.Unlock()
	switch e.Kind {
	case "batch":
		if e.Batch != nil {
			h.merge(h.tenant(e.Tenant), e.Batch)
		}
	case "delta":
		if e.Delta == nil {
			return
		}
		if e.Delta.Kind == DeltaEndpoint {
			// Re-read from Postgres rather than trusting the message: the
			// delta then carries real, current state with a revision.
			h.tenant(e.Tenant).dirty = true
			return
		}
		var data any = e.Delta.Data
		h.broadcastDelta(e.Tenant, nil, DeltaMsg{Type: "delta", Kind: e.Delta.Kind, ID: e.Delta.ID, Data: data})
	case "pool":
		if e.Pool != nil {
			h.pools[e.Pool.Process] = poolEntry{stat: *e.Pool, at: time.Now()}
		}
	case "narration":
		if e.Narration == nil {
			return
		}
		b, _ := json.Marshal(NarrationMsg{Type: "narration", Ch: ChanDemo, Narration: *e.Narration})
		for c := range h.clients {
			if c.tenant == e.Tenant && c.subs[ChanDemo] {
				h.deliverDelta(c, b) // narration is never dropped either
			}
		}
	}
}

func (h *Hub) tenant(id uuid.UUID) *tenantState {
	t := h.tenants[id]
	if t == nil {
		t = &tenantState{revs: map[uuid.UUID]int64{}}
		h.tenants[id] = t
	}
	return t
}

// merge folds a process's batch into the tenant's current window, applying the
// same priority sampling as the publisher across processes.
func (h *Hub) merge(t *tenantState, b *Batch) {
	t.win.Ingested += b.Ingested
	if len(b.Endpoints) > 0 && t.win.Endpoints == nil {
		t.win.Endpoints = map[uuid.UUID]*EndpointWindow{}
	}
	for id, w := range b.Endpoints {
		cur := t.win.Endpoints[id]
		if cur == nil {
			cur = &EndpointWindow{}
			t.win.Endpoints[id] = cur
		}
		cur.OK += w.OK
		cur.Retry += w.Retry
		cur.Dead += w.Dead
		cur.RateLimited += w.RateLimited
		for _, l := range w.Latencies {
			cur.latSeen++
			if len(cur.Latencies) < maxLatencies*4 {
				cur.Latencies = append(cur.Latencies, l)
			} else if j := h.rng.IntN(cur.latSeen); j < len(cur.Latencies) {
				cur.Latencies[j] = l
			}
		}
	}
	for _, s := range b.Samples {
		if s.priority() {
			if countPrio(t.win.Samples) < maxPrioSamples {
				t.win.Samples = append(t.win.Samples, s)
			}
			continue
		}
		t.plain++
		if len(t.win.Samples)-countPrio(t.win.Samples) < maxPlainSamples {
			t.win.Samples = append(t.win.Samples, s)
		}
	}
}

// emitFrames builds one frame per watched tenant (and per watched endpoint
// channel), encodes each ONCE, and offers it to every subscriber.
func (h *Hub) emitFrames() {
	h.mu.Lock()
	defer h.mu.Unlock()
	now := time.Now().UnixMilli()
	pool := h.fleetPool()
	for id, t := range h.tenants {
		win := t.win
		t.win, t.plain = Batch{}, 0
		if t.watchers == 0 || !t.loaded {
			continue
		}
		h.seq++
		f := h.buildFrame(t, win, now)
		f.Seq = h.seq
		f.Pool = pool

		f.Ch = ChanFlow
		flow, _ := json.Marshal(f)
		perEndpoint := map[string][]byte{}
		for c := range h.clients {
			if c.tenant != id {
				continue
			}
			for ch := range c.subs {
				var b []byte
				switch {
				case ch == ChanFlow:
					b = flow
				case strings.HasPrefix(ch, chanEndpoint):
					if b = perEndpoint[ch]; b == nil {
						b = encodeEndpointFrame(f, ch)
						perEndpoint[ch] = b
					}
				default:
					continue
				}
				if c.box.offerFrame(ch, b) {
					h.m.superseded.Inc()
				}
				h.m.frames.Inc()
			}
		}
	}
}

// fleetPool sums the live dispatchers. Caller holds h.mu.
func (h *Hub) fleetPool() Pool {
	var p Pool
	for proc, e := range h.pools {
		if time.Since(e.at) > poolTTL {
			delete(h.pools, proc)
			continue
		}
		p.Workers += e.stat.Workers
		p.Busy += e.stat.Busy
		p.Processes++
	}
	return p
}

func (h *Hub) buildFrame(t *tenantState, win Batch, now int64) Frame {
	f := Frame{Type: "frame", T: now, WindowMS: int(h.frameTick.Milliseconds()), Ingested: win.Ingested,
		Samples: win.Samples, Endpoints: make([]FrameEndpoint, 0, len(t.snap.Endpoints))}
	if f.Samples == nil {
		f.Samples = []Sample{}
	}
	var pending int64
	for _, e := range t.snap.Endpoints {
		fe := FrameEndpoint{ID: e.ID, Name: e.Name, State: e.State, Circuit: e.Circuit, Depth: e.Depth}
		if w := win.Endpoints[e.ID]; w != nil {
			fe.OK, fe.Retry, fe.Dead, fe.RateLimited = w.OK, w.Retry, w.Dead, w.RateLimited
			fe.P50MS, fe.P99MS = percentiles(w.Latencies)
		}
		pending += e.Depth
		f.Endpoints = append(f.Endpoints, fe)
	}
	f.Totals = Totals{Pending: pending, Retrying: t.snap.Retrying, DLQ: t.snap.DLQ, PausedStreams: t.snap.PausedStreams}
	return f
}

// encodeEndpointFrame is the server-side projection for an endpoint channel:
// a client watching one endpoint never receives the firehose.
func encodeEndpointFrame(f Frame, ch string) []byte {
	id, _ := uuid.Parse(strings.TrimPrefix(ch, chanEndpoint))
	out := f
	out.Ch = ch
	out.Endpoints = nil
	for _, e := range f.Endpoints {
		if e.ID == id {
			out.Endpoints = []FrameEndpoint{e}
		}
	}
	if out.Endpoints == nil {
		out.Endpoints = []FrameEndpoint{}
	}
	out.Samples = []Sample{}
	for _, s := range f.Samples {
		if s.EndpointID != nil && *s.EndpointID == id {
			out.Samples = append(out.Samples, s)
		}
	}
	b, _ := json.Marshal(out)
	return b
}

func percentiles(lat []int32) (p50, p99 int) {
	if len(lat) == 0 {
		return 0, 0
	}
	s := slices.Clone(lat)
	slices.Sort(s)
	return int(s[len(s)/2]), int(s[min(len(s)-1, len(s)*99/100)])
}

func (h *Hub) refreshDirty(ctx context.Context) {
	h.mu.Lock()
	var ids []uuid.UUID
	for id, t := range h.tenants {
		if t.dirty && t.watchers > 0 {
			ids = append(ids, id)
			t.dirty = false
		}
	}
	h.mu.Unlock()
	for _, id := range ids {
		h.refresh(ctx, id)
	}
}

func (h *Hub) refreshAll(ctx context.Context) {
	h.mu.Lock()
	var ids []uuid.UUID
	for id, t := range h.tenants {
		if t.watchers > 0 {
			ids = append(ids, id)
		}
	}
	h.mu.Unlock()
	for _, id := range ids {
		h.refresh(ctx, id)
	}
}

// refresh re-reads a tenant from Postgres and emits a delta for every
// endpoint whose revision moved. This is what makes endpoint deltas reliable
// on top of a lossy bus.
func (h *Hub) refresh(ctx context.Context, tenant uuid.UUID) {
	qctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	s, err := h.snap.Snapshot(qctx, tenant)
	if err != nil {
		if ctx.Err() == nil {
			h.log.Warn("live snapshot failed", "tenant", tenant, "error", err)
		}
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	t := h.tenant(tenant)
	first := !t.loaded
	t.snap, t.loaded = s, true
	for _, e := range s.Endpoints {
		rev := e.UpdatedAt.UnixMicro()
		old, seen := t.revs[e.ID]
		t.revs[e.ID] = rev
		if first || (seen && old == rev) {
			continue
		}
		id := e.ID
		h.broadcastDelta(tenant, &id, DeltaMsg{Type: "delta", Kind: DeltaEndpoint, ID: e.ID.String(), Rev: rev, Data: e.Data})
	}
}

// broadcastDelta sends a delta to every subscriber of the tenant's flow
// channel and, when it concerns one endpoint, to that endpoint's channel.
// Caller holds h.mu.
func (h *Hub) broadcastDelta(tenant uuid.UUID, endpoint *uuid.UUID, d DeltaMsg) {
	var epCh string
	if endpoint != nil {
		epCh = chanEndpoint + endpoint.String()
	}
	for c := range h.clients {
		if c.tenant != tenant {
			continue
		}
		switch {
		case c.subs[ChanFlow]:
			d.Ch = ChanFlow
		case epCh != "" && c.subs[epCh]:
			d.Ch = epCh
		default:
			continue
		}
		b, _ := json.Marshal(d)
		h.deliverDelta(c, b)
	}
}

func (h *Hub) deliverDelta(c *client, b []byte) {
	if !c.box.offerDelta(b) {
		h.m.slowDisconnects.Inc()
		go c.closed()
		return
	}
	h.m.deltas.Inc()
}

// ── client registry (used by the WebSocket handler) ─────────────────────────

func (h *Hub) register(c *client) {
	h.mu.Lock()
	h.clients[c] = struct{}{}
	h.mu.Unlock()
	h.m.connections.Inc()
}

func (h *Hub) unregister(c *client) {
	h.mu.Lock()
	if _, ok := h.clients[c]; ok {
		delete(h.clients, c)
		if c.subs[ChanFlow] || hasEndpointSub(c.subs) {
			h.tenant(c.tenant).watchers--
		}
	}
	h.mu.Unlock()
	h.m.connections.Dec()
}

// subscribe adds a channel. The first frame-channel subscription makes the
// tenant "watched": its snapshot is refreshed immediately so the very first
// frame already has endpoints and depth.
func (h *Hub) subscribe(ctx context.Context, c *client, ch string) {
	h.mu.Lock()
	wasWatching := c.subs[ChanFlow] || hasEndpointSub(c.subs)
	c.subs[ch] = true
	watching := c.subs[ChanFlow] || hasEndpointSub(c.subs)
	t := h.tenant(c.tenant)
	needRefresh := false
	if watching && !wasWatching {
		t.watchers++
		needRefresh = !t.loaded
	}
	h.mu.Unlock()
	if needRefresh {
		h.refresh(ctx, c.tenant)
	}
}

func (h *Hub) unsubscribe(c *client, ch string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	wasWatching := c.subs[ChanFlow] || hasEndpointSub(c.subs)
	delete(c.subs, ch)
	if wasWatching && !c.subs[ChanFlow] && !hasEndpointSub(c.subs) {
		h.tenant(c.tenant).watchers--
	}
}

func hasEndpointSub(subs map[string]bool) bool {
	for ch := range subs {
		if strings.HasPrefix(ch, chanEndpoint) {
			return true
		}
	}
	return false
}

type hubMetrics struct {
	connections     prometheus.Gauge
	received        *prometheus.CounterVec
	frames          prometheus.Counter
	superseded      prometheus.Counter
	deltas          prometheus.Counter
	slowDisconnects prometheus.Counter
}

func newHubMetrics(reg prometheus.Registerer) *hubMetrics {
	ns := metrics.Namespace
	m := &hubMetrics{
		connections: prometheus.NewGauge(prometheus.GaugeOpts{Namespace: ns, Subsystem: "ws", Name: "connections", Help: "Open WebSocket connections."}),
		received: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Subsystem: "live", Name: "messages_received_total",
			Help: "Live bus messages received by kind."}, []string{"kind"}),
		frames: prometheus.NewCounter(prometheus.CounterOpts{Namespace: ns, Subsystem: "ws", Name: "frames_offered_total", Help: "Frames offered to clients."}),
		superseded: prometheus.NewCounter(prometheus.CounterOpts{Namespace: ns, Subsystem: "ws", Name: "frames_superseded_total",
			Help: "Unsent frames replaced by a newer one (a slow client). Expected under load; costs nothing but frame rate."}),
		deltas: prometheus.NewCounter(prometheus.CounterOpts{Namespace: ns, Subsystem: "ws", Name: "deltas_sent_total", Help: "Deltas queued to clients."}),
		slowDisconnects: prometheus.NewCounter(prometheus.CounterOpts{Namespace: ns, Subsystem: "ws", Name: "slow_disconnects_total",
			Help: "Clients disconnected because their delta queue overflowed."}),
	}
	reg.MustRegister(m.connections, m.received, m.frames, m.superseded, m.deltas, m.slowDisconnects)
	return m
}
