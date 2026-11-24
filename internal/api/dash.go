package api

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/SharmaSanskar/ithaca/internal/auth"
	"github.com/SharmaSanskar/ithaca/internal/demo"
	"github.com/SharmaSanskar/ithaca/internal/live"
	"github.com/SharmaSanskar/ithaca/internal/platform/httpx"
	"github.com/SharmaSanskar/ithaca/internal/store"
	"github.com/SharmaSanskar/ithaca/internal/store/gen"
)

// Dashboard serves the public, read-only dashboard API for the demo tenant.
//
// There is no login: the public demo is read-only by construction. Every
// route here reads, except the demo actions, which are validated, clamped and
// rate-limited per IP. Management (creating endpoints, rotating keys) stays on
// the API-key-authenticated control plane.
type Dashboard struct {
	store *store.Store
	cp    *ControlPlane

	mu       sync.Mutex
	tenant   uuid.UUID
	resolved time.Time
}

// NewDashboard builds the dashboard API.
func NewDashboard(s *store.Store, cp *ControlPlane) *Dashboard { return &Dashboard{store: s, cp: cp} }

// DemoTenant resolves (and caches) the demo tenant.
func (d *Dashboard) DemoTenant(ctx context.Context) (uuid.UUID, error) {
	d.mu.Lock()
	if d.tenant != uuid.Nil {
		t := d.tenant
		d.mu.Unlock()
		return t, nil
	}
	d.mu.Unlock()
	t, err := demo.TenantID(ctx, d.store)
	if err != nil {
		return uuid.Nil, err
	}
	d.mu.Lock()
	d.tenant, d.resolved = t, time.Now()
	d.mu.Unlock()
	return t, nil
}

// asDemoTenant scopes a request to the demo tenant, so the control plane's
// own read handlers (endpoints, DLQ) are reused unchanged.
func (d *Dashboard) asDemoTenant(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t, err := d.DemoTenant(r.Context())
		if errors.Is(err, demo.ErrNotSeeded) {
			httpx.WriteError(w, r, httpx.ErrUnavailable.WithMessage(err.Error()))
			return
		}
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}
		next.ServeHTTP(w, r.WithContext(auth.WithTenant(r.Context(), t)))
	})
}

// Routes mounts /dash. replay is the rate-limited DLQ replay handler.
func (d *Dashboard) Routes(r chi.Router, limit func(http.Handler) http.Handler) {
	r.Route("/dash", func(r chi.Router) {
		r.Use(d.asDemoTenant)
		r.Get("/overview", d.overview)
		r.Get("/endpoints", d.cp.listEndpoints)
		r.Get("/events", d.events)
		r.Get("/events/{id}", d.event)
		r.Get("/series", d.series)
		r.Get("/dlq", d.cp.listDLQ)
		r.With(limit).Post("/dlq/replay", d.cp.replayDLQ)
	})
}

func (d *Dashboard) overview(w http.ResponseWriter, r *http.Request) {
	t := auth.TenantFrom(r.Context())
	snap, err := live.PGSnapshotter{Store: d.store}.Snapshot(r.Context(), t)
	if err != nil {
		httpx.WriteError(w, r, err)
		return
	}
	eps := make([]any, 0, len(snap.Endpoints))
	var pending int64
	for _, e := range snap.Endpoints {
		eps = append(eps, e.Data)
		pending += e.Depth
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"tenant":    demo.TenantName,
		"endpoints": eps,
		"totals":    live.Totals{Pending: pending, DLQ: snap.DLQ, PausedStreams: snap.PausedStreams},
	})
}

type eventRow struct {
	ID          uuid.UUID `json:"id"`
	Type        string    `json:"type"`
	OrderingKey *string   `json:"ordering_key,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	Pending     int64     `json:"pending"`
	Succeeded   int64     `json:"succeeded"`
	Dead        int64     `json:"dead"`
}

func (d *Dashboard) events(w http.ResponseWriter, r *http.Request) {
	p := gen.ListRecentEventsParams{TenantID: auth.TenantFrom(r.Context()), MaxRows: 50}
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 200 {
			httpx.WriteError(w, r, httpx.ErrBadRequest.WithMessage("limit must be 1-200."))
			return
		}
		p.MaxRows = int32(n) //nolint:gosec // bounded above
	}
	if v := r.URL.Query().Get("cursor"); v != "" {
		t, id, ok := decodeCursor(v)
		if !ok {
			httpx.WriteError(w, r, httpx.ErrBadRequest.WithMessage("Invalid cursor."))
			return
		}
		p.BeforeCreatedAt, p.BeforeID = &t, &id
	}
	rows, err := d.store.ListRecentEvents(r.Context(), p)
	if err != nil {
		httpx.WriteError(w, r, err)
		return
	}
	out := make([]eventRow, 0, len(rows))
	for _, e := range rows {
		out = append(out, eventRow{ID: e.ID, Type: e.EventType, OrderingKey: e.OrderingKey, CreatedAt: e.CreatedAt,
			Pending: e.Pending, Succeeded: e.Succeeded, Dead: e.Dead})
	}
	resp := map[string]any{"data": out}
	if len(rows) == int(p.MaxRows) {
		last := rows[len(rows)-1]
		resp["next_cursor"] = encodeCursor(last.CreatedAt, last.ID)
	}
	httpx.WriteJSON(w, http.StatusOK, resp)
}

type attemptJSON struct {
	Attempt    int32     `json:"attempt"`
	StartedAt  time.Time `json:"started_at"`
	DurationMS *int32    `json:"duration_ms"`
	Outcome    string    `json:"outcome"`
	StatusCode *int32    `json:"status_code,omitempty"`
	ErrorClass *string   `json:"error_class,omitempty"`
	Error      *string   `json:"error,omitempty"`
}

type deliveryJSON struct {
	ID            uuid.UUID     `json:"id"`
	EndpointID    uuid.UUID     `json:"endpoint_id"`
	Seq           int64         `json:"seq"`
	Status        string        `json:"status"` // pending | retrying | succeeded | dead
	Attempts      int32         `json:"attempts"`
	NextAttemptAt *time.Time    `json:"next_attempt_at,omitempty"`
	CompletedAt   *time.Time    `json:"completed_at,omitempty"`
	LastError     *string       `json:"last_error,omitempty"`
	DeadReason    *string       `json:"dead_reason,omitempty"`
	ReplayOf      *uuid.UUID    `json:"replay_of,omitempty"`
	History       []attemptJSON `json:"history"`
}

// event is the event detail page: the payload exactly as stored, and every
// delivery with its full attempt history (the AttemptTimeline's data).
func (d *Dashboard) event(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	ctx := r.Context()
	ev, err := d.store.GetEvent(ctx, gen.GetEventParams{TenantID: auth.TenantFrom(ctx), ID: id})
	if store.IsNoRows(err) {
		httpx.WriteError(w, r, httpx.ErrNotFound)
		return
	}
	if err != nil {
		httpx.WriteError(w, r, err)
		return
	}
	pending, err := d.store.EventPendingDeliveries(ctx, id)
	if err != nil {
		httpx.WriteError(w, r, err)
		return
	}
	done, err := d.store.EventDoneDeliveries(ctx, id)
	if err != nil {
		httpx.WriteError(w, r, err)
		return
	}
	out := make([]deliveryJSON, 0, len(pending)+len(done))
	for _, dl := range done {
		completed := dl.CompletedAt
		out = append(out, deliveryJSON{
			ID: dl.ID, EndpointID: dl.EndpointID, Seq: dl.Seq, Status: dl.FinalState, Attempts: dl.Attempt,
			CompletedAt: &completed, LastError: dl.LastError, DeadReason: dl.DeadReason, ReplayOf: dl.ReplayOf,
		})
	}
	for _, dl := range pending {
		next := dl.NextAttemptAt
		out = append(out, deliveryJSON{
			ID: dl.ID, EndpointID: dl.EndpointID, Seq: dl.Seq, Status: dl.State, Attempts: dl.Attempt,
			NextAttemptAt: &next, LastError: dl.LastError, ReplayOf: dl.ReplayOf,
		})
	}
	ids := make([]uuid.UUID, 0, len(out))
	for _, dl := range out {
		ids = append(ids, dl.ID)
	}
	atts, err := d.store.DeliveryAttempts(ctx, ids)
	if err != nil {
		httpx.WriteError(w, r, err)
		return
	}
	byDelivery := map[uuid.UUID][]attemptJSON{}
	for _, a := range atts {
		byDelivery[a.DeliveryID] = append(byDelivery[a.DeliveryID], attemptJSON{
			Attempt: a.Attempt, StartedAt: a.StartedAt, DurationMS: a.DurationMs, Outcome: a.Outcome,
			StatusCode: a.StatusCode, ErrorClass: a.ErrorClass, Error: a.ErrorDetail,
		})
	}
	for i := range out {
		if out[i].History = byDelivery[out[i].ID]; out[i].History == nil {
			out[i].History = []attemptJSON{}
		}
	}
	slices.SortFunc(out, func(a, b deliveryJSON) int { return cmp.Compare(a.EndpointID.String(), b.EndpointID.String()) })
	payload := json.RawMessage(ev.Payload)
	if !json.Valid(payload) {
		b, _ := json.Marshal(string(ev.Payload))
		payload = b
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"id": ev.ID, "type": ev.EventType, "ordering_key": ev.OrderingKey, "created_at": ev.CreatedAt,
		"payload": payload, "deliveries": out,
	})
}

type seriesPoint struct {
	T           int64   `json:"t"` // unix seconds
	Ingested    int64   `json:"ingested"`
	Succeeded   int64   `json:"succeeded"`
	Failed      int64   `json:"failed"`
	RateLimited int64   `json:"rate_limited"`
	P50MS       float64 `json:"p50_ms"`
	P99MS       float64 `json:"p99_ms"`
}

// series is the time-series chart's data: ingest and delivery throughput with
// latency percentiles, bucketed. ?window=900&bucket=10[&endpoint_id=]
func (d *Dashboard) series(w http.ResponseWriter, r *http.Request) {
	window, bucket := 900, 10
	if v, err := strconv.Atoi(r.URL.Query().Get("window")); err == nil {
		window = min(max(v, 60), 6*3600)
	}
	if v, err := strconv.Atoi(r.URL.Query().Get("bucket")); err == nil {
		bucket = min(max(v, 1), 600)
	}
	if window/bucket > 2000 {
		bucket = window / 2000
	}
	var endpoint *uuid.UUID
	if v := r.URL.Query().Get("endpoint_id"); v != "" {
		id, err := uuid.Parse(v)
		if err != nil {
			httpx.WriteError(w, r, httpx.ErrBadRequest.WithMessage("endpoint_id is not a UUID."))
			return
		}
		endpoint = &id
	}
	t := auth.TenantFrom(r.Context())
	att, err := d.store.ThroughputSeries(r.Context(), gen.ThroughputSeriesParams{
		TenantID: t, BucketSeconds: int32(bucket), WindowSeconds: int32(window), EndpointID: endpoint, //nolint:gosec // clamped
	})
	if err != nil {
		httpx.WriteError(w, r, err)
		return
	}
	ing, err := d.store.IngestSeries(r.Context(), gen.IngestSeriesParams{
		TenantID: t, BucketSeconds: int32(bucket), WindowSeconds: int32(window), //nolint:gosec // clamped
	})
	if err != nil {
		httpx.WriteError(w, r, err)
		return
	}
	// Dense series: every bucket present, zeros included, so a chart never
	// interpolates across a gap and a quiet minute reads as quiet.
	now := time.Now().Unix()
	start := (now - int64(window)) / int64(bucket) * int64(bucket)
	points := map[int64]*seriesPoint{}
	order := []int64{}
	for ts := start; ts <= now; ts += int64(bucket) {
		points[ts] = &seriesPoint{T: ts}
		order = append(order, ts)
	}
	for _, a := range att {
		if p := points[a.Bucket.Unix()]; p != nil {
			p.Succeeded, p.Failed, p.RateLimited, p.P50MS, p.P99MS = a.Succeeded, a.Failed, a.RateLimited, a.P50Ms, a.P99Ms
		}
	}
	if endpoint == nil {
		for _, i := range ing {
			if p := points[i.Bucket.Unix()]; p != nil {
				p.Ingested = i.Ingested
			}
		}
	}
	out := make([]seriesPoint, 0, len(order))
	for _, ts := range order {
		out = append(out, *points[ts])
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"bucket_seconds": bucket, "data": out})
}
