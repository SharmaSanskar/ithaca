package api

import (
	"encoding/base64"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/SharmaSanskar/ithaca/internal/auth"
	"github.com/SharmaSanskar/ithaca/internal/dlq"
	"github.com/SharmaSanskar/ithaca/internal/platform/httpx"
	"github.com/SharmaSanskar/ithaca/internal/store/gen"
)

// DLQRoutes mounts the dead-letter queue API. The caller supplies auth.
//
//	GET  /dlq?endpoint_id=&limit=&cursor=   browse, newest first
//	POST /dlq/replay                        {"ids":[...]} or {"endpoint_id":"...","limit":n}
func (c *ControlPlane) DLQRoutes(r chi.Router) {
	r.Get("/dlq", c.listDLQ)
	r.Post("/dlq/replay", c.replayDLQ)
}

type deadLetterJSON struct {
	ID           uuid.UUID  `json:"id"`
	EventID      uuid.UUID  `json:"event_id"`
	EventType    string     `json:"event_type"`
	EndpointID   uuid.UUID  `json:"endpoint_id"`
	Attempts     int32      `json:"attempts"`
	DeadReason   *string    `json:"dead_reason"`
	LastError    *string    `json:"last_error"`
	DiedAt       time.Time  `json:"died_at"`
	ReplayedAt   *time.Time `json:"replayed_at,omitempty"`
	ReplayedAs   *uuid.UUID `json:"replayed_as,omitempty"`
	ReplayOf     *uuid.UUID `json:"replay_of,omitempty"`
	BlocksStream bool       `json:"blocks_stream"` // an ordered stream is paused behind this one
}

func (c *ControlPlane) listDLQ(w http.ResponseWriter, r *http.Request) {
	p := gen.ListDeadLettersParams{TenantID: auth.TenantFrom(r.Context()), MaxRows: 50}
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 500 {
			httpx.WriteError(w, r, httpx.ErrBadRequest.WithMessage("limit must be 1-500."))
			return
		}
		p.MaxRows = int32(n) //nolint:gosec // bounded to 1-500 just above
	}
	if v := r.URL.Query().Get("endpoint_id"); v != "" {
		id, err := uuid.Parse(v)
		if err != nil {
			httpx.WriteError(w, r, httpx.ErrBadRequest.WithMessage("endpoint_id is not a UUID."))
			return
		}
		p.EndpointID = &id
	}
	if v := r.URL.Query().Get("cursor"); v != "" {
		t, id, ok := decodeCursor(v)
		if !ok {
			httpx.WriteError(w, r, httpx.ErrBadRequest.WithMessage("Invalid cursor."))
			return
		}
		p.BeforeCompletedAt, p.BeforeID = &t, &id
	}

	rows, err := c.store.ListDeadLetters(r.Context(), p)
	if err != nil {
		httpx.WriteError(w, r, err)
		return
	}
	out := make([]deadLetterJSON, 0, len(rows))
	for _, d := range rows {
		out = append(out, deadLetterJSON{
			ID: d.ID, EventID: d.EventID, EventType: d.EventType, EndpointID: d.EndpointID,
			Attempts: d.Attempt, DeadReason: d.DeadReason, LastError: d.LastError, DiedAt: d.CompletedAt,
			ReplayedAt: d.ReplayedAt, ReplayedAs: d.ReplayedAs, ReplayOf: d.ReplayOf, BlocksStream: d.BlocksStream,
		})
	}
	resp := map[string]any{"data": out}
	if len(rows) == int(p.MaxRows) {
		last := rows[len(rows)-1]
		resp["next_cursor"] = encodeCursor(last.CompletedAt, last.ID)
	}
	httpx.WriteJSON(w, http.StatusOK, resp)
}

type replayReq struct {
	IDs        []uuid.UUID `json:"ids"`
	EndpointID *uuid.UUID  `json:"endpoint_id"`
	Limit      int         `json:"limit"`
}

func (c *ControlPlane) replayDLQ(w http.ResponseWriter, r *http.Request) {
	var req replayReq
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, r, err)
		return
	}
	if len(req.IDs) == 0 && req.EndpointID == nil {
		httpx.WriteError(w, r, httpx.ErrBadRequest.WithMessage(
			`Say what to replay: {"ids": [...]} or {"endpoint_id": "...", "limit": n}.`))
		return
	}
	if len(req.IDs) > dlq.MaxBatch || req.Limit > dlq.MaxBatch {
		httpx.WriteError(w, r, httpx.ErrBadRequest.WithMessage("At most 1000 dead letters per replay call."))
		return
	}
	res, err := c.replayer.Replay(r.Context(), dlq.Request{
		TenantID: auth.TenantFrom(r.Context()), IDs: req.IDs, EndpointID: req.EndpointID, Limit: req.Limit,
	})
	if err != nil {
		httpx.WriteError(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]int{
		"replayed": res.Replayed, "resumed_streams": res.Resumed, "skipped": res.Skipped,
	})
}

// Cursors are opaque to clients: base64("<unix nanos>|<uuid>").
func encodeCursor(t time.Time, id uuid.UUID) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.FormatInt(t.UnixNano(), 10) + "|" + id.String()))
}

func decodeCursor(s string) (time.Time, uuid.UUID, bool) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return time.Time{}, uuid.Nil, false
	}
	ts, idStr, ok := strings.Cut(string(b), "|")
	if !ok {
		return time.Time{}, uuid.Nil, false
	}
	n, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return time.Time{}, uuid.Nil, false
	}
	id, err := uuid.Parse(idStr)
	if err != nil {
		return time.Time{}, uuid.Nil, false
	}
	return time.Unix(0, n), id, true
}
