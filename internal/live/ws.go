package live

import (
	"context"
	"encoding/json"
	"maps"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

// Protocol (JSON text messages):
//
//	server → {"type":"hello","channels":[...],"frame_hz":10,"reconnect":{...}}
//	client → {"op":"subscribe","ch":"flow"}        server → {"type":"subscribed","ch":"flow"}
//	client → {"op":"unsubscribe","ch":"flow"}      server → {"type":"unsubscribed","ch":"flow"}
//	server → frame | delta | narration | {"type":"lag","superseded":n} | {"type":"error",...}
//
// Clients reconnect with FULL JITTER: delay = min(30s, 500ms·2^n) · (0.5 + rand).
// A hub rollout disconnects every dashboard at once; without jitter they all
// reconnect in the same instant, which is exactly the thundering-herd bug class
// this whole project is about.

const (
	writeTimeout = 10 * time.Second
	pongWait     = 60 * time.Second
	pingEvery    = 25 * time.Second
	maxReadBytes = 4 << 10 // clients only send tiny control messages
	maxSubs      = 16
)

// TenantResolver decides which tenant a connection may watch.
type TenantResolver func(r *http.Request) (uuid.UUID, bool)

// Handler serves the WebSocket endpoint.
type Handler struct {
	Hub            *Hub
	Resolve        TenantResolver
	AllowedOrigins []string // in addition to same-origin
	MaxConns       int64

	conns atomic.Int64
}

// ServeHTTP upgrades and runs one connection.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	tenant, ok := h.Resolve(r)
	if !ok {
		http.Error(w, "live feed unavailable", http.StatusServiceUnavailable)
		return
	}
	if h.MaxConns > 0 && h.conns.Load() >= h.MaxConns {
		http.Error(w, "too many connections", http.StatusServiceUnavailable)
		return
	}
	up := websocket.Upgrader{
		ReadBufferSize: 1024, WriteBufferSize: 8192,
		CheckOrigin:       h.checkOrigin,
		EnableCompression: true,
	}
	ws, err := up.Upgrade(w, r, nil)
	if err != nil {
		return // Upgrade already wrote the error response
	}
	h.conns.Add(1)
	defer h.conns.Add(-1)
	h.serve(r.Context(), ws, tenant)
}

// checkOrigin allows same-origin (Origin host == Host, which is the production
// setup: dashboard and /ws behind one ingress) plus an explicit allowlist (the
// Vite dev server). Without this, any website a dashboard user visits could
// open a socket to the hub with their cookies.
func (h *Handler) checkOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true // non-browser clients (websocat, tests) send no Origin
	}
	u, err := url.Parse(origin)
	if err != nil {
		return false
	}
	if strings.EqualFold(u.Host, r.Host) {
		return true
	}
	for _, a := range h.AllowedOrigins {
		if strings.EqualFold(a, origin) {
			return true
		}
	}
	return false
}

type controlMsg struct {
	Op string `json:"op"`
	Ch string `json:"ch"`
}

func (h *Handler) serve(ctx context.Context, ws *websocket.Conn, tenant uuid.UUID) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var closeOnce sync.Once
	closeConn := func() { closeOnce.Do(func() { cancel(); _ = ws.Close() }) }
	defer closeConn()

	c := &client{tenant: tenant, box: newOutbox(), subs: map[string]bool{}, closed: closeConn}
	h.Hub.register(c)
	defer h.Hub.unregister(c)

	hello, _ := json.Marshal(map[string]any{
		"type": "hello", "channels": []string{ChanFlow, ChanDemo, chanEndpoint + "<id>"}, "frame_hz": 10,
		"reconnect": map[string]any{"strategy": "full_jitter", "base_ms": 500, "cap_ms": 30000},
	})
	c.box.offerDelta(hello)

	go h.writer(ctx, ws, c, closeConn)

	ws.SetReadLimit(maxReadBytes)
	_ = ws.SetReadDeadline(time.Now().Add(pongWait))
	ws.SetPongHandler(func(string) error { return ws.SetReadDeadline(time.Now().Add(pongWait)) })
	for {
		var m controlMsg
		if err := ws.ReadJSON(&m); err != nil {
			return
		}
		reply := func(v map[string]any) {
			b, _ := json.Marshal(v)
			if !c.box.offerDelta(b) {
				closeConn()
			}
		}
		switch {
		case !ValidChannel(m.Ch):
			reply(map[string]any{"type": "error", "code": "bad_channel", "message": "unknown channel " + m.Ch})
		case m.Op == "subscribe":
			if len(c.subsSnapshot(h.Hub)) >= maxSubs {
				reply(map[string]any{"type": "error", "code": "too_many_subscriptions"})
				continue
			}
			h.Hub.subscribe(ctx, c, m.Ch)
			reply(map[string]any{"type": "subscribed", "ch": m.Ch})
		case m.Op == "unsubscribe":
			h.Hub.unsubscribe(c, m.Ch)
			reply(map[string]any{"type": "unsubscribed", "ch": m.Ch})
		default:
			reply(map[string]any{"type": "error", "code": "bad_op", "message": "op must be subscribe or unsubscribe"})
		}
	}
}

func (c *client) subsSnapshot(h *Hub) map[string]bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return maps.Clone(c.subs)
}

// writer is the only goroutine that writes to the socket (gorilla allows one
// concurrent writer). It drains the outbox whenever woken.
func (h *Handler) writer(ctx context.Context, ws *websocket.Conn, c *client, closeConn func()) {
	ping := time.NewTicker(pingEvery)
	defer ping.Stop()
	var lastLag time.Time
	pendingLag := 0
	write := func(t int, b []byte) bool {
		_ = ws.SetWriteDeadline(time.Now().Add(writeTimeout))
		if err := ws.WriteMessage(t, b); err != nil {
			closeConn()
			return false
		}
		return true
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ping.C:
			if !write(websocket.PingMessage, nil) {
				return
			}
			continue
		case <-c.box.wake:
		}
		msgs, superseded, overflow := c.box.take()
		if overflow {
			_ = ws.WriteControl(websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseTryAgainLater, "too slow; reconnect"),
				time.Now().Add(time.Second))
			closeConn()
			return
		}
		for _, m := range msgs {
			if !write(websocket.TextMessage, m) {
				return
			}
		}
		// Tell the client it is missing frames, at most once a second, so a
		// dashboard can show "reduced frame rate" instead of silently lagging.
		pendingLag += superseded
		if pendingLag > 0 && time.Since(lastLag) > time.Second {
			b, _ := json.Marshal(map[string]any{"type": "lag", "superseded": pendingLag})
			if !write(websocket.TextMessage, b) {
				return
			}
			pendingLag, lastLag = 0, time.Now()
		}
	}
}
