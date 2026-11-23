package ingest

import (
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"

	"github.com/SharmaSanskar/ithaca/internal/auth"
	"github.com/SharmaSanskar/ithaca/internal/platform/httpx"
)

// Handler exposes the ingest service over HTTP.
type Handler struct {
	svc *Service
}

// NewHandler wraps a service.
func NewHandler(svc *Service) *Handler { return &Handler{svc: svc} }

// Routes mounts POST /events. The caller supplies auth and body-size limits.
func (h *Handler) Routes(r chi.Router) {
	r.Post("/events", h.postEvent)
}

const maxIdempotencyKeyLen = 255

var (
	errKeyReuse = httpx.NewError(http.StatusUnprocessableEntity, "idempotency_key_reuse",
		"This Idempotency-Key was already used with a different request body.")
	errBadKey = httpx.NewError(http.StatusBadRequest, "invalid_idempotency_key",
		"Idempotency-Key must be 1-255 printable ASCII characters.")
)

func (h *Handler) postEvent(w http.ResponseWriter, r *http.Request) {
	if ct := r.Header.Get("Content-Type"); ct != "" && !strings.HasPrefix(strings.ToLower(ct), "application/json") {
		httpx.WriteError(w, r, httpx.ErrBadRequest.WithMessage("Content-Type must be application/json."))
		return
	}

	key := r.Header.Get("Idempotency-Key")
	if key != "" && !validIdempotencyKey(key) {
		httpx.WriteError(w, r, errBadKey)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			httpx.WriteError(w, r, httpx.ErrPayloadTooLarge.WithCause(err))
			return
		}
		httpx.WriteError(w, r, httpx.ErrBadRequest.WithMessage("Could not read request body.").WithCause(err))
		return
	}

	res, err := h.svc.Ingest(r.Context(), Request{
		TenantID:       auth.TenantFrom(r.Context()),
		Method:         r.Method,
		Path:           r.URL.Path,
		Body:           body,
		IdempotencyKey: key,
		Traceparent:    r.Header.Get("traceparent"),
	})
	if err != nil {
		var fe *FieldError
		switch {
		case errors.As(err, &fe):
			httpx.WriteError(w, r, httpx.ErrBadRequest.
				WithMessage("The event is invalid.").
				WithDetails(map[string]string{fe.Field: fe.Msg}))
		case errors.Is(err, ErrIdempotencyMismatch):
			httpx.WriteError(w, r, errKeyReuse)
		case errors.Is(err, ErrMalformed):
			httpx.WriteError(w, r, httpx.ErrBadRequest.WithMessage(err.Error()))
		default:
			httpx.WriteError(w, r, err)
		}
		return
	}

	if res.Replayed {
		w.Header().Set("Idempotent-Replay", "true")
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(res.Status)
	_, _ = w.Write(append(res.Body, '\n'))
}

func validIdempotencyKey(k string) bool {
	if len(k) == 0 || len(k) > maxIdempotencyKeyLen {
		return false
	}
	for i := 0; i < len(k); i++ {
		if k[i] < 0x21 || k[i] > 0x7e {
			return false
		}
	}
	return true
}
