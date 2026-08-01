package httpx

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/SharmaSanskar/ithaca/internal/platform/logging"
)

// Error is the single error shape the whole API speaks.
//
// A stable machine-readable `code` matters more than the message: clients
// branch on codes, humans read messages, and messages are allowed to change.
type Error struct {
	// Code is a stable, snake_case identifier. Never renamed once shipped.
	Code string `json:"code"`
	// Message is human-readable and safe to show a developer.
	Message string `json:"message"`
	// Details carries field-level validation problems.
	Details map[string]string `json:"details,omitempty"`
	// RequestID is filled in by WriteError so a bug report is one paste.
	RequestID string `json:"request_id,omitempty"`

	status int
	cause  error
}

func (e *Error) Error() string {
	if e.cause != nil {
		return fmt.Sprintf("%s: %s: %v", e.Code, e.Message, e.cause)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func (e *Error) Unwrap() error { return e.cause }

// Status returns the HTTP status this error maps to.
func (e *Error) Status() int {
	if e.status == 0 {
		return http.StatusInternalServerError
	}
	return e.status
}

// WithCause attaches an internal error. The cause is logged, never serialized —
// internal errors leak schema names, hostnames and query fragments.
func (e *Error) WithCause(err error) *Error {
	cp := *e
	cp.cause = err
	return &cp
}

// WithDetails attaches field-level detail.
func (e *Error) WithDetails(d map[string]string) *Error {
	cp := *e
	cp.Details = d
	return &cp
}

// WithMessage overrides the human-readable message.
func (e *Error) WithMessage(msg string) *Error {
	cp := *e
	cp.Message = msg
	return &cp
}

// NewError builds a one-off error.
func NewError(status int, code, message string) *Error {
	return &Error{Code: code, Message: message, status: status}
}

// The canonical errors. Anything not in this list should be a deliberate
// addition, not an ad-hoc string at a call site.
var (
	ErrBadRequest      = NewError(http.StatusBadRequest, "bad_request", "The request could not be understood.")
	ErrUnauthorized    = NewError(http.StatusUnauthorized, "unauthorized", "Missing or invalid credentials.")
	ErrForbidden       = NewError(http.StatusForbidden, "forbidden", "You do not have access to this resource.")
	ErrNotFound        = NewError(http.StatusNotFound, "not_found", "The requested resource does not exist.")
	ErrConflict        = NewError(http.StatusConflict, "conflict", "The request conflicts with the current state.")
	ErrPayloadTooLarge = NewError(http.StatusRequestEntityTooLarge, "payload_too_large", "The request body is too large.")
	ErrUnprocessable   = NewError(http.StatusUnprocessableEntity, "unprocessable", "The request was well-formed but semantically invalid.")
	ErrRateLimited     = NewError(http.StatusTooManyRequests, "rate_limited", "Too many requests.")
	ErrInternal        = NewError(http.StatusInternalServerError, "internal_error", "Something went wrong on our end.")
	ErrUnavailable     = NewError(http.StatusServiceUnavailable, "unavailable", "The service is temporarily unavailable.")
)

// WriteJSON writes a JSON response.
func WriteJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if body == nil {
		return
	}
	if err := json.NewEncoder(w).Encode(body); err != nil {
		// The status line is already on the wire, so there is nothing to do
		// but record it. Usually means the client hung up mid-write.
		slog.Debug("failed to encode response body", "error", err)
	}
}

// WriteNoContent writes 204.
func WriteNoContent(w http.ResponseWriter) { w.WriteHeader(http.StatusNoContent) }

// WriteError serializes an error response.
//
// Any error that is not an *Error is treated as internal and its text is NOT
// returned to the caller — that is the boundary that stops a wrapped pgx error
// from telling the internet your column names.
func WriteError(w http.ResponseWriter, r *http.Request, err error) {
	var apiErr *Error
	if !errors.As(err, &apiErr) {
		apiErr = ErrInternal.WithCause(err)
	}

	out := *apiErr
	out.RequestID = w.Header().Get(HeaderRequestID)

	if out.Status() >= 500 {
		logging.FromContext(r.Context()).ErrorContext(r.Context(), "request failed",
			"code", out.Code,
			"status", out.Status(),
			"error", apiErr.Error(),
		)
	}

	WriteJSON(w, out.Status(), map[string]any{"error": out})
}

// DecodeJSON reads and strictly decodes a JSON request body.
//
// DisallowUnknownFields is on purpose: silently ignoring a misspelled field
// means a caller thinks they configured something they did not, and the bug
// surfaces days later as "why didn't my retry policy apply".
func DecodeJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	ct := r.Header.Get("Content-Type")
	if ct != "" && !isJSONContentType(ct) {
		return ErrBadRequest.WithMessage("Content-Type must be application/json.")
	}

	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			return ErrPayloadTooLarge.WithCause(err)
		}
		var syn *json.SyntaxError
		if errors.As(err, &syn) {
			return ErrBadRequest.
				WithMessage(fmt.Sprintf("Malformed JSON at byte %d.", syn.Offset)).
				WithCause(err)
		}
		var typeErr *json.UnmarshalTypeError
		if errors.As(err, &typeErr) {
			return ErrBadRequest.
				WithMessage(fmt.Sprintf("Field %q expects %s.", typeErr.Field, typeErr.Type)).
				WithCause(err)
		}
		return ErrBadRequest.WithMessage("Request body is not valid JSON.").WithCause(err)
	}

	// A second value in the stream means the caller sent concatenated JSON,
	// which is almost always a client bug worth surfacing.
	if dec.More() {
		return ErrBadRequest.WithMessage("Request body must contain exactly one JSON value.")
	}
	return nil
}

func isJSONContentType(ct string) bool {
	for i := 0; i < len(ct); i++ {
		if ct[i] == ';' {
			ct = ct[:i]
			break
		}
	}
	switch trimSpace(ct) {
	case "application/json", "application/json+cloudevents", "text/json":
		return true
	}
	return false
}

func trimSpace(s string) string {
	start := 0
	for start < len(s) && (s[start] == ' ' || s[start] == '\t') {
		start++
	}
	end := len(s)
	for end > start && (s[end-1] == ' ' || s[end-1] == '\t') {
		end--
	}
	return s[start:end]
}
