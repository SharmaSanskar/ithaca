package ingest

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
)

// ErrMalformed wraps any body that is not a single, well-formed envelope.
var ErrMalformed = errors.New("malformed event envelope")

// Envelope is the body of POST /v1/events.
//
//	{"type": "order.created", "ordering_key": "order_123", "payload": {...}}
//
// Payload is a json.RawMessage, which holds the EXACT bytes of the payload
// value as the client sent them. Those bytes are stored, signed and delivered
// untouched. Decoding into a map and re-marshaling would reorder keys (Go
// randomizes map iteration) and break every subscriber's signature check.
type Envelope struct {
	Type        string          `json:"type"`
	OrderingKey string          `json:"ordering_key,omitempty"`
	Payload     json.RawMessage `json:"payload"`
}

var eventTypeRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$`)

const maxOrderingKeyLen = 256

// FieldError is a validation failure on one field.
type FieldError struct {
	Field, Msg string
}

func (e *FieldError) Error() string { return fmt.Sprintf("%s: %s", e.Field, e.Msg) }

// ParseEnvelope strictly decodes and validates a request body.
func ParseEnvelope(body []byte) (Envelope, error) {
	var env Envelope
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&env); err != nil {
		return env, fmt.Errorf("%w: %w", ErrMalformed, err)
	}
	if dec.More() {
		return env, fmt.Errorf("%w: body must contain exactly one JSON value", ErrMalformed)
	}

	switch {
	case env.Type == "":
		return env, &FieldError{"type", "is required"}
	case !eventTypeRE.MatchString(env.Type):
		return env, &FieldError{"type", "must be 1-128 chars of letters, digits and _ . : -, starting with a letter or digit"}
	case len(env.OrderingKey) > maxOrderingKeyLen:
		return env, &FieldError{"ordering_key", fmt.Sprintf("must be at most %d bytes", maxOrderingKeyLen)}
	case len(env.Payload) == 0 || bytes.Equal(env.Payload, []byte("null")):
		return env, &FieldError{"payload", "is required"}
	}
	return env, nil
}
