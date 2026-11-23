package ingest

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestParseEnvelopePreservesPayloadBytes(t *testing.T) {
	// Key order, whitespace and number formatting must all survive untouched:
	// these are the bytes the subscriber's HMAC is computed over.
	payload := `{"z":1,  "a":[1.50, 2e3],"m":{"y":null}}`
	body := []byte(`{"type":"order.created","payload":` + payload + `}`)

	env, err := ParseEnvelope(body)
	if err != nil {
		t.Fatalf("ParseEnvelope error = %v", err)
	}
	if !bytes.Equal(env.Payload, []byte(payload)) {
		t.Errorf("payload bytes changed:\n got %s\nwant %s", env.Payload, payload)
	}
}

func TestParseEnvelopeValidation(t *testing.T) {
	cases := []struct {
		name, body string
		field      string // "" = expect ErrMalformed
	}{
		{"not json", `nope`, ""},
		{"two values", `{"type":"a","payload":1}{}`, ""},
		{"unknown field", `{"type":"a","payload":1,"extra":true}`, ""},
		{"missing type", `{"payload":{}}`, "type"},
		{"bad type chars", `{"type":"order created","payload":{}}`, "type"},
		{"type too long", `{"type":"` + strings.Repeat("a", 129) + `","payload":{}}`, "type"},
		{"missing payload", `{"type":"a"}`, "payload"},
		{"null payload", `{"type":"a","payload":null}`, "payload"},
		{"ordering key too long", `{"type":"a","payload":{},"ordering_key":"` + strings.Repeat("k", 257) + `"}`, "ordering_key"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseEnvelope([]byte(tc.body))
			if err == nil {
				t.Fatal("expected an error")
			}
			var fe *FieldError
			if tc.field == "" {
				if !errors.Is(err, ErrMalformed) {
					t.Errorf("error = %v, want ErrMalformed", err)
				}
				return
			}
			if !errors.As(err, &fe) || fe.Field != tc.field {
				t.Errorf("error = %v, want FieldError on %q", err, tc.field)
			}
		})
	}
}

func TestParseEnvelopeAcceptsScalarPayloads(t *testing.T) {
	for _, p := range []string{`"text"`, `42`, `[]`, `true`} {
		if _, err := ParseEnvelope([]byte(`{"type":"a","payload":` + p + `}`)); err != nil {
			t.Errorf("payload %s rejected: %v", p, err)
		}
	}
}

func TestMatchesEventType(t *testing.T) {
	cases := []struct {
		patterns []string
		typ      string
		want     bool
	}{
		{[]string{"*"}, "anything.at.all", true},
		{[]string{"order.created"}, "order.created", true},
		{[]string{"order.created"}, "order.updated", false},
		{[]string{"order.*"}, "order.created", true},
		{[]string{"order.*"}, "order.item.added", true},
		{[]string{"order.*"}, "order", false},          // the bare parent is not "under" it
		{[]string{"order.*"}, "orders.created", false}, // prefix must end at the dot
		{[]string{"user.*", "order.created"}, "order.created", true},
		{[]string{}, "order.created", false},
	}
	for _, tc := range cases {
		if got := MatchesEventType(tc.patterns, tc.typ); got != tc.want {
			t.Errorf("MatchesEventType(%v, %q) = %v, want %v", tc.patterns, tc.typ, got, tc.want)
		}
	}
}

func TestValidPattern(t *testing.T) {
	for _, p := range []string{"*", "order.created", "order.*", "a"} {
		if !ValidPattern(p) {
			t.Errorf("ValidPattern(%q) = false, want true", p)
		}
	}
	for _, p := range []string{"", "order.**", "*.created", "or*der", ".*", "order created"} {
		if ValidPattern(p) {
			t.Errorf("ValidPattern(%q) = true, want false", p)
		}
	}
}

func TestStreamKey(t *testing.T) {
	id := uuid.Must(uuid.NewV7())

	t.Run("ordered with a key uses the key", func(t *testing.T) {
		if got := StreamKey(true, 16, "order_1", id); got != "key:order_1" {
			t.Errorf("got %q", got)
		}
	})

	t.Run("a customer key can never collide with a synthetic lane", func(t *testing.T) {
		if StreamKey(true, 16, "lane:3", id) == "lane:3" {
			t.Error("ordering_key 'lane:3' collided with synthetic lane 3")
		}
	})

	t.Run("unordered ignores the ordering key", func(t *testing.T) {
		if got := StreamKey(false, 16, "order_1", id); !strings.HasPrefix(got, "lane:") {
			t.Errorf("got %q, want a lane", got)
		}
	})

	t.Run("ordered without a key falls back to a lane", func(t *testing.T) {
		if got := StreamKey(true, 16, "", id); !strings.HasPrefix(got, "lane:") {
			t.Errorf("got %q, want a lane", got)
		}
	})

	t.Run("deterministic per event", func(t *testing.T) {
		first, second := StreamKey(false, 16, "", id), StreamKey(false, 16, "", id)
		if first != second {
			t.Error("same event mapped to different lanes")
		}
	})

	t.Run("spreads across lanes", func(t *testing.T) {
		lanes := map[string]int{}
		for range 1600 {
			lanes[StreamKey(false, 16, "", uuid.Must(uuid.NewV7()))]++
		}
		if len(lanes) != 16 {
			t.Fatalf("used %d lanes, want all 16", len(lanes))
		}
		for k, n := range lanes {
			if n < 50 || n > 150 { // expected 100 each; very loose bound
				t.Errorf("lane %s got %d events: hash is badly skewed", k, n)
			}
		}
	})
}

func TestRequestHashCoversMethodPathAndBody(t *testing.T) {
	base := RequestHash("POST", "/v1/events", []byte(`{}`))
	if bytes.Equal(base, RequestHash("POST", "/v1/events", []byte(`{ }`))) {
		t.Error("different body bytes produced the same hash")
	}
	if bytes.Equal(base, RequestHash("PUT", "/v1/events", []byte(`{}`))) {
		t.Error("different method produced the same hash")
	}
	if bytes.Equal(base, RequestHash("POST", "/v1/other", []byte(`{}`))) {
		t.Error("different path produced the same hash")
	}
	if !bytes.Equal(base, RequestHash("POST", "/v1/events", []byte(`{}`))) {
		t.Error("hash is not deterministic")
	}
}

func TestValidIdempotencyKey(t *testing.T) {
	for _, k := range []string{"a", "order-123:retry", strings.Repeat("k", 255)} {
		if !validIdempotencyKey(k) {
			t.Errorf("rejected valid key %q", k)
		}
	}
	for _, k := range []string{"", strings.Repeat("k", 256), "has space", "tab\t", "ünïcode"} {
		if validIdempotencyKey(k) {
			t.Errorf("accepted invalid key %q", k)
		}
	}
}
