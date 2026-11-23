package ingest

import (
	"fmt"
	"hash/fnv"
	"strings"

	"github.com/google/uuid"
)

// MatchesEventType reports whether eventType matches any subscription pattern.
//
//	"*"               everything
//	"order.created"   exactly that type
//	"order.*"         any type under order. (order.created, order.item.added)
func MatchesEventType(patterns []string, eventType string) bool {
	for _, p := range patterns {
		switch {
		case p == "*" || p == eventType:
			return true
		case strings.HasSuffix(p, ".*"):
			if strings.HasPrefix(eventType, p[:len(p)-1]) { // keep the dot
				return true
			}
		}
	}
	return false
}

// ValidPattern reports whether p is a well-formed subscription pattern.
func ValidPattern(p string) bool {
	if p == "*" {
		return true
	}
	p = strings.TrimSuffix(p, ".*")
	return eventTypeRE.MatchString(p) && !strings.Contains(p, "*")
}

// StreamKey decides which stream (lane) an event joins for one subscription.
//
// This one function is the whole difference between the two modes:
//
//   - ordered subscription + ordering_key → the stream IS the key, so every
//     event for "order_123" is serialized through one lane.
//   - otherwise → hash the event id onto one of laneCount synthetic lanes. The
//     lanes are still serial internally, so unordered mode reuses every line
//     of the ordered code path; it just has no causal relationship to protect
//     and spreads load across lanes.
//
// An ordered subscription receiving an event WITHOUT an ordering_key has
// nothing to order it relative to, so it takes the lane path too. Funneling
// all key-less events into one "global" stream would silently cap that
// subscription at ~1/latency deliveries per second.
func StreamKey(ordered bool, laneCount int32, orderingKey string, eventID uuid.UUID) string {
	if ordered && orderingKey != "" {
		return "key:" + orderingKey
	}
	if laneCount < 1 {
		laneCount = 1
	}
	h := fnv.New32a()
	_, _ = h.Write(eventID[:])
	return fmt.Sprintf("lane:%d", h.Sum32()%uint32(laneCount)) //nolint:gosec // laneCount >= 1
}
