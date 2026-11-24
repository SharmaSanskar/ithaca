package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// loader accumulates parse errors so a bad config reports *every* problem at
// once instead of making you fix them one boot at a time.
type loader struct {
	errs []error
}

func (l *loader) fail(key string, err error) {
	l.errs = append(l.errs, fmt.Errorf("%s: %w", key, err))
}

func (l *loader) err() error {
	switch len(l.errs) {
	case 0:
		return nil
	case 1:
		return l.errs[0]
	}
	msgs := make([]string, 0, len(l.errs))
	for _, e := range l.errs {
		msgs = append(msgs, e.Error())
	}
	return fmt.Errorf("invalid configuration:\n  - %s", strings.Join(msgs, "\n  - "))
}

func lookup(key string) (string, bool) {
	v, ok := os.LookupEnv(key)
	if !ok {
		return "", false
	}
	v = strings.TrimSpace(v)
	if v == "" {
		return "", false
	}
	return v, true
}

func (l *loader) str(key, def string) string {
	if v, ok := lookup(key); ok {
		return v
	}
	return def
}

func (l *loader) int(key string, def int) int {
	v, ok := lookup(key)
	if !ok {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		l.fail(key, fmt.Errorf("%q is not an integer", v))
		return def
	}
	return n
}

func (l *loader) bool(key string, def bool) bool { //nolint:unparam // every default is spelled out at its call site on purpose
	v, ok := lookup(key)
	if !ok {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		l.fail(key, fmt.Errorf("%q is not a boolean", v))
		return def
	}
	return b
}

func (l *loader) dur(key string, def time.Duration) time.Duration {
	v, ok := lookup(key)
	if !ok {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		l.fail(key, fmt.Errorf("%q is not a duration (try 30s, 5m, 1h)", v))
		return def
	}
	return d
}

func (l *loader) oneOf(key, def string, allowed ...string) string {
	v := l.str(key, def)
	for _, a := range allowed {
		if v == a {
			return v
		}
	}
	l.fail(key, fmt.Errorf("%q is not one of %s", v, strings.Join(allowed, ", ")))
	return def
}
