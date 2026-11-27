package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/netip"
	"net/url"
	"time"
)

// runProbe implements `ithaca probe <url>`: exit 0 on a 2xx, 1 otherwise.
//
// The runtime image is distroless (no shell, no curl, no wget), which is the
// point of it, so a container health check cannot shell out to anything. The
// binary probes its own admin port instead:
//
//	healthcheck: ["CMD", "/ithaca", "probe", "http://localhost:9080/readyz"]
//
// Kubernetes does not need this (the kubelet probes over HTTP itself); Docker
// Compose does.
func runProbe(args []string) error {
	if len(args) != 1 {
		return errors.New("usage: ithaca probe <url>")
	}
	// A health check probes its own container and nothing else. Refusing
	// anything but loopback keeps this from ever being a request-anywhere tool.
	u, err := url.Parse(args[0])
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return fmt.Errorf("probe: %q is not an http(s) URL", args[0])
	}
	if h := u.Hostname(); h != "localhost" && !isLoopback(h) {
		return fmt.Errorf("probe: %q is not a loopback address; probe checks this container only", h)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil) //nolint:gosec // G704: loopback only, enforced above
	if err != nil {
		return fmt.Errorf("probe: %w", err)
	}
	resp, err := http.DefaultClient.Do(req) //nolint:gosec // G704: loopback only, enforced above
	if err != nil {
		return fmt.Errorf("probe: %w", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("probe: %s returned %d", args[0], resp.StatusCode)
	}
	return nil
}

func isLoopback(host string) bool {
	a, err := netip.ParseAddr(host)
	return err == nil && a.IsLoopback()
}
