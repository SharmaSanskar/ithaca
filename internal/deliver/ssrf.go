package deliver

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"syscall"
)

// ErrBlocked marks a connection refused by the SSRF guard. It is a permanent
// failure: the destination will not become safe without a config change.
var ErrBlocked = errors.New("ssrf: destination blocked")

// blockedPrefixes is every range a webhook must never reach.
//
// The last two are the ones hand-rolled blocklists miss: NAT64 (64:ff9b::/96)
// and 6to4 (2002::/16) both embed an IPv4 address inside a public-looking IPv6
// one, so 64:ff9b::a9fe:a9fe is 169.254.169.254 — the cloud metadata service —
// wearing a disguise.
var blockedPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),      // "this network"
	netip.MustParsePrefix("10.0.0.0/8"),     // private
	netip.MustParsePrefix("100.64.0.0/10"),  // carrier-grade NAT
	netip.MustParsePrefix("127.0.0.0/8"),    // loopback
	netip.MustParsePrefix("169.254.0.0/16"), // link-local: AWS/GCP/Azure metadata lives here
	netip.MustParsePrefix("172.16.0.0/12"),  // private
	netip.MustParsePrefix("192.0.0.0/24"),   // IETF protocol assignments
	netip.MustParsePrefix("192.0.2.0/24"),   // TEST-NET-1
	netip.MustParsePrefix("192.168.0.0/16"), // private
	netip.MustParsePrefix("198.18.0.0/15"),  // benchmarking
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("224.0.0.0/4"), // multicast
	netip.MustParsePrefix("240.0.0.0/4"), // reserved + broadcast
	netip.MustParsePrefix("::/128"),      // unspecified
	netip.MustParsePrefix("::1/128"),     // loopback
	netip.MustParsePrefix("fc00::/7"),    // unique local
	netip.MustParsePrefix("fe80::/10"),   // link-local
	netip.MustParsePrefix("ff00::/8"),    // multicast
	netip.MustParsePrefix("64:ff9b::/96"),
	netip.MustParsePrefix("64:ff9b:1::/48"),
	netip.MustParsePrefix("2002::/16"),
}

// Guard decides whether an address may be dialed.
type Guard struct {
	allow []netip.Prefix // exemptions, e.g. the local receiver in dev
}

// NewGuard builds a guard. allowCIDRs are exempt from the blocklist; in
// production this list is empty.
func NewGuard(allowCIDRs []string) (*Guard, error) {
	g := &Guard{}
	for _, c := range allowCIDRs {
		p, err := netip.ParsePrefix(c)
		if err != nil {
			return nil, fmt.Errorf("allow cidr %q: %w", c, err)
		}
		g.allow = append(g.allow, p.Masked())
	}
	return g, nil
}

// Check reports whether addr may be dialed.
func (g *Guard) Check(addr netip.Addr) error {
	// ::ffff:127.0.0.1 IS 127.0.0.1. Unmap first or the v4 rules never match.
	addr = addr.Unmap()
	for _, p := range g.allow {
		if p.Contains(addr) {
			return nil
		}
	}
	for _, p := range blockedPrefixes {
		if p.Contains(addr) {
			return fmt.Errorf("%w: %s is in %s", ErrBlocked, addr, p)
		}
	}
	return nil
}

// Control is installed as net.Dialer.Control.
//
// Why here and not "resolve, check, then connect": validate-then-connect is a
// TOCTOU hole. You resolve evil.com to 1.2.3.4 and approve it; http.Client
// resolves AGAIN (1-second TTL) to 169.254.169.254 and connects to the
// metadata service. Control runs after resolution and immediately before
// connect(2), with the literal IP about to be dialed, so there is no window —
// and it covers every IP of a multi-address host, because it runs per attempt.
func (g *Guard) Control(network, address string, _ syscall.RawConn) error {
	if network != "tcp4" && network != "tcp6" {
		return fmt.Errorf("%w: network %q", ErrBlocked, network)
	}
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrBlocked, err)
	}
	addr, err := netip.ParseAddr(host)
	if err != nil {
		return fmt.Errorf("%w: unparseable address %q", ErrBlocked, host)
	}
	return g.Check(addr)
}
