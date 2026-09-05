// Package safehttp provides an *http.Client hardened against SSRF.
//
// The client refuses to open a connection to any non-public IP address
// (loopback, RFC1918 private, carrier-grade NAT, link-local — which includes
// the cloud metadata endpoint 169.254.169.254 — IPv6 ULA/link-local, etc.) and
// pins each connection to the exact IP it validated, closing the DNS-rebinding
// TOCTOU window (a hostname that resolves to a public IP on the first lookup
// and an internal IP on the second). Redirects are re-validated per hop.
//
// Use it for every outbound request whose destination host can be influenced by
// user input — OAuth token/refresh/revocation/userinfo URLs, connector
// callbacks, webhook targets, etc. A bare &http.Client{} on such a request is
// an SSRF (and, for OAuth, a client-secret exfiltration) primitive.
//
// Escape hatch: SAFEHTTP_ALLOW_HOSTS is a comma-separated allowlist of exact
// hostnames that bypass the IP guard (e.g. a Docker-internal mock provider used
// by e2e tests). It is unset in production, so production gets the full guard.
package safehttp

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// ErrBlockedAddress is returned when an outbound request resolves to a
// non-public address and is therefore refused.
var ErrBlockedAddress = errors.New("safehttp: destination resolves to a non-public address")

// blockedCIDRs are ranges that must never be dialed. net.IP's own predicates
// (IsLoopback/IsPrivate/IsLinkLocal*/…) cover most of these; the explicit list
// adds the ranges those predicates miss (CGNAT, "this-network", protocol/test
// assignments) and documents intent.
var blockedCIDRs = mustParseCIDRs(
	"0.0.0.0/8",      // "this" network
	"10.0.0.0/8",     // RFC1918 private
	"100.64.0.0/10",  // RFC6598 carrier-grade NAT
	"127.0.0.0/8",    // loopback
	"169.254.0.0/16", // link-local — includes cloud metadata 169.254.169.254
	"172.16.0.0/12",  // RFC1918 private
	"192.0.0.0/24",   // IETF protocol assignments
	"192.168.0.0/16", // RFC1918 private
	"198.18.0.0/15",  // benchmarking
	"::1/128",        // IPv6 loopback
	"fc00::/7",       // IPv6 unique-local (ULA)
	"fe80::/10",      // IPv6 link-local
)

// allowedHosts bypass the IP guard entirely (SAFEHTTP_ALLOW_HOSTS). Read once
// at init; empty in production.
var allowedHosts = parseAllowedHosts(os.Getenv("SAFEHTTP_ALLOW_HOSTS"))

func mustParseCIDRs(cidrs ...string) []*net.IPNet {
	out := make([]*net.IPNet, 0, len(cidrs))
	for _, c := range cidrs {
		_, n, err := net.ParseCIDR(c)
		if err != nil {
			panic("safehttp: bad CIDR " + c)
		}
		out = append(out, n)
	}
	return out
}

func parseAllowedHosts(raw string) map[string]struct{} {
	m := make(map[string]struct{})
	for _, h := range strings.Split(raw, ",") {
		if h = strings.ToLower(strings.TrimSpace(h)); h != "" {
			m[h] = struct{}{}
		}
	}
	return m
}

// IsBlockedIP reports whether ip is a non-public address that must not be
// dialed. Exported for tests and for callers that want to validate an IP ahead
// of time.
func IsBlockedIP(ip net.IP) bool {
	if ip == nil {
		return true
	}
	if v4 := ip.To4(); v4 != nil {
		ip = v4
	}
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsUnspecified() ||
		ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() ||
		ip.IsInterfaceLocalMulticast() {
		return true
	}
	for _, n := range blockedCIDRs {
		if n.Contains(ip) {
			return true
		}
	}
	return false
}

// ValidateURL rejects URLs whose scheme is not http/https. The authoritative
// SSRF check runs at dial time (so it also covers redirects and rebinding);
// this is a cheap early reject for obviously-bad schemes (file://, gopher://…).
func ValidateURL(u *url.URL) error {
	if u == nil {
		return errors.New("safehttp: nil URL")
	}
	switch strings.ToLower(u.Scheme) {
	case "http", "https":
		return nil
	default:
		return fmt.Errorf("safehttp: scheme %q not allowed", u.Scheme)
	}
}

// NewClient returns an *http.Client whose transport refuses to connect to any
// non-public address and pins each connection to the exact IP it validated.
// timeout bounds the whole request (as with http.Client.Timeout).
func NewClient(timeout time.Duration) *http.Client {
	base := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	return &http.Client{
		Timeout: timeout,
		// No Proxy: a proxy would resolve the target host itself and bypass the
		// dial-time guard entirely. This infra egresses directly.
		Transport: &http.Transport{
			DialContext:           guardedDialContext(base),
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 5 {
				return errors.New("safehttp: stopped after 5 redirects")
			}
			return ValidateURL(req.URL)
		},
	}
}

func guardedDialContext(base *net.Dialer) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, err
		}
		if _, ok := allowedHosts[strings.ToLower(host)]; ok {
			return base.DialContext(ctx, network, addr)
		}
		// A literal-IP destination still has to pass the guard.
		if literal := net.ParseIP(host); literal != nil {
			if IsBlockedIP(literal) {
				return nil, fmt.Errorf("%w: %s", ErrBlockedAddress, literal)
			}
			return base.DialContext(ctx, network, net.JoinHostPort(literal.String(), port))
		}
		ips, err := net.DefaultResolver.LookupIP(ctx, "ip", host)
		if err != nil {
			return nil, fmt.Errorf("safehttp: resolve %q: %w", host, err)
		}
		lastErr := error(fmt.Errorf("%w: %s", ErrBlockedAddress, host))
		for _, ip := range ips {
			if IsBlockedIP(ip) {
				continue
			}
			// Pin to the validated IP — dialing the literal means no second
			// lookup, so there is no rebinding window between check and dial.
			conn, derr := base.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
			if derr != nil {
				lastErr = derr
				continue
			}
			return conn, nil
		}
		return nil, lastErr
	}
}
