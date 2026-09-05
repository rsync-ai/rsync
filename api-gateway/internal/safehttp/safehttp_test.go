package safehttp

import (
	"net"
	"net/url"
	"testing"
)

func TestIsBlockedIP_BlocksNonPublic(t *testing.T) {
	blocked := []string{
		"127.0.0.1",              // loopback
		"127.5.5.5",              // loopback /8
		"0.0.0.0",                // unspecified
		"10.0.0.5",               // RFC1918
		"172.16.4.4",             // RFC1918
		"192.168.1.1",            // RFC1918
		"169.254.169.254",        // cloud metadata (link-local)
		"100.64.1.1",             // CGNAT
		"::1",                    // IPv6 loopback
		"fe80::1",                // IPv6 link-local
		"fd00::1",                // IPv6 ULA
		"::ffff:169.254.169.254", // IPv4-mapped metadata
		"::ffff:127.0.0.1",       // IPv4-mapped loopback
	}
	for _, s := range blocked {
		ip := net.ParseIP(s)
		if ip == nil {
			t.Fatalf("bad test IP %q", s)
		}
		if !IsBlockedIP(ip) {
			t.Errorf("IsBlockedIP(%s) = false, want true (must be blocked)", s)
		}
	}
}

func TestIsBlockedIP_AllowsPublic(t *testing.T) {
	allowed := []string{
		"8.8.8.8",              // Google DNS
		"140.82.112.3",         // github.com range
		"1.1.1.1",              // Cloudflare
		"2606:4700:4700::1111", // Cloudflare IPv6
	}
	for _, s := range allowed {
		ip := net.ParseIP(s)
		if ip == nil {
			t.Fatalf("bad test IP %q", s)
		}
		if IsBlockedIP(ip) {
			t.Errorf("IsBlockedIP(%s) = true, want false (public must be allowed)", s)
		}
	}
}

func TestIsBlockedIP_NilBlocked(t *testing.T) {
	if !IsBlockedIP(nil) {
		t.Error("IsBlockedIP(nil) = false, want true")
	}
}

func TestValidateURL(t *testing.T) {
	ok := []string{"https://example.com/x", "http://example.com"}
	bad := []string{"file:///etc/passwd", "gopher://x", "ftp://x", "dict://x"}
	for _, s := range ok {
		u, _ := url.Parse(s)
		if err := ValidateURL(u); err != nil {
			t.Errorf("ValidateURL(%q) = %v, want nil", s, err)
		}
	}
	for _, s := range bad {
		u, _ := url.Parse(s)
		if err := ValidateURL(u); err == nil {
			t.Errorf("ValidateURL(%q) = nil, want error", s)
		}
	}
	if err := ValidateURL(nil); err == nil {
		t.Error("ValidateURL(nil) = nil, want error")
	}
}
