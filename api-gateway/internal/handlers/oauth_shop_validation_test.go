package handlers

import "testing"

// TestSanitizeShopSubdomain locks in the {shop} host-injection defense: only a
// single DNS label is accepted, and every SSRF / secret-exfil payload that
// would smuggle a host into the token URL is rejected.
func TestSanitizeShopSubdomain(t *testing.T) {
	valid := map[string]string{
		"mystore":               "mystore",
		"  mystore  ":           "mystore",
		"mystore.myshopify.com": "mystore",
		"My-Store-1":            "My-Store-1",
		"a":                     "a",
	}
	for in, want := range valid {
		got, err := sanitizeShopSubdomain(in)
		if err != nil {
			t.Errorf("sanitizeShopSubdomain(%q) error = %v, want nil", in, err)
			continue
		}
		if got != want {
			t.Errorf("sanitizeShopSubdomain(%q) = %q, want %q", in, got, want)
		}
	}

	// Host-injection / SSRF / exfil payloads must all be rejected.
	invalid := []string{
		"",
		"   ",
		"169.254.169.254",  // dots -> not a single label (IMDS)
		"169.254.169.254#", // fragment-truncation host injection
		"attacker.com#",    // exfil client_secret to a public attacker host
		"attacker.com",
		"evil/path",       // path injection
		"a@b",             // userinfo injection
		"host:8080",       // port injection
		"foo bar",         // space
		"sub.domain",      // extra label
		"-leadinghyphen",  // invalid DNS label
		"trailinghyphen-", // invalid DNS label
		"under_score",     // underscore not allowed
		"127.0.0.1",       // loopback literal
	}
	for _, in := range invalid {
		if got, err := sanitizeShopSubdomain(in); err == nil {
			t.Errorf("sanitizeShopSubdomain(%q) = %q, nil; want error", in, got)
		}
	}
}
