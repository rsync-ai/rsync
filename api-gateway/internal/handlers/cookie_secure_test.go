package handlers

import (
	"os"
	"strings"
	"testing"
)

// The Secure attribute is the difference between a login that persists and one
// that returns 200 and silently loses the session.
//
// Browsers discard a Secure cookie that arrives over plain http, with exactly one
// exception -- localhost, which they treat as a trustworthy origin. The
// quickstart's default answer for a server serves plain http on a named host or a
// LAN address, so the cookie is dropped, the operator is bounced back to the
// login form, and nothing anywhere logs an error. A laptop install never sees it.
//
// So the knob has to exist; the tests below pin the two properties that make it
// safe. It must default to the old behaviour when unset, and it must not fail
// OPEN on a value it cannot parse: an operator who writes RSYNC_COOKIE_SECURE=yeah
// should get a Secure cookie in production, not a plaintext one.

func TestCookieSecureDefaultsToProductionLike(t *testing.T) {
	// Unset must be indistinguishable from the behaviour before the knob existed,
	// in BOTH directions -- otherwise this is a silent change to every existing
	// deployment rather than an opt-in for a new one.
	for _, environment := range []string{"production", "staging", "", "development", "test", "local"} {
		t.Setenv("ENVIRONMENT", environment)
		os.Unsetenv("RSYNC_COOKIE_SECURE")

		if got, want := cookieSecure(), isProductionLike(); got != want {
			t.Errorf("ENVIRONMENT=%q with RSYNC_COOKIE_SECURE unset: cookieSecure()=%v, want isProductionLike()=%v",
				environment, got, want)
		}
	}
}

func TestCookieSecureIsNotVacuous(t *testing.T) {
	// The test above compares two functions, so it would pass if both were stuck
	// on the same constant. Assert the baseline actually discriminates.
	t.Setenv("ENVIRONMENT", "production")
	if !isProductionLike() {
		t.Fatal("isProductionLike() is false for production -- the default test above proves nothing")
	}
	t.Setenv("ENVIRONMENT", "development")
	if isProductionLike() {
		t.Fatal("isProductionLike() is true for development -- the default test above proves nothing")
	}
}

func TestCookieSecureHonoursAnExplicitValue(t *testing.T) {
	// The whole point: a production-like deployment serving plain http must be
	// able to turn Secure off, and a development one must be able to turn it on.
	cases := []struct {
		raw         string
		environment string
		want        bool
	}{
		{"false", "production", false}, // the server-install case that motivated this
		{"0", "production", false},
		{"FALSE", "production", false},
		{"true", "development", true}, // dev behind a TLS-terminating proxy
		{"1", "development", true},
		{"  false  ", "production", false}, // .env files collect trailing spaces
	}
	for _, tc := range cases {
		t.Setenv("ENVIRONMENT", tc.environment)
		t.Setenv("RSYNC_COOKIE_SECURE", tc.raw)
		if got := cookieSecure(); got != tc.want {
			t.Errorf("RSYNC_COOKIE_SECURE=%q ENVIRONMENT=%q: cookieSecure()=%v, want %v",
				tc.raw, tc.environment, got, tc.want)
		}
	}
}

func TestCookieSecureFailsClosedOnGarbage(t *testing.T) {
	// A typo must not silently downgrade a production cookie to plaintext. The
	// operator gets the safe default and a login that works; the alternative is a
	// session cookie travelling in the clear because someone wrote "no".
	t.Setenv("ENVIRONMENT", "production")
	for _, raw := range []string{"yeah", "no", "off", "-1", "true false", "  "} {
		t.Setenv("RSYNC_COOKIE_SECURE", raw)
		if !cookieSecure() {
			t.Errorf("RSYNC_COOKIE_SECURE=%q in production: cookieSecure()=false, want true (fail closed)", raw)
		}
	}
}

// TestEveryCookieUsesTheKnob is the reason this file is worth more than the sum
// of the tests above. cookieSecure() being correct is useless if a cookie is
// still written with isProductionLike() directly -- and that is precisely how the
// bug would come back, because a new cookie is copied from an old one.
func TestEveryCookieUsesTheKnob(t *testing.T) {
	// The files that actually set cookies. Listed rather than globbed so that a
	// new file has to be added here deliberately.
	files := []string{"auth.go", "csrf_middleware.go"}

	found := 0
	for _, name := range files {
		body, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		text := string(body)

		if strings.Contains(text, "secure := isProductionLike()") {
			t.Errorf("%s still derives a cookie's Secure attribute from isProductionLike() directly; "+
				"use cookieSecure() so RSYNC_COOKIE_SECURE can reach it", name)
		}
		found += strings.Count(text, "cookieSecure()")
	}

	// Anti-vacuity: the assertion above passes trivially on a file that sets no
	// cookies at all -- after a refactor moves them, or a typo in the list.
	if found < 4 {
		t.Errorf("found only %d cookieSecure() call sites across %v; expected at least 4 "+
			"(auth_token + csrf on both the set and clear paths). If cookies moved, retarget "+
			"this test rather than lowering the number -- a zero census passes the check above.", found, files)
	}
}
