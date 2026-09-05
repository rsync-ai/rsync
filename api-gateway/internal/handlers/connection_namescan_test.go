package handlers

import "testing"

// TestRequestMentionsName guards the whole-token matcher used by
// checkConnections to let a user disambiguate same-type connections by naming
// one in the request. It must match a bare token but never a substring of a
// longer identifier, and must treat hyphenated names as single units.
func TestRequestMentionsName(t *testing.T) {
	cases := []struct {
		name      string
		req       string // already-lowercased, as checkConnections passes it
		candidate string
		want      bool
	}{
		{"bare token", `load products from the "shopify" source connection`, "shopify", true},
		{"hyphenated name as unit", `into the "azure-pg-test-dst" postgresql destination`, "azure-pg-test-dst", true},
		{"not a substring of longer id", `sync from test-shopify source`, "shopify", false},
		{"longer hyphenated id matches itself", `sync from test-shopify source`, "test-shopify", true},
		{"absent", `copy products as a one-time snapshot`, "shopify", false},
		{"too short ignored", `pg to my`, "pg", false},
		{"start of string", `shopify to postgres`, "shopify", true},
		{"end of string", `destination is azure-pg-test-dst`, "azure-pg-test-dst", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := requestMentionsName(c.req, c.candidate); got != c.want {
				t.Fatalf("requestMentionsName(%q, %q) = %v, want %v", c.req, c.candidate, got, c.want)
			}
		})
	}
}
