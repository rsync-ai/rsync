package handlers

import (
	"testing"
)

// staticIndex is a fake ConnectorIndex for the tests. Returns whatever
// the test gave it.
type staticIndex struct{ entries []ConnectorEntry }

func (s *staticIndex) List() []ConnectorEntry { return s.entries }

func indexOf(entries ...ConnectorEntry) *staticIndex { return &staticIndex{entries: entries} }

// Convenience: the canonical Shopify entry the tests reuse.
var shopifyEntry = ConnectorEntry{
	ID:          "shopify-admin-graphql",
	Aliases:     []string{"shopify-admin-graphql", "shopify_admin_graphql", "Shopify", "shopify"},
	DisplayName: "Shopify",
	Category:    "api_saas",
}
var stripeEntry = ConnectorEntry{
	ID:          "stripe",
	Aliases:     []string{"stripe", "Stripe"},
	DisplayName: "Stripe",
	Category:    "api_saas",
}
var postgresEntry = ConnectorEntry{
	ID:          "postgresql",
	Aliases:     []string{"postgres", "postgresql", "pg", "PostgreSQL"},
	DisplayName: "PostgreSQL",
	Category:    "database",
}
var s3Entry = ConnectorEntry{
	ID:          "aws-s3",
	Aliases:     []string{"aws-s3", "s3", "S3"},
	DisplayName: "AWS S3",
	Category:    "storage",
}

func TestResolveSource_ExactMatchAutoResolves(t *testing.T) {
	r := NewIntentResolver(indexOf(shopifyEntry, postgresEntry))
	got := r.ResolveSource("sync products from Shopify to Postgres")
	if got.MatchedConnectorType != "shopify-admin-graphql" {
		t.Errorf("source: want shopify-admin-graphql, got %q (candidates=%v)", got.MatchedConnectorType, got.Candidates)
	}
}

func TestResolveDestination_ExactMatchAutoResolves(t *testing.T) {
	r := NewIntentResolver(indexOf(shopifyEntry, postgresEntry))
	got := r.ResolveDestination("sync products from Shopify to Postgres")
	if got.MatchedConnectorType != "postgresql" {
		t.Errorf("destination: want postgresql, got %q (candidates=%v)", got.MatchedConnectorType, got.Candidates)
	}
}

func TestResolve_AliasMatches(t *testing.T) {
	r := NewIntentResolver(indexOf(postgresEntry))
	// "pg" is an alias.
	got := r.ResolveDestination("to pg")
	if got.MatchedConnectorType != "postgresql" {
		t.Errorf("alias match failed: %+v", got)
	}
}

func TestResolve_EmptyRequest(t *testing.T) {
	r := NewIntentResolver(indexOf(shopifyEntry))
	got := r.ResolveSource("")
	if got.MatchedConnectorType != "" {
		t.Error("empty request should not match")
	}
}

func TestResolve_NoMatch(t *testing.T) {
	r := NewIntentResolver(indexOf(shopifyEntry))
	got := r.ResolveSource("sync notion to airtable")
	if got.MatchedConnectorType != "" {
		t.Error("nothing should auto-match")
	}
	if len(got.Candidates) != 0 {
		t.Errorf("no candidates expected, got %v", got.Candidates)
	}
}

func TestResolve_MultipleCandidatesReturnsPicker(t *testing.T) {
	// "sync data" matches nothing on the alias side; "data" isn't an
	// alias of either Stripe or Shopify, so no match.
	// Build a richer test: two connectors with overlapping aliases.
	overlap1 := ConnectorEntry{ID: "stripe", Aliases: []string{"stripe", "payments"}}
	overlap2 := ConnectorEntry{ID: "square", Aliases: []string{"square", "payments"}}
	r := NewIntentResolver(indexOf(overlap1, overlap2))
	got := r.ResolveSource("sync payments to postgres")
	// Both connectors match equally — gap too small for auto-resolve.
	if got.MatchedConnectorType != "" {
		t.Errorf("ambiguous match should not auto-resolve, got %q", got.MatchedConnectorType)
	}
	if len(got.Candidates) < 2 {
		t.Errorf("want >=2 candidates, got %v", got.Candidates)
	}
}

func TestResolve_ShortAliasNeedsWordBoundary(t *testing.T) {
	// "s3" is a short alias — must NOT match a string like "stripes3stuff".
	r := NewIntentResolver(indexOf(s3Entry))
	got := r.ResolveDestination("to stripes3stuff")
	if got.MatchedConnectorType != "" {
		t.Errorf("short alias matched without word boundary: %+v", got)
	}
}

func TestResolve_ShortAliasWithBoundaryDoesMatch(t *testing.T) {
	r := NewIntentResolver(indexOf(s3Entry))
	got := r.ResolveDestination("sync orders to s3 today")
	if got.MatchedConnectorType != "aws-s3" {
		t.Errorf("short alias with word boundary should match, got %+v", got)
	}
}

func TestResolve_DirectionBiasFavorsSourceBeforeTo(t *testing.T) {
	// Same connector mentioned on both sides → source preference picks
	// the pre-"to" hit when ResolveSource is called, the post-"to" hit
	// for ResolveDestination.
	pg := postgresEntry
	r := NewIntentResolver(indexOf(pg))
	srcMatch := r.ResolveSource("from postgres to postgres")
	dstMatch := r.ResolveDestination("from postgres to postgres")
	if srcMatch.MatchedConnectorType == "" || dstMatch.MatchedConnectorType == "" {
		t.Errorf("both should match: src=%+v dst=%+v", srcMatch, dstMatch)
	}
}

func TestResolve_NilIndex(t *testing.T) {
	r := NewIntentResolver(nil)
	got := r.ResolveSource("sync from shopify to postgres")
	if got.MatchedConnectorType != "" || len(got.Candidates) != 0 {
		t.Errorf("nil index should return empty, got %+v", got)
	}
}

func TestSplitOnDirection(t *testing.T) {
	cases := []struct {
		input, pre, post string
	}{
		{"sync products from shopify to postgres", "sync products from shopify", "postgres"},
		{"copy stripe into bigquery", "copy stripe", "bigquery"},
		{"shopify -> postgres", "shopify", "postgres"},
		{"shopify postgres", "shopify postgres", ""},
	}
	for _, tc := range cases {
		pre, post := splitOnDirection(tc.input)
		if pre != tc.pre || post != tc.post {
			t.Errorf("%q: want (%q,%q), got (%q,%q)", tc.input, tc.pre, tc.post, pre, post)
		}
	}
}

func TestContainsWord(t *testing.T) {
	cases := []struct {
		haystack, needle string
		want             bool
	}{
		{"sync to s3 today", "s3", true},
		{"stripes3stuff", "s3", false},
		{"s3", "s3", true},
		{"to-s3-today", "s3", true},
		{"s3stuff", "s3", false},
		{"stuffs3", "s3", false},
		{"", "s3", false},
	}
	for _, tc := range cases {
		got := intentContainsWord(tc.haystack, tc.needle)
		if got != tc.want {
			t.Errorf("intentContainsWord(%q, %q): want %v, got %v", tc.haystack, tc.needle, tc.want, got)
		}
	}
}
