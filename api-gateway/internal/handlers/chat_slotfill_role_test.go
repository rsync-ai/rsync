package handlers

import (
	"strings"
	"testing"
)

// These tests exercise the deterministic single-connector parser + role
// inference added for slot-filling. The role logic (inferConnectorRole) is pure
// and is tested directly with token slices so it does not depend on the
// connector catalog / global DB state, which other tests in this package mutate.

func TestInferConnectorRole(t *testing.T) {
	cases := []struct {
		name   string
		tokens []string
		idx    int
		want   string
	}{
		{"sync verb → source", []string{"sync", "mysql", "all", "data"}, 1, "source"},
		{"into prep → destination", []string{"load", "data", "into", "postgresql"}, 3, "destination"},
		{"from prep → source", []string{"export", "from", "mongodb"}, 2, "source"},
		{"to prep → destination", []string{"send", "to", "snowflake"}, 2, "destination"},
		{"cdc stream from → source", []string{"stream", "changes", "from", "postgres"}, 3, "source"},
		{"get…from → source", []string{"get", "data", "from", "bigquery"}, 3, "source"},
		{"replicate verb → source", []string{"replicate", "mysql"}, 1, "source"},
		{"to-marker after connector → source", []string{"mysql", "to", "a", "file"}, 0, "source"},
		{"load verb → destination", []string{"load", "redshift"}, 1, "destination"},
		{"prep before beats verb", []string{"load", "from", "mysql"}, 2, "source"},
		{"bare connector → ambiguous", []string{"mysql"}, 0, ""},
		{"non-directional verb → ambiguous", []string{"i", "want", "to", "use", "postgres"}, 4, ""},
		{"out-of-range idx → ambiguous", []string{"mysql"}, 5, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := inferConnectorRole(tc.tokens, tc.idx); got != tc.want {
				t.Errorf("inferConnectorRole(%v, %d): want %q, got %q", tc.tokens, tc.idx, tc.want, got)
			}
		})
	}
}

func TestTokenizeForConnectorScan(t *testing.T) {
	// Multi-word connector folding + punctuation stripping.
	got := tokenizeForConnectorScan("Load data into aws s3, please!")
	want := []string{"load", "data", "into", "aws-s3", "please"}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Errorf("want %v, got %v", want, got)
	}
}

func TestParseRoleReply(t *testing.T) {
	cases := map[string]string{
		"mysql is my source":   "source",
		"it's the destination": "destination",
		"source":               "source",
		"destination":          "destination",
		"target":               "destination",
		"from":                 "source",
		"that's nonsense":      "",
		"":                     "",
	}
	for msg, want := range cases {
		if got := parseRoleReply(msg); got != want {
			t.Errorf("parseRoleReply(%q): want %q, got %q", msg, want, got)
		}
	}
}

func TestLooksLikeHelpRequest(t *testing.T) {
	help := []string{"how to sync mysql", "what is postgresql", "help me", "explain CDC"}
	notHelp := []string{"sync mysql all data", "load data into postgresql", "mysql to s3"}
	for _, m := range help {
		if !looksLikeHelpRequest(m) {
			t.Errorf("want help=true for %q", m)
		}
	}
	for _, m := range notHelp {
		if looksLikeHelpRequest(m) {
			t.Errorf("want help=false for %q", m)
		}
	}
}
