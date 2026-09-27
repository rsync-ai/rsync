package llmscrub

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// ContainsRedaction exists for callers that do not merely DISPLAY scrubbed text but
// feed a model's reply back into something executed — the schema healer applies the
// DDL the model suggests. A suggestion echoing a marker back would write
// "[redacted]" into the customer's schema, so the healer asks this package whether
// an answer is marker-bearing rather than keeping its own copy of the token list.
//
// The risk that copy guards against is drift: a new substitution added to Scrub
// emitting a marker nobody adds to redactionMarkers would silently reopen the hole,
// and no behavioural test would notice, because the healer would simply stop
// recognising that one marker. The first test below reads this package's own source
// and fails on exactly that.

// markerInSource matches every bracketed "…redacted" token this package writes.
var markerInSource = regexp.MustCompile(`\[[a-z\-]*redacted\]`)

func TestRedactionMarkersCoverEveryTokenScrubEmits(t *testing.T) {
	src, err := os.ReadFile("scrub.go")
	if err != nil {
		t.Fatalf("reading scrub.go: %v", err)
	}

	found := markerInSource.FindAllString(string(src), -1)
	if len(found) == 0 {
		// Vacuity guard: a regex that matched nothing would let this test pass
		// while proving nothing at all.
		t.Fatal("found no redaction markers in scrub.go; the scan is broken, not the code")
	}

	known := make(map[string]bool, len(redactionMarkers))
	for _, m := range redactionMarkers {
		known[m] = true
	}
	for _, m := range found {
		if !known[m] {
			t.Errorf("scrub.go emits %q but redactionMarkers does not list it: "+
				"ContainsRedaction would not recognise it, so an executed caller "+
				"would run it as if it were real SQL", m)
		}
	}
}

func TestContainsRedactionSeesEveryMarkerScrubActuallyProduces(t *testing.T) {
	// One input per distinct marker, chosen so the assertion is about Scrub's real
	// output rather than about a hand-written copy of the token.
	cases := []struct {
		name string
		in   string
		want string // the marker this input must provoke
	}{
		{"credential kv", "connect failed: password=hunter2-not-a-real-one", "[redacted]"},
		{"json number", `{"row_count": 41234}`, "[num-redacted]"},
		{"address", "notify ops-alerts@example.com on failure", "[email-redacted]"},
		{"host address", "dial tcp 10.1.2.3:5432: connection refused", "[ip-redacted]"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := Scrub(tc.in)
			if !strings.Contains(out, tc.want) {
				t.Fatalf("Scrub(%q) = %q, which does not contain %q; "+
					"this fixture no longer provokes the marker it is here to cover",
					tc.in, out, tc.want)
			}
			if !ContainsRedaction(out) {
				t.Errorf("ContainsRedaction(%q) = false, want true", out)
			}
		})
	}
}

func TestContainsRedactionIsFalseForTextScrubLeftAlone(t *testing.T) {
	// The control. Without it the function could return true unconditionally and
	// every case above would still pass — and a healer that treats every
	// suggestion as marker-bearing silently discards all of them.
	clean := []string{
		"",
		"ALTER TABLE public.users ADD COLUMN nickname text",
		"redacted",              // the bare word, not the bracketed token
		"[redaction] requested", // near-miss
	}
	for _, s := range clean {
		if got := Scrub(s); got != s {
			t.Fatalf("Scrub(%q) = %q; this fixture is meant to pass through untouched", s, got)
		}
		if ContainsRedaction(s) {
			t.Errorf("ContainsRedaction(%q) = true, want false", s)
		}
	}
}
