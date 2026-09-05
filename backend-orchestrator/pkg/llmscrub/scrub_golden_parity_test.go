package llmscrub

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"testing"
)

// TestScrubMatchesCrossLanguageGolden pins the Go scrubber to the shared golden
// fixture generated from the canonical Python scrubber (llm-service masking.py).
// The connector scrubber (shared/mcp-connectors/base_connector.py
// scrub_sensitive) is pinned to the same fixture by
// llm-service/tests/test_scrubber_parity.py, and the kafka-mcp-sink worker's
// scrubLog by its own scrub_golden_parity_test.go. If any of those rule sets
// drifts, its test fails — closing the lockstep-drift leak class (H5).
func TestScrubMatchesCrossLanguageGolden(t *testing.T) {
	path := filepath.Join("..", "..", "..", "shared", "scrubber_golden.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s: %v", path, err)
	}
	var golden []struct {
		Input    string `json:"input"`
		Expected string `json:"expected"`
	}
	if err := json.Unmarshal(data, &golden); err != nil {
		t.Fatalf("parse golden: %v", err)
	}
	if len(golden) == 0 {
		t.Fatal("golden fixture is empty")
	}
	for _, g := range golden {
		if got := Scrub(g.Input); got != g.Expected {
			t.Errorf("Scrub(%q):\n  got:  %q\n  want: %q\n(Go scrubber drifted from masking.py — keep pkg/llmscrub/scrub.go in lockstep)",
				g.Input, got, g.Expected)
		}
	}
}

// reMustCompileLiteral pulls the raw (backtick-quoted) pattern out of each
// regexp.MustCompile call, in source order. Both Go scrubbers write their
// patterns as raw strings, so no unquoting is needed and the comparison stays
// byte-exact.
var reMustCompileLiteral = regexp.MustCompile("regexp\\.MustCompile\\(`([^`]*)`\\)")

// sinkScrubPath is the second Go copy: a separate module, so it cannot import
// this package (that would drag backend-orchestrator into the sink's Docker
// build context) and the rule set is duplicated there by hand.
var sinkScrubPath = filepath.Join("..", "..", "..", "shared", "mcp-connectors", "internal",
	"kafka-mcp-sink", "worker-src", "cmd", "kafka-sink-worker", "scrub.go")

// TestPatternsMatchSinkWorkerSource compares the ORDERED regex literals here
// against the sink worker's copy.
//
// The golden fixture only proves the two agree on inputs somebody thought to
// write down; a rule present in one copy and absent from the other stays
// invisible until a fixture case happens to exercise it. That is exactly how
// the sink lost the IPv4 rule and kept a superseded two-pass quote rule while
// every test in its own package still passed. Comparing the pattern lists needs
// no fixture case: same regexes, same order, or this fails.
//
// It is mirrored in the sink package so EITHER module's CI job catches a patch
// that lands on one copy — the two are behind different change-path filters and
// a one-sided edit only runs one of the jobs.
func TestPatternsMatchSinkWorkerSource(t *testing.T) {
	mine, err := goPatternLiterals(filepath.Join("scrub.go"))
	if err != nil {
		t.Fatalf("read scrub.go: %v", err)
	}
	theirs, err := goPatternLiterals(sinkScrubPath)
	if err != nil {
		t.Fatalf("read %s: %v", sinkScrubPath, err)
	}
	if len(mine) == 0 {
		t.Fatal("no regexp.MustCompile literals found in scrub.go — the extractor is broken, not the scrubber")
	}
	if len(mine) != len(theirs) {
		t.Fatalf("rule count drift: llmscrub/scrub.go has %d patterns, sink scrub.go has %d\nllmscrub: %q\n    sink: %q",
			len(mine), len(theirs), mine, theirs)
	}
	for i := range mine {
		if mine[i] != theirs[i] {
			t.Errorf("rule %d differs:\nllmscrub: %s\n    sink: %s", i, mine[i], theirs[i])
		}
	}
}

func goPatternLiterals(path string) ([]string, error) {
	src, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	matches := reMustCompileLiteral.FindAllSubmatch(src, -1)
	out := make([]string, 0, len(matches))
	for _, m := range matches {
		out = append(out, string(m[1]))
	}
	return out, nil
}
