package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"testing"
)

// reMustCompileLiteral pulls the raw (backtick-quoted) pattern out of each
// regexp.MustCompile call, in source order. Raw strings are what both scrubbers
// use, so no unquoting is needed and the comparison stays byte-exact.
var reMustCompileLiteral = regexp.MustCompile("regexp\\.MustCompile\\(`([^`]*)`\\)")

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

// goldenPath locates shared/scrubber_golden.json from this package directory.
// The fixture lives outside the worker module on purpose — it is the contract
// three separately-compiled scrubbers share, so it cannot belong to any one of
// them. The image build never runs tests (the Dockerfile only `go build`s), so
// reaching outside the module here costs the build nothing.
func goldenPath() string {
	return filepath.Join("..", "..", "..", "..", "..", "..", "..", "shared", "scrubber_golden.json")
}

// TestScrubLogMatchesCrossLanguageGolden pins scrubLog to the same fixture the
// orchestrator (backend-orchestrator/pkg/llmscrub) and llm-service (masking.py)
// scrubbers are pinned to.
//
// Until this test existed the sink was the copy nobody checked, and it had
// silently drifted back to a rule the other two fixed: the two-pass single-quote
// form that redacted the prose and left the quoted identifier in the clear
// ("Couldn't … pipeline 'orders-sync'." → `Couldn'[redacted]'orders-sync'…`).
// It was also missing the IPv4 rule outright. Neither was catchable from inside
// this package, because every assertion here was written against this package's
// own behavior. Pinning to the shared fixture is what makes a patch that lands
// on two of the three copies fail instead of ship.
func TestScrubLogMatchesCrossLanguageGolden(t *testing.T) {
	path := goldenPath()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s: %v (run from a full repo checkout — the fixture is shared with backend-orchestrator and llm-service)", path, err)
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
		if got := scrubLog(g.Input); got != g.Expected {
			t.Errorf("scrubLog(%q):\n  got:  %q\n  want: %q\n(sink scrubber drifted from the shared rule set — keep scrub.go in lockstep with backend-orchestrator/pkg/llmscrub/scrub.go and llm-service/src/utils/masking.py)",
				g.Input, got, g.Expected)
		}
	}
}

// TestScrubLogPatternsMatchLLMScrubSource compares the ORDERED regex literals in
// this file against the canonical ones in llmscrub/scrub.go.
//
// The golden fixture only proves the two agree on inputs somebody thought to
// write down. A rule added to one copy and not the other stays invisible to it
// until a fixture case happens to exercise that rule — which is how the IPv4
// rule went missing here for as long as it did. Comparing the pattern lists
// needs no fixture case at all: the two Go copies are the same regexes in the
// same order, or this fails.
func TestScrubLogPatternsMatchLLMScrubSource(t *testing.T) {
	canonical := filepath.Join("..", "..", "..", "..", "..", "..", "..",
		"backend-orchestrator", "pkg", "llmscrub", "scrub.go")

	mine, err := goPatternLiterals(filepath.Join("scrub.go"))
	if err != nil {
		t.Fatalf("read sink scrub.go: %v", err)
	}
	theirs, err := goPatternLiterals(canonical)
	if err != nil {
		t.Fatalf("read %s: %v", canonical, err)
	}
	if len(mine) == 0 {
		t.Fatal("no regexp.MustCompile literals found in scrub.go — the extractor is broken, not the scrubber")
	}
	if len(mine) != len(theirs) {
		t.Fatalf("rule count drift: sink scrub.go has %d patterns, llmscrub/scrub.go has %d\n sink: %q\nllmscrub: %q",
			len(mine), len(theirs), mine, theirs)
	}
	for i := range mine {
		if mine[i] != theirs[i] {
			t.Errorf("rule %d differs:\n    sink: %s\nllmscrub: %s", i, mine[i], theirs[i])
		}
	}
}
