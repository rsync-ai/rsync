package diagnose

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// The contract ErrorDocURL's own doc comment states -- "slug ... MUST match a
// '## <slug>' heading in docs/errors/README.md" -- had nothing enforcing it.
//
// Nothing could. The URL it builds is a fragment anchor, and a browser follows a
// dead fragment by silently landing at the top of the page: no 404, no error, no
// signal. scripts/check-doc-links.sh does not help either -- it resolves the
// `](path)` targets of markdown links, and these slugs never appear in one. They
// are Go string literals compiled into Remediation.DocURL and returned to users
// in pipeline_run_events.payload and the pipeline status API.
//
// So the failure this guards is a user hitting a real error, clicking the link
// rsync handed them, and reading whatever section happens to be at the top --
// most likely believing it describes their problem.
//
// Both directions are asserted, because they fail differently:
//   - a call site with no heading is the dead anchor above;
//   - a heading with no call site is documentation for an error that cannot be
//     produced, which is how a reader is sent to fix a condition that is not
//     theirs. It also catches the rename half: change a slug in Go and this
//     side names the orphaned heading.
//
// The reverse direction deliberately ignores _test.go call sites. A slug
// referenced only by a test would otherwise keep its heading alive after the
// production code stopped emitting it.

var (
	docURLCall  = regexp.MustCompile(`ErrorDocURL\("([a-z0-9-]+)"\)`)
	docHeading  = regexp.MustCompile(`(?m)^## ([a-z0-9-]+)\s*$`)
	skippedDirs = map[string]bool{".git": true, "node_modules": true, "vendor": true}
)

// repoRoot walks up from this package (backend-orchestrator/pkg/diagnose) to the
// directory holding docs/errors/README.md, rather than hard-coding "../../..".
// A moved package would otherwise turn this test into a skip nobody notices.
func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for i := 0; i < 8; i++ {
		if _, err := os.Stat(filepath.Join(dir, "docs", "errors", "README.md")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	t.Fatal("could not find docs/errors/README.md above the working directory; " +
		"if the doc moved, update this test rather than deleting it")
	return ""
}

// slugCallSites returns slug -> whether any NON-test file passes it to ErrorDocURL.
func slugCallSites(t *testing.T, root string) map[string]bool {
	t.Helper()
	sites := map[string]bool{}
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			if skippedDirs[info.Name()] {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		isTest := strings.HasSuffix(path, "_test.go")
		for _, m := range docURLCall.FindAllStringSubmatch(string(body), -1) {
			sites[m[1]] = sites[m[1]] || !isTest
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking %s: %v", root, err)
	}
	return sites
}

func TestEveryErrorDocSlugHasAHeadingAndViceVersa(t *testing.T) {
	root := repoRoot(t)

	body, err := os.ReadFile(filepath.Join(root, "docs", "errors", "README.md"))
	if err != nil {
		t.Fatalf("reading docs/errors/README.md: %v", err)
	}
	headings := map[string]bool{}
	for _, m := range docHeading.FindAllStringSubmatch(string(body), -1) {
		headings[m[1]] = true
	}

	sites := slugCallSites(t, root)

	// Denominators. Without these, a walk that matched nothing -- a moved
	// package, a renamed helper, a regex that stopped matching after a
	// gofmt change -- would report two empty difference sets and pass while
	// checking nothing at all.
	if len(headings) < 30 {
		t.Fatalf("only %d error-slug headings found in docs/errors/README.md; "+
			"expected the full set (>=30). The heading format changed or the "+
			"file was truncated -- this test cannot check anything.", len(headings))
	}
	if len(sites) < 30 {
		t.Fatalf("only %d distinct ErrorDocURL slugs found across the repo; "+
			"expected >=30. The call-site scan is broken, not the docs.", len(sites))
	}

	var missingHeading, missingCallSite []string
	for slug := range sites {
		if !headings[slug] {
			missingHeading = append(missingHeading, slug)
		}
	}
	for slug := range headings {
		if !sites[slug] {
			missingCallSite = append(missingCallSite, slug)
		}
	}
	sort.Strings(missingHeading)
	sort.Strings(missingCallSite)

	if len(missingHeading) > 0 {
		t.Errorf("ErrorDocURL is called with %d slug(s) that have no "+
			"\"## <slug>\" heading in docs/errors/README.md. The URL still builds "+
			"and still 200s -- GitHub just ignores the unknown fragment and shows "+
			"the top of the page -- so a user following it reads the wrong section:\n  %s",
			len(missingHeading), strings.Join(missingHeading, "\n  "))
	}
	if len(missingCallSite) > 0 {
		t.Errorf("docs/errors/README.md documents %d error slug(s) that no "+
			"non-test Go code ever passes to ErrorDocURL. Either the code stopped "+
			"emitting them (delete the section) or a slug was renamed on one side "+
			"only (fix the other):\n  %s",
			len(missingCallSite), strings.Join(missingCallSite, "\n  "))
	}
}
