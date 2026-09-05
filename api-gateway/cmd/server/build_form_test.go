package main

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// Guard against the defect that made this image unbuildable for two days: a build
// recipe that names a .go FILE instead of a package.
//
// `go build -o bin/server cmd/server/main.go` compiles that one file as an ad-hoc
// `command-line-arguments` package. Every sibling in package main becomes invisible,
// so adding ready.go (#820) broke the image with `undefined: readinessVerdict`.
//
// Why no existing check caught it, and why this one is a *test* rather than a CI step:
// every Go CI job runs `go build ./...` — the package form — so CI was green on code
// whose image could not be built. The only job that builds this image is the PR smoke
// gate, and that gate fires on the `datapath` filter, which does not include
// api-gateway/cmd/**. So the PR that broke it never ran the gate, and the failure
// surfaced two PRs later on an unrelated change. A `go test ./...` guard runs in the
// default suite on every Go PR, which is the one thing CI does reliably (a build-tagged
// test would be zero protection — CI passes no -tags).
//
// This lives beside the file whose sibling was invisible, on purpose.

// goFileArg matches a `go build` argument that is a .go source path rather than a
// package path. Anchored on a whitespace boundary so `./cmd/server` and flags like
// `-ldflags "-s -w"` never match.
var goFileArg = regexp.MustCompile(`go build[^\n]*?\s[^\s]+\.go(\s|$)`)

func TestNoBuildRecipeNamesAGoFileInsteadOfAPackage(t *testing.T) {
	root := repoRootForBuildForm(t)

	// Every place a Go binary is built. Dockerfiles are what ship; the Makefile is
	// what a developer runs; both carried the same defect, and the Makefile's
	// orchestrator target additionally pointed at a cmd/server that does not exist.
	var recipes []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // an unreadable subtree is not this test's business
		}
		if info.IsDir() {
			switch info.Name() {
			case ".git", "node_modules", "venv", "__pycache__", ".next":
				return filepath.SkipDir
			}
			return nil
		}
		base := info.Name()
		if strings.HasPrefix(base, "Dockerfile") || base == "Makefile" {
			recipes = append(recipes, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking %s: %v", root, err)
	}

	// A count of zero is not an error to a range loop — it is how a guard silently
	// stops guarding. Assert the denominator before trusting the verdict.
	if len(recipes) == 0 {
		t.Fatalf("found no Dockerfile/Makefile under %s — the test is broken, not the repo", root)
	}

	var offenders []string
	scanned := 0
	for _, path := range recipes {
		b, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		scanned++
		for i, line := range strings.Split(string(b), "\n") {
			trimmed := strings.TrimSpace(line)
			// Skip comments — the fix commit explains the defect in prose that
			// necessarily contains the broken form.
			if strings.HasPrefix(trimmed, "#") {
				continue
			}
			if !strings.Contains(line, "go build") {
				continue
			}
			if goFileArg.MatchString(line) {
				rel, _ := filepath.Rel(root, path)
				offenders = append(offenders, rel+":"+itoa(i+1)+": "+trimmed)
			}
		}
	}
	if scanned == 0 {
		t.Fatalf("read none of the %d recipe files — the test is broken", len(recipes))
	}

	if len(offenders) > 0 {
		t.Errorf("%d build recipe(s) name a .go file instead of a package. "+
			"Use ./cmd/<name> — naming the file hides every sibling in package main "+
			"and the failure will not show up in `go build ./...`:\n  %s",
			len(offenders), strings.Join(offenders, "\n  "))
	}
	t.Logf("scanned %d build recipes, %d offenders", scanned, len(offenders))
}

// TestThisPackageHasMoreThanOneFile is the control. The guard above is only
// meaningful while package main is actually split across files — if it ever
// collapsed back to a single main.go, the single-file build form would start
// working again and the guard would be protecting nothing. This fails first and
// tells the next reader why the guard exists.
func TestThisPackageHasMoreThanOneFile(t *testing.T) {
	dir := filepath.Dir(mustAbsForBuildForm(t, "main.go"))
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("reading %s: %v", dir, err)
	}
	n := 0
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".go") && !strings.HasSuffix(e.Name(), "_test.go") {
			n++
		}
	}
	if n < 2 {
		t.Errorf("package main has %d non-test .go file(s); the single-file build guard "+
			"assumes more than one. If main.go is genuinely alone again, say so explicitly "+
			"rather than letting the guard quietly stop mattering.", n)
	}
	t.Logf("package main spans %d non-test files", n)
}

func repoRootForBuildForm(t *testing.T) string {
	t.Helper()
	// api-gateway/cmd/server -> repo root
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatalf("resolving repo root: %v", err)
	}
	// Anchor on something that only the repo root has, so a moved test fails loudly
	// instead of silently scanning the wrong tree and reporting zero offenders.
	//
	// LICENSE, not CLAUDE.md. CLAUDE.md is on scripts/flip/excludes.txt, so the public
	// repo does not have it and this t.Fatalf would have been the first public CI
	// run's Go failure -- a guard killed by the absence of a file it only ever used as
	// a landmark. LICENSE is in neither cut list (it is the one file a public repo is
	// least likely to lose), is unique to the root, and is unrelated to what the test
	// asserts, which is exactly what a landmark should be.
	if _, err := os.Stat(filepath.Join(root, "LICENSE")); err != nil {
		t.Fatalf("%s does not look like the repo root (no LICENSE): %v", root, err)
	}
	return root
}

func mustAbsForBuildForm(t *testing.T, rel string) string {
	t.Helper()
	p, err := filepath.Abs(rel)
	if err != nil {
		t.Fatalf("resolving %s: %v", rel, err)
	}
	return p
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(b[pos:])
}
