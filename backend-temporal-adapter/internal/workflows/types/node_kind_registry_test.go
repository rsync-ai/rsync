package types

// Registry conformance for the NodeKind enum.
//
// AllNodeKinds is the list the runtime census in package workflows iterates. A
// Go compiler can catch a constant that DISAPPEARS (the literal stops
// compiling) but nothing in the language catches one that is ADDED — untyped
// string constants have no exhaustiveness check to lean on, and a new kind
// costs one line at the declaration site and zero anywhere else.
//
// This test closes that direction, and only that direction. It reads the
// constant DECLARATIONS out of the package source and requires every constant
// in the package to be classified: either it is a node kind (NodeKind* — and
// then it must appear in AllNodeKinds with a non-empty, unique string value) or
// it is named in knownNonNodeKindConstants below.
//
// Classification is by NAME, not by value, on purpose. Keying on "is the value
// a string literal" would let `KindWebhook = someOtherConst` through; keying on
// "is it in the anchor const block" would let a second const block through.
// Every new constant in this package therefore has to be triaged by a human
// exactly once, and the failure message says so. That is deliberately louder
// than it is convenient: this package is three files and its constants change
// about once a year.
//
// Residual hole, stated plainly: a node kind deliberately misfiled under an
// existing sibling name (e.g. `NodeStatusWebhook = "webhook"`, then used as a
// node.Kind) is classified as a status and escapes. Nothing here can tell
// intent from a name. What it does guarantee is that the misfiling has to be
// deliberate and has to be written next to a comment block that says these are
// statuses.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

const nodeKindNamePrefix = "NodeKind"

// knownNonNodeKindConstants is every constant declared in this package that is
// NOT a node kind. Adding a constant to this package means adding its name
// here — or naming it NodeKind* and adding it to AllNodeKinds. There is no
// third option, and that is the point.
var knownNonNodeKindConstants = map[string]string{
	"GraphSchemaVersion": "graph schema version string",

	"EdgeTypeData":        "edge type, not a node kind",
	"EdgeTypeControlThen": "edge type, not a node kind",
	"EdgeTypeControlElse": "edge type, not a node kind",

	"NodeStatusPending":   "runtime node status, not a node kind",
	"NodeStatusRunning":   "runtime node status, not a node kind",
	"NodeStatusWaiting":   "runtime node status, not a node kind",
	"NodeStatusCompleted": "runtime node status, not a node kind",
	"NodeStatusFailed":    "runtime node status, not a node kind",
	"NodeStatusSkipped":   "runtime node status, not a node kind",
}

func TestNodeKindConstantsAreRegistered(t *testing.T) {
	declaredKinds, otherConsts := parsePackageConstants(t)

	// Positive denominator: a scan that matched nothing would "pass" everything.
	if len(declaredKinds) == 0 {
		t.Fatalf("parsed zero %s* constants from this package — the scan matched "+
			"nothing, so it asserts nothing", nodeKindNamePrefix)
	}
	if len(otherConsts) == 0 {
		t.Fatalf("parsed zero non-node-kind constants from this package — the scan " +
			"is not seeing whole const blocks it should be seeing")
	}

	// 1. Every constant in the package is classified.
	for _, name := range sortedKeys(otherConsts) {
		if _, known := knownNonNodeKindConstants[name]; known {
			continue
		}
		t.Errorf("constant %s (declared at %s) is neither a NodeKind* constant nor "+
			"listed in knownNonNodeKindConstants.\n"+
			"    If it IS a workflow node kind: rename it to %s<Something> and add it "+
			"to AllNodeKinds — otherwise nothing routes it and no test can tell.\n"+
			"    If it is NOT: add its name to knownNonNodeKindConstants with a "+
			"one-line reason.",
			name, otherConsts[name], nodeKindNamePrefix)
	}

	// 2. No stale exemptions: an exemption naming a constant that no longer
	//    exists means the list has drifted from the source.
	for _, name := range sortedKeys(knownNonNodeKindConstants) {
		if _, found := otherConsts[name]; !found {
			t.Errorf("knownNonNodeKindConstants names %q, which this package no longer "+
				"declares — stale exemption, delete it", name)
		}
	}

	// 3. AllNodeKinds covers exactly the declared NodeKind* constants.
	registered := map[string]bool{}
	for _, value := range AllNodeKinds {
		if value == "" {
			t.Errorf("AllNodeKinds contains an empty string — an unroutable node kind")
			continue
		}
		if registered[value] {
			t.Errorf("AllNodeKinds lists %q twice; the census would silently count it once", value)
		}
		registered[value] = true
	}

	declaredValues := map[string]string{} // value -> constant name
	for _, name := range sortedKeys(declaredKinds) {
		value := declaredKinds[name]
		if value == "" {
			t.Errorf("%s declares an empty value — an unroutable node kind", name)
			continue
		}
		if prev, dup := declaredValues[value]; dup {
			t.Errorf("%s and %s both declare the wire value %q — one of them can never "+
				"be routed on its own", prev, name, value)
		}
		declaredValues[value] = name

		if !registered[value] {
			t.Errorf("%s = %q is declared but missing from AllNodeKinds, so the runtime "+
				"conformance census in package workflows never sees it. Add it to "+
				"AllNodeKinds — and expect the census to then tell you nothing routes it.",
				name, value)
		}
	}

	for _, value := range AllNodeKinds {
		if _, ok := declaredValues[value]; !ok {
			t.Errorf("AllNodeKinds contains %q, which no %s* constant declares — stale "+
				"entry", value, nodeKindNamePrefix)
		}
	}

	t.Logf("%d NodeKind constant(s) declared, %d registered in AllNodeKinds, "+
		"%d other constant(s) classified", len(declaredKinds), len(AllNodeKinds), len(otherConsts))
}

// parsePackageConstants returns (NodeKind* constant name -> string value) and
// (every other constant name -> declaration position). It reads the package's
// own non-test sources; a parse error or a NodeKind* constant whose value is not
// a plain string literal is fatal rather than skipped, because an unreadable
// constant is an unguarded one.
func parsePackageConstants(t *testing.T) (map[string]string, map[string]string) {
	t.Helper()

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}

	kinds := map[string]string{}
	others := map[string]string{}
	fset := token.NewFileSet()
	parsed := 0

	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(".", name), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		parsed++

		for _, decl := range file.Decls {
			gd, ok := decl.(*ast.GenDecl)
			if !ok || gd.Tok != token.CONST {
				continue
			}
			for _, spec := range gd.Specs {
				vs, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				for i, ident := range vs.Names {
					if ident.Name == "_" {
						continue
					}
					pos := fset.Position(ident.Pos()).String()
					if !strings.HasPrefix(ident.Name, nodeKindNamePrefix) {
						others[ident.Name] = pos
						continue
					}
					if i >= len(vs.Values) {
						t.Errorf("%s: %s has no explicit value (iota?) — a node kind must "+
							"be a plain string literal so its wire value is unambiguous", pos, ident.Name)
						continue
					}
					lit, ok := vs.Values[i].(*ast.BasicLit)
					if !ok || lit.Kind != token.STRING {
						t.Errorf("%s: %s is not a plain string literal (%T) — this scan "+
							"cannot read it, and an unreadable constant is an unguarded one",
							pos, ident.Name, vs.Values[i])
						continue
					}
					value, err := strconv.Unquote(lit.Value)
					if err != nil {
						t.Errorf("%s: cannot unquote %s = %s: %v", pos, ident.Name, lit.Value, err)
						continue
					}
					kinds[ident.Name] = value
				}
			}
		}
	}

	if parsed == 0 {
		t.Fatalf("parsed zero non-test .go files in this package — the scan is reading nothing")
	}
	return kinds, others
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
