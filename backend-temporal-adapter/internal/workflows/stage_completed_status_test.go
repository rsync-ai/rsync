package workflows

// Census of the pipeline-level Status carried by every STAGE_COMPLETED this
// package emits — the temporal-adapter half of the invariant PR #736 fixed in
// the orchestrator (backend-orchestrator/internal/workers/stage_completed_status_test.go).
//
// The invariant: StateUpdateInput.Status is the status of the whole PIPELINE
// RUN, not of the stage that just finished. StageGroup/Stage name the stage;
// stage_state is derived from EventType by stateForEventType() in activities.go.
// So a STAGE_COMPLETED must never carry a run-terminal status — doing so tells
// stateUpdateActivity to write pipeline_progress.status = 'completed' while the
// run is still executing, and the UI reads that as "the run finished".
//
// Why a census rather than a behavioural test: the defect is a one-word literal
// in a workflow struct. A behavioural test would need a Temporal test env per
// site and would still only cover the sites someone remembered to write one for.
// Parsing every site is the only form that is exhaustive by construction.
//
// This adapter version resolves simple identifiers, unlike the orchestrator's.
// That matters: the live defect here was `Status: execStatus` — a variable, not
// a literal — which a literals-only scan reports as "0 problems" while walking
// straight past it. Any Status expression this test cannot resolve is a FAILURE,
// never a silent skip; an unreadable site is exactly where the next one hides.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"strconv"
	"strings"
	"testing"
)

// Statuses a stage-level event may legitimately carry. Anything else is either
// run-terminal ("completed", "failed", "cancelled") or unknown — both fail.
var nonTerminalStageStatus = map[string]bool{
	"processing":       true,
	"running":          true, // streaming handoff: the run really is still running
	"pending":          true,
	"waiting_for_user": true,
}

func TestStageCompletedNeverCarriesRunTerminalStatus(t *testing.T) {
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", func(fi fs.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatalf("parse package: %v", err)
	}

	total := 0    // STAGE_COMPLETED composite literals found anywhere in the file
	asserted := 0 // ...of those, the ones this test actually reached a verdict on

	for _, pkg := range pkgs {
		for _, file := range pkg.Files {
			// Pass 1: count every site, including any outside a function body.
			ast.Inspect(file, func(n ast.Node) bool {
				if cl, ok := n.(*ast.CompositeLit); ok && isStageCompleted(cl) {
					total++
				}
				return true
			})

			// Pass 2: assert, scoped to a function so identifiers can be resolved.
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				assigns := stringAssignmentsIn(fn)

				ast.Inspect(fn.Body, func(n ast.Node) bool {
					cl, ok := n.(*ast.CompositeLit)
					if !ok || !isStageCompleted(cl) {
						return true
					}
					asserted++
					pos := fset.Position(cl.Pos())

					statusExpr := fieldValue(cl, "Status")
					if statusExpr == nil {
						// No Status field: the zero value "" is written verbatim.
						t.Errorf("%s: STAGE_COMPLETED with no Status field — "+
							"writes an empty pipeline status", pos)
						return true
					}

					values, ok := resolveStringValues(statusExpr, assigns)
					if !ok {
						t.Errorf("%s: STAGE_COMPLETED Status is an expression this "+
							"census cannot resolve (%T). Either use a string literal "+
							"or a local variable assigned only string literals — an "+
							"unreadable site is an unguarded site.", pos, statusExpr)
						return true
					}
					for _, v := range values {
						if !nonTerminalStageStatus[v] {
							t.Errorf("%s: STAGE_COMPLETED carries Status=%q. Status is "+
								"the PIPELINE status, not the stage's — this reports the "+
								"whole run finished while it is still executing. Use "+
								"\"processing\"; the stage's own outcome is already "+
								"recorded via stateForEventType(EventType)=\"succeeded\".",
								pos, v)
						}
					}
					return true
				})
			}
		}
	}

	// A census that matches nothing passes vacuously and guards nothing.
	if total == 0 {
		t.Fatal("found no STAGE_COMPLETED sites — the census matched nothing, so it " +
			"asserts nothing. Did the field names or struct shape change?")
	}
	// ...and one that reaches a verdict on only some of what it found is worse:
	// it looks exhaustive while skipping sites.
	if asserted != total {
		t.Fatalf("found %d STAGE_COMPLETED sites but only reached a verdict on %d — "+
			"%d site(s) sit outside any function body and went unchecked",
			total, asserted, total-asserted)
	}
	t.Logf("checked %d STAGE_COMPLETED site(s)", asserted)
}

// isStageCompleted reports whether cl has EventType: "STAGE_COMPLETED".
func isStageCompleted(cl *ast.CompositeLit) bool {
	v := fieldValue(cl, "EventType")
	lit, ok := v.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return false
	}
	s, err := strconv.Unquote(lit.Value)
	return err == nil && s == "STAGE_COMPLETED"
}

// fieldValue returns the value expression of the named keyed field, or nil.
func fieldValue(cl *ast.CompositeLit, name string) ast.Expr {
	for _, elt := range cl.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		if key, ok := kv.Key.(*ast.Ident); ok && key.Name == name {
			return kv.Value
		}
	}
	return nil
}

// stringAssignmentsIn collects every string-literal value assigned to each local
// identifier anywhere in fn. Deliberately flow-insensitive: a variable that is
// run-terminal on ANY branch is a defect, regardless of which branch reaches the
// emit site.
func stringAssignmentsIn(fn *ast.FuncDecl) map[string][]string {
	out := map[string][]string{}
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		assign, ok := n.(*ast.AssignStmt)
		if !ok {
			return true
		}
		for i, lhs := range assign.Lhs {
			ident, ok := lhs.(*ast.Ident)
			if !ok || i >= len(assign.Rhs) {
				continue
			}
			lit, ok := assign.Rhs[i].(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				// Non-literal assignment: record a sentinel so resolution fails
				// loudly rather than reporting only the literal branches.
				out[ident.Name] = append(out[ident.Name], unresolvable)
				continue
			}
			if s, err := strconv.Unquote(lit.Value); err == nil {
				out[ident.Name] = append(out[ident.Name], s)
			}
		}
		return true
	})
	return out
}

// unresolvable marks an identifier assigned something this census cannot read.
// It can never collide with a real status: statuses are lowercase identifiers.
const unresolvable = "\x00unresolvable"

// resolveStringValues reduces a Status expression to the set of strings it can
// hold. ok=false means "cannot tell" — which the caller treats as a failure.
func resolveStringValues(expr ast.Expr, assigns map[string][]string) ([]string, bool) {
	switch e := expr.(type) {
	case *ast.BasicLit:
		if e.Kind != token.STRING {
			return nil, false
		}
		s, err := strconv.Unquote(e.Value)
		if err != nil {
			return nil, false
		}
		return []string{s}, true
	case *ast.Ident:
		values, found := assigns[e.Name]
		if !found || len(values) == 0 {
			return nil, false
		}
		for _, v := range values {
			if v == unresolvable {
				return nil, false
			}
		}
		return values, true
	default:
		return nil, false
	}
}
