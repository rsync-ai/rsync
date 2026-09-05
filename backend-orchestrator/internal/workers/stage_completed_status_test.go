package workers

// Census of the pipeline-level Status every STAGE_COMPLETED event carries.
//
// ProgressEvent.Status is NOT the stage's status — it is the PIPELINE's status.
// The api-gateway projector writes it through verbatim
// (api-gateway/internal/projector/event_projector.go:320,
// `status = ... THEN EXCLUDED.status`) into pipeline_progress.status, which is
// what the UI, the list view's derived_status and the /flow terminal check all
// read. A *stage* event that claims a *run-terminal* status therefore reports
// the whole run finished while it is still in flight.
//
// That is not hypothetical. On prod 2026-08-03, execution fa0122a9 emitted
// (pipeline_run_events.occurred_at):
//
//	STAGE_COMPLETED | executor | completed  | 7/8 | 14:53:29   <- this bug
//	STAGE_COMPLETED | executor | processing | -/- | 14:53:38   <- adapter, contradicts it
//	PIPELINE_COMPLETED         | completed  |     | 14:53:42   <- the real terminal event
//
// The run-terminal claim was published 17.8 s before the execution actually
// ended (executions.end_time 14:53:46.781) and stood as the newest
// status-bearing event for 9 s, until the temporal-adapter contradicted it.
//
// What makes it survivable on the happy path is precisely that adapter event.
// On a run that dies after step 7 there is no corrective event, so 'completed'
// is the last word and the failure is never surfaced — a false success, which
// is exactly what post-deploy regression check 2 exists to catch.
//
// Ten of the eleven STAGE_COMPLETED sites in this package say "processing"; the
// eleventh is the streaming handoff described below. This file pins that
// convention so the defect cannot come back, and so a new emit site has to make
// a deliberate choice rather than copy the wrong neighbour. The count is not
// hardcoded anywhere — the test logs what it actually checked, so a drifting
// comment cannot make the assertion drift with it.
//
// This census resolves identifiers, not just literals. The earlier version
// scanned literal `Status:` fields only and noted the gap as a known limit —
// then the identical defect was found in the temporal-adapter written exactly
// that way (`Status: execStatus`), where a literals-only scan reports "0
// problems" while walking past it. A stated limit is still a hole. Any Status
// expression this test cannot resolve is now a FAILURE rather than a silent
// skip, and the count of sites *asserted on* is checked against the count of
// sites *found* — a scan that matches ten sites and reaches a verdict on three
// looks exhaustive and is not.
//
// Sibling census, same invariant, other service:
// backend-temporal-adapter/internal/workflows/stage_completed_status_test.go

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"strconv"
	"strings"
	"testing"
)

// nonTerminalEventStatus is the allowlist: statuses that do NOT mark a run
// finished, and so are safe for a mid-run stage event to publish.
//
// Anything else is run-terminal. Adding a status here is a deliberate assertion
// that the UI and derived_status will not treat it as "this run is over".
var nonTerminalEventStatus = map[string]bool{
	"processing": true, // the convention: 9 of 10 sites in this package

	// The streaming handoff (executor.go:464-467). The executor started a CDC/
	// streaming pipeline and the workflow proceeds to monitor_streaming; the
	// run is emphatically not over. UpdatePipelineStatusActivity later
	// reconciles the row to 'running' for the same reason
	// (backend-temporal-adapter/internal/workflows/pipeline_status_activity.go:72).
	"running": true,

	"pending":          true,
	"waiting_for_user": true,
}

func TestStageCompletedNeverClaimsRunTerminalStatus(t *testing.T) {
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", func(fi fs.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatalf("parse workers package: %v", err)
	}

	found := 0    // STAGE_COMPLETED composite literals anywhere in the package
	asserted := 0 // ...of those, the ones this test reached a verdict on

	for _, pkg := range pkgs {
		for _, file := range pkg.Files {
			// Pass 1: count every site, including any outside a function body.
			ast.Inspect(file, func(n ast.Node) bool {
				if cl, ok := n.(*ast.CompositeLit); ok && isStageCompletedLit(cl) {
					found++
				}
				return true
			})

			// Pass 2: assert, scoped to a function so identifiers resolve.
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				assigns := stringAssignmentsIn(fn)

				ast.Inspect(fn.Body, func(n ast.Node) bool {
					cl, ok := n.(*ast.CompositeLit)
					if !ok || !isStageCompletedLit(cl) {
						return true
					}
					asserted++
					pos := fset.Position(cl.Pos())

					expr := fieldValueOf(cl, "Status")
					if expr == nil {
						t.Errorf("%s: STAGE_COMPLETED has no Status field — the zero "+
							"value \"\" is published as the pipeline status.", pos)
						return true
					}

					statuses, ok := resolveStringValues(expr, assigns)
					if !ok {
						t.Errorf("%s: STAGE_COMPLETED Status is an expression this census "+
							"cannot resolve (%T). Use a string literal, or a local variable "+
							"assigned only string literals — an unreadable site is an "+
							"unguarded one, which is how this same bug survived in the "+
							"temporal-adapter.", pos, expr)
						return true
					}
					for _, status := range statuses {
						if nonTerminalEventStatus[status] {
							continue
						}
						t.Errorf(
							"%s: STAGE_COMPLETED publishes Status:%q, which is run-terminal.\n"+
								"    ProgressEvent.Status is the PIPELINE status and the projector writes it\n"+
								"    verbatim into pipeline_progress.status (event_projector.go:320), so this\n"+
								"    marks the whole run finished mid-flight. Use \"processing\" like the other\n"+
								"    STAGE_COMPLETED sites, and let PIPELINE_COMPLETED carry the terminal status.",
							pos, status)
					}
					return true
				})
			}
		}
	}

	// Guard against a silently-passing scan: if the AST walk stops matching
	// (renamed field, changed literal), this test would go green while
	// asserting nothing.
	if found == 0 {
		t.Fatal("census matched no STAGE_COMPLETED emit sites — the scan is broken, not the code")
	}
	// And against a partially-passing one, which is worse: it looks exhaustive.
	if asserted != found {
		t.Fatalf("found %d STAGE_COMPLETED sites but reached a verdict on only %d — "+
			"%d site(s) sit outside any function body and went unchecked",
			found, asserted, found-asserted)
	}
	t.Logf("checked %d STAGE_COMPLETED site(s)", asserted)
}

// isStageCompletedLit reports whether cl has EventType: "STAGE_COMPLETED".
func isStageCompletedLit(cl *ast.CompositeLit) bool {
	lit, ok := fieldValueOf(cl, "EventType").(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return false
	}
	v, err := strconv.Unquote(lit.Value)
	return err == nil && v == "STAGE_COMPLETED"
}

// fieldValueOf returns the value expression of the named keyed field, or nil.
func fieldValueOf(cl *ast.CompositeLit, name string) ast.Expr {
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

// unresolvableStatus marks an identifier assigned something this census cannot
// read. It cannot collide with a real status: those are lowercase identifiers.
const unresolvableStatus = "\x00unresolvable"

// stringAssignmentsIn collects every string-literal value assigned to each local
// identifier anywhere in fn. Deliberately flow-insensitive: a variable that is
// run-terminal on ANY branch is a defect, whichever branch reaches the emit site.
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
				out[ident.Name] = append(out[ident.Name], unresolvableStatus)
				continue
			}
			if v, err := strconv.Unquote(lit.Value); err == nil {
				out[ident.Name] = append(out[ident.Name], v)
			}
		}
		return true
	})
	return out
}

// resolveStringValues reduces a Status expression to the set of strings it can
// hold. ok=false means "cannot tell", which the caller treats as a failure.
func resolveStringValues(expr ast.Expr, assigns map[string][]string) ([]string, bool) {
	switch e := expr.(type) {
	case *ast.BasicLit:
		if e.Kind != token.STRING {
			return nil, false
		}
		v, err := strconv.Unquote(e.Value)
		if err != nil {
			return nil, false
		}
		return []string{v}, true
	case *ast.Ident:
		values, found := assigns[e.Name]
		if !found || len(values) == 0 {
			return nil, false
		}
		for _, v := range values {
			if v == unresolvableStatus {
				return nil, false
			}
		}
		return values, true
	default:
		return nil, false
	}
}
