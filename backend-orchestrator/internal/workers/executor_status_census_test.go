package workers

// Census of every status ExecuteTask can hand to executeWithHealer.
//
// executor_terminal_status_test.go pins the statuses that were KNOWN when
// KI-EXEC-HEALER-RERUNS-NONTERMINAL-STATUS was fixed. Its list was assembled by
// reading the emitting sites by hand, and that is precisely how it came to be
// incomplete: `unverified_completion` is set through a local variable rather
// than as a literal `Status:` field, so it does not look like a status at the
// call site and was not on the list.
//
// This file does not restate the list. It PARSES the sibling package and fails
// if any status found there is unaccounted for, so the next one added upstream
// cannot slip past the allowlist in silence.
//
// Known limits, stated rather than papered over: a status that only ever exists
// as a value forwarded from another struct (executor.go:6881 `Status:
// statusResp.Status`) is invisible to a static scan. This census covers the
// literal-bearing paths, which is where all nine of today's statuses live.

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"strconv"
	"strings"
	"testing"
)

// classified is the ledger. Every status the census finds must appear here with
// a deliberate verdict; adding a status upstream without adding a row is the
// failure this file exists to produce.
//
// true  = non-failure. executeWithHealer must return it untouched.
// false = failure. The healer must see it.
var classified = map[string]bool{
	"success":            true,
	"needs_continuation": true,
	"running":            true,
	"cancelled":          true,
	"stopped":            true,

	// The table-selection HITL park — re-running it repeats full schema
	// discovery against the customer's source database.
	"waiting_for_table_selection": true,

	// The rows/blobs were dispatched; only the ACK could not be confirmed
	// inside the reconcile deadline. blob_lane.go:318-321 states the contract
	// outright — "ambiguous, never a hard fail (the sink retries the ack
	// forever)". It carries Error:"" , so suggestRecoveryAction matches none of
	// its rules and falls through to the default retry_with_backoff, and
	// executeWithHealer then re-dispatches the WHOLE transfer. Against a
	// destination without an upsert key that duplicates the customer's rows,
	// to recover a run that had in all likelihood already succeeded.
	"unverified_completion": true,

	// A connector an operator PAUSED on purpose (executor.go:6779, via
	// pipelineStatus). Re-running ExecuteTask restarts what somebody
	// deliberately stopped — the same wrong as re-running "stopped".
	"paused": true,

	// Genuine data loss. These must reach the healer.
	"silent_drop_detected":         false,
	"silent_partial_drop_detected": false,
	"failed":                       false,
	"failure":                      false,
	"error":                        false,
	"pending":                      false,
	"processing":                   false,

	// Connector state could not be determined. Left a failure deliberately: an
	// indeterminate CDC connector is worth the healer's attention, and the task
	// that emits it is a status probe, so a re-run is cheap.
	"unknown": false,
}

// notAStatus records literals the scan over-catches. The heuristic keys on the
// NAME `…Status`, and several unrelated structs have such a field, so these are
// listed with the reason rather than mislabelled as failures in the ledger
// above — a wrong entry there would be indistinguishable from a real verdict.
var notAStatus = map[string]string{
	"healthy":   "StreamingPipelineInfo.HealthStatus (executor.go:6541), not ExecutorResponse",
	"unhealthy": "local healthStatus → Result[\"health_status\"] (executor.go:6765), not ExecutorResponse",
	"active":    "cdc.CDCResource.Status (hybrid_cdc.go:106), not ExecutorResponse",
}

func TestEveryEmittedStatusIsClassified(t *testing.T) {
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, "../agents/executor", func(fi fs.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatalf("parse ../agents/executor: %v", err)
	}

	found := map[string]string{} // status -> first position that emits it

	record := func(lit *ast.BasicLit) {
		if lit == nil || lit.Kind != token.STRING {
			return
		}
		v, err := strconv.Unquote(lit.Value)
		if err != nil || v == "" {
			return
		}
		if _, seen := found[v]; !seen {
			found[v] = fset.Position(lit.Pos()).String()
		}
	}

	// A name is status-bearing if it is `Status` or ends in `Status`/`status`
	// (status, finalStatus, pipelineStatus, d.Status). That heuristic is what
	// catches the variable-assigned path the hand-written list missed.
	isStatusName := func(n string) bool {
		return n == "Status" || n == "status" ||
			strings.HasSuffix(n, "Status") || strings.HasSuffix(n, "status")
	}
	tailName := func(e ast.Expr) string {
		switch x := e.(type) {
		case *ast.Ident:
			return x.Name
		case *ast.SelectorExpr:
			return x.Sel.Name
		}
		return ""
	}

	for _, pkg := range pkgs {
		for _, file := range pkg.Files {
			ast.Inspect(file, func(n ast.Node) bool {
				switch node := n.(type) {
				case *ast.KeyValueExpr: // Status: "literal"
					if isStatusName(tailName(node.Key)) {
						record(asBasicLit(node.Value))
					}
				case *ast.AssignStmt: // status = "literal" / d.Status = "literal"
					for i, lhs := range node.Lhs {
						if i < len(node.Rhs) && isStatusName(tailName(lhs)) {
							record(asBasicLit(node.Rhs[i]))
						}
					}
				}
				return true
			})
		}
	}

	if len(found) == 0 {
		t.Fatal("census found no statuses at all — the scan is broken, not the code")
	}

	for status, pos := range found {
		if _, skip := notAStatus[status]; skip {
			continue
		}
		nonFailure, ok := classified[status]
		if !ok {
			t.Errorf("status %q emitted at %s is not in the classified ledger — decide "+
				"whether the healer may re-run it, then add it here and to nonFailureStatuses "+
				"if it is a non-failure (or to notAStatus if it is a field on another struct)",
				status, pos)
			continue
		}
		if got := isNonFailureStatus(status); got != nonFailure {
			t.Errorf("isNonFailureStatus(%q) = %v, want %v (emitted at %s)",
				status, got, nonFailure, pos)
		}
	}
}

func asBasicLit(e ast.Expr) *ast.BasicLit {
	lit, _ := e.(*ast.BasicLit)
	return lit
}

// A run whose only fault is that its tables were source-permission-denied must
// not be retried blind.
//
// executor.go:5185 reports that case as silent_partial_drop_detected and says so
// in the error text, adding "The rows that DID land stay landed". A blind re-run
// therefore re-transfers every table that already succeeded in order to re-fail
// on the ones the source will deny again — the denial is deterministic. None of
// suggestRecoveryAction's rules match the wording it actually emits ("source
// denied access", "permissions/scopes"), so it lands on the default branch and
// gets retry_with_backoff.
func TestSourcePermissionDeniedPartialDropIsNotRetried(t *testing.T) {
	// Quoted from executor.go:5186 so a reworded message breaks this test rather
	// than silently reopening the bug.
	errMsg := "silent_partial_drop_detected: 2 table(s) skipped (source denied access): " +
		"customers, orders. Grant the missing source permissions/scopes and retry; " +
		"the remaining tables landed."

	errorContext := map[string]interface{}{
		"error_message": errMsg,
		"attempt_count": 0,
	}
	got := (&ExecutorWorker{}).suggestRecoveryAction(context.Background(), errorContext)
	if got == "retry_with_backoff" || got == "retry_smaller_batch" {
		t.Fatalf("suggestRecoveryAction = %q for a source-permission denial — that re-runs "+
			"the whole batch, re-transferring the tables that already landed, to hit the "+
			"same deterministic denial. reasoning=%v", got, errorContext["reasoning"])
	}
}
