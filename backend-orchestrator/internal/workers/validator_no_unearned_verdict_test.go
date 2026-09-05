package workers

// The validator stage must not publish a verdict it did not compute.
//
// It used to. `Execute` wrote `"policy_approved": true` into two places -- the
// STAGE_COMPLETED artifact the UI persists, and the TaskResult the correlation
// store forwards -- underneath a comment reading "AUTONOMOUS: Check policy
// (access control, cost limits, compliance, safeguards)" that had no code beneath
// it. The worker emits progress and returns success; it evaluates nothing.
//
// Nothing consumed the key, which is exactly why it survived: there was no
// failing test to write, no user-visible bug, no log line. The cost was a
// machine-readable compliance assertion the product cannot support, sitting in a
// repository about to be made public.
//
// A `grep` at review time cannot prevent the recurrence, because the recurrence
// looks like a feature: someone adds a policy engine, wires the true half, and
// ships the constant while the check is still a TODO. So the assertion is
// structural -- an unconditional boolean literal in this file's output paths
// fails the build until either the check exists or the claim is dropped.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

// verdictish keys are the ones that assert something about compliance, approval,
// or safety -- the class where a hardcoded `true` is a lie rather than a default.
// Substring match, so `policy_approved`, `pii_compliant` and `is_approved` are all
// caught without enumerating them.
var verdictish = []string{"approved", "compliant", "compliance", "validated", "policy_ok", "passed_policy"}

func TestValidatorPublishesNoUnearnedVerdict(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "validator.go", nil, 0)
	if err != nil {
		t.Fatalf("parse validator.go: %v", err)
	}

	// Every `"key": true` (or `false`) composite-literal entry in the file.
	literals := 0
	ast.Inspect(file, func(n ast.Node) bool {
		kv, ok := n.(*ast.KeyValueExpr)
		if !ok {
			return true
		}
		key, ok := kv.Key.(*ast.BasicLit)
		if !ok || key.Kind != token.STRING {
			return true
		}
		value, ok := kv.Value.(*ast.Ident)
		if !ok || (value.Name != "true" && value.Name != "false") {
			return true
		}
		literals++

		name := strings.ToLower(strings.Trim(key.Value, `"`))
		for _, bad := range verdictish {
			if strings.Contains(name, bad) {
				t.Errorf("%s: validator.go publishes %s: %s -- a constant, not a computed result.\n"+
					"This stage runs no policy engine. Either compute the value from a real check, or "+
					"drop the key; do not ship a verdict the code did not reach.",
					fset.Position(kv.Pos()), key.Value, value.Name)
			}
		}
		return true
	})

	t.Logf("scanned %d boolean-literal map entries in validator.go", literals)
}

// TestValidatorVerdictScanIsNotVacuous is the half that matters. The test above
// passes on an empty file, on a renamed file that fails to parse in some future
// refactor, and on a scanner whose matcher never fires -- three ways to get a
// green check that proves nothing.
func TestValidatorVerdictScanIsNotVacuous(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "validator.go", nil, 0)
	if err != nil {
		t.Fatalf("parse validator.go: %v", err)
	}

	// The file must still be the validator, and must still contain the two output
	// paths the deleted claim lived on. If Execute or the artifact envelope moves,
	// this test fails and the scan above has to be retargeted rather than quietly
	// covering nothing.
	var sawExecute, sawTaskResult bool
	ast.Inspect(file, func(n ast.Node) bool {
		if fn, ok := n.(*ast.FuncDecl); ok && fn.Name.Name == "Execute" && fn.Recv != nil {
			sawExecute = true
		}
		if id, ok := n.(*ast.Ident); ok && id.Name == "TaskResult" {
			sawTaskResult = true
		}
		return true
	})
	if !sawExecute {
		t.Error("validator.go has no Execute method -- the scan above is pointed at the wrong code")
	}
	if !sawTaskResult {
		t.Error("validator.go builds no TaskResult -- the scan above is pointed at the wrong code")
	}

	// And the matcher itself must fire on the string it was written for. A typo in
	// `verdictish` is invisible otherwise.
	matched := false
	for _, bad := range verdictish {
		if strings.Contains("policy_approved", bad) {
			matched = true
		}
	}
	if !matched {
		t.Error("verdictish matches nothing in `policy_approved`, the exact key this file was written to keep out")
	}
}
