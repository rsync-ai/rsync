package workers

import (
	"context"
	"testing"
)

// Pins KI-EXEC-HEALER-RERUNS-NONTERMINAL-STATUS.
//
// executeWithHealer returned early only for "success" and "needs_continuation".
// Every other status — including the ones that mean "this went fine, it just
// isn't finished" — fell through into the healer, which classified them as
// failures and re-ran the entire ExecuteTask. Observed 3× on prod:
//
//	executor-8defe264  batch HITL park → full re-run
//	executor-de50563b  CDC HITL park   → full re-run
//	executor-9089fb7c  empty error     → a full CDC start ran a second time
//
// The impact is not just a wasted retry: re-running a table-selection park
// re-runs full schema discovery against the customer's source database, so the
// load we put on their infrastructure silently doubles every time a user is
// asked to pick a table.
func TestNonFailureStatusesNeverReachTheHealer(t *testing.T) {
	// Statuses are quoted from the site that emits them, so renaming one breaks
	// this test instead of silently reopening the bug.
	cases := []struct {
		status     string
		nonFailure bool
		emittedAt  string
	}{
		{"success", true, "agents/executor/executor.go:2085"},
		{"needs_continuation", true, "agents/executor/executor.go:5006"},
		// The two shapes that caused the prod re-runs.
		{"running", true, "agents/executor/executor.go:3425 (CDC), :6549 (streaming)"},
		{"waiting_for_table_selection", true, "agents/executor/executor.go:1306, :1349, :1530"},
		// A run the user stopped, and one the operator stopped. Re-running
		// either is as wrong as re-running a park.
		{"cancelled", true, "agents/executor/executor.go:1158"},
		{"stopped", true, "agents/executor/executor.go:6637"},
		// Genuine failures MUST still reach the healer — an allowlist that
		// swallowed these would trade one bug for a worse one.
		{"failed", false, "agents/executor/executor.go:1684 and 4 sibling sites"},
		{"", false, "zero value — an executor that returned nothing is a failure"},
		{"partial_success", false, "set by the healer itself, never returned by ExecuteTask"},
	}

	for _, tc := range cases {
		t.Run(tc.status, func(t *testing.T) {
			if got := isNonFailureStatus(tc.status); got != tc.nonFailure {
				t.Fatalf("isNonFailureStatus(%q) = %v, want %v — emitted at %s",
					tc.status, got, tc.nonFailure, tc.emittedAt)
			}
		})
	}
}

// TestHITLParkClassifiesAsRetryIfItReachesTheHealer is the other half of the
// proof, and it passes both before and after the fix on purpose.
//
// It shows *why* the allowlist above has to catch these statuses: once a park
// reaches suggestRecoveryAction there is no rule that recognises it, so it lands
// on the attemptCount<2 default and comes back "retry_with_backoff" — and the
// caller's backoff is `attemptCount*2` seconds, i.e. **zero** on the first
// attempt. That is why the observed prod gap was 127 ms and looked nothing like
// a retry. Fixing the classifier instead of the allowlist would not have been
// enough: the park is not an error to classify, it is a normal outcome.
func TestHITLParkClassifiesAsRetryIfItReachesTheHealer(t *testing.T) {
	w := &ExecutorWorker{}

	for _, errMsg := range []string{
		// Verbatim from agents/executor/executor.go:1349 / :1530.
		"Select which table(s)/resource(s) to sync (4 available).",
		// executor-9089fb7c carried no error string at all.
		"",
	} {
		got := w.suggestRecoveryAction(context.Background(), map[string]interface{}{
			"error_message": errMsg,
			"attempt_count": 0,
		})
		if got != "retry_with_backoff" {
			t.Fatalf("suggestRecoveryAction(%q) = %q, want retry_with_backoff — if this "+
				"changed, the re-run damage described above changed too and the comment "+
				"on isNonFailureStatus needs revisiting", errMsg, got)
		}
	}
}
