package heal

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/rsync-ai/backend-orchestrator/internal/execrows"
)

// sweptCTE returns only the body of the `swept` CTE — the one that UPDATEs executions.
// Scoping matters here: sweepZombiesQuery mentions `executions` in three places, and an
// assertion against the whole string would be satisfied by the predicate appearing in
// any of them while the UPDATE itself still reaps the anchor.
func sweptCTE(t *testing.T) string {
	t.Helper()
	start := strings.Index(sweepZombiesQuery, "WITH swept AS (")
	if start < 0 {
		t.Fatal("sweepZombiesQuery no longer opens with a `swept` CTE")
	}
	end := strings.Index(sweepZombiesQuery[start:], "RETURNING e.id, e.pipeline_id")
	if end < 0 {
		t.Fatal("the `swept` CTE no longer ends with `RETURNING e.id, e.pipeline_id`")
	}
	return sweepZombiesQuery[start : start+end]
}

// TestSweepZombiesQuery_SkipsSyntheticCDCAnchor is the whole point of the fix.
//
// The sink writes ONE executions row per CDC pipeline as an audit anchor, with
// id = pipeline_id, status='running' and end_time NULL, and never closes it —
// ensureExecutionRowForCDCAudit is ON CONFLICT DO NOTHING, so nothing ever reopens
// it either. That row matches the zombie predicate BY CONSTRUCTION the moment it
// turns 4h old.
//
// The CDC carve-out that already exists in this query does NOT save it: that carve-out
// lives in the pipelines_closed CTE and guards the `pipelines` row. Nothing guarded the
// executions row, so every healthy CDC stream got a fabricated
// "zombie: execution timed out" failure stamped onto its audit anchor, permanently —
// which then fed the healer's own candidate sweep, the diagnoser (which recognised its
// own handwriting and asked an operator to approve re-running a working stream), and
// the connector-health rollup.
func TestSweepZombiesQuery_SkipsSyntheticCDCAnchor(t *testing.T) {
	if !strings.Contains(sweptCTE(t), execrows.NotSynthetic) {
		t.Fatalf("the `swept` CTE does not carry %s.\n"+
			"The sink's CDC audit anchor (id = pipeline_id, status='running', end_time NULL, never "+
			"closed) matches this UPDATE by construction at 4h, so every healthy CDC stream is reaped "+
			"into a fabricated zombie failure. The pipelines_closed CDC carve-out does not help: it "+
			"guards the pipelines row, not this one.", execrows.NotSynthetic)
	}
}

// TestSweepCandidatesQuery_SkipsSyntheticCDCAnchor closes the second door. Even with the
// sweep fixed, an anchor already reaped on a previous release — or written 'failed' by any
// other path — is a terminal executions row with an end_time, which is exactly what the
// candidate sweep looks for. Diagnosing it produces a heal attempt against a pipeline that
// never failed.
func TestSweepCandidatesQuery_SkipsSyntheticCDCAnchor(t *testing.T) {
	if !strings.Contains(sweepCandidatesQuery, execrows.NotSynthetic) {
		t.Fatalf("sweepCandidatesQuery does not carry %s — an already-reaped CDC audit anchor is a "+
			"terminal row with an end_time, so the healer will diagnose and act on a failure that "+
			"never happened", execrows.NotSynthetic)
	}
}

// TestVerifierSuccessorLookup_SkipsSyntheticCDCAnchor asserts the predicate reaches the
// database, not just the source file: sqlmock matches the expectation as a regexp against
// the SQL actually handed to the driver.
//
// The anchor predates every heal attempt (it is created when the stream starts), but it is
// also the row the sink keeps writing to — so its start_time can sit inside the verifier's
// window and be mistaken for "a new run happened after the heal". The verifier would then
// grade the heal off a row that no heal produced.
func TestVerifierSuccessorLookup_SkipsSyntheticCDCAnchor(t *testing.T) {
	v, mock, done := newVerifier(t)
	defer done()

	noNewerAttempts(mock)
	// The expectation IS the assertion: if the predicate is not in the query text the
	// driver receives, this ExpectQuery does not match and ExpectationsWereMet fails.
	mock.ExpectQuery(`e\.id <> e\.pipeline_id`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "status"}).AddRow("exec-new", "completed"))

	verdict, succ, decided := v.Verify(context.Background(), attemptAged(time.Minute))

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("the successor lookup sent to the driver does not exclude the CDC audit anchor: %v", err)
	}
	if !decided || verdict != VerdictHealed || succ != "exec-new" {
		t.Errorf("successor lookup broke: verdict=%q successor=%q decided=%v", verdict, succ, decided)
	}
}
