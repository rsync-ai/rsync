package heal

import (
	"context"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// Pins KI-HEAL-IGNORES-SENTINEL-ISSUES.
//
// The Sentinel has three detectors that write into sentinel_active_issues — CDC
// lag, CDC connector-down, batch stall — and the healer reads that table
// NOWHERE. `grep -rn sentinel_active_issues internal/agents/heal/` returns
// nothing. Detection and healing have been two systems that happen to share a
// process: the Sentinel notices, writes a row, and the loop that could act on it
// never looks.
//
// The heal sweep's only candidate source is `executions`, and only rows with
// `end_time IS NOT NULL` (worker.go:295). A pipeline that is actively broken but
// still marked running is therefore invisible to the healer until the 4-hour
// zombie sweep notices it — which is exactly the state every issue in that table
// describes.
//
// This test drives the missing half: a seeded issue must produce a diagnosis, a
// recorded decision, and a stamp saying the healer looked.
const issueSweepPipelineID = "33333333-3333-4333-8333-333333333333"

func TestIssueSweepDiagnosesAnIssueNobodyElseReads(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	issueID := "cdc-connector-down-" + issueSweepPipelineID

	mock.ExpectQuery("FROM sentinel_active_issues").
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "type", "severity", "component_id", "description", "source_type", "dest_type",
		}).AddRow(
			issueID, "connector_down", "critical", issueSweepPipelineID,
			"CDC connector cdc-abc12345 is FAILED and did not recover after restart",
			"postgresql", "postgresql",
		))

	// The decision has to land somewhere an operator can see it. This is the
	// same surface the execution sweep writes to, so the self-healing panel
	// renders issue-derived verdicts with no UI change at all.
	mock.ExpectExec("INSERT INTO pipeline_run_events").
		WillReturnResult(sqlmock.NewResult(1, 1))

	// And the issue has to be stamped, or the next 60-second tick re-diagnoses
	// it, and the one after that, for as long as the issue stays open.
	mock.ExpectExec(regexp.QuoteMeta("UPDATE sentinel_active_issues")).
		WithArgs(issueID).
		WillReturnResult(sqlmock.NewResult(0, 1))

	// Attempts stays nil on purpose: every AttemptStore method is nil-safe, so
	// leaving it out keeps the expectations above to exactly the two writes this
	// test is about.
	w := &HealWorker{DB: db, Healer: New()}
	w.sweepIssues(context.Background())

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("the issue sweep did not run the Diagnose→Heal→record loop: %v\n\n"+
			"Before the fix the healer never read sentinel_active_issues at all, so a "+
			"connector the Sentinel had already declared dead produced no diagnosis, no "+
			"decision event, and nothing on the run timeline.", err)
	}
}

// TestIssueDerivedDiagnosisNeverReachesTheAutoBand is the safety half, and it is
// the reason this change is allowed to be small.
//
// Heal auto-executes at confidence >= AutoBand (heal.go:127). Wiring a brand-new
// candidate source into that switch without a bound would mean the first thing
// this change does on production is take unattended actions on a signal path
// that has never driven one. Capping strictly below the band makes every
// issue-derived verdict a recommendation: recorded, surfaced, and waiting for a
// human — the same posture the orchestration rules already sit in deliberately
// (heal.go:144-152).
//
// Derived from AutoBand rather than written as a literal, so moving the band
// moves the cap with it.
func TestIssueDerivedDiagnosisNeverReachesTheAutoBand(t *testing.T) {
	for _, in := range []float64{0.0, 0.3, 0.5, 0.84, AutoBand, 0.95, 1.0} {
		got := capIssueConfidence(in)
		if got >= AutoBand {
			t.Fatalf("capIssueConfidence(%v) = %v, which is at or above AutoBand %v — "+
				"an issue-derived diagnosis would auto-execute", in, got, AutoBand)
		}
		if in < AutoBand && got != in {
			t.Fatalf("capIssueConfidence(%v) = %v — the cap moved a confidence that was "+
				"already below the band, which distorts the ledger for no safety gain", in, got)
		}
	}
}

// TestIssueSweepQueryNeverCastsASentinelColumnToUUID guards the exact shape that
// shipped green in #723.
//
// component_id is VARCHAR(255), and filtering on component_type does NOT
// guarantee it holds a UUID: cdc_wal_watchdog.go:305-307 writes
// component_type='cdc_pipeline' with the replication SLOT NAME as the
// component_id when the slot has no pipeline attached. A join written
// `p.id = c.component_id::uuid` therefore raises `invalid input syntax for type
// uuid: "rsync_slot_abc"` and takes the entire sweep down with it — every
// pipeline, not just the orphaned slot — the first time one of those rows is
// open.
//
// Casting the other direction (`p.id::text = c.component_id`) cannot fail for
// any value either column can hold. sqlmock cannot catch this — it matches SQL
// as strings and never type-checks — so the property is asserted on the query
// text here and executed against a real planner in issue_sweep_pg_test.go.
func TestIssueSweepQueryNeverCastsASentinelColumnToUUID(t *testing.T) {
	q := issueSweepCandidatesQuery

	if strings.Contains(q, "component_id::uuid") {
		t.Fatal("the sweep casts component_id to uuid — a non-UUID component_id on any " +
			"unresolved issue row will abort the entire sweep with a runtime cast error")
	}
	if !strings.Contains(q, "p.id::text") {
		t.Fatal("the join no longer compares pipelines.id as text; if the comparison moved, " +
			"re-check that no sentinel-authored string is being cast to a typed column")
	}
	// Both halves of the marker semantics. `heal_attempted_at IS NULL` alone
	// would retire an issue forever after one look; the `last_occurrence >`
	// clause is what re-admits a problem that keeps happening.
	for _, clause := range []string{
		"resolved_at IS NULL",
		"heal_attempted_at IS NULL",
		"last_occurrence > c.heal_attempted_at",
	} {
		if !strings.Contains(q, clause) {
			t.Fatalf("issueSweepCandidatesQuery is missing %q", clause)
		}
	}
}
