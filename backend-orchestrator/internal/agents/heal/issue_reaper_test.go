package heal

// Default-suite coverage for the orphaned-issue reaper.
//
// The real proof lives in issue_reaper_pg_test.go, which runs the statement
// against a live planner over a mixed-content VARCHAR column. That file is behind
// the integration_pg build tag and CI does not set it — `grep -rn integration_pg
// .github/workflows/` returns nothing — so without this file the reaper would
// have no automated guard at all on the path that actually runs on every PR.
//
// These tests therefore cover the two things sqlmock CAN settle honestly:
// that the statement is wired into the sweep, and that its two structural
// invariants are still present in the text. What they cannot settle is whether
// PostgreSQL accepts it; do not read a green run here as that.

import (
	"context"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestReapOrphanedIssuesClosesRowsAndCountsThem(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("UPDATE sentinel_active_issues").
		WithArgs(uuidTextPattern).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).
			AddRow("cdc-connector-down-1").
			AddRow("batch-stall-2"))

	n, err := (&HealWorker{DB: db}).reapOrphanedIssues(context.Background())
	if err != nil {
		t.Fatalf("reapOrphanedIssues: %v", err)
	}
	if n != 2 {
		t.Fatalf("reaped %d, want 2", n)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

// A failing query must surface as an error, never as a quiet 0.
//
// "Reaped nothing" and "could not tell" are different states with the same
// return value, and collapsing them is how #730's batch-stall bug sat behind a
// green test for weeks: the code `continue`d past a driver error and the absence
// of issues looked identical under the bug and under the fix.
func TestReapOrphanedIssuesReportsQueryFailureInsteadOfZero(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("UPDATE sentinel_active_issues").
		WillReturnError(context.DeadlineExceeded)

	if _, err := (&HealWorker{DB: db}).reapOrphanedIssues(context.Background()); err == nil {
		t.Fatal("a failing reap query returned nil error — indistinguishable from having nothing to reap")
	}
}

// TestReapQueryKeepsItsTwoStructuralGuards pins the invariants that make the
// statement safe. Both have already been violated once in this subsystem's
// history, which is why they are asserted rather than trusted.
func TestReapQueryKeepsItsTwoStructuralGuards(t *testing.T) {
	q := reapOrphanedIssuesQuery

	// 1. The cast direction. `component_id::uuid` raises `invalid input syntax
	// for type uuid` on the WAL watchdog's slot-name rows and aborts the whole
	// statement — the #723 `text = uuid` shape.
	if strings.Contains(q, "component_id::uuid") {
		t.Fatal("the reaper casts component_id to uuid — one slot-name row would abort the " +
			"entire statement instead of being skipped")
	}
	if !strings.Contains(q, "p.id::text = c.component_id") {
		t.Fatal("the reaper no longer compares via p.id::text; that cast cannot fail for any " +
			"value either column can hold, and its replacement probably can")
	}

	// 2. The shape guard. Without it this is the naive "close every pipeline
	// issue with no matching pipeline", which also closes the WAL watchdog's
	// orphaned-slot alarm — a slot name matches no pipeline BY CONSTRUCTION, and
	// an unattached slot pins WAL on the source.
	if !strings.Contains(q, "c.component_id ~ $1") {
		t.Fatal("the UUID shape guard is gone — the reaper would silence orphaned-replication-slot " +
			"alarms, which is worse than the leak it fixes")
	}

	// And the guard has to mean what it says.
	re, err := regexp.Compile(uuidTextPattern)
	if err != nil {
		t.Fatalf("uuidTextPattern does not compile: %v", err)
	}
	if re.MatchString("rsync_slot_orphan_probe") {
		t.Fatal("uuidTextPattern matches a replication slot name")
	}
	if !re.MatchString("bbbbbbbb-0000-4000-8000-000000000003") {
		t.Fatal("uuidTextPattern rejects a canonical pipeline UUID")
	}
	// Anchored at both ends, or a slot name with a UUID embedded in it slips past.
	if re.MatchString("slot_bbbbbbbb-0000-4000-8000-000000000003_x") {
		t.Fatal("uuidTextPattern is not anchored — a slot name containing a UUID would match")
	}

	// 3. Never DELETE. The row is the record that the alarm fired.
	if strings.Contains(strings.ToUpper(q), "DELETE") {
		t.Fatal("the reaper DELETEs; it must set resolved_at so the audit trail survives")
	}
}
