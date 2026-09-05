//go:build integration_pg

// Real-PostgreSQL coverage for the orphaned-issue reaper.
//
// This lives beside issue_sweep_pg_test.go and reuses its fixture on purpose:
// that fixture already seeds the one row that separates a correct reaper from a
// destructive one — a cdc_pipeline-typed issue whose component_id is a
// replication SLOT NAME, not a UUID (cdc_wal_watchdog.go:305-307).
//
// A reaper written as "close every pipeline issue whose component_id matches no
// pipeline" passes the happy-path test and silently closes that alarm too, because
// a slot name matches no pipeline BY CONSTRUCTION — it never referred to one. The
// orphaned slot it reports is a real, ongoing problem (an unattached slot pins WAL
// and can fill the source disk), so closing it is worse than the leak this fix is
// for: it turns a stuck-open alarm into a disappearing one.
//
// sqlmock cannot settle this. It matches statements as strings and never runs
// them, so the regex shape-guard would "pass" there whether or not PostgreSQL
// agrees with it. Only a real planner over a real mixed-content VARCHAR column
// does. Same reason issue_sweep_pg_test.go exists — see #723's `text = uuid`.
//
// Server setup: see the header of issue_sweep_pg_test.go.
package heal

import (
	"context"
	"database/sql"
	"testing"
)

// pgIssueState reports whether the row is still present and whether it has been
// closed out. Both halves matter: the reaper must set resolved_at, NOT DELETE.
// The table is the healer's audit trail of what it was told and when — a DELETE
// erases the evidence that the alarm ever fired, which is precisely what makes
// the current leak hard to reason about after the fact.
func pgIssueState(t *testing.T, db *sql.DB, issueID string) (exists, resolved bool) {
	t.Helper()
	err := db.QueryRow(
		`SELECT resolved_at IS NOT NULL FROM sentinel_active_issues WHERE id = $1`,
		issueID).Scan(&resolved)
	if err == sql.ErrNoRows {
		return false, false
	}
	if err != nil {
		t.Fatalf("read resolved_at for %q: %v", issueID, err)
	}
	return true, resolved
}

// TestPGReapClosesIssuesWhosePipelineWasDeleted is the leak itself.
//
// Every resolver the Sentinel has is a RE-OBSERVATION resolver: batch_sentinel.go
// resolveStaleIssues, cdc_sentinel.go resolveLagIssue, cdc_wal_watchdog.go
// resolveWALAlert all close a row on the tick that finds the component healthy
// again. Once the pipeline is deleted there is no next observation, so the row is
// unreachable by every one of them and stays open forever. Measured on production
// 2026-08-03: 9 of 10 open issues pointed at deleted pipelines, and 0 of 10 rows
// had ever been resolved by anything.
func TestPGReapClosesIssuesWhosePipelineWasDeleted(t *testing.T) {
	db := pgHealDB(t)
	issueID := pgSeedOpenIssue(t, db)
	slotIssueID := "cdc-wal-" + pgHealOrphanSlotComponentID

	// The event no resolver survives.
	if _, err := db.Exec(`DELETE FROM pipelines WHERE id = $1::uuid`, pgHealPipelineID); err != nil {
		t.Fatalf("delete pipeline: %v", err)
	}

	n, err := pgHealWorker(db).reapOrphanedIssues(context.Background())
	if err != nil {
		t.Fatalf("reapOrphanedIssues: %v", err)
	}
	if n != 1 {
		t.Fatalf("reaped %d issues, want exactly 1 (the orphan, and only the orphan)", n)
	}

	exists, resolved := pgIssueState(t, db, issueID)
	if !exists {
		t.Fatal("reaper DELETEd the orphaned issue; it must set resolved_at and keep the audit row")
	}
	if !resolved {
		t.Fatal("orphaned issue still has resolved_at IS NULL — the leak is not closed")
	}

	// The discriminator. This row's component_id never referred to a pipeline, so
	// "no matching pipeline" is its normal state, not evidence of an orphan.
	exists, resolved = pgIssueState(t, db, slotIssueID)
	if !exists {
		t.Fatal("reaper DELETEd the WAL-watchdog orphan-slot alarm")
	}
	if resolved {
		t.Fatal("reaper closed the WAL-watchdog orphan-slot alarm: a slot name matches no " +
			"pipeline by construction, so matching on that alone silences a live problem")
	}
}

// TestPGReapLeavesIssuesWithALivePipelineAlone is the guard against the reaper
// becoming a second, unattended way to silence alarms. An issue whose pipeline
// still exists is the healer's actual work queue (issue_sweep.go) — reaping it
// would delete that queue.
func TestPGReapLeavesIssuesWithALivePipelineAlone(t *testing.T) {
	db := pgHealDB(t)
	issueID := pgSeedOpenIssue(t, db) // pipeline deliberately left alive
	slotIssueID := "cdc-wal-" + pgHealOrphanSlotComponentID

	n, err := pgHealWorker(db).reapOrphanedIssues(context.Background())
	if err != nil {
		t.Fatalf("reapOrphanedIssues: %v", err)
	}
	if n != 0 {
		t.Fatalf("reaped %d issues, want 0 — nothing here is orphaned", n)
	}

	for _, id := range []string{issueID, slotIssueID} {
		exists, resolved := pgIssueState(t, db, id)
		if !exists || resolved {
			t.Fatalf("issue %q: exists=%v resolved=%v, want still present and open", id, exists, resolved)
		}
	}
}
