//go:build integration_pg

// Real-PostgreSQL coverage for the issue-driven heal sweep.
//
// The sqlmock suite in issue_sweep_test.go proves the control flow — query,
// decision event, stamp — and asserts the no-uuid-cast property against the
// query TEXT. Neither of those is proof that the SQL is executable. sqlmock
// matches statements as strings and never type-checks them, which is exactly how
// the #723 `text = uuid` comparison shipped green.
//
// That matters more here than almost anywhere else in the healer, because this
// query joins a VARCHAR(255) column that holds pipeline UUIDs for some rows and
// free-form component names ("sentinel-agent-1") for others. Only a real planner
// can settle whether the join survives that mixture.
//
// Not part of the default suite — needs a live server:
//
//	docker run -d --name heal-pg -e POSTGRES_PASSWORD=verify \
//	    -e POSTGRES_DB=pipeline_db -p 55440:5432 postgres:16
//	for m in api-gateway/migrations/*.sql; do
//	    docker exec -i heal-pg psql -U postgres -d pipeline_db -v ON_ERROR_STOP=1 -q < "$m"
//	done
//	SENTINEL_PG_DSN='postgres://postgres:verify@localhost:55440/pipeline_db?sslmode=disable' \
//	    go test -tags integration_pg ./internal/agents/heal/ -run PG -v
package heal

import (
	"context"
	"database/sql"
	"os"
	"testing"

	_ "github.com/rsync-ai/shared/pgdriver"
)

// Fixed IDs so a crashed run leaves nothing to collide with: every test deletes
// this set before seeding rather than relying on cleanup having happened.
const (
	pgHealUserID      = "bbbbbbbb-0000-4000-8000-000000000001"
	pgHealWorkspaceID = "bbbbbbbb-0000-4000-8000-000000000002"
	pgHealPipelineID  = "bbbbbbbb-0000-4000-8000-000000000003"
	// Deliberately NOT a UUID, and deliberately carried on a row whose
	// component_type IS 'cdc_pipeline'. This is not a hypothetical: the WAL
	// watchdog writes exactly this shape — cdc_wal_watchdog.go:305-307 falls back
	// to the replication slot name when the slot has no pipeline attached — so
	// filtering on component_type is not enough to make `component_id::uuid` safe.
	pgHealOrphanSlotComponentID = "rsync_slot_orphan_probe"
)

func pgHealDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("SENTINEL_PG_DSN")
	if dsn == "" {
		t.Skip("SENTINEL_PG_DSN not set — see the file header for the two commands that provide one")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("ping %s: %v", dsn, err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func pgHealPurge(t *testing.T, db *sql.DB) {
	t.Helper()
	for _, q := range []string{
		`DELETE FROM sentinel_active_issues WHERE component_id IN ($1, $2)`,
	} {
		if _, err := db.Exec(q, pgHealPipelineID, pgHealOrphanSlotComponentID); err != nil {
			t.Fatalf("purge %q: %v", q, err)
		}
	}
	// pipelines cascades to executions, pipeline_progress and pipeline_run_events.
	if _, err := db.Exec(`DELETE FROM pipelines WHERE id = $1::uuid`, pgHealPipelineID); err != nil {
		t.Fatalf("purge pipeline: %v", err)
	}
	if _, err := db.Exec(`DELETE FROM workspaces WHERE id = $1::uuid`, pgHealWorkspaceID); err != nil {
		t.Fatalf("purge workspace: %v", err)
	}
	if _, err := db.Exec(`DELETE FROM users WHERE id = $1::uuid`, pgHealUserID); err != nil {
		t.Fatalf("purge user: %v", err)
	}
}

// pgSeedOpenIssue creates a CDC pipeline with one unresolved, never-healed issue
// against it, plus a WAL-watchdog issue for an orphaned replication slot — same
// component_type, component_id that is not a UUID.
func pgSeedOpenIssue(t *testing.T, db *sql.DB) string {
	t.Helper()
	pgHealPurge(t, db)
	t.Cleanup(func() { pgHealPurge(t, db) })

	mustExec := func(q string, args ...interface{}) {
		t.Helper()
		if _, err := db.Exec(q, args...); err != nil {
			t.Fatalf("seed %.60q: %v", q, err)
		}
	}
	mustExec(`INSERT INTO users (id, email, password_hash) VALUES ($1::uuid, $2, 'x')`,
		pgHealUserID, "issue-sweep-verify@example.invalid")
	mustExec(`INSERT INTO workspaces (id, name, slug, owner_id)
	          VALUES ($1::uuid, 'issue-sweep-verify', 'issue-sweep-verify', $2::uuid)`,
		pgHealWorkspaceID, pgHealUserID)
	// status='running' on purpose: this is the state the execution-driven sweep
	// structurally cannot see (it requires end_time IS NOT NULL), and the state
	// every open Sentinel issue describes.
	mustExec(`INSERT INTO pipelines
	          (id, name, natural_language_request, workspace_id, status, sync_mode, cdc_mode, created_by)
	          VALUES ($1::uuid, 'issue sweep verify', 'verify', $2::uuid, 'running', 'cdc', 'streaming_only', $3::uuid)`,
		pgHealPipelineID, pgHealWorkspaceID, pgHealUserID)

	issueID := "cdc-connector-down-" + pgHealPipelineID
	mustExec(`INSERT INTO sentinel_active_issues
	          (id, type, severity, component_id, component_type, description, detected_at, last_occurrence)
	          VALUES ($1, 'connector_down', 'critical', $2, 'cdc_pipeline',
	                  'CDC connector cdc-abc12345 is FAILED', NOW(), NOW())`,
		issueID, pgHealPipelineID)

	// The row that makes this test worth running against a real server: a
	// cdc_pipeline-typed issue whose component_id can never be cast to uuid.
	// Verified by hand — `JOIN pipelines p ON p.id = c.component_id::uuid` over
	// this fixture returns:
	//
	//	ERROR: invalid input syntax for type uuid: "rsync_slot_..."
	//
	// and it aborts the whole statement, so under that join NONE of the
	// assertions below would pass, for any pipeline.
	mustExec(`INSERT INTO sentinel_active_issues
	          (id, type, severity, component_id, component_type, description, detected_at, last_occurrence)
	          VALUES ($1, 'high_lag', 'warning', $2, 'cdc_pipeline',
	                  'replication slot has no pipeline attached', NOW(), NOW())`,
		"cdc-wal-"+pgHealOrphanSlotComponentID, pgHealOrphanSlotComponentID)

	return issueID
}

func pgHealDecisionCount(t *testing.T, db *sql.DB) int {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM pipeline_run_events
	                       WHERE pipeline_id = $1::uuid AND event_type = 'healer_decision'`,
		pgHealPipelineID).Scan(&n); err != nil {
		t.Fatalf("count decisions: %v", err)
	}
	return n
}

func pgIssueStamped(t *testing.T, db *sql.DB, issueID string) bool {
	t.Helper()
	var stamped sql.NullTime
	if err := db.QueryRow(
		`SELECT heal_attempted_at FROM sentinel_active_issues WHERE id = $1`, issueID).
		Scan(&stamped); err != nil {
		t.Fatalf("read stamp: %v", err)
	}
	return stamped.Valid
}

func pgHealWorker(db *sql.DB) *HealWorker {
	// Attempts/Verifier left nil: both are nil-safe, and leaving them out keeps
	// the assertions below about the sweep itself rather than the ledger.
	return &HealWorker{DB: db, Healer: New()}
}

// TestPGIssueSweepReachesAPipelineTheExecutionSweepCannotSee is the whole point
// of the change, expressed as the one query prod could not answer.
//
// The seeded pipeline is status='running' with no terminal execution, so
// sweepCandidatesQuery matches nothing against it — by construction, not by
// accident. If a healer_decision appears, it can only have come from the issue
// sweep.
func TestPGIssueSweepReachesAPipelineTheExecutionSweepCannotSee(t *testing.T) {
	db := pgHealDB(t)
	issueID := pgSeedOpenIssue(t, db)
	w := pgHealWorker(db)
	ctx := context.Background()

	// Sanity: the failure-driven query really is blind to this pipeline. Run
	// against the real planner rather than asserted from the SQL text, so this
	// stays honest if the predicate changes. Not via w.sweep — sweep() now calls
	// sweepIssues itself, which is exactly the thing under test.
	rows, err := db.Query(sweepCandidatesQuery,
		GracePeriod.String(), RehealCooldown.String(), BatchSize)
	if err != nil {
		t.Fatalf("sweepCandidatesQuery: %v", err)
	}
	seen := rows.Next()
	rows.Close()
	if seen {
		t.Fatal("the execution-driven sweep query matched a pipeline with no terminal " +
			"execution — this test can no longer prove the issue sweep is what acted")
	}

	w.sweepIssues(ctx)

	if n := pgHealDecisionCount(t, db); n != 1 {
		t.Fatalf("healer_decision rows = %d after the issue sweep, want 1.\n\n"+
			"Either the query did not match the seeded issue, or it errored — the most "+
			"likely error is a uuid cast meeting the non-UUID 'agent' component_id seeded "+
			"alongside it.", n)
	}
	if !pgIssueStamped(t, db, issueID) {
		t.Fatal("the issue was diagnosed but not stamped — the next 60-second tick will " +
			"diagnose it again, and every tick after that for as long as it stays open")
	}
}

// TestPGIssueSweepDoesNotLoopOnAnOpenIssue pins the marker semantics, which are
// the reason migration 083 exists at all.
//
// An issue stays open for as long as the problem lasts. Without the stamp the
// healer would re-diagnose it once a minute forever, filling the run timeline
// with identical verdicts and the ledger with identical attempts.
func TestPGIssueSweepDoesNotLoopOnAnOpenIssue(t *testing.T) {
	db := pgHealDB(t)
	pgSeedOpenIssue(t, db)
	w := pgHealWorker(db)
	ctx := context.Background()

	w.sweepIssues(ctx)
	w.sweepIssues(ctx)
	w.sweepIssues(ctx)

	if n := pgHealDecisionCount(t, db); n != 1 {
		t.Fatalf("healer_decision rows = %d after three sweeps, want 1 — the stamp is not "+
			"excluding an already-diagnosed issue", n)
	}
}

// TestPGIssueSweepRe-admitsAnIssueThatHappensAgain is the complement, and it is
// the half that a plain `heal_attempted_at IS NULL` predicate would get wrong.
//
// This mirrors the executions rule exactly (worker.go sweepCandidatesQuery): the
// stamp records that THIS occurrence was looked at, not that the row is retired.
// A Sentinel issue whose occurrence_count keeps climbing is new evidence, and it
// earns a fresh look.
func TestPGIssueSweepReadmitsAnIssueThatHappensAgain(t *testing.T) {
	db := pgHealDB(t)
	issueID := pgSeedOpenIssue(t, db)
	w := pgHealWorker(db)
	ctx := context.Background()

	w.sweepIssues(ctx)
	if n := pgHealDecisionCount(t, db); n != 1 {
		t.Fatalf("first sweep produced %d decisions, want 1", n)
	}

	// What the Sentinel's UPSERT does when it re-observes the same problem.
	if _, err := db.Exec(`UPDATE sentinel_active_issues
	                      SET last_occurrence = NOW(), occurrence_count = occurrence_count + 1
	                      WHERE id = $1`, issueID); err != nil {
		t.Fatalf("bump occurrence: %v", err)
	}

	w.sweepIssues(ctx)
	if n := pgHealDecisionCount(t, db); n != 2 {
		t.Fatalf("healer_decision rows = %d after the issue recurred, want 2 — a stamp that "+
			"is never re-opened makes an issue healable exactly once, which is the same bug "+
			"#725 fixed for executions", n)
	}
}

// TestPGIssueSweepIgnoresResolvedIssues — a resolved issue is a problem that
// went away. Diagnosing it would put a verdict on the timeline for something
// nobody needs to act on.
func TestPGIssueSweepIgnoresResolvedIssues(t *testing.T) {
	db := pgHealDB(t)
	issueID := pgSeedOpenIssue(t, db)
	if _, err := db.Exec(
		`UPDATE sentinel_active_issues SET resolved_at = NOW() WHERE id = $1`, issueID); err != nil {
		t.Fatalf("resolve: %v", err)
	}

	pgHealWorker(db).sweepIssues(context.Background())

	if n := pgHealDecisionCount(t, db); n != 0 {
		t.Fatalf("healer_decision rows = %d for a resolved issue, want 0", n)
	}
}
