//go:build integration_pg

// Real-PostgreSQL proof for the orchestration-execution-id column (migration 090).
//
// The sqlmock suite proves the right SQL is emitted with the right bindings. It cannot
// prove the statement WORKS: sqlmock never submits anything to a planner, so a column that
// does not exist, a cast that fails, and a COALESCE that resolves the wrong way all look
// identical to a passing test. The whole risk here is a merge rule between two producers
// racing on one row, and only a real server settles that.
//
//	SENTINEL_PG_DSN='postgres://postgres:verify@localhost:55440/pipeline_db?sslmode=disable' \
//	    go test -tags integration_pg ./internal/projector/ -run PGOrchestration -v
package projector

import (
	"database/sql"
	"os"
	"testing"

	_ "github.com/rsync-ai/shared/pgdriver"
)

const (
	pgOrchUserID      = "dddddddd-0000-4000-8000-000000000011"
	pgOrchWorkspaceID = "dddddddd-0000-4000-8000-000000000012"
	// A CDC pipeline: execution_id in the stats row is forced to this value.
	pgOrchPipeID = "dddddddd-0000-4000-8000-000000000013"
	// What the orchestrator actually minted, and what the sink's logs carry.
	pgOrchExecID = "dddddddd-0000-4000-8000-000000000014"
	pgOrchTable  = "pipeline_test.demo_products"
)

func pgProjectorDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("SENTINEL_PG_DSN")
	if dsn == "" {
		t.Skip("SENTINEL_PG_DSN not set — see the file header for the command that provides one")
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

func pgSeedOrchPipeline(t *testing.T, db *sql.DB) *EventProjector {
	t.Helper()
	purge := func() {
		// Ordered by FK: stats and executions cascade from the pipeline.
		for _, d := range []struct{ q, id string }{
			{`DELETE FROM pipelines WHERE id = $1::uuid`, pgOrchPipeID},
			{`DELETE FROM workspaces WHERE id = $1::uuid`, pgOrchWorkspaceID},
			{`DELETE FROM users WHERE id = $1::uuid`, pgOrchUserID},
		} {
			if _, err := db.Exec(d.q, d.id); err != nil {
				t.Fatalf("purge %.40q: %v", d.q, err)
			}
		}
	}
	purge()
	t.Cleanup(purge)

	mustExec := func(q string, args ...interface{}) {
		t.Helper()
		if _, err := db.Exec(q, args...); err != nil {
			t.Fatalf("seed %.60q: %v", q, err)
		}
	}
	mustExec(`INSERT INTO users (id, email, password_hash) VALUES ($1::uuid, $2, 'x')`,
		pgOrchUserID, "orch-id-verify@example.invalid")
	mustExec(`INSERT INTO workspaces (id, name, slug, owner_id)
	          VALUES ($1::uuid, 'orch-id-verify', 'orch-id-verify', $2::uuid)`,
		pgOrchWorkspaceID, pgOrchUserID)
	mustExec(`INSERT INTO pipelines
	          (id, name, natural_language_request, workspace_id, status, sync_mode, cdc_mode, created_by)
	          VALUES ($1::uuid, 'orch id', 'verify', $2::uuid, 'running', 'cdc', 'streaming_only', $3::uuid)`,
		pgOrchPipeID, pgOrchWorkspaceID, pgOrchUserID)

	return &EventProjector{db: db, lastSeq: map[string]int64{}, gapSeen: map[string]bool{}}
}

// sinkEvent is what kafka-mcp-sink emits: applied counters plus the orchestration id.
func sinkEvent() map[string]interface{} {
	return map[string]interface{}{
		"event_type":   "TABLE_STATS",
		"pipeline_id":  pgOrchPipeID,
		"execution_id": pgOrchPipeID, // already rewritten by parseCDCMessage
		"metadata": map[string]interface{}{
			"mode":                       "cdc",
			"status":                     "running",
			"orchestration_execution_id": pgOrchExecID,
			"table": map[string]interface{}{
				"schema": "pipeline_test", "name": "demo_products", "qualified_name": pgOrchTable,
			},
			"counts": map[string]interface{}{
				"inserts": float64(3), "total_events": float64(3), "read_rows": float64(3),
			},
		},
	}
}

// cdcstatsEvent is what the orchestrator's cdcstats agent emits on every tick: captured
// counters, the same key, and no idea this column exists. Field-for-field the shape of
// cdcstats/events.go — metadata.ops, no metadata.counts, and no destination or id fields.
func cdcstatsEvent() map[string]interface{} {
	return map[string]interface{}{
		"event_type":   "TABLE_STATS",
		"pipeline_id":  pgOrchPipeID,
		"execution_id": pgOrchPipeID,
		"metadata": map[string]interface{}{
			"source": "cdc_stats_consumer",
			"mode":   "cdc",
			"status": "running",
			"table": map[string]interface{}{
				"schema": "pipeline_test", "name": "demo_products", "qualified_name": pgOrchTable,
			},
			"ops": map[string]interface{}{
				"inserts": float64(9), "updates": float64(0), "deletes": float64(0), "total": float64(9),
			},
		},
	}
}

func pgOrchStored(t *testing.T, db *sql.DB) (execID string, orchID sql.NullString) {
	t.Helper()
	if err := db.QueryRow(
		`SELECT execution_id::text, orchestration_execution_id::text
		 FROM pipeline_run_table_stats WHERE pipeline_id = $1::uuid AND qualified_name = $2`,
		pgOrchPipeID, pgOrchTable).Scan(&execID, &orchID); err != nil {
		t.Fatalf("read stats row: %v", err)
	}
	return
}

// The statement runs against the real schema, the UUID cast holds, and the id lands —
// without displacing the normalized execution_id the two producers collide on.
func TestPGOrchestrationIDIsStoredAlongsideTheNormalizedExecutionID(t *testing.T) {
	db := pgProjectorDB(t)
	p := pgSeedOrchPipeline(t, db)

	if err := p.upsertTableStats(sinkEvent()); err != nil {
		t.Fatalf("upsertTableStats did not execute against PostgreSQL: %v\n\n"+
			"The sqlmock suite cannot catch this — it matches SQL as a string and never "+
			"submits it to a planner.", err)
	}

	execID, orchID := pgOrchStored(t, db)
	if !orchID.Valid || orchID.String != pgOrchExecID {
		t.Errorf("orchestration_execution_id = %v, want %q", orchID, pgOrchExecID)
	}
	if execID != pgOrchPipeID {
		t.Errorf("execution_id = %q, want the pipeline id %q — the CDC normalization is the "+
			"conflict key both producers depend on, and this column is added, not substituted",
			execID, pgOrchPipeID)
	}
}

// The reason the ON CONFLICT clause COALESCEs. cdcstats ticks land between the sink's
// continuously on prod (ENABLE_CDC_TABLE_STATS=true), so a plain assignment would blank the
// id seconds after it arrived and the column would read NULL forever on exactly the CDC
// pipelines it was added for. sqlmock cannot see this: it never merges anything.
func TestPGOrchestrationIDSurvivesTheOtherProducersTicks(t *testing.T) {
	db := pgProjectorDB(t)
	p := pgSeedOrchPipeline(t, db)

	if err := p.upsertTableStats(sinkEvent()); err != nil {
		t.Fatalf("sink upsert: %v", err)
	}
	for i := 0; i < 3; i++ {
		if err := p.upsertTableStats(cdcstatsEvent()); err != nil {
			t.Fatalf("cdcstats upsert %d: %v", i, err)
		}
	}

	_, orchID := pgOrchStored(t, db)
	if !orchID.Valid || orchID.String != pgOrchExecID {
		t.Errorf("orchestration_execution_id = %v after three cdcstats ticks, want %q still. "+
			"The other producer does not know this id and sends NULL; an assignment here erases "+
			"the sink's answer and nothing ever restores it.", orchID, pgOrchExecID)
	}

	// The control: both producers really are hitting ONE row, so the merge above was
	// exercised rather than sidestepped by a second row appearing.
	var rows int
	if err := db.QueryRow(`SELECT COUNT(*) FROM pipeline_run_table_stats WHERE pipeline_id = $1::uuid`,
		pgOrchPipeID).Scan(&rows); err != nil {
		t.Fatalf("count: %v", err)
	}
	if rows != 1 {
		t.Fatalf("%d stats rows for one CDC table, want 1 — the producers stopped colliding, so "+
			"this test proves nothing about the merge", rows)
	}
	// And the captured counters from the ticks did land, so the ticks were real upserts.
	var inserts int64
	if err := db.QueryRow(`SELECT COALESCE(inserts,0) FROM pipeline_run_table_stats
	                       WHERE pipeline_id = $1::uuid`, pgOrchPipeID).Scan(&inserts); err != nil {
		t.Fatalf("read inserts: %v", err)
	}
	if inserts != 9 {
		t.Fatalf("inserts = %d, want 9 — the cdcstats ticks did not take effect", inserts)
	}
}

// A malformed label must not reach the driver: the column is UUID and a failed cast aborts
// the transaction, taking the real counters down with it. This is the case the Go-side
// uuid.Parse exists for, and the one a SQL-side cast would fail.
func TestPGOrchestrationIDMalformedLabelDoesNotLoseTheCounters(t *testing.T) {
	db := pgProjectorDB(t)
	p := pgSeedOrchPipeline(t, db)

	ev := sinkEvent()
	ev["metadata"].(map[string]interface{})["orchestration_execution_id"] = "not-a-uuid"

	if err := p.upsertTableStats(ev); err != nil {
		t.Fatalf("a malformed label failed the whole upsert: %v\n\n"+
			"This column is a label. Losing the counters it was riding alongside is a far worse "+
			"outcome than losing the label.", err)
	}

	execID, orchID := pgOrchStored(t, db)
	if orchID.Valid {
		t.Errorf("orchestration_execution_id = %q, want NULL", orchID.String)
	}
	if execID != pgOrchPipeID {
		t.Errorf("execution_id = %q, want %q", execID, pgOrchPipeID)
	}
	var totalEvents int64
	if err := db.QueryRow(`SELECT COALESCE(total_events,0) FROM pipeline_run_table_stats
	                       WHERE pipeline_id = $1::uuid`, pgOrchPipeID).Scan(&totalEvents); err != nil {
		t.Fatalf("read total_events: %v", err)
	}
	if totalEvents != 3 {
		t.Errorf("total_events = %d, want 3 — the counters this event carried were lost", totalEvents)
	}
}
