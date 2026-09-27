//go:build integration_pg

// Real-PostgreSQL proof for BUG #10 (captured counters) and BUG #7 (snapshot rows,
// migration 114). The sqlmock suite (table_stats_captured_test.go) pins the bindings and
// the statement text; only a real server merges the two producers' rows.
//
//	SENTINEL_PG_DSN='postgres://postgres:verify@localhost:55440/pipeline_db?sslmode=disable' \
//	    go test -tags integration_pg ./internal/projector/ -run PGCaptured -v
package projector

import (
	"database/sql"
	"testing"
)

type pgCapturedRow struct {
	inserts, totalEvents, appliedInserts int64
	snapshotRows, appliedSnapshotRows    sql.NullInt64
}

func pgCapturedStored(t *testing.T, db *sql.DB) pgCapturedRow {
	t.Helper()
	var r pgCapturedRow
	if err := db.QueryRow(
		`SELECT COALESCE(inserts,0), COALESCE(total_events,0), COALESCE(applied_inserts,0),
		        snapshot_rows, applied_snapshot_rows
		 FROM pipeline_run_table_stats WHERE pipeline_id = $1::uuid AND qualified_name = $2`,
		pgOrchPipeID, pgOrchTable).Scan(&r.inserts, &r.totalEvents, &r.appliedInserts,
		&r.snapshotRows, &r.appliedSnapshotRows); err != nil {
		t.Fatalf("read stats row: %v", err)
	}
	return r
}

// cdcstatsTick is the stats consumer's real shape, last_event_ts included.
func cdcstatsTick(inserts, reads float64) map[string]interface{} {
	ev := cdcstatsEvent()
	meta := ev["metadata"].(map[string]interface{})
	meta["last_event_ts"] = "2026-09-24T10:00:00Z"
	meta["ops"] = map[string]interface{}{
		"inserts": inserts, "updates": float64(0), "deletes": float64(0), "total": inserts, "reads": reads,
	}
	return ev
}

// sinkTick is the sink's shape with an applied count above what cdcstats last reported —
// the only case where the pre-fix copy was visible through GREATEST.
func sinkTick(inserts, snapshot float64) map[string]interface{} {
	ev := sinkEvent()
	ev["metadata"].(map[string]interface{})["source"] = "kafka_mcp_sink"
	ev["metadata"].(map[string]interface{})["counts"] = map[string]interface{}{
		"inserts": inserts, "total_events": inserts, "snapshot_rows": snapshot,
	}
	return ev
}

func TestPGCapturedSinkAfterStatsConsumerKeepsCaptured(t *testing.T) {
	db := pgProjectorDB(t)
	p := pgSeedOrchPipeline(t, db)

	if err := p.upsertTableStats(cdcstatsTick(5, 100)); err != nil {
		t.Fatalf("cdcstats upsert: %v", err)
	}
	if err := p.upsertTableStats(sinkTick(8, 90)); err != nil {
		t.Fatalf("sink upsert: %v", err)
	}
	got := pgCapturedStored(t, db)
	if got.inserts != 5 || got.totalEvents != 5 {
		t.Errorf("captured inserts/total = %d/%d after a sink event, want 5/5 — the applied copy "+
			"overwrote what cdcstats captured (BUG #10)", got.inserts, got.totalEvents)
	}
	if !got.snapshotRows.Valid || got.snapshotRows.Int64 != 100 {
		t.Errorf("snapshot_rows = %v, want 100 (cdcstats' ops.reads, not the sink's copy)", got.snapshotRows)
	}
	if got.appliedInserts != 8 {
		t.Errorf("applied_inserts = %d, want 8", got.appliedInserts)
	}
	if !got.appliedSnapshotRows.Valid || got.appliedSnapshotRows.Int64 != 90 {
		t.Errorf("applied_snapshot_rows = %v, want 90", got.appliedSnapshotRows)
	}

	// cdcstats still advances its own columns afterwards.
	if err := p.upsertTableStats(cdcstatsTick(12, 100)); err != nil {
		t.Fatalf("second cdcstats upsert: %v", err)
	}
	if got := pgCapturedStored(t, db); got.inserts != 12 {
		t.Errorf("captured inserts = %d after the next cdcstats tick, want 12", got.inserts)
	}
}

// The control: with no cdcstats writer (ENABLE_CDC_TABLE_STATS off) the sink's counts still
// fill captured, and keep filling it on later events.
func TestPGCapturedFromAppliedWithoutStatsConsumer(t *testing.T) {
	db := pgProjectorDB(t)
	p := pgSeedOrchPipeline(t, db)

	for _, n := range []float64{3, 8} {
		if err := p.upsertTableStats(sinkTick(n, 2)); err != nil {
			t.Fatalf("sink upsert %v: %v", n, err)
		}
	}
	got := pgCapturedStored(t, db)
	if got.inserts != 8 {
		t.Errorf("captured inserts = %d, want the applied copy 8", got.inserts)
	}
	if !got.snapshotRows.Valid || got.snapshotRows.Int64 != 2 {
		t.Errorf("snapshot_rows = %v, want the applied copy 2", got.snapshotRows)
	}
}

// NULL means "not counted": an event without the fields leaves them NULL, and a later
// smaller value never lowers a stored one.
func TestPGSnapshotRowsNullAndMonotone(t *testing.T) {
	db := pgProjectorDB(t)
	p := pgSeedOrchPipeline(t, db)

	if err := p.upsertTableStats(sinkEvent()); err != nil { // no snapshot_rows
		t.Fatalf("sink upsert: %v", err)
	}
	if got := pgCapturedStored(t, db); got.snapshotRows.Valid || got.appliedSnapshotRows.Valid {
		t.Fatalf("snapshot_rows/applied_snapshot_rows = %v/%v, want NULL/NULL", got.snapshotRows, got.appliedSnapshotRows)
	}
	for _, n := range []float64{50, 20} {
		if err := p.upsertTableStats(sinkTick(3, n)); err != nil {
			t.Fatalf("sink upsert %v: %v", n, err)
		}
	}
	if err := p.upsertTableStats(sinkEvent()); err != nil {
		t.Fatalf("sink upsert: %v", err)
	}
	if got := pgCapturedStored(t, db); !got.appliedSnapshotRows.Valid || got.appliedSnapshotRows.Int64 != 50 {
		t.Errorf("applied_snapshot_rows = %v, want 50 (monotone, and kept by an event without the field)", got.appliedSnapshotRows)
	}
}

// legacySinkTick is a sink from before migration 114: snapshot rows counted as inserts,
// no snapshot_rows field.
func legacySinkTick(inserts float64) map[string]interface{} {
	ev := sinkEvent()
	ev["metadata"].(map[string]interface{})["source"] = "kafka_mcp_sink"
	ev["metadata"].(map[string]interface{})["counts"] = map[string]interface{}{
		"inserts": inserts, "total_events": inserts,
	}
	return ev
}

// An existing pipeline across the upgrade: the old sink stored 10 inserts + 100 snapshot
// rows as inserts=110; the new sink re-reads its ledger and reports 10 and 100 apart. The
// 100 must not be shown twice (Applied I 110 next to Applied Snapshot 100).
func TestPGLegacyInsertsSplitOnceOnUpgrade(t *testing.T) {
	db := pgProjectorDB(t)
	p := pgSeedOrchPipeline(t, db)

	if err := p.upsertTableStats(legacySinkTick(110)); err != nil {
		t.Fatalf("legacy sink upsert: %v", err)
	}
	if got := pgCapturedStored(t, db); got.appliedInserts != 110 || got.inserts != 110 {
		t.Fatalf("legacy row applied/captured inserts = %d/%d, want 110/110", got.appliedInserts, got.inserts)
	}
	if err := p.upsertTableStats(sinkTick(10, 100)); err != nil {
		t.Fatalf("new sink upsert: %v", err)
	}
	got := pgCapturedStored(t, db)
	if got.appliedInserts != 10 {
		t.Errorf("applied_inserts = %d after the upgrade, want 10 — the snapshot rows are counted twice", got.appliedInserts)
	}
	if got.inserts != 10 {
		t.Errorf("captured inserts (applied copy) = %d after the upgrade, want 10", got.inserts)
	}
	// Once split, the columns are monotone again.
	if err := p.upsertTableStats(sinkTick(4, 100)); err != nil {
		t.Fatalf("stale sink upsert: %v", err)
	}
	if got := pgCapturedStored(t, db); got.appliedInserts != 10 {
		t.Errorf("applied_inserts = %d after a smaller later value, want 10 (monotone)", got.appliedInserts)
	}
}

// A new sink whose ledger seed failed restarts its counters near zero. The split must not
// drop the stored total to that: GREATEST keeps the old value.
func TestPGLegacySplitNeverDropsBelowStoredMinusSnapshot(t *testing.T) {
	db := pgProjectorDB(t)
	p := pgSeedOrchPipeline(t, db)

	if err := p.upsertTableStats(legacySinkTick(110)); err != nil {
		t.Fatalf("legacy sink upsert: %v", err)
	}
	if err := p.upsertTableStats(sinkTick(1, 0)); err != nil {
		t.Fatalf("new sink upsert: %v", err)
	}
	if got := pgCapturedStored(t, db); got.appliedInserts != 110 {
		t.Errorf("applied_inserts = %d, want 110 kept (nothing was re-reported as snapshot rows)", got.appliedInserts)
	}
}
