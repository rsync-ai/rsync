//go:build integration_pg

// The namespace probe was reachable ONLY through the table-selection HITL
// (KI-NSLOCK-PROBE-UNREACHABLE-WITHOUT-HITL), and every existing test drove
// ResumeTables — so every one of them passed on the broken build. That is the whole
// test gap: the tests proved the probe DECIDES correctly, never that it RUNS.
//
// This file covers the other half. It drives lockNamespaceForRun, the run-boundary
// entry point the executor calls, and never touches ResumeTables — the same shape as
// prod pipeline 12c3579c, whose prompt named its table, so it never parked, never
// resumed, and merged 2000 rows into another pipeline's table with no probe, no lock
// and no notification.
//
// Each case asserts the lock state BEFORE and AFTER, so a passing assertion cannot be
// satisfied by a fixture that was already in the wanted state.
//
// It also covers what the FIRST prod run of the fix exposed: relocation used to leave
// the pipeline's resume checkpoints behind, so the run that moved it to a brand-new
// namespace resumed "already complete" and transferred 0 rows into a schema that was
// therefore never created (KI-NSLOCK-RELOCATION-STRANDS-CHECKPOINT). The clear has to
// be exactly once — an unconditional one would turn every resume into a full reload —
// so the negative controls below (no relocation; second run after relocation) matter
// as much as the positive one.
//
//	docker run -d --name ns-own-pg -e POSTGRES_PASSWORD=verify \
//	    -e POSTGRES_DB=cplane -p 55442:5432 postgres:16
//	OWNERSHIP_PG_DSN='postgres://postgres:verify@localhost:55442/cplane?sslmode=disable' \
//	    go test -tags integration_pg ./internal/handlers/ -run PG_RunBoundary -v

package handlers

import (
	"context"
	"database/sql"
	"testing"

	"github.com/rsync-ai/shared/crypto"

	_ "github.com/rsync-ai/shared/pgdriver"
)

func TestPG_RunBoundaryLocksNamespaceWithoutHITL(t *testing.T) {
	t.Setenv("ENVIRONMENT", "development")
	t.Setenv("ENCRYPTION_KEY", "test-only-key-not-a-secret-0123456789")

	db := ownershipPGDB(t)
	ctx := context.Background()

	const (
		ws       = "aaaaaaaa-0000-0000-0000-000000000001"
		destConn = "bbbbbbbb-0000-0000-0000-000000000001"
		user     = "dddddddd-0000-0000-0000-000000000001"

		// The pipeline under test: never parks, never resumes.
		quiet = "12c3579c-52bc-47f2-96ae-10719e4e943c"
		// The pipeline that already owns demo_customers in public.
		owner = "50899bae-0000-0000-0000-000000000001"
	)
	selected := []string{"demo_src.demo_customers"}

	cleanup := func() {
		_, _ = db.ExecContext(ctx, `DROP TABLE IF EXISTS pipelines, connections, pipeline_notifications, pipeline_checkpoints`)
		_, _ = db.ExecContext(ctx, `DROP TABLE IF EXISTS public.demo_customers`)
		_, _ = db.ExecContext(ctx, `DROP SCHEMA IF EXISTS rsync_public CASCADE`)
	}
	cleanup()
	t.Cleanup(cleanup)

	if _, err := db.ExecContext(ctx, `
		CREATE TABLE pipelines (
			id                        UUID PRIMARY KEY,
			workspace_id              UUID NOT NULL,
			destination_connection_id UUID,
			created_by                UUID,
			config                    JSONB,
			updated_at                TIMESTAMPTZ DEFAULT NOW());
		CREATE TABLE connections (
			id             UUID PRIMARY KEY,
			workspace_id   UUID NOT NULL,
			connector_type TEXT NOT NULL,
			config         TEXT NOT NULL);
		CREATE TABLE pipeline_checkpoints (
			id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			pipeline_id   UUID NOT NULL,
			connection_id UUID NOT NULL,
			source_table  VARCHAR(255) NOT NULL,
			position      JSONB NOT NULL,
			created_at    TIMESTAMPTZ DEFAULT NOW(),
			updated_at    TIMESTAMPTZ DEFAULT NOW(),
			UNIQUE(pipeline_id, source_table));
		CREATE TABLE pipeline_notifications (
			id              BIGSERIAL PRIMARY KEY,
			pipeline_id     UUID,
			user_id         UUID,
			type            TEXT,
			severity        TEXT,
			title           TEXT,
			message         TEXT,
			action_url      TEXT,
			metadata        JSONB,
			delivery_status TEXT,
			dedup_key       TEXT,
			created_at      TIMESTAMPTZ)`); err != nil {
		t.Fatalf("create control-plane tables: %v", err)
	}

	encrypted, err := crypto.Encrypt(mustJSON(t, destConfigFromDSN(t, mustDSN(t))))
	if err != nil {
		t.Fatalf("encrypt destination config: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO connections (id, workspace_id, connector_type, config) VALUES ($1,$2,'postgresql',$3)`,
		destConn, ws, encrypted); err != nil {
		t.Fatalf("insert connection: %v", err)
	}

	addPipeline := func(id, namespace string, tables []string, extra map[string]interface{}) {
		t.Helper()
		cfg := map[string]interface{}{
			"selected_tables":    tables,
			"destination_config": map[string]interface{}{"namespace": namespace},
		}
		for k, v := range extra {
			cfg[k] = v
		}
		if _, err := db.ExecContext(ctx,
			`INSERT INTO pipelines (id, workspace_id, destination_connection_id, created_by, config)
			 VALUES ($1,$2,$3,$4,$5::jsonb)`, id, ws, destConn, user, string(mustJSON(t, cfg))); err != nil {
			t.Fatalf("insert pipeline %s: %v", id, err)
		}
	}

	lockState := func(id string) (bool, string) {
		t.Helper()
		var locked sql.NullBool
		var ns sql.NullString
		if err := db.QueryRowContext(ctx, `
			SELECT COALESCE((config->>'destination_namespace_locked')::bool, false),
			       NULLIF(TRIM(COALESCE(config->>'destination_namespace','')), '')
			FROM pipelines WHERE id = $1::uuid`, id).Scan(&locked, &ns); err != nil {
			t.Fatalf("read lock state for %s: %v", id, err)
		}
		return locked.Bool, ns.String
	}
	// A checkpoint in the shape the batch lane writes at the end of a full sweep:
	// the exact state that makes the NEXT run a 0-row no-op.
	seedCheckpoint := func(id string) {
		t.Helper()
		if _, err := db.ExecContext(ctx, `
			INSERT INTO pipeline_checkpoints (pipeline_id, connection_id, source_table, position)
			VALUES ($1::uuid, $2::uuid, 'demo_customers',
			        '{"cursor": 2000, "rows_so_far": 2000, "table_complete": true}'::jsonb)
			ON CONFLICT (pipeline_id, source_table) DO UPDATE SET position = EXCLUDED.position`,
			id, destConn); err != nil {
			t.Fatalf("seed checkpoint for %s: %v", id, err)
		}
	}
	checkpointCount := func(id string) int {
		t.Helper()
		var n int
		if err := db.QueryRowContext(ctx,
			`SELECT count(*) FROM pipeline_checkpoints WHERE pipeline_id = $1::uuid`, id).Scan(&n); err != nil {
			t.Fatalf("count checkpoints: %v", err)
		}
		return n
	}
	notificationCount := func(id string) int {
		t.Helper()
		var n int
		if err := db.QueryRowContext(ctx,
			`SELECT count(*) FROM pipeline_notifications
			 WHERE pipeline_id = $1::uuid AND type = 'destination_namespace_relocated'`, id).Scan(&n); err != nil {
			t.Fatalf("count notifications: %v", err)
		}
		return n
	}

	// The trap, rebuilt: the table is physically in public AND another pipeline
	// owns it. Pre-fix, `quiet` never reached the probe and merged into it.
	if _, err := db.ExecContext(ctx,
		`CREATE TABLE public.demo_customers (id INT PRIMARY KEY, email TEXT)`); err != nil {
		t.Fatalf("create pre-existing destination table: %v", err)
	}
	addPipeline(owner, "public", selected, nil)
	addPipeline(quiet, "public", selected, nil)

	// The pipeline already ran once against `public` (that is how 12c3579c reached
	// the fix at all), so it carries a completed checkpoint into the relocation.
	seedCheckpoint(quiet)

	// ---- The regression this closes. No HITL, no ResumeTables, no resume signal.
	if locked, _ := lockState(quiet); locked {
		t.Fatal("fixture is already locked — the post-state assertion below would prove nothing")
	}
	if checkpointCount(quiet) != 1 {
		t.Fatal("fixture has no checkpoint — the clear assertion below would prove nothing")
	}
	res, err := lockNamespaceForRun(ctx, db, quiet, selected)
	if err != nil {
		t.Fatalf("run-boundary lock: %v", err)
	}
	if !res.Locked {
		t.Fatal("run-boundary lock stood down; a pipeline that never parks is exactly the case this exists for")
	}
	if res.Namespace != "rsync_public" {
		t.Errorf("resolved = %q, want \"rsync_public\" — the table is owned by another pipeline", res.Namespace)
	}
	if !res.Relocated {
		t.Error("relocated = false; the caller cannot report a move it is not told about")
	}
	// The stranded-checkpoint bug: relocation points the pipeline at an empty
	// namespace while `table_complete: true` still says there is nothing left to
	// transfer, so the run succeeds having moved 0 rows and never creates it.
	if n := checkpointCount(quiet); n != 0 {
		t.Errorf("checkpoints after relocation = %d, want 0 — a stale resume position makes the new namespace unreachable", n)
	}
	if res.CheckpointsCleared != 1 {
		t.Errorf("checkpoints_cleared = %d, want 1 — the run trace is the only place this reset is visible", res.CheckpointsCleared)
	}
	locked, storedNS := lockState(quiet)
	if !locked {
		t.Error("destination_namespace_locked is not set — the lock is what makes the answer stick")
	}
	if storedNS != "rsync_public" {
		t.Errorf("stored destination_namespace = %q, want \"rsync_public\"", storedNS)
	}
	// The user's only chance to learn their data moved. 12c3579c got none.
	if n := notificationCount(quiet); n != 1 {
		t.Errorf("relocation notifications = %d, want 1 — a silent move is the original bug", n)
	}

	// ---- Idempotent: a second run must reuse the locked answer, never re-probe
	// into a fresh relocation. A namespace that can move mid-life is worse than
	// one that was wrong to begin with.
	// The relocated run wrote its own checkpoint against the NEW namespace. Clearing
	// that one too would make every subsequent run a full reload.
	seedCheckpoint(quiet)
	res2, err := lockNamespaceForRun(ctx, db, quiet, selected)
	if err != nil || !res2.Locked || res2.Namespace != "rsync_public" {
		t.Errorf("second run: (%q, %v, %v), want (\"rsync_public\", true, nil)", res2.Namespace, res2.Locked, err)
	}
	if res2.Relocated || res2.CheckpointsCleared != 0 {
		t.Errorf("second run: relocated=%v cleared=%d, want false/0 — relocation is observable exactly once",
			res2.Relocated, res2.CheckpointsCleared)
	}
	if n := checkpointCount(quiet); n != 1 {
		t.Errorf("checkpoints after second run = %d, want 1 — clearing on every run is a full reload every run", n)
	}
	if n := notificationCount(quiet); n != 1 {
		t.Errorf("notifications after second run = %d, want 1 — the notice must not repeat every run", n)
	}

	// ---- Negative control: a pipeline whose chosen namespace is free locks without
	// relocating, and keeps its resume position. Without this, "delete always" passes
	// every assertion above.
	const settled = "cccccccc-0000-0000-0000-000000000001"
	addPipeline(settled, "analytics", selected, nil)
	seedCheckpoint(settled)
	resSettled, err := lockNamespaceForRun(ctx, db, settled, selected)
	if err != nil || !resSettled.Locked {
		t.Fatalf("unrelocated lock: (%+v, %v)", resSettled, err)
	}
	if resSettled.Relocated {
		t.Error("relocated = true for a namespace no other pipeline owns")
	}
	if n := checkpointCount(settled); n != 1 {
		t.Errorf("checkpoints for an unrelocated pipeline = %d, want 1 — its resume position is still valid", n)
	}

	// ---- An empty table set must NOT lock. Locking there would freeze the seeded
	// namespace having proven nothing about it, and the lock is permanent.
	const blank = "eeeeeeee-0000-0000-0000-000000000001"
	addPipeline(blank, "public", selected, nil)
	if _, err := lockNamespaceForRun(ctx, db, blank, nil); err != errNamespaceLockNoTables {
		t.Errorf("empty table set: err = %v, want errNamespaceLockNoTables", err)
	}
	if locked, _ := lockState(blank); locked {
		t.Error("empty table set locked the namespace — an unprobed lock is permanent and unfounded")
	}

	// ---- Schema mirroring stands down, same as the HITL path: there is no single
	// namespace to probe, and attaching one flattens every source schema into it.
	const mirrored = "ffffffff-0000-0000-0000-000000000001"
	addPipeline(mirrored, "public", selected, map[string]interface{}{"destination_schema_mode": "preserve"})
	if r, err := lockNamespaceForRun(ctx, db, mirrored, selected); err != nil || r.Locked {
		t.Errorf("mirroring pipeline: (locked=%v, err=%v), want (false, nil)", r.Locked, err)
	}
	if locked, _ := lockState(mirrored); locked {
		t.Error("mirroring pipeline was locked to a single namespace — that is the PR #549 data loss")
	}
}
