//go:build integration_pg

// Real-PostgreSQL coverage for the sink consumer group resolver.
//
// The sqlmock suite in sink_consumer_group_test.go proves the SHAPE of the query — that
// the backfill tiebreak is present, sorts above created_at, and is a preference rather
// than a filter. What it structurally cannot prove is that any of that SQL is executable
// or that PostgreSQL orders rows the way the text implies. sqlmock matches statements as
// regexes and never type-checks them, which is exactly how the #723 `text = uuid`
// comparison shipped green. `metadata->>'backfill'` inside ORDER BY is new here — no
// other query in backend-orchestrator uses a jsonb accessor — so a green default suite is
// NOT evidence this statement parses under the pgx shim.
//
// This file closes that gap: real migrations, real rows, real ordering. The control case
// is the load-bearing one — it proves the tiebreak PREFERS the streaming row rather than
// FILTERING the backfill row out, which is the regression that would blind the
// prod-reachable lag probe at cmd/orchestrator/main.go:270.
//
// Not part of the default suite — needs a live server:
//
//	docker run -d --name sinkgroup-pg -e POSTGRES_PASSWORD=verify \
//	    -e POSTGRES_DB=pipeline_db -p 55441:5432 postgres:16
//	for m in api-gateway/migrations/*.sql; do
//	    docker exec -i sinkgroup-pg psql -U postgres -d pipeline_db -v ON_ERROR_STOP=1 -q < "$m"
//	done
//	SINKGROUP_PG_DSN='postgres://postgres:verify@localhost:55441/pipeline_db?sslmode=disable' \
//	    go test -tags integration_pg ./internal/handlers/ -run PG -v
package handlers

import (
	"context"
	"database/sql"
	"os"
	"strconv"
	"testing"

	_ "github.com/rsync-ai/shared/pgdriver"

	"github.com/rsync-ai/backend-orchestrator/internal/utils"
)

// Fixed IDs so a crashed run leaves nothing to collide with: every test purges this set
// before seeding rather than relying on cleanup having happened.
const (
	pgSinkUserID      = "bbbbbbbb-0000-4000-8000-000000000001"
	pgSinkWorkspaceID = "bbbbbbbb-0000-4000-8000-000000000002"
	pgSinkPipelineID  = "bbbbbbbb-0000-4000-8000-000000000003"
	pgSinkExecutionID = "bbbbbbbb-0000-4000-8000-000000000004"
)

// The two group shapes the executor mints for a hybrid-CDC pipeline. Built from the same
// SafeID8 the executor uses so the fixture cannot drift from the real naming rule.
func pgSinkStreamingGroup() string { return "sink-" + utils.SafeID8(pgSinkPipelineID) }
func pgSinkBackfillGroup() string  { return pgSinkStreamingGroup() + "-batch" }

func pgSinkTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("SINKGROUP_PG_DSN")
	if dsn == "" {
		t.Skip("SINKGROUP_PG_DSN not set — see the file header for the two commands that provide one")
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

func pgSinkPurge(t *testing.T, db *sql.DB) {
	t.Helper()
	// pipelines cascades to executions and pipeline_dependencies.
	if _, err := db.Exec(`DELETE FROM pipelines WHERE id = $1::uuid`, pgSinkPipelineID); err != nil {
		t.Fatalf("purge pipeline: %v", err)
	}
	if _, err := db.Exec(`DELETE FROM workspaces WHERE id = $1::uuid`, pgSinkWorkspaceID); err != nil {
		t.Fatalf("purge workspace: %v", err)
	}
	if _, err := db.Exec(`DELETE FROM users WHERE id = $1::uuid`, pgSinkUserID); err != nil {
		t.Fatalf("purge user: %v", err)
	}
}

// pgSinkSeedPipeline creates the pipeline + one running execution. syncMode feeds
// pipelines.sync_mode; the resolver does not read it, but seeding it truthfully keeps the
// fixture honest about which real shape each case represents.
func pgSinkSeedPipeline(t *testing.T, db *sql.DB, syncMode string, cdcMode interface{}) {
	t.Helper()
	pgSinkPurge(t, db)
	t.Cleanup(func() { pgSinkPurge(t, db) })

	mustExec := func(q string, args ...interface{}) {
		t.Helper()
		if _, err := db.Exec(q, args...); err != nil {
			t.Fatalf("seed %.60q: %v", q, err)
		}
	}
	mustExec(`INSERT INTO users (id, email, password_hash) VALUES ($1::uuid, $2, 'x')`,
		pgSinkUserID, "sinkgroup-verify@example.invalid")
	mustExec(`INSERT INTO workspaces (id, name, slug, owner_id)
	          VALUES ($1::uuid, 'sinkgroup-verify', 'sinkgroup-verify', $2::uuid)`,
		pgSinkWorkspaceID, pgSinkUserID)
	mustExec(`INSERT INTO pipelines
	          (id, name, natural_language_request, workspace_id, status, sync_mode, cdc_mode, created_by)
	          VALUES ($1::uuid, 'sinkgroup verify', 'verify', $2::uuid, 'running', $3, $4, $5::uuid)`,
		pgSinkPipelineID, pgSinkWorkspaceID, syncMode, cdcMode, pgSinkUserID)
	mustExec(`INSERT INTO executions (id, pipeline_id, status, start_time, end_time)
	          VALUES ($1::uuid, $2::uuid, 'running', NOW() - '10 minutes'::interval, NULL)`,
		pgSinkExecutionID, pgSinkPipelineID)
}

// pgSinkSeedDependency writes one kafka_sink_worker row. ageSeconds is subtracted from
// NOW() so the ordering between rows is deterministic rather than insertion-speed
// dependent — the whole defect lives in what created_at DESC returns.
func pgSinkSeedDependency(t *testing.T, db *sql.DB, identifier, metadata string, ageSeconds int) {
	t.Helper()
	_, err := db.Exec(`
		INSERT INTO pipeline_dependencies
		    (pipeline_id, execution_id, kind, identifier, required_phases, metadata, created_at)
		VALUES ($1::uuid, $2::uuid, 'kafka_sink_worker', $3, '{syncing,streaming}', $4::jsonb,
		        NOW() - $5::interval)
	`, pgSinkPipelineID, pgSinkExecutionID, identifier, metadata,
		strconv.Itoa(ageSeconds)+" seconds")
	if err != nil {
		t.Fatalf("seed dependency %q: %v", identifier, err)
	}
}

// TestPGResolveSinkGroupSkipsTheBackfillDuringTheBackfillPhase is the defect itself.
//
// Against HEAD-before-the-fix this returns "...-batch": both rows exist, both carry
// sink_mode "cdc" and start_offset "earliest", and created_at DESC has nothing else to go
// on. A CDC sink restart resolving to that group stop_sinks the running backfill worker.
//
// NOTE ON THE AGES: the backfill row here is the NEWER of the two (5s vs 10s old). That is
// deliberate and it is the sharp version of the test — it forces the tiebreak to be what
// decides, rather than created_at coincidentally already preferring the streaming row.
func TestPGResolveSinkGroupSkipsTheBackfillDuringTheBackfillPhase(t *testing.T) {
	db := pgSinkTestDB(t)
	pgSinkSeedPipeline(t, db, "cdc", "initial")

	pgSinkSeedDependency(t, db, pgSinkBackfillGroup(),
		`{"sink_mode":"cdc","start_offset":"earliest","backfill":true}`, 5)
	pgSinkSeedDependency(t, db, pgSinkStreamingGroup(),
		`{"sink_mode":"cdc","start_offset":"earliest","backfill":false}`, 10)

	got := ResolveSinkConsumerGroup(context.Background(), db, pgSinkPipelineID)
	if got != pgSinkStreamingGroup() {
		t.Fatalf("hybrid-CDC pipeline resolved to %q, want the streaming group %q. Returning "+
			"the ...-batch group here is the hijack: RestartCDCSinkWorker would stop_sink the "+
			"running backfill worker and re-register its group with sink_mode=\"cdc\" and CDC "+
			"topics.", got, pgSinkStreamingGroup())
	}

	// CONTROL — the load-bearing half. Delete the streaming row and the backfill row must
	// come back. If it does not, the tiebreak has become a FILTER, and a pure-batch
	// pipeline (whose only row is a backfill row) would fall through to the derived name.
	if _, err := db.Exec(`DELETE FROM pipeline_dependencies WHERE pipeline_id = $1::uuid AND identifier = $2`,
		pgSinkPipelineID, pgSinkStreamingGroup()); err != nil {
		t.Fatalf("delete streaming row: %v", err)
	}
	got = ResolveSinkConsumerGroup(context.Background(), db, pgSinkPipelineID)
	if got != pgSinkBackfillGroup() {
		t.Fatalf("with only the backfill row present the resolver returned %q, want %q. The "+
			"tiebreak has become an exclusion rather than a preference — see the trap in "+
			"sinkConsumerGroupQuery.", got, pgSinkBackfillGroup())
	}
}

// TestPGResolveSinkGroupStillReturnsAPureBatchPipelinesOnlyRow is the trap, stated as the
// real shape it protects rather than as a deletion.
//
// A pure-batch pipeline registers ONLY a backfill row — isBatchBackfillTopic is true for
// its "pipeline.<id>.data" topic. This is what keeps the lag probe at
// cmd/orchestrator/main.go:270 armed: DerivedSinkConsumerGroup returns a group that never
// existed, and GetConsumerGroupLag answers a nonexistent group with an empty map and no
// error, so a dead sink would look exactly like a healthy idle one forever.
func TestPGResolveSinkGroupStillReturnsAPureBatchPipelinesOnlyRow(t *testing.T) {
	db := pgSinkTestDB(t)
	pgSinkSeedPipeline(t, db, "batch", nil)

	pgSinkSeedDependency(t, db, pgSinkBackfillGroup(),
		`{"sink_mode":"batch","start_offset":"earliest","backfill":true}`, 5)

	got := ResolveSinkConsumerGroup(context.Background(), db, pgSinkPipelineID)
	if got == DerivedSinkConsumerGroup(pgSinkPipelineID) {
		t.Fatalf("a pure-batch pipeline resolved to the DERIVED name %q instead of its only "+
			"manifest row %q. This is the regression the prefer-never-exclude design exists to "+
			"prevent: it silently blinds the lag probe at cmd/orchestrator/main.go:270.",
			got, pgSinkBackfillGroup())
	}
	if got != pgSinkBackfillGroup() {
		t.Fatalf("pure-batch pipeline resolved to %q, want its only row %q", got, pgSinkBackfillGroup())
	}
}

// TestPGResolveSinkGroupTreatsLegacyRowsAsNonBackfill pins the COALESCE.
//
// Rows written before the 'backfill' key existed have no such member, and metadata->>'x'
// on a missing key is NULL. A NULL sort key sorts LAST under ASC, so a bare accessor would
// rank a NEW backfill row AHEAD of a LEGACY streaming row — a fresh defect introduced by
// the fix. COALESCE(..., 'false') makes legacy rows fall through to created_at DESC, i.e.
// exactly the pre-change behaviour, until the next run rewrites them.
func TestPGResolveSinkGroupTreatsLegacyRowsAsNonBackfill(t *testing.T) {
	db := pgSinkTestDB(t)
	pgSinkSeedPipeline(t, db, "cdc", "initial")

	pgSinkSeedDependency(t, db, pgSinkBackfillGroup(), `{"sink_mode":"cdc"}`, 30)
	pgSinkSeedDependency(t, db, pgSinkStreamingGroup(), `{"sink_mode":"cdc"}`, 10)

	got := ResolveSinkConsumerGroup(context.Background(), db, pgSinkPipelineID)
	if got != pgSinkStreamingGroup() {
		t.Fatalf("with neither row carrying a 'backfill' key the resolver returned %q, want the "+
			"newest row %q. Legacy rows must fall through to created_at DESC — pre-change "+
			"behaviour — rather than being reordered by a NULL sort key.",
			got, pgSinkStreamingGroup())
	}
}
