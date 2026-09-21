//go:build integration_pg

// Real-PostgreSQL coverage for the shared derived_status CASE
// (pipelineDerivedStatusCaseSQL) — issue #6, list said "Completed" while the
// detail page said "Running".
//
// sqlmock matches SQL as text and never executes it, so it cannot tell whether a
// CASE branch order or a NULL-safe predicate actually yields the right badge.
// These tests run the real handlers (ListPipelines, its COUNT, GetPipelineStats
// and the detail page's GetPipelineState) against a real schema built from every
// migration, over a seeded set of CDC and batch pipelines, and assert:
//
//  1. each pipeline's list badge;
//  2. the list COUNT for a ?status= filter equals the rows that page returns;
//  3. the "Running" stat card equals the list's running rows;
//  4. for the streaming-handoff shapes, the list agrees with /state.
//
// The default suite pins that all three queries splice the same const
// (pipelines_derived_status_test.go); this file proves what the const DOES.
// Not part of the default suite — needs a live server:
//
//	docker run -d --name status6-pg -e POSTGRES_PASSWORD=verify \
//	    -e POSTGRES_DB=pipeline_db -p 55463:5432 postgres:16-alpine
//	# wait for the FINAL server (the init server only listens on the socket):
//	until docker exec status6-pg pg_isready -h 127.0.0.1 -U postgres; do sleep 1; done
//	for m in api-gateway/migrations/*.sql; do
//	    docker exec -i status6-pg psql -h 127.0.0.1 -U postgres -d pipeline_db -v ON_ERROR_STOP=1 -q < "$m"
//	done
//	PIPELINE_STATUS_PG_DSN='postgres://postgres:verify@localhost:55463/pipeline_db?sslmode=disable' \
//	    go test -tags integration_pg ./internal/handlers/ -run PG_DerivedStatus -v
//	docker rm -f status6-pg
package handlers

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"api-gateway/internal/db"

	"github.com/gin-gonic/gin"
	_ "github.com/rsync-ai/shared/pgdriver"
)

// Fixed ids so a crashed run leaves nothing to collide with: every run deletes
// this workspace's pipelines before seeding.
const (
	pgDSWorkspaceID = "d5d5d5d5-0000-4000-8000-000000000001"
	pgDSUserID      = "d5d5d5d5-0000-4000-8000-000000000002"
)

type pgDSCase struct {
	id   string
	name string
	// pipelines
	pStatus  string
	syncMode *string
	cdcMode  *string
	// latest executions row (nil execStatus = no execution)
	execStatus *string
	execClosed bool
	// pipeline_progress (nil = no row); progress points at the execution above
	ppStatus *string
	// an unhealthy pipeline_dependency_health row
	deadDep bool

	wantList string
	// wantState is the detail page's /state status bucket for the same pipeline;
	// "" = not compared (the dead-dependency case is /runtime's job, not /state's).
	wantState string
}

func strp(s string) *string { return &s }

func pgDSCases() []pgDSCase {
	return []pgDSCase{
		{
			// THE #6 shape: the snapshot execution closed 'completed' at the handoff,
			// and a later progress event left pp.status at 'processing' for it.
			// Pre-fix the stale-heartbeat branch turned this into 'passed'.
			id: "d5d5d5d5-0000-4000-8000-000000000101", name: "cdc-handoff-processing",
			pStatus: "running", syncMode: strp("cdc"),
			execStatus: strp("completed"), execClosed: true, ppStatus: strp("processing"),
			wantList: "running", wantState: "running",
		},
		{
			// A streaming pipeline whose sync_mode/cdc_mode was never persisted: the
			// handoff wrote pp.status='running'. Pre-fix the 24h success branch
			// (which can only exclude pipelines it KNOWS are CDC) said 'passed'.
			id: "d5d5d5d5-0000-4000-8000-000000000102", name: "streaming-unflagged",
			pStatus:    "running",
			execStatus: strp("completed"), execClosed: true, ppStatus: strp("running"),
			wantList: "running", wantState: "running",
		},
		{
			// The normal, flagged handoff — already 'running' before the fix; a
			// control that the new branch did not break it.
			id: "d5d5d5d5-0000-4000-8000-000000000103", name: "cdc-handoff-running",
			pStatus: "running", syncMode: strp("cdc"), cdcMode: strp("streaming_only"),
			execStatus: strp("completed"), execClosed: true, ppStatus: strp("running"),
			wantList: "running", wantState: "running",
		},
		{
			// Control: batch stale heartbeat still resolves to the closed execution.
			id: "d5d5d5d5-0000-4000-8000-000000000104", name: "batch-stale-heartbeat",
			pStatus: "active", syncMode: strp("batch"),
			execStatus: strp("completed"), execClosed: true, ppStatus: strp("processing"),
			wantList: "passed",
		},
		{
			// Control: NULL sync_mode AND NULL cdc_mode is batch. A non-NULL-safe
			// CDC guard would make the branch condition NULL and drop this to 'running'.
			id: "d5d5d5d5-0000-4000-8000-000000000105", name: "null-mode-stale-heartbeat",
			pStatus:    "active",
			execStatus: strp("completed"), execClosed: true, ppStatus: strp("processing"),
			wantList: "passed",
		},
		{
			// Control: a CDC run whose execution closed FAILED still reads failed —
			// the CDC exemption is only for a successful (handoff) close.
			id: "d5d5d5d5-0000-4000-8000-000000000106", name: "cdc-failed-close",
			pStatus: "running", syncMode: strp("cdc"),
			execStatus: strp("failed"), execClosed: true, ppStatus: strp("processing"),
			wantList: "failed",
		},
		{
			// Control: pausing writes only pipelines.status — progress still says
			// 'running'. The new pp.status='running' branch must not outrank it.
			id: "d5d5d5d5-0000-4000-8000-000000000107", name: "cdc-paused",
			pStatus: "paused", syncMode: strp("cdc"),
			execStatus: strp("completed"), execClosed: true, ppStatus: strp("running"),
			wantList: "paused", wantState: "paused",
		},
		{
			// Control: a dead required dependency still downgrades the stream.
			id: "d5d5d5d5-0000-4000-8000-000000000108", name: "cdc-dead-dependency",
			pStatus: "running", syncMode: strp("cdc"),
			execStatus: strp("completed"), execClosed: true, ppStatus: strp("running"),
			deadDep:  true,
			wantList: "failed",
		},
		{
			// Control: a finished batch run is still 'passed'.
			id: "d5d5d5d5-0000-4000-8000-000000000109", name: "batch-done",
			pStatus: "active", syncMode: strp("batch"),
			execStatus: strp("completed"), execClosed: true, ppStatus: strp("completed"),
			wantList: "passed", wantState: "passed",
		},
	}
}

func pgDSOpen(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("PIPELINE_STATUS_PG_DSN")
	if dsn == "" {
		t.Skip("PIPELINE_STATUS_PG_DSN not set — see the file header for the commands that provide one")
	}
	conn, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := conn.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}
	prev := db.DB
	db.DB = conn
	t.Cleanup(func() {
		db.DB = prev
		_ = conn.Close()
	})
	return conn
}

func pgDSExec(t *testing.T, conn *sql.DB, q string, args ...any) {
	t.Helper()
	if _, err := conn.Exec(q, args...); err != nil {
		t.Fatalf("exec failed: %v\nSQL: %s", err, q)
	}
}

func pgDSSeed(t *testing.T, conn *sql.DB, cases []pgDSCase) {
	t.Helper()
	// pipelines children CASCADE; delete first so a crashed run cannot collide.
	pgDSExec(t, conn, `DELETE FROM pipelines WHERE workspace_id = $1`, pgDSWorkspaceID)
	pgDSExec(t, conn, `
		INSERT INTO users (id, email, password_hash, name)
		VALUES ($1, 'pg-derived-status-probe@example.invalid', 'x', 'pg derived status probe')
		ON CONFLICT (id) DO NOTHING`, pgDSUserID)
	pgDSExec(t, conn, `
		INSERT INTO workspaces (id, name, slug, owner_id)
		VALUES ($1, 'pg derived status probe', 'pg-derived-status-probe', $2)
		ON CONFLICT (id) DO NOTHING`, pgDSWorkspaceID, pgDSUserID)
	pgDSExec(t, conn, `
		INSERT INTO workspace_members (workspace_id, user_id, role)
		VALUES ($1, $2, 'owner')
		ON CONFLICT (workspace_id, user_id) DO NOTHING`, pgDSWorkspaceID, pgDSUserID)

	for i, tc := range cases {
		pgDSExec(t, conn, `
			INSERT INTO pipelines (id, name, natural_language_request, status, sync_mode, cdc_mode, workspace_id, created_at)
			VALUES ($1, $2, 'probe', $3, $4, $5, $6, NOW() - ($7 * INTERVAL '1 minute'))`,
			tc.id, tc.name, tc.pStatus, tc.syncMode, tc.cdcMode, pgDSWorkspaceID, i)
		execID := ""
		if tc.execStatus != nil {
			// Deterministic, distinct from the pipeline id (so it is never the
			// synthetic `id = pipeline_id` CDC audit row).
			execID = tc.id[:len(tc.id)-3] + "e" + tc.id[len(tc.id)-2:]
			var end any
			if tc.execClosed {
				end = time.Now().Add(-10 * time.Minute)
			}
			pgDSExec(t, conn, `
				INSERT INTO executions (id, pipeline_id, status, start_time, end_time)
				VALUES ($1, $2, $3, NOW() - INTERVAL '1 hour', $4)`,
				execID, tc.id, *tc.execStatus, end)
		}
		if tc.ppStatus != nil {
			var exec any
			if execID != "" {
				exec = execID
			}
			pgDSExec(t, conn, `
				INSERT INTO pipeline_progress (pipeline_id, execution_id, status, current_stage, message, updated_at)
				VALUES ($1, $2, $3, 'streaming', 'Streaming pipeline active', NOW())`,
				tc.id, exec, *tc.ppStatus)
		}
		if tc.deadDep {
			var depID string
			if err := conn.QueryRow(`
				INSERT INTO pipeline_dependencies (pipeline_id, kind, identifier)
				VALUES ($1, 'debezium_connector', 'probe-connector') RETURNING id`, tc.id).Scan(&depID); err != nil {
				t.Fatalf("seed dependency: %v", err)
			}
			pgDSExec(t, conn, `
				INSERT INTO pipeline_dependency_health (dependency_id, status)
				VALUES ($1, 'unhealthy')`, depID)
		}
	}
	t.Cleanup(func() {
		_, _ = conn.Exec(`DELETE FROM pipelines WHERE workspace_id = $1`, pgDSWorkspaceID)
	})
}

func pgDSRouter() *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(func(c *gin.Context) {
		c.Set("user_id", pgDSUserID)
		c.Set(ctxWorkspaceID, pgDSWorkspaceID)
		c.Set(ctxWorkspaceRole, "owner")
		c.Next()
	})
	r.GET("/api/v1/pipelines", ListPipelines)
	r.GET("/api/v1/pipelines/stats", GetPipelineStats)
	r.GET("/api/v1/pipelines/:id/state", GetPipelineState)
	return r
}

func pgDSGet(t *testing.T, r *gin.Engine, path string, out any) {
	t.Helper()
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
	if w.Code != http.StatusOK {
		t.Fatalf("GET %s: want 200, got %d: %s", path, w.Code, w.Body.String())
	}
	if err := json.Unmarshal(w.Body.Bytes(), out); err != nil {
		t.Fatalf("GET %s: decode: %v (%s)", path, err, w.Body.String())
	}
}

type pgDSListResp struct {
	Pipelines []struct {
		ID            string `json:"id"`
		Name          string `json:"name"`
		DerivedStatus string `json:"derived_status"`
	} `json:"pipelines"`
	Pagination struct {
		Total int `json:"total"`
	} `json:"pagination"`
}

// stateBucket maps /state's status onto the list's derived_status vocabulary
// (the frontend renders processing/running as "Running", completed as "Completed").
func stateBucket(s string) string {
	switch s {
	case "processing", "running":
		return "running"
	case "completed":
		return "passed"
	}
	return s
}

func TestPG_DerivedStatus_ListCountStatsAndStateAgree(t *testing.T) {
	conn := pgDSOpen(t)
	cases := pgDSCases()
	pgDSSeed(t, conn, cases)
	r := pgDSRouter()

	var list pgDSListResp
	pgDSGet(t, r, "/api/v1/pipelines?page=1&per_page=100", &list)

	// Positive denominator: an empty page would make every per-pipeline check
	// below vacuous.
	if len(list.Pipelines) != len(cases) || list.Pagination.Total != len(cases) {
		t.Fatalf("denominator: want %d seeded pipelines on the page and in the total, got page=%d total=%d",
			len(cases), len(list.Pipelines), list.Pagination.Total)
	}
	got := map[string]string{}
	for _, p := range list.Pipelines {
		got[p.ID] = p.DerivedStatus
	}

	wantByStatus := map[string]int{}
	for _, tc := range cases {
		wantByStatus[tc.wantList]++
		if got[tc.id] != tc.wantList {
			t.Errorf("%s: list derived_status = %q, want %q", tc.name, got[tc.id], tc.wantList)
		}
	}
	if wantByStatus["running"] == 0 {
		t.Fatalf("fixture must contain running pipelines, or the stat-card check is vacuous")
	}

	// The COUNT query must agree with the page for every filter value. Before the
	// fix the COUNT copy had no stale-heartbeat branch, so ?status=running counted
	// a pipeline the page badged 'passed'.
	for status, want := range wantByStatus {
		var filtered pgDSListResp
		pgDSGet(t, r, "/api/v1/pipelines?page=1&per_page=100&status="+status, &filtered)
		if filtered.Pagination.Total != len(filtered.Pipelines) {
			t.Errorf("?status=%s: COUNT total %d != rows on the page %d", status, filtered.Pagination.Total, len(filtered.Pipelines))
		}
		if len(filtered.Pipelines) != want {
			t.Errorf("?status=%s: page has %d rows, want %d", status, len(filtered.Pipelines), want)
		}
	}

	// The "Running" stat card counts exactly the list's running rows.
	var stats struct {
		Executions struct {
			Running int `json:"running"`
		} `json:"executions"`
	}
	pgDSGet(t, r, "/api/v1/pipelines/stats", &stats)
	if stats.Executions.Running != wantByStatus["running"] {
		t.Errorf("stats executions.running = %d, want %d (the list's running rows)", stats.Executions.Running, wantByStatus["running"])
	}

	// The detail page's /state answer agrees with the list badge.
	compared := 0
	for _, tc := range cases {
		if tc.wantState == "" {
			continue
		}
		var st struct {
			Status string `json:"status"`
		}
		pgDSGet(t, r, "/api/v1/pipelines/"+tc.id+"/state", &st)
		compared++
		if stateBucket(st.Status) != tc.wantState {
			t.Errorf("%s: /state status %q (bucket %q), want %q", tc.name, st.Status, stateBucket(st.Status), tc.wantState)
		}
		if stateBucket(st.Status) != got[tc.id] {
			t.Errorf("%s: list says %q but the detail page's /state says %q", tc.name, got[tc.id], st.Status)
		}
	}
	if compared == 0 {
		t.Fatalf("no /state comparisons ran")
	}
}
