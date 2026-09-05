package handlers

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// RBAC P1 (A1): pipeline executions are WORKSPACE-owned, not user-owned. The
// list/get/cancel handlers historically filtered by pipelines.created_by, so a
// teammate who shares a workspace could neither SEE nor CANCEL a run another
// member started — collaboration-breaking. The fix scopes these to the caller's
// ACTIVE workspace:
//   - ListExecutions / GetExecution: read, scoped by p.workspace_id (any member,
//     incl. a viewer, sees the workspace's runs).
//   - CancelExecution: mutation, scoped by p.workspace_id AND gated WSMember (a
//     viewer may watch a run but not cancel it).
//
// These reproducers bind the ACTIVE WORKSPACE (wsScopeWS) — distinct from the
// caller's user id (wsScopeUser) — so the pre-fix created_by scoping fails
// loudly (arg mismatch → 404/500), turning GREEN only once the WHERE clause is
// workspace-scoped. wsScopeUser / wsScopeWS / wsScopePipeline / wsScopeMockDB /
// wsScopeRouterAsRole / gateRoleRows come from sibling *_test.go files.

const wsScopeExecution = "44444444-4444-4444-4444-444444444444"

func TestGetExecution_ScopedToActiveWorkspace(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	// Post-fix the single fetch binds the ACTIVE WORKSPACE ($2), not the caller's
	// user id. A VIEWER may read — reads have no role gate, only workspace scoping.
	// The 11th column (records_processed) is the pipeline_run_table_stats SUM —
	// see TestGetExecution_RecordsProcessedFromTableStats, which pins where it
	// comes from. Here it only has to be present so the Scan arity matches.
	mock.ExpectQuery(`FROM executions e`).
		WithArgs(wsScopeExecution, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "pipeline_id", "status", "trigger_source", "schedule_id",
			"scheduled_time", "start_time", "end_time", "error_message", "pipeline_name",
			"records_processed",
		}).AddRow(wsScopeExecution, wsScopePipeline, "completed", "manual", nil, nil, nil, nil, nil, "Shared Pipe", int64(0)))

	r := wsScopeRouterAsRole(http.MethodGet, "/executions/:id", GetExecution, "viewer")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/executions/"+wsScopeExecution, nil))

	if w.Code != http.StatusOK {
		t.Fatalf("a viewer must read a teammate's execution in the active workspace; want 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestListExecutions_ScopedToActiveWorkspace(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)

	// Global view (GET /executions, no pipeline filter). ALL THREE writes/reads
	// must bind the active workspace, not the caller. We mock each one with an
	// explicit wsScopeWS arg and assert ExpectationsWereMet so a regression that
	// drops the scope from the repair UPDATE or the COUNT (whose errors the
	// handler swallows) is caught — a swallowed-error query is invisible to a
	// response-code-only assertion, so it MUST be pinned by an expectation.
	mock.ExpectExec(`INTERVAL '5 minutes'`). // global stale-run repair
							WithArgs(wsScopeWS).
							WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM executions`).
		WithArgs(wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectQuery(`pipeline_run_table_stats`). // main list query
							WithArgs(wsScopeWS).
							WillReturnRows(sqlmock.NewRows([]string{
			"id", "pipeline_id", "status", "trigger_source", "schedule_id",
			"scheduled_time", "start_time", "end_time", "error_message",
			"pipeline_name", "records_processed",
		}))

	r := wsScopeRouterAsRole(http.MethodGet, "/executions", ListExecutions, "viewer")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/executions", nil))

	if w.Code != http.StatusOK {
		t.Fatalf("execution list must be scoped to the active workspace (not created_by); want 200, got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("repair/COUNT/list must each bind the active workspace: %v", err)
	}
}

// TestListExecutions_PerPipelineRepairScopedToWorkspace pins the nested route
// GET /pipelines/:id/executions. The per-pipeline stale-run repair UPDATE used
// to fire on the bare path :id with NO workspace check — so a member of WS-A
// hitting /pipelines/<WS-B-pipeline>/executions would mutate WS-B's running
// rows even though the SELECT (correctly scoped) returned nothing. The repair
// must bind the active workspace ($2) just like the global branch does. The
// repair error is swallowed, so this is provable ONLY by pinning the Exec arg
// and asserting ExpectationsWereMet — a 200 alone says nothing about the write.
func TestListExecutions_PerPipelineRepairScopedToWorkspace(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)

	// BUG-2: the nested route now runs an existence/visibility pre-check first;
	// for a pipeline that IS in the workspace it returns true and the handler
	// proceeds to the repair/COUNT/list queries below.
	mock.ExpectQuery(`SELECT EXISTS`).
		WithArgs(wsScopePipeline, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))

	// Repair UPDATE must carry BOTH the pipeline id and the active workspace.
	mock.ExpectExec(`Cancelled \(stale run\)`).
		WithArgs(wsScopePipeline, wsScopeWS).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM executions`).
		WithArgs(wsScopeWS, wsScopePipeline).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectQuery(`pipeline_run_table_stats`).
		WithArgs(wsScopeWS, wsScopePipeline).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "pipeline_id", "status", "trigger_source", "schedule_id",
			"scheduled_time", "start_time", "end_time", "error_message",
			"pipeline_name", "records_processed",
		}))

	r := wsScopeRouterAsRole(http.MethodGet, "/pipelines/:id/executions", ListExecutions, "viewer")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/pipelines/"+wsScopePipeline+"/executions", nil))

	if w.Code != http.StatusOK {
		t.Fatalf("nested execution list want 200, got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("per-pipeline stale-run repair must bind the active workspace ($2): %v", err)
	}
}

// TestListExecutions_PerPipelineRepairHasAgeGuard pins that the per-pipeline
// stale-run repair UPDATE (GET /pipelines/:id/executions) carries the same
// `start_time < NOW() - INTERVAL '5 minutes'` age bound the global branch has.
//
// Without it, polling this endpoint in the brief window right after launching a
// run — before the new run seeds pipeline_progress.execution_id, while pp still
// points at the PREVIOUS terminal run — cancels the FRESH run with
// error_message="Cancelled (stale run)" (observed on staging 2026-07-17). The
// age guard is exactly what excludes a just-launched run (start_time ≈ now).
//
// The repair error is swallowed by the handler, so a 200 alone proves nothing:
// we pin the Exec pattern to REQUIRE the age guard and assert ExpectationsWereMet.
// If the guard is dropped, the emitted query no longer matches this expectation,
// the (swallowed) Exec errors, and the unmet expectation fails the test.
func TestListExecutions_PerPipelineRepairHasAgeGuard(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)

	// Existence/visibility pre-check for the nested route.
	mock.ExpectQuery(`SELECT EXISTS`).
		WithArgs(wsScopePipeline, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))

	// The repair UPDATE MUST contain the age guard (and still bind pipeline + ws).
	mock.ExpectExec(`start_time < NOW\(\) - INTERVAL '5 minutes'`).
		WithArgs(wsScopePipeline, wsScopeWS).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM executions`).
		WithArgs(wsScopeWS, wsScopePipeline).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectQuery(`pipeline_run_table_stats`).
		WithArgs(wsScopeWS, wsScopePipeline).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "pipeline_id", "status", "trigger_source", "schedule_id",
			"scheduled_time", "start_time", "end_time", "error_message",
			"pipeline_name", "records_processed",
		}))

	r := wsScopeRouterAsRole(http.MethodGet, "/pipelines/:id/executions", ListExecutions, "viewer")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/pipelines/"+wsScopePipeline+"/executions", nil))

	if w.Code != http.StatusOK {
		t.Fatalf("nested execution list want 200, got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("per-pipeline stale-run repair must age-guard the UPDATE so a fresh run isn't cancelled: %v", err)
	}
}

// TestGetExecutionTransformLogs_ScopedToActiveWorkspace pins the transform-logs
// read. It was the one execution read A1 missed: it stayed on p.created_by = $2
// (the caller), so a teammate in the same workspace got 0 rows. It must scope by
// p.workspace_id like GetExecution, and a viewer (read, no role gate) may see it.
func TestGetExecutionTransformLogs_ScopedToActiveWorkspace(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	// Post-fix the query binds the ACTIVE WORKSPACE ($2), not the caller's user
	// id. Pre-fix it bound wsScopeUser → arg mismatch → query error → 500.
	mock.ExpectQuery(`FROM transform_execution_logs tel`).
		WithArgs(wsScopeExecution, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "pipeline_id", "execution_id", "table_name", "transform_id",
			"transform_order", "transform_type", "status", "error_message",
			"input_rows", "output_rows", "duration_ms", "config_snapshot",
			"created_at", "updated_at",
		}))

	r := wsScopeRouterAsRole(http.MethodGet, "/executions/:id/transforms", GetExecutionTransformLogs, "viewer")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/executions/"+wsScopeExecution+"/transforms", nil))

	if w.Code != http.StatusOK {
		t.Fatalf("transform logs must be workspace-scoped so a teammate sees them; want 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCancelExecution_ViewerDenied(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)

	// Status lookup is workspace-scoped (binds $2 = active workspace) and yields
	// the pipeline id, which the WSMember gate then authorizes against.
	mock.ExpectQuery(`SELECT e\.status, e\.pipeline_id`).
		WithArgs(wsScopeExecution, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"status", "pipeline_id"}).AddRow("running", wsScopePipeline))
	mock.ExpectQuery(`SELECT wm\.role\s+FROM pipelines r`).
		WithArgs(wsScopePipeline, wsScopeUser, wsScopeWS).
		WillReturnRows(gateRoleRows("viewer"))

	r := wsScopeRouterAsRole(http.MethodPost, "/executions/:id/cancel", CancelExecution, "viewer")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/executions/"+wsScopeExecution+"/cancel", nil))

	if w.Code != http.StatusForbidden || !strings.Contains(w.Body.String(), "insufficient workspace role") {
		t.Fatalf("a viewer must be denied execution-cancel with a role error; got %d: %s", w.Code, w.Body.String())
	}
}
