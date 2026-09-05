package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// BUG-1: GetPipeline + ListPipelines must report the SAME authoritative row count
// that GET /pipelines/{id}/executions reports — the SUM over
// pipeline_run_table_stats (GREATEST(batch inserted_rows, CDC applied_*)). Before
// the fix, both read only executions.metrics (a nullable JSON column that stays
// null for completed runs), so rows_processed came back 0 and
// last_execution.metrics was null even though data moved.
//
// These reproducers pin the authoritative source by anchoring the main read on
// pipeline_run_table_stats — absent pre-fix, sqlmock has no matching expectation,
// the handler's read fails, and the assertion goes RED. wsScopeUser/wsScopeWS/
// wsScopeMockDB/wsScopeRouter/wsScopeRouterAsRole/gateRoleRows come from sibling
// *_test.go files.

const rcPipeline = "33333333-3333-3333-3333-333333333333"

func TestGetPipeline_RowCountFromTableStats(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	// GetPipeline runs several best-effort follow-up reads (data-loading strategy,
	// schedule) whose errors it swallows; we only pin the role gate + main read and
	// do NOT assert ExpectationsWereMet so those unmocked reads degrade to 200.
	mock.MatchExpectationsInOrder(false)

	// Role gate (requireResourceRole): owner may read. Binds resource id + caller +
	// active workspace.
	mock.ExpectQuery(`SELECT wm\.role\s+FROM pipelines r`).
		WithArgs(rcPipeline, wsScopeUser, wsScopeWS).
		WillReturnRows(gateRoleRows("owner"))

	ts := time.Unix(1700000000, 0).UTC()
	// last_execution.metrics carries records_processed post-fix (merged in SQL).
	lastExec := `{"id":"e1","status":"completed","metrics":{"records_processed":928},"records_processed":928}`
	// Post-fix the main read joins pipeline_run_table_stats and projects a 17th
	// column (rows_processed) scanned into Pipeline.RowsProcessed. cdc_mode is set
	// so GetPipeline skips the non-CDC pipeline_progress reconciliation path.
	mock.ExpectQuery(`pipeline_run_table_stats`).
		WithArgs(rcPipeline).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "name", "description", "status", "created_at", "updated_at",
			"created_by", "source_connection_id", "destination_connection_id",
			"sync_mode", "cdc_mode", "sync_mode_source", "config",
			"source_connection", "destination_connection", "last_execution",
			"rows_processed",
		}).AddRow(
			rcPipeline, "P", nil, "completed", ts, ts,
			nil, nil, nil, nil, "initial", nil, []byte(`{}`),
			nil, nil, lastExec, int64(928),
		))

	r := wsScopeRouterAsRole(http.MethodGet, "/pipelines/:id", GetPipeline, "owner")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/pipelines/"+rcPipeline, nil))

	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("bad json: %v", err)
	}
	if got, _ := resp["rows_processed"].(float64); got != 928 {
		t.Fatalf("rows_processed must come from pipeline_run_table_stats; want 928, got %v", resp["rows_processed"])
	}
	le, _ := resp["last_execution"].(map[string]any)
	if le == nil {
		t.Fatalf("last_execution must be non-null after a completed run; got %v", resp["last_execution"])
	}
	m, _ := le["metrics"].(map[string]any)
	if m == nil || m["records_processed"] == nil {
		t.Fatalf("last_execution.metrics.records_processed must be populated; got %v", le)
	}
}

// TestListPipelines_RowCountSourcedFromTableStats pins that the list page query
// derives its count from pipeline_run_table_stats (not the stale executions.metrics
// JSON). The page query is uniquely identified by its LIMIT/OFFSET tail AND must
// now contain pipeline_run_table_stats; the COUNT query is matched separately.
func TestListPipelines_RowCountSourcedFromTableStats(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)

	cols := []string{
		"id", "name", "description", "pipeline_status", "created_at", "updated_at",
		"created_by", "sync_mode", "cdc_mode", "source_connection", "destination_connection",
		"schedule", "last_execution", "derived_status",
	}
	// Page query: must reference pipeline_run_table_stats AND end in LIMIT/OFFSET.
	mock.ExpectQuery(`pipeline_run_table_stats[\s\S]*LIMIT \$7 OFFSET \$8`).
		WithArgs(wsScopeWS, sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows(cols))
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM base`).
		WithArgs(wsScopeWS, sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	r := wsScopeRouter(http.MethodGet, "/api/v1/pipelines", ListPipelines)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/pipelines", nil))

	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("list page query must source the count from pipeline_run_table_stats: %v", err)
	}
}

// GET /executions/{id} must derive records_processed from the same
// pipeline_run_table_stats SUM that ListExecutions uses. Before the fix
// GetExecution selected 10 columns, never touched pipeline_run_table_stats and
// never populated Metrics — so the execution detail page rendered no record
// count while the Execution History row it was opened from rendered one. Same
// execution, two answers.
//
// This is a RED-first reproducer by construction: the expectation is anchored on
// the substring `pipeline_run_table_stats`, which the pre-fix query does not
// contain. sqlmock then has no matching expectation, QueryRow errors, and
// GetExecution's error branch returns 404 — the want-200 assertion fails.
func TestGetExecution_RecordsProcessedFromTableStats(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	ts := time.Unix(1700000000, 0).UTC()
	mock.ExpectQuery(`pipeline_run_table_stats`).
		WithArgs(wsScopeExecution, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "pipeline_id", "status", "trigger_source", "schedule_id",
			"scheduled_time", "start_time", "end_time", "error_message",
			"pipeline_name", "records_processed",
		}).AddRow(
			wsScopeExecution, rcPipeline, "completed", "manual", nil,
			nil, ts, ts, nil,
			"P", int64(2000),
		))

	r := wsScopeRouter(http.MethodGet, "/executions/:id", GetExecution)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/executions/"+wsScopeExecution, nil))

	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("bad json: %v", err)
	}
	m, _ := resp["metrics"].(map[string]any)
	if m == nil {
		t.Fatalf("metrics must be populated when the run moved rows; got %v", resp["metrics"])
	}
	if got, _ := m["records_processed"].(float64); got != 2000 {
		t.Fatalf("records_processed must come from pipeline_run_table_stats; want 2000, got %v", m["records_processed"])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("GetExecution must read pipeline_run_table_stats: %v", err)
	}
}

// A run that moved zero rows must stay "—" in the UI, not a noisy "0": metrics
// is omitted entirely (omitempty on a nil pointer) rather than serialized as
// {"records_processed":0}. Mirrors the `recordsProcessed > 0` guard in
// ListExecutions.
func TestGetExecution_ZeroRowsOmitsMetrics(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	ts := time.Unix(1700000000, 0).UTC()
	mock.ExpectQuery(`pipeline_run_table_stats`).
		WithArgs(wsScopeExecution, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "pipeline_id", "status", "trigger_source", "schedule_id",
			"scheduled_time", "start_time", "end_time", "error_message",
			"pipeline_name", "records_processed",
		}).AddRow(
			wsScopeExecution, rcPipeline, "completed", "manual", nil,
			nil, ts, ts, nil,
			"P", int64(0),
		))

	r := wsScopeRouter(http.MethodGet, "/executions/:id", GetExecution)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/executions/"+wsScopeExecution, nil))

	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("bad json: %v", err)
	}
	if _, present := resp["metrics"]; present {
		t.Fatalf("metrics must be omitted for a zero-row run; got %v", resp["metrics"])
	}
}
