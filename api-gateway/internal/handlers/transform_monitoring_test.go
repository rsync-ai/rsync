package handlers

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"api-gateway/internal/db"

	"github.com/DATA-DOG/go-sqlmock"
)

// Tenancy + behavior tests for the read-only transform monitoring/versioning
// endpoints (transform_monitoring.go). Shares the wsScope* / gateRoleRows /
// gateQueryRe / idorPipelineID helpers with the other handler tests.
//
// rollupQueryRe / historyQueryRe anchor on the FROM clause so the tests fail loudly
// if the tenant-scoping WHERE or the GROUP BY is ever dropped.
const (
	rollupQueryRe  = `FROM transform_execution_logs tel\s+WHERE tel\.pipeline_id = \$1\s+GROUP BY tel\.execution_id`
	historyQueryRe = `FROM transform_execution_logs tel\s+WHERE tel\.pipeline_id = \$1`
)

// --- P0' rollup: cross-workspace is denied before any aggregation runs. ---

func TestGetPipelineTransformRollup_CrossWorkspace404(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	h := NewTransformHandler(db.GetDB(), "")

	// Pipeline belongs to another workspace → gate finds no membership row → 404,
	// and the rollup query must never run.
	mock.ExpectQuery(gateQueryRe).
		WithArgs(idorPipelineID, wsScopeUser, wsScopeWS).
		WillReturnError(sql.ErrNoRows)

	r := wsScopeRouter(http.MethodGet, "/api/v1/transforms/pipeline/:pipeline_id/rollup", h.GetPipelineTransformRollup)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/transforms/pipeline/"+idorPipelineID+"/rollup", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 (cross-workspace denied), got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

// --- P0' rollup: a workspace viewer may read it, and the honest fields pass through. ---

func TestGetPipelineTransformRollup_ViewerAllowed(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	h := NewTransformHandler(db.GetDB(), "")

	first := time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)
	latest := time.Date(2026, 7, 17, 10, 5, 0, 0, time.UTC)

	mock.ExpectQuery(gateQueryRe).
		WithArgs(idorPipelineID, wsScopeUser, wsScopeWS).
		WillReturnRows(gateRoleRows("viewer")) // read requires only >= viewer
	mock.ExpectQuery(rollupQueryRe).
		WithArgs(idorPipelineID, 51). // default limit 50, queried as limit+1 for truncation detection
		WillReturnRows(sqlmock.NewRows([]string{
			"execution_id", "transform_count", "total_input_rows", "total_output_rows",
			"rows_dropped", "total_duration_ms", "failed_count", "first_activity_at", "latest_activity_at",
		}).AddRow("exec-1", 2, 1000, 720, 280, 1234, 0, first, latest))

	r := wsScopeRouter(http.MethodGet, "/api/v1/transforms/pipeline/:pipeline_id/rollup", h.GetPipelineTransformRollup)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/transforms/pipeline/"+idorPipelineID+"/rollup", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 (viewer reads rollup), got %d: %s", w.Code, w.Body.String())
	}
	var resp struct {
		PipelineID string                     `json:"pipeline_id"`
		Executions []TransformExecutionRollup `json:"executions"`
		Truncated  bool                       `json:"truncated"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v (%s)", err, w.Body.String())
	}
	if len(resp.Executions) != 1 {
		t.Fatalf("expected 1 execution, got %d", len(resp.Executions))
	}
	if resp.Truncated {
		t.Errorf("expected truncated=false when fewer than limit executions returned")
	}
	e := resp.Executions[0]
	if e.RowsDropped != 280 || e.TotalInputRows != 1000 || e.TotalOutputRows != 720 {
		t.Errorf("row totals not passed through: %+v", e)
	}
	if e.HasError {
		t.Errorf("expected HasError=false when failed_count=0")
	}
	if e.LatestActivityAt == nil || !e.LatestActivityAt.Equal(latest) {
		t.Errorf("freshness (latest_activity_at) not surfaced: %+v", e.LatestActivityAt)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

// --- P1 config history: cross-workspace is denied before any read. ---

func TestGetPipelineTransformConfigHistory_CrossWorkspace404(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	h := NewTransformHandler(db.GetDB(), "")

	mock.ExpectQuery(gateQueryRe).
		WithArgs(idorPipelineID, wsScopeUser, wsScopeWS).
		WillReturnError(sql.ErrNoRows)

	r := wsScopeRouter(http.MethodGet, "/api/v1/transforms/pipeline/:pipeline_id/config-history", h.GetPipelineTransformConfigHistory)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/transforms/pipeline/"+idorPipelineID+"/config-history", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 (cross-workspace denied), got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

// --- P1 config history: canonical dedup + top-level diff + dead-key stripping. ---

func TestGetPipelineTransformConfigHistory_DedupesAndDiffs(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	h := NewTransformHandler(db.GetDB(), "")

	t1 := time.Date(2026, 7, 17, 9, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 7, 17, 9, 5, 0, 0, time.UTC)
	t3 := time.Date(2026, 7, 17, 9, 10, 0, 0, time.UTC)
	t4 := time.Date(2026, 7, 17, 9, 15, 0, 0, time.UTC)

	// One "orders" slot (single revision) + one "users" slot with a real change,
	// a canonical-equal re-run, and an empty/failed snapshot. Rows arrive in the
	// query's ORDER BY (table_name ASC, transform_order ASC, created_at ASC) order.
	ordersCfg := `{"id":"t-ord","order":0,"type":"map","enabled":true,"config":{"operation":"rename","from":"a","to":"b"}}`
	usersV1 := `{"id":"t-usr","order":0,"type":"filter","enabled":true,"config":{"operation":"filter","condition":"age>30"},"scope":{"table":"users"}}`
	// Same as usersV1 but keys shuffled AND enabled flipped (a dead key) — must dedupe.
	usersV1Shuffled := `{"scope":{"table":"users"},"config":{"condition":"age>30","operation":"filter"},"type":"filter","order":0,"enabled":false,"id":"t-usr"}`
	usersV2 := `{"id":"t-usr","order":0,"type":"filter","enabled":true,"config":{"operation":"filter","condition":"age>40"},"scope":{"table":"users"}}`

	cols := []string{"execution_id", "table_name", "transform_order", "transform_id", "transform_type", "config_snapshot", "created_at"}
	mock.ExpectQuery(gateQueryRe).
		WithArgs(idorPipelineID, wsScopeUser, wsScopeWS).
		WillReturnRows(gateRoleRows("viewer"))
	mock.ExpectQuery(historyQueryRe).
		WithArgs(idorPipelineID, configHistoryScanCap+1).
		WillReturnRows(sqlmock.NewRows(cols).
			AddRow("exec-o", "orders", 0, "t-ord", "map", []byte(ordersCfg), t1).
			AddRow("exec-a", "users", 0, "t-usr", "filter", []byte(usersV1), t1).
			AddRow("exec-b", "users", 0, "t-usr", "filter", []byte(usersV1Shuffled), t2).
			AddRow("exec-c", "users", 0, "t-usr", "filter", []byte(usersV2), t3).
			AddRow("exec-d", "users", 0, "t-usr", "filter", []byte("{}"), t4))

	r := wsScopeRouter(http.MethodGet, "/api/v1/transforms/pipeline/:pipeline_id/config-history", h.GetPipelineTransformConfigHistory)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/transforms/pipeline/"+idorPipelineID+"/config-history", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp struct {
		PipelineID string                `json:"pipeline_id"`
		Slots      []TransformConfigSlot `json:"slots"`
		Truncated  bool                  `json:"truncated"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v (%s)", err, w.Body.String())
	}
	if resp.Truncated {
		t.Errorf("expected truncated=false for a small history")
	}
	if len(resp.Slots) != 2 {
		t.Fatalf("expected 2 slots (orders, users), got %d: %+v", len(resp.Slots), resp.Slots)
	}

	// Slots come back in table_name order: orders first, then users.
	orders := resp.Slots[0]
	if orders.TableName != "orders" || orders.RevisionCount != 1 {
		t.Errorf("orders slot: got table=%q revisions=%d", orders.TableName, orders.RevisionCount)
	}

	users := resp.Slots[1]
	if users.TableName != "users" {
		t.Fatalf("expected users slot second, got %q", users.TableName)
	}
	// exec-b (canonical-equal re-run) and exec-d (empty) must NOT create revisions.
	if users.RevisionCount != 2 {
		t.Fatalf("expected 2 users revisions (dedup + skip-empty), got %d: %+v", users.RevisionCount, users.Revisions)
	}
	if users.Revisions[0].ExecutionID != "exec-a" || len(users.Revisions[0].ChangedKeys) != 0 {
		t.Errorf("baseline revision wrong: %+v", users.Revisions[0])
	}
	// Regression: the baseline's changed_keys must serialize as [] not null.
	// A nil []string marshals to JSON null, which crashed the FE panel
	// (TransformMonitoringPanel did changed_keys.length). Unmarshaling [] yields
	// a non-nil empty slice; unmarshaling null yields nil — so this catches it.
	if users.Revisions[0].ChangedKeys == nil {
		t.Errorf("baseline changed_keys must be non-nil [] (JSON null crashes the FE)")
	}
	if users.Revisions[1].ExecutionID != "exec-c" {
		t.Errorf("second revision should be exec-c (the real change), got %q", users.Revisions[1].ExecutionID)
	}
	if len(users.Revisions[1].ChangedKeys) != 1 || users.Revisions[1].ChangedKeys[0] != "config" {
		t.Errorf("expected changed_keys=[config], got %v", users.Revisions[1].ChangedKeys)
	}
	// Dead keys must be stripped from what we return.
	if _, ok := users.Revisions[0].ConfigSnapshot["enabled"]; ok {
		t.Errorf("dead key 'enabled' should be stripped from config_snapshot")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

// clampListLimit unit coverage — cheap and it guards the limit contract.
func TestClampListLimit(t *testing.T) {
	cases := []struct {
		raw            string
		def, max, want int
	}{
		{"", 50, 200, 50},
		{"0", 50, 200, 50},
		{"-3", 50, 200, 50},
		{"abc", 50, 200, 50},
		{"10", 50, 200, 10},
		{"9999", 50, 200, 200},
	}
	for _, c := range cases {
		if got := clampListLimit(c.raw, c.def, c.max); got != c.want {
			t.Errorf("clampListLimit(%q,%d,%d) = %d, want %d", c.raw, c.def, c.max, got, c.want)
		}
	}
}
