package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// Issues #7/#21: the free-plan banner must show the SAME pipeline count the
// create gate enforces. GET /api/v1/usage/plan resolves the ACTIVE workspace
// (as checkPipelineCreateOK does) and counts through countWorkspacePipelines —
// the query the gate uses — so the two numbers cannot drift.
func TestGetPlanSummary_UsesActiveWorkspaceAndGateCount(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	mock.ExpectQuery(`FROM plans`).WillReturnRows(planCatalogueRows())
	mock.ExpectQuery(`FROM workspaces w`).
		WithArgs(wsScopeWS).
		WillReturnRows(workspacePlanRow("free"))
	mock.ExpectQuery(`COUNT\(\*\) FROM pipelines WHERE workspace_id = \$1`).
		WithArgs(wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectQuery(`SELECT plan_expires_at FROM workspaces WHERE id = \$1`).
		WithArgs(wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"plan_expires_at"}).AddRow(nil))

	r := wsScopeRouter(http.MethodGet, "/api/v1/usage/plan", GetPlanSummary)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/usage/plan", nil))

	if w.Code != http.StatusOK {
		t.Fatalf("GetPlanSummary = %d (%s), want 200", w.Code, w.Body.String())
	}
	var body map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["plan"] != "free" || body["pipelines_used"] != float64(1) || body["pipelines_limit"] != float64(2) {
		t.Fatalf("unexpected summary: %v", body)
	}
	if body["workspace_id"] != wsScopeWS {
		t.Fatalf("summary must be for the active workspace, got %v", body["workspace_id"])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

func TestGetPlanSummary_DatabaseUnavailableReturns503(t *testing.T) {
	defer withNilDB(t)()
	r := wsScopeRouter(http.MethodGet, "/api/v1/usage/plan", GetPlanSummary)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/usage/plan", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("GetPlanSummary with no DB = %d, want 503", w.Code)
	}
}
