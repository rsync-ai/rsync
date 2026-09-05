package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// BUG-2: GET /pipelines/{id}/executions must 404 when the pipeline id is not
// visible in the caller's active workspace — matching GET /pipelines/{id} and
// /state, which already 404. Pre-fix it returned 200 {"executions":[],"total":0}
// (indistinguishable from "this pipeline has no runs"). The fix adds an existence
// pre-check (SELECT EXISTS ... WHERE id=$1 AND workspace_id=$2) that returns 404
// before any repair/count/list query runs. No data leak either way (all reads are
// workspace-scoped), but the status code must not conflate "unknown" with "empty".
// Helpers (wsScopeWS/wsScopeRouterAsRole) come from sibling *_test.go files.

const foreignPipeline = "55555555-5555-5555-5555-555555555555"

func TestListExecutions_ForeignPipelineID_404(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	// Existence/visibility pre-check returns false → handler must 404 immediately,
	// before the stale-run repair / COUNT / list queries.
	mock.ExpectQuery(`SELECT EXISTS`).
		WithArgs(foreignPipeline, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))

	r := wsScopeRouterAsRole(http.MethodGet, "/pipelines/:id/executions", ListExecutions, "viewer")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/pipelines/"+foreignPipeline+"/executions", nil))

	if w.Code != http.StatusNotFound {
		t.Fatalf("foreign/unknown pipeline id must 404 (not 200-empty); got %d: %s", w.Code, w.Body.String())
	}
}
