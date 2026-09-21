package handlers

import (
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// GetPipeline used to answer 404 pipeline_not_found for ANY error from its detail
// query. The workspace-role gate has already seen the row by then, so the only honest
// "not found" is a pipeline deleted in between. A dropped connection, a timeout or a
// scan failure told the caller a live pipeline was gone, and the model page's Graph tab
// had to special-case the code because it could not tell the two apart.
//
// Each branch is pinned on its own, as in pipeline_namespace_lock_notfound_test.go:
// a handler that 404s every error passes the first test and fails the second; one
// that 500s every error does the reverse.
//
// wsScope* / wsScopeMockDB / wsScopeRouterAsRole / gateRoleRows come from the sibling
// *_test.go files (same package).

// getPipelineAfterGate passes the viewer read gate, then hands the detail query the
// given error (or, with badRow, a row whose created_at cannot scan into time.Time).
func getPipelineAfterGate(t *testing.T, detailErr error, badRow bool) *httptest.ResponseRecorder {
	t.Helper()
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	mock.ExpectQuery(`SELECT wm\.role\s+FROM pipelines r`).
		WithArgs(wsScopePipeline, wsScopeUser, wsScopeWS).
		WillReturnRows(gateRoleRows("viewer"))
	detail := mock.ExpectQuery(`WITH latest_exec AS`).WithArgs(wsScopePipeline)
	if badRow {
		detail.WillReturnRows(sqlmock.NewRows([]string{
			"id", "name", "description", "status", "created_at", "updated_at",
			"created_by", "source_connection_id", "destination_connection_id",
			"sync_mode", "cdc_mode", "sync_mode_source", "config",
			"source_connection", "destination_connection", "last_execution",
			"rows_processed",
		}).AddRow(wsScopePipeline, "Shared Pipe", nil, "active", "not-a-time", time.Now(),
			nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, int64(0)))
	} else {
		detail.WillReturnError(detailErr)
	}

	r := wsScopeRouterAsRole(http.MethodGet, "/pipelines/:id", GetPipeline, "viewer")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/pipelines/"+wsScopePipeline, nil))

	// Both queries ran: the answer came from the detail query's branch, not the gate.
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
	return w
}

// TestGetPipeline_RowVanishedAfterGateIs404: the pipeline was deleted between the
// gate and the detail read. That is the one case pipeline_not_found is for.
func TestGetPipeline_RowVanishedAfterGateIs404(t *testing.T) {
	for name, err := range map[string]error{
		"bare":    sql.ErrNoRows,
		"wrapped": fmt.Errorf("detail read: %w", sql.ErrNoRows),
	} {
		t.Run(name, func(t *testing.T) {
			w := getPipelineAfterGate(t, err, false)
			if w.Code != http.StatusNotFound {
				t.Fatalf("a row deleted after the gate must be 404; got %d: %s", w.Code, w.Body.String())
			}
			body := w.Body.String()
			if !strings.Contains(body, `"pipeline_not_found"`) {
				t.Fatalf("want error code pipeline_not_found, got: %s", body)
			}
			if strings.Contains(body, sql.ErrNoRows.Error()) {
				t.Fatalf("raw driver text leaked into the body: %s", body)
			}
		})
	}
}

// TestGetPipeline_DBErrorIs5xxNotNotFound: a failed read of a pipeline that exists is
// a server error, never pipeline_not_found, and the driver's words stay in the log.
func TestGetPipeline_DBErrorIs5xxNotNotFound(t *testing.T) {
	driverErr := errors.New("pq: could not connect to server: connection refused")
	cases := []struct {
		name    string
		err     error
		badRow  bool
		leakers []string
	}{
		{name: "connection refused", err: driverErr, leakers: []string{driverErr.Error(), "pq:"}},
		{name: "scan failure", badRow: true, leakers: []string{"Scan error", "sql:", "not-a-time"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := getPipelineAfterGate(t, tc.err, tc.badRow)
			if w.Code != http.StatusInternalServerError {
				t.Fatalf("a failed read must be 500, not %d: %s", w.Code, w.Body.String())
			}
			body := w.Body.String()
			if strings.Contains(body, "pipeline_not_found") {
				t.Fatalf("a failed read must not claim the pipeline is gone: %s", body)
			}
			if !strings.Contains(body, `"pipeline_fetch_failed"`) {
				t.Fatalf("want error code pipeline_fetch_failed, got: %s", body)
			}
			for _, s := range tc.leakers {
				if strings.Contains(body, s) {
					t.Fatalf("raw DB error text %q leaked into the body: %s", s, body)
				}
			}
		})
	}
}
