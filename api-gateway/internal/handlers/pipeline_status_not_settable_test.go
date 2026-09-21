package handlers

// PATCH /pipelines/:id used to write any status string. "archived" left CDC running
// with its replication slot, and archived pipelines did not block deleting the source
// connection, whose cascade erased the slot's record. Status now follows the lifecycle
// endpoints, and a connection stays undeletable while an archived pipeline still holds
// CDC resources.

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

const pipelineStatusRead = `SELECT status FROM pipelines WHERE id = \$1 AND workspace_id = \$2`

func patchPipeline(t *testing.T, body map[string]any) *httptest.ResponseRecorder {
	t.Helper()
	b, _ := json.Marshal(body)
	r := wsScopeRouter(http.MethodPatch, "/api/v1/pipelines/:id", UpdatePipeline)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPatch, "/api/v1/pipelines/"+wsScopePipeline, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	return w
}

func expectPipelineGate(mock sqlmock.Sqlmock) {
	mock.ExpectQuery(`SELECT wm\.role\s+FROM pipelines r`).
		WithArgs(wsScopePipeline, wsScopeUser, wsScopeWS).
		WillReturnRows(gateRoleRows("member"))
}

func TestUpdatePipelineRefusesToArchiveARunningPipeline(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)
	expectPipelineGate(mock)
	mock.ExpectQuery(pipelineStatusRead).
		WithArgs(wsScopePipeline, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("running"))
	// Armed and required to stay unused: a swallowed unexpected Exec would not show.
	mock.ExpectExec(`UPDATE pipelines SET`).WillReturnResult(sqlmock.NewResult(0, 1))

	w := patchPipeline(t, map[string]any{"name": "renamed", "status": "archived"})

	if w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "status_not_settable") {
		t.Fatalf("want 400 status_not_settable, got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err == nil || !strings.Contains(err.Error(), "UPDATE pipelines SET") {
		t.Fatalf("a rejected request must write nothing, including the name; expectations: %v", err)
	}
}

// A client that sends the pipeline back with its current status is not an error.
func TestUpdatePipelineAcceptsTheCurrentStatusAsANoOp(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)
	expectPipelineGate(mock)
	mock.ExpectQuery(pipelineStatusRead).
		WithArgs(wsScopePipeline, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("running"))
	mock.ExpectExec(`UPDATE pipelines SET name=\$1`).
		WithArgs("renamed", wsScopePipeline, wsScopeWS).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`UPDATE pipelines SET status=`).WillReturnResult(sqlmock.NewResult(0, 1))

	w := patchPipeline(t, map[string]any{"name": "renamed", "status": "Running"})

	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", w.Code, w.Body.String())
	}
	err := mock.ExpectationsWereMet()
	if err == nil || !strings.Contains(err.Error(), "UPDATE pipelines SET status=") {
		t.Fatalf("the name must be written and the status must not; expectations: %v", err)
	}
	if strings.Contains(err.Error(), "SET name=") {
		t.Fatalf("the name write did not happen: %v", err)
	}
}

func TestUpdatePipelineStatusForAPipelineOutsideTheWorkspaceIsNotFound(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)
	expectPipelineGate(mock)
	mock.ExpectQuery(pipelineStatusRead).
		WithArgs(wsScopePipeline, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"status"}))

	if w := patchPipeline(t, map[string]any{"status": "archived"}); w.Code != http.StatusNotFound {
		t.Fatalf("want 404, got %d: %s", w.Code, w.Body.String())
	}
}

// The delete guard's predicate is SQL; sqlmock cannot evaluate it, so pin the clauses
// that carry the rule and prove both guard queries use it.
func TestConnectionDeleteGuardCountsArchivedPipelinesThatStillHoldCDC(t *testing.T) {
	for _, clause := range []string{
		"(source_connection_id = $1 OR destination_connection_id = $1)",
		"status != 'archived' OR EXISTS",
		"r.pipeline_id = pipelines.id AND r.status <> 'deleted'",
	} {
		if !strings.Contains(pipelineNeedsConnection, clause) {
			t.Errorf("pipelineNeedsConnection lost %q", clause)
		}
	}

	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)
	mock.ExpectQuery(`SELECT wm\.role\s+FROM connections r`).
		WithArgs(wsScopeConnection, wsScopeUser, wsScopeWS).
		WillReturnRows(gateRoleRows("member"))
	predicate := regexp.QuoteMeta(pipelineNeedsConnection)
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM pipelines\s+WHERE ` + predicate).
		WithArgs(wsScopeConnection).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectQuery(`FROM pipelines\s+WHERE ` + predicate + `\s+ORDER BY updated_at DESC`).
		WithArgs(wsScopeConnection).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "status", "role"}).
			AddRow(wsScopePipeline, "orders cdc", "archived", "source"))

	r := wsScopeRouter(http.MethodDelete, "/api/v1/connections/:id", DeleteConnection)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodDelete, "/api/v1/connections/"+wsScopeConnection, nil))

	if w.Code != http.StatusConflict || !strings.Contains(w.Body.String(), `"status":"archived"`) {
		t.Fatalf("want 409 naming the archived pipeline, got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("both guard queries must use the shared predicate: %v", err)
	}
}
