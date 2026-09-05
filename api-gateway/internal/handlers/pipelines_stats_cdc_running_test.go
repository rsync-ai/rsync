package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// Regression guards for KI-PIPELINES-LIST-RUNNING-STATCARD-ZERO.
//
// The Pipelines page asked "is this pipeline running?" twice and got two
// different answers: the per-row badge came from the list's derived_status CASE
// (which treats a healthy CDC stream as running), while the "Running" stat card
// came from COUNT(*) FILTER (WHERE e.status = 'running') over executions. A
// streaming CDC pipeline has NO executions row in status 'running' — the
// temporal-adapter closes the backfill execution at the snapshot→streaming
// handoff — so the card read 0 on the very page whose row badge said "Running".
//
// The fix makes the card count PIPELINES with the list's own predicates. These
// tests pin the two properties that matter: the running probe is a pipelines
// query (not an executions FILTER), and the synthetic CDC audit rows are
// excluded from every executions roll-up.
//
// wsScopeMockDB / wsScopeRouter / wsScopeWS come from the sibling
// *_scoping_test.go files (same package).

// statsRows is the shape GetPipelineStats' three queries return, in order:
// pipelines total/active, the executions roll-up, then the running pipeline
// count. recent_runs is served last and left empty here.
func statsExpectations(t *testing.T, mock sqlmock.Sqlmock, running int) {
	t.Helper()

	mock.ExpectQuery(`FROM pipelines WHERE workspace_id = \$1`).
		WithArgs(wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"count", "active"}).AddRow(3, 3))

	// The executions roll-up must NOT carry a 'running' FILTER any more, and it
	// must exclude synthetic CDC audit rows (id = pipeline_id).
	mock.ExpectQuery(`COUNT\(\*\) FILTER \(WHERE e\.status = 'completed'\)[\s\S]*e\.id <> e\.pipeline_id`).
		WithArgs(wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"completed", "failed", "total"}).AddRow(7, 1, 8))

	// The running card is a COUNT over pipelines, gated on the list's
	// derived_status predicates — including the CDC branch.
	mock.ExpectQuery(`SELECT COUNT\(\*\)[\s\S]*FROM pipelines p[\s\S]*p\.sync_mode = 'cdc'`).
		WithArgs(wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"running"}).AddRow(running))

	mock.ExpectQuery(`FROM executions e[\s\S]*ORDER BY e\.start_time DESC LIMIT 10`).
		WithArgs(wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"pipeline_id", "status", "start_time", "end_time"}))
}

func getStats(t *testing.T) map[string]any {
	t.Helper()
	r := wsScopeRouter(http.MethodGet, "/api/v1/pipelines/stats", GetPipelineStats)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/pipelines/stats", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal response: %v (%s)", err, w.Body.String())
	}
	return body
}

// TestGetPipelineStats_RunningCardCountsStreamingCDCPipelines is the core guard:
// with zero open executions rows but one live CDC stream, the card must report 1.
// Before the fix this endpoint reported 0 while the row badge said "Running".
func TestGetPipelineStats_RunningCardCountsStreamingCDCPipelines(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	statsExpectations(t, mock, 1)

	body := getStats(t)
	execs, ok := body["executions"].(map[string]any)
	if !ok {
		t.Fatalf("missing executions object: %v", body)
	}
	if got := execs["running"]; got != float64(1) {
		t.Fatalf("running card: want 1 (the streaming CDC pipeline), got %v", got)
	}
	// The sibling counts must still be executions-row counts — the card labels
	// say "Successful runs"/"Failed runs", so changing their meaning would be a
	// different lie, not a fix.
	if got := execs["completed"]; got != float64(7) {
		t.Fatalf("completed: want 7, got %v", got)
	}
	if got := execs["failed"]; got != float64(1) {
		t.Fatalf("failed: want 1, got %v", got)
	}
	if got := execs["total"]; got != float64(8) {
		t.Fatalf("total: want 8, got %v", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sqlmock expectations: %v", err)
	}
}

// TestGetPipelineStats_RunningCardIsZeroWhenNothingRuns proves the new predicate
// can still say zero — a card that always reports "running" would be as wrong as
// one that always reported 0.
func TestGetPipelineStats_RunningCardIsZeroWhenNothingRuns(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	statsExpectations(t, mock, 0)

	body := getStats(t)
	execs := body["executions"].(map[string]any)
	if got := execs["running"]; got != float64(0) {
		t.Fatalf("running card: want 0, got %v", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sqlmock expectations: %v", err)
	}
}

// TestGetPipelineStats_KeepsResponseShape pins the JSON contract. The fix
// deliberately kept the key name and nesting (executions.running) so the
// frontend needed no change — PipelinesPageClient reads
// pipelineStats.executions?.running for the card labelled "Running". A rename
// here would silently blank the card.
func TestGetPipelineStats_KeepsResponseShape(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	statsExpectations(t, mock, 2)

	body := getStats(t)
	for _, key := range []string{"pipelines", "executions", "recent_runs"} {
		if _, ok := body[key]; !ok {
			t.Fatalf("response lost top-level key %q: %v", key, body)
		}
	}
	execs, ok := body["executions"].(map[string]any)
	if !ok {
		t.Fatalf("executions is not an object: %v", body["executions"])
	}
	for _, key := range []string{"total", "running", "completed", "failed"} {
		if _, ok := execs[key]; !ok {
			t.Fatalf("executions lost key %q: %v", key, execs)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sqlmock expectations: %v", err)
	}
}
