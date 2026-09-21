package handlers

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

var runHistoryColumns = []string{"run_id", "saved_query_id", "schedule_id", "trigger_source", "status",
	"statement_class", "target_table", "rows_affected", "error", "auto_pause_reason", "started_at", "finished_at",
	"ran_as_user_id", "upstream_kind", "upstream_id", "upstream_name", "upstream_run_id", "origin_execution_id",
	"trigger_depth", "coalesced_count", "skip_reason"}

func expectVisibleRunsModel(mock sqlmock.Sqlmock) {
	mock.ExpectQuery(`FROM saved_queries r\s+JOIN workspace_members`).
		WithArgs(savedQueryID, wsScopeUser, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("viewer"))
	mock.ExpectQuery(`FROM saved_queries\s+WHERE id = \$1 AND workspace_id = \$2`).
		WithArgs(savedQueryID, wsScopeWS, wsScopeUser).
		WillReturnRows(sqlmock.NewRows([]string{"visible"}).AddRow(1))
}

func addRun(rows *sqlmock.Rows, runID, status string, at time.Time) *sqlmock.Rows {
	return rows.AddRow(runID, savedQueryID, "", "manual", status, "read", "", nil, "", "",
		at.Add(-time.Second), at, "", "", "", "", "", "", 0, 0, "")
}

// A full page asks for one row more than it returns, and hands back the last returned
// row as the cursor; the next request pages strictly below that row.
func TestListSavedQueryRuns_PagesByKeysetCursor(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	expectVisibleRunsModel(mock)

	t1 := time.Date(2026, 9, 16, 10, 0, 0, 0, time.UTC)
	rows := sqlmock.NewRows(runHistoryColumns)
	addRun(rows, provRunB, "failed", t1)
	addRun(rows, provRunA, "failed", t1.Add(-time.Hour))
	addRun(rows, provUpstream, "failed", t1.Add(-2*time.Hour)) // the probe row
	mock.ExpectQuery(`FROM saved_query_runs r[\s\S]*\(\$3 = '' OR r.status = \$3\)[\s\S]*\(r.finished_at, r.run_id\) < \(\$4::timestamptz, \$5::uuid\)[\s\S]*ORDER BY r.finished_at DESC, r.run_id DESC\s+LIMIT \$6`).
		WithArgs(savedQueryID, wsScopeUser, "failed", nil, nil, 3).
		WillReturnRows(rows)

	r := savedQueryRouter(http.MethodGet, "/explorer/saved/:id/runs", "viewer", ListSavedQueryRuns)
	w := doJSON(r, http.MethodGet, "/explorer/saved/"+savedQueryID+"/runs?limit=2&status=failed", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var body struct {
		Runs       []SavedQueryRun `json:"runs"`
		NextCursor string          `json:"next_cursor"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Runs) != 2 || body.Runs[1].RunID != provRunA {
		t.Fatalf("runs = %+v, want the two newest", body.Runs)
	}
	at, runID, err := decodeRunCursor(body.NextCursor)
	if err != nil || !at.Equal(t1.Add(-time.Hour)) || runID != provRunA {
		t.Fatalf("next_cursor = %q (%v, %q, %v), want the last returned row", body.NextCursor, at, runID, err)
	}

	// The cursor feeds the next page's bounds, and a short page carries no cursor.
	expectVisibleRunsModel(mock)
	last := sqlmock.NewRows(runHistoryColumns)
	addRun(last, provUpstream, "failed", t1.Add(-2*time.Hour))
	mock.ExpectQuery(`FROM saved_query_runs r`).
		WithArgs(savedQueryID, wsScopeUser, "failed", at, provRunA, 3).
		WillReturnRows(last)
	w = doJSON(r, http.MethodGet, "/explorer/saved/"+savedQueryID+"/runs?limit=2&status=failed&before="+body.NextCursor, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var second map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &second)
	if _, has := second["next_cursor"]; has || second["count"] != float64(1) {
		t.Fatalf("last page = %s", w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// Bad paging input is refused before any query runs, rather than silently widened.
func TestListSavedQueryRuns_RefusesBadPagingInput(t *testing.T) {
	for _, q := range []string{"limit=0", "limit=201", "limit=ten", "status=running", "before=yesterday",
		"before=2026-09-16T10:00:00Z_not-a-uuid"} {
		t.Run(q, func(t *testing.T) {
			mock, cleanup := wsScopeMockDB(t)
			defer cleanup()
			r := savedQueryRouter(http.MethodGet, "/explorer/saved/:id/runs", "viewer", ListSavedQueryRuns)
			w := doJSON(r, http.MethodGet, "/explorer/saved/"+savedQueryID+"/runs?"+q, nil)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// The cursor survives a round trip at microsecond precision, which is what Postgres
// stores; a truncated time would re-serve or skip the boundary row.
func TestRunCursor_RoundTripsMicroseconds(t *testing.T) {
	at := time.Date(2026, 9, 16, 10, 0, 0, 123456000, time.FixedZone("CEST", 7200))
	got, id, err := decodeRunCursor(encodeRunCursor(at, provRunA))
	if err != nil || !got.Equal(at) || id != provRunA {
		t.Fatalf("round trip = %v %q %v", got, id, err)
	}
}

// ?saved_query_id= narrows the same visibility-checked list to one model.
func TestListSavedQuerySchedules_FiltersToOneModel(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.ExpectQuery(`FROM saved_query_schedules s[\s\S]+\(sq\.visibility = 'workspace' OR sq\.created_by = \$2\)[\s\S]+AND s\.saved_query_id = \$3`).
		WithArgs(wsScopeWS, wsScopeUser, savedQueryID).
		WillReturnRows(sqlmock.NewRows(scheduledSummaryColumns))
	mock.ExpectQuery(`FROM saved_query_schedule_upstreams u`).
		WithArgs(wsScopeWS, wsScopeUser).
		WillReturnRows(sqlmock.NewRows([]string{"schedule_id", "upstream_kind", "upstream_id", "name"}))

	r := savedQueryRouter(http.MethodGet, "/explorer/schedules", "viewer", ListSavedQuerySchedules)
	if w := doJSON(r, http.MethodGet, "/explorer/schedules?saved_query_id="+savedQueryID, nil); w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if w := doJSON(r, http.MethodGet, "/explorer/schedules?saved_query_id=nope", nil); w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for a bad id, got %d", w.Code)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
