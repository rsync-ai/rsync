package handlers

// GET /pipelines/:id/cdc/snapshot-requests is what the UI polls between "load
// requested" and rows arriving (migration 113). It is read-only, so a VIEWER may
// ask; a pipeline outside the caller's active workspace answers 404 with no data
// query behind it; a pipeline with no requests answers an empty list, not 404.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

const snapshotRequestsQuery = `FROM cdc_snapshot_requests`

var snapshotRequestCols = []string{
	"id", "mode", "tables", "source", "status", "attempts",
	"completed_tables", "last_error",
	"requested_at", "sent_at", "started_at", "last_progress_at", "completed_at",
}

func serveSnapshotRequests(t *testing.T, role string) *httptest.ResponseRecorder {
	t.Helper()
	r := wsScopeRouterAsRole(http.MethodGet, "/p/:id/cdc/snapshot-requests", GetPipelineCDCSnapshotRequests, role)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/p/"+wsScopePipeline+"/cdc/snapshot-requests", nil))
	return w
}

func TestGetPipelineCDCSnapshotRequests_ViewerReadsNewestFirst(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.ExpectQuery(`SELECT wm\.role\s+FROM pipelines r`).
		WithArgs(wsScopePipeline, wsScopeUser, wsScopeWS).
		WillReturnRows(gateRoleRows("viewer"))

	requested := time.Date(2026, 9, 24, 10, 0, 0, 0, time.FixedZone("IST", 5*3600+1800))
	sent := time.Date(2026, 9, 24, 4, 31, 0, 0, time.UTC)
	mock.ExpectQuery(regexp.QuoteMeta(snapshotRequestsQuery)+`[\s\S]*ORDER BY requested_at DESC[\s\S]*LIMIT \$2`).
		WithArgs(wsScopePipeline, cdcSnapshotRequestsLimit).
		WillReturnRows(sqlmock.NewRows(snapshotRequestCols).
			// Newest: still running, one of two tables done.
			AddRow("a0000000-0000-4000-8000-000000000002", "incremental", `["public.orders","public.users"]`,
				"table_edit", "started", 1, `["public.orders"]`, nil,
				requested, sent, sent, sent, nil).
			// Older: never sent, with the reason.
			AddRow("a0000000-0000-4000-8000-000000000001", "blocking", `["public.users"]`,
				"resnapshot", "failed", 0, `[]`, "connector missing",
				requested.Add(-time.Hour), nil, nil, nil, nil))

	w := serveSnapshotRequests(t, "viewer")
	if w.Code != http.StatusOK {
		t.Fatalf("a viewer must be able to read snapshot requests; got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}

	var body struct {
		Requests []map[string]any `json:"requests"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(body.Requests) != 2 {
		t.Fatalf("got %d requests, want 2: %s", len(body.Requests), w.Body.String())
	}
	first, second := body.Requests[0], body.Requests[1]
	if first["id"] != "a0000000-0000-4000-8000-000000000002" || first["status"] != "started" || first["source"] != "table_edit" {
		t.Fatalf("first request = %v; want the newest (started, table_edit) first", first)
	}
	// RFC3339 in UTC, whatever zone the driver handed back.
	if first["requested_at"] != "2026-09-24T04:30:00Z" || first["sent_at"] != "2026-09-24T04:31:00Z" {
		t.Fatalf("timestamps = %v / %v; want RFC3339 UTC", first["requested_at"], first["sent_at"])
	}
	if done, _ := first["completed_tables"].([]any); len(done) != 1 || done[0] != "public.orders" {
		t.Fatalf("completed_tables = %#v; want [public.orders]", first["completed_tables"])
	}
	if _, has := first["completed_at"]; has {
		t.Fatalf("a NULL completed_at must be omitted, got %v", first["completed_at"])
	}
	if _, has := first["last_error"]; has {
		t.Fatalf("a NULL last_error must be omitted, got %v", first["last_error"])
	}
	if second["last_error"] != "connector missing" || second["attempts"] != float64(0) {
		t.Fatalf("second request = %v; want last_error and attempts 0", second)
	}
	for _, k := range []string{"sent_at", "started_at", "last_progress_at", "completed_at"} {
		if _, has := second[k]; has {
			t.Fatalf("a NULL %s must be omitted, got %v", k, second[k])
		}
	}
	// An empty JSONB array stays a JSON array, never null.
	if done, ok := second["completed_tables"].([]any); !ok || len(done) != 0 {
		t.Fatalf("completed_tables = %#v; want []", second["completed_tables"])
	}
}

func TestGetPipelineCDCSnapshotRequests_NoneIsAnEmptyList(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.ExpectQuery(`SELECT wm\.role\s+FROM pipelines r`).
		WithArgs(wsScopePipeline, wsScopeUser, wsScopeWS).
		WillReturnRows(gateRoleRows("viewer"))
	mock.ExpectQuery(regexp.QuoteMeta(snapshotRequestsQuery)).
		WillReturnRows(sqlmock.NewRows(snapshotRequestCols))

	w := serveSnapshotRequests(t, "viewer")
	if w.Code != http.StatusOK {
		t.Fatalf("no requests must be 200, got %d: %s", w.Code, w.Body.String())
	}
	if strings.TrimSpace(w.Body.String()) != `{"requests":[]}` {
		t.Fatalf("body = %s; want {\"requests\":[]}", w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

// Tenant gate: the role query binds the ACTIVE workspace, so a pipeline outside it
// resolves no row and the handler answers 404 — and never reaches the data query
// (sqlmock would answer an unexpected query with an error, i.e. a 500, not a 404).
func TestGetPipelineCDCSnapshotRequests_ForeignWorkspace404(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.ExpectQuery(`SELECT wm\.role\s+FROM pipelines r`).
		WithArgs(wsScopePipeline, wsScopeUser, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"role"}))

	w := serveSnapshotRequests(t, "owner")
	if w.Code != http.StatusNotFound {
		t.Fatalf("a pipeline outside the active workspace must be 404, got %d: %s", w.Code, w.Body.String())
	}
	if strings.Contains(w.Body.String(), "requests") {
		t.Fatalf("the 404 leaked data: %s", w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("the gate query never ran, so the 404 proves nothing: %v", err)
	}
}

func TestGetPipelineCDCSnapshotRequests_BadIDIsRejected(t *testing.T) {
	_, cleanup := wsScopeMockDB(t)
	defer cleanup()
	r := wsScopeRouterAsRole(http.MethodGet, "/p/:id/cdc/snapshot-requests", GetPipelineCDCSnapshotRequests, "viewer")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/p/not-a-uuid/cdc/snapshot-requests", nil))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("a non-UUID id must be 400, got %d: %s", w.Code, w.Body.String())
	}
}
