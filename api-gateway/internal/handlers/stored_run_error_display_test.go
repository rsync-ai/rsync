package handlers

import (
	"database/sql/driver"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// #51 prod retest: the write-side scrub only cleans rows written after it shipped. These
// are the shapes a row written before it still holds, read back through every route that
// returns stored run error text. 203.0.113.7 stands in for the source server's address.
const (
	storedRawRunError = "failed to connect to `user=app database=orders`: 203.0.113.7:5432 (203.0.113.7): dial error: dial tcp 203.0.113.7:5432: connect: connection timed out"
	storedCleanError  = "could not connect to the database: connect: connection timed out"
)

func assertNoSourceAddress(t *testing.T, where, body string) {
	t.Helper()
	if strings.Contains(body, "203.0.113.7") || strings.Contains(body, ":5432") {
		t.Fatalf("%s still names the source server: %s", where, body)
	}
	if !strings.Contains(body, storedCleanError) {
		t.Fatalf("%s lost the reason along with the address: %s", where, body)
	}
}

func TestStoredRunErrorForDisplay_ScrubsOldRowsAndLeavesCleanOnesAlone(t *testing.T) {
	if got := storedRunErrorForDisplay(storedRawRunError); got != storedCleanError {
		t.Fatalf("old row: got %q, want %q", got, storedCleanError)
	}
	// A row written after the write-side scrub is read back unchanged.
	for _, clean := range []string{storedCleanError, `ERROR: relation "public.orders" does not exist (SQLSTATE 42P01)`} {
		if got := storedRunErrorForDisplay(clean); got != clean {
			t.Fatalf("clean row changed on read: %q -> %q", clean, got)
		}
	}
	for _, empty := range []string{"", "   "} {
		if got := storedRunErrorForDisplay(empty); got != "" {
			t.Fatalf("empty row must stay empty, got %q", got)
		}
	}
}

func TestListSavedQueryRuns_ScrubsAnErrorStoredBeforeTheScrub(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	expectVisibleRunsModel(mock)

	at := time.Date(2026, 9, 18, 4, 30, 0, 0, time.UTC)
	rows := sqlmock.NewRows(runHistoryColumns).AddRow(provRunA, savedQueryID, "", "schedule", "failed", "read", "", nil,
		storedRawRunError, "", at.Add(-time.Second), at, "", "", "", "", "", "", 0, 0, "")
	mock.ExpectQuery(`FROM saved_query_runs r`).WillReturnRows(rows)

	r := savedQueryRouter(http.MethodGet, "/explorer/saved/:id/runs", "viewer", ListSavedQueryRuns)
	w := doJSON(r, http.MethodGet, "/explorer/saved/"+savedQueryID+"/runs", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	assertNoSourceAddress(t, "run history", w.Body.String())
}

func TestListSavedQuerySchedules_ScrubsAStoredLastRunError(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	row := []driver.Value{"b2c3d4e5-1111-2222-3333-000000000001", savedQueryID, "orders_daily", "",
		savedQueryConn, "warehouse", "postgresql",
		"interval", []byte(`{"interval_seconds":3600}`), "active",
		"table", "public.orders_daily", "read",
		time.Now(), "failed", storedRawRunError,
		wsScopeUser, time.Now(), time.Now(),
		nil, "",
		nil, "",
		upstreamPolicyAll}
	mock.ExpectQuery(`FROM saved_query_schedules s`).
		WillReturnRows(sqlmock.NewRows(scheduledSummaryColumns).AddRow(row...))
	mock.ExpectQuery(`FROM saved_query_schedule_upstreams u`).
		WillReturnRows(sqlmock.NewRows([]string{"schedule_id", "upstream_kind", "upstream_id", "name"}))

	r := savedQueryRouter(http.MethodGet, "/explorer/schedules", "viewer", ListSavedQuerySchedules)
	w := doJSON(r, http.MethodGet, "/explorer/schedules", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	assertNoSourceAddress(t, "schedule list", w.Body.String())
}

// savedQueryRowsWithLastRunError is the savedQueryRows fixture with a failed last run.
func savedQueryRowsWithLastRunError(lastRunError string) *sqlmock.Rows {
	return sqlmock.NewRows(savedQueryScanColumns).
		AddRow(savedQueryID, wsScopeWS, savedQueryConn, "Daily MRR", "",
			"SELECT 1", "", "read", "workspace",
			wsScopeUser, wsScopeUser, "2026-08-13T00:00:00Z", savedQueryFixtureUpdatedAt, nil,
			"table", "public.daily_mrr", "failed", lastRunError,
			"", "", nil, "postgresql", nil)
}

func TestListSavedQueries_ScrubsAStoredLastRunError(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.ExpectQuery(`FROM saved_queries sq[\s\S]+WHERE sq\.workspace_id = \$1`).
		WillReturnRows(savedQueryRowsWithLastRunError(storedRawRunError))

	r := savedQueryRouter(http.MethodGet, "/explorer/saved", "viewer", ListSavedQueries)
	w := doJSON(r, http.MethodGet, "/explorer/saved", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	assertNoSourceAddress(t, "model list", w.Body.String())
}

func TestGetSavedQuery_ScrubsAStoredLastRunError(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.ExpectQuery(`FROM saved_queries r\s+JOIN workspace_members`).
		WithArgs(savedQueryID, wsScopeUser, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("viewer"))
	mock.ExpectQuery(`FROM saved_queries sq[\s\S]+WHERE sq\.id = \$1`).
		WithArgs(savedQueryID).
		WillReturnRows(savedQueryRowsWithLastRunError(storedRawRunError))

	r := savedQueryRouter(http.MethodGet, "/explorer/saved/:id", "viewer", GetSavedQuery)
	w := doJSON(r, http.MethodGet, "/explorer/saved/"+savedQueryID, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	assertNoSourceAddress(t, "model detail", w.Body.String())
}
