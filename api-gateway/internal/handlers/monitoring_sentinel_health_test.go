package handlers

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"api-gateway/internal/config"
	"api-gateway/internal/db"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
)

// TestMain loads the monitoring feature flags once for the whole handlers package.
//
// config.LoadFeatures() is guarded by a sync.Once, so the first caller inside a test binary
// fixes the flags for every test that follows. Before this, exactly one test called it
// (pipeline_read_gate_workspace_test.go) and worked only because nothing else did — a
// second caller anywhere in the package would silently lose the race, and the loser would
// assert against flags it believed it had set. Loading here, before any test runs, makes
// the order irrelevant; both flags are enabled because both are gates that answer 404 when
// off, which would turn a real assertion into a pass for the wrong reason.
func TestMain(m *testing.M) {
	os.Setenv("FEATURE_MONITORING_INFRA", "true")
	os.Setenv("FEATURE_MONITORING_OVERVIEW", "true")
	config.LoadFeatures()
	os.Exit(m.Run())
}

func sentinelHealthColumns() []string {
	return []string{
		"component_id", "component_type", "status", "last_heartbeat",
		"messages_processed", "error_count", "consumer_lag", "last_error",
		"metadata", "updated_at",
	}
}

func serveSentinelHealth(t *testing.T, mockDB *sqlmock.Sqlmock) *httptest.ResponseRecorder {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/api/v1/monitoring/sentinel/health", GetSentinelHealth)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/api/v1/monitoring/sentinel/health", nil)
	r.ServeHTTP(w, req)
	return w
}

func withMockDB(t *testing.T) (sqlmock.Sqlmock, func()) {
	t.Helper()
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	prev := db.DB
	db.DB = mockDB
	return mock, func() {
		db.DB = prev
		_ = mockDB.Close()
	}
}

// A row that will not scan is a broken read, not a component to leave out. Skipping it
// produced the worst available answer: a 200 whose total agreed with the truncated list,
// so nothing in the response disclosed that anything had been dropped.
func TestSentinelHealthFailsLoudlyWhenARowWillNotScan(t *testing.T) {
	mock, cleanup := withMockDB(t)
	defer cleanup()

	// "not-a-timestamp" cannot scan into the LastHeartbeat time.Time.
	rows := sqlmock.NewRows(sentinelHealthColumns()).
		AddRow("infrastructure:kafka", "infrastructure", "healthy", "not-a-timestamp",
			int64(0), int64(0), int64(0), nil, []byte("{}"), time.Now())
	mock.ExpectQuery(`FROM sentinel_component_health`).WillReturnRows(rows)

	w := serveSentinelHealth(t, &mock)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want %d — an unreadable row was reported as a healthy fleet", w.Code, http.StatusInternalServerError)
	}
}

// rows.Next() returning false does not distinguish "finished" from "failed": a connection
// dropped mid-iteration ends the loop exactly like a complete read. Unchecked, a database
// problem rendered as an empty-but-200 list of zero unhealthy components — the most
// reassuring possible presentation of an outage.
func TestSentinelHealthFailsLoudlyWhenIterationBreaksMidway(t *testing.T) {
	mock, cleanup := withMockDB(t)
	defer cleanup()

	rows := sqlmock.NewRows(sentinelHealthColumns()).
		AddRow("infrastructure:kafka", "infrastructure", "healthy", time.Now(),
			int64(0), int64(0), int64(0), nil, []byte("{}"), time.Now()).
		AddRow("infrastructure:postgres", "infrastructure", "healthy", time.Now(),
			int64(0), int64(0), int64(0), nil, []byte("{}"), time.Now()).
		RowError(1, errors.New("connection reset by peer"))
	mock.ExpectQuery(`FROM sentinel_component_health`).WillReturnRows(rows)

	w := serveSentinelHealth(t, &mock)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want %d — a read that died halfway was reported as complete", w.Code, http.StatusInternalServerError)
	}
}

// The control. A fix that answered 500 more often would pass both tests above while
// destroying the endpoint; this asserts the healthy path still returns every row, and that
// total counts the whole result set rather than whatever survived scanning.
func TestSentinelHealthReturnsEveryRowAndCountsThemAllWhenTheReadSucceeds(t *testing.T) {
	mock, cleanup := withMockDB(t)
	defer cleanup()

	rows := sqlmock.NewRows(sentinelHealthColumns())
	for _, id := range []string{"infrastructure:kafka", "infrastructure:postgres", "agent:executor"} {
		rows = rows.AddRow(id, "infrastructure", "healthy", time.Now(),
			int64(0), int64(0), int64(0), nil, []byte(`{"url":"x"}`), time.Now())
	}
	mock.ExpectQuery(`FROM sentinel_component_health`).WillReturnRows(rows)

	w := serveSentinelHealth(t, &mock)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %s", w.Code, w.Body.String())
	}

	var got PaginatedHealthResponse
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(got.Components) != 3 {
		t.Errorf("components = %d, want 3", len(got.Components))
	}
	if got.Total != 3 {
		t.Errorf("total = %d, want 3 — total must count the result set, not the survivors", got.Total)
	}
	if got.HasMore {
		t.Error("has_more = true, but the query is unpaginated and returned everything")
	}
}

// Metadata that is valid JSON but not an object costs that one component its metadata; it
// must not cost the caller the status column, and it must not pass silently the way the
// dropped Unmarshal error did.
func TestSentinelHealthSurvivesUnusableMetadataWithoutLosingTheComponent(t *testing.T) {
	mock, cleanup := withMockDB(t)
	defer cleanup()

	rows := sqlmock.NewRows(sentinelHealthColumns()).
		AddRow("infrastructure:kafka", "infrastructure", "unhealthy", time.Now(),
			int64(0), int64(1), int64(0), "no brokers available", []byte(`["not","an","object"]`), time.Now())
	mock.ExpectQuery(`FROM sentinel_component_health`).WillReturnRows(rows)

	w := serveSentinelHealth(t, &mock)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 — a bad metadata blob is not a broken read; body = %s", w.Code, w.Body.String())
	}
	var got PaginatedHealthResponse
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(got.Components) != 1 || got.Components[0].Status != "unhealthy" {
		t.Errorf("want the component reported unhealthy despite its metadata; got %+v", got.Components)
	}
}
