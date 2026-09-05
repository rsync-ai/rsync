package handlers

import (
	"database/sql"
	"errors"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"api-gateway/internal/db"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
)

// A valid-but-nonexistent pipeline id used to return 500 namespace_lock_failed with
// the raw driver string "sql: no rows in result set" in the response body. Two things
// were wrong with that, and each is pinned separately below so a fix to one cannot
// quietly regress the other:
//
//   - the STATUS. RunPipelineInternal (pipelines.go:3309) runs the identical
//     `SELECT ... FROM pipelines WHERE id = $1` on the identical table and answers
//     404. Two internal routes disagreeing about what a missing pipeline is makes the
//     status code useless for deciding whether to retry.
//   - the BODY. `err.Error()` was echoed straight through, so whatever the driver
//     happened to say became part of this endpoint's contract.
//
// The negative control matters as much as the positive one: a handler that answered
// 404 for every error would satisfy the first test and be strictly worse than the bug,
// because the executor's fail-soft path would then treat a dead database as a
// well-formed "no such pipeline".

const nsLockWorkspaceQuery = `SELECT workspace_id::text FROM pipelines WHERE id = $1::uuid`

// nsLockRequest drives the real route with one mocked answer to the workspace lookup.
func nsLockRequest(t *testing.T, scanErr error) *httptest.ResponseRecorder {
	t.Helper()

	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	prev := db.DB
	db.DB = mockDB
	t.Cleanup(func() {
		db.DB = prev
		_ = mockDB.Close()
	})

	mock.ExpectQuery(regexp.QuoteMeta(nsLockWorkspaceQuery)).WillReturnError(scanErr)

	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/api/v1/internal/pipelines/:id/namespace/lock", LockPipelineNamespaceInternal)

	w := httptest.NewRecorder()
	req := httptest.NewRequest(
		"POST",
		"/api/v1/internal/pipelines/12c3579c-52bc-47f2-96ae-10719e4e943c/namespace/lock",
		strings.NewReader(`{"selected_tables":["demo_src.demo_customers"]}`),
	)
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("the workspace lookup never ran, so this test proves nothing about it: %v", err)
	}
	return w
}

func TestLockPipelineNamespaceInternal_UnknownPipelineIs404(t *testing.T) {
	w := nsLockRequest(t, sql.ErrNoRows)

	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want %d for a well-formed id with no pipeline row", w.Code, http.StatusNotFound)
	}
	body := w.Body.String()
	if !strings.Contains(body, `"pipeline_not_found"`) {
		t.Errorf("body = %s, want error code pipeline_not_found", body)
	}
	if strings.Contains(body, sql.ErrNoRows.Error()) {
		t.Errorf("body = %s, still leaks the driver string %q", body, sql.ErrNoRows.Error())
	}
}

func TestLockPipelineNamespaceInternal_RealFailureStays500AndKeepsItsDetailInTheLog(t *testing.T) {
	// Negative control. Without this, "return 404 unconditionally" passes the test
	// above while telling the executor a broken database is a missing pipeline.
	driverErr := errors.New("pq: could not connect to server: connection refused")
	w := nsLockRequest(t, driverErr)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want %d for an error that is not sql.ErrNoRows", w.Code, http.StatusInternalServerError)
	}
	body := w.Body.String()
	if !strings.Contains(body, `"namespace_lock_failed"`) {
		t.Errorf("body = %s, want error code namespace_lock_failed", body)
	}
	if strings.Contains(body, driverErr.Error()) {
		t.Errorf("body = %s, echoes the driver error verbatim", body)
	}
}

func TestLockNamespaceForRun_ClassifiesMissingPipeline(t *testing.T) {
	// The sentinel itself, pinned below the HTTP layer: the integration test drives
	// lockNamespaceForRun directly and needs the classification to hold there too.
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = mockDB.Close() }()

	mock.ExpectQuery(regexp.QuoteMeta(nsLockWorkspaceQuery)).WillReturnError(sql.ErrNoRows)

	_, err = lockNamespaceForRun(t.Context(), mockDB,
		"12c3579c-52bc-47f2-96ae-10719e4e943c", []string{"demo_src.demo_customers"})
	if err != errNamespaceLockNotFound {
		t.Errorf("err = %v, want errNamespaceLockNotFound", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("workspace lookup never ran: %v", err)
	}
}
