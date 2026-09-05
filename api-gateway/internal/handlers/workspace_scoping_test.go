package handlers

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"api-gateway/internal/db"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
)

// P1: resource CREATE/LIST must be scoped to the ACTIVE WORKSPACE (the single
// company), not to the calling user. Members of a workspace share connections
// and pipelines, so:
//   - CREATE stamps connections.workspace_id / pipelines.workspace_id with the
//     active workspace (migration 069 made these columns NOT NULL).
//   - LIST filters by the active workspace, NOT by user_id / created_by.
//
// The active workspace is distinct from the caller's user_id in these tests so a
// handler that still scopes by user_id fails loudly.

const (
	wsScopeUser = "11111111-1111-1111-1111-111111111111"
	wsScopeWS   = "22222222-2222-2222-2222-222222222222"
)

// wsScopeRouter pins user_id + the workspace context keys (as the auth +
// WorkspaceContextMiddleware would) before invoking the handler under test.
func wsScopeRouter(method, path string, h gin.HandlerFunc) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(func(c *gin.Context) {
		c.Set("user_id", wsScopeUser)
		c.Set(ctxWorkspaceID, wsScopeWS)
		c.Set(ctxWorkspaceRole, "owner")
		c.Next()
	})
	r.Handle(method, path, h)
	return r
}

func wsScopeMockDB(t *testing.T) (sqlmock.Sqlmock, func()) {
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

func TestListConnections_ScopedToActiveWorkspace(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	// MUST filter by the active workspace, bound as $1 — never by user_id.
	mock.ExpectQuery(`FROM connections\s+WHERE workspace_id = \$1`).
		WithArgs(wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "user_id", "name", "type", "connector_type", "connector_version",
			"sync_mode", "cdc_mode", "description", "config", "status",
			"last_tested_at", "last_test_status", "last_test_error", "created_at", "updated_at",
		}))

	r := wsScopeRouter(http.MethodGet, "/api/v1/connections", ListConnections)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/connections", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

func TestListPipelines_ScopedToActiveWorkspace(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	cols := []string{
		"id", "name", "description", "pipeline_status", "created_at", "updated_at",
		"created_by", "sync_mode", "cdc_mode", "source_connection", "destination_connection",
		"schedule", "last_execution", "derived_status",
	}
	// The page query and the COUNT query both scope by p.workspace_id = $1; they
	// run in either order, so match order-agnostically with distinct anchors. The
	// $1 = active workspace binding is what proves the scoping (old code bound the
	// caller's user_id / created_by, which differs).
	mock.MatchExpectationsInOrder(false)
	mock.ExpectQuery(`LIMIT \$7 OFFSET \$8`).
		WithArgs(wsScopeWS, sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows(cols))
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM base`).
		WithArgs(wsScopeWS, sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	r := wsScopeRouter(http.MethodGet, "/api/v1/pipelines", ListPipelines)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/pipelines", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

func TestCreateConnection_StampsActiveWorkspace(t *testing.T) {
	// crypto.EncryptString fatals outside dev unless a key is configured.
	t.Setenv("ENVIRONMENT", "development")
	t.Setenv("ENCRYPTION_KEY", "0123456789abcdef0123456789abcdef")

	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	// The INSERT must carry workspace_id = active workspace as a new bound arg.
	// user_id stays the caller; workspace_id is asserted distinct from it.
	mock.ExpectExec(`INSERT INTO connections`).
		WithArgs(
			sqlmock.AnyArg(), // id
			wsScopeUser,      // user_id
			sqlmock.AnyArg(), // name
			sqlmock.AnyArg(), // type
			sqlmock.AnyArg(), // connector_type
			sqlmock.AnyArg(), // connector_version
			sqlmock.AnyArg(), // sync_mode
			sqlmock.AnyArg(), // cdc_mode
			sqlmock.AnyArg(), // description
			sqlmock.AnyArg(), // config
			sqlmock.AnyArg(), // status
			sqlmock.AnyArg(), // last_tested_at
			sqlmock.AnyArg(), // last_test_status
			sqlmock.AnyArg(), // created_at
			sqlmock.AnyArg(), // updated_at
			wsScopeWS,        // workspace_id  <-- P1 assertion
		).
		WillReturnResult(sqlmock.NewResult(0, 1))

	body, _ := json.Marshal(map[string]any{
		"name":            "pg-src",
		"connection_type": "source",
		"connector_type":  "postgresql",
		"force_save":      true, // skip the pre-save connectivity test
		"config":          map[string]any{"host": "localhost"},
	})
	r := wsScopeRouter(http.MethodPost, "/api/v1/connections", CreateConnection)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/connections", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

func TestCreatePipeline_StampsActiveWorkspace(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)

	// Plan gate fails open when the plans catalogue is unavailable → no further
	// quota queries. Lets the handler reach the INSERT with no source/dest.
	mock.ExpectQuery(`FROM plans`).WillReturnError(os.ErrClosed)

	// The INSERT must carry workspace_id = active workspace; created_by stays the
	// caller and is asserted distinct from workspace_id.
	mock.ExpectExec(`INSERT INTO pipelines`).
		WithArgs(
			sqlmock.AnyArg(), // id
			sqlmock.AnyArg(), // name
			sqlmock.AnyArg(), // description
			sqlmock.AnyArg(), // natural_language_request
			sqlmock.AnyArg(), // status
			sqlmock.AnyArg(), // created_at
			sqlmock.AnyArg(), // updated_at
			wsScopeUser,      // created_by
			sqlmock.AnyArg(), // source_connection_id
			sqlmock.AnyArg(), // destination_connection_id
			sqlmock.AnyArg(), // sync_mode
			sqlmock.AnyArg(), // cdc_mode
			sqlmock.AnyArg(), // sync_mode_source
			sqlmock.AnyArg(), // dataset
			sqlmock.AnyArg(), // default_run_mode
			sqlmock.AnyArg(), // config
			wsScopeWS,        // workspace_id  <-- P1 assertion
			sqlmock.AnyArg(), // cdc_initial_load
		).
		WillReturnResult(sqlmock.NewResult(0, 1))

	body, _ := json.Marshal(map[string]any{
		"name":    "p1",
		"request": "copy data",
	})
	r := wsScopeRouter(http.MethodPost, "/api/v1/pipelines", CreatePipeline)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/pipelines", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusCreated && w.Code != http.StatusOK {
		t.Fatalf("expected 2xx, got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}
