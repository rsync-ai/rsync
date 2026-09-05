package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"api-gateway/internal/db"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
)

// GetConnection accepts either a UUID or a human-friendly name slug as :id
// (BUG-1). A non-UUID is no longer rejected up front — it is resolved as a
// workspace-scoped (P2c), case-insensitive name slug. This test pins the
// contract: a non-UUID that matches no row in the active workspace falls
// through to a clean 404 (not 400/500), proving the slug fallback reaches the
// DB and fails closed.
func TestGetConnection_NonUUIDSlug_NotFoundReturns404(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	prev := db.DB
	db.DB = mockDB
	defer func() {
		db.DB = prev
		_ = mockDB.Close()
	}()

	const userID = "00000000-0000-0000-0000-000000000000"
	const wsID = "00000000-0000-0000-0000-0000000000ff"

	// The slug branch queries by name scoped to the active workspace ($2 = wsID,
	// never user_id); return no rows so the handler must produce a 404. A loose
	// regexp keeps the test resilient to whitespace.
	mock.ExpectQuery("SELECT .* FROM connections").
		WithArgs("not-a-uuid", wsID).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "user_id", "name", "type", "connector_type", "connector_version",
			"sync_mode", "cdc_mode", "description", "config", "status",
			"last_tested_at", "last_test_status", "last_test_error", "created_at", "updated_at",
		}))

	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(func(c *gin.Context) {
		c.Set("user_id", userID)
		c.Set(ctxWorkspaceID, wsID)
		c.Next()
	})
	r.GET("/api/v1/connections/:id", GetConnection)

	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/v1/connections/not-a-uuid", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected %d, got %d (body=%s)", http.StatusNotFound, w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

// Unauthenticated requests must fail closed before any DB access.
func TestGetConnection_Unauthenticated_Returns401(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/api/v1/connections/:id", GetConnection)

	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/v1/connections/not-a-uuid", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected %d, got %d", http.StatusUnauthorized, w.Code)
	}
}
