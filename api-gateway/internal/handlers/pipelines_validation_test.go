package handlers

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"api-gateway/internal/db"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
)

func TestUpdatePipeline_InvalidJSON_Returns400(t *testing.T) {
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

	const pid = "550e8400-e29b-41d4-a716-446655440000"
	const wsID = "660e8400-e29b-41d4-a716-446655440000"
	// The mutation gate (requireResourceRole) must succeed so we reach the
	// JSON-bind branch we're trying to test. It needs the active workspace on
	// the context plus a row proving the caller's role in that workspace.
	// user_id + workspace_id are set on the gin context via a wrapper below.
	mock.ExpectQuery(`SELECT wm\.role\s+FROM pipelines r`).
		WithArgs(pid, "user-A", wsID).
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("member"))

	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.PATCH("/api/v1/pipelines/:id", func(c *gin.Context) {
		c.Set("user_id", "user-A")
		c.Set(ctxWorkspaceID, wsID)
		UpdatePipeline(c)
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest("PATCH", "/api/v1/pipelines/"+pid, strings.NewReader("{not-json"))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected %d, got %d", http.StatusBadRequest, w.Code)
	}
}

