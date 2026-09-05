package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"api-gateway/internal/db"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
)

func TestAdminListAuditLogs_OffsetTooLarge_Returns400(t *testing.T) {
	mockDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	prev := db.DB
	db.DB = mockDB
	defer func() {
		db.DB = prev
		_ = mockDB.Close()
	}()

	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/api/v1/admin/audit-logs", AdminListAuditLogs)

	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/v1/admin/audit-logs?offset=999999999", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected %d, got %d", http.StatusBadRequest, w.Code)
	}
}

