package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
)

// RBAC P1 (B1): an audited action happens INSIDE a workspace, but logAudit
// never recorded which one — so per-workspace audit trails / attribution were
// impossible. logAudit now stamps the caller's ACTIVE workspace into the new
// audit_logs.workspace_id column (migration 070). Auth-only routes that run
// before any workspace context legitimately write NULL.
//
// wsScopeUser / wsScopeWS / wsScopePipeline / wsScopeMockDB come from sibling
// *_test.go files (same package).

func TestLogAudit_StampsActiveWorkspace(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	// Post-fix: the INSERT carries workspace_id ($7) = the active workspace.
	mock.ExpectExec(`INSERT INTO audit_logs`).
		WithArgs(wsScopeUser, "pipeline.create", "pipeline", wsScopePipeline,
			sqlmock.AnyArg(), sqlmock.AnyArg(), wsScopeWS).
		WillReturnResult(sqlmock.NewResult(1, 1))

	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/", nil)
	c.Set("user_id", wsScopeUser)
	c.Set(ctxWorkspaceID, wsScopeWS)

	logAudit(c, "pipeline.create", "pipeline", wsScopePipeline, nil)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("audit insert must include the active workspace_id: %v", err)
	}
}

func TestLogAudit_NullWorkspaceWhenNoContext(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	// Auth-only path: no active workspace set → workspace_id bound as NULL, but
	// the row is still written (best-effort audit must not break).
	mock.ExpectExec(`INSERT INTO audit_logs`).
		WithArgs(wsScopeUser, "user.login", "user", wsScopeUser,
			sqlmock.AnyArg(), sqlmock.AnyArg(), nil).
		WillReturnResult(sqlmock.NewResult(1, 1))

	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/", nil)
	c.Set("user_id", wsScopeUser) // no ctxWorkspaceID

	logAudit(c, "user.login", "user", wsScopeUser, nil)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("audit insert must bind NULL workspace_id when no active workspace: %v", err)
	}
}
