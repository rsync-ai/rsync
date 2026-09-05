package handlers

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"api-gateway/internal/db"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
)

// P0: an interim manual-upgrade path (until Stripe/P2 becomes the authoritative
// plan writer). Setting a workspace's billing plan is a PLATFORM-admin action —
// the route lives under the AdminRoleMiddleware group (users.role='admin'), NOT
// a workspace-role gate, because a workspace admin who could set their own plan
// to 'pro' would be a free self-upgrade. These tests exercise the handler's
// own logic (catalogue validation, update, audit); the platform-admin gate is
// covered by admin_middleware_test.go.

const adminTargetWS = "33333333-3333-3333-3333-333333333333"

func adminPlanRouter(h gin.HandlerFunc) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(func(c *gin.Context) {
		c.Set("user_id", wsScopeUser) // the acting platform admin
		c.Set(ctxWorkspaceID, wsScopeWS)
		c.Next()
	})
	r.POST("/api/v1/admin/workspaces/:id/plan", h)
	return r
}

func postAdminPlan(t *testing.T, body map[string]any) *httptest.ResponseRecorder {
	t.Helper()
	b, _ := json.Marshal(body)
	r := adminPlanRouter(AdminSetWorkspacePlan)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/workspaces/"+adminTargetWS+"/plan", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	return w
}

func TestAdminSetWorkspacePlan_UpdatesAndAudits(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	// 1. Validate the requested plan exists in the catalogue.
	mock.ExpectQuery(`SELECT 1 FROM plans WHERE name = \$1`).
		WithArgs("pro").
		WillReturnRows(sqlmock.NewRows([]string{"?column?"}).AddRow(1))
	// 2. Write the plan onto the target WORKSPACE (bound as $2), clearing expiry.
	mock.ExpectExec(`UPDATE workspaces SET plan`).
		WithArgs("pro", adminTargetWS).
		WillReturnResult(sqlmock.NewResult(0, 1))
	// 3. Audit the privileged change.
	mock.ExpectExec(`INSERT INTO audit_logs`).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	w := postAdminPlan(t, map[string]any{"plan": "pro"})
	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

func TestAdminSetWorkspacePlan_RejectsUnknownPlan(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	// An unknown plan name must be rejected BEFORE any write (workspaces.plan has
	// no FK to plans, so the handler must validate the catalogue itself).
	mock.ExpectQuery(`SELECT 1 FROM plans WHERE name = \$1`).
		WithArgs("gold").
		WillReturnError(sql.ErrNoRows)

	w := postAdminPlan(t, map[string]any{"plan": "gold"})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("want 400 for unknown plan, got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations (no UPDATE should run): %v", err)
	}
}

func TestAdminSetWorkspacePlan_NotFound(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	mock.ExpectQuery(`SELECT 1 FROM plans WHERE name = \$1`).
		WithArgs("pro").
		WillReturnRows(sqlmock.NewRows([]string{"?column?"}).AddRow(1))
	// 0 rows affected ⇒ the workspace id doesn't exist ⇒ 404, no audit.
	mock.ExpectExec(`UPDATE workspaces SET plan`).
		WithArgs("pro", adminTargetWS).
		WillReturnResult(sqlmock.NewResult(0, 0))

	w := postAdminPlan(t, map[string]any{"plan": "pro"})
	if w.Code != http.StatusNotFound {
		t.Fatalf("want 404 for unknown workspace, got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

func TestAdminSetWorkspacePlan_RejectsEmptyPlan(t *testing.T) {
	_, cleanup := wsScopeMockDB(t)
	defer cleanup()

	// Empty/missing plan is a 400 with no DB round-trip at all.
	_ = db.DB
	w := postAdminPlan(t, map[string]any{"plan": ""})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("want 400 for empty plan, got %d: %s", w.Code, w.Body.String())
	}
}
