package handlers

import (
	"database/sql"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"api-gateway/internal/db"
	"api-gateway/internal/security"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
)

// Fixed UUIDs for the workspace-context tests.
const (
	wsUser     = "user-A"
	wsTeam     = "22222222-2222-2222-2222-222222222222"
	wsPersonal = "33333333-3333-3333-3333-333333333333"
	wsStale    = "44444444-4444-4444-4444-444444444444"
	wsResource = "55555555-5555-5555-5555-555555555555"
)

// SQL the middleware/gates issue (sqlmock matches these as regexp).
const (
	membershipQ = `SELECT role FROM workspace_members WHERE workspace_id = \$1 AND user_id = \$2`
	personalQ   = `w\.owner_id = \$1 AND w\.is_personal`
)

// wsMiddlewareEngine builds a gin engine that stands in for the real router:
// a first middleware sets user_id from X-Test-User (like the auth middleware
// would), then WorkspaceContextMiddleware runs on the /api/v1 group. The probe
// handlers echo the workspace context so tests can assert what was resolved.
func wsMiddlewareEngine() *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(func(c *gin.Context) {
		if u := c.GetHeader("X-Test-User"); u != "" {
			c.Set("user_id", u)
		}
		c.Next()
	})
	api := r.Group("/api/v1")
	api.Use(WorkspaceContextMiddleware())
	echo := func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"workspace_id":   c.GetString("workspace_id"),
			"workspace_role": c.GetString("workspace_role"),
		})
	}
	api.GET("/probe", echo)
	api.GET("/workspaces", echo) // workspace-CRUD route: context is optional here
	return r
}

func wsReq(r *gin.Engine, path, user, header, cookie string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	if user != "" {
		req.Header.Set("X-Test-User", user)
	}
	if header != "" {
		req.Header.Set("X-Workspace-ID", header)
	}
	if cookie != "" {
		req.AddCookie(&http.Cookie{Name: "rsync_active_workspace_id", Value: cookie})
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func TestWorkspaceContext_HeaderMember_SetsContext(t *testing.T) {
	_, mock, cleanup := setupMockDB(t)
	defer cleanup()
	mock.ExpectQuery(membershipQ).WithArgs(wsTeam, wsUser).
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("admin"))

	w := wsReq(wsMiddlewareEngine(), "/api/v1/probe", wsUser, wsTeam, "")
	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d (%s)", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), wsTeam) || !strings.Contains(w.Body.String(), "admin") {
		t.Fatalf("context not set from header: %s", w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestWorkspaceContext_HeaderNonMember_404(t *testing.T) {
	_, mock, cleanup := setupMockDB(t)
	defer cleanup()
	mock.ExpectQuery(membershipQ).WithArgs(wsTeam, wsUser).
		WillReturnError(sql.ErrNoRows)

	w := wsReq(wsMiddlewareEngine(), "/api/v1/probe", wsUser, wsTeam, "")
	// The header is an untrusted hint: a non-member must not learn whether the
	// workspace exists, and the request must not proceed under it.
	if w.Code != http.StatusNotFound {
		t.Fatalf("want 404 for non-member header, got %d (%s)", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestWorkspaceContext_CookieMember_SetsContext(t *testing.T) {
	_, mock, cleanup := setupMockDB(t)
	defer cleanup()
	mock.ExpectQuery(membershipQ).WithArgs(wsTeam, wsUser).
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("member"))

	w := wsReq(wsMiddlewareEngine(), "/api/v1/probe", wsUser, "", wsTeam)
	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d (%s)", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), wsTeam) || !strings.Contains(w.Body.String(), "member") {
		t.Fatalf("context not set from cookie: %s", w.Body.String())
	}
}

func TestWorkspaceContext_StaleCookie_FallsBackToPersonal(t *testing.T) {
	_, mock, cleanup := setupMockDB(t)
	defer cleanup()
	// A stale cookie (no longer a member) must not 404 — it falls through to
	// the caller's personal workspace.
	mock.ExpectQuery(membershipQ).WithArgs(wsStale, wsUser).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery(personalQ).WithArgs(wsUser).
		WillReturnRows(sqlmock.NewRows([]string{"id", "role"}).AddRow(wsPersonal, "owner"))

	w := wsReq(wsMiddlewareEngine(), "/api/v1/probe", wsUser, "", wsStale)
	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d (%s)", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), wsPersonal) || !strings.Contains(w.Body.String(), "owner") {
		t.Fatalf("did not fall back to personal workspace: %s", w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestWorkspaceContext_NoSelection_UsesPersonal(t *testing.T) {
	_, mock, cleanup := setupMockDB(t)
	defer cleanup()
	mock.ExpectQuery(personalQ).WithArgs(wsUser).
		WillReturnRows(sqlmock.NewRows([]string{"id", "role"}).AddRow(wsPersonal, "owner"))

	w := wsReq(wsMiddlewareEngine(), "/api/v1/probe", wsUser, "", "")
	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d (%s)", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), wsPersonal) {
		t.Fatalf("personal workspace not used: %s", w.Body.String())
	}
}

func TestWorkspaceContext_OptionalRoute_BadHeader_DoesNotAbort(t *testing.T) {
	_, mock, cleanup := setupMockDB(t)
	defer cleanup()
	// /api/v1/workspaces is in the optional set: a bad header must NOT 404,
	// so the user can always list/create workspaces to recover.
	mock.ExpectQuery(membershipQ).WithArgs(wsStale, wsUser).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery(personalQ).WithArgs(wsUser).
		WillReturnRows(sqlmock.NewRows([]string{"id", "role"}).AddRow(wsPersonal, "owner"))

	w := wsReq(wsMiddlewareEngine(), "/api/v1/workspaces", wsUser, wsStale, "")
	if w.Code != http.StatusOK {
		t.Fatalf("optional route must not abort on bad header; got %d (%s)", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), wsPersonal) {
		t.Fatalf("optional route did not fall back to personal: %s", w.Body.String())
	}
}

func TestWorkspaceContext_Unauthenticated_PassesThrough(t *testing.T) {
	_, mock, cleanup := setupMockDB(t)
	defer cleanup()
	// No user_id (auth middleware will reject downstream). Workspace middleware
	// must not touch the DB and must not abort.
	w := wsReq(wsMiddlewareEngine(), "/api/v1/probe", "", "", "")
	if w.Code != http.StatusOK {
		t.Fatalf("want 200 passthrough, got %d (%s)", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), `"workspace_id":""`) {
		t.Fatalf("expected empty workspace context, got %s", w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expected no DB queries: %v", err)
	}
}

func TestWorkspaceContext_DBDown_503(t *testing.T) {
	prev := db.DB
	db.DB = nil
	defer func() { db.DB = prev }()

	w := wsReq(wsMiddlewareEngine(), "/api/v1/probe", wsUser, wsTeam, "")
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("want 503 when DB down, got %d (%s)", w.Code, w.Body.String())
	}
}

// ---- requireWorkspaceRole (ladder gate over the context role) ----

func runWSRoleGate(ctxRole string, min security.WorkspaceRole) (security.WorkspaceRole, bool, int) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/", nil)
	if ctxRole != "" {
		c.Set("workspace_role", ctxRole)
	}
	role, ok := requireWorkspaceRole(c, min)
	return role, ok, w.Code
}

func TestRequireWorkspaceRole(t *testing.T) {
	cases := []struct {
		name       string
		ctxRole    string
		min        security.WorkspaceRole
		wantOK     bool
		wantStatus int
	}{
		{"admin meets member", "admin", security.WSMember, true, http.StatusOK},
		{"viewer below admin", "viewer", security.WSAdmin, false, http.StatusForbidden},
		{"no role fails closed", "", security.WSViewer, false, http.StatusForbidden},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, ok, status := runWSRoleGate(tc.ctxRole, tc.min)
			if ok != tc.wantOK {
				t.Fatalf("ok=%v want %v", ok, tc.wantOK)
			}
			if !tc.wantOK && status != tc.wantStatus {
				t.Fatalf("status=%d want %d", status, tc.wantStatus)
			}
		})
	}
}

// ---- requireResourceRole (IDOR-safe per-resource gate) ----

func runResourceGate(t *testing.T, user, activeWS, table, resourceID string, min security.WorkspaceRole, setup func(sqlmock.Sqlmock)) (security.WorkspaceRole, bool, int, string) {
	t.Helper()
	_, mock, cleanup := setupMockDB(t)
	defer cleanup()
	if setup != nil {
		setup(mock)
	}
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/", nil)
	if user != "" {
		c.Set("user_id", user)
	}
	if activeWS != "" {
		c.Set("workspace_id", activeWS)
	}
	role, ok := requireResourceRole(c, table, resourceID, min)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
	return role, ok, w.Code, w.Body.String()
}

func TestRequireResourceRole_Allows_MemberOfOwningWorkspace(t *testing.T) {
	role, ok, status, body := runResourceGate(t, wsUser, wsTeam, "pipelines", wsResource, security.WSMember,
		func(m sqlmock.Sqlmock) {
			m.ExpectQuery(`FROM pipelines r`).
				WithArgs(wsResource, wsUser, wsTeam).
				WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("admin"))
		})
	if !ok {
		t.Fatalf("expected allow, got ok=false status=%d body=%s", status, body)
	}
	if role != security.WSAdmin {
		t.Fatalf("want role admin, got %q", role)
	}
}

func TestRequireResourceRole_NotInActiveWorkspace_404(t *testing.T) {
	// The join finds no row because the resource's workspace_id != active ws
	// (or the caller isn't a member). Indistinguishable to the caller: 404.
	_, ok, status, _ := runResourceGate(t, wsUser, wsTeam, "connections", wsResource, security.WSViewer,
		func(m sqlmock.Sqlmock) {
			m.ExpectQuery(`FROM connections r`).
				WithArgs(wsResource, wsUser, wsTeam).
				WillReturnError(sql.ErrNoRows)
		})
	if ok {
		t.Fatalf("expected deny")
	}
	if status != http.StatusNotFound {
		t.Fatalf("want 404, got %d", status)
	}
}

func TestRequireResourceRole_RoleBelowMinimum_403(t *testing.T) {
	_, ok, status, _ := runResourceGate(t, wsUser, wsTeam, "pipelines", wsResource, security.WSAdmin,
		func(m sqlmock.Sqlmock) {
			m.ExpectQuery(`FROM pipelines r`).
				WithArgs(wsResource, wsUser, wsTeam).
				WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("viewer"))
		})
	if ok {
		t.Fatalf("expected deny for viewer < admin")
	}
	if status != http.StatusForbidden {
		t.Fatalf("want 403, got %d", status)
	}
}

func TestRequireResourceRole_DisallowedTable_500_NoQuery(t *testing.T) {
	// An un-allowlisted table name must never be interpolated into SQL.
	_, ok, status, _ := runResourceGate(t, wsUser, wsTeam, "users", wsResource, security.WSViewer, nil)
	if ok {
		t.Fatalf("expected deny for disallowed table")
	}
	if status != http.StatusInternalServerError {
		t.Fatalf("want 500, got %d", status)
	}
}

func TestRequireResourceRole_NoActiveWorkspace_404_NoQuery(t *testing.T) {
	_, ok, status, _ := runResourceGate(t, wsUser, "" /* no active ws */, "pipelines", wsResource, security.WSViewer, nil)
	if ok {
		t.Fatalf("expected deny with no active workspace")
	}
	if status != http.StatusNotFound {
		t.Fatalf("want 404, got %d", status)
	}
}

func TestRequireResourceRole_Unauthenticated_401_NoQuery(t *testing.T) {
	_, ok, status, _ := runResourceGate(t, "" /* no user */, wsTeam, "pipelines", wsResource, security.WSViewer, nil)
	if ok {
		t.Fatalf("expected deny when unauthenticated")
	}
	if status != http.StatusUnauthorized {
		t.Fatalf("want 401, got %d", status)
	}
}
