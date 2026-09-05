package handlers

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

// RBAC P0: resource CREATE + pipeline-config MUTATIONS must enforce a minimum
// WORKSPACE ROLE of `member`, not mere membership. A live staging probe proved a
// VIEWER could POST /connections (force_save) and have a real row persisted, and
// POST /pipelines passed authorization — because CreateConnection / CreatePipeline
// resolved the active workspace but never checked the caller's role. Likewise the
// pipeline-config mutators (CDC tables, selected tables, schedules) gated on
// canAccessPipeline() (membership only), so a viewer could mutate shared config.
//
// These reproducers pin the fix: a VIEWER in the active workspace is denied with a
// ROLE error. The viewer-vs-member distinction is what the create-stamp tests in
// workspace_scoping_test.go (which run as owner) cannot catch.
//
// wsScopeUser / wsScopeWS / wsScopeMockDB / gateRoleRows / wsScopePipeline come
// from the sibling *_scoping_test.go files (same package).

// wsScopeRouterAsRole mirrors wsScopeRouter but pins an explicit workspace role,
// so a test can exercise the role gate as a viewer (read-only) rather than owner.
func wsScopeRouterAsRole(method, path string, h gin.HandlerFunc, role string) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(func(c *gin.Context) {
		c.Set("user_id", wsScopeUser)
		c.Set(ctxWorkspaceID, wsScopeWS)
		c.Set(ctxWorkspaceRole, role)
		c.Next()
	})
	r.Handle(method, path, h)
	return r
}

func TestCreateConnection_ViewerDenied(t *testing.T) {
	// Defensive: if the gate were ABSENT, the handler would reach encryption,
	// which fatals outside dev without a key. Set these so the pre-fix path
	// returns a non-403 rather than aborting the test binary.
	t.Setenv("ENVIRONMENT", "development")
	t.Setenv("ENCRYPTION_KEY", "0123456789abcdef0123456789abcdef")

	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	// No DB expectations: the gate must short-circuit (403) before any query.

	body, _ := json.Marshal(map[string]any{
		"name":            "viewer-conn",
		"connection_type": "source",
		"connector_type":  "postgresql",
		"force_save":      true,
		"config":          map[string]any{"host": "localhost"},
	})
	r := wsScopeRouterAsRole(http.MethodPost, "/api/v1/connections", CreateConnection, "viewer")
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/connections", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusForbidden {
		t.Fatalf("viewer must be denied connection-create; want 403, got %d: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "insufficient workspace role") {
		t.Fatalf("want role-gate error, got: %s", w.Body.String())
	}
	_ = mock
}

func TestCreatePipeline_ViewerDenied(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]any{"name": "viewer-pipe", "request": "copy data"})
	r := wsScopeRouterAsRole(http.MethodPost, "/api/v1/pipelines", CreatePipeline, "viewer")
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/pipelines", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusForbidden {
		t.Fatalf("viewer must be denied pipeline-create; want 403, got %d: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "insufficient workspace role") {
		t.Fatalf("want role-gate error, got: %s", w.Body.String())
	}
	_ = mock
}

func TestUpdatePipelineCDCTables_ViewerDenied(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)

	// Post-fix the handler authorizes via requireResourceRole, whose single role
	// query returns the caller's role for the pipeline in the active workspace.
	mock.ExpectQuery(`SELECT wm\.role\s+FROM pipelines r`).
		WithArgs(wsScopePipeline, wsScopeUser, wsScopeWS).
		WillReturnRows(gateRoleRows("viewer"))

	body, _ := json.Marshal(map[string]any{"tables": []string{"db.t1"}})
	r := wsScopeRouterAsRole(http.MethodPost, "/api/v1/pipelines/:id/cdc/tables", UpdatePipelineCDCTables, "viewer")
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/pipelines/"+wsScopePipeline+"/cdc/tables", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusForbidden || !strings.Contains(w.Body.String(), "insufficient workspace role") {
		t.Fatalf("viewer must be denied CDC-table mutation with a role error; got %d: %s", w.Code, w.Body.String())
	}
}

func TestUpdatePipelineTables_ViewerDenied(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)

	mock.ExpectQuery(`SELECT wm\.role\s+FROM pipelines r`).
		WithArgs(wsScopePipeline, wsScopeUser, wsScopeWS).
		WillReturnRows(gateRoleRows("viewer"))

	body, _ := json.Marshal(map[string]any{"tables": []string{"db.t1"}})
	r := wsScopeRouterAsRole(http.MethodPost, "/api/v1/pipelines/:id/tables", UpdatePipelineTables, "viewer")
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/pipelines/"+wsScopePipeline+"/tables", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusForbidden || !strings.Contains(w.Body.String(), "insufficient workspace role") {
		t.Fatalf("viewer must be denied selected-tables mutation with a role error; got %d: %s", w.Code, w.Body.String())
	}
}

func TestCreatePipelineSchedule_ViewerDenied(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)

	mock.ExpectQuery(`SELECT wm\.role\s+FROM pipelines r`).
		WithArgs(wsScopePipeline, wsScopeUser, wsScopeWS).
		WillReturnRows(gateRoleRows("viewer"))

	body, _ := json.Marshal(map[string]any{"schedule_type": "interval", "spec": map[string]any{"every": "1h"}})
	r := wsScopeRouterAsRole(http.MethodPost, "/api/v1/pipelines/:id/schedules", CreatePipelineSchedule, "viewer")
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/pipelines/"+wsScopePipeline+"/schedules", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusForbidden || !strings.Contains(w.Body.String(), "insufficient workspace role") {
		t.Fatalf("viewer must be denied schedule-create with a role error; got %d: %s", w.Code, w.Body.String())
	}
}
