package handlers

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

// RBAC P1 (A2): the pipeline OPERATION handlers — CDC controls, HITL resumes,
// and schema-change approve/reject — historically gated on requirePipelineOwner,
// which (despite its name) authorized by mere workspace MEMBERSHIP via
// canAccessPipeline. A VIEWER therefore could pause/resume CDC, answer HITL
// prompts, and approve/reject schema changes on a shared pipeline — a mutation a
// read-only role must never perform.
//
// These reproducers pin the fix: a VIEWER in the active workspace is denied with
// a ROLE error (403 "insufficient workspace role") on each operation class. The
// gate is requirePipelineWorkspaceRole(WSMember), whose single role query the
// mock below stands in for. Pre-fix the handler runs canAccessPipeline's 2-arg
// membership query (which does not match this mock), so it 404s rather than 403s
// — i.e. these tests are RED until the gate is swapped.
//
// wsScopeUser / wsScopeWS / wsScopeMockDB / gateRoleRows / wsScopePipeline come
// from the sibling *_scoping_test.go files (same package); wsScopeRouterAsRole
// from workspace_create_role_gate_test.go.

// expectViewerRoleGate stamps the requireResourceRole role lookup, returning the
// caller's role as "viewer" so the >= member gate denies with 403.
func expectViewerRoleGate(t *testing.T) func() {
	t.Helper()
	mock, cleanup := wsScopeMockDB(t)
	mock.MatchExpectationsInOrder(false)
	mock.ExpectQuery(`SELECT wm\.role\s+FROM pipelines r`).
		WithArgs(wsScopePipeline, wsScopeUser, wsScopeWS).
		WillReturnRows(gateRoleRows("viewer"))
	return cleanup
}

func assertViewerDenied(t *testing.T, w *httptest.ResponseRecorder, what string) {
	t.Helper()
	if w.Code != http.StatusForbidden || !strings.Contains(w.Body.String(), "insufficient workspace role") {
		t.Fatalf("viewer must be denied %s with a role error; got %d: %s", what, w.Code, w.Body.String())
	}
}

func TestPausePipelineCDC_ViewerDenied(t *testing.T) {
	defer expectViewerRoleGate(t)()
	r := wsScopeRouterAsRole(http.MethodPost, "/p/:id/cdc/pause", PausePipelineCDC, "viewer")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/p/"+wsScopePipeline+"/cdc/pause", nil))
	assertViewerDenied(t, w, "CDC pause")
}

func TestResumePipelineCDC_ViewerDenied(t *testing.T) {
	defer expectViewerRoleGate(t)()
	r := wsScopeRouterAsRole(http.MethodPost, "/p/:id/cdc/resume", ResumePipelineCDC, "viewer")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/p/"+wsScopePipeline+"/cdc/resume", nil))
	assertViewerDenied(t, w, "CDC resume")
}

func TestResumePipelineTables_ViewerDenied(t *testing.T) {
	defer expectViewerRoleGate(t)()
	r := wsScopeRouterAsRole(http.MethodPost, "/p/:id/resume/tables", ResumePipelineTables, "viewer")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/p/"+wsScopePipeline+"/resume/tables", nil))
	assertViewerDenied(t, w, "HITL tables resume")
}

func TestApproveSchemaChange_ViewerDenied(t *testing.T) {
	defer expectViewerRoleGate(t)()
	r := newSchemaChangeViewerRouter(ApproveSchemaChange, "approve")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/p/"+wsScopePipeline+"/schema-changes/change-1/approve", nil))
	assertViewerDenied(t, w, "schema-change approve")
}

func TestRejectSchemaChange_ViewerDenied(t *testing.T) {
	// RejectSchemaChange carries the SAME WSMember gate as approve — a viewer
	// must not be able to reject a teammate's schema change either. The existing
	// schema_evolution_test.go happy paths run as owner, so without this the
	// reject gate's role boundary is untested.
	defer expectViewerRoleGate(t)()
	r := newSchemaChangeViewerRouter(RejectSchemaChange, "reject")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/p/"+wsScopePipeline+"/schema-changes/change-1/reject", nil))
	assertViewerDenied(t, w, "schema-change reject")
}

// newSchemaChangeViewerRouter wires a schema-change handler behind a shim that
// stamps a VIEWER context (user_id + active workspace + role), so the
// requirePipelineWorkspaceRole(WSMember) gate is the only thing under test.
func newSchemaChangeViewerRouter(handler gin.HandlerFunc, verb string) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	wire := func(c *gin.Context) {
		c.Set("user_id", wsScopeUser)
		c.Set(ctxWorkspaceID, wsScopeWS)
		c.Set(ctxWorkspaceRole, "viewer")
		handler(c)
	}
	r.POST("/p/:id/schema-changes/:changeId/"+verb, wire)
	return r
}
