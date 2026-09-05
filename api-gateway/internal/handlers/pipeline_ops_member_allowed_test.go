package handlers

import (
	"database/sql"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// RBAC P1 (A2/A1) — POSITIVE (allow-path) coverage for the workspace-role gates.
//
// The viewer-denial tests prove a VIEWER is rejected, but on their own they
// can't catch a regression that over-hardens a mutation gate (e.g. WSMember →
// WSAdmin): a viewer would still be denied, so a deny-only test stays green
// while a legitimate MEMBER is locked out. These tests pin the lower boundary —
// a MEMBER must be allowed past each mutation gate, and a VIEWER must be allowed
// past each READ gate. Together with the *_ViewerDenied tests they bracket the
// exact role each handler requires.
//
// wsScope* / wsScopeMockDB / wsScopeRouterAsRole / gateRoleRows come from the
// sibling *_test.go files (same package).

// expectMemberRoleGate stamps the requireResourceRole role lookup so the caller
// resolves as a MEMBER (>= WSMember → the mutation gate passes).
func expectMemberRoleGate(mock sqlmock.Sqlmock) {
	mock.ExpectQuery(`SELECT wm\.role\s+FROM pipelines r`).
		WithArgs(wsScopePipeline, wsScopeUser, wsScopeWS).
		WillReturnRows(gateRoleRows("member"))
}

// assertGatePassed asserts the role gate ALLOWED the request through — i.e. the
// response is anything but a 403 role denial. The proxy/DB work past the gate
// fails in a unit test (orchestrator unreachable, unmocked query), but that is
// downstream of authorization; what matters is the request was NOT rejected as
// "insufficient workspace role". If the gate were tightened to WSAdmin this
// member call would 403 and the assertion would fail.
func assertGatePassed(t *testing.T, w *httptest.ResponseRecorder, what string) {
	t.Helper()
	if w.Code == http.StatusForbidden || strings.Contains(w.Body.String(), "insufficient workspace role") {
		t.Fatalf("a member must pass the %s gate, not be denied; got %d: %s", what, w.Code, w.Body.String())
	}
}

func TestPausePipelineCDC_MemberAllowed(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)
	expectMemberRoleGate(mock)

	r := wsScopeRouterAsRole(http.MethodPost, "/p/:id/cdc/pause", PausePipelineCDC, "member")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/p/"+wsScopePipeline+"/cdc/pause", nil))
	assertGatePassed(t, w, "CDC pause")
}

func TestResumePipelineCDC_MemberAllowed(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)
	expectMemberRoleGate(mock)

	r := wsScopeRouterAsRole(http.MethodPost, "/p/:id/cdc/resume", ResumePipelineCDC, "member")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/p/"+wsScopePipeline+"/cdc/resume", nil))
	assertGatePassed(t, w, "CDC resume")
}

func TestResumePipelineTables_MemberAllowed(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)
	expectMemberRoleGate(mock)

	r := wsScopeRouterAsRole(http.MethodPost, "/p/:id/resume/tables", ResumePipelineTables, "member")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/p/"+wsScopePipeline+"/resume/tables", nil))
	assertGatePassed(t, w, "HITL tables resume")
}

// TestCancelExecution_MemberAllowed is a full happy-path: a MEMBER cancels a
// running execution in the active workspace and gets 200. Brackets
// TestCancelExecution_ViewerDenied (viewer → 403) on the WSMember boundary.
func TestCancelExecution_MemberAllowed(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)

	// Workspace-scoped status lookup yields the pipeline the role gate authorizes.
	mock.ExpectQuery(`SELECT e\.status, e\.pipeline_id`).
		WithArgs(wsScopeExecution, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"status", "pipeline_id"}).AddRow("running", wsScopePipeline))
	expectMemberRoleGate(mock)
	mock.ExpectExec(`UPDATE executions SET status = 'cancelled'`).
		WithArgs(wsScopeExecution).
		WillReturnResult(sqlmock.NewResult(0, 1))
	// Best-effort projections (errors are swallowed by the handler).
	mock.ExpectExec(`UPDATE pipeline_progress`).
		WithArgs(wsScopePipeline, wsScopeExecution).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`UPDATE pipelines SET status = 'stopped'`).
		WithArgs(wsScopePipeline).
		WillReturnResult(sqlmock.NewResult(0, 1))

	r := wsScopeRouterAsRole(http.MethodPost, "/executions/:id/cancel", CancelExecution, "member")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/executions/"+wsScopeExecution+"/cancel", nil))

	if w.Code != http.StatusOK {
		t.Fatalf("a member must be allowed to cancel a teammate's run; got %d: %s", w.Code, w.Body.String())
	}
}

// TestGetPipeline_ViewerCanRead restores the read-gate coverage lost when
// pipeline_ownership_test.go was deleted: a VIEWER (the lowest role) must be
// able to read a pipeline in the active workspace.
func TestGetPipeline_ViewerCanRead(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)

	// Read gate: requirePipelineWorkspaceRole(WSViewer) — a viewer resolves >= viewer.
	mock.ExpectQuery(`SELECT wm\.role\s+FROM pipelines r`).
		WithArgs(wsScopePipeline, wsScopeUser, wsScopeWS).
		WillReturnRows(gateRoleRows("viewer"))
	// Detail fetch (17 cols). created_at/updated_at must be real times (Scan into
	// time.Time rejects NULL); everything else nullable. BUG-1 added the 17th
	// column rows_processed (authoritative count from pipeline_run_table_stats).
	mock.ExpectQuery(`WITH latest_exec AS`).
		WithArgs(wsScopePipeline).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "name", "description", "status", "created_at", "updated_at",
			"created_by", "source_connection_id", "destination_connection_id",
			"sync_mode", "cdc_mode", "sync_mode_source", "config",
			"source_connection", "destination_connection", "last_execution",
			"rows_processed",
		}).AddRow(wsScopePipeline, "Shared Pipe", nil, "active", time.Now(), time.Now(),
			nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, int64(0)))

	r := wsScopeRouterAsRole(http.MethodGet, "/pipelines/:id", GetPipeline, "viewer")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/pipelines/"+wsScopePipeline, nil))

	if w.Code != http.StatusOK {
		t.Fatalf("a viewer must read a pipeline in the active workspace; got %d: %s", w.Code, w.Body.String())
	}
}

// TestGetPipeline_NonMemberDenied: a caller with no membership row for the
// pipeline's workspace gets 404 (IDOR-safe — never leaks the pipeline's
// existence), not a readable body.
func TestGetPipeline_NonMemberDenied(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	// No workspace_members row → requireResourceRole 404s before any detail fetch.
	mock.ExpectQuery(`SELECT wm\.role\s+FROM pipelines r`).
		WithArgs(wsScopePipeline, wsScopeUser, wsScopeWS).
		WillReturnError(sql.ErrNoRows)

	r := wsScopeRouterAsRole(http.MethodGet, "/pipelines/:id", GetPipeline, "viewer")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/pipelines/"+wsScopePipeline, nil))

	if w.Code != http.StatusNotFound {
		t.Fatalf("a non-member must get 404 (no existence leak); got %d: %s", w.Code, w.Body.String())
	}
}
