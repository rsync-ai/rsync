package handlers

import (
	"bytes"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// UpdateWorkspace and ListWorkspaceMembers used to hand-roll their membership
// check instead of calling requireWorkspaceParamRole, and answered 403 to a caller
// who is not a member. Every other :id-param workspace endpoint answers 404 there,
// deliberately: a 403 ("insufficient role") confirms the workspace id EXISTS, while
// a 404 does not. Two endpoints disagreeing turned the pair into a membership
// oracle — probe a workspace id and read the status code to learn whether it is
// real. These tests pin both onto the shared helper's 404.
//
// The inline check in UpdateWorkspace also carried a second, hardcoded copy of the
// role ordering (`role != "owner" && role != "admin"`) that could drift away from
// security.WorkspaceRole; routing through the helper leaves exactly one.
//
// wsScopeUser / wsScopeWS / wsScopeRouter / wsScopeMockDB / wsParamGate come from
// sibling test files in this package.

// wsNoMembership stubs requireWorkspaceParamRole's lookup finding no membership row.
func wsNoMembership(mock sqlmock.Sqlmock) {
	mock.ExpectQuery(`SELECT role FROM workspace_members WHERE workspace_id = \$1 AND user_id = \$2`).
		WithArgs(wsScopeWS, wsScopeUser).
		WillReturnError(sql.ErrNoRows)
}

func wsRenameReq() *http.Request {
	req := httptest.NewRequest(http.MethodPatch, "/api/v1/workspaces/"+wsScopeWS,
		bytes.NewBufferString(`{"name":"renamed"}`))
	req.Header.Set("Content-Type", "application/json")
	return req
}

func updateWorkspaceRouter() http.Handler {
	return wsScopeRouter(http.MethodPatch, "/api/v1/workspaces/:id", UpdateWorkspace)
}

func listMembersRouter() http.Handler {
	return wsScopeRouter(http.MethodGet, "/api/v1/workspaces/:id/members", ListWorkspaceMembers)
}

func TestUpdateWorkspace_NonMemberGets404NotOracle(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)

	wsNoMembership(mock)

	resp := httptest.NewRecorder()
	updateWorkspaceRouter().ServeHTTP(resp, wsRenameReq())

	if resp.Code != http.StatusNotFound {
		t.Fatalf("expected 404 (never confirm the workspace exists), got %d: %s", resp.Code, resp.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// A member below the admin gate still gets 403 — they have already proven
// membership, so there is nothing left for the status code to disclose.
func TestUpdateWorkspace_MemberBelowAdminGets403(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)

	wsParamGate(mock, "member")

	resp := httptest.NewRecorder()
	updateWorkspaceRouter().ServeHTTP(resp, wsRenameReq())

	if resp.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d: %s", resp.Code, resp.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestListWorkspaceMembers_NonMemberGets404NotOracle(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)

	wsNoMembership(mock)

	resp := httptest.NewRecorder()
	listMembersRouter().ServeHTTP(resp, httptest.NewRequest(
		http.MethodGet, "/api/v1/workspaces/"+wsScopeWS+"/members", nil))

	if resp.Code != http.StatusNotFound {
		t.Fatalf("expected 404 (never confirm the workspace exists), got %d: %s", resp.Code, resp.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// A viewer may still read the roster — the gate is WSViewer, not WSAdmin. Pins
// that routing through the shared helper did not tighten who can list members.
func TestListWorkspaceMembers_ViewerCanRead(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)

	wsParamGate(mock, "viewer")
	mock.ExpectQuery(`FROM workspace_members wm JOIN users u`).
		WithArgs(wsScopeWS).
		WillReturnRows(sqlmock.
			NewRows([]string{"id", "workspace_id", "user_id", "email", "role", "created_at"}).
			AddRow("66666666-6666-6666-6666-666666666666", wsScopeWS, wsScopeUser, "viewer@example.com", "viewer", time.Now()))

	resp := httptest.NewRecorder()
	listMembersRouter().ServeHTTP(resp, httptest.NewRequest(
		http.MethodGet, "/api/v1/workspaces/"+wsScopeWS+"/members", nil))

	if resp.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", resp.Code, resp.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// randomSlugSuffix replaced a clock-derived retry candidate
// (time.Now().Format("0405") — minute+second) that was IDENTICAL on every
// iteration of a loop running in microseconds, so CreateWorkspace's de-dup loop
// retested one unchanged candidate ten times and then fell back to a value
// deterministic per (user, name). Creating the same-named workspace again could
// therefore reach the INSERT with a slug already taken and surface the UNIQUE
// violation as a generic 500. The suffix must be fresh on every call.
func TestRandomSlugSuffix_FreshOnEveryCall(t *testing.T) {
	const n = 200
	seen := make(map[string]bool, n)
	for i := 0; i < n; i++ {
		s := randomSlugSuffix()
		if len(s) != 8 {
			t.Fatalf("suffix %q: want 8 chars, got %d", s, len(s))
		}
		for _, r := range s {
			if !(r >= '0' && r <= '9') && !(r >= 'a' && r <= 'f') {
				t.Fatalf("suffix %q is not slug-safe lowercase hex", s)
			}
		}
		if seen[s] {
			t.Fatalf("suffix %q repeated within %d calls — a repeating candidate is exactly the bug this replaced", s, n)
		}
		seen[s] = true
	}
}
