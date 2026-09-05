package handlers

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// P3c: workspace member management — ChangeMemberRole (admin+) and RemoveMember
// (admin+ to remove others, any member to self-leave). Both are workspace-
// ADMINISTRATION endpoints gated on the URL :id param via requireWorkspaceParamRole
// (IDOR-safe; see workspace_invites_test.go header).
//
// Two invariants beyond the plain role gate:
//   - Last-owner guard: a workspace must always keep ≥1 owner. Demoting the sole
//     owner (ChangeMemberRole) or removing the sole owner (RemoveMember) → 409.
//   - Ownership is owner-only: only a caller who is themselves an owner may grant
//     the owner role or modify an existing owner. An admin cannot self-promote to
//     owner (privilege-escalation guard).
//
// wsScopeUser / wsScopeWS / wsScopeRouter / wsScopeMockDB / wsParamGate come from
// sibling test files in this package.

const wsTargetUser = "33333333-3333-3333-3333-333333333333"

// errTestDBDown stands in for a transient (non-ErrNoRows) DB failure.
var errTestDBDown = errors.New("connection reset by peer")

// wsTargetRole stubs the target member's current-role lookup (distinct args from
// the caller gate, so order-agnostic matching disambiguates by user_id).
func wsTargetRole(mock sqlmock.Sqlmock, role string) {
	mock.ExpectQuery(`SELECT role FROM workspace_members WHERE workspace_id = \$1 AND user_id = \$2`).
		WithArgs(wsScopeWS, wsTargetUser).
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow(role))
}

// wsTargetMissing stubs a target lookup that finds no row (→ 404).
func wsTargetMissing(mock sqlmock.Sqlmock) {
	mock.ExpectQuery(`SELECT role FROM workspace_members WHERE workspace_id = \$1 AND user_id = \$2`).
		WithArgs(wsScopeWS, wsTargetUser).
		WillReturnRows(sqlmock.NewRows([]string{"role"}))
}

// wsOwnerCount stubs the last-owner-guard count query.
func wsOwnerCount(mock sqlmock.Sqlmock, n int) {
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM workspace_members WHERE workspace_id = \$1 AND role = 'owner'`).
		WithArgs(wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(n))
}

// wsLockWorkspace stubs the FOR UPDATE row lock that serializes ownership changes
// per workspace, closing the count-then-mutate TOCTOU window. Owner demotions and
// removals run inside a transaction: BEGIN -> lock -> count -> mutate -> COMMIT.
func wsLockWorkspace(mock sqlmock.Sqlmock) {
	mock.ExpectQuery(`SELECT id FROM workspaces WHERE id = \$1 FOR UPDATE`).
		WithArgs(wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(wsScopeWS))
}

func roleReq(target string, role string) *http.Request {
	body, _ := json.Marshal(map[string]any{"role": role})
	req := httptest.NewRequest(http.MethodPost,
		"/api/v1/workspaces/"+wsScopeWS+"/members/"+target+"/role", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	return req
}

// ---- ChangeMemberRole ----

func TestChangeMemberRole_AdminPromotesMemberToAdmin(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)
	wsParamGate(mock, "admin")
	wsTargetRole(mock, "member")
	mock.ExpectExec(`UPDATE workspace_members SET role = \$1 WHERE workspace_id = \$2 AND user_id = \$3`).
		WithArgs("admin", wsScopeWS, wsTargetUser).
		WillReturnResult(sqlmock.NewResult(0, 1))

	r := wsScopeRouter(http.MethodPost, "/api/v1/workspaces/:id/members/:userId/role", ChangeMemberRole)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, roleReq(wsTargetUser, "admin"))

	if resp.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", resp.Code, resp.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestChangeMemberRole_MemberForbidden(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)
	wsParamGate(mock, "member") // gate requires admin+

	r := wsScopeRouter(http.MethodPost, "/api/v1/workspaces/:id/members/:userId/role", ChangeMemberRole)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, roleReq(wsTargetUser, "admin"))

	if resp.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d: %s", resp.Code, resp.Body.String())
	}
}

func TestChangeMemberRole_TargetNotFound(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)
	wsParamGate(mock, "admin")
	wsTargetMissing(mock)

	r := wsScopeRouter(http.MethodPost, "/api/v1/workspaces/:id/members/:userId/role", ChangeMemberRole)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, roleReq(wsTargetUser, "admin"))

	if resp.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", resp.Code, resp.Body.String())
	}
}

func TestChangeMemberRole_InvalidRole(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)
	wsParamGate(mock, "admin")

	r := wsScopeRouter(http.MethodPost, "/api/v1/workspaces/:id/members/:userId/role", ChangeMemberRole)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, roleReq(wsTargetUser, "superadmin"))

	if resp.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", resp.Code, resp.Body.String())
	}
}

// An admin must NOT be able to grant the owner role (privilege escalation).
func TestChangeMemberRole_AdminCannotGrantOwner(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)
	wsParamGate(mock, "admin")

	r := wsScopeRouter(http.MethodPost, "/api/v1/workspaces/:id/members/:userId/role", ChangeMemberRole)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, roleReq(wsTargetUser, "owner"))

	if resp.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d: %s", resp.Code, resp.Body.String())
	}
}

// Last-owner guard: an owner demoting themselves when they are the sole owner → 409.
// The guard is atomic: BEGIN -> lock workspace -> count -> (reject) -> ROLLBACK.
func TestChangeMemberRole_SelfDemoteSoleOwnerRejected(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)
	wsParamGate(mock, "owner") // caller (== target) is the sole owner
	mock.ExpectBegin()
	wsLockWorkspace(mock)
	wsOwnerCount(mock, 1)
	mock.ExpectRollback() // 409 returns before commit; deferred rollback hits the driver

	r := wsScopeRouter(http.MethodPost, "/api/v1/workspaces/:id/members/:userId/role", ChangeMemberRole)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, roleReq(wsScopeUser, "member")) // target is self

	if resp.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d: %s", resp.Code, resp.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// An owner CAN demote another owner while ≥1 owner remains. Atomic path:
// BEGIN -> lock -> count(2) -> UPDATE -> COMMIT.
func TestChangeMemberRole_OwnerDemotesAnotherOwnerWhenNotLast(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)
	wsParamGate(mock, "owner")
	wsTargetRole(mock, "owner")
	mock.ExpectBegin()
	wsLockWorkspace(mock)
	wsOwnerCount(mock, 2)
	mock.ExpectExec(`UPDATE workspace_members SET role = \$1 WHERE workspace_id = \$2 AND user_id = \$3`).
		WithArgs("member", wsScopeWS, wsTargetUser).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	r := wsScopeRouter(http.MethodPost, "/api/v1/workspaces/:id/members/:userId/role", ChangeMemberRole)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, roleReq(wsTargetUser, "member"))

	if resp.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", resp.Code, resp.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// A transient DB error on the target-role lookup surfaces as 503 (retryable),
// matching requireWorkspaceParamRole — not 500.
func TestChangeMemberRole_TargetLookupDBError(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)
	wsParamGate(mock, "admin")
	mock.ExpectQuery(`SELECT role FROM workspace_members WHERE workspace_id = \$1 AND user_id = \$2`).
		WithArgs(wsScopeWS, wsTargetUser).
		WillReturnError(errTestDBDown)

	r := wsScopeRouter(http.MethodPost, "/api/v1/workspaces/:id/members/:userId/role", ChangeMemberRole)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, roleReq(wsTargetUser, "admin"))

	if resp.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d: %s", resp.Code, resp.Body.String())
	}
}

// ---- RemoveMember ----

func delReq(target string) *http.Request {
	return httptest.NewRequest(http.MethodDelete,
		"/api/v1/workspaces/"+wsScopeWS+"/members/"+target, nil)
}

func TestRemoveMember_AdminRemovesMember(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)
	wsParamGate(mock, "admin")
	wsTargetRole(mock, "member")
	mock.ExpectExec(`DELETE FROM workspace_members WHERE workspace_id = \$1 AND user_id = \$2`).
		WithArgs(wsScopeWS, wsTargetUser).
		WillReturnResult(sqlmock.NewResult(0, 1))

	r := wsScopeRouter(http.MethodDelete, "/api/v1/workspaces/:id/members/:userId", RemoveMember)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, delReq(wsTargetUser))

	if resp.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", resp.Code, resp.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// Any member may remove themselves (self-leave), even a viewer.
func TestRemoveMember_SelfLeave(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)
	wsParamGate(mock, "viewer")
	mock.ExpectExec(`DELETE FROM workspace_members WHERE workspace_id = \$1 AND user_id = \$2`).
		WithArgs(wsScopeWS, wsScopeUser).
		WillReturnResult(sqlmock.NewResult(0, 1))

	r := wsScopeRouter(http.MethodDelete, "/api/v1/workspaces/:id/members/:userId", RemoveMember)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, delReq(wsScopeUser)) // target == caller

	if resp.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", resp.Code, resp.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// A non-admin cannot remove a DIFFERENT member.
func TestRemoveMember_NonAdminCannotRemoveOther(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)
	wsParamGate(mock, "member")

	r := wsScopeRouter(http.MethodDelete, "/api/v1/workspaces/:id/members/:userId", RemoveMember)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, delReq(wsTargetUser))

	if resp.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d: %s", resp.Code, resp.Body.String())
	}
}

// Last-owner guard: the sole owner cannot leave. Atomic path:
// BEGIN -> lock -> count(1) -> (reject) -> ROLLBACK.
func TestRemoveMember_SoleOwnerCannotLeave(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)
	wsParamGate(mock, "owner")
	mock.ExpectBegin()
	wsLockWorkspace(mock)
	wsOwnerCount(mock, 1)
	mock.ExpectRollback()

	r := wsScopeRouter(http.MethodDelete, "/api/v1/workspaces/:id/members/:userId", RemoveMember)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, delReq(wsScopeUser)) // sole owner self-leave

	if resp.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d: %s", resp.Code, resp.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// Owner-only guard (symmetric with ChangeMemberRole): a mere admin must NOT be
// able to evict an owner, even when other owners remain. Removal is the most
// severe modification of an owner, so it is owner-only.
func TestRemoveMember_AdminCannotRemoveOwner(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)
	wsParamGate(mock, "admin")
	wsTargetRole(mock, "owner")
	// No BEGIN / count / DELETE: the owner-only guard rejects before the atomic path.

	r := wsScopeRouter(http.MethodDelete, "/api/v1/workspaces/:id/members/:userId", RemoveMember)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, delReq(wsTargetUser))

	if resp.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d: %s", resp.Code, resp.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// An owner CAN remove another owner while ≥1 owner remains. Atomic path:
// BEGIN -> lock -> count(2) -> DELETE -> COMMIT.
func TestRemoveMember_OwnerRemovesAnotherOwnerWhenNotLast(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)
	wsParamGate(mock, "owner")
	wsTargetRole(mock, "owner")
	mock.ExpectBegin()
	wsLockWorkspace(mock)
	wsOwnerCount(mock, 2)
	mock.ExpectExec(`DELETE FROM workspace_members WHERE workspace_id = \$1 AND user_id = \$2`).
		WithArgs(wsScopeWS, wsTargetUser).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	r := wsScopeRouter(http.MethodDelete, "/api/v1/workspaces/:id/members/:userId", RemoveMember)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, delReq(wsTargetUser))

	if resp.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", resp.Code, resp.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestRemoveMember_TargetNotFound(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)
	wsParamGate(mock, "admin")
	wsTargetMissing(mock)

	r := wsScopeRouter(http.MethodDelete, "/api/v1/workspaces/:id/members/:userId", RemoveMember)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, delReq(wsTargetUser))

	if resp.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", resp.Code, resp.Body.String())
	}
}
