package handlers

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// P3a: workspace invite lifecycle — CreateWorkspaceInvite (admin+),
// ListWorkspaceInvites (member+), RevokeWorkspaceInvite (admin+).
//
// These are workspace-ADMINISTRATION endpoints: the target workspace is the URL
// :id param, NOT the active-workspace header. The role gate therefore binds the
// :id param (requireWorkspaceParamRole → SELECT role FROM workspace_members
// WHERE workspace_id=:id AND user_id), mirroring the existing inline check in
// UpdateWorkspace/ListWorkspaceMembers. This is IDOR-safe by construction: a
// workspace-A admin cannot administer workspace B by sending X-Workspace-ID: A,
// because the gate proves their role IN B (the path), independent of the header.
//
// wsScopeUser / wsScopeWS / wsScopeRouter / wsScopeMockDB / gateRoleRows come
// from the sibling test files in this package.

const (
	wsInviteID    = "55555555-5555-5555-5555-555555555555"
	wsInviteEmail = "newteammate@example.com"
)

// wsParamGate is the membership row requireWorkspaceParamRole expects for the
// :id-param workspace (distinct from the active-workspace context role).
func wsParamGate(mock sqlmock.Sqlmock, role string) {
	mock.ExpectQuery(`SELECT role FROM workspace_members WHERE workspace_id = \$1 AND user_id = \$2`).
		WithArgs(wsScopeWS, wsScopeUser).
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow(role))
}

func TestCreateWorkspaceInvite_AdminCanInvite(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)

	// Gate: caller's role in the :id-param workspace is admin.
	wsParamGate(mock, "admin")
	expectIsPersonal(mock, false) // a shared (non-personal) workspace accepts invites

	// Not already a member.
	mock.ExpectQuery(`SELECT 1 FROM workspace_members wm JOIN users u`).
		WithArgs(wsScopeWS, wsInviteEmail).
		WillReturnError(sql.ErrNoRows)

	// No pending invite already outstanding for this email.
	mock.ExpectQuery(`SELECT 1 FROM workspace_invites\s+WHERE workspace_id = \$1 AND lower\(email\)`).
		WithArgs(wsScopeWS, wsInviteEmail).
		WillReturnError(sql.ErrNoRows)

	// Insert mints a token; expires_at is set server-side.
	mock.ExpectQuery(`INSERT INTO workspace_invites`).
		WithArgs(wsScopeWS, wsInviteEmail, "member", sqlmock.AnyArg(), wsScopeUser).
		WillReturnRows(sqlmock.NewRows([]string{"id", "status", "expires_at", "created_at"}).
			AddRow(wsInviteID, "pending", time.Now().Add(7*24*time.Hour), time.Now()))

	body, _ := json.Marshal(map[string]any{"email": wsInviteEmail, "role": "member"})
	r := wsScopeRouter(http.MethodPost, "/api/v1/workspaces/:id/invites", CreateWorkspaceInvite)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/workspaces/"+wsScopeWS+"/invites", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", w.Code, w.Body.String())
	}
	var resp WorkspaceInvite
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Token == "" {
		t.Fatalf("expected the minted token to be returned to the inviter; got empty")
	}
	if resp.Email != wsInviteEmail || resp.Role != "member" || resp.Status != "pending" {
		t.Fatalf("unexpected invite payload: %+v", resp)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

func TestCreateWorkspaceInvite_MemberForbidden(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	// A plain member may not invite — gate returns "member", below admin.
	wsParamGate(mock, "member")

	body, _ := json.Marshal(map[string]any{"email": wsInviteEmail, "role": "member"})
	r := wsScopeRouter(http.MethodPost, "/api/v1/workspaces/:id/invites", CreateWorkspaceInvite)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/workspaces/"+wsScopeWS+"/invites", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

func TestCreateWorkspaceInvite_NonMemberNotFound(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	// Caller is not a member of the :id-param workspace → gate 404 (never
	// confirm existence to a non-member).
	mock.ExpectQuery(`SELECT role FROM workspace_members WHERE workspace_id = \$1 AND user_id = \$2`).
		WithArgs(wsScopeWS, wsScopeUser).
		WillReturnError(sql.ErrNoRows)

	body, _ := json.Marshal(map[string]any{"email": wsInviteEmail, "role": "member"})
	r := wsScopeRouter(http.MethodPost, "/api/v1/workspaces/:id/invites", CreateWorkspaceInvite)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/workspaces/"+wsScopeWS+"/invites", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCreateWorkspaceInvite_RejectsOwnerRole(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	// Gate passes (admin) but role=owner is never invitable.
	wsParamGate(mock, "admin")
	expectIsPersonal(mock, false)

	body, _ := json.Marshal(map[string]any{"email": wsInviteEmail, "role": "owner"})
	r := wsScopeRouter(http.MethodPost, "/api/v1/workspaces/:id/invites", CreateWorkspaceInvite)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/workspaces/"+wsScopeWS+"/invites", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCreateWorkspaceInvite_AlreadyMemberConflict(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)

	wsParamGate(mock, "admin")
	expectIsPersonal(mock, false)

	// The invitee is already a member → 409, no invite minted.
	mock.ExpectQuery(`SELECT 1 FROM workspace_members wm JOIN users u`).
		WithArgs(wsScopeWS, wsInviteEmail).
		WillReturnRows(sqlmock.NewRows([]string{"?column?"}).AddRow(1))

	body, _ := json.Marshal(map[string]any{"email": wsInviteEmail, "role": "member"})
	r := wsScopeRouter(http.MethodPost, "/api/v1/workspaces/:id/invites", CreateWorkspaceInvite)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/workspaces/"+wsScopeWS+"/invites", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

func TestListWorkspaceInvites_MemberCanList(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)

	// List is open to any member.
	wsParamGate(mock, "member")

	cols := []string{"id", "workspace_id", "email", "role", "status", "invited_by", "expires_at", "accepted_at", "created_at"}
	mock.ExpectQuery(`FROM workspace_invites WHERE workspace_id = \$1`).
		WithArgs(wsScopeWS).
		WillReturnRows(sqlmock.NewRows(cols).
			AddRow(wsInviteID, wsScopeWS, wsInviteEmail, "member", "pending", wsScopeUser,
				time.Now().Add(7*24*time.Hour), nil, time.Now()))

	r := wsScopeRouter(http.MethodGet, "/api/v1/workspaces/:id/invites", ListWorkspaceInvites)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/workspaces/"+wsScopeWS+"/invites", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp struct {
		Invites []WorkspaceInvite `json:"invites"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(resp.Invites) != 1 || resp.Invites[0].Email != wsInviteEmail {
		t.Fatalf("unexpected invites: %+v", resp.Invites)
	}
	// The bearer token must NEVER be exposed in a list any member can read.
	if resp.Invites[0].Token != "" {
		t.Fatalf("token must not be returned in the list response")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

func TestRevokeWorkspaceInvite_AdminCanRevoke(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)

	wsParamGate(mock, "admin")

	// Revoke is workspace-scoped (id AND workspace_id) and pending-only.
	mock.ExpectExec(`UPDATE workspace_invites SET status = 'revoked' WHERE id = \$1 AND workspace_id = \$2 AND status = 'pending'`).
		WithArgs(wsInviteID, wsScopeWS).
		WillReturnResult(sqlmock.NewResult(0, 1))

	r := wsScopeRouter(http.MethodDelete, "/api/v1/workspaces/:id/invites/:inviteId", RevokeWorkspaceInvite)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodDelete, "/api/v1/workspaces/"+wsScopeWS+"/invites/"+wsInviteID, nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

func TestRevokeWorkspaceInvite_NotFound(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)

	wsParamGate(mock, "admin")

	// No pending invite matched (wrong workspace, already accepted/revoked, or
	// unknown id) → 0 rows → 404.
	mock.ExpectExec(`UPDATE workspace_invites SET status = 'revoked' WHERE id = \$1 AND workspace_id = \$2 AND status = 'pending'`).
		WithArgs(wsInviteID, wsScopeWS).
		WillReturnResult(sqlmock.NewResult(0, 0))

	r := wsScopeRouter(http.MethodDelete, "/api/v1/workspaces/:id/invites/:inviteId", RevokeWorkspaceInvite)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodDelete, "/api/v1/workspaces/"+wsScopeWS+"/invites/"+wsInviteID, nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}
