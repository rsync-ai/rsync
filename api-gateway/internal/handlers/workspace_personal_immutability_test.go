package handlers

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// RBAC P1 (B3): a user's auto-provisioned PERSONAL workspace is immutable — it
// cannot be renamed or gain members. DeleteWorkspace already 409s on a personal
// workspace; UpdateWorkspace (rename) and CreateWorkspaceInvite did not. (A
// personal workspace's membership is already protected: it holds exactly one
// owner, and RemoveMember's last-owner guard 409s a sole-owner self-leave.)
//
// wsScopeUser / wsScopeWS / wsScopeMockDB / wsScopeRouter / wsParamGate /
// wsInviteEmail come from sibling *_test.go files (same package).

// expectIsPersonal stamps the is_personal lookup requireNonPersonalWorkspace
// performs for the :id-param workspace.
func expectIsPersonal(mock sqlmock.Sqlmock, personal bool) {
	mock.ExpectQuery(`SELECT is_personal FROM workspaces WHERE id = \$1`).
		WithArgs(wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"is_personal"}).AddRow(personal))
}

func TestUpdateWorkspace_PersonalWorkspaceRejected(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	// Owner gate passes, then the workspace is found to be personal → 409, and the
	// rename UPDATE is never issued (not mocked; would error if reached).
	wsParamGate(mock, "owner")
	expectIsPersonal(mock, true)

	body, _ := json.Marshal(map[string]any{"name": "Renamed Personal"})
	r := wsScopeRouter(http.MethodPatch, "/api/v1/workspaces/:id", UpdateWorkspace)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPatch, "/api/v1/workspaces/"+wsScopeWS, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusConflict || !strings.Contains(w.Body.String(), "personal workspace") {
		t.Fatalf("renaming a personal workspace must 409; got %d: %s", w.Code, w.Body.String())
	}
}

func TestCreateWorkspaceInvite_PersonalWorkspaceRejected(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	// Admin gate passes, then the workspace is found to be personal → 409, and no
	// invite is minted (the membership/INSERT queries are never mocked).
	wsParamGate(mock, "admin")
	expectIsPersonal(mock, true)

	body, _ := json.Marshal(map[string]any{"email": wsInviteEmail, "role": "member"})
	r := wsScopeRouter(http.MethodPost, "/api/v1/workspaces/:id/invites", CreateWorkspaceInvite)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/workspaces/"+wsScopeWS+"/invites", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusConflict || !strings.Contains(w.Body.String(), "personal workspace") {
		t.Fatalf("inviting to a personal workspace must 409; got %d: %s", w.Code, w.Body.String())
	}
}
