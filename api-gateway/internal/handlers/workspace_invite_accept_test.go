package handlers

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// P3b: invite acceptance — the onboarding half of the invite lifecycle.
//   - GetWorkspaceInvite (public, by token): preview an invite before signing in.
//   - AcceptWorkspaceInvite (authed): claim the invite. Three non-negotiables —
//     (1) email-bind HARD CHECK (the accepting user's email must equal the invite
//     email, server-side), (2) single-use via an atomic UPDATE ... WHERE
//     status='pending' AND expires_at > NOW() RETURNING (so concurrent accepts
//     race on one row), (3) expired → 410.
//
// wsScopeUser / wsScopeWS / wsScopeRouter / wsScopeMockDB / wsInviteEmail come
// from the sibling test files in this package.

const wsInviteToken = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestGetWorkspaceInvite_Pending(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	mock.ExpectQuery(`FROM workspace_invites i JOIN workspaces w`).
		WithArgs(wsInviteToken).
		WillReturnRows(sqlmock.NewRows([]string{"workspace_id", "email", "role", "status", "expires_at", "name"}).
			AddRow(wsScopeWS, wsInviteEmail, "member", "pending", time.Now().Add(7*24*time.Hour), "Acme Inc"))

	r := wsScopeRouter(http.MethodGet, "/api/v1/workspace-invites/:token", GetWorkspaceInvite)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/workspace-invites/"+wsInviteToken, nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	body := w.Body.String()
	if !strings.Contains(body, "Acme Inc") || !strings.Contains(body, wsInviteEmail) {
		t.Fatalf("expected workspace name + invitee email in preview, got %s", body)
	}
	if strings.Contains(body, wsInviteToken) {
		t.Fatalf("preview must not echo the token back")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

func TestGetWorkspaceInvite_NotFound(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	mock.ExpectQuery(`FROM workspace_invites i JOIN workspaces w`).
		WithArgs(wsInviteToken).
		WillReturnRows(sqlmock.NewRows([]string{"workspace_id", "email", "role", "status", "expires_at", "name"}))

	r := wsScopeRouter(http.MethodGet, "/api/v1/workspace-invites/:token", GetWorkspaceInvite)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/workspace-invites/"+wsInviteToken, nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestGetWorkspaceInvite_Expired(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	mock.ExpectQuery(`FROM workspace_invites i JOIN workspaces w`).
		WithArgs(wsInviteToken).
		WillReturnRows(sqlmock.NewRows([]string{"workspace_id", "email", "role", "status", "expires_at", "name"}).
			AddRow(wsScopeWS, wsInviteEmail, "member", "pending", time.Now().Add(-1*time.Hour), "Acme Inc"))

	r := wsScopeRouter(http.MethodGet, "/api/v1/workspace-invites/:token", GetWorkspaceInvite)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/workspace-invites/"+wsInviteToken, nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusGone {
		t.Fatalf("expected 410, got %d: %s", w.Code, w.Body.String())
	}
}

func TestAcceptWorkspaceInvite_Success(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	// The accepting user's authoritative email matches the invite email.
	mock.ExpectQuery(`SELECT email FROM users WHERE id = \$1`).
		WithArgs(wsScopeUser).
		WillReturnRows(sqlmock.NewRows([]string{"email"}).AddRow(wsInviteEmail))

	mock.ExpectBegin()
	// Atomic single-use claim: pending + not expired, RETURNING the target.
	mock.ExpectQuery(`UPDATE workspace_invites SET status = 'accepted'`).
		WithArgs(wsScopeUser, wsInviteToken).
		WillReturnRows(sqlmock.NewRows([]string{"workspace_id", "email", "role"}).
			AddRow(wsScopeWS, wsInviteEmail, "member"))
	// Membership granted (idempotent against the unique (workspace_id,user_id)).
	mock.ExpectExec(`INSERT INTO workspace_members`).
		WithArgs(wsScopeWS, wsScopeUser, "member").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	r := wsScopeRouter(http.MethodPost, "/api/v1/workspace-invites/:token/accept", AcceptWorkspaceInvite)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/workspace-invites/"+wsInviteToken+"/accept", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), wsScopeWS) {
		t.Fatalf("expected the joined workspace id in the response, got %s", w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

func TestAcceptWorkspaceInvite_EmailMismatch(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	// The accepting user's email differs from the invite email → must NOT join.
	mock.ExpectQuery(`SELECT email FROM users WHERE id = \$1`).
		WithArgs(wsScopeUser).
		WillReturnRows(sqlmock.NewRows([]string{"email"}).AddRow("stranger@example.com"))

	mock.ExpectBegin()
	mock.ExpectQuery(`UPDATE workspace_invites SET status = 'accepted'`).
		WithArgs(wsScopeUser, wsInviteToken).
		WillReturnRows(sqlmock.NewRows([]string{"workspace_id", "email", "role"}).
			AddRow(wsScopeWS, wsInviteEmail, "member"))
	// The claim is rolled back — the invite stays pending for the right person.
	mock.ExpectRollback()

	r := wsScopeRouter(http.MethodPost, "/api/v1/workspace-invites/:token/accept", AcceptWorkspaceInvite)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/workspace-invites/"+wsInviteToken+"/accept", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

func TestAcceptWorkspaceInvite_Expired(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	mock.ExpectQuery(`SELECT email FROM users WHERE id = \$1`).
		WithArgs(wsScopeUser).
		WillReturnRows(sqlmock.NewRows([]string{"email"}).AddRow(wsInviteEmail))

	mock.ExpectBegin()
	// Claim matches 0 rows (pending+not-expired predicate fails).
	mock.ExpectQuery(`UPDATE workspace_invites SET status = 'accepted'`).
		WithArgs(wsScopeUser, wsInviteToken).
		WillReturnRows(sqlmock.NewRows([]string{"workspace_id", "email", "role"}))
	// Disambiguation: still pending but past expiry → expired.
	mock.ExpectQuery(`SELECT status, expires_at FROM workspace_invites WHERE token = \$1`).
		WithArgs(wsInviteToken).
		WillReturnRows(sqlmock.NewRows([]string{"status", "expires_at"}).
			AddRow("pending", time.Now().Add(-1*time.Hour)))
	mock.ExpectRollback()

	r := wsScopeRouter(http.MethodPost, "/api/v1/workspace-invites/:token/accept", AcceptWorkspaceInvite)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/workspace-invites/"+wsInviteToken+"/accept", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusGone {
		t.Fatalf("expected 410, got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

func TestAcceptWorkspaceInvite_AlreadyUsed(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	mock.ExpectQuery(`SELECT email FROM users WHERE id = \$1`).
		WithArgs(wsScopeUser).
		WillReturnRows(sqlmock.NewRows([]string{"email"}).AddRow(wsInviteEmail))

	mock.ExpectBegin()
	mock.ExpectQuery(`UPDATE workspace_invites SET status = 'accepted'`).
		WithArgs(wsScopeUser, wsInviteToken).
		WillReturnRows(sqlmock.NewRows([]string{"workspace_id", "email", "role"}))
	// Disambiguation: already accepted/revoked → 409.
	mock.ExpectQuery(`SELECT status, expires_at FROM workspace_invites WHERE token = \$1`).
		WithArgs(wsInviteToken).
		WillReturnRows(sqlmock.NewRows([]string{"status", "expires_at"}).
			AddRow("accepted", time.Now().Add(24*time.Hour)))
	mock.ExpectRollback()

	r := wsScopeRouter(http.MethodPost, "/api/v1/workspace-invites/:token/accept", AcceptWorkspaceInvite)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/workspace-invites/"+wsInviteToken+"/accept", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

func TestAcceptWorkspaceInvite_NotFound(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	mock.ExpectQuery(`SELECT email FROM users WHERE id = \$1`).
		WithArgs(wsScopeUser).
		WillReturnRows(sqlmock.NewRows([]string{"email"}).AddRow(wsInviteEmail))

	mock.ExpectBegin()
	mock.ExpectQuery(`UPDATE workspace_invites SET status = 'accepted'`).
		WithArgs(wsScopeUser, wsInviteToken).
		WillReturnRows(sqlmock.NewRows([]string{"workspace_id", "email", "role"}))
	// Disambiguation: token unknown → 404.
	mock.ExpectQuery(`SELECT status, expires_at FROM workspace_invites WHERE token = \$1`).
		WithArgs(wsInviteToken).
		WillReturnRows(sqlmock.NewRows([]string{"status", "expires_at"}))
	mock.ExpectRollback()

	r := wsScopeRouter(http.MethodPost, "/api/v1/workspace-invites/:token/accept", AcceptWorkspaceInvite)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/workspace-invites/"+wsInviteToken+"/accept", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}
