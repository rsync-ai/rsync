package handlers

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// P3.9: workspace-invite email + shareable accept URL. buildInviteAcceptURL builds
// the frontend invite-landing link from APP_BASE_URL; CreateWorkspaceInvite echoes
// it as accept_url so the inviter can share the link even when email delivery is
// off (Resend unconfigured). The async email send itself is gated on
// emailClient.IsConfigured() (false in tests) so it never runs here — the email
// builder is covered in the email package's resend_test.go.
//
// wsScope* / wsParamGate / wsInviteEmail / wsInviteID come from sibling test files.

func TestBuildInviteAcceptURL(t *testing.T) {
	const token = "abc123token"

	t.Setenv("APP_BASE_URL", "https://app.rsync.ai")
	if got := buildInviteAcceptURL(token); got != "https://app.rsync.ai/invite/"+token {
		t.Fatalf("with APP_BASE_URL: got %q", got)
	}

	// A trailing slash on the base must not double up.
	t.Setenv("APP_BASE_URL", "https://app.rsync.ai/")
	if got := buildInviteAcceptURL(token); got != "https://app.rsync.ai/invite/"+token {
		t.Fatalf("trailing slash: got %q", got)
	}

	// Unset → localhost default (mirrors auth.go email links).
	t.Setenv("APP_BASE_URL", "")
	if got := buildInviteAcceptURL(token); got != "http://localhost:3000/invite/"+token {
		t.Fatalf("default base: got %q", got)
	}
}

func TestCreateWorkspaceInvite_ReturnsAcceptURL(t *testing.T) {
	t.Setenv("APP_BASE_URL", "https://app.rsync.ai")
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)

	wsParamGate(mock, "admin")
	expectIsPersonal(mock, false)
	mock.ExpectQuery(`SELECT 1 FROM workspace_members wm JOIN users u`).
		WithArgs(wsScopeWS, wsInviteEmail).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery(`SELECT 1 FROM workspace_invites\s+WHERE workspace_id = \$1 AND lower\(email\)`).
		WithArgs(wsScopeWS, wsInviteEmail).
		WillReturnError(sql.ErrNoRows)
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
	// accept_url must be present and embed the minted token, so the inviter can
	// share the link directly when email is disabled.
	if resp.AcceptURL == "" || !strings.HasSuffix(resp.AcceptURL, "/invite/"+resp.Token) {
		t.Fatalf("expected accept_url ending in /invite/<token>; got %q (token=%q)", resp.AcceptURL, resp.Token)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

// sendWorkspaceInviteEmail is the extracted goroutine body that CreateWorkspaceInvite
// dispatches for the (gated, best-effort) invite email. This pins the wiring the
// handler test cannot reach (emailClient is unconfigured in tests): the workspace-
// name lookup is issued and the args land in the right SendWorkspaceInvite slots.
// Catches arg-swap, wsName→wsID, and dropped-lookup mutations.
func TestSendWorkspaceInviteEmail_WiresArgsAndFetchesName(t *testing.T) {
	dbConn, mock, cleanup := wsSignupMockDB(t)
	defer cleanup()
	mock.ExpectQuery(`SELECT name FROM workspaces WHERE id = \$1`).
		WithArgs(wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow("Acme Analytics"))

	var got struct {
		to, ws, role, url string
		called            bool
	}
	stub := func(ctx context.Context, toEmail, workspaceName, role, acceptURL string) error {
		got.to, got.ws, got.role, got.url, got.called = toEmail, workspaceName, role, acceptURL, true
		return nil
	}

	sendWorkspaceInviteEmail(dbConn, stub, "invitee@example.com", "member", "https://app.rsync.ai/invite/TOK", wsScopeWS)

	if !got.called {
		t.Fatal("expected the invite sender to be called")
	}
	if got.to != "invitee@example.com" || got.ws != "Acme Analytics" ||
		got.role != "member" || got.url != "https://app.rsync.ai/invite/TOK" {
		t.Fatalf("mis-wired invite-email args: %+v", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}
