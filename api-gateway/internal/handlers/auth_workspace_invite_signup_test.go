package handlers

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"

	"api-gateway/internal/db"
)

// P3.8: signup-with-invite. A brand-new user who registers carrying a workspace
// team-invite token (workspace_invites, distinct from the instance-level
// `invitations` registration gate) is joined to the invited workspace with the
// invited role. Two pure helpers carry the logic so it is testable without
// standing up the whole Register fan-out:
//
//   - validateWorkspaceInviteForSignup: read-only, pre-account-creation. Rejects a
//     bad/foreign invite up-front (returns a problem code) so no half-provisioned
//     user is left behind. Empty token is a no-op. Email-bind is a hard check.
//   - acceptWorkspaceInviteAtSignup: post-creation, atomic single-use claim +
//     membership insert in one transaction (mirrors AcceptWorkspaceInvite). A race
//     that consumed the invite first is a benign no-join, not an error.
//
// wsScopeWS / wsScopeUser / wsInviteToken / wsInviteEmail / errTestDBDown come from
// sibling test files in this package.

func wsSignupMockDB(t *testing.T) (*sql.DB, sqlmock.Sqlmock, func()) {
	t.Helper()
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	return mockDB, mock, func() { _ = mockDB.Close() }
}

// ---- validateWorkspaceInviteForSignup ----

func TestValidateWorkspaceInviteForSignup_EmptyTokenNoOp(t *testing.T) {
	dbConn, mock, cleanup := wsSignupMockDB(t)
	defer cleanup()
	// No query should run for an empty token.
	problem, err := validateWorkspaceInviteForSignup(dbConn, "", wsInviteEmail)
	if err != nil || problem != "" {
		t.Fatalf("empty token: want ('',nil), got (%q,%v)", problem, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unexpected query for empty token: %v", err)
	}
}

func TestValidateWorkspaceInviteForSignup_Valid(t *testing.T) {
	dbConn, mock, cleanup := wsSignupMockDB(t)
	defer cleanup()
	mock.ExpectQuery(`SELECT lower\(email\), status, expires_at FROM workspace_invites WHERE token = \$1`).
		WithArgs(wsInviteToken).
		WillReturnRows(sqlmock.NewRows([]string{"email", "status", "expires_at"}).
			AddRow(wsInviteEmail, "pending", time.Now().Add(time.Hour)))

	problem, err := validateWorkspaceInviteForSignup(dbConn, wsInviteToken, wsInviteEmail)
	if err != nil || problem != "" {
		t.Fatalf("valid invite: want ('',nil), got (%q,%v)", problem, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

func TestValidateWorkspaceInviteForSignup_NotFound(t *testing.T) {
	dbConn, mock, cleanup := wsSignupMockDB(t)
	defer cleanup()
	mock.ExpectQuery(`SELECT lower\(email\), status, expires_at FROM workspace_invites WHERE token = \$1`).
		WithArgs(wsInviteToken).
		WillReturnError(sql.ErrNoRows)

	problem, err := validateWorkspaceInviteForSignup(dbConn, wsInviteToken, wsInviteEmail)
	if err != nil || problem != wsInviteProblemNotFound {
		t.Fatalf("not found: want (%q,nil), got (%q,%v)", wsInviteProblemNotFound, problem, err)
	}
}

func TestValidateWorkspaceInviteForSignup_NonPending(t *testing.T) {
	dbConn, mock, cleanup := wsSignupMockDB(t)
	defer cleanup()
	mock.ExpectQuery(`SELECT lower\(email\), status, expires_at FROM workspace_invites WHERE token = \$1`).
		WithArgs(wsInviteToken).
		WillReturnRows(sqlmock.NewRows([]string{"email", "status", "expires_at"}).
			AddRow(wsInviteEmail, "accepted", time.Now().Add(time.Hour)))

	problem, err := validateWorkspaceInviteForSignup(dbConn, wsInviteToken, wsInviteEmail)
	if err != nil || problem != wsInviteProblemUnusable {
		t.Fatalf("non-pending: want (%q,nil), got (%q,%v)", wsInviteProblemUnusable, problem, err)
	}
}

func TestValidateWorkspaceInviteForSignup_Expired(t *testing.T) {
	dbConn, mock, cleanup := wsSignupMockDB(t)
	defer cleanup()
	mock.ExpectQuery(`SELECT lower\(email\), status, expires_at FROM workspace_invites WHERE token = \$1`).
		WithArgs(wsInviteToken).
		WillReturnRows(sqlmock.NewRows([]string{"email", "status", "expires_at"}).
			AddRow(wsInviteEmail, "pending", time.Now().Add(-time.Hour)))

	problem, err := validateWorkspaceInviteForSignup(dbConn, wsInviteToken, wsInviteEmail)
	if err != nil || problem != wsInviteProblemExpired {
		t.Fatalf("expired: want (%q,nil), got (%q,%v)", wsInviteProblemExpired, problem, err)
	}
}

func TestValidateWorkspaceInviteForSignup_EmailMismatch(t *testing.T) {
	dbConn, mock, cleanup := wsSignupMockDB(t)
	defer cleanup()
	mock.ExpectQuery(`SELECT lower\(email\), status, expires_at FROM workspace_invites WHERE token = \$1`).
		WithArgs(wsInviteToken).
		WillReturnRows(sqlmock.NewRows([]string{"email", "status", "expires_at"}).
			AddRow(wsInviteEmail, "pending", time.Now().Add(time.Hour)))

	problem, err := validateWorkspaceInviteForSignup(dbConn, wsInviteToken, "someone-else@example.com")
	if err != nil || problem != wsInviteProblemEmailMismatch {
		t.Fatalf("mismatch: want (%q,nil), got (%q,%v)", wsInviteProblemEmailMismatch, problem, err)
	}
}

func TestValidateWorkspaceInviteForSignup_DBError(t *testing.T) {
	dbConn, mock, cleanup := wsSignupMockDB(t)
	defer cleanup()
	mock.ExpectQuery(`SELECT lower\(email\), status, expires_at FROM workspace_invites WHERE token = \$1`).
		WithArgs(wsInviteToken).
		WillReturnError(errTestDBDown)

	problem, err := validateWorkspaceInviteForSignup(dbConn, wsInviteToken, wsInviteEmail)
	if err == nil || problem != "" {
		t.Fatalf("db error: want ('',err), got (%q,%v)", problem, err)
	}
}

// ---- acceptWorkspaceInviteAtSignup ----

func expectSignupClaim(mock sqlmock.Sqlmock) *sqlmock.ExpectedQuery {
	return mock.ExpectQuery(`UPDATE workspace_invites SET status = 'accepted'.*WHERE token = \$2 AND status = 'pending' AND expires_at > NOW\(\) AND lower\(email\) = lower\(\$3\)\s+RETURNING workspace_id, role`).
		WithArgs(wsScopeUser, wsInviteToken, wsInviteEmail)
}

func TestAcceptWorkspaceInviteAtSignup_Success(t *testing.T) {
	dbConn, mock, cleanup := wsSignupMockDB(t)
	defer cleanup()
	mock.ExpectBegin()
	expectSignupClaim(mock).
		WillReturnRows(sqlmock.NewRows([]string{"workspace_id", "role"}).AddRow(wsScopeWS, "member"))
	mock.ExpectExec(`INSERT INTO workspace_members \(workspace_id, user_id, role\) VALUES \(\$1, \$2, \$3\) ON CONFLICT \(workspace_id, user_id\) DO NOTHING`).
		WithArgs(wsScopeWS, wsScopeUser, "member").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	wsID, role, err := acceptWorkspaceInviteAtSignup(dbConn, wsInviteToken, wsScopeUser, wsInviteEmail)
	if err != nil || wsID != wsScopeWS || role != "member" {
		t.Fatalf("success: want (%q,member,nil), got (%q,%q,%v)", wsScopeWS, wsID, role, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

func TestAcceptWorkspaceInviteAtSignup_AlreadyConsumed(t *testing.T) {
	dbConn, mock, cleanup := wsSignupMockDB(t)
	defer cleanup()
	mock.ExpectBegin()
	expectSignupClaim(mock).WillReturnError(sql.ErrNoRows) // race: gone/expired/email drift
	mock.ExpectRollback()

	wsID, role, err := acceptWorkspaceInviteAtSignup(dbConn, wsInviteToken, wsScopeUser, wsInviteEmail)
	if err != nil || wsID != "" || role != "" {
		t.Fatalf("consumed: want ('','',nil), got (%q,%q,%v)", wsID, role, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

func TestAcceptWorkspaceInviteAtSignup_MemberInsertError(t *testing.T) {
	dbConn, mock, cleanup := wsSignupMockDB(t)
	defer cleanup()
	mock.ExpectBegin()
	expectSignupClaim(mock).
		WillReturnRows(sqlmock.NewRows([]string{"workspace_id", "role"}).AddRow(wsScopeWS, "member"))
	mock.ExpectExec(`INSERT INTO workspace_members`).
		WithArgs(wsScopeWS, wsScopeUser, "member").
		WillReturnError(errTestDBDown)
	mock.ExpectRollback()

	wsID, role, err := acceptWorkspaceInviteAtSignup(dbConn, wsInviteToken, wsScopeUser, wsInviteEmail)
	if err == nil || wsID != "" || role != "" {
		t.Fatalf("insert error: want ('','',err), got (%q,%q,%v)", wsID, role, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

func TestAcceptWorkspaceInviteAtSignup_BeginError(t *testing.T) {
	dbConn, mock, cleanup := wsSignupMockDB(t)
	defer cleanup()
	mock.ExpectBegin().WillReturnError(errTestDBDown)

	wsID, role, err := acceptWorkspaceInviteAtSignup(dbConn, wsInviteToken, wsScopeUser, wsInviteEmail)
	if err == nil || wsID != "" || role != "" {
		t.Fatalf("begin error: want ('','',err), got (%q,%q,%v)", wsID, role, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

// ---- Register wiring (the helpers are exercised through the real handler) ----
//
// These cover the load-bearing wiring that the isolated helper tests cannot: the
// ?workspace_invite query-param read, the up-front reject status-code mapping
// (400/403/500), and the post-creation join call. Mirrors the httptest+gin pattern
// AcceptWorkspaceInvite (P3b) uses. The reject paths short-circuit right after
// validation, so they need only two mocked queries (registration_mode + the invite
// lookup) — no account is created.

func registerRouter(mockDB *sql.DB) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	h := &AuthHandler{db: mockDB}
	r.POST("/api/v1/auth/register", h.Register)
	return r
}

func registerReq(workspaceInvite, email, password string) *http.Request {
	body, _ := json.Marshal(map[string]string{"email": email, "password": password})
	url := "/api/v1/auth/register"
	if workspaceInvite != "" {
		url += "?workspace_invite=" + workspaceInvite
	}
	req := httptest.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	return req
}

// expectRegistrationMode stubs getInstanceSetting("registration_mode") → default
// "open" (ErrNoRows), the first query every Register makes.
func expectRegistrationMode(mock sqlmock.Sqlmock) {
	mock.ExpectQuery(`SELECT value FROM instance_settings WHERE key = \$1`).
		WithArgs("registration_mode").
		WillReturnError(sql.ErrNoRows)
}

func expectInviteValidation(mock sqlmock.Sqlmock) *sqlmock.ExpectedQuery {
	return mock.ExpectQuery(`SELECT lower\(email\), status, expires_at FROM workspace_invites WHERE token = \$1`).
		WithArgs(wsInviteToken)
}

func assertRegisterReject(t *testing.T, wantCode int, wantErr string, configure func(sqlmock.Sqlmock)) {
	t.Helper()
	dbConn, mock, cleanup := wsSignupMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)
	expectRegistrationMode(mock)
	configure(mock)

	resp := httptest.NewRecorder()
	registerRouter(dbConn).ServeHTTP(resp, registerReq(wsInviteToken, wsInviteEmail, "password123"))

	if resp.Code != wantCode {
		t.Fatalf("expected %d, got %d: %s", wantCode, resp.Code, resp.Body.String())
	}
	if !strings.Contains(resp.Body.String(), wantErr) {
		t.Fatalf("expected error %q, got %s", wantErr, resp.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

func TestRegister_WorkspaceInvite_UnknownToken(t *testing.T) {
	assertRegisterReject(t, http.StatusBadRequest, wsInviteProblemNotFound, func(m sqlmock.Sqlmock) {
		expectInviteValidation(m).WillReturnError(sql.ErrNoRows)
	})
}

func TestRegister_WorkspaceInvite_Used(t *testing.T) {
	assertRegisterReject(t, http.StatusBadRequest, wsInviteProblemUnusable, func(m sqlmock.Sqlmock) {
		expectInviteValidation(m).WillReturnRows(sqlmock.NewRows([]string{"email", "status", "expires_at"}).
			AddRow(wsInviteEmail, "accepted", time.Now().Add(time.Hour)))
	})
}

func TestRegister_WorkspaceInvite_Expired(t *testing.T) {
	assertRegisterReject(t, http.StatusBadRequest, wsInviteProblemExpired, func(m sqlmock.Sqlmock) {
		expectInviteValidation(m).WillReturnRows(sqlmock.NewRows([]string{"email", "status", "expires_at"}).
			AddRow(wsInviteEmail, "pending", time.Now().Add(-time.Hour)))
	})
}

func TestRegister_WorkspaceInvite_EmailMismatch(t *testing.T) {
	// Invite addressed to a DIFFERENT email than the signup email → 403, before any
	// account is created. Pins both the 403 mapping AND that req.Email is the value
	// checked.
	assertRegisterReject(t, http.StatusForbidden, wsInviteProblemEmailMismatch, func(m sqlmock.Sqlmock) {
		expectInviteValidation(m).WillReturnRows(sqlmock.NewRows([]string{"email", "status", "expires_at"}).
			AddRow("someone-else@example.com", "pending", time.Now().Add(time.Hour)))
	})
}

func TestRegister_WorkspaceInvite_ValidationDBError(t *testing.T) {
	assertRegisterReject(t, http.StatusInternalServerError, "Database error", func(m sqlmock.Sqlmock) {
		expectInviteValidation(m).WillReturnError(errTestDBDown)
	})
}

// TestRegister_WorkspaceInvite_HappyPathJoins drives a full successful signup with a
// valid invite and asserts the post-creation join is wired: the atomic claim +
// membership insert mock expectations MUST be met, proving the join block actually
// runs (a deleted/!"" join would leave these unmet). emailClient is unconfigured in
// tests (no RESEND_API_KEY) so the async onboarding goroutines are skipped; logAudit
// uses the global DB which we pin nil → no-op.
func TestRegister_WorkspaceInvite_HappyPathJoins(t *testing.T) {
	const personalWS = "88888888-8888-8888-8888-888888888888"
	dbConn, mock, cleanup := wsSignupMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)

	// logAudit reads the global DB; pin it nil so it no-ops deterministically.
	prevGlobal := db.DB
	db.DB = nil
	defer func() { db.DB = prevGlobal }()

	expectRegistrationMode(mock)
	expectInviteValidation(mock).WillReturnRows(sqlmock.NewRows([]string{"email", "status", "expires_at"}).
		AddRow(wsInviteEmail, "pending", time.Now().Add(time.Hour)))
	mock.ExpectQuery(`SELECT EXISTS`).WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM users`).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectQuery(`SELECT duration_days FROM plans WHERE name = 'free'`).
		WillReturnRows(sqlmock.NewRows([]string{"duration_days"}).AddRow(30))
	mock.ExpectExec(`INSERT INTO users`).WillReturnResult(sqlmock.NewResult(0, 1))
	// provisionPersonalWorkspace: workspace + owner membership.
	mock.ExpectQuery(`INSERT INTO workspaces .* RETURNING id`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(personalWS))
	mock.ExpectExec(`INSERT INTO workspace_members \(workspace_id, user_id, role\)\s+VALUES \(\$1, \$2, 'owner'\)`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	// The team-invite join — the wiring under test.
	mock.ExpectBegin()
	mock.ExpectQuery(`UPDATE workspace_invites SET status = 'accepted'.*RETURNING workspace_id, role`).
		WillReturnRows(sqlmock.NewRows([]string{"workspace_id", "role"}).AddRow(wsScopeWS, "member"))
	mock.ExpectExec(`INSERT INTO workspace_members .* ON CONFLICT \(workspace_id, user_id\) DO NOTHING`).
		WithArgs(wsScopeWS, sqlmock.AnyArg(), "member").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	mock.ExpectExec(`INSERT INTO sessions`).WillReturnResult(sqlmock.NewResult(0, 1))

	resp := httptest.NewRecorder()
	registerRouter(dbConn).ServeHTTP(resp, registerReq(wsInviteToken, wsInviteEmail, "password123"))

	if resp.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", resp.Code, resp.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations (join not wired?): %v", err)
	}
}
