package handlers

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
	"golang.org/x/crypto/bcrypt"

	"api-gateway/internal/identity"
)

// Login goes through the identity extension point, and every way that point can
// fail has to end with no session. A denial that still mints one is not a denial.
//
// The probe below exists because the obvious test here is a fiction. sqlmock
// treats an *unexpected* query as an error the caller sees, not as an unmet
// expectation, so `ExpectationsWereMet` stays nil even when the handler ran a
// query the test never set up. A fallback to the local password provider would
// therefore have slipped straight through an assertion that looked like it was
// watching for one. credentialProbe watches the thing itself: whether the
// provider's password read happened at all.

// credentialProbe records whether the local provider's password read ran. It is a
// sqlmock.Argument, so it is consulted exactly when that query matches.
type credentialProbe struct{ ran bool }

func (p *credentialProbe) Match(driver.Value) bool {
	p.ran = true
	return true
}

func expectAccountRow(mock sqlmock.Sqlmock, status string) {
	mock.ExpectQuery(`SELECT id, email, role`).
		WithArgs("user@example.com").
		WillReturnRows(sqlmock.NewRows(
			[]string{"id", "email", "role", "status", "name", "email_verified"},
		).AddRow("user-1", "user@example.com", "admin", status, "Test", true))
}

// expectSuccessfulCredentialRead arms the test with everything a completed login
// needs: the stored hash of the password that will be sent, plus the session
// insert and the last-login stamp. Nothing here is required to be consumed. It is
// there so that a handler which wrongly falls through to the local provider
// SUCCEEDS -- 200, with a cookie -- and the assertions can tell that apart from a
// denial. A test whose failure mode and success mode both end in 503 proves
// nothing.
func expectSuccessfulCredentialRead(t *testing.T, mock sqlmock.Sqlmock, probe *credentialProbe) {
	t.Helper()
	hash, err := bcrypt.GenerateFromPassword([]byte("correct-horse"), bcrypt.MinCost)
	if err != nil {
		t.Fatalf("bcrypt: %v", err)
	}
	mock.ExpectQuery(`SELECT password_hash FROM users`).
		WithArgs(probe).
		WillReturnRows(sqlmock.NewRows([]string{"password_hash"}).AddRow(string(hash)))
	mock.ExpectExec(`INSERT INTO sessions`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`UPDATE users SET last_login_at = NOW\(\)`).
		WillReturnResult(sqlmock.NewResult(0, 1))
}

func postLogin(t *testing.T, h *AuthHandler, body string) *httptest.ResponseRecorder {
	t.Helper()
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", strings.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")
	h.Login(c)
	return w
}

func assertNoSession(t *testing.T, w *httptest.ResponseRecorder) {
	t.Helper()
	for _, ck := range w.Result().Cookies() {
		if ck.Name == "auth_token" && ck.Value != "" {
			t.Fatalf("a denied login set an auth_token cookie (%q) -- the denial minted a credential", ck.Value)
		}
	}
	if strings.Contains(w.Body.String(), `"token"`) {
		t.Fatalf("a denied login returned a token in its body: %s", w.Body.String())
	}
}

func newMock(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	dbConn, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = dbConn.Close() })
	return dbConn, mock
}

// The configured provider is not in this build: a typo, a half-finished rollout,
// or a provider dropped from the image. The instance must lock out rather than
// quietly authenticate everyone with a password while its operator believes every
// login goes through their identity service.
//
// The password read is armed and would succeed. If this ever returns 200, the
// gateway silently downgraded authentication to a weaker method.
func TestLoginDeniesWhenTheConfiguredProviderIsNotRegistered(t *testing.T) {
	t.Setenv(identity.ProviderEnvVar, "sso-that-is-not-in-this-build")

	dbConn, mock := newMock(t)
	expectAccountRow(mock, "active")
	probe := &credentialProbe{}
	expectSuccessfulCredentialRead(t, mock, probe)

	w := postLogin(t, newAuthHandler(dbConn), `{"email":"user@example.com","password":"correct-horse"}`)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status %d, want 503 -- a gateway that cannot resolve its identity provider must deny, and must not report it as a wrong password", w.Code)
	}
	assertNoSession(t, w)
	if probe.ran {
		t.Fatal("an unresolvable provider fell back to the local password provider -- that is an authentication downgrade one typo wide")
	}
}

// The same denial, reached the other way: a handler whose registry was never
// wired. It denies instead of dereferencing nil, so the process survives to log
// the reason.
func TestLoginDeniesWhenNoRegistryIsWired(t *testing.T) {
	dbConn, mock := newMock(t)
	expectAccountRow(mock, "active")
	probe := &credentialProbe{}
	expectSuccessfulCredentialRead(t, mock, probe)

	w := postLogin(t, &AuthHandler{db: dbConn}, `{"email":"user@example.com","password":"correct-horse"}`)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status %d, want 503", w.Code)
	}
	assertNoSession(t, w)
	if probe.ran {
		t.Fatal("an unwired registry fell back to the local password provider")
	}
}

// A provider that could not answer is not a rejected password. Reporting it as
// 401 would put a wrong reason in the audit log at the moment someone reads one,
// and would tell a user to retype a password that was never the problem.
func TestLoginSeparatesAnUnavailableProviderFromAWrongPassword(t *testing.T) {
	dbConn, mock := newMock(t)
	expectAccountRow(mock, "active")
	mock.ExpectQuery(`SELECT password_hash FROM users`).
		WillReturnError(errors.New("connection refused"))

	w := postLogin(t, newAuthHandler(dbConn), `{"email":"user@example.com","password":"correct-horse"}`)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status %d, want 503 -- a provider that could not answer must not be reported as an invalid credential", w.Code)
	}
	assertNoSession(t, w)
}

// The ordinary rejection still reads as one, and this is also the probe's control:
// it is the case where the credential read MUST happen, so a probe that never
// fires would be caught here rather than silently passing the tests above.
func TestLoginStillRejectsAWrongPasswordAsUnauthorized(t *testing.T) {
	dbConn, mock := newMock(t)
	expectAccountRow(mock, "active")
	probe := &credentialProbe{}
	expectSuccessfulCredentialRead(t, mock, probe)

	w := postLogin(t, newAuthHandler(dbConn), `{"email":"user@example.com","password":"wrong-horse"}`)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status %d, want 401", w.Code)
	}
	assertNoSession(t, w)
	if !probe.ran {
		t.Fatal("the credential read never happened on the one path that must perform it -- the probe cannot detect a fallback either, so the fail-closed tests above are not proving anything")
	}
}

// The account-status check stays ahead of the credential check, where it has
// always been. Moving the password comparison behind an interface is not licence
// to reorder what happens before it. Status alone cannot show this -- a reordered
// handler still ends in 403 -- so the assertion is on whether the credential was
// read at all.
func TestADeactivatedAccountIsRejectedBeforeAnyCredentialCheck(t *testing.T) {
	dbConn, mock := newMock(t)
	expectAccountRow(mock, "deactivated")
	probe := &credentialProbe{}
	expectSuccessfulCredentialRead(t, mock, probe)

	w := postLogin(t, newAuthHandler(dbConn), `{"email":"user@example.com","password":"correct-horse"}`)

	if w.Code != http.StatusForbidden {
		t.Fatalf("status %d, want 403", w.Code)
	}
	assertNoSession(t, w)
	if probe.ran {
		t.Fatal("a deactivated account's credential was checked; the status gate no longer runs first")
	}
}
