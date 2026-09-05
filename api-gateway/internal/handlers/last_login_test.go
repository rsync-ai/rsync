package handlers

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
	"golang.org/x/crypto/bcrypt"
)

// The defect these tests guard against was not a wrong line of code -- it was a
// wrong *source*. "Last Login" was derived from `MAX(sessions.created_at)`, and
// logout DELETEs the session row, so the number ran backwards. Nothing failed;
// the value was always a real timestamp, just not the one the label promised.
//
// So the assertions below are about provenance, not formatting: the read must come
// from the stamped column, the stamp must actually be written on every path that
// mints a session, and neither read may reach into `sessions` again.

// ---- the read side ----

// lastLoginMustNotDeriveFromSessions is the whole regression guard in one place.
// Any future edit that reintroduces a sessions subquery fails here, in the default
// `go test ./...` suite -- no build tag, so CI actually runs it.
func lastLoginMustNotDeriveFromSessions(t *testing.T, name, query string) {
	t.Helper()
	if !strings.Contains(query, "u.last_login_at") {
		t.Errorf("%s: must project the stamped users.last_login_at column; got:\n%s", name, query)
	}
	if strings.Contains(query, "sessions") {
		t.Errorf("%s: must not derive last_login from the sessions table -- logout deletes "+
			"those rows, which is the bug migration 092 fixed; got:\n%s", name, query)
	}
}

func TestAdminUserListQueryReadsStampedColumn(t *testing.T) {
	q := buildAdminUserListQuery(" WHERE u.role = $1 ", "2", "3")
	lastLoginMustNotDeriveFromSessions(t, "buildAdminUserListQuery", q)

	// The WHERE clause and the pagination placeholders must survive assembly --
	// pushing the SQL into a builder is only safe if the builder is faithful.
	for _, want := range []string{"WHERE u.role = $1", "LIMIT $2", "OFFSET $3", "ORDER BY u.created_at DESC"} {
		if !strings.Contains(q, want) {
			t.Errorf("buildAdminUserListQuery: missing %q in:\n%s", want, q)
		}
	}
}

func TestAdminUserGetQueryReadsStampedColumn(t *testing.T) {
	lastLoginMustNotDeriveFromSessions(t, "adminUserGetQuery", adminUserGetQuery)
	if !strings.Contains(adminUserGetQuery, "WHERE u.id = $1") {
		t.Errorf("adminUserGetQuery: lost its id predicate:\n%s", adminUserGetQuery)
	}
}

// Both reads share one projection so they cannot drift apart -- which is exactly
// how the original bug came to exist in two places at once.
func TestBothAdminReadsShareOneProjection(t *testing.T) {
	if !strings.Contains(buildAdminUserListQuery("", "1", "2"), lastLoginProjection) {
		t.Error("list query no longer uses lastLoginProjection")
	}
	if !strings.Contains(adminUserGetQuery, lastLoginProjection) {
		t.Error("get query no longer uses lastLoginProjection")
	}
}

// ---- the write side ----

func TestStampLastLoginWritesTheColumn(t *testing.T) {
	dbConn, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = dbConn.Close() }()

	mock.ExpectExec(`UPDATE users SET last_login_at = NOW\(\) WHERE id = \$1`).
		WithArgs("user-1").
		WillReturnResult(sqlmock.NewResult(0, 1))

	stampLastLogin(dbConn, "user-1")

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("stampLastLogin did not write the column: %v", err)
	}
}

// A failed stamp must not be able to fail a login. The void signature is what
// guarantees it; this pins the behaviour so a later refactor to `error` has to
// confront the question rather than propagate by accident.
func TestStampLastLoginSurvivesDatabaseError(t *testing.T) {
	dbConn, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = dbConn.Close() }()

	mock.ExpectExec(`UPDATE users SET last_login_at`).
		WillReturnError(errors.New("connection reset"))

	stampLastLogin(dbConn, "user-1") // must not panic

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expected the UPDATE to be attempted: %v", err)
	}
}

func TestStampLastLoginIgnoresNilDatabase(t *testing.T) {
	stampLastLogin(nil, "user-1") // must not panic
}

// ---- the call sites ----

// The unit tests above prove stampLastLogin works. They cannot prove it is
// *called*, and an uncalled stamp is indistinguishable from the original bug: the
// column stays NULL and the admin UI shows "-" forever. So this drives Login all
// the way through with a mock database and asserts the UPDATE actually fires.
//
// logAudit reads the package-level db.GetDB() rather than h.db, so it does not
// consume expectations here -- the only writes this mock sees are the session
// insert and the stamp.
func TestLoginStampsLastLogin(t *testing.T) {
	gin.SetMode(gin.TestMode)

	dbConn, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = dbConn.Close() }()

	// MinCost: this test is about the stamp, not about bcrypt's work factor.
	hash, err := bcrypt.GenerateFromPassword([]byte("correct-horse"), bcrypt.MinCost)
	if err != nil {
		t.Fatalf("bcrypt: %v", err)
	}

	mock.ExpectQuery(`SELECT id, email, password_hash`).
		WithArgs("user@example.com").
		WillReturnRows(sqlmock.NewRows(
			[]string{"id", "email", "password_hash", "role", "status", "name", "email_verified"},
		).AddRow("user-1", "user@example.com", string(hash), "admin", "active", "Test", true))
	mock.ExpectExec(`INSERT INTO sessions`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`UPDATE users SET last_login_at = NOW\(\) WHERE id = \$1`).
		WithArgs("user-1").
		WillReturnResult(sqlmock.NewResult(0, 1))

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/api/v1/auth/login",
		strings.NewReader(`{"email":"user@example.com","password":"correct-horse"}`))
	c.Request.Header.Set("Content-Type", "application/json")

	(&AuthHandler{db: dbConn}).Login(c)

	if w.Code != http.StatusOK {
		t.Fatalf("login failed: status %d body %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("Login did not stamp last_login_at: %v", err)
	}
}
