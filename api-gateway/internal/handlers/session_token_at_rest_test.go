package handlers

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"api-gateway/internal/db"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
	sharedcrypto "github.com/rsync-ai/shared/crypto"
	"golang.org/x/crypto/bcrypt"
)

// sessions.token used to hold the bearer token verbatim, which made the table a
// list of working credentials rather than a list of references to them: anything
// that could read one row -- a backup, a managed-service snapshot, a read replica,
// a support query, a read-only SQL injection, or a dump carried between clouds --
// could replay it as the user until it expired.
//
// The failure mode these tests exist for is quiet. A handler that forgets to hash
// still logs in, still sets a cookie, still returns 200; the only visible
// difference is a column value nobody reads by eye. So the assertions below are on
// the argument actually bound to the driver, not on the status code.

// capturedArg records what a query really bound, so a test can assert on the value
// instead of on sqlmock's matched/not-matched boolean. Matching always succeeds --
// the assertions happen after the handler returns, which keeps a failure legible
// ("bound the plaintext") rather than surfacing as an opaque expectation mismatch.
type capturedArg struct{ value driver.Value }

func (c *capturedArg) Match(v driver.Value) bool {
	c.value = v
	return true
}

func (c *capturedArg) string(t *testing.T) string {
	t.Helper()
	s, ok := c.value.(string)
	if !ok {
		t.Fatalf("sessions.token was bound as %T (%v), want a string", c.value, c.value)
	}
	return s
}

// assertStoredIsHashOf is the single property under test, in one place: the value
// written to sessions.token is the SHA-256 of the token the client was handed, and
// therefore is not the token itself.
func assertStoredIsHashOf(t *testing.T, stored, issued string) {
	t.Helper()
	if issued == "" {
		t.Fatal("handler returned no token, so there is nothing to compare the stored value against")
	}
	if stored == issued {
		t.Fatalf("sessions.token was bound to the plaintext token %q -- a stolen row is a working credential", issued)
	}
	if want := sharedcrypto.HashSessionToken(issued); stored != want {
		t.Fatalf("sessions.token = %q, want sha256(issued token) = %q", stored, want)
	}
}

func TestLogin_StoresHashNotPlaintextToken(t *testing.T) {
	gin.SetMode(gin.TestMode)
	dbConn, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = dbConn.Close() }()

	// MinCost: this test is about what reaches sessions.token, not about bcrypt's
	// work factor (users.password_hash is separately and correctly bcrypt cost 12).
	hash, err := bcrypt.GenerateFromPassword([]byte("correct-horse"), bcrypt.MinCost)
	if err != nil {
		t.Fatalf("bcrypt: %v", err)
	}

	mock.ExpectQuery(`SELECT id, email, password_hash`).
		WithArgs("user@example.com").
		WillReturnRows(sqlmock.NewRows(
			[]string{"id", "email", "password_hash", "role", "status", "name", "email_verified"},
		).AddRow("user-1", "user@example.com", string(hash), "admin", "active", "Test", true))

	stored := &capturedArg{}
	// Arg 3 is sessions.token in `INSERT INTO sessions (id, user_id, token, ...)`.
	mock.ExpectExec(`INSERT INTO sessions`).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), stored, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`UPDATE users SET last_login_at = NOW\(\)`).
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

	var resp struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode login response: %v", err)
	}
	assertStoredIsHashOf(t, stored.string(t), resp.Token)
}

// The plaintext token still has to reach the client -- it is the credential. This
// pins the other half of the boundary: hashed in the database, verbatim in the
// Set-Cookie header. A "fix" that hashed the cookie too would pass the test above
// while logging everyone out.
func TestLogin_CookieCarriesThePlaintextToken(t *testing.T) {
	gin.SetMode(gin.TestMode)
	dbConn, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = dbConn.Close() }()

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
	mock.ExpectExec(`UPDATE users SET last_login_at = NOW\(\)`).
		WithArgs("user-1").WillReturnResult(sqlmock.NewResult(0, 1))

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/api/v1/auth/login",
		strings.NewReader(`{"email":"user@example.com","password":"correct-horse"}`))
	c.Request.Header.Set("Content-Type", "application/json")

	(&AuthHandler{db: dbConn}).Login(c)
	if w.Code != http.StatusOK {
		t.Fatalf("login failed: status %d body %s", w.Code, w.Body.String())
	}

	var resp struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode login response: %v", err)
	}

	var cookie string
	for _, ck := range w.Result().Cookies() {
		if ck.Name == "auth_token" {
			cookie = ck.Value
		}
	}
	if cookie == "" {
		t.Fatal("no auth_token cookie was set")
	}
	if cookie != resp.Token {
		t.Fatalf("auth_token cookie = %q, want the issued plaintext token %q -- the client cannot authenticate with a hash", cookie, resp.Token)
	}
}

// Write and read are in different files, and (worse) in different *services*:
// api-gateway and backend-orchestrator each compare this column independently. If
// the two sides ever disagree about whether to hash, nothing fails at compile time
// -- every request just returns 401. This walks a token from login through the
// middleware to prove the two halves agree on one value.
func TestSessionToken_WriteThenReadRoundTrip(t *testing.T) {
	gin.SetMode(gin.TestMode)
	dbConn, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = dbConn.Close() }()

	hash, err := bcrypt.GenerateFromPassword([]byte("correct-horse"), bcrypt.MinCost)
	if err != nil {
		t.Fatalf("bcrypt: %v", err)
	}
	mock.ExpectQuery(`SELECT id, email, password_hash`).
		WithArgs("user@example.com").
		WillReturnRows(sqlmock.NewRows(
			[]string{"id", "email", "password_hash", "role", "status", "name", "email_verified"},
		).AddRow("user-1", "user@example.com", string(hash), "admin", "active", "Test", true))

	stored := &capturedArg{}
	mock.ExpectExec(`INSERT INTO sessions`).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), stored, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`UPDATE users SET last_login_at = NOW\(\)`).
		WithArgs("user-1").WillReturnResult(sqlmock.NewResult(0, 1))

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/api/v1/auth/login",
		strings.NewReader(`{"email":"user@example.com","password":"correct-horse"}`))
	c.Request.Header.Set("Content-Type", "application/json")
	(&AuthHandler{db: dbConn}).Login(c)
	if w.Code != http.StatusOK {
		t.Fatalf("login failed: status %d body %s", w.Code, w.Body.String())
	}

	var resp struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode login response: %v", err)
	}
	writeSide := stored.string(t)

	// Now the read side. The row the middleware is allowed to find is keyed by the
	// value login actually wrote -- so if the middleware bound anything else, this
	// expectation goes unmatched and the request 401s.
	prev := db.DB
	db.DB = dbConn
	defer func() { db.DB = prev }()

	mock.ExpectQuery(`SELECT u.id, u.email, u.role`).
		WithArgs(writeSide).
		WillReturnRows(sqlmock.NewRows([]string{"id", "email", "role"}).
			AddRow("user-1", "user@example.com", "admin"))

	r := gin.New()
	r.GET("/whoami", AuthRequiredMiddleware(), func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"user_id": c.GetString("user_id")})
	})

	rw := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/whoami", nil)
	// The client presents the plaintext it was issued -- it has never seen the hash.
	req.Header.Set("Authorization", "Bearer "+resp.Token)
	r.ServeHTTP(rw, req)

	if rw.Code != http.StatusOK {
		t.Fatalf("round trip failed: the token login issued did not authenticate (status %d, body %s)", rw.Code, rw.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

// A stray plaintext write would be invisible in the round trip above (both sides
// would simply agree on the plaintext), so pin the negative directly: presenting a
// token whose *plaintext* is the stored value must not authenticate.
func TestAuthMiddleware_DoesNotLookUpByPlaintext(t *testing.T) {
	gin.SetMode(gin.TestMode)
	dbConn, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = dbConn.Close() }()

	prev := db.DB
	db.DB = dbConn
	defer func() { db.DB = prev }()

	const token = "3f2504e0-4f89-11d3-9a0c-0305e82c3301"

	// A database that only holds the plaintext -- i.e. the pre-fix world. The
	// middleware must miss it.
	mock.ExpectQuery(`SELECT u.id, u.email, u.role`).
		WithArgs(sharedcrypto.HashSessionToken(token)).
		WillReturnError(sql.ErrNoRows)

	r := gin.New()
	r.GET("/whoami", AuthRequiredMiddleware(), func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})

	rw := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/whoami", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	r.ServeHTTP(rw, req)

	if rw.Code != http.StatusUnauthorized {
		t.Fatalf("status %d, want 401: a session row holding the plaintext token must not authenticate", rw.Code)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("middleware did not query by the hash: %v", err)
	}
}
