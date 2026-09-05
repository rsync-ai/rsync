package handlers

import (
	"database/sql"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"api-gateway/internal/db"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
	sharedcrypto "github.com/rsync-ai/shared/crypto"
)

func setupMockDB(t *testing.T) (*sql.DB, sqlmock.Sqlmock, func()) {
	t.Helper()
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}

	prev := db.DB
	db.DB = mockDB

	cleanup := func() {
		db.DB = prev
		_ = mockDB.Close()
	}
	return mockDB, mock, cleanup
}

func newTestEngine() *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/admin/ping", AdminRoleMiddleware(), func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})
	return r
}

func expectAdminLookup(mock sqlmock.Sqlmock, token string, userID string, email string, role string, status string, err error) {
	q := `SELECT u.id, u.email, u.role, COALESCE\(u.status, 'active'\)
			FROM users u
			JOIN sessions s ON s.user_id = u.id
			WHERE s.token = \$1 AND s.expires_at > NOW\(\)`

	// `token` is the plaintext bearer token a client would send. What the query is
	// allowed to bind is its SHA-256 -- so this helper doubles as the regression
	// test for #830's sibling fix: if a handler ever stops hashing before the
	// lookup, it binds the plaintext, sqlmock finds no matching expectation, and
	// every test through this helper fails. Passing the plaintext here instead
	// would make that regression invisible.
	stored := sharedcrypto.HashSessionToken(token)

	if err != nil {
		mock.ExpectQuery(q).WithArgs(stored).WillReturnError(err)
		return
	}
	mock.ExpectQuery(q).
		WithArgs(stored).
		WillReturnRows(sqlmock.NewRows([]string{"id", "email", "role", "status"}).
			AddRow(userID, email, role, status))
}

func TestAdminRoleMiddleware_Unauthorized_NoToken(t *testing.T) {
	_, mock, cleanup := setupMockDB(t)
	defer cleanup()

	adminGlobalLimiter = newFixedWindowLimiter(100, time.Minute)

	r := newTestEngine()
	req := httptest.NewRequest("GET", "/admin/ping", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected %d, got %d", http.StatusUnauthorized, w.Code)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestAdminRoleMiddleware_Unauthorized_InvalidToken(t *testing.T) {
	_, mock, cleanup := setupMockDB(t)
	defer cleanup()

	adminGlobalLimiter = newFixedWindowLimiter(100, time.Minute)

	expectAdminLookup(mock, "tok", "", "", "", "", sql.ErrNoRows)

	r := newTestEngine()
	req := httptest.NewRequest("GET", "/admin/ping", nil)
	req.Header.Set("Authorization", "Bearer tok")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected %d, got %d", http.StatusUnauthorized, w.Code)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestAdminRoleMiddleware_Forbidden_NonAdminRole(t *testing.T) {
	_, mock, cleanup := setupMockDB(t)
	defer cleanup()

	adminGlobalLimiter = newFixedWindowLimiter(100, time.Minute)

	expectAdminLookup(mock, "tok", "u1", "user@example.com", "user", "active", nil)

	r := newTestEngine()
	req := httptest.NewRequest("GET", "/admin/ping", nil)
	req.Header.Set("Authorization", "Bearer tok")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusForbidden {
		t.Fatalf("expected %d, got %d", http.StatusForbidden, w.Code)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestAdminRoleMiddleware_Forbidden_Deactivated(t *testing.T) {
	_, mock, cleanup := setupMockDB(t)
	defer cleanup()

	adminGlobalLimiter = newFixedWindowLimiter(100, time.Minute)

	expectAdminLookup(mock, "tok", "u1", "admin@example.com", "admin", "deactivated", nil)

	r := newTestEngine()
	req := httptest.NewRequest("GET", "/admin/ping", nil)
	req.Header.Set("Authorization", "Bearer tok")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusForbidden {
		t.Fatalf("expected %d, got %d", http.StatusForbidden, w.Code)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestAdminRoleMiddleware_OK_Admin(t *testing.T) {
	_, mock, cleanup := setupMockDB(t)
	defer cleanup()

	adminGlobalLimiter = newFixedWindowLimiter(100, time.Minute)

	expectAdminLookup(mock, "tok", "u1", "admin@example.com", "admin", "active", nil)

	r := newTestEngine()
	req := httptest.NewRequest("GET", "/admin/ping", nil)
	req.Header.Set("Authorization", "Bearer tok")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected %d, got %d", http.StatusOK, w.Code)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestAdminRoleMiddleware_OK_Admin_CookieToken(t *testing.T) {
	_, mock, cleanup := setupMockDB(t)
	defer cleanup()

	adminGlobalLimiter = newFixedWindowLimiter(100, time.Minute)

	expectAdminLookup(mock, "tok", "u1", "admin@example.com", "admin", "active", nil)

	r := newTestEngine()
	req := httptest.NewRequest("GET", "/admin/ping", nil)
	req.AddCookie(&http.Cookie{Name: "auth_token", Value: "tok"})
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected %d, got %d", http.StatusOK, w.Code)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestAdminRoleMiddleware_RateLimited(t *testing.T) {
	mockDB, mock, cleanup := setupMockDB(t)
	defer cleanup()

	_ = mockDB // silence unused in case of build tags
	adminGlobalLimiter = newFixedWindowLimiter(1, time.Minute)

	// First request: allow
	expectAdminLookup(mock, "tok", "u1", "admin@example.com", "admin", "active", nil)
	// Second request: allow lookup then block by limiter and attempt audit insert.
	expectAdminLookup(mock, "tok", "u1", "admin@example.com", "admin", "active", nil)
	mock.ExpectExec(`INSERT INTO audit_logs`).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	r := newTestEngine()

	req1 := httptest.NewRequest("GET", "/admin/ping", nil)
	req1.Header.Set("Authorization", "Bearer tok")
	w1 := httptest.NewRecorder()
	r.ServeHTTP(w1, req1)
	if w1.Code != http.StatusOK {
		t.Fatalf("expected %d, got %d", http.StatusOK, w1.Code)
	}

	req2 := httptest.NewRequest("GET", "/admin/ping", nil)
	req2.Header.Set("Authorization", "Bearer tok")
	w2 := httptest.NewRecorder()
	r.ServeHTTP(w2, req2)
	if w2.Code != http.StatusTooManyRequests {
		t.Fatalf("expected %d, got %d", http.StatusTooManyRequests, w2.Code)
	}
	if ra := w2.Header().Get("Retry-After"); ra == "" {
		t.Fatalf("expected Retry-After header to be set")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

