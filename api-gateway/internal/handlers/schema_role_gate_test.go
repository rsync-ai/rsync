package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

// newSchemaGateEngine wires the four mutating Schema Registry routes with the
// same role middleware main.go applies (SEC-M-03), backed by a stub handler so
// the test exercises only the gate:
//   - writes (POST /schemas/:subject, PUT /schemas/:subject/config) -> power_user|admin
//   - destructive/global-config (PUT /schemas/config, DELETE /schemas/:subject) -> admin
func newSchemaGateEngine() *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	ok := func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"ok": true}) }
	r.PUT("/schemas/config", AdminRoleMiddleware(), ok)
	r.POST("/schemas/:subject", PowerUserOrAdminMiddleware(), ok)
	r.DELETE("/schemas/:subject", AdminRoleMiddleware(), ok)
	r.PUT("/schemas/:subject/config", PowerUserOrAdminMiddleware(), ok)
	return r
}

func doSchemaReq(r *gin.Engine, method, path string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, nil)
	req.Header.Set("Authorization", "Bearer tok")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func TestSchemaRoutes_Write_ForbiddenForUser(t *testing.T) {
	_, mock, cleanup := setupMockDB(t)
	defer cleanup()

	expectAdminLookup(mock, "tok", "u1", "user@example.com", "user", "active", nil)

	w := doSchemaReq(newSchemaGateEngine(), http.MethodPost, "/schemas/orders-value")
	if w.Code != http.StatusForbidden {
		t.Fatalf("expected %d, got %d (%s)", http.StatusForbidden, w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestSchemaRoutes_Write_OKForPowerUser(t *testing.T) {
	_, mock, cleanup := setupMockDB(t)
	defer cleanup()

	expectAdminLookup(mock, "tok", "u1", "pu@example.com", "power_user", "active", nil)

	w := doSchemaReq(newSchemaGateEngine(), http.MethodPost, "/schemas/orders-value")
	if w.Code != http.StatusOK {
		t.Fatalf("expected %d, got %d (%s)", http.StatusOK, w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestSchemaRoutes_SubjectConfig_OKForPowerUser(t *testing.T) {
	_, mock, cleanup := setupMockDB(t)
	defer cleanup()

	expectAdminLookup(mock, "tok", "u1", "pu@example.com", "power_user", "active", nil)

	w := doSchemaReq(newSchemaGateEngine(), http.MethodPut, "/schemas/orders-value/config")
	if w.Code != http.StatusOK {
		t.Fatalf("expected %d, got %d (%s)", http.StatusOK, w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestSchemaRoutes_Destructive_ForbiddenForPowerUser(t *testing.T) {
	_, mock, cleanup := setupMockDB(t)
	defer cleanup()

	adminGlobalLimiter = newFixedWindowLimiter(100, time.Minute)
	// power_user is below the admin bar the DELETE route requires.
	expectAdminLookup(mock, "tok", "u1", "pu@example.com", "power_user", "active", nil)

	w := doSchemaReq(newSchemaGateEngine(), http.MethodDelete, "/schemas/orders-value")
	if w.Code != http.StatusForbidden {
		t.Fatalf("expected %d, got %d (%s)", http.StatusForbidden, w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestSchemaRoutes_GlobalConfig_ForbiddenForUser(t *testing.T) {
	_, mock, cleanup := setupMockDB(t)
	defer cleanup()

	adminGlobalLimiter = newFixedWindowLimiter(100, time.Minute)
	expectAdminLookup(mock, "tok", "u1", "user@example.com", "user", "active", nil)

	w := doSchemaReq(newSchemaGateEngine(), http.MethodPut, "/schemas/config")
	if w.Code != http.StatusForbidden {
		t.Fatalf("expected %d, got %d (%s)", http.StatusForbidden, w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestSchemaRoutes_Destructive_OKForAdmin(t *testing.T) {
	_, mock, cleanup := setupMockDB(t)
	defer cleanup()

	adminGlobalLimiter = newFixedWindowLimiter(100, time.Minute)
	expectAdminLookup(mock, "tok", "u1", "admin@example.com", "admin", "active", nil)

	w := doSchemaReq(newSchemaGateEngine(), http.MethodDelete, "/schemas/orders-value")
	if w.Code != http.StatusOK {
		t.Fatalf("expected %d, got %d (%s)", http.StatusOK, w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestSchemaRoutes_GlobalConfig_OKForAdmin(t *testing.T) {
	_, mock, cleanup := setupMockDB(t)
	defer cleanup()

	adminGlobalLimiter = newFixedWindowLimiter(100, time.Minute)
	expectAdminLookup(mock, "tok", "u1", "admin@example.com", "admin", "active", nil)

	w := doSchemaReq(newSchemaGateEngine(), http.MethodPut, "/schemas/config")
	if w.Code != http.StatusOK {
		t.Fatalf("expected %d, got %d (%s)", http.StatusOK, w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}
