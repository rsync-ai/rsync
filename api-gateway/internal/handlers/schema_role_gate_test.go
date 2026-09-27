package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

// newSchemaGateEngine wires the Schema Registry routes with the same role
// middleware main.go applies, backed by a stub handler so the test exercises only
// the gate. The whole surface — reads included — is admin.
//
// SEC-M-03 originally gated the mutating routes at power_user and left the reads
// open to any authenticated user. That split is superseded: the registry is ONE
// shared Confluent instance with no workspace in a subject name and no tenant
// concept to filter on, so an open read let a member of one workspace enumerate
// every other tenant's subject names and schemas, and "may overwrite any tenant's
// schema but may not read one" was not a coherent rule to keep.
//
// This engine is a stub, so it can only prove the middleware behaves. That
// main.go actually attaches it to every route is a separate, positional question,
// pinned by TestEverySchemaRouteIsRegisteredOnTheAdminGatedGroup in cmd/server —
// the check this suite could not make, and the reason the read routes stayed
// ungated while this file sat green beside them.
func newSchemaGateEngine() *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	ok := func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"ok": true}) }
	r.GET("/schemas", AdminRoleMiddleware(), ok)
	r.GET("/schemas/:subject", AdminRoleMiddleware(), ok)
	r.PUT("/schemas/config", AdminRoleMiddleware(), ok)
	r.POST("/schemas/:subject", AdminRoleMiddleware(), ok)
	r.DELETE("/schemas/:subject", AdminRoleMiddleware(), ok)
	r.PUT("/schemas/:subject/config", AdminRoleMiddleware(), ok)
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

func TestSchemaRoutes_Write_ForbiddenForPowerUser(t *testing.T) {
	_, mock, cleanup := setupMockDB(t)
	defer cleanup()

	adminGlobalLimiter = newFixedWindowLimiter(100, time.Minute)
	// power_user used to be allowed to register a schema. It is not any more:
	// there is no tenant axis on a subject, so the write is global.
	expectAdminLookup(mock, "tok", "u1", "pu@example.com", "power_user", "active", nil)

	w := doSchemaReq(newSchemaGateEngine(), http.MethodPost, "/schemas/orders-value")
	if w.Code != http.StatusForbidden {
		t.Fatalf("expected %d, got %d (%s)", http.StatusOK, w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestSchemaRoutes_SubjectConfig_ForbiddenForPowerUser(t *testing.T) {
	_, mock, cleanup := setupMockDB(t)
	defer cleanup()

	adminGlobalLimiter = newFixedWindowLimiter(100, time.Minute)
	expectAdminLookup(mock, "tok", "u1", "pu@example.com", "power_user", "active", nil)

	w := doSchemaReq(newSchemaGateEngine(), http.MethodPut, "/schemas/orders-value/config")
	if w.Code != http.StatusForbidden {
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

// TestSchemaRoutes_Read_ForbiddenForUser is the finding itself. Listing subjects
// is not a harmless read on a shared registry: a subject name is a topic name is
// a table name, so an ordinary member of workspace A could enumerate workspace
// B's tables, then fetch the schema behind each one.
func TestSchemaRoutes_Read_ForbiddenForUser(t *testing.T) {
	_, mock, cleanup := setupMockDB(t)
	defer cleanup()

	adminGlobalLimiter = newFixedWindowLimiter(100, time.Minute)
	expectAdminLookup(mock, "tok", "u1", "user@example.com", "user", "active", nil)

	w := doSchemaReq(newSchemaGateEngine(), http.MethodGet, "/schemas")
	if w.Code != http.StatusForbidden {
		t.Fatalf("expected %d, got %d (%s)", http.StatusForbidden, w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestSchemaRoutes_Read_ForbiddenForPowerUser(t *testing.T) {
	_, mock, cleanup := setupMockDB(t)
	defer cleanup()

	adminGlobalLimiter = newFixedWindowLimiter(100, time.Minute)
	expectAdminLookup(mock, "tok", "u1", "pu@example.com", "power_user", "active", nil)

	w := doSchemaReq(newSchemaGateEngine(), http.MethodGet, "/schemas/orders-value")
	if w.Code != http.StatusForbidden {
		t.Fatalf("expected %d, got %d (%s)", http.StatusForbidden, w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

// The control. Without it every assertion above is satisfied by a gate that
// refuses everyone, which would take the surface offline rather than scope it.
func TestSchemaRoutes_Read_OKForAdmin(t *testing.T) {
	_, mock, cleanup := setupMockDB(t)
	defer cleanup()

	adminGlobalLimiter = newFixedWindowLimiter(100, time.Minute)
	expectAdminLookup(mock, "tok", "u1", "admin@example.com", "admin", "active", nil)

	w := doSchemaReq(newSchemaGateEngine(), http.MethodGet, "/schemas")
	if w.Code != http.StatusOK {
		t.Fatalf("expected %d, got %d (%s)", http.StatusOK, w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}
