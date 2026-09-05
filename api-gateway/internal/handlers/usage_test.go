package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"api-gateway/internal/db"
)

// withNilDB swaps the package DB global to nil for the duration of a test so the
// handler's "database not connected" branch is exercised, then restores it.
func withNilDB(t *testing.T) func() {
	t.Helper()
	prev := db.DB
	db.DB = nil
	return func() { db.DB = prev }
}

// TestAdminUsage_DatabaseUnavailableReturns503 locks the fix for the usage-500
// handoff item: a missing DB connection is a transient availability problem, so
// AdminUsage must return 503 (like GetWorkspaceUsage), not a raw 500.
func TestAdminUsage_DatabaseUnavailableReturns503(t *testing.T) {
	defer withNilDB(t)()

	r := wsScopeRouter(http.MethodGet, "/api/v1/admin/usage", AdminUsage)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/admin/usage", nil))

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("AdminUsage with no DB = %d, want 503", w.Code)
	}
}

// TestGetWorkspaceUsage_DatabaseUnavailableReturns503 regression-locks the
// existing (correct) 503 behavior on the per-workspace endpoint so it stays
// consistent with AdminUsage.
func TestGetWorkspaceUsage_DatabaseUnavailableReturns503(t *testing.T) {
	defer withNilDB(t)()

	r := wsScopeRouter(http.MethodGet, "/api/v1/usage", GetWorkspaceUsage)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/usage", nil))

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("GetWorkspaceUsage with no DB = %d, want 503", w.Code)
	}
}
