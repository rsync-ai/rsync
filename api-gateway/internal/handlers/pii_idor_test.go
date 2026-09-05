package handlers

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"api-gateway/internal/db"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
)

// SEC (cross-tenant IDOR, pentest-found against the #601 code): the PII handlers
// queried pii_policies / pii_scan_* / pii_approval_* with NO tenancy filter, so a
// member of workspace A could read, modify, and delete workspace B's PII policies
// (proven live: GET returned B's policy; PUT/DELETE succeeded on B's row).
// migration 077 added workspace_id to these tables; every handler now scopes to
// the caller's ACTIVE workspace (wsScopeWS) and gates on the workspace role.
//
// These tests pin the fix: a policy belonging to ANOTHER workspace (foreignPolicy)
// is never returned, mutated, or deleted — a cross-tenant GET/PUT/DELETE returns
// 404 (the bound workspace_id = active never matches the foreign row, so 0 rows →
// 404, never revealing existence). Reads require >= viewer, mutations >= member.
//
// wsScopeUser / wsScopeWS / wsScopeMockDB come from workspace_scoping_test.go
// (same package). The active workspace (wsScopeWS) is deliberately distinct from
// the foreign resource ids so a handler that dropped the workspace_id filter would
// 200 instead of 404 and fail loudly.

const (
	// A pii_policy that lives in a DIFFERENT workspace than the caller's active one.
	foreignPolicyID = "88888888-8888-8888-8888-888888888888"
	// A pii_scan_job that lives in a DIFFERENT workspace.
	foreignScanJobID = "77777777-7777-7777-7777-777777777777"
)

// piiRoleRouter pins user_id + the active workspace/role (as AuthRequired +
// WorkspaceContextMiddleware would) at the given role, then routes to handler.
func piiRoleRouter(method, path, role string, handler gin.HandlerFunc) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(func(c *gin.Context) {
		c.Set("user_id", wsScopeUser)
		c.Set(ctxWorkspaceID, wsScopeWS)
		c.Set(ctxWorkspaceRole, role)
		c.Next()
	})
	r.Handle(method, path, handler)
	return r
}

func newPIIHandlerWithMock() *PIIHandler {
	// db.GetDB() returns the sqlmock swapped in by wsScopeMockDB.
	return NewPIIHandler(db.GetDB(), nil)
}

// TestPII_Policy_CrossTenantMutation404 — a member of workspace A (active =
// wsScopeWS) cannot modify or delete a policy owned by workspace B. The scoped
// write binds workspace_id = wsScopeWS, matches B's row 0 times → 404.
func TestPII_Policy_CrossTenantMutation404(t *testing.T) {
	cases := []struct {
		name       string
		method     string
		path       string
		route      string
		body       string
		handlerFor func(h *PIIHandler) gin.HandlerFunc
		expectSQL  func(mock sqlmock.Sqlmock)
	}{
		{
			name:       "PUT cross-tenant policy",
			method:     http.MethodPut,
			path:       "/api/v1/pii/policies/" + foreignPolicyID,
			route:      "/api/v1/pii/policies/:id",
			body:       `{"enabled": false}`,
			handlerFor: func(h *PIIHandler) gin.HandlerFunc { return h.UpdatePolicy },
			expectSQL: func(mock sqlmock.Sqlmock) {
				// updated_at=$1, enabled=$2, WHERE id=$3 AND workspace_id=$4; the
				// active workspace ($4) is bound, and 0 rows are affected because
				// the row belongs to another workspace.
				mock.ExpectExec(`UPDATE pii_policies SET updated_at = \$1, enabled = \$2 WHERE id = \$3 AND workspace_id = \$4`).
					WithArgs(sqlmock.AnyArg(), false, foreignPolicyID, wsScopeWS).
					WillReturnResult(sqlmock.NewResult(0, 0))
			},
		},
		{
			name:       "DELETE cross-tenant policy",
			method:     http.MethodDelete,
			path:       "/api/v1/pii/policies/" + foreignPolicyID,
			route:      "/api/v1/pii/policies/:id",
			body:       "",
			handlerFor: func(h *PIIHandler) gin.HandlerFunc { return h.DeletePolicy },
			expectSQL: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(`DELETE FROM pii_policies WHERE id = \$1 AND workspace_id = \$2`).
					WithArgs(foreignPolicyID, wsScopeWS).
					WillReturnResult(sqlmock.NewResult(0, 0))
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock, cleanup := wsScopeMockDB(t)
			defer cleanup()
			tc.expectSQL(mock)

			h := newPIIHandlerWithMock()
			var reader *bytes.Reader
			if tc.body != "" {
				reader = bytes.NewReader([]byte(tc.body))
			} else {
				reader = bytes.NewReader(nil)
			}
			r := piiRoleRouter(tc.method, tc.route, "member", tc.handlerFor(h))
			w := httptest.NewRecorder()
			req := httptest.NewRequest(tc.method, tc.path, reader)
			req.Header.Set("Content-Type", "application/json")
			r.ServeHTTP(w, req)

			if w.Code != http.StatusNotFound {
				t.Fatalf("expected 404 (cross-tenant blocked), got %d: %s", w.Code, w.Body.String())
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("db expectations: %v", err)
			}
		})
	}
}

// TestPII_GetPolicies_ScopedToActiveWorkspace — the list read binds the active
// workspace (returning only own + shared NULL-global rows), never every tenant's.
func TestPII_GetPolicies_ScopedToActiveWorkspace(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	mock.ExpectQuery(`FROM pii_policies\s+WHERE workspace_id = \$1 OR \(workspace_id IS NULL AND created_by IS NULL\)`).
		WithArgs(wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "org_id", "policy_type", "pii_type", "action", "hash_function", "condition", "priority", "enabled",
		}))

	h := newPIIHandlerWithMock()
	r := piiRoleRouter(http.MethodGet, "/api/v1/pii/policies", "viewer", h.GetPolicies)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/pii/policies", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

// TestPII_GlobalRead_OnlySeededBuiltinsAreGlobal pins the read-side residual IDOR
// fix (adversarial-review-found, live-confirmed on the isolated stack against the
// merged #605 binary). The list reads for policies and hash functions expose the
// seeded global built-ins to every workspace via `workspace_id IS NULL`. The
// original predicate `OR workspace_id IS NULL` was too broad: it ALSO returned
// unattributed tenant rows — workspace_id NULL but created_by NOT NULL, e.g. a
// policy whose creator migration 077 could not map to a personal workspace — to
// EVERY tenant, a cross-tenant config read leak. Only a SEEDED built-in (created_by
// IS NULL) is global; an unattributed row must fail closed.
//
// Enforcement is in the SQL predicate, so each case asserts the read binds the
// active workspace AND gates the NULL branch on created_by IS NULL. sqlmock matches
// the statement by regex: a regression to the broad `OR workspace_id IS NULL` no
// longer matches, ExpectationsWereMet fails, and the test breaks loudly.
func TestPII_GlobalRead_OnlySeededBuiltinsAreGlobal(t *testing.T) {
	cases := []struct {
		name       string
		route      string
		table      string
		columns    []string
		handlerFor func(h *PIIHandler) gin.HandlerFunc
	}{
		{
			name:       "GetPolicies: unattributed NULL-workspace row is not global",
			route:      "/api/v1/pii/policies",
			table:      "pii_policies",
			columns:    []string{"id", "org_id", "policy_type", "pii_type", "action", "hash_function", "condition", "priority", "enabled"},
			handlerFor: func(h *PIIHandler) gin.HandlerFunc { return h.GetPolicies },
		},
		{
			name:       "GetHashFunctions: unattributed NULL-workspace row is not global",
			route:      "/api/v1/hash-functions",
			table:      "custom_hash_functions",
			columns:    []string{"id", "name", "type", "description", "reversible", "enabled"},
			handlerFor: func(h *PIIHandler) gin.HandlerFunc { return h.GetHashFunctions },
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock, cleanup := wsScopeMockDB(t)
			defer cleanup()

			mock.ExpectQuery(`FROM ` + tc.table + `\s+WHERE workspace_id = \$1 OR \(workspace_id IS NULL AND created_by IS NULL\)`).
				WithArgs(wsScopeWS).
				WillReturnRows(sqlmock.NewRows(tc.columns))

			h := newPIIHandlerWithMock()
			r := piiRoleRouter(http.MethodGet, tc.route, "viewer", tc.handlerFor(h))
			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, tc.route, nil)
			r.ServeHTTP(w, req)

			if w.Code != http.StatusOK {
				t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("db expectations (query missing created_by-IS-NULL guard?): %v", err)
			}
		})
	}
}

// TestPII_GetScanJob_CrossTenant404 — a scan-job id in another workspace resolves
// to no row under the workspace_id filter → 404 (a cross-tenant GET-by-id).
func TestPII_GetScanJob_CrossTenant404(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	mock.ExpectQuery(`FROM pii_scan_jobs WHERE id = \$1 AND workspace_id = \$2`).
		WithArgs(foreignScanJobID, wsScopeWS).
		WillReturnError(sql.ErrNoRows)

	h := newPIIHandlerWithMock()
	r := piiRoleRouter(http.MethodGet, "/api/v1/pii/scan/jobs/:id", "viewer", h.GetScanJob)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/pii/scan/jobs/"+foreignScanJobID, nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 (cross-tenant scan job), got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

// TestPII_UpdatePolicy_MemberEditsOwnPolicy — the positive path: a member editing
// a policy IN the active workspace (1 row affected) succeeds. Proves the
// workspace_id filter does not over-block a legitimate same-workspace edit.
func TestPII_UpdatePolicy_MemberEditsOwnPolicy(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	ownPolicyID := "33333333-3333-3333-3333-333333333333"
	mock.ExpectExec(`UPDATE pii_policies SET updated_at = \$1, enabled = \$2 WHERE id = \$3 AND workspace_id = \$4`).
		WithArgs(sqlmock.AnyArg(), true, ownPolicyID, wsScopeWS).
		WillReturnResult(sqlmock.NewResult(0, 1))

	h := newPIIHandlerWithMock()
	body, _ := json.Marshal(map[string]any{"enabled": true})
	r := piiRoleRouter(http.MethodPut, "/api/v1/pii/policies/:id", "member", h.UpdatePolicy)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPut, "/api/v1/pii/policies/"+ownPolicyID, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 (member edits own-workspace policy), got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

// TestPII_CreatePolicy_StampsActiveWorkspace — a created policy carries
// workspace_id = active workspace as the final bound arg, so it can never be a
// cross-tenant-visible global (only seeded built-ins are NULL).
func TestPII_CreatePolicy_StampsActiveWorkspace(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	mock.ExpectExec(`INSERT INTO pii_policies`).
		WithArgs(
			sqlmock.AnyArg(), // id
			"always_mask",    // policy_type
			"ssn",            // pii_type
			"hash",           // action
			sqlmock.AnyArg(), // hash_function
			sqlmock.AnyArg(), // condition
			sqlmock.AnyArg(), // priority
			wsScopeUser,      // created_by
			sqlmock.AnyArg(), // created_at (== updated_at)
			wsScopeWS,        // workspace_id  <-- scoping assertion
		).
		WillReturnResult(sqlmock.NewResult(0, 1))

	h := newPIIHandlerWithMock()
	body, _ := json.Marshal(map[string]any{
		"policy_type": "always_mask",
		"pii_type":    "ssn",
		"action":      "hash",
	})
	r := piiRoleRouter(http.MethodPost, "/api/v1/pii/policies", "member", h.CreatePolicy)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/pii/policies", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

// TestPII_DeletePolicy_ViewerForbidden — a viewer (read-only) cannot delete a
// policy: the role gate returns 403 before any DB access (no query expected).
func TestPII_DeletePolicy_ViewerForbidden(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	h := newPIIHandlerWithMock()
	r := piiRoleRouter(http.MethodDelete, "/api/v1/pii/policies/:id", "viewer", h.DeletePolicy)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodDelete, "/api/v1/pii/policies/"+foreignPolicyID, nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusForbidden {
		t.Fatalf("expected 403 (viewer cannot mutate), got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}
