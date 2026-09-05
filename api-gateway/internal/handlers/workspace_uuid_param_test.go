package handlers

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// GET /api/v1/workspaces/current returned 500 {"error":"failed to fetch workspace"}
// on production. There is no /workspaces/current route: gin binds the literal
// "current" to the :id param of /workspaces/:id, GetWorkspace passed it straight
// into `WHERE w.id = $2` against a uuid column, and Postgres answered
//
//	ERROR: invalid input syntax for type uuid: "current"
//
// which is not sql.ErrNoRows, so it fell to the generic 500 branch — a client
// error reported as a server fault. The branch also discarded err, which is why
// the gateway logs held nothing for the failing path.
//
// These pin the boundary: a non-UUID :id is a 400 that never reaches SQL, while
// every pre-existing status code on the valid-UUID path is unchanged.
//
// wsScopeUser / wsScopeWS / wsScopeRouter / wsScopeMockDB come from sibling
// *_test.go files in this package.

func getWorkspaceRouter() http.Handler {
	return wsScopeRouter(http.MethodGet, "/api/v1/workspaces/:id", GetWorkspace)
}

func getWorkspace(t *testing.T, id string) *httptest.ResponseRecorder {
	t.Helper()
	resp := httptest.NewRecorder()
	getWorkspaceRouter().ServeHTTP(resp, httptest.NewRequest(
		http.MethodGet, "/api/v1/workspaces/"+id, nil))
	return resp
}

// The exact production symptom.
func TestGetWorkspace_CurrentIsRejectedNotA500(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	// No query is mocked: a malformed id must be rejected before any SQL runs.
	resp := getWorkspace(t, "current")

	if resp.Code != http.StatusBadRequest {
		t.Fatalf("a non-UUID :id is a client error and must 400, got %d: %s", resp.Code, resp.Body.String())
	}
	if !strings.Contains(resp.Body.String(), "invalid_workspace_id") {
		t.Fatalf("expected the invalid_workspace_id error code, got: %s", resp.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

// The same defect for any malformed id, not just the one that was reported.
func TestGetWorkspace_MalformedIDsRejectedBeforeSQL(t *testing.T) {
	for _, id := range []string{
		"current",
		"me",
		"not-a-uuid",
		"12345",
		"22222222-2222-2222-2222-22222222222",  // one char short of a UUID
		"22222222-2222-2222-2222-2222222222zz", // right shape, non-hex
	} {
		t.Run(id, func(t *testing.T) {
			mock, cleanup := wsScopeMockDB(t)
			defer cleanup()

			resp := getWorkspace(t, id)

			if resp.Code != http.StatusBadRequest {
				t.Fatalf("%q must 400, got %d: %s", id, resp.Code, resp.Body.String())
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("db expectations: %v", err)
			}
		})
	}
}

// Control: the guard must not swallow the happy path.
func TestGetWorkspace_ValidUUIDStillReturnsWorkspace(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	mock.ExpectQuery(`FROM workspaces w\s+JOIN workspace_members wm`).
		WithArgs(wsScopeUser, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "name", "slug", "owner_id", "plan", "is_personal", "created_at", "updated_at", "role",
		}).AddRow(wsScopeWS, "Acme", "acme", wsScopeUser, "free", false, time.Now(), time.Now(), "owner"))

	resp := getWorkspace(t, wsScopeWS)

	if resp.Code != http.StatusOK {
		t.Fatalf("a valid UUID must still 200, got %d: %s", resp.Code, resp.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

// The 400 must not become a membership oracle. A well-formed id the caller cannot
// see stays a 404 — identical to a well-formed id that does not exist — so the
// status code still never distinguishes "exists" from "not yours". The 400 is
// reserved for ids that could not name any workspace in the first place.
// See workspace_param_gate_consistency_test.go for the rule this preserves.
func TestGetWorkspace_UnknownOrForbiddenUUIDStays404(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	const otherWS = "99999999-9999-9999-9999-999999999999"
	// Zero rows: the membership JOIN matches nothing, so Scan yields sql.ErrNoRows —
	// the same answer whether the workspace is absent or merely not the caller's.
	mock.ExpectQuery(`FROM workspaces w\s+JOIN workspace_members wm`).
		WithArgs(wsScopeUser, otherWS).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "name", "slug", "owner_id", "plan", "is_personal", "created_at", "updated_at", "role",
		}))

	resp := getWorkspace(t, otherWS)

	if resp.Code != http.StatusNotFound {
		t.Fatalf("a well-formed but invisible id must stay 404 (never confirm existence), got %d: %s",
			resp.Code, resp.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

// A genuine database fault on a valid UUID must still be a 500 — the guard
// narrows the 500 to real server faults, it does not hide them.
func TestGetWorkspace_RealDBErrorStillIs500(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	mock.ExpectQuery(`FROM workspaces w\s+JOIN workspace_members wm`).
		WithArgs(wsScopeUser, wsScopeWS).
		WillReturnError(errors.New("connection reset by peer"))

	resp := getWorkspace(t, wsScopeWS)

	if resp.Code != http.StatusInternalServerError {
		t.Fatalf("a real DB fault must still 500, got %d: %s", resp.Code, resp.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}
