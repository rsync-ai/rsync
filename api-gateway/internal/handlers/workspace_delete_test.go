package handlers

import (
	"database/sql"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// P3d: DeleteWorkspace (DELETE /api/v1/workspaces/:id) — owner-only, gated on the
// URL :id param via requireWorkspaceParamRole (IDOR-safe; see
// workspace_invites_test.go header). Two safety guards beyond the owner gate:
//
//   - Personal workspaces are undeletable: a user's auto-provisioned personal
//     workspace (workspaces.is_personal = true) is their identity anchor and the
//     backfill target for connections (migration 069) → 409, never deleted.
//   - Block-if-non-empty: pipelines.workspace_id, connections.workspace_id and
//     saved_queries.workspace_id are ON DELETE CASCADE, so deleting a workspace
//     silently cascade-deletes every pipeline, connection and saved query under it
//     — and, through saved_query_id, every schedule and run belonging to those
//     queries. We refuse to delete a workspace that still owns any of them → 409.
//     The owner must remove them first, making the destructive cascade an explicit,
//     deliberate sequence. See wsResourceCountSQL.
//
// An empty, non-personal workspace deletes cleanly (204); workspace_members and
// workspace_invites cascade away with it.
//
// wsScopeUser / wsScopeWS / wsScopeRouter / wsScopeMockDB / wsParamGate /
// errTestDBDown come from sibling test files in this package.

// wsIsPersonal stubs the is_personal lookup the delete handler runs after the gate.
func wsIsPersonal(mock sqlmock.Sqlmock, personal bool) {
	mock.ExpectQuery(`SELECT is_personal FROM workspaces WHERE id = \$1`).
		WithArgs(wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"is_personal"}).AddRow(personal))
}

// wsResourceCountSQL pins the emptiness guard to ALL THREE cascade parents. Every
// table listed here is ON DELETE CASCADE off workspaces, so one omitted from the
// count is a category of data the workspace delete destroys silently. saved_queries
// is the one that was missing: its own cascade reaches saved_query_schedules and
// saved_query_runs (migrations 084-086), so a single uncounted saved query takes
// live schedules and the whole run history with it. Dropping any table from the
// handler's count makes this expectation stop matching and fails these tests.
const wsResourceCountSQL = `FROM pipelines WHERE workspace_id = \$1` +
	`.*FROM connections WHERE workspace_id = \$1` +
	`.*FROM saved_queries WHERE workspace_id = \$1`

// wsResourceCount stubs the combined pipelines+connections+saved_queries guard.
func wsResourceCount(mock sqlmock.Sqlmock, n int) {
	mock.ExpectQuery(wsResourceCountSQL).
		WithArgs(wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"total"}).AddRow(n))
}

func wsDeleteReq() *http.Request {
	return httptest.NewRequest(http.MethodDelete, "/api/v1/workspaces/"+wsScopeWS, nil)
}

func deleteWorkspaceRouter() http.Handler {
	return wsScopeRouter(http.MethodDelete, "/api/v1/workspaces/:id", DeleteWorkspace)
}

func TestDeleteWorkspace_OwnerDeletesEmptyWorkspace(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)

	wsParamGate(mock, "owner")
	wsIsPersonal(mock, false)
	wsResourceCount(mock, 0)
	mock.ExpectExec(`DELETE FROM workspaces WHERE id = \$1`).
		WithArgs(wsScopeWS).
		WillReturnResult(sqlmock.NewResult(0, 1))

	resp := httptest.NewRecorder()
	deleteWorkspaceRouter().ServeHTTP(resp, wsDeleteReq())

	if resp.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d: %s", resp.Code, resp.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestDeleteWorkspace_NonOwnerForbidden(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)

	// An admin is below the owner gate: rejected before any is_personal/count/delete.
	wsParamGate(mock, "admin")

	resp := httptest.NewRecorder()
	deleteWorkspaceRouter().ServeHTTP(resp, wsDeleteReq())

	if resp.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d: %s", resp.Code, resp.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestDeleteWorkspace_PersonalWorkspaceUndeletable(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)

	wsParamGate(mock, "owner")
	wsIsPersonal(mock, true) // personal → reject; no count, no delete

	resp := httptest.NewRecorder()
	deleteWorkspaceRouter().ServeHTTP(resp, wsDeleteReq())

	if resp.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d: %s", resp.Code, resp.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestDeleteWorkspace_NonEmptyBlocked(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)

	wsParamGate(mock, "owner")
	wsIsPersonal(mock, false)
	wsResourceCount(mock, 3) // still owns resources → refuse; no delete

	resp := httptest.NewRecorder()
	deleteWorkspaceRouter().ServeHTTP(resp, wsDeleteReq())

	if resp.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d: %s", resp.Code, resp.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestDeleteWorkspace_NotFoundAfterGate(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)

	// Race: gate passed, but the workspace row vanished before the is_personal
	// lookup → 404, not a 500.
	wsParamGate(mock, "owner")
	mock.ExpectQuery(`SELECT is_personal FROM workspaces WHERE id = \$1`).
		WithArgs(wsScopeWS).
		WillReturnError(sql.ErrNoRows)

	resp := httptest.NewRecorder()
	deleteWorkspaceRouter().ServeHTTP(resp, wsDeleteReq())

	if resp.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", resp.Code, resp.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestDeleteWorkspace_IsPersonalLookupDBError(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)

	wsParamGate(mock, "owner")
	mock.ExpectQuery(`SELECT is_personal FROM workspaces WHERE id = \$1`).
		WithArgs(wsScopeWS).
		WillReturnError(errTestDBDown)

	resp := httptest.NewRecorder()
	deleteWorkspaceRouter().ServeHTTP(resp, wsDeleteReq())

	if resp.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d: %s", resp.Code, resp.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestDeleteWorkspace_CountDBError(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)

	wsParamGate(mock, "owner")
	wsIsPersonal(mock, false)
	mock.ExpectQuery(wsResourceCountSQL).
		WithArgs(wsScopeWS).
		WillReturnError(errTestDBDown)

	resp := httptest.NewRecorder()
	deleteWorkspaceRouter().ServeHTTP(resp, wsDeleteReq())

	if resp.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d: %s", resp.Code, resp.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// Pins the DELETE-affects-0-rows -> 404 branch (workspace vanished between the
// emptiness check and the delete). Distinct from TestDeleteWorkspace_NotFoundAfterGate,
// which 404s at the is_personal lookup, never reaching the DELETE.
func TestDeleteWorkspace_DeleteAffectsZeroRows(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)

	wsParamGate(mock, "owner")
	wsIsPersonal(mock, false)
	wsResourceCount(mock, 0)
	mock.ExpectExec(`DELETE FROM workspaces WHERE id = \$1`).
		WithArgs(wsScopeWS).
		WillReturnResult(sqlmock.NewResult(0, 0)) // 0 rows affected

	resp := httptest.NewRecorder()
	deleteWorkspaceRouter().ServeHTTP(resp, wsDeleteReq())

	if resp.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", resp.Code, resp.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// Pins the DELETE-Exec-error -> 503 branch (consistent with the gate and the
// earlier lookup/count error paths).
func TestDeleteWorkspace_DeleteDBError(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)

	wsParamGate(mock, "owner")
	wsIsPersonal(mock, false)
	wsResourceCount(mock, 0)
	mock.ExpectExec(`DELETE FROM workspaces WHERE id = \$1`).
		WithArgs(wsScopeWS).
		WillReturnError(errTestDBDown)

	resp := httptest.NewRecorder()
	deleteWorkspaceRouter().ServeHTTP(resp, wsDeleteReq())

	if resp.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d: %s", resp.Code, resp.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}
