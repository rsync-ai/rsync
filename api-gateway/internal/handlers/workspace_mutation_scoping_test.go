package handlers

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"api-gateway/internal/db"

	"github.com/DATA-DOG/go-sqlmock"
)

// P2: resource MUTATIONS (UPDATE/DELETE/STOP/…) must be scoped to the ACTIVE
// WORKSPACE, not to created_by. P0 already made the read/access gate
// (canAccessPipeline / requireResourceRole) membership-based, so a teammate who
// is a member of the pipeline's workspace passes the gate — but the write SQL
// still filtered `AND created_by = $userID`, so a non-creator member's UPDATE
// matched 0 rows (silent no-op on PATCH, 404 on DELETE). These tests pin the
// fix: the caller (wsScopeUser) is a MEMBER, NOT the creator, and must be able
// to mutate the shared pipeline. The write must bind workspace_id = active
// workspace (wsScopeWS), never created_by = wsScopeUser.
//
// wsScopeUser / wsScopeWS / wsScopeRouter / wsScopeMockDB come from
// workspace_scoping_test.go (same package).

const wsScopePipeline = "33333333-3333-3333-3333-333333333333"

// gateRoleRows is the single-row result requireResourceRole expects: the
// caller's workspace role for the resource. "member" proves a non-owner member
// can mutate — the old created_by scoping would have excluded them.
func gateRoleRows(role string) *sqlmock.Rows {
	return sqlmock.NewRows([]string{"role"}).AddRow(role)
}

func TestUpdatePipeline_MemberCanEditSharedPipeline(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)

	// Gate: requireResourceRole proves membership in the ACTIVE workspace and
	// returns the role — bound by (resourceID, userID, activeWS).
	mock.ExpectQuery(`SELECT wm\.role\s+FROM pipelines r`).
		WithArgs(wsScopePipeline, wsScopeUser, wsScopeWS).
		WillReturnRows(gateRoleRows("member"))

	// Write: scoped by workspace_id = active workspace, NEVER created_by.
	mock.ExpectExec(`UPDATE pipelines SET name=\$1, updated_at=NOW\(\) WHERE id=\$2 AND workspace_id=\$3`).
		WithArgs("renamed-by-teammate", wsScopePipeline, wsScopeWS).
		WillReturnResult(sqlmock.NewResult(0, 1))

	body, _ := json.Marshal(map[string]any{"name": "renamed-by-teammate"})
	r := wsScopeRouter(http.MethodPatch, "/api/v1/pipelines/:id", UpdatePipeline)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPatch, "/api/v1/pipelines/"+wsScopePipeline, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

func TestStopPipeline_MemberCanStopSharedPipeline(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)

	mock.ExpectQuery(`SELECT wm\.role\s+FROM pipelines r`).
		WithArgs(wsScopePipeline, wsScopeUser, wsScopeWS).
		WillReturnRows(gateRoleRows("member"))

	// Status read is workspace-scoped, not created_by-scoped.
	mock.ExpectQuery(`SELECT status FROM pipelines WHERE id = \$1 AND workspace_id = \$2`).
		WithArgs(wsScopePipeline, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("running"))

	// Stop write is workspace-scoped.
	mock.ExpectExec(`UPDATE pipelines SET status = 'stopped', updated_at = NOW\(\) WHERE id = \$1 AND workspace_id = \$2`).
		WithArgs(wsScopePipeline, wsScopeWS).
		WillReturnResult(sqlmock.NewResult(0, 1))

	r := wsScopeRouter(http.MethodPost, "/api/v1/pipelines/:id/stop", StopPipeline)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/pipelines/"+wsScopePipeline+"/stop", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

func TestDeletePipeline_MemberCanDeleteSharedPipeline(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	// Gate first (before the CDC cleanup + transaction).
	mock.ExpectQuery(`SELECT wm\.role\s+FROM pipelines r`).
		WithArgs(wsScopePipeline, wsScopeUser, wsScopeWS).
		WillReturnRows(gateRoleRows("member"))

	// Orphan-schedule cleanup reads the pipeline's Temporal schedule IDs BEFORE
	// the delete (they cascade away with the row). None here → empty result.
	mock.ExpectQuery(`SELECT temporal_schedule_id FROM pipeline_schedules WHERE pipeline_id = \$1`).
		WithArgs(wsScopePipeline).
		WillReturnRows(sqlmock.NewRows([]string{"temporal_schedule_id"}))

	mock.ExpectBegin()
	// Executions delete is scoped via a workspace_id subquery, not created_by.
	mock.ExpectExec(`DELETE FROM executions`).
		WithArgs(wsScopePipeline, wsScopeWS).
		WillReturnResult(sqlmock.NewResult(0, 0))
	// Pipeline delete is workspace-scoped; one row affected → 200.
	mock.ExpectExec(`DELETE FROM pipelines WHERE id=\$1 AND workspace_id=\$2`).
		WithArgs(wsScopePipeline, wsScopeWS).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	r := wsScopeRouter(http.MethodDelete, "/api/v1/pipelines/:id", DeletePipeline)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodDelete, "/api/v1/pipelines/"+wsScopePipeline, nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

// A workspace member who is not the creator must be able to RUN a shared
// pipeline. RunPipeline keeps userID for the per-USER cost/plan quotas, but its
// pipeline-access SELECT must be workspace-scoped, not created_by-scoped. With
// the gate passed and connections NULL + Temporal unconfigured in tests, the
// handler runs all the way to the orchestration-unavailable terminal (503) —
// proving the member was authorized past the access SELECT. The old created_by
// scoping would 404 at that SELECT (created_by belongs to a different user).
func TestRunPipeline_MemberCanRunSharedPipeline(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)

	// Authz gate (requireResourceRole) — caller is a member of the active ws.
	mock.ExpectQuery(`SELECT wm\.role\s+FROM pipelines r`).
		WithArgs(wsScopePipeline, wsScopeUser, wsScopeWS).
		WillReturnRows(gateRoleRows("member"))

	// Pipeline-access SELECT is workspace-scoped; created_by is a DIFFERENT user
	// (the creator), proving a non-creator member can run it.
	const creator = "99999999-9999-9999-9999-999999999999"
	mock.ExpectQuery(`FROM pipelines WHERE id = \$1 AND workspace_id = \$2`).
		WithArgs(wsScopePipeline, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{
			"name", "status", "source_connection_id", "destination_connection_id",
			"natural_language_request", "created_by", "config", "dataset", "default_run_mode",
		}).AddRow("shared-p", "ready", nil, nil, nil, creator, []byte("{}"), nil, nil))

	// Status flip + execution record (no-snapshot branch: connections are NULL).
	mock.ExpectExec(`UPDATE pipelines SET status = 'running', updated_at = NOW\(\) WHERE id = \$1`).
		WithArgs(wsScopePipeline).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO executions`).
		WithArgs(sqlmock.AnyArg(), wsScopePipeline).
		WillReturnResult(sqlmock.NewResult(0, 1))

	// ack_warnings=true skips the assessment gate; cost/plan/cooldown gates fail
	// open on their unmocked queries.
	r := wsScopeRouter(http.MethodPost, "/api/v1/pipelines/:id/run", RunPipeline)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/pipelines/"+wsScopePipeline+"/run?ack_warnings=true", nil)
	r.ServeHTTP(w, req)

	if w.Code == http.StatusNotFound {
		t.Fatalf("member was denied the shared pipeline (404); want a past-SELECT outcome: %s", w.Body.String())
	}
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 (orchestration unavailable — i.e. authorized past the access SELECT), got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

// P2d: a pipeline may only reference connections in its OWN (active) workspace.
// resolveConnectionID's UUID fast-path returns a caller-supplied UUID without a
// tenancy check, so CreatePipeline must reject a source/destination connection
// UUID that belongs to a DIFFERENT workspace — otherwise a member of workspace A
// could wire a pipeline to workspace B's connection (cross-tenant IDOR). The
// caller's active workspace is wsScopeWS; wsScopeConnection stands in for a
// connection that is NOT in it.
func TestCreatePipeline_RejectsConnectionOutsideActiveWorkspace(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)

	// Plan/quota gate fails open with no plans catalogue → reach resolution.
	mock.ExpectQuery(`FROM plans`).WillReturnError(os.ErrClosed)

	// The cross-workspace guard: the supplied source connection UUID is NOT in
	// the active workspace (no row) → CreatePipeline must 400 and never INSERT.
	mock.ExpectQuery(`FROM connections WHERE id = \$1 AND workspace_id = \$2`).
		WithArgs(wsScopeConnection, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"one"}))

	body, _ := json.Marshal(map[string]any{
		"name":                 "x-ws-pipeline",
		"request":              "copy data",
		"source_connection_id": wsScopeConnection, // a UUID from ANOTHER workspace
	})
	r := wsScopeRouter(http.MethodPost, "/api/v1/pipelines", CreatePipeline)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/pipelines", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 (foreign-workspace connection rejected), got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

// connectionInWorkspace is the P2d tenancy guard. This pins BOTH branches so the
// guard can't silently regress to always-false (which would break every
// connection-bearing pipeline create) or always-true (which would reopen the
// IDOR): a row present → true (allow), empty id → false with no query (fail
// closed).
func TestConnectionInWorkspace(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	mock.ExpectQuery(`SELECT 1 FROM connections WHERE id = \$1 AND workspace_id = \$2`).
		WithArgs(wsScopeConnection, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"one"}).AddRow(1))

	if !connectionInWorkspace(db.GetDB(), wsScopeConnection, wsScopeWS) {
		t.Fatalf("expected true for an in-workspace connection")
	}
	// Empty id must fail closed WITHOUT issuing a query.
	if connectionInWorkspace(db.GetDB(), "", wsScopeWS) {
		t.Fatalf("expected false for an empty connection id")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}
