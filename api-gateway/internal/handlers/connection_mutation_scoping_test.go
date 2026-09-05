package handlers

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/rsync-ai/shared/crypto"
)

// P2c: connection CRUD (Get / Update / Delete) and the shared resolveConnectionID
// resolver must be scoped to the ACTIVE WORKSPACE, not to created_by / user_id.
// P1 already made ListConnections workspace-scoped; these pin the rest so a
// workspace MEMBER who is not the creator can read, edit, and delete a SHARED
// connection. The caller (wsScopeUser) is a member, NOT the creator
// (connCreator); the queries must bind the active workspace (wsScopeWS), never
// the caller's user id. The old user_id scoping would 0-row → 404/500 a
// teammate's connection.
//
// wsScopeUser / wsScopeWS / wsScopeRouter / wsScopeMockDB come from
// workspace_scoping_test.go; gateRoleRows from workspace_mutation_scoping_test.go
// (all the same package).

const (
	wsScopeConnection = "44444444-4444-4444-4444-444444444444"
	// connCreator is a DIFFERENT user than wsScopeUser — proving a non-creator
	// member of the workspace can address the shared connection.
	connCreator = "99999999-9999-9999-9999-999999999999"
)

func TestGetConnection_MemberCanReadSharedConnection(t *testing.T) {
	// GetConnection decrypts the stored config; crypto fatals outside dev unless
	// a key is configured.
	t.Setenv("ENVIRONMENT", "development")
	t.Setenv("ENCRYPTION_KEY", "0123456789abcdef0123456789abcdef")

	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	enc, err := crypto.EncryptString("{}")
	if err != nil {
		t.Fatalf("encrypt config: %v", err)
	}
	now := time.Now()

	// Read is scoped by workspace_id (UUID branch), bound as $2 = active
	// workspace — never user_id. The row's user_id is a DIFFERENT creator.
	mock.ExpectQuery(`FROM connections\s+WHERE id = \$1 AND workspace_id = \$2`).
		WithArgs(wsScopeConnection, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "user_id", "name", "type", "connector_type", "connector_version",
			"sync_mode", "cdc_mode", "description", "config", "status",
			"last_tested_at", "last_test_status", "last_test_error", "created_at", "updated_at",
		}).AddRow(
			wsScopeConnection, connCreator, "shared-conn", "source", "postgresql", nil,
			nil, nil, nil, enc, "active",
			nil, nil, nil, now, now,
		))

	r := wsScopeRouter(http.MethodGet, "/api/v1/connections/:id", GetConnection)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/connections/"+wsScopeConnection, nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 (member reads shared connection), got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

func TestUpdateConnection_MemberCanEditSharedConnection(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)

	// Authz gate (requireResourceRole): proves the connection lives in the active
	// workspace and the caller holds >= member. "member" = a non-owner teammate.
	mock.ExpectQuery(`SELECT wm\.role\s+FROM connections r`).
		WithArgs(wsScopeConnection, wsScopeUser, wsScopeWS).
		WillReturnRows(gateRoleRows("member"))

	// Internal-connector guard SELECT is workspace-scoped now, not user-scoped.
	mock.ExpectQuery(`SELECT connector_type FROM connections WHERE id = \$1 AND workspace_id = \$2`).
		WithArgs(wsScopeConnection, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"connector_type"}).AddRow("postgresql"))

	// Name-only update → no config/crypto path. The write scopes by workspace_id.
	mock.ExpectExec(`UPDATE connections SET name = \$1, updated_at = \$2 WHERE id = \$3 AND workspace_id = \$4`).
		WithArgs("renamed-by-teammate", sqlmock.AnyArg(), wsScopeConnection, wsScopeWS).
		WillReturnResult(sqlmock.NewResult(0, 1))

	body, _ := json.Marshal(map[string]any{"name": "renamed-by-teammate"})
	r := wsScopeRouter(http.MethodPatch, "/api/v1/connections/:id", UpdateConnection)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPatch, "/api/v1/connections/"+wsScopeConnection, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 (member edits shared connection), got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

func TestDeleteConnection_MemberCanDeleteSharedConnection(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)

	// Authz gate.
	mock.ExpectQuery(`SELECT wm\.role\s+FROM connections r`).
		WithArgs(wsScopeConnection, wsScopeUser, wsScopeWS).
		WillReturnRows(gateRoleRows("member"))

	// Pipeline-usage guard is scoped only by connection id (unchanged) → 0 uses.
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM pipelines`).
		WithArgs(wsScopeConnection).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	// Delete scopes by workspace_id; one row affected → 200.
	mock.ExpectExec(`DELETE FROM connections\s+WHERE id = \$1 AND workspace_id = \$2`).
		WithArgs(wsScopeConnection, wsScopeWS).
		WillReturnResult(sqlmock.NewResult(0, 1))

	r := wsScopeRouter(http.MethodDelete, "/api/v1/connections/:id", DeleteConnection)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodDelete, "/api/v1/connections/"+wsScopeConnection, nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 (member deletes shared connection), got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}
