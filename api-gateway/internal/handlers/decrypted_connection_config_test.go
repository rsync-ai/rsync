package handlers

import (
	"database/sql"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// TestDecryptedConnectionConfig_ScopedToWorkspace proves the credential-decrypt
// helper scopes by workspace_id: a connection id from another tenant matches no
// row and the helper errors out (credential-blob IDOR guard) — and that the
// caller's workspace id is actually bound into the query.
func TestDecryptedConnectionConfig_ScopedToWorkspace(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer mockDB.Close()

	// The WithArgs(connID, workspaceID) match only succeeds if BOTH the id and
	// the workspace_id are bound — i.e. the scoping predicate is present.
	mock.ExpectQuery(`SELECT config, connector_type FROM connections WHERE id = \$1 AND workspace_id = \$2`).
		WithArgs("conn-owned-by-A", "workspace-B").
		WillReturnError(sql.ErrNoRows)

	var ct string
	cfg, err := decryptedConnectionConfig(mockDB, "workspace-B", "conn-owned-by-A", &ct)
	if err == nil {
		t.Fatalf("expected error for a foreign-workspace connection, got cfg=%v", cfg)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("workspace-scoped query was not executed as expected: %v", err)
	}
}
