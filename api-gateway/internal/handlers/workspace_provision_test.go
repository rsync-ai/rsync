package handlers

import (
	"errors"
	"strings"
	"testing"

	"api-gateway/internal/db"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestPersonalWorkspaceNameAndSlug(t *testing.T) {
	if got := personalWorkspaceName("alice@example.com"); got != "alice's workspace" {
		t.Fatalf("name = %q, want %q", got, "alice's workspace")
	}
	// Uppercase folds to lower and non-alphanumerics become hyphens; a short
	// random suffix guarantees uniqueness.
	slug := personalWorkspaceSlug("Alice.Smith@example.com")
	if !strings.HasPrefix(slug, "alice-smith-") {
		t.Fatalf("slug = %q, want prefix %q", slug, "alice-smith-")
	}
	if slug == personalWorkspaceSlug("Alice.Smith@example.com") {
		t.Fatalf("slug suffix should be random, got identical: %q", slug)
	}
}

func TestProvisionPersonalWorkspace_Success(t *testing.T) {
	_, mock, cleanup := setupMockDB(t)
	defer cleanup()

	mock.ExpectQuery(`INSERT INTO workspaces`).
		WithArgs("alice's workspace", sqlmock.AnyArg(), "user-1").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("ws-1"))
	mock.ExpectExec(`INSERT INTO workspace_members`).
		WithArgs("ws-1", "user-1").
		WillReturnResult(sqlmock.NewResult(1, 1))

	wsID, err := provisionPersonalWorkspace(db.GetDB(), "user-1", "alice@example.com")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if wsID != "ws-1" {
		t.Fatalf("want ws-1, got %q", wsID)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestProvisionPersonalWorkspace_WorkspaceInsertFails_NoMembership(t *testing.T) {
	_, mock, cleanup := setupMockDB(t)
	defer cleanup()

	// Workspace insert fails → membership insert must NOT run.
	mock.ExpectQuery(`INSERT INTO workspaces`).
		WithArgs("bob's workspace", sqlmock.AnyArg(), "user-2").
		WillReturnError(errors.New("boom"))

	wsID, err := provisionPersonalWorkspace(db.GetDB(), "user-2", "bob@example.com")
	if err == nil {
		t.Fatalf("expected error from workspace insert")
	}
	if wsID != "" {
		t.Fatalf("want empty wsID on failure, got %q", wsID)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}
