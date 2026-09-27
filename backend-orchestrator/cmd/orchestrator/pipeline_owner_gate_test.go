package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
)

// TestAssertPipelineOwnerUsesTheWorkspaceGate pins the CDC control plane on ONE
// authorization rule.
//
// assertPipelineOwner gates the four inline CDC routes in main.go (pause /
// resume / status / sink restart). It used to carry its own policy —
// `SELECT created_by FROM pipelines WHERE id = $1`, compared to the caller —
// while every handler-side CDC route authorized by workspace role. That is
// workspace-blind in BOTH directions, and both directions are asserted below:
//
//   - a user removed from the workspace keeps `created_by`, so the old gate let
//     them go on pausing and resuming a pipeline they no longer have any role on;
//   - a teammate holding a real role on a pipeline the workspace collectively
//     owns did not create it, so the old gate refused them.
//
// The assertions are on the QUERY as much as the verdict: sqlmock fails if the
// membership join is never run, which is what makes this test unable to pass
// against the old creator-only implementation no matter how the verdict lands.
func TestAssertPipelineOwnerUsesTheWorkspaceGate(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const caller = "11111111-1111-1111-1111-111111111111"
	const someoneElse = "22222222-2222-2222-2222-222222222222"
	const pipelineID = "33333333-3333-3333-3333-333333333333"
	const ws = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"

	// Direction 1: the removed creator. created_by is still the caller, so the
	// old gate returned true; the workspace gate sees a NULL role and refuses.
	t.Run("removed creator loses CDC control", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer db.Close()
		mock.ExpectQuery(`FROM pipelines p`).
			WithArgs(pipelineID, caller).
			WillReturnRows(sqlmock.NewRows([]string{"workspace_id", "created_by", "role"}).
				AddRow(ws, caller, nil))

		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Set("auth_user_id", caller)

		if assertPipelineOwner(c, db, pipelineID) {
			t.Fatal("a caller with no workspace membership kept control of the pipeline")
		}
		if w.Code != http.StatusForbidden {
			t.Fatalf("status = %d; want 403", w.Code)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("workspace membership was never queried — this is still the creator-only gate: %v", err)
		}
	})

	// Direction 2: the teammate. created_by is someone else, so the old gate
	// returned false and refused a legitimate workspace member.
	t.Run("workspace member who did not create it is allowed", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer db.Close()
		mock.ExpectQuery(`FROM pipelines p`).
			WithArgs(pipelineID, caller).
			WillReturnRows(sqlmock.NewRows([]string{"workspace_id", "created_by", "role"}).
				AddRow(ws, someoneElse, "admin"))

		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Set("auth_user_id", caller)

		if !assertPipelineOwner(c, db, pipelineID) {
			t.Fatalf("a workspace admin was refused; status=%d body=%s", w.Code, w.Body.String())
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("workspace membership was never queried: %v", err)
		}
	})

	// A viewer must not drive a mutating CDC action, creator or not.
	t.Run("viewer is refused", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer db.Close()
		mock.ExpectQuery(`FROM pipelines p`).
			WithArgs(pipelineID, caller).
			WillReturnRows(sqlmock.NewRows([]string{"workspace_id", "created_by", "role"}).
				AddRow(ws, caller, "viewer"))

		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Set("auth_user_id", caller)

		if assertPipelineOwner(c, db, pipelineID) {
			t.Fatal("a viewer was allowed to drive a mutating CDC action")
		}
		if w.Code != http.StatusForbidden {
			t.Fatalf("status = %d; want 403", w.Code)
		}
	})
}
