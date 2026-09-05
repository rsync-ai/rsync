package handlers

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// The chat "diagnose" path resolved its target with `p.created_by = <caller>`,
// which ignores the active workspace entirely. Two consequences, both reachable
// from a plain chat message:
//
//   - "why did my last run fail" after a workspace switch answered about a
//     pipeline in the workspace the user had LEFT — naming it, and quoting its
//     error text and row counts back into the chat.
//   - a teammate asking the same question about a shared pipeline they did not
//     create got "I couldn't find a pipeline to diagnose yet".
//
// The resolvers now bind workspace_id. These pin the argument the queries are
// scoped by, so a regression back to created_by fails on the SQL, not on a
// downstream assertion that might pass for another reason.

const (
	diagWS     = "99999999-9999-9999-9999-999999999999"
	diagPipeID = "11111111-1111-1111-1111-111111111111"
	diagExecID = "22222222-2222-2222-2222-222222222222"
)

func TestResolveDiagnosisTargetFromID_ScopedToActiveWorkspace(t *testing.T) {
	t.Run("pipeline outside the active workspace resolves to nothing", func(t *testing.T) {
		sqlDB, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer sqlDB.Close()

		// Both lookups are workspace-scoped, so a pasted id belonging to another
		// workspace matches neither — the caller gets "couldn't find", not a
		// diagnosis of someone else's pipeline.
		mock.ExpectQuery(`FROM pipelines p\s+WHERE p\.id = \$1 AND p\.workspace_id = \$2`).
			WithArgs(diagPipeID, diagWS).
			WillReturnRows(sqlmock.NewRows([]string{"id", "name"}))
		mock.ExpectQuery(`FROM executions e\s+JOIN pipelines p.*WHERE e\.id = \$1 AND p\.workspace_id = \$2`).
			WithArgs(diagPipeID, diagWS).
			WillReturnRows(sqlmock.NewRows([]string{"pipeline_id", "id", "name"}))

		pipelineID, execID, name := resolveDiagnosisTargetFromID(sqlDB, diagPipeID, diagWS)
		if pipelineID != "" || execID != "" || name != "" {
			t.Fatalf("expected no target for a pipeline outside the active workspace, got (%q,%q,%q)",
				pipelineID, execID, name)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("db expectations: %v", err)
		}
	})

	t.Run("pipeline in the active workspace resolves regardless of creator", func(t *testing.T) {
		sqlDB, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer sqlDB.Close()

		// No creator argument anywhere: a teammate who shares the workspace but
		// did not create the pipeline must resolve it.
		mock.ExpectQuery(`FROM pipelines p\s+WHERE p\.id = \$1 AND p\.workspace_id = \$2`).
			WithArgs(diagPipeID, diagWS).
			WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(diagPipeID, "shared pipeline"))
		mock.ExpectQuery(`SELECT id FROM executions WHERE pipeline_id = \$1`).
			WithArgs(diagPipeID).
			WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(diagExecID))

		pipelineID, execID, name := resolveDiagnosisTargetFromID(sqlDB, diagPipeID, diagWS)
		if pipelineID != diagPipeID || execID != diagExecID || name != "shared pipeline" {
			t.Fatalf("expected the shared pipeline to resolve, got (%q,%q,%q)", pipelineID, execID, name)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("db expectations: %v", err)
		}
	})
}

// "diagnose my last run" with no id: the candidate set must be the ACTIVE
// workspace's pipelines. Scoped by creator, this is what named the previous
// tenant's pipeline in chat right after a switch.
func TestFindMostRelevantPipeline_ScopedToActiveWorkspace(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer sqlDB.Close()

	mock.ExpectQuery(`FROM pipelines p\s+WHERE p\.workspace_id = \$1`).
		WithArgs(diagWS).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(diagPipeID, "failing pipeline"))
	mock.ExpectQuery(`SELECT id FROM executions WHERE pipeline_id = \$1`).
		WithArgs(diagPipeID).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(diagExecID))

	pipelineID, execID, name := findMostRelevantPipeline(sqlDB, diagWS)
	if pipelineID != diagPipeID || execID != diagExecID || name != "failing pipeline" {
		t.Fatalf("expected the active workspace's pipeline, got (%q,%q,%q)", pipelineID, execID, name)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

// Defense in depth: even if a resolver ever hands back a foreign pipeline,
// buildDiagnosisResponse re-checks the workspace before reading any evidence.
func TestDiagnosisPipelineInWorkspace(t *testing.T) {
	cases := []struct {
		name string
		rows *sqlmock.Rows
		want bool
	}{
		{"in the active workspace", sqlmock.NewRows([]string{"?column?"}).AddRow(1), true},
		{"outside the active workspace", sqlmock.NewRows([]string{"?column?"}), false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sqlDB, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock: %v", err)
			}
			defer sqlDB.Close()

			mock.ExpectQuery(`SELECT 1 FROM pipelines WHERE id = \$1 AND workspace_id = \$2`).
				WithArgs(diagPipeID, diagWS).
				WillReturnRows(tc.rows)

			if got := diagnosisPipelineInWorkspace(sqlDB, diagPipeID, diagWS); got != tc.want {
				t.Fatalf("expected %v, got %v", tc.want, got)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("db expectations: %v", err)
			}
		})
	}
}

// An empty workspace id must never widen the search: the handler bails instead of
// running an unscoped query. This is the fail-closed half of the fix — the
// workspace middleware falls back header → cookie → personal workspace, so an
// empty value here means resolution genuinely failed.
func TestMaybeHandleDiagnoseCommand_EmptyWorkspace_NotHandled(t *testing.T) {
	h := &ChatHandler{}
	if _, handled := h.maybeHandleDiagnoseCommand(t.Context(), "why did my last run fail", "", "trace-1"); handled {
		t.Fatal("expected the diagnose path to decline when no workspace could be resolved")
	}
}
