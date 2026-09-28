package executor

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestRewindCheckpointsBehindNegativeAcksRewindsEachLostExecution(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT DISTINCT c.position->>'execution_id'")).
		WithArgs("pipe-1", "this-run").
		WillReturnRows(sqlmock.NewRows([]string{"execution_id"}).AddRow("lost-a").AddRow("lost-b"))
	for _, id := range []string{"lost-a", "lost-b"} {
		mock.ExpectBegin()
		mock.ExpectExec("UPDATE pipeline_checkpoints SET position = position->'run_start'").
			WithArgs("pipe-1", id).WillReturnResult(sqlmock.NewResult(0, 2))
		mock.ExpectExec("DELETE FROM pipeline_checkpoints").
			WithArgs("pipe-1", id).WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit()
	}
	if got := rewindCheckpointsBehindNegativeAcks(context.Background(), db, "pipe-1", "this-run"); got != 6 {
		t.Fatalf("rewound %d checkpoints, want 6 (2 restored + 1 deleted per lost execution)", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestRewindCheckpointsBehindNegativeAcksIsBestEffort(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("SELECT DISTINCT").WillReturnError(errors.New("relation does not exist"))
	if got := rewindCheckpointsBehindNegativeAcks(context.Background(), db, "pipe-1", "this-run"); got != 0 {
		t.Fatalf("a failed lookup must rewind nothing, got %d", got)
	}
	if got := rewindCheckpointsBehindNegativeAcks(context.Background(), nil, "pipe-1", "this-run"); got != 0 {
		t.Fatalf("no db must rewind nothing, got %d", got)
	}
}

// The query must only pick executions that still own a checkpoint, never the
// running one, and only for batches no later ack wrote. Checked against a real
// Postgres when written; these pin the clauses that make it so.
func TestLostBatchExecutionsQueryShape(t *testing.T) {
	for _, clause := range []string{
		"FROM pipeline_checkpoints c",
		"NOT IN ('', $2)",
		"n.execution_id::text = c.position->>'execution_id'",
		"n.rows_written = 0 AND COALESCE(n.last_error, '') <> ''",
		"AND p.rows_written > 0",
	} {
		if !strings.Contains(lostBatchExecutionsQuery, clause) {
			t.Errorf("lostBatchExecutionsQuery lost %q", clause)
		}
	}
}

// Both halves must be wired: the heal before any table reads its checkpoint on a
// non-reload run, and the rewind on the source-count silent-drop failure.
func TestResumeHealAndProbeDropRewindAreWired(t *testing.T) {
	batch := executeBatchDataTransferBody(t)
	heal := strings.Index(batch, "rewindCheckpointsBehindNegativeAcks(ctx, a.db, task.PipelineID, executionID)")
	reload := strings.Index(batch, "cdc.DeleteCheckpointsNotFromExecution(")
	firstRead := strings.Index(batch, "existingCheckpoint")
	if heal < 0 {
		t.Fatal("a non-reload run must rewind earlier runs' lost batches before resuming")
	}
	if reload < 0 || firstRead < 0 || !(reload < heal && heal < firstRead) {
		t.Errorf("the heal must sit in the reload gate's else, before any checkpoint read (reload=%d heal=%d read=%d)", reload, heal, firstRead)
	}
	probe := strings.Index(batch, "CheckForSilentDrop(")
	if probe < 0 {
		t.Fatal("CheckForSilentDrop call not found; if it moved, update this test")
	}
	end := strings.Index(batch[probe:], "Result:")
	if end < 0 || !strings.Contains(batch[probe:probe+end], "drop.Reason + rewindAfterLostBatches(") {
		t.Error("a source-count silent drop must rewind this run's checkpoints, or Resume skips the missing rows")
	}
}

func executeBatchDataTransferBody(t *testing.T) string {
	t.Helper()
	fset := token.NewFileSet()
	src, err := os.ReadFile("executor.go")
	if err != nil {
		t.Fatalf("read executor.go: %v", err)
	}
	f, err := parser.ParseFile(fset, "executor.go", src, 0)
	if err != nil {
		t.Fatalf("parse executor.go: %v", err)
	}
	for _, d := range f.Decls {
		if fd, ok := d.(*ast.FuncDecl); ok && fd.Name.Name == "executeBatchDataTransfer" {
			return string(src[fset.Position(fd.Body.Pos()).Offset:fset.Position(fd.Body.End()).Offset])
		}
	}
	t.Fatal("executeBatchDataTransfer not found in executor.go; if it moved or was renamed, update this test")
	return ""
}
