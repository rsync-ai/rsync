package executor

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// B-RESUME-DLQ. Checkpoints advance when a batch is PRODUCED; the sink can still
// dead-letter it afterwards. The run then fails on ack-ledger evidence, but its
// checkpoint is already past the lost batch, so Resume read 0 rows and the gap
// stayed. The fix has two halves and both must stay wired.
func TestResumeAfterLostBatchesWiring(t *testing.T) {
	fset := token.NewFileSet()
	src, err := os.ReadFile("executor.go")
	if err != nil {
		t.Fatalf("read executor.go: %v", err)
	}
	f, err := parser.ParseFile(fset, "executor.go", src, 0)
	if err != nil {
		t.Fatalf("parse executor.go: %v", err)
	}
	var batch string
	for _, d := range f.Decls {
		if fd, ok := d.(*ast.FuncDecl); ok && fd.Name.Name == "executeBatchDataTransfer" {
			batch = string(src[fset.Position(fd.Body.Pos()).Offset:fset.Position(fd.Body.End()).Offset])
		}
	}
	if batch == "" {
		t.Fatal("executeBatchDataTransfer not found in executor.go; if it moved or was renamed, update this test")
	}

	// 1. Every checkpoint records where its table stood before this run.
	if !strings.Contains(batch, "runStart = cdc.RunStartPosition(existingCheckpoint, executionID)") {
		t.Error("the resume path must derive run_start from the checkpoint it loaded")
	}
	if !strings.Contains(batch, "checkpointPosition[cdc.RunStartKey] = runStart") {
		t.Error("the checkpoint save must carry run_start, or a lossy run has nothing to rewind to")
	}

	// 2. The ack-evidenced drop rewinds them before failing the run.
	drop := strings.Index(batch, "case decision.AckEvidencedDrop:")
	next := strings.Index(batch, "case decision.UnverifiedCompletion:")
	if drop < 0 || next < drop {
		t.Fatal("AckEvidencedDrop branch not found; if it moved, update this test")
	}
	if !strings.Contains(batch[drop:next], "rewindAfterLostBatches(") {
		t.Error("the AckEvidencedDrop branch must rewind this run's checkpoints, or Resume skips the lost rows")
	}
}

func TestRewindAfterLostBatchesTellsTheUserWhatResumeDoes(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectBegin()
	mock.ExpectExec(`UPDATE pipeline_checkpoints`).WithArgs("p1", "e1").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`DELETE FROM pipeline_checkpoints`).WithArgs("p1", "e1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	got := rewindAfterLostBatches(context.Background(), db, "p1", "e1")
	if !strings.Contains(got, "1 table(s) were rewound") || !strings.Contains(got, "Resume re-reads") {
		t.Fatalf("got %q", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestRewindAfterLostBatchesFailureSaysReload(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectBegin().WillReturnError(errors.New("db down"))
	if got := rewindAfterLostBatches(context.Background(), db, "p1", "e1"); !strings.Contains(got, "run a Reload") {
		t.Fatalf("got %q", got)
	}
}

func TestRewindAfterLostBatchesNothingToRewindAddsNothing(t *testing.T) {
	if got := rewindAfterLostBatches(context.Background(), nil, "p1", "e1"); got != "" {
		t.Fatalf("got %q", got)
	}
}
