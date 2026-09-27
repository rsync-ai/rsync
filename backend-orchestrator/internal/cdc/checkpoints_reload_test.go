package cdc

import (
	"context"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// A reload keeps the checkpoints its own execution wrote, so a chunk
// continuation resumes instead of restarting the table at offset 0.
func TestDeleteCheckpointsNotFromExecutionKeepsThisRun(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectExec(`DELETE FROM pipeline_checkpoints WHERE pipeline_id = \$1 AND COALESCE\(position->>'execution_id', ''\) <> \$2`).
		WithArgs("p1", "e1").WillReturnResult(sqlmock.NewResult(0, 3))
	if err := DeleteCheckpointsNotFromExecution(context.Background(), db, "p1", "e1"); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestDeleteCheckpointsNotFromExecutionWithoutIDDeletesAll(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectExec(`DELETE FROM pipeline_checkpoints WHERE pipeline_id = \$1$`).
		WithArgs("p1").WillReturnResult(sqlmock.NewResult(0, 3))
	if err := DeleteCheckpointsNotFromExecution(context.Background(), db, "p1", " "); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
