package cdc

import (
	"context"
	"reflect"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

func TestRunStartPosition(t *testing.T) {
	const exec = "e2"
	earlier := map[string]interface{}{"execution_id": "e1", "offset": 500.0, "key_ordinal": 5.0,
		RunStartKey: map[string]interface{}{"offset": 0.0}}
	cases := []struct {
		name     string
		existing *Checkpoint
		want     map[string]interface{}
	}{
		{"no checkpoint", nil, map[string]interface{}{}},
		{"checkpoint with no position", &Checkpoint{}, map[string]interface{}{}},
		{"an earlier run's checkpoint, minus its own run_start", &Checkpoint{Position: earlier},
			map[string]interface{}{"execution_id": "e1", "offset": 500.0, "key_ordinal": 5.0}},
		{"a pre-execution_id checkpoint", &Checkpoint{Position: map[string]interface{}{"offset": 7.0}},
			map[string]interface{}{"offset": 7.0}},
		{"this run's chunk carries its run_start forward", &Checkpoint{Position: map[string]interface{}{
			"execution_id": exec, "offset": 900.0, RunStartKey: map[string]interface{}{"offset": 500.0}}},
			map[string]interface{}{"offset": 500.0}},
		{"this run's chunk carries 'none before' forward", &Checkpoint{Position: map[string]interface{}{
			"execution_id": exec, RunStartKey: map[string]interface{}{}}}, map[string]interface{}{}},
		{"this run's chunk written before run_start existed is unknown", &Checkpoint{Position: map[string]interface{}{
			"execution_id": exec, "offset": 900.0}}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := RunStartPosition(tc.existing, exec)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %#v, want %#v", got, tc.want)
			}
		})
	}
	if _, still := earlier[RunStartKey]; !still {
		t.Fatal("RunStartPosition mutated the checkpoint it was given")
	}
}

// B-RESUME-DLQ: a run whose batches the sink dead-lettered must put its tables back
// where it started, restoring what it can and removing checkpoints it created.
func TestRewindCheckpointsOfExecution(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectBegin()
	mock.ExpectExec(`UPDATE pipeline_checkpoints SET position = position->'run_start', updated_at = NOW\(\)\s+WHERE pipeline_id = \$1 AND position->>'execution_id' = \$2\s+AND jsonb_typeof\(position->'run_start'\) = 'object' AND position->'run_start' <> '\{\}'::jsonb`).
		WithArgs("p1", "e1").WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectExec(`DELETE FROM pipeline_checkpoints\s+WHERE pipeline_id = \$1 AND position->>'execution_id' = \$2 AND position->'run_start' = '\{\}'::jsonb`).
		WithArgs("p1", "e1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	n, err := RewindCheckpointsOfExecution(context.Background(), db, "p1", "e1")
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("rewound %d, want 3", n)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestRewindCheckpointsOfExecutionRollsBackOnError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectBegin()
	mock.ExpectExec(`UPDATE pipeline_checkpoints`).WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectExec(`DELETE FROM pipeline_checkpoints`).WillReturnError(context.DeadlineExceeded)
	mock.ExpectRollback()
	if _, err := RewindCheckpointsOfExecution(context.Background(), db, "p1", "e1"); err == nil {
		t.Fatal("want an error")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// Without an execution id there is no way to tell this run's checkpoints from any
// other run's, so nothing may be touched.
func TestRewindCheckpointsOfExecutionWithoutIDTouchesNothing(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, id := range []string{"", "  "} {
		if n, err := RewindCheckpointsOfExecution(context.Background(), db, "p1", id); err != nil || n != 0 {
			t.Fatalf("id %q: n=%d err=%v", id, n, err)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
