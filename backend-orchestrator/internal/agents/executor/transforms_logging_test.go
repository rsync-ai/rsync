package executor

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestApplyTransformsToData_LogsPerTransformBestEffort(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	pipelineID := "11111111-1111-1111-1111-111111111111"
	execID := "22222222-2222-2222-2222-222222222222"
	tableName := "public.users"

	// Expect one upsert per transform in order.
	mock.ExpectExec("INSERT INTO transform_execution_logs").
		WithArgs(
			pipelineID,
			execID,
			tableName,
			"", // transform_id (none provided)
			1,  // transform_order
			"exclude_columns",
			"success",
			nil,              // error_message
			1,                // input_rows
			1,                // output_rows
			sqlmock.AnyArg(), // duration_ms
			sqlmock.AnyArg(), // config_snapshot
		).
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectExec("INSERT INTO transform_execution_logs").
		WithArgs(
			pipelineID,
			execID,
			tableName,
			"",
			2,
			"truncate",
			"success",
			nil,
			1,
			1,
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(1, 1))

	task := ExecutorTask{
		PipelineID: pipelineID,
		Params: map[string]interface{}{
			"metadata": map[string]interface{}{
				"transforms": []interface{}{
					map[string]interface{}{
						"type":  "exclude_columns",
						"order": 1,
						"config": map[string]interface{}{
							"columns": []interface{}{"password"},
						},
					},
					map[string]interface{}{
						"type":  "truncate",
						"order": 2,
						"config": map[string]interface{}{
							"column":     "name",
							"max_length": 3,
						},
					},
				},
			},
		},
	}

	in := []map[string]interface{}{
		{"name": "abcdef", "password": "secret"},
	}

	out, err := applyTransformsToData(context.Background(), db, execID, in, task, tableName)
	if err != nil {
		t.Fatalf("applyTransformsToData error: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 output row, got %d", len(out))
	}
	if out[0]["name"] != "abc" {
		t.Fatalf("expected name truncated to abc, got %#v", out[0]["name"])
	}
	if _, ok := out[0]["password"]; ok {
		t.Fatalf("expected password excluded")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}
