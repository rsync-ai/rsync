package executor

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// When no transforms are present in the task payload, applyTransformsToData must
// fall back to loading producer transforms from transform_definitions. Without
// this, batch pipelines silently run untransformed (PII-leak risk).
func TestApplyTransformsToData_LoadsProducerTransformsFromDB(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	pipelineID := "11111111-1111-1111-1111-111111111111"
	execID := "22222222-2222-2222-2222-222222222222"
	tableName := "public.users"

	// The DB fallback query returns one producer transform (mask_pii on email).
	mock.ExpectQuery("SELECT id::text, transform_order, transform_config, enabled\\s+FROM transform_definitions").
		WithArgs(pipelineID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "transform_order", "transform_config", "enabled"}).
			AddRow(
				"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
				1,
				[]byte(`{"operation":"mask_pii","column":"email","mask_type":"hash"}`),
				true,
			))

	// The per-transform execution log upsert is best-effort; accept it.
	mock.ExpectExec("INSERT INTO transform_execution_logs").
		WillReturnResult(sqlmock.NewResult(1, 1))

	// Task has NO transforms in Params/Payload — forces the DB fallback.
	task := ExecutorTask{PipelineID: pipelineID}

	in := []map[string]interface{}{
		{"email": "alice@example.com", "name": "Alice"},
	}

	out, err := applyTransformsToData(context.Background(), db, execID, in, task, tableName)
	if err != nil {
		t.Fatalf("applyTransformsToData error: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 output row, got %d", len(out))
	}
	masked, _ := out[0]["email"].(string)
	if masked == "alice@example.com" || masked == "" {
		t.Fatalf("expected email masked via DB-loaded transform, got %#v", out[0]["email"])
	}
	if out[0]["name"] != "Alice" {
		t.Fatalf("expected non-masked column preserved, got %#v", out[0]["name"])
	}
}

// A DB error while loading producer transforms must fail closed (abort the
// export) rather than silently letting data flow untransformed.
func TestApplyTransformsToData_DBErrorFailsClosed(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	pipelineID := "11111111-1111-1111-1111-111111111111"
	execID := "22222222-2222-2222-2222-222222222222"

	mock.ExpectQuery("FROM transform_definitions").
		WithArgs(pipelineID).
		WillReturnError(context.DeadlineExceeded)

	task := ExecutorTask{PipelineID: pipelineID}
	in := []map[string]interface{}{{"email": "a@b.com"}}

	_, err = applyTransformsToData(context.Background(), db, execID, in, task, "public.users")
	if err == nil {
		t.Fatalf("expected fail-closed error on DB load failure, got nil")
	}
}

// A non-UUID pipeline ID (adhoc/E2E runs) must skip the DB fallback entirely and
// return data unchanged — no DB query fired.
func TestApplyTransformsToData_NonUUIDSkipsDBFallback(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	task := ExecutorTask{PipelineID: "adhoc-test-pipeline"}
	in := []map[string]interface{}{{"email": "a@b.com"}}

	out, err := applyTransformsToData(context.Background(), db, "exec-1", in, task, "public.users")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out[0]["email"] != "a@b.com" {
		t.Fatalf("expected data unchanged, got %#v", out[0]["email"])
	}
	// No DB query should have been expected/fired.
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unexpected DB interaction: %v", err)
	}
}
