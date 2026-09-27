package cdc

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// ---------------------------------------------------------------- S3: prechecks

// The whole point of a precheck is that it can FAIL. A query that errors out
// checked nothing, and reporting that as a pass sends the pipeline on to die
// inside Debezium with the error this validator exists to prevent.
func TestEvaluateBinlogRowImage_AFailedQueryIsNotAPass(t *testing.T) {
	queryErr := errors.New("Access denied; you need SUPER or SYSTEM_VARIABLES_ADMIN")

	got, err := evaluateBinlogRowImage(queryErr, "")

	if err == nil {
		t.Fatal("a failed binlog_row_image query was reported as a successful check")
	}
	if !errors.Is(err, queryErr) {
		t.Errorf("the underlying cause was dropped: %v", err)
	}
	if got != nil {
		t.Errorf("a failed check must not also produce validation errors: %v", got)
	}
}

func TestEvaluateBinlogRowImage_MinimalIsABlockingError(t *testing.T) {
	got, err := evaluateBinlogRowImage(nil, "MINIMAL")
	if err != nil {
		t.Fatalf("unexpected hard failure: %v", err)
	}
	if len(got) != 1 || got[0].Code != "MYSQL_BINLOG_ROW_IMAGE" || got[0].Severity != "error" {
		t.Fatalf("binlog_row_image=MINIMAL must be a blocking error, got %+v", got)
	}
}

func TestEvaluateBinlogRowImage_FullPasses(t *testing.T) {
	got, err := evaluateBinlogRowImage(nil, "FULL")
	if err != nil || len(got) != 0 {
		t.Fatalf("FULL must pass cleanly, got %+v / %v", got, err)
	}
}

// A server that has no such variable (MariaDB, MySQL 5.5) answered the question -
// it just cannot answer THIS one. That is a warning, not a silent pass and not a
// hard failure of the whole validation run.
func TestEvaluateBinlogRowImage_MissingVariableWarnsRatherThanPassingSilently(t *testing.T) {
	got, err := evaluateBinlogRowImage(sql.ErrNoRows, "")
	if err != nil {
		t.Fatalf("a server without the variable must not fail the run: %v", err)
	}
	if len(got) != 1 || got[0].Severity != "warning" || got[0].Code != "MYSQL_BINLOG_ROW_IMAGE_UNKNOWN" {
		t.Fatalf("an unverifiable row image must be surfaced as a warning, got %+v", got)
	}
}

// --------------------------------------------------------------- S4: resume state

func checkpointRows(position string) *sqlmock.Rows {
	now := time.Now()
	return sqlmock.NewRows([]string{
		"id", "pipeline_id", "connection_id", "source_table",
		"cdc_resource_id", "position", "created_at", "updated_at",
	}).AddRow("cp-1", "p-1", "conn-1", "public.orders", nil, []byte(position), now, now)
}

// A checkpoint whose position cannot be decoded must not read like a table that
// has no checkpoint. The resume path treats a nil Position as "start over": full
// re-read, key_ordinal back to 0, part-000000 written over the previous sweep's
// objects. The caller has to be able to tell the two apart.
func TestGetCheckpointForTable_CorruptPositionIsAnErrorNotAnEmptyResumeState(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT id, pipeline_id").
		WithArgs("p-1", "public.orders").
		WillReturnRows(checkpointRows(`{"batch_idx": 7, "offset":`)) // truncated JSON

	cp, err := GetCheckpointForTable(context.Background(), db, "p-1", "public.orders")

	if err == nil {
		t.Fatal("a corrupt checkpoint position was reported as a successful read")
	}
	if !errors.Is(err, ErrCheckpointPositionUnreadable) {
		t.Errorf("caller cannot classify the failure: %v", err)
	}
	if cp != nil {
		t.Errorf("an unreadable checkpoint must not be returned as usable resume state: %+v", cp)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// The control: the same code path must still decode a good position, or the test
// above would pass for a function that simply always fails.
func TestGetCheckpointForTable_ValidPositionStillDecodes(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT id, pipeline_id").
		WithArgs("p-1", "public.orders").
		WillReturnRows(checkpointRows(`{"batch_idx": 7, "offset": 700, "table_complete": true}`))

	cp, err := GetCheckpointForTable(context.Background(), db, "p-1", "public.orders")
	if err != nil {
		t.Fatalf("a valid position failed to decode: %v", err)
	}
	if cp == nil || cp.Position == nil {
		t.Fatal("valid checkpoint came back empty")
	}
	if cp.Position["batch_idx"] != float64(7) {
		t.Errorf("position did not decode: %+v", cp.Position)
	}
}

// "No checkpoint at all" keeps its own distinct answer: (nil, nil).
func TestGetCheckpointForTable_NoRowsIsStillNotAnError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT id, pipeline_id").
		WithArgs("p-1", "public.orders").
		WillReturnError(sql.ErrNoRows)

	cp, err := GetCheckpointForTable(context.Background(), db, "p-1", "public.orders")
	if err != nil || cp != nil {
		t.Fatalf("a table with no checkpoint must read as (nil, nil), got %+v / %v", cp, err)
	}
}

func TestGetCheckpoints_CorruptPositionFailsTheWholeRead(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT id, pipeline_id").
		WithArgs("p-1").
		WillReturnRows(checkpointRows(`not json at all`))

	cps, err := GetCheckpoints(context.Background(), db, "p-1")
	if err == nil {
		t.Fatalf("a corrupt position was returned as %d usable checkpoints", len(cps))
	}
	if !errors.Is(err, ErrCheckpointPositionUnreadable) {
		t.Errorf("caller cannot classify the failure: %v", err)
	}
}
