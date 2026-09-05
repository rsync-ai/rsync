package workers

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// These are the regression guards for KI-BATCH-RESUME-MSG-UNDERCOUNT.
//
// The bug: the STAGE_COMPLETED message was formatted from
// response.RowsProcessed, which covers only ONE executor dispatch. The V2
// workflow re-dispatches the executor (chunked continuation, large-table
// resume, HITL repair, activity retry), each dispatch emits its own
// STAGE_COMPLETED, and every projection of `message` is last-write-wins
// (workers/executor.go per-task emit → event_projector rewrites `message =
// EXCLUDED.message`). So a tail dispatch that found no new rows overwrote the
// run's message with "Processed 0 rows" for a run that had actually moved 42k.
//
// runRowsProcessed is the fix: it prefers the run TOTAL landed in the durable
// ack ledger (pipeline_batch_acks, summed over pipeline_id + execution_id —
// exactly the columns of idx_batch_acks_pipeline_exec) and falls back to the
// dispatch's own count whenever the ledger cannot answer.

func TestRunRowsProcessed_PrefersLedgerRunTotalOverThisDispatch(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT COALESCE\\(SUM\\(rows_written\\), 0\\)").
		WithArgs("pipe-1", "exec-1").
		WillReturnRows(sqlmock.NewRows([]string{"sum"}).AddRow(int64(42000)))

	// The tail dispatch found 0 new rows — the exact shape that produced the
	// bogus "Processed 0 rows" message.
	got := runRowsProcessed(context.Background(), db, "pipe-1", "exec-1", "batch", 0)
	if got != 42000 {
		t.Fatalf("expected the run total 42000 from the ack ledger, got %d", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sqlmock expectations: %v", err)
	}
}

func TestRunRowsProcessed_NeverReportsFewerRowsThanThisDispatchProved(t *testing.T) {
	// The ledger is written by the sink asynchronously, so it can legitimately
	// lag behind the dispatch that just finished. A lagging ledger must never
	// shrink the reported count below what this dispatch already demonstrated —
	// the helper takes the MAX, not the ledger unconditionally.
	cases := []struct {
		name     string
		ledger   int64
		taskRows int
		wantRows int64
	}{
		{name: "ledger still empty", ledger: 0, taskRows: 500, wantRows: 500},
		{name: "ledger lagging behind", ledger: 120, taskRows: 500, wantRows: 500},
		{name: "ledger caught up exactly", ledger: 500, taskRows: 500, wantRows: 500},
		{name: "ledger ahead (earlier dispatches counted)", ledger: 900, taskRows: 500, wantRows: 900},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock.New: %v", err)
			}
			defer db.Close()

			mock.ExpectQuery("SELECT COALESCE\\(SUM\\(rows_written\\), 0\\)").
				WithArgs("pipe-1", "exec-1").
				WillReturnRows(sqlmock.NewRows([]string{"sum"}).AddRow(tc.ledger))

			got := runRowsProcessed(context.Background(), db, "pipe-1", "exec-1", "batch", tc.taskRows)
			if got != tc.wantRows {
				t.Fatalf("ledger=%d dispatch=%d: want %d, got %d", tc.ledger, tc.taskRows, tc.wantRows, got)
			}
		})
	}
}

// TestRunRowsProcessed_DoesNotConsultLedgerForNonBatch is the guard that keeps
// this fix from colliding with the CDC ack-ledger FK fix
// (KI-CDC-ACKLEDGER-FK-MISMATCH). Once the sink's CDC path successfully writes
// pipeline_batch_acks rows, those rows are keyed on the CDC convention
// execution_id == pipeline_id and count STREAMING events, not run rows. Summing
// them into a batch-style "Processed N rows" message would leak one convention's
// numbers into the other's UI string. The pipelineType gate is what prevents it,
// so the ledger must not be queried at all for non-batch pipelines.
func TestRunRowsProcessed_DoesNotConsultLedgerForNonBatch(t *testing.T) {
	for _, pipelineType := range []string{"cdc", "streaming", "", "CDC"} {
		t.Run("type="+pipelineType, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock.New: %v", err)
			}
			defer db.Close()

			// No ExpectQuery is registered: sqlmock fails the test if the helper
			// issues ANY query. That is the assertion.
			got := runRowsProcessed(context.Background(), db, "pipe-1", "exec-1", pipelineType, 7)
			if got != 7 {
				t.Fatalf("%s: expected the dispatch count 7 passed through, got %d", pipelineType, got)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("%s: unmet sqlmock expectations: %v", pipelineType, err)
			}
		})
	}
}

// TestRunRowsProcessed_FallsBackWhenLedgerUnavailable covers the fail-soft
// contract: a missing db handle, blank identifiers, or a query error must all
// degrade to the dispatch's own count rather than reporting 0 or panicking.
func TestRunRowsProcessed_FallsBackWhenLedgerUnavailable(t *testing.T) {
	t.Run("nil db", func(t *testing.T) {
		if got := runRowsProcessed(context.Background(), nil, "pipe-1", "exec-1", "batch", 33); got != 33 {
			t.Fatalf("want 33, got %d", got)
		}
	})

	t.Run("blank identifiers", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer db.Close()
		// Again: no ExpectQuery — a blank pipeline/execution id must short-circuit
		// before touching the DB.
		if got := runRowsProcessed(context.Background(), db, "", "exec-1", "batch", 33); got != 33 {
			t.Fatalf("blank pipeline id: want 33, got %d", got)
		}
		if got := runRowsProcessed(context.Background(), db, "pipe-1", "   ", "batch", 33); got != 33 {
			t.Fatalf("blank execution id: want 33, got %d", got)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet sqlmock expectations: %v", err)
		}
	})

	t.Run("query error", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer db.Close()

		mock.ExpectQuery("SELECT COALESCE\\(SUM\\(rows_written\\), 0\\)").
			WithArgs("pipe-1", "exec-1").
			WillReturnError(context.DeadlineExceeded)

		if got := runRowsProcessed(context.Background(), db, "pipe-1", "exec-1", "batch", 33); got != 33 {
			t.Fatalf("want the dispatch count 33 on query error, got %d", got)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet sqlmock expectations: %v", err)
		}
	})
}
