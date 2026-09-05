package handlers

import (
	"database/sql"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// Four live prod pipelines (oldest 2026-07-05) sit at pipeline_progress
// status='waiting_for_user' pointing at an execution that is already 'failed' with
// an end_time, while pipelines.status still says 'running'. The list card resolved
// that in favour of the execution and rendered "Failed: zombie: execution timed out";
// GET /pipelines/:id did the same; only GET /pipelines/:id/state kept the park — and
// that is the endpoint the detail page's badge polls. So the pipeline list and the
// pipeline's own page disagreed, and the page offered a "Select table(s) to sync"
// prompt with nothing alive to receive it.
//
// staleParkTerminalStatus is the predicate that closes that gap. Its two halves pull
// against each other: it must retire a park nothing can answer, and it must never
// retire one that a live run is genuinely blocked on — the table-selection wedge is
// the single most common reason a real pipeline is waiting on a real user.
func TestStaleParkTerminalStatus(t *testing.T) {
	const execID = "11111111-1111-1111-1111-111111111111"
	closed := time.Date(2026, 7, 13, 13, 0, 58, 0, time.UTC)

	execRow := func(status string, end interface{}) *sqlmock.Rows {
		return sqlmock.NewRows([]string{"status", "end_time"}).AddRow(status, end)
	}

	cases := []struct {
		name           string
		progressStatus string
		execution      *sqlmock.Rows // nil = no query expected
		want           string
	}{
		{
			name:           "park on a failed closed execution is dead",
			progressStatus: "waiting_for_user",
			execution:      execRow("failed", closed),
			want:           "failed",
		},
		{
			name:           "processing on a failed closed execution is dead too",
			progressStatus: "processing",
			execution:      execRow("failed", closed),
			want:           "failed",
		},
		{
			name:           "cancelled execution reports stopped",
			progressStatus: "waiting_for_user",
			execution:      execRow("cancelled", closed),
			want:           "stopped",
		},
		{
			// The table-selection wedge: a real run, really waiting on a real
			// person, for up to 24h. Retiring this would be the regression.
			name:           "park on a still-running execution is live",
			progressStatus: "waiting_for_user",
			execution:      execRow("running", nil),
			want:           "",
		},
		{
			// CDC closes its execution row as 'success' at the backfill→streaming
			// handoff while the feed keeps running, so a closed success proves
			// nothing about whether a park can still be answered.
			name:           "closed success is not terminal for this purpose",
			progressStatus: "waiting_for_user",
			execution:      execRow("success", closed),
			want:           "",
		},
		{
			// Terminal status with no end_time is a row mid-write, not a verdict.
			name:           "failed without end_time waits for the next poll",
			progressStatus: "waiting_for_user",
			execution:      execRow("failed", nil),
			want:           "",
		},
		{
			// Already-terminal progress isn't claiming to wait on anything, so
			// there is nothing to retire and no reason to hit the DB.
			name:           "terminal progress status short-circuits",
			progressStatus: "failed",
			execution:      nil,
			want:           "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mockDB, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock.New: %v", err)
			}
			defer mockDB.Close()

			if tc.execution != nil {
				mock.ExpectQuery("SELECT status, end_time FROM executions").
					WithArgs(execID).
					WillReturnRows(tc.execution)
			}

			got := staleParkTerminalStatus(mockDB, tc.progressStatus, sql.NullString{String: execID, Valid: true})
			if got != tc.want {
				t.Errorf("staleParkTerminalStatus = %q, want %q", got, tc.want)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unmet/unexpected queries: %v", err)
			}
		})
	}
}

// No execution to check means no evidence either way. A progress row with a NULL
// execution_id predates the projector's execution stamping; guessing "failed" there
// would invent a terminal state for a pipeline nobody has closed.
func TestStaleParkTerminalStatus_NoExecutionIsNotEvidence(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	for _, id := range []sql.NullString{{Valid: false}, {String: "   ", Valid: true}} {
		if got := staleParkTerminalStatus(mockDB, "waiting_for_user", id); got != "" {
			t.Errorf("execution_id %+v: got %q, want empty", id, got)
		}
	}
	if got := staleParkTerminalStatus(nil, "waiting_for_user", sql.NullString{String: "x", Valid: true}); got != "" {
		t.Errorf("nil DB: got %q, want empty", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unexpected queries: %v", err)
	}
}

// An unreadable execution row (deleted, transient DB error) must not be read as a
// verdict either — this endpoint drives the badge and the Run button, and failing a
// live run because one SELECT hiccuped is worse than showing a stale park one more
// poll.
func TestStaleParkTerminalStatus_QueryErrorLeavesParkAlone(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	mock.ExpectQuery("SELECT status, end_time FROM executions").
		WithArgs("missing").
		WillReturnError(sql.ErrNoRows)

	if got := staleParkTerminalStatus(mockDB, "waiting_for_user", sql.NullString{String: "missing", Valid: true}); got != "" {
		t.Errorf("got %q, want empty", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}
