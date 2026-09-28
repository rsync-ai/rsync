package handlers

import (
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// KI-EVENTS-DUAL-ID-NAMESPACE-DUPES: the orchestrator's stage workers
// (schema_version 1) and the V2 workflow (schema_version 2) each write their own
// row for one stage transition, with different payloads and no shared id. The
// run-comparison view counted rows, so every run read as roughly twice as
// eventful and a failed stage as two errors. The bug class is "a reader counts
// producer copies as transitions"; these pin that each transition counts once
// while a transition only one producer saw (executor re-dispatch) is not lost.

const runEventGroupsQuery = `(?s)FROM pipeline_run_events e.*GROUP BY 1, 2, 3`

var runEventGroupCols = []string{"event_type", "stage_id", "producer", "count", "errors"}

func TestLogicalRunEventCountsCountsEachTransitionOnce(t *testing.T) {
	groups := []runEventGroup{
		// planner ran once; both producers reported it.
		{"STAGE_STARTED", "planner", "1", 1, 0},
		{"STAGE_STARTED", "planner", "2", 1, 0},
		{"STAGE_COMPLETED", "planner", "1", 1, 0},
		{"STAGE_COMPLETED", "planner", "2", 1, 0},
		// Same stage under each producer's own name.
		{"STAGE_STARTED", "connection_validator", "1", 1, 0},
		{"STAGE_STARTED", "connection_validation", "2", 1, 0},
		// intent failed once: one error, not two.
		{"STAGE_FAILED", "intent", "1", 1, 1},
		{"STAGE_FAILED", "intent", "2", 1, 1},
		// HITL park reported by both.
		{"PIPELINE_WAITING", "executor", "1", 1, 0},
		{"PIPELINE_WAITING", "executor", "2", 1, 0},
		// Executor re-dispatched 3x: the orchestrator reports each dispatch, the
		// workflow one completion. Three real transitions, not one.
		{"STAGE_COMPLETED", "executor", "1", 3, 0},
		{"STAGE_COMPLETED", "executor", "2", 1, 0},
		// Single-producer events count as stored.
		{"TABLE_STATS", "executor", "1", 5, 0},
		{"PIPELINE_COMPLETED", "", "2", 1, 0},
	}
	// planner 2 + connection 1 + intent 1 + waiting 1 + executor 3 + stats 5 + completed 1
	events, errors := logicalRunEventCounts(groups)
	if events != 14 {
		t.Errorf("events = %d, want 14 — each stage transition counted once, re-dispatches kept (raw rows: 20)", events)
	}
	if errors != 1 {
		t.Errorf("errors = %d, want 1 — one failed stage reported by two producers is one error", errors)
	}
}

func TestExecutionSummaryDoesNotCountProducerCopies(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer sqlDB.Close()
	const pid, eid = "p-1", "11111111-1111-4111-8111-111111111111"
	now := time.Now()

	// Raw rows: 4 lifecycle copies + 1 completion = 5, 2 of them errors.
	mock.ExpectQuery(`MIN\(COALESCE\(e.occurred_at, e.received_at\)\)`).WithArgs(pid, eid).
		WillReturnRows(sqlmock.NewRows([]string{"start_time", "end_time", "event_count", "error_count", "retry_count", "has_completed", "has_failed"}).
			AddRow(now.Add(-time.Minute), now, 5, 2, 0, false, true))
	mock.ExpectQuery(runEventGroupsQuery).WithArgs(pid, eid).
		WillReturnRows(sqlmock.NewRows(runEventGroupCols).
			AddRow("STAGE_STARTED", "intent", "1", 1, 0).
			AddRow("STAGE_STARTED", "intent", "2", 1, 0).
			AddRow("STAGE_FAILED", "intent", "1", 1, 1).
			AddRow("STAGE_FAILED", "intent", "2", 1, 1).
			AddRow("PIPELINE_COMPLETED", "", "2", 1, 0))
	mock.ExpectQuery(`FROM executions e.*WHERE e.id = \$1::uuid`).WithArgs(eid, pid).
		WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("failed"))

	got, err := getExecutionSummary(sqlDB, pid, eid)
	if err != nil {
		t.Fatalf("getExecutionSummary: %v", err)
	}
	if got.EventCount != 3 || got.ErrorCount != 1 {
		t.Fatalf("event/error count = %d/%d, want 3/1 — the comparison view counts both producers' copies of one transition", got.EventCount, got.ErrorCount)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}
