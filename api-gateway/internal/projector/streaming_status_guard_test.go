package projector

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"regexp"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/segmentio/kafka-go"
)

// Issue #7: a healthy CDC stream showed "Stale" with a red Stop button. One cause was the
// projector: the temporal adapter writes pipeline_progress.status='running' at the
// streaming hand-off, and events that raced that write — the adapter's own executor
// STAGE_COMPLETED (emitStageEvent always sends status "processing"), a last executor
// heartbeat, or a legacy event with no status (defaulted to "processing") — landed after
// it and turned the row back into "processing" with a heartbeat that never moves again.
// These tests drive projectEvent against the SQL it actually issues.

const (
	guardPipelineID = "11111111-2222-3333-4444-555555555555"
	guardExecID     = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	guardOtherExec  = "ffffffff-bbbb-cccc-dddd-eeeeeeeeeeee"
)

// statusArg matches the status argument ($3) of the pipeline_progress upsert.
type statusArg string

func (s statusArg) Match(v driver.Value) bool {
	got, ok := v.(string)
	return ok && got == string(s)
}

// projectAgainstRow runs one event through projectEvent with pipeline_progress holding
// (existingStatus, existingExec). wantStatus == "" means the event must be skipped (no
// upsert may be issued — sqlmock fails an unexpected Exec, which projectEvent returns);
// otherwise the upsert must be issued with that status. expectSyncModeUpdate is set when
// the event is an executor event mentioning "streaming", which also backfills sync_mode.
func projectAgainstRow(t *testing.T, existingStatus, existingExec string, event map[string]interface{}, expectSyncModeUpdate bool, wantStatus string) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO pipeline_run_events")).
		WillReturnResult(sqlmock.NewResult(0, 1))
	if expectSyncModeUpdate {
		mock.ExpectExec(regexp.QuoteMeta("UPDATE pipelines")).
			WithArgs(guardPipelineID).
			WillReturnResult(sqlmock.NewResult(0, 0))
	}
	mock.ExpectQuery(regexp.QuoteMeta("SELECT COALESCE(version, 1), status, execution_id")).
		WithArgs(guardPipelineID).
		WillReturnRows(sqlmock.NewRows([]string{"version", "status", "execution_id"}).
			AddRow(7, existingStatus, existingExec))
	if wantStatus != "" {
		args := make([]driver.Value, 22)
		for i := range args {
			args[i] = sqlmock.AnyArg()
		}
		args[0] = guardPipelineID
		args[2] = statusArg(wantStatus)
		args[21] = 8 // version bumps from the stored 7
		mock.ExpectExec(regexp.QuoteMeta("INSERT INTO pipeline_progress")).
			WithArgs(args...).
			WillReturnResult(sqlmock.NewResult(0, 1))
	}

	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	p := &EventProjector{db: db, ctx: context.Background(), lastSeq: map[string]int64{}, gapSeen: map[string]bool{}}
	if err := p.projectEvent(kafka.Message{Value: payload}); err != nil {
		t.Fatalf("projectEvent returned %v (an unexpected pipeline_progress write shows up here)", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

func TestProjectEvent_RunningStream_NotDowngradedByDefaultedProcessingEvent(t *testing.T) {
	// A legacy/minimal STAGE_PROGRESS with no status: projectEvent defaults it to
	// "processing". It must not overwrite the streaming run's 'running'.
	projectAgainstRow(t, "running", guardExecID, map[string]interface{}{
		"event_type":   "STAGE_PROGRESS",
		"pipeline_id":  guardPipelineID,
		"execution_id": guardExecID,
		"stage":        "executor",
		"progress":     map[string]interface{}{"percent": 88, "stage": "executor"},
	}, false, "")
}

func TestProjectEvent_RunningStream_NotDowngradedByAdapterExecutorCompleted(t *testing.T) {
	// The exact payload shape emitStageEvent sends for the executor stage: explicit
	// status "processing", which reaches Kafka before the adapter writes 'running'.
	projectAgainstRow(t, "running", guardExecID, map[string]interface{}{
		"schema_version": 2,
		"event_type":     "STAGE_COMPLETED",
		"pipeline_id":    guardPipelineID,
		"execution_id":   guardExecID,
		"stage":          "executor",
		"stage_group":    "execution",
		"state":          "succeeded",
		"status":         "processing",
		"metadata":       map[string]interface{}{"result_summary": "Pipeline executed"},
	}, false, "")
}

func TestProjectEvent_ProcessingToRunning_StillApplies(t *testing.T) {
	// Control: the orchestrator's streaming STAGE_COMPLETED moves processing → running.
	projectAgainstRow(t, "processing", guardExecID, map[string]interface{}{
		"event_type":   "STAGE_COMPLETED",
		"pipeline_id":  guardPipelineID,
		"execution_id": guardExecID,
		"stage":        "executor",
		"status":       "running",
		"message":      "Streaming pipeline started",
	}, true, "running")
}

func TestProjectEvent_RunningToCompleted_StillApplies(t *testing.T) {
	// Control: a terminal transition over 'running' for the same execution applies
	// (status defaulted from the event type).
	projectAgainstRow(t, "running", guardExecID, map[string]interface{}{
		"event_type":   "PIPELINE_COMPLETED",
		"pipeline_id":  guardPipelineID,
		"execution_id": guardExecID,
	}, false, "completed")
}

func TestProjectEvent_RunningToFailed_StillApplies(t *testing.T) {
	projectAgainstRow(t, "running", guardExecID, map[string]interface{}{
		"event_type":   "PIPELINE_FAILED",
		"pipeline_id":  guardPipelineID,
		"execution_id": guardExecID,
		"message":      "Stream stopped",
	}, false, "failed")
}

func TestProjectEvent_RunningThenProcessingForNewExecution_Applies(t *testing.T) {
	// Control: a new run (different execution) must always be able to take the row over.
	projectAgainstRow(t, "running", guardExecID, map[string]interface{}{
		"event_type":   "STAGE_STARTED",
		"pipeline_id":  guardPipelineID,
		"execution_id": guardOtherExec,
		"stage":        "intent",
		"status":       "processing",
	}, false, "processing")
}

func TestProjectEvent_CompletedNotReopenedByProcessing(t *testing.T) {
	// The pre-existing terminal guard still holds.
	projectAgainstRow(t, "completed", guardExecID, map[string]interface{}{
		"event_type":   "STAGE_PROGRESS",
		"pipeline_id":  guardPipelineID,
		"execution_id": guardExecID,
		"status":       "processing",
	}, false, "")
}

func TestLateEventWouldRegressStatus(t *testing.T) {
	cases := []struct {
		name                     string
		existing, exec, incoming string
		incomingExec             string
		want                     bool
	}{
		{"running over processing, same exec", "running", "e1", "processing", "e1", true},
		{"running over processing, case/space", " Running ", "e1", "PROCESSING", " e1", true},
		{"running over running", "running", "e1", "running", "e1", false},
		{"running over completed", "running", "e1", "completed", "e1", false},
		{"running over failed", "running", "e1", "failed", "e1", false},
		{"running over cancelled", "running", "e1", "cancelled", "e1", false},
		{"running over waiting_for_user", "running", "e1", "waiting_for_user", "e1", false},
		{"running over processing, other exec", "running", "e1", "processing", "e2", false},
		{"running over processing, no incoming exec", "running", "e1", "processing", "", false},
		{"processing over running", "processing", "e1", "running", "e1", false},
		{"processing over processing", "processing", "e1", "processing", "e1", false},
		{"completed over processing", "completed", "e1", "processing", "e1", true},
		{"failed over running", "failed", "e1", "running", "e1", true},
		{"completed over failed", "completed", "e1", "failed", "e1", false},
		{"cancelled over processing, other exec", "cancelled", "e1", "processing", "e2", false},
		// A cancelled execution is finished too: nothing non-terminal reopens it, and a
		// cancel is itself a terminal write that still lands over any other status.
		{"cancelled over processing, same exec", "cancelled", "e1", "processing", "e1", true},
		{"cancelled over running, same exec", " CANCELLED ", "e1", "running", "e1", true},
		{"cancelled over waiting_for_user", "cancelled", "e1", "waiting_for_user", "e1", true},
		{"cancelled over completed", "cancelled", "e1", "completed", "e1", false},
		{"cancelled over failed", "cancelled", "e1", "failed", "e1", false},
		{"completed over cancelled", "completed", "e1", "cancelled", "e1", false},
		{"failed over cancelled", "failed", "e1", "Cancelled", "e1", false},
		{"running over cancelled, case", "running", "e1", "CANCELLED", "e1", false},
		{"processing over cancelled", "processing", "e1", "cancelled", "e1", false},
	}
	if len(cases) == 0 {
		t.Fatal("no cases")
	}
	var dropped, applied int
	for _, tc := range cases {
		if tc.want {
			dropped++
		} else {
			applied++
		}
		if got := lateEventWouldRegressStatus(tc.existing, tc.exec, tc.incoming, tc.incomingExec); got != tc.want {
			t.Errorf("%s: lateEventWouldRegressStatus(%q,%q,%q,%q) = %v, want %v",
				tc.name, tc.existing, tc.exec, tc.incoming, tc.incomingExec, got, tc.want)
		}
	}
	if dropped == 0 || applied == 0 {
		t.Fatalf("table must hold both outcomes: dropped=%d applied=%d", dropped, applied)
	}
}

func TestProjectEvent_CancelledNotReopenedByLateProgress(t *testing.T) {
	// A progress tick that raced the cancel must not put a cancelled run back to
	// "processing" (the row would then read as a stuck run with a Stop button).
	projectAgainstRow(t, "cancelled", guardExecID, map[string]interface{}{
		"event_type":   "STAGE_PROGRESS",
		"pipeline_id":  guardPipelineID,
		"execution_id": guardExecID,
		"stage":        "executor",
		"status":       "processing",
	}, false, "")
}

func TestProjectEvent_CancelOverFinishedRow_StillApplies(t *testing.T) {
	// Control: a cancel for the same execution is a terminal write and still lands over
	// a row that already says completed (projectEvent does not default "cancelled", so
	// the event carries it explicitly).
	projectAgainstRow(t, "completed", guardExecID, map[string]interface{}{
		"event_type":   "PIPELINE_FAILED",
		"pipeline_id":  guardPipelineID,
		"execution_id": guardExecID,
		"status":       "cancelled",
		"message":      "Pipeline cancelled",
	}, false, "cancelled")
}
