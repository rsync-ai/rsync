package handlers

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"api-gateway/internal/db"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
)

// Issue #7: a healthy CDC stream with no new changes showed "Stale" and a red Stop button.
// A streaming CDC pipeline stops heartbeating at the hand-off, so heartbeat age says
// nothing about it. /state now takes a streaming pipeline's staleness from the liveness
// signal /runtime uses (destination apply time + pending backlog + dependency health),
// never recommends cancelling it, and leaves every other pipeline on the heartbeat rule.

const (
	cdcStaleExecID = "22222222-3333-4444-5555-666666666666"

	// The whole hand-off predicate, pinned: sqlmock cannot evaluate SQL, so the query text
	// IS the contract with the database. The temporal adapter's "streaming_active" hand-off
	// closes the execution with status='completed' and end_time=NOW()
	// (backend-temporal-adapter/internal/workflows/pipeline_status_activity.go); an execution
	// still 'running' is a CDC initial load, and the row must belong to this pipeline.
	reExecClosed = `SELECT EXISTS \(\s*SELECT 1 FROM executions\s+` +
		`WHERE id = \$1 AND pipeline_id = \$2\s+` +
		`AND status IN \('completed', 'success'\)\s+` +
		`AND end_time IS NOT NULL\s*\)`
	reLiveness      = `SELECT MAX\(last_applied_ts\)`
	reDeps          = `FROM pipeline_dependencies d`
	reFirstDataWait = `FROM executions e`
)

var depCols = []string{"kind", "identifier", "status", "last_checked_at", "last_healthy_at",
	"consecutive_failures", "last_error", "details"}

func newStateMock(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	return sqlDB, mock
}

// executorState is a /state row sitting in the executor stage whose last heartbeat is
// heartbeatAge old.
func executorState(status string, heartbeatAge time.Duration) PipelineState {
	hb := time.Now().Add(-heartbeatAge)
	return PipelineState{
		PipelineID:      gatePipeID,
		ExecutionID:     cdcStaleExecID,
		Status:          status,
		CurrentStage:    "executor",
		StageGroup:      "executing",
		LastHeartbeatAt: &hb,
	}
}

func expectHandoff(mock sqlmock.Sqlmock, closed bool) {
	mock.ExpectQuery(reExecClosed).
		WithArgs(cdcStaleExecID, gatePipeID).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(closed))
}

// lastApplied nil = no destination apply recorded yet.
func expectLiveness(mock sqlmock.Sqlmock, lastApplied *time.Time, pending int64) {
	row := sqlmock.NewRows([]string{"max", "pending"})
	if lastApplied == nil {
		row.AddRow(nil, pending)
	} else {
		row.AddRow(*lastApplied, pending)
	}
	mock.ExpectQuery(reLiveness).WithArgs(gatePipeID).WillReturnRows(row)
}

func expectDeps(mock sqlmock.Sqlmock, statuses ...string) {
	rows := sqlmock.NewRows(depCols)
	now := time.Now()
	for i, s := range statuses {
		rows.AddRow([]string{"source", "sink", "destination"}[i%3], "dep-"+s, s, now, now, 0, "", []byte("{}"))
	}
	mock.ExpectQuery(reDeps).WithArgs(gatePipeID).WillReturnRows(rows)
}

func ago(d time.Duration) *time.Time {
	t := time.Now().Add(-d)
	return &t
}

func assertElapsedNear(t *testing.T, got int64, want time.Duration) {
	t.Helper()
	w := int64(want.Seconds())
	if got < w-5 || got > w+5 {
		t.Fatalf("stale_elapsed_seconds = %d, want about %d", got, w)
	}
}

// requireHeartbeatStale proves the fixture's heartbeat is old enough that the heartbeat
// rule alone WOULD call it stale — otherwise a "not stale" result would prove nothing.
func requireHeartbeatStale(t *testing.T, st PipelineState) {
	t.Helper()
	if stale, _ := computeStaleness(executorState("processing", time.Since(*st.LastHeartbeatAt))); !stale {
		t.Fatalf("precondition: a %s-old executor heartbeat must be stale under the heartbeat rule",
			time.Since(*st.LastHeartbeatAt).Round(time.Second))
	}
}

// requireNoStaleness asserts every staleness field is at its zero value.
func requireNoStaleness(t *testing.T, label string, st PipelineState) {
	t.Helper()
	if st.Streaming || st.IsStale || st.CancelRecommended || st.StaleReason != "" || st.StaleElapsedSeconds != 0 {
		t.Fatalf("%s: streaming=%v is_stale=%v cancel=%v reason=%q elapsed=%d, want all empty",
			label, st.Streaming, st.IsStale, st.CancelRecommended, st.StaleReason, st.StaleElapsedSeconds)
	}
}

func TestApplyStateStaleness_StreamingCDC_QuietStreamIsNotStale(t *testing.T) {
	// The reported case: the projector put "processing" back on a stream that handed off
	// 10 minutes ago, the heartbeat stopped then, and the source has had no new changes
	// for 20 minutes. Nothing is waiting and every dependency is healthy.
	database, mock := newStateMock(t)
	st := executorState("processing", 10*time.Minute)
	requireHeartbeatStale(t, st)

	expectHandoff(mock, true)
	expectLiveness(mock, ago(20*time.Minute), 0)
	expectDeps(mock, "healthy", "healthy", "healthy")

	applyStateStaleness(database, gatePipeID, "cdc", &st)

	if st.IsStale || st.CancelRecommended || st.StaleReason != "" || st.StaleElapsedSeconds != 0 {
		t.Fatalf("quiet healthy stream: is_stale=%v cancel_recommended=%v reason=%q elapsed=%d, want all empty",
			st.IsStale, st.CancelRecommended, st.StaleReason, st.StaleElapsedSeconds)
	}
	if !st.Streaming {
		t.Fatal("streaming must be true for a CDC pipeline past the hand-off")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

func TestApplyStateStaleness_StreamingCDC_RunningStatusQuietIsNotStale(t *testing.T) {
	// 'running' is only written by the streaming hand-off, so no execution lookup is needed.
	database, mock := newStateMock(t)
	st := executorState("running", 30*time.Minute)

	expectLiveness(mock, ago(20*time.Minute), 0)
	expectDeps(mock, "healthy")

	applyStateStaleness(database, gatePipeID, "CDC", &st)

	if st.IsStale || st.CancelRecommended || !st.Streaming {
		t.Fatalf("running quiet stream: is_stale=%v cancel=%v streaming=%v, want false/false/true",
			st.IsStale, st.CancelRecommended, st.Streaming)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

func TestApplyStateStaleness_CDCOnlyRunningOrHandedOffIsStreaming(t *testing.T) {
	// A CDC pipeline that is stopped, paused, failed, finished or parked on a question is
	// not a live stream: it gets no streaming flag, no stream liveness lookups (the mock
	// has no expectations for them) and no heartbeat staleness either, because only a
	// "processing" row is judged by its heartbeat. The heartbeat on every fixture is 10
	// minutes old, which the heartbeat rule would call stale.
	cases := []struct {
		name          string
		status        string
		syncMode      string
		setup         func(sqlmock.Sqlmock)
		wantStreaming bool
	}{
		{name: "stopped", status: "stopped", syncMode: "cdc"},
		{name: "paused", status: "paused", syncMode: "cdc"},
		{name: "failed", status: "failed", syncMode: "cdc"},
		{name: "completed", status: "completed", syncMode: "cdc"},
		{name: "cancelled", status: "cancelled", syncMode: "cdc"},
		{name: "waiting for the user", status: "waiting_for_user", syncMode: "cdc"},
		{
			// Positive control: the same fixture as 'running' IS a stream (mixed case and
			// padding on sync_mode, as legacy rows carry).
			name: "running (control)", status: "running", syncMode: " Cdc ",
			setup: func(m sqlmock.Sqlmock) {
				expectLiveness(m, ago(time.Minute), 0)
				expectDeps(m, "healthy")
			},
			wantStreaming: true,
		},
	}
	var streams, notStreams int
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			database, mock := newStateMock(t)
			st := executorState(tc.status, 10*time.Minute)
			if tc.setup != nil {
				tc.setup(mock)
			}

			applyStateStaleness(database, gatePipeID, tc.syncMode, &st)

			if tc.wantStreaming {
				streams++
				if !st.Streaming || st.IsStale || st.CancelRecommended || st.StaleReason != "" || st.StaleElapsedSeconds != 0 {
					t.Fatalf("streaming=%v is_stale=%v cancel=%v reason=%q elapsed=%d, want a quiet live stream",
						st.Streaming, st.IsStale, st.CancelRecommended, st.StaleReason, st.StaleElapsedSeconds)
				}
			} else {
				notStreams++
				requireNoStaleness(t, "CDC "+tc.status, st)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("db expectations: %v", err)
			}
		})
	}
	if streams == 0 || notStreams == 0 {
		t.Fatalf("table must hold both outcomes: streams=%d not-streams=%d", streams, notStreams)
	}
}

func TestApplyStateStaleness_StreamingCDC_RealStallsStillSurface(t *testing.T) {
	cases := []struct {
		name        string
		setup       func(sqlmock.Sqlmock)
		wantElapsed time.Duration // always asserted (±5s)
		wantReason  string
	}{
		{
			name: "backlog not draining",
			setup: func(m sqlmock.Sqlmock) {
				expectLiveness(m, ago(20*time.Minute), 42)
				expectDeps(m, "healthy", "healthy")
			},
			wantElapsed: 20 * time.Minute,
			wantReason:  "42 changes are waiting to be written, but nothing has reached the destination for 20m",
		},
		{
			name: "one change waiting",
			setup: func(m sqlmock.Sqlmock) {
				expectLiveness(m, ago(20*time.Minute), 1)
				expectDeps(m, "healthy")
			},
			wantElapsed: 20 * time.Minute,
			wantReason:  "1 change is waiting to be written",
		},
		{
			name: "quiet but a dependency is degraded",
			setup: func(m sqlmock.Sqlmock) {
				expectLiveness(m, ago(20*time.Minute), 0)
				expectDeps(m, "healthy", "degraded")
			},
			wantElapsed: 20 * time.Minute,
			wantReason:  "health checks have not all passed",
		},
		{
			// No dependency manifest (older pipelines): /runtime reads this as idle too.
			name: "quiet and no health checks registered",
			setup: func(m sqlmock.Sqlmock) {
				expectLiveness(m, ago(20*time.Minute), 0)
				expectDeps(m)
			},
			wantElapsed: 20 * time.Minute,
			wantReason:  "no health checks that confirm it is still picking up changes",
		},
		{
			name: "dependency unhealthy",
			setup: func(m sqlmock.Sqlmock) {
				expectLiveness(m, ago(30*time.Second), 0)
				expectDeps(m, "healthy", "unhealthy")
			},
			wantElapsed: 30 * time.Second,
			wantReason:  "A service this stream depends on is not healthy",
		},
		{
			// Nothing ever applied, so there is no quiet time to report; must not panic.
			name: "dependency unhealthy before anything was delivered",
			setup: func(m sqlmock.Sqlmock) {
				expectLiveness(m, nil, 0)
				expectDeps(m, "unhealthy")
				m.ExpectQuery(reFirstDataWait).
					WithArgs(gatePipeID, cdcStaleExecID).
					WillReturnRows(sqlmock.NewRows([]string{"end_time", "exists"}).AddRow(*ago(time.Minute), false))
			},
			wantElapsed: 0,
			wantReason:  "A service this stream depends on is not healthy",
		},
		{
			name: "never delivered anything after the hand-off",
			setup: func(m sqlmock.Sqlmock) {
				expectLiveness(m, nil, 0)
				expectDeps(m, "healthy")
				m.ExpectQuery(reFirstDataWait).
					WithArgs(gatePipeID, cdcStaleExecID).
					WillReturnRows(sqlmock.NewRows([]string{"end_time", "exists"}).AddRow(*ago(10 * time.Minute), false))
			},
			wantElapsed: 10 * time.Minute,
			wantReason:  "nothing has reached the destination yet",
		},
	}
	if len(cases) == 0 {
		t.Fatal("no cases")
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			database, mock := newStateMock(t)
			st := executorState("processing", 10*time.Minute)
			expectHandoff(mock, true)
			tc.setup(mock)

			applyStateStaleness(database, gatePipeID, "cdc", &st)

			if !st.IsStale {
				t.Fatal("a stalled stream must still be reported stale")
			}
			if st.CancelRecommended {
				t.Fatal("cancel must never be recommended for a streaming pipeline")
			}
			if !st.Streaming {
				t.Fatal("streaming must be true")
			}
			if !strings.Contains(st.StaleReason, tc.wantReason) {
				t.Fatalf("stale_reason = %q, want it to contain %q", st.StaleReason, tc.wantReason)
			}
			assertElapsedNear(t, st.StaleElapsedSeconds, tc.wantElapsed)
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("db expectations: %v", err)
			}
		})
	}
}

func TestApplyStateStaleness_HeartbeatRuleUnchanged(t *testing.T) {
	// Controls: everything that is not a streaming CDC pipeline keeps the heartbeat rule.
	t.Run("batch, heartbeat 10m old: stale and cancel recommended", func(t *testing.T) {
		st := executorState("processing", 10*time.Minute)
		requireHeartbeatStale(t, st)
		// nil database: the batch path must not need one.
		applyStateStaleness(nil, gatePipeID, "batch", &st)
		if !st.IsStale || !st.CancelRecommended || st.Streaming {
			t.Fatalf("batch: is_stale=%v cancel=%v streaming=%v, want true/true/false", st.IsStale, st.CancelRecommended, st.Streaming)
		}
		if !strings.HasPrefix(st.StaleReason, "No update for 10m") {
			t.Fatalf("stale_reason = %q, want the heartbeat reason", st.StaleReason)
		}
		assertElapsedNear(t, st.StaleElapsedSeconds, 10*time.Minute)
	})

	t.Run("sync_mode unset, heartbeat 10m old: stale and cancel recommended", func(t *testing.T) {
		st := executorState("processing", 10*time.Minute)
		applyStateStaleness(nil, gatePipeID, "", &st)
		if !st.IsStale || !st.CancelRecommended || st.Streaming {
			t.Fatalf("legacy row: is_stale=%v cancel=%v streaming=%v, want true/true/false", st.IsStale, st.CancelRecommended, st.Streaming)
		}
	})

	t.Run("batch, heartbeat 4m old: stale, no cancel yet", func(t *testing.T) {
		st := executorState("processing", 4*time.Minute)
		applyStateStaleness(nil, gatePipeID, "batch", &st)
		if !st.IsStale || st.CancelRecommended {
			t.Fatalf("batch 4m: is_stale=%v cancel=%v, want true/false", st.IsStale, st.CancelRecommended)
		}
		assertElapsedNear(t, st.StaleElapsedSeconds, 4*time.Minute)
	})

	t.Run("batch, running status: no staleness computed (as before)", func(t *testing.T) {
		st := executorState("running", 10*time.Minute)
		applyStateStaleness(nil, gatePipeID, "batch", &st)
		requireNoStaleness(t, "batch running", st)
	})

	t.Run("CDC initial load (execution still open): heartbeat rule", func(t *testing.T) {
		database, mock := newStateMock(t)
		st := executorState("processing", 10*time.Minute)
		expectHandoff(mock, false)
		applyStateStaleness(database, gatePipeID, "cdc", &st)
		if !st.IsStale || !st.CancelRecommended || st.Streaming {
			t.Fatalf("CDC initial load: is_stale=%v cancel=%v streaming=%v, want true/true/false", st.IsStale, st.CancelRecommended, st.Streaming)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("db expectations: %v", err)
		}
	})

	t.Run("CDC processing row with no execution id: heartbeat rule", func(t *testing.T) {
		database, mock := newStateMock(t)
		st := executorState("processing", 10*time.Minute)
		st.ExecutionID = "  "
		applyStateStaleness(database, gatePipeID, "cdc", &st)
		if !st.IsStale || !st.CancelRecommended || st.Streaming {
			t.Fatalf("no execution id: is_stale=%v cancel=%v streaming=%v, want true/true/false", st.IsStale, st.CancelRecommended, st.Streaming)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("db expectations: %v", err)
		}
	})

	t.Run("CDC, hand-off lookup fails: heartbeat rule", func(t *testing.T) {
		database, mock := newStateMock(t)
		st := executorState("processing", 10*time.Minute)
		mock.ExpectQuery(reExecClosed).WithArgs(cdcStaleExecID, gatePipeID).WillReturnError(errors.New("boom"))
		applyStateStaleness(database, gatePipeID, "cdc", &st)
		if !st.IsStale || !st.CancelRecommended || st.Streaming {
			t.Fatalf("lookup error: is_stale=%v cancel=%v streaming=%v, want true/true/false", st.IsStale, st.CancelRecommended, st.Streaming)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("db expectations: %v", err)
		}
	})
}

func TestCDCLivenessPhase_IdleOnlyWithLiveness(t *testing.T) {
	// applyStreamingLiveness dereferences liveness in its "idle" branch. That is safe only
	// because cdcLivenessPhase never answers "idle" without liveness; pin that contract.
	var checked int
	for _, dep := range []string{"healthy", "degraded", "unknown", "unhealthy", ""} {
		for _, wait := range []time.Time{{}, time.Now().Add(-time.Minute), time.Now().Add(-time.Hour)} {
			checked++
			if got := cdcLivenessPhase(dep, nil, wait); got == "idle" {
				t.Fatalf("cdcLivenessPhase(%q, nil, %v) = idle; idle needs liveness", dep, wait)
			}
		}
	}
	if checked == 0 {
		t.Fatal("no combinations checked")
	}
	// Control: the same function does answer idle when liveness shows a stalled backlog.
	if got := cdcLivenessPhase("healthy", &RuntimeLiveness{StaleSeconds: 600, PendingEvents: 5}, time.Time{}); got != "idle" {
		t.Fatalf("control: stalled backlog phase = %q, want idle", got)
	}
}

// ---------------------------------------------------------------------------
// Through the real handler: pipelines.sync_mode must reach the staleness decision.
// ---------------------------------------------------------------------------

type stateFixture struct {
	progressStatus string // pipeline_progress.status
	pipelineStatus string // pipelines.status
	syncMode       string // pipelines.sync_mode
	// The executions row the stale-park check reads; it runs only for a
	// processing/waiting_for_user progress row.
	execStatus string
	execEnd    *time.Time
	extra      func(sqlmock.Sqlmock)
}

func serveState(t *testing.T, f stateFixture) map[string]interface{} {
	t.Helper()
	gin.SetMode(gin.TestMode)
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	prev := db.DB
	db.DB = sqlDB
	t.Cleanup(func() { db.DB = prev; _ = sqlDB.Close() })

	now := time.Now()
	heartbeat := now.Add(-10 * time.Minute)

	mock.ExpectQuery(gateRoleQuery).
		WithArgs(gatePipeID, gateUserID, activeWS).
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("viewer"))
	mock.ExpectQuery(`FROM pipeline_progress\s+WHERE pipeline_id = \$1`).
		WithArgs(gatePipeID).
		WillReturnRows(sqlmock.NewRows([]string{
			"pipeline_id", "execution_id", "status", "current_stage", "schema_version",
			"stage_group", "stage_state", "stage_started_at", "stage_last_heartbeat_at",
			"stage_duration_ms", "stage_attempt", "stage_max_attempts", "stage_summary",
			"progress_percent", "progress_current_step", "progress_total_steps",
			"blocking_reason_type", "blocking_reason_description", "blocking_reason_estimated_seconds",
			"message", "metadata", "created_at", "updated_at",
		}).AddRow(
			gatePipeID, cdcStaleExecID, f.progressStatus, "executor", 2,
			"executing", "running", now.Add(-time.Hour), heartbeat,
			nil, 1, 3, "executor completed",
			88, 6, 7,
			nil, nil, nil,
			"Executing pipeline", "{}", now.Add(-2*time.Hour), heartbeat,
		))
	mock.ExpectQuery(`SELECT status, updated_at, sync_mode\s+FROM pipelines`).
		WithArgs(gatePipeID).
		WillReturnRows(sqlmock.NewRows([]string{"status", "updated_at", "sync_mode"}).AddRow(f.pipelineStatus, now, f.syncMode))
	if f.progressStatus == "processing" || f.progressStatus == "waiting_for_user" {
		endVal := interface{}(nil)
		if f.execEnd != nil {
			endVal = *f.execEnd
		}
		mock.ExpectQuery(`SELECT status, end_time FROM executions`).
			WithArgs(cdcStaleExecID).
			WillReturnRows(sqlmock.NewRows([]string{"status", "end_time"}).AddRow(f.execStatus, endVal))
	}
	if f.extra != nil {
		f.extra(mock)
	}

	rr := httptest.NewRecorder()
	newGateRouter("/api/v1/pipelines/:id/state", GetPipelineState).
		ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/api/v1/pipelines/"+gatePipeID+"/state", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("GET /state = %d: %s", rr.Code, rr.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
	var body map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["last_heartbeat_at"] == nil {
		t.Fatalf("fixture must carry a heartbeat for the staleness check to see: %s", rr.Body.String())
	}
	return body
}

func requireStatus(t *testing.T, body map[string]interface{}, want string) {
	t.Helper()
	if body["status"] != want {
		t.Fatalf("status = %v, want %q", body["status"], want)
	}
}

func TestGetPipelineState_StreamingCDC_QuietStreamNotStale(t *testing.T) {
	handoff := time.Now().Add(-10 * time.Minute)
	body := serveState(t, stateFixture{
		progressStatus: "processing", pipelineStatus: "running", syncMode: "cdc",
		execStatus: "completed", execEnd: &handoff,
		extra: func(m sqlmock.Sqlmock) {
			expectHandoff(m, true)
			expectLiveness(m, ago(20*time.Minute), 0)
			expectDeps(m, "healthy", "healthy")
		},
	})
	requireStatus(t, body, "processing")
	if v, ok := body["is_stale"]; ok && v != false {
		t.Fatalf("is_stale = %v, want false/absent", v)
	}
	if v, ok := body["cancel_recommended"]; ok && v != false {
		t.Fatalf("cancel_recommended = %v, want false/absent", v)
	}
	if body["streaming"] != true {
		t.Fatalf("streaming = %v, want true", body["streaming"])
	}
}

func TestGetPipelineState_StreamingCDC_RunningRowNotStale(t *testing.T) {
	// The row exactly as the hand-off leaves it: pipeline_progress.status='running'.
	body := serveState(t, stateFixture{
		progressStatus: "running", pipelineStatus: "running", syncMode: "cdc",
		extra: func(m sqlmock.Sqlmock) {
			expectLiveness(m, ago(20*time.Minute), 0)
			expectDeps(m, "healthy")
		},
	})
	requireStatus(t, body, "running")
	if body["streaming"] != true {
		t.Fatalf("streaming = %v, want true", body["streaming"])
	}
	for _, k := range []string{"is_stale", "stale_reason", "stale_elapsed_seconds", "cancel_recommended"} {
		if v, ok := body[k]; ok {
			t.Fatalf("%s = %v, want absent for a quiet live stream", k, v)
		}
	}
}

func TestGetPipelineState_StoppedCDCPipeline_NotAStream(t *testing.T) {
	// A stopped CDC pipeline whose progress row was left at "processing": the stop wins,
	// and a stopped pipeline is neither a stream nor stale.
	body := serveState(t, stateFixture{
		progressStatus: "processing", pipelineStatus: "stopped", syncMode: "cdc",
		execStatus: "running",
	})
	requireStatus(t, body, "stopped")
	for _, k := range []string{"streaming", "is_stale", "stale_reason", "stale_elapsed_seconds", "cancel_recommended"} {
		if v, ok := body[k]; ok {
			t.Fatalf("%s = %v, want absent for a stopped pipeline", k, v)
		}
	}
}

func TestGetPipelineState_BatchSameHeartbeat_StaleWithCancel(t *testing.T) {
	body := serveState(t, stateFixture{
		progressStatus: "processing", pipelineStatus: "running", syncMode: "batch",
		execStatus: "running",
	})
	requireStatus(t, body, "processing")
	if body["is_stale"] != true {
		t.Fatalf("is_stale = %v, want true", body["is_stale"])
	}
	if body["cancel_recommended"] != true {
		t.Fatalf("cancel_recommended = %v, want true", body["cancel_recommended"])
	}
	elapsed, _ := body["stale_elapsed_seconds"].(float64)
	if elapsed <= 300 {
		t.Fatalf("stale_elapsed_seconds = %v, want > 300", body["stale_elapsed_seconds"])
	}
	if _, ok := body["streaming"]; ok {
		t.Fatalf("streaming must be absent for a batch pipeline, got %v", body["streaming"])
	}
}
