package handlers

import (
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"api-gateway/internal/db"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
)

func trendRun(status string, durationMs int64) ExecutionSummary {
	s := ExecutionSummary{Status: status}
	if durationMs > 0 {
		d := durationMs
		s.DurationMs = &d
	}
	return s
}

// TestSummarizeTrendWindow_RateUsesTheWindowNotAllTime locks the success-rate fix.
// GetPipelineTrends used to divide the window's successes by COUNT(DISTINCT
// execution_id) over the pipeline's whole history, so a pipeline with 100 runs
// whose last 10 all succeeded reported 0.10.
func TestSummarizeTrendWindow_RateUsesTheWindowNotAllTime(t *testing.T) {
	window := make([]ExecutionSummary, 0, 10)
	for i := 0; i < 10; i++ {
		window = append(window, trendRun("completed", 1000))
	}
	got := summarizeTrendWindow(window)
	if got.SuccessRate != 1.0 {
		t.Fatalf("SuccessRate = %v, want 1.0 (10 of 10 finished runs succeeded)", got.SuccessRate)
	}
	if got.FinishedRuns != 10 || got.SucceededRuns != 10 {
		t.Fatalf("Finished/Succeeded = %d/%d, want 10/10", got.FinishedRuns, got.SucceededRuns)
	}
}

// A run still in flight has no outcome, so it must count toward neither side.
func TestSummarizeTrendWindow_RunningRunsAreExcluded(t *testing.T) {
	got := summarizeTrendWindow([]ExecutionSummary{
		trendRun("running", 0),
		trendRun("completed", 2000),
		trendRun("completed", 4000),
		trendRun("failed", 600),
	})
	if got.FinishedRuns != 3 || got.SucceededRuns != 2 {
		t.Fatalf("Finished/Succeeded = %d/%d, want 3/2", got.FinishedRuns, got.SucceededRuns)
	}
	if math.Abs(got.SuccessRate-2.0/3.0) > 1e-9 {
		t.Fatalf("SuccessRate = %v, want 2/3", got.SuccessRate)
	}
	if got.AvgDurationMs == nil || *got.AvgDurationMs != 2200 {
		t.Fatalf("AvgDurationMs = %v, want 2200 (runs with an end time only)", got.AvgDurationMs)
	}
	if len(got.RecentExecutions) != 4 {
		t.Fatalf("RecentExecutions len = %d, want all 4 runs kept", len(got.RecentExecutions))
	}
}

// Nothing finished yet: the rate is 0 with FinishedRuns 0, so a client can tell
// "no outcome yet" from "every run failed" (which has FinishedRuns > 0).
func TestSummarizeTrendWindow_NoFinishedRuns(t *testing.T) {
	got := summarizeTrendWindow([]ExecutionSummary{trendRun("running", 0)})
	if got.FinishedRuns != 0 || got.SuccessRate != 0 || got.AvgDurationMs != nil {
		t.Fatalf("got %+v, want zero finished, rate 0, no average", got)
	}
	allFailed := summarizeTrendWindow([]ExecutionSummary{trendRun("failed", 10), trendRun("failed", 30)})
	if allFailed.FinishedRuns != 2 || allFailed.SuccessRate != 0 {
		t.Fatalf("all-failed got %+v, want FinishedRuns 2, rate 0", allFailed)
	}
}

// A CDC pipeline's stream stats carry execution_id = pipeline_id. Both the run
// list and the run count must leave that id out, or the stream reads as the
// newest run and the comparison card compares a run against it.
func TestGetPipelineTrends_TheCDCStreamIsNotARun(t *testing.T) {
	gin.SetMode(gin.TestMode)
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	prev := db.DB
	db.DB = sqlDB
	t.Cleanup(func() { db.DB = prev; _ = sqlDB.Close() })

	mock.ExpectQuery(gateRoleQuery).
		WithArgs(gatePipeID, gateUserID, activeWS).
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("viewer"))
	mock.ExpectQuery(`SELECT e.execution_id::text, MAX\(e.received_at\).*e.execution_id <> e.pipeline_id\s+GROUP BY`).
		WithArgs(gatePipeID).
		WillReturnRows(sqlmock.NewRows([]string{"execution_id", "last_seen"}))
	mock.ExpectQuery(`SELECT COUNT\(DISTINCT e.execution_id\).*e.execution_id <> e.pipeline_id`).
		WithArgs(gatePipeID).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	rr := httptest.NewRecorder()
	newGateRouter("/api/v1/pipelines/:id/trends", GetPipelineTrends).
		ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/api/v1/pipelines/"+gatePipeID+"/trends", nil))

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rr.Code, rr.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

// Prod 2026-09-26: a run the postflight check failed after PIPELINE_COMPLETED had
// been emitted read "1 of the last 1 finished run succeeded" on the pipeline page
// while the live panel said FAILED. The events cannot say a run failed, so the
// executions row decides; a run with no executions row keeps the events' answer.
func TestGetPipelineTrends_TheExecutionsRowDecidesTheStatus(t *testing.T) {
	gin.SetMode(gin.TestMode)
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	prev := db.DB
	db.DB = sqlDB
	t.Cleanup(func() { db.DB = prev; _ = sqlDB.Close() })

	const failedRun = "11111111-1111-4111-8111-111111111111"
	const legacyRun = "22222222-2222-4222-8222-222222222222"
	now := time.Now()

	mock.ExpectQuery(gateRoleQuery).
		WithArgs(gatePipeID, gateUserID, activeWS).
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("viewer"))
	mock.ExpectQuery(`SELECT e.execution_id::text, MAX\(e.received_at\)`).
		WithArgs(gatePipeID).
		WillReturnRows(sqlmock.NewRows([]string{"execution_id", "last_seen"}).
			AddRow(failedRun, now).AddRow(legacyRun, now.Add(-time.Hour)))
	mock.ExpectQuery(`SELECT COUNT\(DISTINCT e.execution_id\)`).
		WithArgs(gatePipeID).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(2))

	eventCols := []string{"start_time", "end_time", "event_count", "error_count", "retry_count", "has_completed", "has_failed"}
	statusQuery := `FROM executions e.*WHERE e.id = \$1::uuid AND e.pipeline_id = \$2::uuid`
	for _, run := range []struct {
		id         string
		execStatus *string
	}{{failedRun, strptr("failed")}, {legacyRun, nil}} {
		mock.ExpectQuery(`MIN\(COALESCE\(e.occurred_at, e.received_at\)\)`).
			WithArgs(gatePipeID, run.id).
			WillReturnRows(sqlmock.NewRows(eventCols).
				AddRow(now.Add(-time.Minute), now, 9, 0, 0, true, false))
		mock.ExpectQuery(runEventGroupsQuery).
			WithArgs(gatePipeID, run.id).
			WillReturnRows(sqlmock.NewRows(runEventGroupCols).AddRow("PIPELINE_COMPLETED", "", "2", 1, 0))
		rows := sqlmock.NewRows([]string{"status"})
		if run.execStatus != nil {
			rows.AddRow(*run.execStatus)
		}
		mock.ExpectQuery(statusQuery).WithArgs(run.id, gatePipeID).WillReturnRows(rows)
	}

	rr := httptest.NewRecorder()
	newGateRouter("/api/v1/pipelines/:id/trends", GetPipelineTrends).
		ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/api/v1/pipelines/"+gatePipeID+"/trends", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rr.Code, rr.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}

	var got PipelineTrends
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	statuses := []string{}
	for _, e := range got.RecentExecutions {
		statuses = append(statuses, e.Status)
	}
	if strings.Join(statuses, ",") != "failed,completed" {
		t.Fatalf("statuses = %v, want [failed completed] (executions row for the first, events for the legacy run)", statuses)
	}
	if got.FinishedRuns != 2 || got.SucceededRuns != 1 {
		t.Fatalf("finished/succeeded = %d/%d, want 2/1", got.FinishedRuns, got.SucceededRuns)
	}
}

func TestTrendStatusFromExecution(t *testing.T) {
	for in, want := range map[string]string{
		"completed":                    "completed",
		"success":                      "completed",
		"failed":                       "failed",
		"error":                        "failed",
		"silent_drop_detected":         "failed",
		"silent_partial_drop_detected": "failed",
		"credential_check_failed":      "failed",
		"cancelled":                    "cancelled",
		"running":                      "running",
		"waiting_for_user":             "running",
		"pending":                      "running",
	} {
		if got := trendStatusFromExecution(in); got != want {
			t.Errorf("trendStatusFromExecution(%q) = %q, want %q", in, got, want)
		}
	}
}
