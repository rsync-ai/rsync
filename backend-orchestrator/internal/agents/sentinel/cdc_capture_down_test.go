package sentinel

import (
	"context"
	"database/sql/driver"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/rsync-ai/backend-orchestrator/pkg/diagnose"
)

// Pins KI-DEBEZIUM-WORKER-DEATH-NOT-SURFACED.
//
// The bug class: the CAPTURE side of a CDC pipeline dies (Kafka Connect worker
// OOM-killed, connector deleted, task FAILED) and the only record of it is
// pipeline_dependency_health. Lag drains to 0 because the producer is gone, the
// pipeline keeps reading 'running', and no issue/event/notification is raised.
// The worst variant is an unreachable Connect: the Sentinel's getDebeziumStatus
// returns early, so every Connect-driven check is skipped on exactly that tick.
//
// Every PG-family, MySQL, MongoDB, SQL Server and Oracle source hangs its capture
// off the same `debezium_task` dependency, so the class is source-agnostic.

const captureTestPID = "11111111-2222-4333-8444-555555555555"

func captureRows() *sqlmock.Rows {
	return sqlmock.NewRows([]string{"pipeline_id", "identifier", "status", "last_error", "consecutive_failures"})
}

func expectCaptureIssueInsert(mock sqlmock.Sqlmock, pid string) {
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO sentinel_active_issues")).
		WithArgs(
			captureStoppedIssueID(pid),
			string(IssueTypeCaptureStopped),
			string(IssueSeverityCritical),
			pid,
			string(ComponentTypeCDCPipeline),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(0, 1))
}

// TestDeadCaptureIsSurfacedEvenWhenConnectIsUnreachable drives the real tick,
// checkActivePipelines, with Kafka Connect down (a closed server) and a capture
// dependency the probe has read unhealthy four times running. It must raise the
// capture-stopped issue on the pipeline before giving up on Connect.
func TestDeadCaptureIsSurfacedEvenWhenConnectIsUnreachable(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	dead := httptest.NewServer(http.NotFoundHandler())
	deadURL := dead.URL
	dead.Close() // connection refused from here on: the OOM-killed worker

	s := &CDCSentinel{
		db:         db,
		connectURL: deadURL,
		httpClient: &http.Client{Timeout: 2 * time.Second},
	}

	mock.ExpectQuery("SELECT id").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(captureTestPID))
	mock.ExpectQuery(regexp.QuoteMeta("FROM pipeline_dependencies d")).
		WillReturnRows(captureRows().AddRow(
			captureTestPID, "cdc-11111111", "unhealthy",
			"kafka connect unreachable: dial tcp 127.0.0.1:8083: connect: connection refused",
			CaptureDownFloor))
	expectCaptureIssueInsert(mock, captureTestPID)

	s.checkActivePipelines(context.Background())

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("a dead capture side raised no issue while Connect was unreachable: %v\n\n"+
			"The pipeline keeps reading 'running' with lag 0 and nobody is told "+
			"(KI-DEBEZIUM-WORKER-DEATH-NOT-SURFACED).", err)
	}
}

// TestCaptureDownVerdict covers each probe state: only unhealthy at/over the floor
// raises; everything else resolves (a DELETE by this pipeline's own id).
func TestCaptureDownVerdict(t *testing.T) {
	cases := []struct {
		name   string
		status string
		fails  int
		raise  bool
	}{
		{"unhealthy at floor", "unhealthy", CaptureDownFloor, true},
		{"unhealthy well past floor", "unhealthy", 40, true},
		{"unhealthy below floor (rebalance/restart)", "unhealthy", CaptureDownFloor - 1, false},
		{"healthy", "healthy", 0, false},
		{"degraded (paused on purpose)", "degraded", 9, false},
		{"not probed yet", "unknown", 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock: %v", err)
			}
			defer db.Close()
			s := &CDCSentinel{db: db}

			mock.ExpectQuery(regexp.QuoteMeta("FROM pipeline_dependencies d")).
				WillReturnRows(captureRows().AddRow(captureTestPID, "cdc-11111111", tc.status, "", tc.fails))
			if tc.raise {
				expectCaptureIssueInsert(mock, captureTestPID)
			} else {
				mock.ExpectExec(regexp.QuoteMeta("DELETE FROM sentinel_active_issues WHERE id = $1")).
					WithArgs(captureStoppedIssueID(captureTestPID)).
					WillReturnResult(sqlmock.NewResult(0, 0))
			}

			s.surfaceCaptureDown(context.Background())

			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
		})
	}
}

// TestCaptureStoppedIssueIDIsItsOwnClass keeps this resolver from deleting the
// Sentinel's terminal connector-down verdict (or a lag issue) when capture recovers.
func TestCaptureStoppedIssueIDIsItsOwnClass(t *testing.T) {
	got := captureStoppedIssueID("abc")
	for _, other := range []string{connectorIssueID("abc"), sinkLagIssueID("abc"), "cdc-lag-abc"} {
		if got == other {
			t.Fatalf("captureStoppedIssueID collides with %q", other)
		}
	}
}

// TestCaptureStoppedDescriptionFollowsTheHealerRule: the issue sweep diagnoses the
// description. An unreachable Connect is transient → backoff-retry; a missing
// connector is provisioning → escalate. The fixed text must not itself trip the
// provisioning rule, or every outage would escalate.
func TestCaptureStoppedDescriptionFollowsTheHealerRule(t *testing.T) {
	describe := func(lastError string) string {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer db.Close()
		s := &CDCSentinel{db: db}
		var desc string
		mock.ExpectQuery(regexp.QuoteMeta("FROM pipeline_dependencies d")).
			WillReturnRows(captureRows().AddRow(captureTestPID, "cdc-11111111", "unhealthy", lastError, CaptureDownFloor))
		mock.ExpectExec(regexp.QuoteMeta("INSERT INTO sentinel_active_issues")).
			WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
				descCapture{&desc}, sqlmock.AnyArg()).
			WillReturnResult(sqlmock.NewResult(0, 1))
		s.surfaceCaptureDown(context.Background())
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("no issue raised: %v", err)
		}
		return desc
	}

	// Built exactly as heal/issue_sweep.go builds it for a sentinel issue.
	d := diagnose.New()
	for _, tc := range []struct {
		lastError string
		want      diagnose.Action
	}{
		{"kafka connect unreachable: dial tcp 10.0.0.5:8083: connect: connection refused", diagnose.ActionBackoffRetry},
		{"debezium connector not found in kafka connect", diagnose.ActionEscalate},
	} {
		desc := describe(tc.lastError)
		got := d.Diagnose(diagnose.Signal{
			PipelineID:     captureTestPID,
			ErrorMessage:   desc,
			ExecutorStatus: string(IssueTypeCaptureStopped),
			Stage:          "sentinel",
			LastEvents:     []string{"sentinel issue capture_stopped (severity critical)"},
		})
		if got.SuggestedAction != tc.want {
			t.Errorf("last_error %q → %s, want %s (description: %q)", tc.lastError, got.SuggestedAction, tc.want, desc)
		}
	}
}

// descCapture is a sqlmock.Argument that records the description it was given.
type descCapture struct{ out *string }

func (c descCapture) Match(v driver.Value) bool {
	s, ok := v.(string)
	if ok {
		*c.out = s
	}
	return ok && strings.HasPrefix(s, "CDC capture stopped")
}
