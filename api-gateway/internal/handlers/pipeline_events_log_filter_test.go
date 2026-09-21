package handlers

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"api-gateway/internal/db"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
)

// A run's "View Logs" asks GET /pipelines/:id/events for
// `execution_id=<run>&scope=run&exclude_routine=true`. What the two filters
// select is proven against real PostgreSQL in pipeline_events_pg_test.go
// (TestPG_PipelineEventsRunScope / TestPG_PipelineEventsExcludeRoutine); this
// file proves the handler turns the query string into those filters, and that a
// request without them still gets the strict, unfiltered query it always did.

const logFilterExecID = "dddddddd-0000-4000-8000-000000000001"

var eventRowColumns = []string{
	"pipeline_id", "execution_id", "event_id", "seq", "event_type",
	"stage_id", "stage_group", "severity", "trace_id", "occurred_at",
	"received_at", "payload",
}

func serveEvents(t *testing.T, query string, expect func(sqlmock.Sqlmock)) {
	t.Helper()
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
	expect(mock)

	rr := httptest.NewRecorder()
	newGateRouter("/api/v1/pipelines/:id/events", GetPipelineEvents).
		ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/api/v1/pipelines/"+gatePipeID+"/events?"+query, nil))

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rr.Code, rr.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

func TestGetPipelineEvents_RunLogParamsReachTheQuery(t *testing.T) {
	serveEvents(t, "execution_id="+logFilterExecID+"&scope=run&exclude_routine=true", func(mock sqlmock.Sqlmock) {
		// The run window join and the heartbeat test are both in the SQL, and the
		// routine types are bound as arguments after the execution id.
		mock.ExpectQuery(`(?s)FROM pipeline_run_events e.*FROM executions x.*payload->'metadata'->>'heartbeat'`).
			WithArgs(gatePipeID, logFilterExecID, "DATA_PLANE_METRICS", "TABLE_STATS").
			WillReturnRows(sqlmock.NewRows(eventRowColumns))
	})
}

func TestGetPipelineEvents_WithoutRunLogParamsKeepsStrictQuery(t *testing.T) {
	serveEvents(t, "execution_id="+logFilterExecID, func(mock sqlmock.Sqlmock) {
		// Two arguments only: a routine filter would bind two more, and the
		// strict execution match is still there.
		mock.ExpectQuery(`FROM pipeline_run_events e\s+WHERE e.pipeline_id = \$1 AND e.execution_id = \$2::uuid`).
			WithArgs(gatePipeID, logFilterExecID).
			WillReturnRows(sqlmock.NewRows(eventRowColumns))
	})
}

func TestBuildPipelineEventsQuery_RunScopeNeedsAnExecution(t *testing.T) {
	query, args := buildPipelineEventsQuery(gatePipeID, pipelineEventsFilter{RunScope: true}, pipelineEventsCursor{}, 10)
	if len(args) != 1 {
		t.Fatalf("RunScope without ExecutionID bound %d args, want 1 (pipeline id only): %v", len(args), args)
	}
	for _, frag := range []string{"executions x", "execution_id ="} {
		if strings.Contains(query, frag) {
			t.Errorf("RunScope without ExecutionID should not filter by run; query contains %q", frag)
		}
	}
}
