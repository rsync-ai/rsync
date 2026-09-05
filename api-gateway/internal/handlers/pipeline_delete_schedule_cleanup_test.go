package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// DP-1 regression: deleting a pipeline must also tear down its Temporal schedule.
//
// The pipeline_schedules row is removed by ON DELETE CASCADE (migration 023) when
// the pipeline row is deleted, but the Temporal schedule is an EXTERNAL resource
// with no cascade. DeletePipeline previously never touched schedules at all, so
// the cron kept firing forever for a pipeline that no longer existed — burning
// worker slots and polluting usage meters (observed live: 6 orphaned schedules on
// prod, all for deleted pipelines).
//
// The fix reads the pipeline's temporal_schedule_id(s) BEFORE the delete (they are
// gone immediately after) and removes them from Temporal afterward. This test pins
// the pre-delete read — the step that was entirely missing before. temporalClient
// is unset in tests, so the external delete is a safe no-op even when the read
// returns a live schedule ID (proving deleteTemporalSchedulesBestEffort's nil-guard
// keeps the handler at 200 rather than panicking / 500-ing).
func TestDeletePipeline_ReadsAndClearsSchedulesBeforeDelete(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	// Gate: membership in the active workspace.
	mock.ExpectQuery(`SELECT wm\.role\s+FROM pipelines r`).
		WithArgs(wsScopePipeline, wsScopeUser, wsScopeWS).
		WillReturnRows(gateRoleRows("member"))

	// The fix: schedule IDs are read up-front, while the rows still exist. Return a
	// live schedule ID so the nil-temporalClient path is exercised end-to-end.
	mock.ExpectQuery(`SELECT temporal_schedule_id FROM pipeline_schedules WHERE pipeline_id = \$1`).
		WithArgs(wsScopePipeline).
		WillReturnRows(sqlmock.NewRows([]string{"temporal_schedule_id"}).
			AddRow("aaaaaaaa-1111-2222-3333-444444444444"))

	mock.ExpectBegin()
	mock.ExpectExec(`DELETE FROM executions`).
		WithArgs(wsScopePipeline, wsScopeWS).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`DELETE FROM pipelines WHERE id=\$1 AND workspace_id=\$2`).
		WithArgs(wsScopePipeline, wsScopeWS).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	r := wsScopeRouter(http.MethodDelete, "/api/v1/pipelines/:id", DeletePipeline)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodDelete, "/api/v1/pipelines/"+wsScopePipeline, nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	// ExpectationsWereMet fails if the schedule SELECT never fired — that is the
	// regression guard: pre-fix, DeletePipeline issued no such query.
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}
