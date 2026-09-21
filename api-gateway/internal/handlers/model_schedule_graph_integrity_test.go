package handlers

import (
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"testing"
	"time"

	"api-gateway/internal/db"

	"github.com/DATA-DOG/go-sqlmock"
)

// The stored trigger graph has to stay true to what can actually fire. Each test here pins
// one way it drifted in the Phase 1.0 baseline: a deleted schedule still closing a ring
// (G1), a model that never builds accepted as an upstream (G5), and a schedule left active
// with no upstream at all after that upstream was deleted (G6).

// G1. A deleted schedule is a soft delete, so its upstream rows stay behind; walking them
// refused an acyclic edit. The fix is in the SQL, so the SQL is what is pinned.
func TestCheckUpstreamCycle_TheWalkIgnoresDeletedSchedules(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	other := "55555555-5555-5555-5555-555555555555"
	mock.ExpectQuery(`WITH RECURSIVE ancestors[\s\S]*JOIN saved_query_schedules s ON s.saved_query_id = a.model_id AND s.status <> 'deleted'`).
		WithArgs(other, savedQueryID).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))

	c, w := cycleTestContext()
	if !checkUpstreamCycle(c, db.DB, savedQueryID, []scheduleUpstream{{Kind: upstreamKindModel, ID: other}}) {
		t.Fatalf("an acyclic set was refused: %d %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("the ancestor walk must skip deleted schedules: %v", err)
	}
}

// G5. A plain saved query never completes a build, so it can never wake anything.
func TestRefuseUnbuildableUpstreams_APlainSavedQueryIsRefusedByName(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	other := "55555555-5555-5555-5555-555555555555"
	mock.ExpectQuery(`SELECT name, materialization FROM saved_queries WHERE id = \$1::uuid`).
		WithArgs(other).
		WillReturnRows(sqlmock.NewRows([]string{"name", "materialization"}).AddRow("e_plain", matNone))

	c, w := cycleTestContext()
	if refuseUnbuildableUpstreams(c, db.DB, []scheduleUpstream{{Kind: upstreamKindModel, ID: other}}) {
		t.Fatal("a model that never builds was accepted as an upstream")
	}
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
	if got := scheduleErrorBody(t, w.Body.Bytes()); !strings.Contains(got, `"e_plain"`) {
		t.Errorf("error = %q, want it to name the upstream", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

// Positive control for the refusal above: the same query answering 'table' is accepted,
// and a pipeline upstream is not looked up at all. Without this, a check that refused
// every model would pass the test above.
func TestRefuseUnbuildableUpstreams_ABuildingModelAndAPipelinePass(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	other := "55555555-5555-5555-5555-555555555555"
	mock.ExpectQuery(`SELECT name, materialization FROM saved_queries WHERE id = \$1::uuid`).
		WithArgs(other).
		WillReturnRows(sqlmock.NewRows([]string{"name", "materialization"}).AddRow("stg_orders", matTable))

	c, w := cycleTestContext()
	if !refuseUnbuildableUpstreams(c, db.DB, []scheduleUpstream{
		{Kind: upstreamKindPipeline, ID: savedQueryConn},
		{Kind: upstreamKindModel, ID: other},
	}) {
		t.Fatalf("a building model was refused: %d %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

// A failed read is unknown, and unknown refuses: the schedule would otherwise be stored
// on a guess.
func TestRefuseUnbuildableUpstreams_AFailedReadRefuses(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	mock.ExpectQuery(`SELECT name, materialization FROM saved_queries`).
		WillReturnError(fmt.Errorf("connection reset"))

	c, w := cycleTestContext()
	if refuseUnbuildableUpstreams(c, db.DB, []scheduleUpstream{{Kind: upstreamKindModel, ID: savedQueryOther}}) {
		t.Fatal("an upstream was accepted after its materialization could not be read")
	}
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", w.Code)
	}
}

// G6, the statement. The column is chosen by kind, and the "only upstream" test is a
// NOT EXISTS over IS DISTINCT FROM, so that a row for the OTHER kind (NULL in this column)
// counts as a surviving upstream. With "<>" that row would compare NULL, drop out, and a
// schedule that still has a pipeline upstream would be paused when its model upstream
// went.
func TestPauseTriggersOrphanedBy_PicksTheColumnByKindAndOnlyPausesSoleUpstreams(t *testing.T) {
	for _, tc := range []struct {
		kind, col string
	}{
		{upstreamKindModel, "upstream_saved_query_id"},
		{upstreamKindPipeline, "upstream_pipeline_id"},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			mock, cleanup := wsScopeMockDB(t)
			defer cleanup()

			col := regexp.QuoteMeta(tc.col)
			// The reason names no upstream: everyone who can see the downstream reads it, and
			// the deleted upstream may have been another member's private model.
			mock.ExpectExec(`UPDATE saved_query_schedules s[\s\S]*auto_paused_reason = 'an upstream it depended on was deleted` +
				`[\s\S]*WHERE s.status = 'active'[\s\S]*s.schedule_type = 'after_upstream'` +
				`[\s\S]*AND EXISTS[\s\S]*u.` + col + ` = \$1::uuid` +
				`[\s\S]*AND NOT EXISTS[\s\S]*u.` + col + ` IS DISTINCT FROM \$1::uuid`).
				WithArgs(savedQueryOther).
				WillReturnResult(sqlmock.NewResult(0, 2))

			n, err := pauseTriggersOrphanedBy(t.Context(), db.DB, tc.kind, savedQueryOther)
			if err != nil {
				t.Fatalf("pause: %v", err)
			}
			if n != 2 {
				t.Errorf("paused = %d, want the affected row count 2", n)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("sql expectations: %v", err)
			}
		})
	}
}

// G6, the delete path. The pause runs inside the delete's transaction and BEFORE the
// delete: afterwards the upstream rows that say who depended on it have cascaded away.
func TestDeleteSavedQuery_PausesOrphanedTriggersInTheSameTransactionFirst(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	expectDeleteSavedQueryPreamble(mock)
	mock.ExpectBegin()
	mock.ExpectExec(`UPDATE saved_query_schedules s`).
		WithArgs(savedQueryID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`DELETE FROM saved_queries WHERE id = \$1`).
		WithArgs(savedQueryID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	r := savedQueryRouter(http.MethodDelete, "/explorer/saved/:id", "admin", DeleteSavedQuery)
	w := doJSON(r, http.MethodDelete, "/explorer/saved/"+savedQueryID, nil)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), `"paused_downstream_schedules":1`) {
		t.Errorf("response should report the paused downstream count: %s", w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

// A delete that fails must not leave downstream schedules paused for an upstream that still
// exists. sqlmock's ExpectRollback is the proof the pause was never committed.
func TestDeleteSavedQuery_AFailedDeleteRollsThePauseBack(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	expectDeleteSavedQueryPreamble(mock)
	mock.ExpectBegin()
	mock.ExpectExec(`UPDATE saved_query_schedules s`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`DELETE FROM saved_queries WHERE id = \$1`).
		WillReturnError(fmt.Errorf("lock timeout"))
	mock.ExpectRollback()

	r := savedQueryRouter(http.MethodDelete, "/explorer/saved/:id", "admin", DeleteSavedQuery)
	w := doJSON(r, http.MethodDelete, "/explorer/saved/"+savedQueryID, nil)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("the pause must roll back with the failed delete: %v", err)
	}
}

func expectDeleteSavedQueryPreamble(mock sqlmock.Sqlmock) {
	mock.ExpectQuery(`FROM saved_queries r\s+JOIN workspace_members`).
		WithArgs(savedQueryID, wsScopeUser, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("admin"))
	mock.ExpectQuery(`FROM saved_queries sq\s+LEFT JOIN saved_query_schedules s`).
		WithArgs(savedQueryID).
		WillReturnRows(savedQueryRows(wsScopeUser, "workspace", "SELECT 1", "read"))
	// Event triggers carry no Temporal schedule; the filter keeps their NULL out of a
	// string scan.
	mock.ExpectQuery(`SELECT temporal_schedule_id FROM saved_query_schedules WHERE saved_query_id = \$1 AND temporal_schedule_id IS NOT NULL`).
		WithArgs(savedQueryID).
		WillReturnRows(sqlmock.NewRows([]string{"temporal_schedule_id"}))
}

// The resume guard: an event trigger whose every upstream was deleted must not come back
// active, because nothing can ever wake it.
func TestResumeSavedQuerySchedule_AnEventTriggerWithNoUpstreamsIsRefused(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	const scheduleID = "ba6fc134-0ed4-4210-8180-5fb0ad8660f3"
	expectPausedEventScheduleForResume(mock, scheduleID)
	mock.ExpectQuery(`FROM saved_query_schedule_upstreams u`).
		WithArgs(scheduleID).
		WillReturnRows(sqlmock.NewRows([]string{"schedule_id", "upstream_kind", "id", "name"}))

	r := savedQueryRouter(http.MethodPost, "/explorer/saved/:id/schedule/resume", "admin", ResumeSavedQuerySchedule)
	w := doJSON(r, http.MethodPost, "/explorer/saved/"+savedQueryID+"/schedule/resume", nil)

	if w.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d: %s", w.Code, w.Body.String())
	}
	// No UPDATE is expected, so a handler that went on to write status='active' fails here.
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

// Positive control: the same schedule with one upstream left resumes, and the write lands.
func TestResumeSavedQuerySchedule_AnEventTriggerWithAnUpstreamResumes(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	const scheduleID = "ba6fc134-0ed4-4210-8180-5fb0ad8660f3"
	expectPausedEventScheduleForResume(mock, scheduleID)
	mock.ExpectQuery(`FROM saved_query_schedule_upstreams u`).
		WithArgs(scheduleID).
		WillReturnRows(sqlmock.NewRows([]string{"schedule_id", "upstream_kind", "id", "name"}).
			AddRow(scheduleID, upstreamKindPipeline, savedQueryConn, "Nightly ingest"))
	mock.ExpectExec(`UPDATE saved_query_schedules\s+SET status = 'active'`).
		WithArgs(scheduleID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	// The read-back, left unmocked: the status under test is the write above.
	mock.ExpectQuery(`FROM saved_query_schedules s`).WillReturnError(fmt.Errorf("stop here"))

	r := savedQueryRouter(http.MethodPost, "/explorer/saved/:id/schedule/resume", "admin", ResumeSavedQuerySchedule)
	_ = doJSON(r, http.MethodPost, "/explorer/saved/"+savedQueryID+"/schedule/resume", nil)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("an event trigger that still has an upstream must resume: %v", err)
	}
}

func expectPausedEventScheduleForResume(mock sqlmock.Sqlmock, scheduleID string) {
	mock.ExpectQuery(`FROM saved_queries r\s+JOIN workspace_members`).
		WithArgs(savedQueryID, wsScopeUser, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("admin"))
	mock.ExpectQuery(`FROM saved_queries\s+WHERE id = \$1 AND workspace_id = \$2`).
		WithArgs(savedQueryID, wsScopeWS, wsScopeUser).
		WillReturnRows(sqlmock.NewRows([]string{"visible"}).AddRow(1))
	mock.ExpectQuery(`FROM saved_query_schedules s\s+WHERE s.saved_query_id = \$1`).
		WithArgs(savedQueryID).
		WillReturnRows(sqlmock.NewRows([]string{
			"schedule_id", "saved_query_id", "schedule_type", "schedule_spec", "temporal_schedule_id",
			"status", "run_as_user_id", "created_by", "created_at", "updated_at",
			"paused_at", "paused_reason", "auto_paused_at", "auto_paused_reason", "upstream_policy",
		}).AddRow(scheduleID, savedQueryID, scheduleAfterUpstream, []byte(`{"timezone":"UTC"}`), nil,
			"paused", wsScopeUser, wsScopeUser, time.Now(), time.Now(),
			nil, nil, time.Now(), "an upstream it depended on was deleted", upstreamPolicyAny))
	mock.ExpectQuery(`FROM saved_query_schedule_upstreams u`).
		WithArgs(scheduleID).
		WillReturnRows(sqlmock.NewRows([]string{"schedule_id", "upstream_kind", "id", "name"}))
	mock.ExpectQuery(`FROM saved_queries sq\s+JOIN connections c`).
		WithArgs(savedQueryID).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "workspace_id", "connection_id", "name", "sql_text",
			"materialization", "target_table", "target_owned", "connector_type", "config",
		}).AddRow(savedQueryID, wsScopeWS, savedQueryConn, "Daily MRR", "SELECT 1",
			matTable, "public.daily_mrr", true, "postgresql", "{}"))
	mock.ExpectQuery(`SELECT role FROM workspace_members`).
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("admin"))
}

func TestNormalizeUpstreamPolicy(t *testing.T) {
	for _, tc := range []struct {
		scheduleType, in, want string
		wantErr                bool
	}{
		{scheduleAfterUpstream, "", upstreamPolicyAny, false},
		{scheduleAfterUpstream, "all", upstreamPolicyAll, false},
		{scheduleAfterUpstream, " any ", upstreamPolicyAny, false},
		{scheduleAfterUpstream, "most", "", true},
		{"cron", "", upstreamPolicyAny, false},
		{"cron", "any", upstreamPolicyAny, false},
		{"cron", "all", "", true},
	} {
		got, err := normalizeUpstreamPolicy(tc.scheduleType, tc.in)
		if (err != nil) != tc.wantErr || got != tc.want {
			t.Errorf("normalizeUpstreamPolicy(%q, %q) = %q, %v; want %q, err=%v",
				tc.scheduleType, tc.in, got, err, tc.want, tc.wantErr)
		}
	}
}
