package handlers

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"hash/crc32"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"api-gateway/internal/db"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
)

// POST /api/v1/internal/explorer/models/:id/run-failed.
//
// A scheduled run whose workflow never got an answer from the run endpoint (gateway
// down, request timed out) used to leave nothing in saved_query_runs, so the history
// panel showed only the runs that worked. These tests drive the handler through the
// same middleware the route is registered behind and decide each outcome from what the
// database answers. saved_query_run_failure_pg_test.go runs the same writes against a
// real, migrated Postgres, which is the only place the dedupe lock and the scoping SQL
// actually execute.

const (
	rfModelID    = "dddddddd-0000-4000-8000-000000000001"
	rfScheduleID = "dddddddd-0000-4000-8000-000000000002"
	rfWorkspace  = "dddddddd-0000-4000-8000-000000000003"
	rfRunAs      = "dddddddd-0000-4000-8000-000000000004"
	rfOtherSched = "dddddddd-0000-4000-8000-000000000005"
	rfSecret     = "rf-unit-test-internal-secret"
	rfPath       = "/api/v1/internal/explorer/models/" + rfModelID + "/run-failed"
)

// rfStartedWire carries nanoseconds on purpose; rfStartedStored is the microsecond
// value TIMESTAMPTZ keeps, and the one every read and write must use.
const rfStartedWire = "2026-09-16T10:00:00.123456789Z"

var rfStartedStored = time.Date(2026, 9, 16, 10, 0, 0, 123456000, time.UTC)

func rfRouter() *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	internal := r.Group("/api/v1/internal")
	internal.Use(InternalServiceMiddleware())
	internal.POST("/explorer/models/:id/run-failed", RecordSavedQueryModelRunFailureInternal)
	return r
}

func rfMock(t *testing.T) sqlmock.Sqlmock {
	t.Helper()
	conn, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	prev := db.DB
	db.DB = conn
	t.Cleanup(func() {
		db.DB = prev
		_ = conn.Close()
	})
	return mock
}

func rfBody(t *testing.T, fields map[string]any) string {
	t.Helper()
	b, err := json.Marshal(fields)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(b)
}

func rfServe(t *testing.T, path, body, secret string) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if secret != "" {
		req.Header.Set("X-Internal-Secret", secret)
	}
	rfRouter().ServeHTTP(w, req)
	return w
}

func rfDecode(t *testing.T, w *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("response is not JSON (%d): %s", w.Code, w.Body.String())
	}
	return out
}

// rfCapture matches any string argument and keeps it, so a test can assert on what was
// actually written rather than on what it expected to be written.
type rfCapture struct{ got *string }

func (c rfCapture) Match(v driver.Value) bool {
	s, ok := v.(string)
	if ok {
		*c.got = s
	}
	return ok
}

// rfInstant matches a time argument by instant.
type rfInstant struct{ want time.Time }

func (a rfInstant) Match(v driver.Value) bool {
	got, ok := v.(time.Time)
	return ok && got.Equal(a.want)
}

// rfExpectUpToDedupe queues everything before the EXISTS check: the transaction, the
// model's workspace, the schedule through the requested door, and the lock.
func rfExpectUpToDedupe(mock sqlmock.Sqlmock, eventDoor bool) {
	rfExpectUpToDedupeWithStatus(mock, eventDoor, "active")
}

// rfExpectUpToDedupeWithStatus is rfExpectUpToDedupe for a schedule in the given status.
func rfExpectUpToDedupeWithStatus(mock sqlmock.Sqlmock, eventDoor bool, status string) {
	rfExpectModel(mock)
	mock.ExpectQuery(rfScheduleLookupSQL(eventDoor)).
		WithArgs(rfModelID, rfScheduleID, scheduleAfterUpstream).
		WillReturnRows(sqlmock.NewRows([]string{"schedule_id", "temporal_schedule_id", "run_as_user_id", "status"}).
			AddRow(rfScheduleID, "tsched-1", rfRunAs, status))
	mock.ExpectExec(rfLockSQL).
		WithArgs(int32(modelRunFailureLockNamespace), int32(crc32.ChecksumIEEE([]byte(rfScheduleID)))).
		WillReturnResult(sqlmock.NewResult(0, 0))
}

const (
	rfModelSQL  = `SELECT workspace_id::text FROM saved_queries WHERE id = \$1`
	rfLockSQL   = `SELECT pg_advisory_xact_lock\(\$1, \$2\)`
	rfExistsSQL = `SELECT EXISTS \([\s\S]*FROM saved_query_runs[\s\S]*saved_query_id = \$1 AND schedule_id = \$2::uuid AND started_at = \$3`
	// The badge write, pinned down to the reason it stores: a stamp that says "failed"
	// with no reason is a badge nobody can act on.
	// The history row, pinned to the status it writes and the started_at it binds: a
	// started_at derived from NOW() would differ on every retry and defeat the dedupe.
	rfInsertSQL = `INSERT INTO saved_query_runs[\s\S]*'failed', NULLIF\(\$4, ''\)[\s\S]*NULLIF\(\$5, ''\)::uuid, \$6, NOW\(\)`
	rfStampSQL  = `UPDATE saved_queries[\s\S]*last_run_status = 'failed', last_run_error = NULLIF\(\$2, ''\)[\s\S]*WHERE id = \$1 AND workspace_id = \$3::uuid[\s\S]*last_run_at <= \$4`
)

// rfInsertArgs is the history INSERT's argument list: the row, then its provenance.
func rfInsertArgs(trigger modelRunTrigger, errArg driver.Value, prov ...driver.Value) []driver.Value {
	return append([]driver.Value{rfModelID, rfScheduleID, string(trigger), errArg, rfRunAs, rfInstant{rfStartedStored}}, prov...)
}

// rfNoProvenance is the provenance half of a clock-door row: nothing, whatever was sent.
var rfNoProvenance = []driver.Value{"", "", "", "", int64(0), int64(0)}

func rfScheduleLookupSQL(eventDoor bool) string {
	door := `s\.schedule_type != \$3`
	if eventDoor {
		door = `s\.schedule_type = \$3`
	}
	return `FROM saved_query_schedules s[\s\S]*WHERE s\.saved_query_id = \$1 AND s\.schedule_id = \$2[\s\S]*` + door
}

// rfExpectModel queues the transaction and the model's workspace lookup.
func rfExpectModel(mock sqlmock.Sqlmock) {
	mock.ExpectBegin()
	mock.ExpectQuery(rfModelSQL).
		WithArgs(rfModelID).
		WillReturnRows(sqlmock.NewRows([]string{"workspace_id"}).AddRow(rfWorkspace))
}

func rfExpectExists(mock sqlmock.Sqlmock, exists bool) {
	mock.ExpectQuery(rfExistsSQL).
		WithArgs(rfModelID, rfScheduleID, rfInstant{rfStartedStored}).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(exists))
}

func TestRecordModelRunFailure_RecordsAFailedRowUnderTheClockDoor(t *testing.T) {
	t.Setenv("INTERNAL_SERVICE_SECRET", rfSecret)
	mock := rfMock(t)

	rfExpectUpToDedupe(mock, false)
	rfExpectExists(mock, false)
	var stamped, stored string
	mock.ExpectExec(rfStampSQL).
		WithArgs(rfModelID, rfCapture{&stamped}, rfWorkspace, rfInstant{rfStartedStored}).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(rfInsertSQL).
		WithArgs(rfInsertArgs(triggerScheduled, rfCapture{&stored}, rfNoProvenance...)...).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	reason := "The scheduled run could not reach the model service after 3 attempts: " +
		"dial postgres://admin:hunter2@10.20.30.40:5432/warehouse failed, password=hunter2."
	w := rfServe(t, rfPath, rfBody(t, map[string]any{
		"schedule_id": rfScheduleID, "error": reason, "started_at": rfStartedWire,
	}), rfSecret)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if got := rfDecode(t, w); got["recorded"] != true {
		t.Fatalf("response = %v, want recorded:true", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("writes not made as expected: %v", err)
	}
	if stored == "" {
		t.Fatal("no error text reached the history row")
	}
	if stamped != stored {
		t.Errorf("badge error %q differs from history error %q", stamped, stored)
	}
	for _, leak := range []string{"hunter2", "10.20.30.40"} {
		if strings.Contains(stored, leak) {
			t.Errorf("stored error still contains %q: %s", leak, stored)
		}
	}
	if !strings.Contains(stored, "could not reach the model service") {
		t.Errorf("the plain-words part of the reason was lost: %s", stored)
	}
}

func TestRecordModelRunFailure_EventDoorIsRecordedAsTriggered(t *testing.T) {
	t.Setenv("INTERNAL_SERVICE_SECRET", rfSecret)
	mock := rfMock(t)

	rfExpectUpToDedupe(mock, true)
	rfExpectExists(mock, false)
	mock.ExpectExec(`UPDATE saved_queries`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(rfInsertSQL).
		// provenanceFor counts a pipeline upstream as hop 1, whatever depth was sent.
		WithArgs(rfInsertArgs(triggerTriggered, sqlmock.AnyArg(),
			upstreamKindPipeline, provUpstream, "", "exec-1", int64(1), int64(2))...).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	w := rfServe(t, rfPath, rfBody(t, map[string]any{
		"schedule_id": rfScheduleID, "trigger": scheduleAfterUpstream,
		"error": "timed out", "started_at": rfStartedWire,
		"upstream_kind": upstreamKindPipeline, "upstream_id": provUpstream, "execution_id": "exec-1",
		"depth": 0, "coalesced": 2,
	}), rfSecret)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("event-door failure not recorded as triggered: %v", err)
	}
}

// A retry of the recording activity sends the same started_at. The second delivery
// must find the first row and write nothing — no second history row, no second stamp.
func TestRecordModelRunFailure_ADuplicateDeliveryWritesNothing(t *testing.T) {
	t.Setenv("INTERNAL_SERVICE_SECRET", rfSecret)
	mock := rfMock(t)

	rfExpectUpToDedupe(mock, false)
	rfExpectExists(mock, true)
	mock.ExpectRollback()

	w := rfServe(t, rfPath, rfBody(t, map[string]any{
		"schedule_id": rfScheduleID, "error": "request timed out", "started_at": rfStartedWire,
	}), rfSecret)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (a duplicate is not a failure to retry): %s", w.Code, w.Body.String())
	}
	got := rfDecode(t, w)
	if got["recorded"] != false || got["duplicate"] != true {
		t.Fatalf("response = %v, want recorded:false duplicate:true", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("a duplicate delivery wrote something: %v", err)
	}
}

// A schedule deleted while its run was failing: 404 so the activity stops, and nothing
// written — not the history row, not the badge.
func TestRecordModelRunFailure_DeletedScheduleRecordsNothing(t *testing.T) {
	t.Setenv("INTERNAL_SERVICE_SECRET", rfSecret)
	mock := rfMock(t)

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT workspace_id::text FROM saved_queries WHERE id = \$1`).
		WithArgs(rfModelID).
		WillReturnRows(sqlmock.NewRows([]string{"workspace_id"}).AddRow(rfWorkspace))
	mock.ExpectQuery(`FROM saved_query_schedules s`).
		WithArgs(rfModelID, rfScheduleID, scheduleAfterUpstream).
		WillReturnRows(sqlmock.NewRows([]string{"schedule_id", "temporal_schedule_id", "run_as_user_id", "status"}))
	mock.ExpectRollback()

	w := rfServe(t, rfPath, rfBody(t, map[string]any{
		"schedule_id": rfScheduleID, "error": "gateway unreachable", "started_at": rfStartedWire,
	}), rfSecret)

	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("a deleted schedule's failure was written: %v", err)
	}
}

func TestRecordModelRunFailure_DeletedModelRecordsNothing(t *testing.T) {
	t.Setenv("INTERNAL_SERVICE_SECRET", rfSecret)
	mock := rfMock(t)

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT workspace_id::text FROM saved_queries WHERE id = \$1`).
		WithArgs(rfModelID).
		WillReturnRows(sqlmock.NewRows([]string{"workspace_id"}))
	mock.ExpectRollback()

	w := rfServe(t, rfPath, rfBody(t, map[string]any{
		"schedule_id": rfScheduleID, "error": "gateway unreachable", "started_at": rfStartedWire,
	}), rfSecret)

	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("a deleted model's failure was written: %v", err)
	}
}

// A schedule id that belongs to a different model — in another workspace, or the same
// one — does not match this model's schedule, so nothing is filed under either model.
// The lookup is asked for exactly this (model, schedule) pair; the pg test proves the
// SQL answers "no row" for it against real data.
func TestRecordModelRunFailure_AnotherModelsScheduleIsRefused(t *testing.T) {
	t.Setenv("INTERNAL_SERVICE_SECRET", rfSecret)
	mock := rfMock(t)

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT workspace_id::text FROM saved_queries WHERE id = \$1`).
		WithArgs(rfModelID).
		WillReturnRows(sqlmock.NewRows([]string{"workspace_id"}).AddRow(rfWorkspace))
	mock.ExpectQuery(`WHERE s\.saved_query_id = \$1 AND s\.schedule_id = \$2`).
		WithArgs(rfModelID, rfOtherSched, scheduleAfterUpstream).
		WillReturnRows(sqlmock.NewRows([]string{"schedule_id", "temporal_schedule_id", "run_as_user_id", "status"}))
	mock.ExpectRollback()

	w := rfServe(t, rfPath, rfBody(t, map[string]any{
		"schedule_id": rfOtherSched, "error": "gateway unreachable", "started_at": rfStartedWire,
	}), rfSecret)

	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("another model's schedule was accepted: %v", err)
	}
}

// A database error at any step must reach the activity as a 5xx so its retry runs. Two
// wrong answers are possible and both lose the record silently: a 404 (the activity
// takes "the model or schedule is gone" as done and stops) and a 200 (it takes the
// record as written). Every step is failed in turn; ExpectationsWereMet proves the
// failing step was actually reached, so each 500 comes from that step and no other.
func TestRecordModelRunFailure_ADatabaseErrorAtAnyStepIsReportedForRetry(t *testing.T) {
	dbDown := errors.New("connection reset by peer")
	stampAndInsert := func(mock sqlmock.Sqlmock) {
		mock.ExpectExec(rfStampSQL).WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec(rfInsertSQL).WillReturnResult(sqlmock.NewResult(0, 1))
	}
	cases := []struct {
		name   string
		expect func(sqlmock.Sqlmock)
	}{
		{"begin", func(mock sqlmock.Sqlmock) {
			mock.ExpectBegin().WillReturnError(dbDown)
		}},
		{"model lookup", func(mock sqlmock.Sqlmock) {
			mock.ExpectBegin()
			mock.ExpectQuery(rfModelSQL).WithArgs(rfModelID).WillReturnError(dbDown)
			mock.ExpectRollback()
		}},
		{"schedule lookup", func(mock sqlmock.Sqlmock) {
			rfExpectModel(mock)
			mock.ExpectQuery(rfScheduleLookupSQL(false)).
				WithArgs(rfModelID, rfScheduleID, scheduleAfterUpstream).
				WillReturnError(dbDown)
			mock.ExpectRollback()
		}},
		{"dedupe lock", func(mock sqlmock.Sqlmock) {
			rfExpectModel(mock)
			mock.ExpectQuery(rfScheduleLookupSQL(false)).
				WithArgs(rfModelID, rfScheduleID, scheduleAfterUpstream).
				WillReturnRows(sqlmock.NewRows([]string{"schedule_id", "temporal_schedule_id", "run_as_user_id", "status"}).
					AddRow(rfScheduleID, "tsched-1", rfRunAs, "active"))
			mock.ExpectExec(rfLockSQL).WillReturnError(dbDown)
			mock.ExpectRollback()
		}},
		{"existing-record check", func(mock sqlmock.Sqlmock) {
			rfExpectUpToDedupe(mock, false)
			mock.ExpectQuery(rfExistsSQL).WillReturnError(dbDown)
			mock.ExpectRollback()
		}},
		{"badge stamp", func(mock sqlmock.Sqlmock) {
			rfExpectUpToDedupe(mock, false)
			rfExpectExists(mock, false)
			mock.ExpectExec(rfStampSQL).WillReturnError(dbDown)
			mock.ExpectRollback()
		}},
		{"history insert", func(mock sqlmock.Sqlmock) {
			rfExpectUpToDedupe(mock, false)
			rfExpectExists(mock, false)
			mock.ExpectExec(rfStampSQL).WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectExec(rfInsertSQL).WillReturnError(dbDown)
			mock.ExpectRollback()
		}},
		{"commit", func(mock sqlmock.Sqlmock) {
			rfExpectUpToDedupe(mock, false)
			rfExpectExists(mock, false)
			stampAndInsert(mock)
			// A failed Commit ends the transaction, so the deferred Rollback never reaches
			// the driver and there is no rollback to expect.
			mock.ExpectCommit().WillReturnError(dbDown)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("INTERNAL_SERVICE_SECRET", rfSecret)
			mock := rfMock(t)
			tc.expect(mock)

			w := rfServe(t, rfPath, rfBody(t, map[string]any{
				"schedule_id": rfScheduleID, "error": "gateway unreachable", "started_at": rfStartedWire,
			}), rfSecret)

			if w.Code != http.StatusInternalServerError {
				t.Fatalf("status = %d, want 500 so the activity retries: %s", w.Code, w.Body.String())
			}
			got := rfDecode(t, w)
			if got["recorded"] == true || got["status"] == "not_found" {
				t.Fatalf("response = %v claims an outcome the database never confirmed", got)
			}
			if got["error"] != "failed to record the failed run" {
				t.Fatalf("response = %v, want the handler's own write-failure answer", got)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("the failing step was not the one reached: %v", err)
			}
		})
	}
}

// Pausing stops the next tick, not one that already fired and failed. A paused
// schedule's failure is recorded like any other.
func TestRecordModelRunFailure_APausedScheduleIsStillRecorded(t *testing.T) {
	t.Setenv("INTERNAL_SERVICE_SECRET", rfSecret)
	mock := rfMock(t)

	rfExpectUpToDedupeWithStatus(mock, false, "paused")
	rfExpectExists(mock, false)
	mock.ExpectExec(rfStampSQL).
		WithArgs(rfModelID, sqlmock.AnyArg(), rfWorkspace, rfInstant{rfStartedStored}).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(rfInsertSQL).
		WithArgs(rfInsertArgs(triggerScheduled, sqlmock.AnyArg(), rfNoProvenance...)...).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	w := rfServe(t, rfPath, rfBody(t, map[string]any{
		"schedule_id": rfScheduleID, "error": "gateway unreachable", "started_at": rfStartedWire,
	}), rfSecret)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if got := rfDecode(t, w); got["recorded"] != true {
		t.Fatalf("response = %v, want recorded:true for a paused schedule", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("a paused schedule's failure was not written: %v", err)
	}
}

// The route sits behind InternalServiceMiddleware, which fails closed: no secret
// configured refuses every call, and a wrong or missing header is 401. None of these
// reaches the database. The last case is the control — the right secret does reach the
// handler — so the refusals above it cannot pass because the route is not wired at all.
func TestRecordModelRunFailure_RequiresTheInternalSecret(t *testing.T) {
	body := `{"schedule_id":"` + rfScheduleID + `","error":"x","started_at":"` + rfStartedWire + `"}`
	cases := []struct {
		name       string
		configured string
		header     string
		want       int
	}{
		{"secret not configured", "", rfSecret, http.StatusServiceUnavailable},
		{"no header", rfSecret, "", http.StatusUnauthorized},
		{"wrong header", rfSecret, "not-the-secret", http.StatusUnauthorized},
		{"right header reaches the handler", rfSecret, rfSecret, http.StatusNotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("INTERNAL_SERVICE_SECRET", tc.configured)
			mock := rfMock(t)
			if tc.want == http.StatusNotFound {
				mock.ExpectBegin()
				mock.ExpectQuery(`SELECT workspace_id::text FROM saved_queries`).
					WillReturnRows(sqlmock.NewRows([]string{"workspace_id"}))
				mock.ExpectRollback()
			}
			w := rfServe(t, rfPath, body, tc.header)
			if w.Code != tc.want {
				t.Fatalf("status = %d, want %d: %s", w.Code, tc.want, w.Body.String())
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("database use did not match: %v", err)
			}
		})
	}
}

// Malformed calls are 400 before any database work: no retry would fix them.
func TestRecordModelRunFailure_RejectsMalformedCalls(t *testing.T) {
	t.Setenv("INTERNAL_SERVICE_SECRET", rfSecret)
	cases := []struct {
		name, path, body string
	}{
		{"model id is not a uuid", "/api/v1/internal/explorer/models/not-a-uuid/run-failed",
			`{"schedule_id":"` + rfScheduleID + `","started_at":"` + rfStartedWire + `"}`},
		{"schedule id is not a uuid", rfPath,
			`{"schedule_id":"nope","started_at":"` + rfStartedWire + `"}`},
		{"unknown trigger", rfPath,
			`{"schedule_id":"` + rfScheduleID + `","trigger":"hourly","started_at":"` + rfStartedWire + `"}`},
		{"started_at missing", rfPath,
			`{"schedule_id":"` + rfScheduleID + `"}`},
		{"started_at not a time", rfPath,
			`{"schedule_id":"` + rfScheduleID + `","started_at":"yesterday"}`},
		{"body not json", rfPath, `schedule_id=` + rfScheduleID},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock := rfMock(t)
			w := rfServe(t, tc.path, tc.body, rfSecret)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400: %s", w.Code, w.Body.String())
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("a malformed call reached the database: %v", err)
			}
		})
	}
}

func TestModelRunFailureMessage_ScrubsBoundsAndNeverStoresNothing(t *testing.T) {
	long := "The scheduled run could not be completed: " + strings.Repeat("x ", 4000)
	got := modelRunFailureMessage(long)
	if n := utf8.RuneCountInString(got); n > modelRunFailureErrorMaxRunes+1 {
		t.Errorf("stored error is %d runes, want at most %d plus the ellipsis", n, modelRunFailureErrorMaxRunes)
	}
	if utf8.RuneCountInString(long) <= modelRunFailureErrorMaxRunes {
		t.Fatal("fixture is not longer than the bound, so the bound is untested")
	}

	secret := modelRunFailureMessage("connect to postgres://svc:s3cr3t-pass@db.internal:5432/app failed; api_key=abcd1234")
	for _, leak := range []string{"s3cr3t-pass", "abcd1234"} {
		if strings.Contains(secret, leak) {
			t.Errorf("credential %q survived: %s", leak, secret)
		}
	}

	for _, empty := range []string{"", "   \n"} {
		if msg := modelRunFailureMessage(empty); strings.TrimSpace(msg) == "" {
			t.Errorf("an empty reason %q stored an empty error; the history row would show no reason at all", empty)
		}
	}
}

// #51: a failed run showed pgx's connect error verbatim, source IP included.
func TestModelRunErrorMessageDropsSourceHost(t *testing.T) {
	cases := []struct{ raw, want string }{
		{
			"failed to connect to `user=app database=orders`: 10.20.30.40:5432 (10.20.30.40): dial error: dial tcp 10.20.30.40:5432: connect: connection refused",
			"could not connect to the database: connect: connection refused",
		},
		{
			"failed to connect to `user=app database=orders`: db.internal:5432 (10.20.30.40): server error: FATAL: password authentication failed for user \"app\" (SQLSTATE 28P01)",
			"could not connect to the database: server error: FATAL: password authentication failed for user \"app\" (SQLSTATE 28P01)",
		},
		{
			"failed to connect to `host=db.internal user=app database=orders`: hostname resolving error (dial tcp: lookup db.internal on 127.0.0.11:53: no such host)",
			"could not connect to the database: hostname resolving error (host lookup failed: no such host)",
		},
		{
			"ERROR: relation \"public.orders\" does not exist (SQLSTATE 42P01)",
			"ERROR: relation \"public.orders\" does not exist (SQLSTATE 42P01)",
		},
	}
	for _, c := range cases {
		got := modelRunErrorMessage(c.raw)
		if got != c.want {
			t.Errorf("modelRunErrorMessage(%q)\n got %q\nwant %q", c.raw, got, c.want)
		}
		if strings.Contains(got, "10.20.30.40") || strings.Contains(got, "db.internal") {
			t.Errorf("source host survived: %q", got)
		}
	}
}
