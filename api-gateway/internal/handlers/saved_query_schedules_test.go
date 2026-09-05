package handlers

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/jackc/pgx/v5/pgconn"
)

// nextScheduleRun reproduces Temporal's tick arithmetic locally instead of asking
// Temporal, so these tests exist to pin the arithmetic to Temporal's actual rules. The
// failure mode is quiet: a wrong next-run time renders as a confident timestamp on the
// Scheduled Queries page and a user stops checking a schedule that is not firing when
// they think it is.

// Temporal's IntervalSpec matches times expressible as Epoch + N*Every (+Offset), so
// ticks are aligned to the Unix epoch — NOT to `now`, and NOT to when the schedule was
// created. `from + every` is the plausible wrong answer this pins against.
func TestNextScheduleRun_IntervalAlignsToUnixEpochNotToNow(t *testing.T) {
	// 10:00:37Z — deliberately off-cadence, so epoch alignment and "from + every"
	// give visibly different answers.
	from := time.Date(2026, 8, 15, 10, 0, 37, 0, time.UTC)

	got := nextScheduleRun("interval", ScheduleSpec{EverySeconds: 3600}, from)
	if got == nil {
		t.Fatal("expected a next run for a valid interval schedule")
	}
	want := time.Date(2026, 8, 15, 11, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Errorf("interval next run = %s, want %s (epoch-aligned, not from+every)", got, want)
	}
	if got.Equal(from.Add(time.Hour)) {
		t.Error("next run is from+every: that is the un-aligned answer Temporal does not use")
	}
}

func TestNextScheduleRun_IntervalIsStrictlyInTheFuture(t *testing.T) {
	// Exactly on a boundary: the next tick must be the FOLLOWING one, never `from`
	// itself, or a list would show a next run that is already in the past.
	from := time.Date(2026, 8, 15, 10, 0, 0, 0, time.UTC)
	got := nextScheduleRun("interval", ScheduleSpec{EverySeconds: 3600}, from)
	if got == nil {
		t.Fatal("expected a next run")
	}
	if !got.After(from) {
		t.Errorf("next run %s is not after %s", got, from)
	}
	want := time.Date(2026, 8, 15, 11, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Errorf("interval next run = %s, want %s", got, want)
	}
}

// The whole reason main.go imports time/tzdata: the runtime image is alpine and ships
// no zoneinfo, and the create dialog defaults the timezone to the BROWSER's zone. If
// this test fails in CI but passes locally, the embed was dropped.
func TestNextScheduleRun_CronHonoursNonUTCTimezone(t *testing.T) {
	// 02:00 in Asia/Kolkata (UTC+05:30) is 20:30 UTC the previous day.
	from := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	got := nextScheduleRun("cron", ScheduleSpec{Cron: "0 2 * * *", Timezone: "Asia/Kolkata"}, from)
	if got == nil {
		t.Fatal("expected a next run; if this is nil the embedded tzdata is missing")
	}
	want := time.Date(2026, 8, 15, 20, 30, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Errorf("cron next run = %s, want %s", got, want)
	}
}

func TestNextScheduleRun_CronDefaultsToUTC(t *testing.T) {
	from := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	got := nextScheduleRun("cron", ScheduleSpec{Cron: "0 2 * * *"}, from)
	if got == nil {
		t.Fatal("expected a next run")
	}
	want := time.Date(2026, 8, 16, 2, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Errorf("cron next run = %s, want %s", got, want)
	}
}

// Every unknowable case must be nil, never a guess. A blank cell reads as "unknown";
// a wrong timestamp reads as a promise.
func TestNextScheduleRun_UnknowableCasesReturnNil(t *testing.T) {
	from := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name         string
		scheduleType string
		spec         ScheduleSpec
	}{
		{"empty cron", "cron", ScheduleSpec{}},
		{"unparseable cron", "cron", ScheduleSpec{Cron: "not a cron"}},
		{"unknown timezone", "cron", ScheduleSpec{Cron: "0 2 * * *", Timezone: "Mars/Olympus_Mons"}},
		{"zero interval", "interval", ScheduleSpec{EverySeconds: 0}},
		{"negative interval", "interval", ScheduleSpec{EverySeconds: -60}},
		{"unknown schedule type", "sometimes", ScheduleSpec{Cron: "0 2 * * *"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := nextScheduleRun(tc.scheduleType, tc.spec, from); got != nil {
				t.Errorf("expected nil for %s, got %s", tc.name, got)
			}
		})
	}
}

// Anything validateScheduleSpec lets through must be computable here, or a schedule
// could be created that the list page can never describe.
func TestNextScheduleRun_AcceptsEverySpecValidationAllows(t *testing.T) {
	from := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		scheduleType string
		spec         ScheduleSpec
	}{
		{"cron", ScheduleSpec{Cron: "0 * * * *"}},
		{"cron", ScheduleSpec{Cron: "*/5 * * * *"}},
		{"cron", ScheduleSpec{Cron: "0 0 1 * *"}},
		{"cron", ScheduleSpec{Cron: "0 9 * * 1", Timezone: "America/New_York"}},
		{"interval", ScheduleSpec{EverySeconds: 60}},
		{"interval", ScheduleSpec{EverySeconds: 86400}},
	}
	for _, tc := range cases {
		if err := validateScheduleSpec(tc.scheduleType, tc.spec); err != nil {
			t.Fatalf("test premise broken: %v is not actually valid: %v", tc.spec, err)
		}
		got := nextScheduleRun(tc.scheduleType, tc.spec, from)
		if got == nil {
			t.Errorf("validateScheduleSpec accepts %+v but nextScheduleRun cannot compute it", tc.spec)
			continue
		}
		if !got.After(from) {
			t.Errorf("next run %s for %+v is not in the future", got, tc.spec)
		}
	}
}

// modelRunTrigger exists so a call site cannot reach the saved_query_runs CHECK
// constraint with a value it will reject. Pin the constants to the exact strings the
// constraint names.
func TestModelRunTriggerValuesMatchTheCheckConstraint(t *testing.T) {
	if string(triggerManual) != "manual" {
		t.Errorf("triggerManual = %q, want %q (migration 086 CHECK)", triggerManual, "manual")
	}
	if string(triggerScheduled) != "scheduled" {
		t.Errorf("triggerScheduled = %q, want %q (migration 086 CHECK)", triggerScheduled, "scheduled")
	}
	if string(triggerTriggered) != "triggered" {
		t.Errorf("triggerTriggered = %q, want %q (migration 095 CHECK)", triggerTriggered, "triggered")
	}
}

// ============================================================================
// after_pipeline (migration 095)
// ============================================================================

// The boundary this whole design rests on. validateScheduleSpec is shared with pipeline
// schedules (pipeline_schedules.go, pipelines.go attachScheduleForChat), and every one of
// those callers hands what it accepts straight to createTemporalSchedule — which has no
// branch for a type that is not a cadence. It would not fail; it would register an empty
// client.ScheduleSpec and return success, leaving a pipeline schedule that never fires and
// never says why.
//
// So: teaching the SHARED validator about after_pipeline is the bug, and this test is
// what fails when someone does it.
func TestSharedScheduleValidatorStillRejectsAfterPipeline(t *testing.T) {
	if err := validateScheduleSpec(scheduleAfterPipeline, ScheduleSpec{}); err == nil {
		t.Fatal("validateScheduleSpec accepted after_pipeline. Every caller of it builds a " +
			"Temporal schedule from the result, and createTemporalSchedule has no case for a " +
			"non-cadence type — it would silently register a schedule that never fires. " +
			"after_pipeline belongs in validateModelScheduleSpec only.")
	}
}

func TestValidateModelScheduleSpec_AfterPipelineNeedsAPipeline(t *testing.T) {
	pipelineID := "8f14e45f-ceea-467a-9f52-f5b3a1f2c7d9"

	cases := []struct {
		name         string
		scheduleType string
		spec         ScheduleSpec
		trigger      string
		wantErr      bool
	}{
		{"valid trigger", scheduleAfterPipeline, ScheduleSpec{}, pipelineID, false},
		{"trigger with surrounding space", scheduleAfterPipeline, ScheduleSpec{}, "  " + pipelineID + "  ", false},
		{"trigger with no pipeline", scheduleAfterPipeline, ScheduleSpec{}, "", true},
		{"trigger with blank pipeline", scheduleAfterPipeline, ScheduleSpec{}, "   ", true},
		{"trigger with a non-uuid pipeline", scheduleAfterPipeline, ScheduleSpec{}, "the-nightly-load", true},
		// A cadence is still validated by the shared rules, unchanged.
		{"cron still valid", "cron", ScheduleSpec{Cron: "0 2 * * *"}, "", false},
		{"interval still valid", "interval", ScheduleSpec{EverySeconds: 3600}, "", false},
		{"bad cron still rejected", "cron", ScheduleSpec{Cron: "not a cron"}, "", true},
		// Both halves set has two readings and no safe one. Migration 095's CHECK would
		// reject the row anyway; catching it here makes the message a sentence rather
		// than a constraint name.
		{"cron carrying a pipeline", "cron", ScheduleSpec{Cron: "0 2 * * *"}, pipelineID, true},
		{"interval carrying a pipeline", "interval", ScheduleSpec{EverySeconds: 3600}, pipelineID, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateModelScheduleSpec(tc.scheduleType, tc.spec, tc.trigger)
			if tc.wantErr && err == nil {
				t.Errorf("expected an error for %s/%q, got none", tc.scheduleType, tc.trigger)
			}
			if !tc.wantErr && err != nil {
				t.Errorf("unexpected error for %s/%q: %v", tc.scheduleType, tc.trigger, err)
			}
		})
	}
}

// An event trigger has no next run, and the list page must render that as blank rather
// than as a time. nextScheduleRun returning nil for an unknown type already does this —
// this pins it so a later "helpful" default (now? last run + a guess?) has to fail here
// first. A timestamp in that cell is a promise the platform cannot keep: nothing is
// scheduled, and the model rebuilds if and only if the upstream pipeline finishes.
func TestNextScheduleRun_AfterPipelineHasNoNextRun(t *testing.T) {
	from := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	if got := nextScheduleRun(scheduleAfterPipeline, ScheduleSpec{}, from); got != nil {
		t.Errorf("after_pipeline next run = %s, want nil — an event trigger has no clock", got)
	}
	// Even if a stale cron were left in the spec by a converted schedule, the TYPE is
	// what decides. Otherwise a converted schedule would keep advertising its old cadence.
	if got := nextScheduleRun(scheduleAfterPipeline, ScheduleSpec{Cron: "0 2 * * *"}, from); got != nil {
		t.Errorf("after_pipeline with a leftover cron in the spec = %s, want nil", got)
	}
}

// Every Temporal call site guards on EventDriven(), so this predicate is what stands
// between an event trigger and a nil-pointer path through the Temporal client.
func TestSavedQuerySchedule_EventDriven(t *testing.T) {
	cases := map[string]bool{
		scheduleAfterPipeline: true,
		"cron":                false,
		"interval":            false,
		"":                    false,
	}
	for scheduleType, want := range cases {
		s := &SavedQuerySchedule{ScheduleType: scheduleType}
		if got := s.EventDriven(); got != want {
			t.Errorf("EventDriven() for %q = %v, want %v", scheduleType, got, want)
		}
	}
}

// scheduledSummaryColumns is the exact column list ListSavedQuerySchedules scans, in
// order. Kept as one list so a query change that forgets the scan (or the reverse) fails
// here rather than as a 500 in the Scheduled Queries page.
var scheduledSummaryColumns = []string{
	"schedule_id", "saved_query_id", "name", "description",
	"connection_id", "connection_name", "connector_type",
	"schedule_type", "schedule_spec", "status",
	"trigger_pipeline_id", "trigger_pipeline_name",
	"materialization", "target_table", "statement_class",
	"last_run_at", "last_run_status", "last_run_error",
	"created_by", "created_at", "updated_at",
	"paused_at", "paused_reason",
	"auto_paused_at", "auto_paused_reason",
}

// The Scheduled Queries page offers an edit affordance that opens the same model dialog
// the Explorer does, so this payload carries supports_materialization for the same
// reason the saved-query list does — and gets it from the same resolver. Two payloads
// answering the same question from two hand-written rules is the drift this asserts
// against: the flag is checked against ResolveExplorerCapability, not against a literal.
func TestListSavedQuerySchedules_ReportsMaterializationSupportFromTheConnector(t *testing.T) {
	for _, connectorType := range []string{"postgresql", "mysql", "bigquery", "clickhouse", "databricks", ""} {
		t.Run(connectorType, func(t *testing.T) {
			mock, cleanup := wsScopeMockDB(t)
			defer cleanup()

			mock.ExpectQuery(`FROM saved_query_schedules s[\s\S]+WHERE sq\.workspace_id = \$1`).
				WithArgs(wsScopeWS, wsScopeUser).
				WillReturnRows(sqlmock.NewRows(scheduledSummaryColumns).
					AddRow("b2c3d4e5-1111-2222-3333-444455556666", savedQueryID, "Daily MRR", "",
						savedQueryConn, "warehouse", connectorType,
						"cron", []byte(`{"cron":"0 2 * * *","timezone":"UTC"}`), "active",
						"", "",
						"table", "public.daily_mrr", "read",
						nil, "", "",
						wsScopeUser, time.Now(), time.Now(),
						nil, "",
						nil, ""))

			r := savedQueryRouter(http.MethodGet, "/explorer/schedules", "viewer", ListSavedQuerySchedules)
			w := doJSON(r, http.MethodGet, "/explorer/schedules", nil)
			if w.Code != http.StatusOK {
				t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
			}

			var body struct {
				Schedules []struct {
					ConnectorType           string `json:"connector_type"`
					SupportsMaterialization bool   `json:"supports_materialization"`
				} `json:"schedules"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode schedules response: %v", err)
			}
			if len(body.Schedules) != 1 {
				t.Fatalf("expected 1 schedule, got %d: %s", len(body.Schedules), w.Body.String())
			}
			want := ResolveExplorerCapability(connectorType).SupportsMaterialization
			if body.Schedules[0].SupportsMaterialization != want {
				t.Errorf("connector %q: supports_materialization=%v, want %v (per the capability table)",
					connectorType, body.Schedules[0].SupportsMaterialization, want)
			}
		})
	}
}

// ============================================================================
// Schedule creation vs. the approval gate (the write side of the same race)
// ============================================================================
//
// UpdateSavedQuery's approval gate asks "is anything scheduled on this query?"
// and refuses to write sql_text directly if the answer is yes. That answer is
// only worth anything if it cannot change between the asking and the writing.
// The read side is closed by asking inside the transaction that holds the row
// lock; this is the write side — the INSERT that makes the answer yes takes a
// lock on the same row, in the same transaction, so the two orderings are
// mutually exclusive rather than merely unlikely to interleave.
//
// Without it the sequence is: the edit locks the row and sees no schedule; this
// handler inserts one and commits; the edit writes new SQL ungated — onto a query
// that is scheduled by the time the write lands, which is exactly the case the
// gate exists to catch.

// expectSchedulePreamble sets up everything CreateSavedQuerySchedule does before
// it reaches the transaction: the saved-query role gate, the upstream pipeline's
// own role gate, the model load, and the run-as authorization.
//
// after_pipeline rather than a cron on purpose — an event trigger registers
// nothing with Temporal, so this exercises the insert path without a Temporal
// client standing in the way.
func expectSchedulePreamble(mock sqlmock.Sqlmock) {
	mock.ExpectQuery(`FROM saved_queries r\s+JOIN workspace_members`).
		WithArgs(savedQueryID, wsScopeUser, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("admin"))
	mock.ExpectQuery(`FROM pipelines r\s+JOIN workspace_members`).
		WithArgs(schedTriggerPipeline, wsScopeUser, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("admin"))
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

const schedTriggerPipeline = "66666666-6666-6666-6666-666666666666"

func createScheduleBody() map[string]any {
	return map[string]any{
		"schedule_type":       scheduleAfterPipeline,
		"schedule_spec":       map[string]any{"timezone": "UTC"},
		"trigger_pipeline_id": schedTriggerPipeline,
	}
}

// The lock must be taken, must precede the INSERT, and must be in the same
// transaction as it — all three, or it closes nothing. sqlmock is ordered, so the
// sequence Begin → FOR SHARE → INSERT → Commit is the assertion: a handler that
// inserts on the pool fails on the unexpected Begin, one that locks after
// inserting fails on the order, and one that commits the lock before inserting
// fails on the extra Commit.
func TestCreateSavedQuerySchedule_LocksTheSavedQueryInTheInsertTransaction(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	expectSchedulePreamble(mock)

	mock.ExpectBegin()
	// FOR SHARE, not FOR UPDATE: scheduling changes nothing about the saved query,
	// it only has to exclude an edit in flight. Two schedulers of the same query do
	// not need to exclude each other — the partial unique index does that — and
	// FOR UPDATE would make them queue for no reason.
	mock.ExpectQuery(`SELECT id FROM saved_queries WHERE id = \$1 FOR SHARE`).
		WithArgs(savedQueryID).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(savedQueryID))
	mock.ExpectExec(`INSERT INTO saved_query_schedules`).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// Read back for the response. Its presence here is also the proof that the
	// transaction ENDED at the commit above rather than wrapping the rest of the
	// handler: a lock held across the Temporal call and this read would block every
	// edit to the query for as long as they took.
	mock.ExpectQuery(`FROM saved_query_schedules s`).
		WithArgs(savedQueryID).
		WillReturnRows(sqlmock.NewRows([]string{
			"schedule_id", "saved_query_id", "schedule_type", "schedule_spec", "temporal_schedule_id",
			"status", "run_as_user_id", "created_by", "created_at", "updated_at",
			"paused_at", "paused_reason", "auto_paused_at", "auto_paused_reason",
			"trigger_pipeline_id", "name",
		}).AddRow("77777777-7777-7777-7777-777777777777", savedQueryID, scheduleAfterPipeline,
			[]byte(`{"timezone":"UTC"}`), nil,
			"active", wsScopeUser, wsScopeUser, time.Now(), time.Now(),
			nil, nil, nil, nil, schedTriggerPipeline, "Nightly ingest"))

	r := savedQueryRouter(http.MethodPost, "/explorer/saved/:id/schedule", "admin", CreateSavedQuerySchedule)
	w := doJSON(r, http.MethodPost, "/explorer/saved/"+savedQueryID+"/schedule", createScheduleBody())

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("the schedule row must land behind a lock on the query it schedules: %v", err)
	}
}

// The saved query is gone by the time the lock is taken — deleted between the role
// gate and here. That must be a 404 and must not insert, rather than an FK
// violation surfacing as a 500.
func TestCreateSavedQuerySchedule_VanishedQueryIs404AndInsertsNothing(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	expectSchedulePreamble(mock)

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id FROM saved_queries WHERE id = \$1 FOR SHARE`).
		WithArgs(savedQueryID).
		WillReturnError(sql.ErrNoRows)
	// Nothing after the rollback: no INSERT, no commit, no read-back.
	mock.ExpectRollback()

	r := savedQueryRouter(http.MethodPost, "/explorer/saved/:id/schedule", "admin", CreateSavedQuerySchedule)
	w := doJSON(r, http.MethodPost, "/explorer/saved/"+savedQueryID+"/schedule", createScheduleBody())

	if w.Code != http.StatusNotFound {
		t.Fatalf("a query that vanished under the lock must 404, got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("%v", err)
	}
}

// A second schedule for the same query is refused by the partial unique index
// (095). The handler must render that as a 409 the caller can act on, not as a
// 500 — and must roll the transaction back rather than leaving the lock held.
func TestCreateSavedQuerySchedule_SecondScheduleIsAConflictNotAServerError(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	expectSchedulePreamble(mock)

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id FROM saved_queries WHERE id = \$1 FOR SHARE`).
		WithArgs(savedQueryID).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(savedQueryID))
	// A *pgconn.PgError because isUniqueViolation matches through pgdriver.SQLState;
	// see the note in saved_queries_test.go on why a pq.Error would silently stop
	// matching and turn this into a 500.
	mock.ExpectExec(`INSERT INTO saved_query_schedules`).
		WillReturnError(&pgconn.PgError{Code: "23505"})
	mock.ExpectRollback()

	r := savedQueryRouter(http.MethodPost, "/explorer/saved/:id/schedule", "admin", CreateSavedQuerySchedule)
	w := doJSON(r, http.MethodPost, "/explorer/saved/"+savedQueryID+"/schedule", createScheduleBody())

	if w.Code != http.StatusConflict {
		t.Fatalf("a duplicate schedule must 409, got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("%v", err)
	}
}
