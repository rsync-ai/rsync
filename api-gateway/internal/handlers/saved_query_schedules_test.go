package handlers

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgconn"

	"api-gateway/internal/db"
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
// after_upstream (migration 095, widened by 100)
// ============================================================================

// The boundary this whole design rests on. validateScheduleSpec is shared with pipeline
// schedules (pipeline_schedules.go, pipelines.go attachScheduleForChat), and every one of
// those callers hands what it accepts straight to createTemporalSchedule — which has no
// branch for a type that is not a cadence. It would not fail; it would register an empty
// client.ScheduleSpec and return success, leaving a pipeline schedule that never fires and
// never says why.
//
// So: teaching the SHARED validator about after_upstream is the bug, and this test is
// what fails when someone does it.
func TestSharedScheduleValidatorStillRejectsAfterUpstream(t *testing.T) {
	if err := validateScheduleSpec(scheduleAfterUpstream, ScheduleSpec{}); err == nil {
		t.Fatal("validateScheduleSpec accepted after_upstream. Every caller of it builds a " +
			"Temporal schedule from the result, and createTemporalSchedule has no case for a " +
			"non-cadence type — it would silently register a schedule that never fires. " +
			"after_upstream belongs in validateModelScheduleSpec only.")
	}
}

// Migration 100 moved the upstream out of a column and into a child table, and a parent
// row cannot be constrained by what does or does not reference it. So the CHECK that used
// to refuse an event schedule with no trigger is gone, and this validator is what replaced
// it: every rule below was a column constraint before and is now only Go.
func TestValidateModelScheduleSpec_AfterUpstreamNeedsAtLeastOneUpstream(t *testing.T) {
	pipelineID := "8f14e45f-ceea-467a-9f52-f5b3a1f2c7d9"
	modelID := "0d3d5f2c-1c8e-4b1a-9a2e-51c0b7a6d4f1"
	pipe := func(id string) scheduleUpstream { return scheduleUpstream{Kind: upstreamKindPipeline, ID: id} }
	model := func(id string) scheduleUpstream { return scheduleUpstream{Kind: upstreamKindModel, ID: id} }

	many := make([]scheduleUpstream, 0, maxScheduleUpstreams+1)
	for i := 0; i <= maxScheduleUpstreams; i++ {
		many = append(many, pipe(fmt.Sprintf("8f14e45f-ceea-467a-9f52-f5b3a1f2c%03d", i)))
	}

	cases := []struct {
		name         string
		scheduleType string
		spec         ScheduleSpec
		upstreams    []scheduleUpstream
		wantErr      bool
	}{
		{"one pipeline", scheduleAfterUpstream, ScheduleSpec{}, []scheduleUpstream{pipe(pipelineID)}, false},
		{"one model", scheduleAfterUpstream, ScheduleSpec{}, []scheduleUpstream{model(modelID)}, false},
		{"fan-in across both kinds", scheduleAfterUpstream, ScheduleSpec{}, []scheduleUpstream{pipe(pipelineID), model(modelID)}, false},
		{"surrounding space", scheduleAfterUpstream, ScheduleSpec{}, []scheduleUpstream{pipe("  " + pipelineID + "  ")}, false},
		// The same id under two kinds is two different producers, so it is not a duplicate.
		{"same id, different kinds", scheduleAfterUpstream, ScheduleSpec{}, []scheduleUpstream{pipe(pipelineID), model(pipelineID)}, false},

		{"no upstream at all", scheduleAfterUpstream, ScheduleSpec{}, nil, true},
		{"empty upstream list", scheduleAfterUpstream, ScheduleSpec{}, []scheduleUpstream{}, true},
		{"blank id", scheduleAfterUpstream, ScheduleSpec{}, []scheduleUpstream{pipe("   ")}, true},
		{"non-uuid id", scheduleAfterUpstream, ScheduleSpec{}, []scheduleUpstream{pipe("the-nightly-load")}, true},
		{"unknown kind", scheduleAfterUpstream, ScheduleSpec{}, []scheduleUpstream{{Kind: "connection", ID: pipelineID}}, true},
		{"missing kind", scheduleAfterUpstream, ScheduleSpec{}, []scheduleUpstream{{ID: pipelineID}}, true},
		// Refused rather than deduplicated: collapsing them would hide that the caller and
		// the handler disagree about what the set is.
		{"the same producer twice", scheduleAfterUpstream, ScheduleSpec{}, []scheduleUpstream{pipe(pipelineID), pipe(pipelineID)}, true},
		{"more upstreams than the cap", scheduleAfterUpstream, ScheduleSpec{}, many, true},

		// A cadence is still validated by the shared rules, unchanged.
		{"cron still valid", "cron", ScheduleSpec{Cron: "0 2 * * *"}, nil, false},
		{"interval still valid", "interval", ScheduleSpec{EverySeconds: 3600}, nil, false},
		{"bad cron still rejected", "cron", ScheduleSpec{Cron: "not a cron"}, nil, true},
		// Both halves set has two readings and no safe one. Since 100 nothing at the column
		// level refuses it — a cadence row simply has no upstream rows — so this check is
		// the whole of the rule rather than a readable copy of a constraint.
		{"cron carrying an upstream", "cron", ScheduleSpec{Cron: "0 2 * * *"}, []scheduleUpstream{pipe(pipelineID)}, true},
		{"interval carrying an upstream", "interval", ScheduleSpec{EverySeconds: 3600}, []scheduleUpstream{pipe(pipelineID)}, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateModelScheduleSpec(tc.scheduleType, tc.spec, tc.upstreams)
			if tc.wantErr && err == nil {
				t.Errorf("expected an error for %s/%v, got none", tc.scheduleType, tc.upstreams)
			}
			if !tc.wantErr && err != nil {
				t.Errorf("unexpected error for %s/%v: %v", tc.scheduleType, tc.upstreams, err)
			}
		})
	}
}

// An event trigger has no next run, and the list page must render that as blank rather
// than as a time. nextScheduleRun returning nil for an unknown type already does this —
// this pins it so a later "helpful" default (now? last run + a guess?) has to fail here
// first. A timestamp in that cell is a promise the platform cannot keep: nothing is
// scheduled, and the model rebuilds if and only if an upstream of it finishes.
func TestNextScheduleRun_AfterUpstreamHasNoNextRun(t *testing.T) {
	from := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	if got := nextScheduleRun(scheduleAfterUpstream, ScheduleSpec{}, from); got != nil {
		t.Errorf("after_upstream next run = %s, want nil — an event trigger has no clock", got)
	}
	// Even if a stale cron were left in the spec by a converted schedule, the TYPE is
	// what decides. Otherwise a converted schedule would keep advertising its old cadence.
	if got := nextScheduleRun(scheduleAfterUpstream, ScheduleSpec{Cron: "0 2 * * *"}, from); got != nil {
		t.Errorf("after_upstream with a leftover cron in the spec = %s, want nil", got)
	}
}

// Every Temporal call site guards on EventDriven(), so this predicate is what stands
// between an event trigger and a nil-pointer path through the Temporal client.
func TestSavedQuerySchedule_EventDriven(t *testing.T) {
	cases := map[string]bool{
		scheduleAfterUpstream: true,
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
	"materialization", "target_table", "statement_class",
	"last_run_at", "last_run_status", "last_run_error",
	"created_by", "created_at", "updated_at",
	"paused_at", "paused_reason",
	"auto_paused_at", "auto_paused_reason",
	"upstream_policy",
}

// The list is where a fan-in's cadence is read ("After all of orders, customers run"),
// and the policy is the half of that sentence the client cannot work out for itself. It
// used to be absent from this payload while present on the single-schedule route, so
// the edit dialog showed "all" and the list and the model page said "any" about the
// same schedule. Two rows with different policies, so a hard-coded value fails too.
func TestListSavedQuerySchedules_CarriesTheUpstreamPolicy(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	const waits, eager = "b2c3d4e5-1111-2222-3333-000000000001", "b2c3d4e5-1111-2222-3333-000000000002"
	row := func(scheduleID, name, policy string) []driver.Value {
		return []driver.Value{scheduleID, savedQueryID, name, "",
			savedQueryConn, "warehouse", "postgresql",
			scheduleAfterUpstream, []byte(`{"timezone":"UTC"}`), "active",
			"table", "public." + name, "read",
			nil, "", "",
			wsScopeUser, time.Now(), time.Now(),
			nil, "",
			nil, "",
			policy}
	}
	mock.ExpectQuery(`FROM saved_query_schedules s[\s\S]+WHERE sq\.workspace_id = \$1`).
		WithArgs(wsScopeWS, wsScopeUser).
		WillReturnRows(sqlmock.NewRows(scheduledSummaryColumns).
			AddRow(row(waits, "fanin_all", upstreamPolicyAll)...).
			AddRow(row(eager, "fanin_any", upstreamPolicyAny)...))
	mock.ExpectQuery(`FROM saved_query_schedule_upstreams u`).
		WithArgs(wsScopeWS, wsScopeUser).
		WillReturnRows(sqlmock.NewRows([]string{"schedule_id", "upstream_kind", "upstream_id", "name"}))

	r := savedQueryRouter(http.MethodGet, "/explorer/schedules", "viewer", ListSavedQuerySchedules)
	w := doJSON(r, http.MethodGet, "/explorer/schedules", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var body struct {
		Schedules []map[string]any `json:"schedules"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode schedules response: %v", err)
	}
	got := map[string]any{}
	for _, s := range body.Schedules {
		got[s["schedule_id"].(string)] = s["upstream_policy"]
	}
	if got[waits] != upstreamPolicyAll || got[eager] != upstreamPolicyAny {
		t.Errorf("upstream_policy per schedule = %v, want %s=all and %s=any", got, waits, eager)
	}
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
						"table", "public.daily_mrr", "read",
						nil, "", "",
						wsScopeUser, time.Now(), time.Now(),
						nil, "",
						nil, "",
						upstreamPolicyAny))
			// Since migration 100 the upstreams are a second workspace-scoped query rather
			// than a join, so the list handler is two round trips and this expectation is
			// what fails if someone folds them back into one.
			mock.ExpectQuery(`FROM saved_query_schedule_upstreams u`).
				WithArgs(wsScopeWS, wsScopeUser).
				WillReturnRows(sqlmock.NewRows([]string{"schedule_id", "upstream_kind", "upstream_id", "name"}).
					AddRow("b2c3d4e5-1111-2222-3333-444455556666", upstreamKindPipeline, schedTriggerPipeline, "Nightly ingest"))

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
// after_upstream rather than a cron on purpose — an event trigger registers
// nothing with Temporal, so this exercises the insert path without a Temporal
// client standing in the way.
func expectSchedulePreamble(mock sqlmock.Sqlmock) {
	mock.ExpectQuery(`FROM saved_queries r\s+JOIN workspace_members`).
		WithArgs(savedQueryID, wsScopeUser, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("admin"))
	// The visibility half of requireVisibleSavedQuery. The role gate above answers
	// "you are a member"; this one answers "and you may see this row".
	mock.ExpectQuery(`FROM saved_queries\s+WHERE id = \$1 AND workspace_id = \$2`).
		WithArgs(savedQueryID, wsScopeWS, wsScopeUser).
		WillReturnRows(sqlmock.NewRows([]string{"visible"}).AddRow(1))
	// A pipeline upstream takes the role gate alone: no per-member visibility there.
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
		"schedule_type": scheduleAfterUpstream,
		"schedule_spec": map[string]any{"timezone": "UTC"},
		"upstreams": []map[string]any{
			{"kind": upstreamKindPipeline, "id": schedTriggerPipeline},
		},
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
	// The upstreams are a child table since 100, and they are written inside the SAME
	// transaction as the parent. Committing the schedule without them would leave an
	// event trigger that is active, listed, and impossible for any producer to fire.
	mock.ExpectExec(`INSERT INTO saved_query_schedule_upstreams`).
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
			"paused_at", "paused_reason", "auto_paused_at", "auto_paused_reason", "upstream_policy",
		}).AddRow("77777777-7777-7777-7777-777777777777", savedQueryID, scheduleAfterUpstream,
			[]byte(`{"timezone":"UTC"}`), nil,
			"active", wsScopeUser, wsScopeUser, time.Now(), time.Now(),
			nil, nil, nil, nil, upstreamPolicyAny))
	// The upstreams come back in their own query since 100 — a join here would return one
	// copy of the schedule per upstream to a caller that scans exactly one row.
	mock.ExpectQuery(`FROM saved_query_schedule_upstreams u`).
		WillReturnRows(sqlmock.NewRows([]string{"schedule_id", "upstream_kind", "upstream_id", "name"}).
			AddRow("77777777-7777-7777-7777-777777777777", upstreamKindPipeline, schedTriggerPipeline, "Nightly ingest"))

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

// ============================================================================
// Rings, refused at write time
// ============================================================================
// checkUpstreamCycle is the only place a person is ever told they drew a loop. The
// depth bound in the fire path stops one that gets stored, but silently and forever:
// every completion of every model in the ring walks it again up to the bound. So the
// three outcomes below each have to hold on their own — the named one-hop case, the
// recursive case, and what happens when the graph query itself fails.

// A model naming itself. Refused by name, and refused before the recursive query runs:
// sqlmock has no expectation queued after the preamble, so a handler that falls through
// to the graph walk gets an unexpected-query error and answers 500 instead of 400.
func TestCreateSavedQuerySchedule_AModelNamingItselfIsRefusedBeforeAnyGraphQuery(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	// The path's own gate — role then visibility — and then the same pair again as the
	// upstream being authorized: authorizeUpstreams checks every entry as a resource in
	// its own right and does not special-case the model doing the naming.
	mock.ExpectQuery(`FROM saved_queries r\s+JOIN workspace_members`).
		WithArgs(savedQueryID, wsScopeUser, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("admin"))
	mock.ExpectQuery(`FROM saved_queries\s+WHERE id = \$1 AND workspace_id = \$2`).
		WithArgs(savedQueryID, wsScopeWS, wsScopeUser).
		WillReturnRows(sqlmock.NewRows([]string{"visible"}).AddRow(1))
	mock.ExpectQuery(`FROM saved_queries r\s+JOIN workspace_members`).
		WithArgs(savedQueryID, wsScopeUser, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("admin"))
	// The visibility half of the upstream gate, returning a row: this model is the
	// caller's own, so it is visible. This expectation is also the positive control for
	// the two refusal tests below — it is what proves they fail on visibility rather
	// than on the gate refusing every model it is shown.
	mock.ExpectQuery(`FROM saved_queries\s+WHERE id = \$1 AND workspace_id = \$2`).
		WithArgs(savedQueryID, wsScopeWS, wsScopeUser).
		WillReturnRows(sqlmock.NewRows([]string{"visible"}).AddRow(1))
	mock.ExpectQuery(`FROM saved_queries sq\s+JOIN connections c`).
		WithArgs(savedQueryID).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "workspace_id", "connection_id", "name", "sql_text",
			"materialization", "target_table", "target_owned", "connector_type", "config",
		}).AddRow(savedQueryID, wsScopeWS, savedQueryConn, "Daily MRR", "SELECT 1",
			matTable, "public.daily_mrr", true, "postgresql", "{}"))
	mock.ExpectQuery(`SELECT role FROM workspace_members`).
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("admin"))

	body := map[string]any{
		"schedule_type": scheduleAfterUpstream,
		"schedule_spec": map[string]any{"timezone": "UTC"},
		"upstreams": []map[string]any{
			{"kind": upstreamKindModel, "id": savedQueryID},
		},
	}
	r := savedQueryRouter(http.MethodPost, "/explorer/saved/:id/schedule", "admin", CreateSavedQuerySchedule)
	w := doJSON(r, http.MethodPost, "/explorer/saved/"+savedQueryID+"/schedule", body)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
	// The message matters as much as the status. "would create a cycle" on a one-hop
	// self-reference is the answer an operator is least able to act on.
	if got := scheduleErrorBody(t, w.Body.Bytes()); got != "a model cannot wait on itself" {
		t.Errorf("error = %q, want the self-reference message", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

// ============================================================================
// Whose models a member may wait on
// ============================================================================
// requireResourceRole proves membership and role, and says so itself: there is no
// created_by fallback in it. Visibility is a separate column (migration 084), so
// membership alone let a member name ANOTHER member's private model as an upstream of
// their own schedule — an id that answers 404 on every direct read, whose name then came
// back through the upstream list's join, and whose completion fired that member's
// schedule. Both write paths are covered because both call authorizeUpstreams, and an
// edit is a fresh request from a caller whose access may have changed.
//
// The refusal must be 404 "not found" — what loadSavedQuery answers for a row the caller
// may not see, and what a nonexistent id answers — so the two cannot be told apart and
// private ids cannot be probed. These tests live in the default suite on purpose: CI runs
// `go test ./...` with no -tags, so an integration_pg guard would never run there.

const otherMembersPrivateModel = "77777777-7777-7777-7777-777777777777"

// CREATE. The role gate passes — the caller really is a member of the workspace holding
// that model — and the visibility query is what refuses.
func TestCreateSavedQuerySchedule_AnotherMembersPrivateModelCannotBeNamedAsAnUpstream(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	mock.ExpectQuery(`FROM saved_queries r\s+JOIN workspace_members`).
		WithArgs(savedQueryID, wsScopeUser, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("admin"))
	// The path's own visibility half: the caller may see the model they are scheduling.
	mock.ExpectQuery(`FROM saved_queries\s+WHERE id = \$1 AND workspace_id = \$2`).
		WithArgs(savedQueryID, wsScopeWS, wsScopeUser).
		WillReturnRows(sqlmock.NewRows([]string{"visible"}).AddRow(1))
	// The upstream's own role gate: membership is real, which is exactly why it is not
	// enough on its own.
	mock.ExpectQuery(`FROM saved_queries r\s+JOIN workspace_members`).
		WithArgs(otherMembersPrivateModel, wsScopeUser, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("admin"))
	// Private, written by someone else: the predicate matches nothing.
	mock.ExpectQuery(`FROM saved_queries\s+WHERE id = \$1 AND workspace_id = \$2`).
		WithArgs(otherMembersPrivateModel, wsScopeWS, wsScopeUser).
		WillReturnRows(sqlmock.NewRows([]string{"visible"}))

	body := map[string]any{
		"schedule_type": scheduleAfterUpstream,
		"schedule_spec": map[string]any{"timezone": "UTC"},
		"upstreams": []map[string]any{
			{"kind": upstreamKindModel, "id": otherMembersPrivateModel},
		},
	}
	r := savedQueryRouter(http.MethodPost, "/explorer/saved/:id/schedule", "admin", CreateSavedQuerySchedule)
	w := doJSON(r, http.MethodPost, "/explorer/saved/"+savedQueryID+"/schedule", body)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
	// 403 would confirm the id exists, which is the thing being withheld.
	if got := scheduleErrorBody(t, w.Body.Bytes()); got != "not found" {
		t.Errorf("error = %q, want the same %q loadSavedQuery gives a hidden row", got, "not found")
	}
	// Nothing after the refusal ran: no insert, and no query that could return the
	// hidden model's name.
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

// UPDATE. The same refusal on the edit path, where the caller already owns a schedule and
// is swapping in an upstream they cannot see.
func TestUpdateSavedQuerySchedule_AnotherMembersPrivateModelCannotBeNamedAsAnUpstream(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	const scheduleID = "ba6fc134-0ed4-4210-8180-5fb0ad8660f3"

	mock.ExpectQuery(`FROM saved_queries r\s+JOIN workspace_members`).
		WithArgs(savedQueryID, wsScopeUser, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("admin"))
	// mutableSavedQuerySchedule's visibility half, before it reads the schedule at all.
	mock.ExpectQuery(`FROM saved_queries\s+WHERE id = \$1 AND workspace_id = \$2`).
		WithArgs(savedQueryID, wsScopeWS, wsScopeUser).
		WillReturnRows(sqlmock.NewRows([]string{"visible"}).AddRow(1))
	// Paused, so loadSavedQuerySchedule skips the blocked-connection check; the status is
	// irrelevant to the gate under test and every extra expectation is a way for this
	// test to fail for a reason that is not the one it is about.
	mock.ExpectQuery(`FROM saved_query_schedules s\s+WHERE s.saved_query_id = \$1`).
		WithArgs(savedQueryID).
		WillReturnRows(sqlmock.NewRows([]string{
			"schedule_id", "saved_query_id", "schedule_type", "schedule_spec", "temporal_schedule_id",
			"status", "run_as_user_id", "created_by", "created_at", "updated_at",
			"paused_at", "paused_reason", "auto_paused_at", "auto_paused_reason", "upstream_policy",
		}).AddRow(scheduleID, savedQueryID, scheduleAfterUpstream, []byte(`{"timezone":"UTC"}`), nil,
			"paused", wsScopeUser, wsScopeUser, time.Now(), time.Now(),
			nil, nil, nil, nil, upstreamPolicyAny))
	mock.ExpectQuery(`FROM saved_query_schedule_upstreams u`).
		WithArgs(scheduleID).
		WillReturnRows(sqlmock.NewRows([]string{"schedule_id", "upstream_kind", "id", "name"}))
	mock.ExpectQuery(`FROM saved_queries r\s+JOIN workspace_members`).
		WithArgs(otherMembersPrivateModel, wsScopeUser, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("admin"))
	mock.ExpectQuery(`FROM saved_queries\s+WHERE id = \$1 AND workspace_id = \$2`).
		WithArgs(otherMembersPrivateModel, wsScopeWS, wsScopeUser).
		WillReturnRows(sqlmock.NewRows([]string{"visible"}))

	body := map[string]any{
		"schedule_type": scheduleAfterUpstream,
		"schedule_spec": map[string]any{"timezone": "UTC"},
		"upstreams": []map[string]any{
			{"kind": upstreamKindModel, "id": otherMembersPrivateModel},
		},
	}
	r := savedQueryRouter(http.MethodPut, "/explorer/saved/:id/schedule", "admin", UpdateSavedQuerySchedule)
	w := doJSON(r, http.MethodPut, "/explorer/saved/"+savedQueryID+"/schedule", body)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
	if got := scheduleErrorBody(t, w.Body.Bytes()); got != "not found" {
		t.Errorf("error = %q, want the same %q loadSavedQuery gives a hidden row", got, "not found")
	}
	// The cycle walk sits right after this gate on the edit path. No expectation is
	// queued for it, so a handler that refused later than it should would hit an
	// unexpected query and fail here rather than pass quietly.
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

// The recursive case: the proposed upstream already reaches back to this model through
// some number of hops. The walk goes UP from the upstream, so the row it returns is the
// answer to "does this producer already depend on me".
func TestCheckUpstreamCycle_AnUpstreamThatAlreadyReachesBackIsRefused(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	other := "55555555-5555-5555-5555-555555555555"
	mock.ExpectQuery(`WITH RECURSIVE ancestors`).
		WithArgs(other, savedQueryID).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))

	c, w := cycleTestContext()
	ok := checkUpstreamCycle(c, db.DB, savedQueryID, []scheduleUpstream{{Kind: upstreamKindModel, ID: other}})

	if ok {
		t.Fatal("a ring was allowed")
	}
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

// A pipeline upstream cannot close a model ring — pipelines have no upstreams of their
// own in this table. Walking one would be a query per entry for an answer that is
// always false, and it must not be mistaken for a self-reference either.
func TestCheckUpstreamCycle_APipelineUpstreamIsNotWalked(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	c, _ := cycleTestContext()
	if !checkUpstreamCycle(c, db.DB, savedQueryID, []scheduleUpstream{
		{Kind: upstreamKindPipeline, ID: savedQueryID},
	}) {
		t.Fatal("a pipeline upstream was refused")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("a pipeline upstream must not reach the graph query: %v", err)
	}
}

// The graph query failed, so whether this set closes a ring is unknown. Unknown has to
// refuse: the check cannot be re-run after the write, and a ring that gets stored walks
// itself to the depth bound on every completion of every model in it, forever.
func TestCheckUpstreamCycle_AFailedGraphQueryRefusesRatherThanAllows(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	other := "55555555-5555-5555-5555-555555555555"
	mock.ExpectQuery(`WITH RECURSIVE ancestors`).
		WillReturnError(fmt.Errorf("connection reset"))

	c, w := cycleTestContext()
	ok := checkUpstreamCycle(c, db.DB, savedQueryID, []scheduleUpstream{{Kind: upstreamKindModel, ID: other}})

	if ok {
		t.Fatal("the set was allowed after the cycle check could not be completed")
	}
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

// cycleTestContext is the minimum checkUpstreamCycle reads off a request: a context to
// carry the deadline and a recorder to write the refusal into.
func cycleTestContext() (*gin.Context, *httptest.ResponseRecorder) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/explorer/saved/"+savedQueryID+"/schedule", nil)
	return c, w
}

func scheduleErrorBody(t *testing.T, raw []byte) string {
	t.Helper()
	var out struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("response was not JSON: %v (%s)", err, raw)
	}
	return out.Error
}

// ============================================================================
// Whose models a member may reach by id
// ============================================================================
// The upstream gap above had a sibling on the primary resource. Every per-id model
// endpoint gated on requireResourceRole ALONE, and that gate is membership plus role by
// design. loadSavedQuerySchedule then filters on saved_query_id only — its own comment
// used to say a "role gate" was enough — so a member could read, pause, retarget, run
// and delete another member's PRIVATE model and its schedule: ids that answer 404 on
// GET /explorer/saved/:id. The list endpoint was never affected; it carries the
// visibility predicate in its own query, which is what made the gap easy to miss.
//
// The refusal is 404 "not found" everywhere, identical to a nonexistent id. Before the
// fix the responses were distinguishable — a live id answered "no schedule for this
// saved query" or 200 where a nonexistent one answered "not found" — which is an
// existence oracle on its own, needing no schedule to exist at all.

// Every per-id entry point, one table. A new route that gates on the role half alone
// belongs here; leaving it out is how the next one of these ships.
func TestPerIDModelEndpoints_AnotherMembersPrivateModelIsNotFound(t *testing.T) {
	cases := []struct {
		name   string
		method string
		route  string
		path   string
		h      gin.HandlerFunc
		body   any
	}{
		{"read the schedule", http.MethodGet, "/explorer/saved/:id/schedule", "/schedule", GetSavedQuerySchedule, nil},
		{"read the run history", http.MethodGet, "/explorer/saved/:id/runs", "/runs", ListSavedQueryRuns, nil},
		{"create a schedule", http.MethodPost, "/explorer/saved/:id/schedule", "/schedule", CreateSavedQuerySchedule,
			map[string]any{"schedule_type": scheduleAfterUpstream, "schedule_spec": map[string]any{"timezone": "UTC"}}},
		{"pause the schedule", http.MethodPost, "/explorer/saved/:id/schedule/pause", "/schedule/pause", PauseSavedQuerySchedule, nil},
		{"delete the schedule", http.MethodDelete, "/explorer/saved/:id/schedule", "/schedule", DeleteSavedQuerySchedule, nil},
		{"retarget the materialization", http.MethodPut, "/explorer/saved/:id/materialization", "/materialization",
			SetSavedQueryMaterialization, map[string]any{"materialization": "table", "target_table": "attacker.pwned"}},
		{"run it by hand", http.MethodPost, "/explorer/saved/:id/run", "/run", RunSavedQueryModel, nil},
		// Lives in saved_query_freshness.go, and is the reason this table is a table:
		// it shipped with the same role-only gate months after the ones above, because
		// nothing made the omission visible. Widening a deadline silences the breach
		// alert on a model the caller cannot even read.
		{"widen the freshness deadline", http.MethodPut, "/explorer/saved/:id/freshness", "/freshness",
			SetSavedQueryFreshness, map[string]any{"deadline_seconds": 31536000}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock, cleanup := wsScopeMockDB(t)
			defer cleanup()

			// The role gate passes: the caller really is an admin of the workspace
			// holding that model. That is exactly why it is not enough on its own.
			mock.ExpectQuery(`FROM saved_queries r\s+JOIN workspace_members`).
				WithArgs(otherMembersPrivateModel, wsScopeUser, wsScopeWS).
				WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("admin"))
			// Private, written by someone else: the predicate matches nothing.
			mock.ExpectQuery(`FROM saved_queries\s+WHERE id = \$1 AND workspace_id = \$2`).
				WithArgs(otherMembersPrivateModel, wsScopeWS, wsScopeUser).
				WillReturnRows(sqlmock.NewRows([]string{"visible"}))

			r := savedQueryRouter(tc.method, tc.route, "admin", tc.h)
			w := doJSON(r, tc.method, "/explorer/saved/"+otherMembersPrivateModel+tc.path, tc.body)

			if w.Code != http.StatusNotFound {
				t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
			}
			// 403, or any message naming the schedule, would confirm the id is real.
			if got := scheduleErrorBody(t, w.Body.Bytes()); got != "not found" {
				t.Errorf("error = %q, want the same %q a nonexistent id gives", got, "not found")
			}
			// No expectation is queued past the refusal, so a handler that read the
			// schedule, the runs or the model row anyway hits an unexpected query and
			// fails here rather than passing quietly.
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("sql expectations: %v", err)
			}
		})
	}
}

// The positive control for the table above. Feed the same visibility query a row and the
// handler must carry on to its own work — proving the 404s come from the visibility
// predicate and not from a gate that refuses every model it is shown.
func TestPerIDModelEndpoints_AVisibleModelStillReachesTheHandler(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	mock.ExpectQuery(`FROM saved_queries r\s+JOIN workspace_members`).
		WithArgs(savedQueryID, wsScopeUser, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("admin"))
	mock.ExpectQuery(`FROM saved_queries\s+WHERE id = \$1 AND workspace_id = \$2`).
		WithArgs(savedQueryID, wsScopeWS, wsScopeUser).
		WillReturnRows(sqlmock.NewRows([]string{"visible"}).AddRow(1))
	// Past the gate: the handler's own read runs, and finds no schedule.
	mock.ExpectQuery(`FROM saved_query_schedules s\s+WHERE s.saved_query_id = \$1`).
		WithArgs(savedQueryID).
		WillReturnRows(sqlmock.NewRows([]string{"schedule_id"}))

	r := savedQueryRouter(http.MethodGet, "/explorer/saved/:id/schedule", "admin", GetSavedQuerySchedule)
	w := doJSON(r, http.MethodGet, "/explorer/saved/"+savedQueryID+"/schedule", nil)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
	// A DIFFERENT 404: this one is the handler's own answer, which only a caller who
	// passed the gate can see. Getting "not found" here would mean the control proved
	// nothing, because the gate would have refused a model the caller may see.
	if got := scheduleErrorBody(t, w.Body.Bytes()); got != "no schedule for this saved query" {
		t.Fatalf("error = %q, want the handler's own answer past the gate", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}
