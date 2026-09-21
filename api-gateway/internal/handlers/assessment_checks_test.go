package handlers

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// ── Grading ─────────────────────────────────────────────────────────────────

func TestGradeCheck(t *testing.T) {
	cases := []struct {
		name       string
		code       string
		sev        AssessmentSeverity
		passed     bool
		wantLevel  AssessmentLevel
		wantResult AssessmentCheckResult
	}{
		{"an error blocks", "POSTGRES_WAL_LEVEL_NOT_LOGICAL", AssessmentError, false, LevelCritical, ResultFailed},
		// sourceReadinessTable keeps an error even when passed, and the gate
		// blocks on it, so the tab must call it critical too.
		{"an error blocks even when marked passed", "POSTGRES_WAL_LEVEL_NOT_LOGICAL", AssessmentError, true, LevelCritical, ResultFailed},
		{"a passed info check passes at its nominal level", "POSTGRES_WAL_LEVEL_NOT_LOGICAL", AssessmentInfo, true, LevelCritical, ResultPassed},
		// A keyless table "passes" (it loads) with a warning about duplicates.
		{"a passed warning is still a warning", "CDC_TABLE_MISSING_PRIMARY_KEY", AssessmentWarning, true, LevelHigh, ResultWarning},
		{"a failed info check is an advisory", "POSTGRES_MAX_SLOT_WAL_KEEP_SIZE_UNLIMITED", AssessmentInfo, false, LevelHigh, ResultWarning},
		{"an advisory never grades critical", "MONGODB_NOT_REPLICA_SET", AssessmentInfo, false, LevelHigh, ResultWarning},
		{"a warning never grades critical", "MONGODB_CHANGE_STREAM_UNAUTHORIZED", AssessmentWarning, false, LevelHigh, ResultWarning},
		{"a low advisory stays low", "POSTGRES_LOGICAL_DECODING_WORK_MEM_LOW", AssessmentInfo, false, LevelLow, ResultWarning},
		{"an unknown code warns at medium", "SOMETHING_NEW", AssessmentWarning, false, LevelMedium, ResultWarning},
		{"an unknown severity fails cautious", "POSTGRES_WAL_SENDER_TIMEOUT_LOW", AssessmentSeverity("bogus"), false, LevelMedium, ResultWarning},
		{"an unknown code that passed is low", "SOMETHING_NEW", AssessmentInfo, true, LevelLow, ResultPassed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			level, result := gradeCheck(tc.code, tc.sev, tc.passed)
			if level != tc.wantLevel || result != tc.wantResult {
				t.Fatalf("gradeCheck(%s, %s, %v) = %s/%s; want %s/%s",
					tc.code, tc.sev, tc.passed, level, result, tc.wantLevel, tc.wantResult)
			}
		})
	}
}

func TestGradeFinding(t *testing.T) {
	cases := []struct {
		code       string
		sev        AssessmentSeverity
		wantLevel  AssessmentLevel
		wantResult AssessmentCheckResult
	}{
		{FindingSinkNoDDL, AssessmentError, LevelCritical, ResultFailed},
		{FindingNoPrimaryKey, AssessmentWarning, LevelHigh, ResultWarning},
		{FindingDestNamespaceWillCreate, AssessmentWarning, LevelLow, ResultWarning},
		// A warning whose code is nominally critical is capped, so Critical
		// stays the set of findings that block.
		{FindingDestNamespaceMissing, AssessmentWarning, LevelHigh, ResultWarning},
		{FindingJSONCollapse, AssessmentInfo, LevelLow, ResultInfo},
		{FindingDestNamespaceExists, AssessmentInfo, LevelLow, ResultInfo},
		{"SOMETHING_NEW", AssessmentWarning, LevelMedium, ResultWarning},
	}
	for _, tc := range cases {
		level, result := gradeFinding(tc.code, tc.sev)
		if level != tc.wantLevel || result != tc.wantResult {
			t.Errorf("gradeFinding(%s, %s) = %s/%s; want %s/%s",
				tc.code, tc.sev, level, result, tc.wantLevel, tc.wantResult)
		}
	}
}

func TestCheckTitle(t *testing.T) {
	if got := checkTitle("POSTGRES_WAL_LEVEL_NOT_LOGICAL"); got != "wal_level is logical" {
		t.Errorf("catalog title = %q", got)
	}
	if got := checkTitle("SOME_NEW_CHECK"); got != "Some new check" {
		t.Errorf("humanized title = %q; want %q", got, "Some new check")
	}
	if got := checkTitle(""); got != "Check" {
		t.Errorf("empty code title = %q", got)
	}
}

// ── Grouping ────────────────────────────────────────────────────────────────

func findCheckRow(checks []AssessmentCheck, code string, result AssessmentCheckResult) (AssessmentCheck, bool) {
	for _, c := range checks {
		if c.Code == code && c.Result == result {
			return c, true
		}
	}
	return AssessmentCheck{}, false
}

func sampleReportAndReadiness() (*AssessmentReport, *orchestratorAssessment) {
	ra := &orchestratorAssessment{Checks: []orchestratorCheck{
		{Code: "POSTGRES_WAL_LEVEL_NOT_LOGICAL", Severity: "error", Message: "wal_level is replica",
			Remediation: &AssessmentRemediation{Steps: []string{"Set wal_level"}, SQLToRun: []string{"ALTER SYSTEM SET wal_level = logical;"}}},
		{Code: "CONNECTOR_TABLE_READABLE", Severity: "info", Passed: true, Object: "public.users", Message: "read public.users"},
		{Code: "CONNECTOR_TABLE_READABLE", Severity: "info", Passed: true, Object: "public.orders", Message: "read public.orders"},
		{Code: "POSTGRES_MAX_SLOT_WAL_KEEP_SIZE_UNLIMITED", Severity: "info", Message: "unlimited"},
		{Code: "POSTGRES_LOGICAL_DECODING_WORK_MEM_LOW", Severity: "info", Message: "64MB"},
		{Code: "POSTGRES_WAL_SENDER_TIMEOUT_LOW", Severity: "info", Passed: true, Message: "60s"},
		{Code: "CDC_TABLE_MISSING_PRIMARY_KEY", Severity: "warning", Passed: true, Object: "public.orders", Message: "keyless, hashed",
			Remediation: &AssessmentRemediation{SQLToRun: []string{`ALTER TABLE "public"."orders" ADD PRIMARY KEY (id);`}}},
	}}
	report := &AssessmentReport{
		SourceType: "postgresql",
		Tables: []AssessmentTable{
			{Name: "users", Schema: "public", Findings: []AssessmentFinding{
				{Code: FindingNoPrimaryKey, Severity: AssessmentWarning, Message: "users has no primary key"},
			}},
			{Name: "orders", Schema: "public", Findings: []AssessmentFinding{
				{Code: FindingNoPrimaryKey, Severity: AssessmentWarning, Message: "orders has no primary key"},
				{Code: FindingJSONCollapse, Severity: AssessmentInfo, Message: "2 nested columns"},
			}},
			// The modal's copy of the orchestrator checks: the tab reads those
			// directly, so this table must not produce a second row.
			{Name: sourceReadinessTableName, Findings: []AssessmentFinding{
				{Code: "POSTGRES_WAL_LEVEL_NOT_LOGICAL", Severity: AssessmentError, Message: "wal_level is replica"},
			}},
			{Name: "(destination namespace)", Findings: []AssessmentFinding{
				{Code: FindingDestNamespaceExists, Severity: AssessmentInfo, Message: "schema analytics exists"},
			}},
		},
	}
	return report, ra
}

func TestAttachChecks_GroupsGradesAndCounts(t *testing.T) {
	report, ra := sampleReportAndReadiness()
	attachChecks(report, ra)

	wal, ok := findCheckRow(report.Checks, "POSTGRES_WAL_LEVEL_NOT_LOGICAL", ResultFailed)
	if !ok || wal.Level != LevelCritical || wal.Category != CategorySource {
		t.Fatalf("wal_level row = %+v, ok=%v", wal, ok)
	}
	n := 0
	for _, c := range report.Checks {
		if c.Code == "POSTGRES_WAL_LEVEL_NOT_LOGICAL" {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("wal_level listed %d times; the (source readiness) table must be skipped", n)
	}
	if wal.Remediation == nil || len(wal.Remediation.SQLToRun) != 1 {
		t.Fatalf("wal_level lost its fix: %+v", wal.Remediation)
	}

	pk, ok := findCheckRow(report.Checks, FindingNoPrimaryKey, ResultWarning)
	if !ok || pk.Level != LevelHigh || pk.Category != CategoryTables {
		t.Fatalf("NO_PRIMARY_KEY row = %+v, ok=%v", pk, ok)
	}
	if len(pk.Objects) != 2 || pk.Objects[0].Name != "public.users" || pk.Objects[1].Name != "public.orders" {
		t.Fatalf("NO_PRIMARY_KEY objects = %+v", pk.Objects)
	}
	if pk.Objects[1].Message != "orders has no primary key" {
		t.Fatalf("per-object message lost: %+v", pk.Objects[1])
	}
	if pk.Message != "Reported on 2 objects — see each one below." {
		t.Fatalf("row message = %q", pk.Message)
	}

	read, ok := findCheckRow(report.Checks, "CONNECTOR_TABLE_READABLE", ResultPassed)
	if !ok || len(read.Objects) != 2 || read.Category != CategoryTables {
		t.Fatalf("table-read row = %+v, ok=%v", read, ok)
	}

	keyless, ok := findCheckRow(report.Checks, "CDC_TABLE_MISSING_PRIMARY_KEY", ResultWarning)
	if !ok || keyless.Level != LevelHigh || keyless.Message != "keyless, hashed" {
		t.Fatalf("a passed warning must list as a warning: %+v, ok=%v", keyless, ok)
	}

	if dest, ok := findCheckRow(report.Checks, FindingDestNamespaceExists, ResultInfo); !ok || dest.Category != CategoryDestination {
		t.Fatalf("destination row = %+v, ok=%v", dest, ok)
	}
	if js, ok := findCheckRow(report.Checks, FindingJSONCollapse, ResultInfo); !ok || len(js.Objects) != 1 {
		t.Fatalf("JSON_COLLAPSE row = %+v, ok=%v", js, ok)
	}

	want := AssessmentCounts{Critical: 1, High: 3, Medium: 0, Low: 1, Passed: 2}
	if report.Counts == nil || *report.Counts != want {
		t.Fatalf("counts = %+v; want %+v (info rows are not counted)", report.Counts, want)
	}

	// Most severe first; within a level failures before warnings before passes.
	for i := 1; i < len(report.Checks); i++ {
		a, b := report.Checks[i-1], report.Checks[i]
		if levelRank[a.Level] > levelRank[b.Level] ||
			(a.Level == b.Level && resultRank[a.Result] > resultRank[b.Result]) {
			t.Fatalf("rows out of order at %d: %s/%s before %s/%s", i, a.Level, a.Result, b.Level, b.Result)
		}
	}
}

func TestAttachChecks_MergesRemediationAcrossObjects(t *testing.T) {
	report := &AssessmentReport{}
	ra := &orchestratorAssessment{Checks: []orchestratorCheck{
		{Code: "CDC_TABLE_MISSING_PRIMARY_KEY", Severity: "error", Object: "public.a", Message: "a",
			Remediation: &AssessmentRemediation{Steps: []string{"Add a key"}, SQLToRun: []string{"ALTER TABLE a;"}}},
		{Code: "CDC_TABLE_MISSING_PRIMARY_KEY", Severity: "error", Object: "public.b", Message: "b",
			Remediation: &AssessmentRemediation{Steps: []string{"Add a key"}, SQLToRun: []string{"ALTER TABLE b;", "ALTER TABLE a;"}}},
	}}
	attachChecks(report, ra)
	if len(report.Checks) != 1 {
		t.Fatalf("want one row, got %+v", report.Checks)
	}
	rem := report.Checks[0].Remediation
	if rem == nil || len(rem.Steps) != 1 || strings.Join(rem.SQLToRun, " ") != "ALTER TABLE a; ALTER TABLE b;" {
		t.Fatalf("remediation = %+v; want the first steps and the union of the SQL", rem)
	}
	// The first check's remediation must not be aliased and grown in place.
	if got := len(ra.Checks[0].Remediation.SQLToRun); got != 1 {
		t.Fatalf("merge mutated the orchestrator's check: %d statements", got)
	}
}

func TestAttachChecks_NilSafe(t *testing.T) {
	attachChecks(nil, nil)
	report := &AssessmentReport{}
	attachChecks(report, nil)
	if report.Checks == nil || len(report.Checks) != 0 || report.Counts == nil {
		t.Fatalf("an empty report must still carry an empty list and zero counts: %+v", report)
	}
	body, _ := json.Marshal(report)
	if !strings.Contains(string(body), `"checks":[]`) {
		t.Fatalf("checks must serialize as [] for the tab, got %s", body)
	}
}

// TestCriticalIsExactlyBlocking pins the one promise the levels make: a
// Critical row is shown if and only if the gate blocks the start. It builds
// the report the way buildPipelineAssessment does — gate from the tables,
// sourceReadinessTable included — for every severity × passed combination.
func TestCriticalIsExactlyBlocking(t *testing.T) {
	sevs := []string{"error", "warning", "info", "bogus", ""}
	findingSevs := []AssessmentSeverity{AssessmentError, AssessmentWarning, AssessmentInfo}
	for _, sev := range sevs {
		for _, passed := range []bool{true, false} {
			for _, fsev := range findingSevs {
				ra := &orchestratorAssessment{Checks: []orchestratorCheck{
					{Code: "POSTGRES_WAL_LEVEL_NOT_LOGICAL", Severity: sev, Passed: passed, Message: "m"},
				}}
				report := &AssessmentReport{Tables: []AssessmentTable{
					{Name: "t", Schema: "public", Findings: []AssessmentFinding{{Code: FindingNoPrimaryKey, Severity: fsev, Message: "f"}}},
				}}
				if tbl, ok := sourceReadinessTable(ra); ok {
					report.Tables = append(report.Tables, tbl)
				}
				report.Blocking = hasBlockingFindings(report.Tables)
				attachChecks(report, ra)
				if (report.Counts.Critical > 0) != report.Blocking {
					t.Errorf("check sev=%q passed=%v, finding sev=%q: critical=%d blocking=%v",
						sev, passed, fsev, report.Counts.Critical, report.Blocking)
				}
			}
		}
	}
}

// ── New-issue detection ─────────────────────────────────────────────────────

func issue(code string, level AssessmentLevel, result AssessmentCheckResult, objects ...string) AssessmentCheck {
	c := AssessmentCheck{Code: code, Level: level, Result: result}
	for _, o := range objects {
		c.Objects = append(c.Objects, AssessmentCheckObject{Name: o})
	}
	return c
}

func TestNewAlertIssues(t *testing.T) {
	base := []AssessmentCheck{
		issue("POSTGRES_WAL_LEVEL_NOT_LOGICAL", LevelCritical, ResultFailed),
		issue(FindingNoPrimaryKey, LevelHigh, ResultWarning, "public.users"),
		issue("POSTGRES_WAL_SENDER_TIMEOUT_LOW", LevelMedium, ResultWarning),
		issue("CONNECTOR_TABLE_READABLE", LevelCritical, ResultPassed, "public.users"),
	}

	t.Run("the first run reports every critical and high issue", func(t *testing.T) {
		got := newAlertIssues(nil, base)
		want := []string{"NO_PRIMARY_KEY|public.users", "POSTGRES_WAL_LEVEL_NOT_LOGICAL"}
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("got %v; want %v", got, want)
		}
	})
	t.Run("an unchanged run reports nothing", func(t *testing.T) {
		if got := newAlertIssues(base, base); len(got) != 0 {
			t.Fatalf("got %v", got)
		}
	})
	t.Run("a new keyless table is news even though the check already fired", func(t *testing.T) {
		cur := append([]AssessmentCheck(nil), base...)
		cur[1] = issue(FindingNoPrimaryKey, LevelHigh, ResultWarning, "public.users", "public.orders")
		got := newAlertIssues(base, cur)
		if len(got) != 1 || got[0] != "NO_PRIMARY_KEY|public.orders" {
			t.Fatalf("got %v", got)
		}
	})
	t.Run("a new medium issue is not news", func(t *testing.T) {
		cur := append(append([]AssessmentCheck(nil), base...),
			issue("POSTGRES_PUBLICATION_PRIVILEGE", LevelMedium, ResultWarning))
		if got := newAlertIssues(base, cur); len(got) != 0 {
			t.Fatalf("got %v", got)
		}
	})
	t.Run("a resolved issue is not news", func(t *testing.T) {
		if got := newAlertIssues(base, base[1:]); len(got) != 0 {
			t.Fatalf("got %v", got)
		}
	})
	t.Run("a high warning that became a critical failure is news", func(t *testing.T) {
		prev := []AssessmentCheck{issue("MONGODB_CHANGE_STREAM_UNAUTHORIZED", LevelHigh, ResultWarning)}
		cur := []AssessmentCheck{issue("MONGODB_CHANGE_STREAM_UNAUTHORIZED", LevelCritical, ResultFailed)}
		// Same code, same (no) object: the key does not change, so this is
		// deliberately NOT re-announced — the owner already knows the stream
		// is denied. Pinned so a change here is a decision, not an accident.
		if got := newAlertIssues(prev, cur); len(got) != 0 {
			t.Fatalf("got %v", got)
		}
	})
}

// ── Notification payload ────────────────────────────────────────────────────

func decodePayload(t *testing.T, raw []byte) map[string]interface{} {
	t.Helper()
	var p map[string]interface{}
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatalf("payload is not JSON: %v", err)
	}
	return p
}

func TestAssessmentNotificationPayload(t *testing.T) {
	const pid = "2cb685ed-4cf7-445b-9f77-071794d25423"

	t.Run("a critical issue alerts as critical", func(t *testing.T) {
		report := &AssessmentReport{SourceType: "postgresql", Checks: []AssessmentCheck{
			issue("POSTGRES_WAL_LEVEL_NOT_LOGICAL", LevelCritical, ResultFailed),
			issue(FindingNoPrimaryKey, LevelHigh, ResultWarning, "public.a", "public.b", "public.c", "public.d"),
		}}
		fresh := newAlertIssues(nil, report.Checks)
		p := decodePayload(t, assessmentNotificationPayload(pid, report, fresh))

		if p["type"] != "pre_migration_assessment" || p["pipeline_id"] != pid {
			t.Fatalf("envelope = %v", p)
		}
		if p["action_url"] != "/pipelines/"+pid+"?tab=assessment" {
			t.Fatalf("action_url = %v", p["action_url"])
		}
		se, _ := p["error"].(map[string]interface{})
		if se["code"] != assessmentNotificationCode || se["severity"] != "critical" || se["audience"] != "user" {
			t.Fatalf("error envelope = %v", se)
		}
		if se["source_db_type"] != "postgresql" {
			t.Fatalf("source_db_type = %v", se["source_db_type"])
		}
		if se["dedup_subject"] != strings.Join(fresh, ",") {
			t.Fatalf("dedup_subject = %v; want %q", se["dedup_subject"], strings.Join(fresh, ","))
		}
		msg, _ := p["message"].(string)
		for _, want := range []string{
			"found 2 new issues",
			"Table primary key (high): public.a, public.b, public.c, 1 more",
			"wal_level is logical (critical)",
		} {
			if !strings.Contains(msg, want) {
				t.Errorf("message %q lacks %q", msg, want)
			}
		}
		if se["user_message"] != msg {
			t.Errorf("user_message differs from message")
		}
	})

	t.Run("a high issue alone alerts as a warning", func(t *testing.T) {
		report := &AssessmentReport{Checks: []AssessmentCheck{
			issue("MONGODB_OPLOG_WINDOW_SHORT", LevelHigh, ResultWarning),
		}}
		p := decodePayload(t, assessmentNotificationPayload(pid, report, []string{"MONGODB_OPLOG_WINDOW_SHORT"}))
		se, _ := p["error"].(map[string]interface{})
		if se["severity"] != "warning" {
			t.Fatalf("severity = %v", se["severity"])
		}
		if msg, _ := p["message"].(string); !strings.Contains(msg, "found 1 new issue: Oplog window (high).") {
			t.Fatalf("message = %q", msg)
		}
	})
}

// ── Persistence ─────────────────────────────────────────────────────────────

type fakeAssessmentPublisher struct{ got chan []byte }

func (f *fakeAssessmentPublisher) Publish(_ context.Context, raw []byte) error {
	f.got <- raw
	return nil
}

func withFakeNotifier(t *testing.T) *fakeAssessmentPublisher {
	t.Helper()
	f := &fakeAssessmentPublisher{got: make(chan []byte, 4)}
	SetAssessmentNotifier(f)
	t.Cleanup(func() { SetAssessmentNotifier(nil) })
	return f
}

func checksJSON(t *testing.T, checks []AssessmentCheck) []byte {
	t.Helper()
	b, err := json.Marshal(checks)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

const (
	assessPID  = "2cb685ed-4cf7-445b-9f77-071794d25423"
	assessUser = "11111111-1111-1111-1111-111111111111"
)

func TestRecordAssessmentRun_StoresTrimsAndAnnouncesOnlyWhatIsNew(t *testing.T) {
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	pub := withFakeNotifier(t)

	previous := []AssessmentCheck{issue(FindingNoPrimaryKey, LevelHigh, ResultWarning, "public.users")}
	report := &AssessmentReport{SourceType: "postgresql", Checks: []AssessmentCheck{
		issue(FindingNoPrimaryKey, LevelHigh, ResultWarning, "public.users", "public.orders"),
		issue("POSTGRES_WAL_SENDER_TIMEOUT_LOW", LevelMedium, ResultWarning),
		issue("CONNECTOR_TABLE_READABLE", LevelCritical, ResultPassed, "public.users"),
	}}
	report.Counts = countChecks(report.Checks)

	mock.ExpectQuery(`SELECT COALESCE\(report->'checks', '\[\]'::jsonb\)\s+FROM pipeline_assessment_runs`).
		WithArgs(assessPID).
		WillReturnRows(sqlmock.NewRows([]string{"checks"}).AddRow(checksJSON(t, previous)))
	mock.ExpectQuery(`INSERT INTO pipeline_assessment_runs`).
		WithArgs(assessPID, AssessmentTriggerManual, assessUser, false, 0, 1, 1, 0, 1, sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("run-1"))
	mock.ExpectExec(`DELETE FROM pipeline_assessment_runs`).
		WithArgs(assessPID, assessmentRunsKept).
		WillReturnResult(sqlmock.NewResult(0, 0))

	runID, err := recordAssessmentRun(context.Background(), database, assessPID, AssessmentTriggerManual, assessUser, report)
	if err != nil || runID != "run-1" {
		t.Fatalf("runID=%q err=%v", runID, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sqlmock expectations: %v", err)
	}

	select {
	case raw := <-pub.got:
		se, _ := decodePayload(t, raw)["error"].(map[string]interface{})
		if se["dedup_subject"] != "NO_PRIMARY_KEY|public.orders" {
			t.Fatalf("announced %v; want only the new keyless table", se["dedup_subject"])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a new high issue was not announced")
	}
}

func TestRecordAssessmentRun_NothingNewIsNotAnnounced(t *testing.T) {
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	pub := withFakeNotifier(t)

	checks := []AssessmentCheck{issue("POSTGRES_WAL_LEVEL_NOT_LOGICAL", LevelCritical, ResultFailed)}
	report := &AssessmentReport{Blocking: true, Checks: checks}

	mock.ExpectQuery(`FROM pipeline_assessment_runs`).
		WithArgs(assessPID).
		WillReturnRows(sqlmock.NewRows([]string{"checks"}).AddRow(checksJSON(t, checks)))
	// A scheduled run has no user: triggered_by must be NULL, not "".
	mock.ExpectQuery(`INSERT INTO pipeline_assessment_runs`).
		WithArgs(assessPID, AssessmentTriggerScheduled, nil, true, 1, 0, 0, 0, 0, sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("run-2"))
	mock.ExpectExec(`DELETE FROM pipeline_assessment_runs`).
		WithArgs(assessPID, assessmentRunsKept).
		WillReturnResult(sqlmock.NewResult(0, 0))

	if _, err := recordAssessmentRun(context.Background(), database, assessPID, AssessmentTriggerScheduled, "", report); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sqlmock expectations: %v", err)
	}
	select {
	case raw := <-pub.got:
		t.Fatalf("announced an issue the previous run already had: %s", raw)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestRecordAssessmentRun_FirstRunAnnouncesAndStoresTheReport(t *testing.T) {
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	pub := withFakeNotifier(t)

	report, ra := sampleReportAndReadiness()
	report.Blocking = true
	attachChecks(report, ra)

	var stored string
	mock.ExpectQuery(`FROM pipeline_assessment_runs`).
		WithArgs(assessPID).
		WillReturnRows(sqlmock.NewRows([]string{"checks"})) // no previous run
	mock.ExpectQuery(`INSERT INTO pipeline_assessment_runs`).
		WithArgs(assessPID, AssessmentTriggerRunGate, assessUser, true, 1, 3, 0, 1, 2, captureJSONArg(&stored)).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("run-3"))
	mock.ExpectExec(`DELETE FROM pipeline_assessment_runs`).
		WithArgs(assessPID, assessmentRunsKept).
		WillReturnResult(sqlmock.NewResult(0, 0))

	if _, err := recordAssessmentRun(context.Background(), database, assessPID, AssessmentTriggerRunGate, assessUser, report); err != nil {
		t.Fatal(err)
	}
	var back AssessmentReport
	if err := json.Unmarshal([]byte(stored), &back); err != nil || len(back.Checks) != len(report.Checks) {
		t.Fatalf("stored report does not round-trip its checks: err=%v n=%d", err, len(back.Checks))
	}
	select {
	case raw := <-pub.got:
		se, _ := decodePayload(t, raw)["error"].(map[string]interface{})
		if se["severity"] != "critical" {
			t.Fatalf("severity = %v", se["severity"])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the first run's critical issue was not announced")
	}
}

func TestRecordAssessmentRun_APreviousRunReadFailureStoresNothing(t *testing.T) {
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	pub := withFakeNotifier(t)

	mock.ExpectQuery(`FROM pipeline_assessment_runs`).
		WithArgs(assessPID).
		WillReturnError(errors.New("connection reset"))

	report := &AssessmentReport{Checks: []AssessmentCheck{issue("POSTGRES_WAL_LEVEL_NOT_LOGICAL", LevelCritical, ResultFailed)}}
	if _, err := recordAssessmentRun(context.Background(), database, assessPID, AssessmentTriggerManual, assessUser, report); err == nil {
		t.Fatal("want an error")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	// Without the previous run every issue would look new: announcing then
	// would re-alert on every transient read failure.
	select {
	case raw := <-pub.got:
		t.Fatalf("announced without knowing the previous run: %s", raw)
	case <-time.After(200 * time.Millisecond):
	}
}

// captureJSONArg is a sqlmock argument matcher that records the value it saw.
type captureArg struct{ into *string }

func captureJSONArg(into *string) sqlmock.Argument { return captureArg{into} }

func (c captureArg) Match(v driver.Value) bool {
	switch s := v.(type) {
	case string:
		*c.into = s
	case []byte:
		*c.into = string(s)
	default:
		return false
	}
	return true
}

// ── Scheduler ───────────────────────────────────────────────────────────────

func TestAssessmentRecheckInterval(t *testing.T) {
	cases := map[string]time.Duration{
		"":        6 * time.Hour,
		"0":       0,
		"0s":      0,
		"2h":      2 * time.Hour,
		"6m":      15 * time.Minute, // a typo must not hammer every source
		"-1h":     6 * time.Hour,
		"garbage": 6 * time.Hour,
		" 12h ":   12 * time.Hour,
	}
	for in, want := range cases {
		t.Setenv("ASSESSMENT_RECHECK_INTERVAL", in)
		if got := assessmentRecheckInterval(); got != want {
			t.Errorf("ASSESSMENT_RECHECK_INTERVAL=%q → %s; want %s", in, got, want)
		}
	}
}

func TestAssessmentRecheckTick(t *testing.T) {
	cases := map[time.Duration]time.Duration{
		6 * time.Hour:    time.Hour,
		3 * time.Hour:    30 * time.Minute,
		15 * time.Minute: 5 * time.Minute,
		24 * time.Hour:   time.Hour,
	}
	for in, want := range cases {
		if got := assessmentRecheckTick(in); got != want {
			t.Errorf("tick(%s) = %s; want %s", in, got, want)
		}
	}
}

// TestDueAssessmentsSQL_UsesTheSharedCDCTest guards the p.mode trap: in
// Postgres an unqualified mode() is the ordered-set aggregate, so a CDC test
// written as p.mode fails at runtime while every string check stays green.
func TestDueAssessmentsSQL_UsesTheSharedCDCTest(t *testing.T) {
	if !strings.Contains(dueAssessmentsSQL, pipelineRowIsCDCSQL) {
		t.Fatal("the re-check must select CDC pipelines with pipelineRowIsCDCSQL")
	}
	if strings.Contains(dueAssessmentsSQL, "p.mode") {
		t.Fatal("pipelines has no mode column; p.mode resolves to the mode() aggregate")
	}
	for _, want := range []string{"p.status = 'running'", "make_interval(secs => $1)", "LIMIT $2"} {
		if !strings.Contains(dueAssessmentsSQL, want) {
			t.Errorf("due query lacks %q", want)
		}
	}
}

func TestRecheckRunningCDCPipelines_SkipsWhenAnotherReplicaHoldsTheLock(t *testing.T) {
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	mock.ExpectQuery(`SELECT pg_try_advisory_lock\(\$1\)`).
		WithArgs(int64(assessmentRecheckLockNamespace)).
		WillReturnRows(sqlmock.NewRows([]string{"locked"}).AddRow(false))

	recheckRunningCDCPipelines(context.Background(), database, 6*time.Hour)
	// No due-pipeline query and no unlock: sqlmock fails either as unexpected.
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestRecheckRunningCDCPipelines_ReleasesTheLockAndSkipsOwnerless(t *testing.T) {
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	mock.ExpectQuery(`SELECT pg_try_advisory_lock\(\$1\)`).
		WithArgs(int64(assessmentRecheckLockNamespace)).
		WillReturnRows(sqlmock.NewRows([]string{"locked"}).AddRow(true))
	mock.ExpectQuery(`FROM pipelines p\s+WHERE p\.status = 'running'`).
		WithArgs(float64(6*3600), assessmentRecheckBatch).
		WillReturnRows(sqlmock.NewRows([]string{"id", "workspace_id", "created_by"}).
			AddRow(assessPID, "", "")) // no owner: buildPipelineAssessment cannot load it
	mock.ExpectExec(`SELECT pg_advisory_unlock\(\$1\)`).
		WithArgs(int64(assessmentRecheckLockNamespace)).
		WillReturnResult(sqlmock.NewResult(0, 0))

	recheckRunningCDCPipelines(context.Background(), database, 6*time.Hour)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// ── History endpoints ───────────────────────────────────────────────────────

var runColumns = []string{"id", "trigger", "triggered_by", "blocking",
	"critical_count", "high_count", "medium_count", "low_count", "passed_count", "created_at"}

func expectAssessmentViewerGate(mock sqlmock.Sqlmock) {
	mock.ExpectQuery(`SELECT wm\.role\s+FROM pipelines r`).
		WithArgs(wsScopePipeline, wsScopeUser, wsScopeWS).
		WillReturnRows(gateRoleRows("viewer"))
}

func TestListPipelineAssessments_ViewerSeesHistoryNewestFirst(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	expectAssessmentViewerGate(mock)
	now := time.Now().UTC().Truncate(time.Second)
	mock.ExpectQuery(`FROM pipeline_assessment_runs\s+WHERE pipeline_id = \$1::uuid\s+ORDER BY created_at DESC\s+LIMIT \$2`).
		WithArgs(wsScopePipeline, assessmentRunsKept). // ?limit=500 is capped
		WillReturnRows(sqlmock.NewRows(runColumns).
			AddRow("run-2", "scheduled", nil, false, 0, 1, 0, 0, 5, now).
			AddRow("run-1", "manual", wsScopeUser, true, 1, 0, 0, 0, 4, now.Add(-time.Hour)))

	r := wsScopeRouterAsRole(http.MethodGet, "/p/:id/assessments", ListPipelineAssessments, "viewer")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/p/"+wsScopePipeline+"/assessments?limit=500", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
	var body struct {
		Runs []assessmentRunSummary `json:"runs"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Runs) != 2 || body.Runs[0].ID != "run-2" || body.Runs[0].TriggeredBy != nil {
		t.Fatalf("runs = %+v", body.Runs)
	}
	if body.Runs[1].TriggeredBy == nil || *body.Runs[1].TriggeredBy != wsScopeUser || body.Runs[1].Counts.Critical != 1 {
		t.Fatalf("second run = %+v", body.Runs[1])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestListPipelineAssessments_EmptyHistoryIsAnEmptyList(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	expectAssessmentViewerGate(mock)
	mock.ExpectQuery(`FROM pipeline_assessment_runs`).
		WithArgs(wsScopePipeline, 20).
		WillReturnRows(sqlmock.NewRows(runColumns))

	r := wsScopeRouterAsRole(http.MethodGet, "/p/:id/assessments", ListPipelineAssessments, "viewer")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/p/"+wsScopePipeline+"/assessments", nil))
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"runs":[]`) {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
}

func TestGetPipelineAssessment(t *testing.T) {
	cols := append(append([]string(nil), runColumns...), "report")
	report := `{"blocking":false,"checks":[{"code":"MONGODB_OPLOG_WINDOW_SHORT","level":"high","result":"warning"}]}`

	t.Run("latest returns the newest run with its report", func(t *testing.T) {
		mock, cleanup := wsScopeMockDB(t)
		defer cleanup()
		expectAssessmentViewerGate(mock)
		mock.ExpectQuery(`FROM pipeline_assessment_runs\s+WHERE pipeline_id = \$1::uuid ORDER BY created_at DESC LIMIT 1`).
			WithArgs(wsScopePipeline).
			WillReturnRows(sqlmock.NewRows(cols).
				AddRow("run-9", "scheduled", nil, false, 0, 1, 0, 0, 3, time.Now(), []byte(report)))

		r := wsScopeRouterAsRole(http.MethodGet, "/p/:id/assessments/:run_id", GetPipelineAssessment, "viewer")
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/p/"+wsScopePipeline+"/assessments/latest", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("status %d: %s", w.Code, w.Body.String())
		}
		var body struct {
			Run    assessmentRunSummary `json:"run"`
			Report AssessmentReport     `json:"report"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if body.Run.ID != "run-9" || len(body.Report.Checks) != 1 || body.Report.Checks[0].Level != LevelHigh {
			t.Fatalf("body = %+v", body)
		}
	})

	t.Run("a run id is scoped to the pipeline", func(t *testing.T) {
		const runID = "44444444-4444-4444-4444-444444444444"
		mock, cleanup := wsScopeMockDB(t)
		defer cleanup()
		expectAssessmentViewerGate(mock)
		mock.ExpectQuery(`WHERE pipeline_id = \$1::uuid AND id = \$2::uuid`).
			WithArgs(wsScopePipeline, runID).
			WillReturnError(sql.ErrNoRows)

		r := wsScopeRouterAsRole(http.MethodGet, "/p/:id/assessments/:run_id", GetPipelineAssessment, "viewer")
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/p/"+wsScopePipeline+"/assessments/"+runID, nil))
		if w.Code != http.StatusNotFound {
			t.Fatalf("status %d: %s", w.Code, w.Body.String())
		}
	})

	t.Run("a malformed run id is a 400", func(t *testing.T) {
		mock, cleanup := wsScopeMockDB(t)
		defer cleanup()
		expectAssessmentViewerGate(mock)

		r := wsScopeRouterAsRole(http.MethodGet, "/p/:id/assessments/:run_id", GetPipelineAssessment, "viewer")
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/p/"+wsScopePipeline+"/assessments/not-a-uuid", nil))
		if w.Code != http.StatusBadRequest {
			t.Fatalf("status %d: %s", w.Code, w.Body.String())
		}
	})
}
