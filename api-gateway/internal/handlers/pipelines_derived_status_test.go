package handlers

import (
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// Default-suite guards for issue #6 (list said "Completed" while the detail page
// said "Running"). The behavioural proof — the CASE evaluated by a real
// PostgreSQL over seeded CDC/batch pipelines — lives in
// pipelines_derived_status_pg_test.go, which needs `-tags integration_pg` and so
// never runs in CI. These tests run everywhere and pin the two things that let
// the bug happen:
//
//  1. drift — the list page, the list COUNT and the "Running" stat card each
//     carried a hand-copied CASE, and the COUNT copy had lost a branch. All three
//     must now splice the one const, pipelineDerivedStatusCaseSQL;
//  2. precedence — inside that const, the streaming handoff's
//     `pp.status = 'running'` must outrank the closed snapshot execution but not
//     pipelines.status (pause/stop) or a dead CDC dependency, and the
//     stale-heartbeat branch must skip a successfully-closed CDC execution with a
//     NULL-safe predicate.
//
// wsScopeMockDB / wsScopeRouter / wsScopeWS come from workspace_scoping_test.go.

// sharedCaseRe matches the const verbatim. The anti-vacuity test below proves it
// cannot match a CASE that differs from the const.
func sharedCaseRe() string { return regexp.QuoteMeta(pipelineDerivedStatusCaseSQL) }

func TestPipelineDerivedStatusCase_MatcherIsNotVacuous(t *testing.T) {
	// QuoteMeta("") matches every query, so an emptied const would make the splice
	// tests below pass for any SQL.
	if len(pipelineDerivedStatusCaseSQL) < 500 {
		t.Fatalf("pipelineDerivedStatusCaseSQL is suspiciously short (%d bytes)", len(pipelineDerivedStatusCaseSQL))
	}
	re := regexp.MustCompile(sharedCaseRe())
	if !re.MatchString("SELECT " + pipelineDerivedStatusCaseSQL + " AS derived_status") {
		t.Fatalf("positive control: the matcher must match a query that splices the const")
	}
	drifted := strings.Replace(pipelineDerivedStatusCaseSQL, "WHEN pp.status = 'running' THEN 'running'", "", 1)
	if drifted == pipelineDerivedStatusCaseSQL {
		t.Fatalf("negative control could not be built: the pp.status='running' branch is missing")
	}
	if re.MatchString("SELECT " + drifted + " AS derived_status") {
		t.Fatalf("the matcher accepts a CASE with a branch removed — the splice tests would be vacuous")
	}
}

// TestListPipelines_PageAndCountSpliceTheSharedCase: a ?status= filter's total
// must be computed with the same CASE as the rows it pages over.
func TestListPipelines_PageAndCountSpliceTheSharedCase(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	cols := []string{
		"id", "name", "description", "pipeline_status", "created_at", "updated_at",
		"created_by", "sync_mode", "cdc_mode", "source_connection", "destination_connection",
		"schedule", "last_execution", "derived_status",
	}
	mock.MatchExpectationsInOrder(false)
	// COUNT: the shared CASE feeds base.derived_status, which the filter ($5) reads.
	mock.ExpectQuery(sharedCaseRe()+` AS derived_status[\s\S]*SELECT COUNT\(\*\) FROM base WHERE \(\$5::text IS NULL OR base\.derived_status = \$5\)`).
		WithArgs(wsScopeWS, sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), "running", sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	// Page: the same CASE.
	mock.ExpectQuery(sharedCaseRe()+` AS derived_status[\s\S]*LIMIT \$7 OFFSET \$8`).
		WithArgs(wsScopeWS, sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), "running",
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows(cols))

	r := wsScopeRouter(http.MethodGet, "/api/v1/pipelines", ListPipelines)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/pipelines?page=1&per_page=25&status=running", nil))

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("both list queries must splice pipelineDerivedStatusCaseSQL: %v", err)
	}
}

// TestGetPipelineStats_RunningCardEvaluatesTheListCase: the "Running" card counts
// pipelines whose list badge is 'running' — by evaluating the list's CASE, not a
// hand-mirror of some of its predicates.
func TestGetPipelineStats_RunningCardEvaluatesTheListCase(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	mock.ExpectQuery(`FROM pipelines WHERE workspace_id = \$1`).
		WithArgs(wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"count", "active"}).AddRow(4, 4))
	mock.ExpectQuery(`COUNT\(\*\) FILTER \(WHERE \(CASE WHEN ls\.live_stream THEN 'running'[\s\S]*IN \('completed', 'success'\)\)[\s\S]*e\.id <> e\.pipeline_id`).
		WithArgs(wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"completed", "failed", "total"}).AddRow(5, 0, 5))
	mock.ExpectQuery(`SELECT COUNT\(\*\)[\s\S]*FROM pipelines p[\s\S]*AND \(` + sharedCaseRe() + `\) = 'running'`).
		WithArgs(wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"running"}).AddRow(3))
	mock.ExpectQuery(`FROM executions e[\s\S]*ORDER BY e\.start_time DESC LIMIT 10`).
		WithArgs(wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"pipeline_id", "status", "start_time", "end_time"}))

	body := getStats(t)
	execs, ok := body["executions"].(map[string]any)
	if !ok {
		t.Fatalf("missing executions object: %v", body)
	}
	if got := execs["running"]; got != float64(3) {
		t.Fatalf("running card: want 3 (the shared-CASE count), got %v", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("the running card must evaluate pipelineDerivedStatusCaseSQL: %v", err)
	}
}

// caseSQLWithoutComments drops `-- …` comments so a marker quoted in a comment
// cannot satisfy a precedence check.
func caseSQLWithoutComments() string {
	return regexp.MustCompile(`--[^\n]*`).ReplaceAllString(pipelineDerivedStatusCaseSQL, "")
}

func mustIndexOnce(t *testing.T, sql, marker string) int {
	t.Helper()
	if n := strings.Count(sql, marker); n != 1 {
		t.Fatalf("want exactly one %q in pipelineDerivedStatusCaseSQL, found %d", marker, n)
	}
	return strings.Index(sql, marker)
}

// TestPipelineDerivedStatusCase_Precedence pins branch order. CASE picks the FIRST
// matching WHEN, so order is behaviour.
func TestPipelineDerivedStatusCase_Precedence(t *testing.T) {
	sql := caseSQLWithoutComments()

	ppRunning := mustIndexOnce(t, sql, "WHEN pp.status = 'running' THEN 'running'")

	// Must come AFTER: pipelines.status (pausing/stopping writes only that column,
	// pipeline_progress keeps saying 'running') and the CDC dead-dependency downgrade.
	for _, earlier := range []string{
		"WHEN LOWER(p.status) = 'stopped' THEN 'stopped'",
		"WHEN LOWER(p.status) = 'paused' THEN 'paused'",
		"WHEN LOWER(p.status) = 'failed' THEN 'failed'",
		"WHEN LOWER(p.status) = 'completed' THEN 'passed'",
		"h.status = 'unhealthy'",
	} {
		if i := mustIndexOnce(t, sql, earlier); i > ppRunning {
			t.Errorf("%q must precede `pp.status = 'running'` (a paused or dead stream must not read as running)", earlier)
		}
	}
	// Must come BEFORE: every branch that reads the (closed) executions row.
	for _, later := range []string{
		"WHEN le.execution_status = 'running' THEN 'running'",
		"WHEN le.execution_status = 'failed' AND le.started_at >= NOW() - INTERVAL '24 hours' THEN 'failed'",
		"WHEN (le.execution_status = 'success' OR le.execution_status = 'completed')",
	} {
		if i := mustIndexOnce(t, sql, later); i < ppRunning {
			t.Errorf("%q must follow `pp.status = 'running'` (the closed snapshot execution must not outrank the live stream)", later)
		}
	}

	// The stale-heartbeat branch (the first WHEN, ending at its nested THEN CASE)
	// must exempt a successfully-closed CDC execution, NULL-safely.
	firstThen := mustIndexOnce(t, sql, "THEN CASE")
	branch1 := sql[:firstThen]
	for _, want := range []string{
		"pp.status IN ('processing', 'waiting_for_user')",
		// Not CDC by the shared predicate: an explicit sync_mode wins over a stray
		// cdc_mode, so a batch row with cdc_mode='initial' is still checked.
		"NOT " + pipelineRowIsCDCSQL,
		"OR COALESCE(le.execution_status, '') NOT IN ('success', 'completed')",
	} {
		if !strings.Contains(branch1, want) {
			t.Errorf("stale-heartbeat branch lost %q", want)
		}
	}

	// pipelines has no `mode` column; PostgreSQL resolves p.mode to the mode()
	// aggregate instead of failing.
	if regexp.MustCompile(`\bp\.mode\b`).MatchString(sql) {
		t.Errorf("pipelineDerivedStatusCaseSQL references p.mode")
	}
}
