package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// TestLoadCDCLiveness_ReadsCorrectTableAndSurfacesStaleness locks the fix for
// KI-CDC-RUNTIME-LIVENESS-WRONG-TABLE. The CDC liveness read used to query a table named
// `table_stats` that does not exist (the real table is pipeline_run_table_stats, migration
// 033) and discarded the resulting error, so lastEventAt was always NULL, the staleness
// branch in cdcLivenessPhase was dead code, and the UI always reported "streaming".
//
// It must key on the DESTINATION-APPLY column last_applied_ts (migration 038), written only
// from the sink's own post-apply TABLE_STATS event (source="kafka_mcp_sink"), NOT the
// source-side last_event_ts (Debezium ts_ms, written by the independent cdcstats consumer).
// last_event_ts stays fresh while the sink is wedged and the destination falls behind, so a
// liveness read keyed on it would keep reporting "streaming" during exactly the wedge this
// bug is about — relocating the defect rather than fixing it. This expectation keys on
// MAX(last_applied_ts): against a query on last_event_ts it never matches, sqlmock errors,
// and the helper returns an invalid time — the RED that proves the wrong column.
func TestLoadCDCLiveness_ReadsCorrectTableAndSurfacesStaleness(t *testing.T) {
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer database.Close()

	pipelineID := "2cb685ed-4cf7-445b-9f77-071794d25423"
	stale := time.Now().Add(-10 * time.Minute) // 600s ago → beyond the 300s staleness bound

	mock.ExpectQuery(`MAX\(last_applied_ts\)[\s\S]*FROM pipeline_run_table_stats`).
		WithArgs(pipelineID).
		WillReturnRows(sqlmock.NewRows([]string{"max", "pending"}).AddRow(stale, int64(0)))

	got, pending := loadCDCLiveness(database, pipelineID)

	if !got.Valid {
		t.Fatalf("expected a valid last_event_ts (wrong table queried?), got NULL")
	}
	if secs := int64(time.Since(got.Time).Seconds()); secs < 300 {
		t.Errorf("staleness = %ds, want > 300s so cdcLivenessPhase can act on it", secs)
	}
	if pending != 0 {
		t.Errorf("pending = %d, want 0", pending)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

// TestLoadCDCLiveness_SumsTheCapturedMinusAppliedBacklog locks the second half of the
// liveness read: the pending count that makes staleness interpretable
// (KI-CDC-QUIET-STREAM-REPORTS-IDLE). Without it the caller has a number of seconds and no
// way to tell a drained stream from a wedged one, which is the whole bug.
//
// Two properties are pinned, and both have bitten in the wild:
//
//   - The SUM must be per-table then summed, clamped at zero per table. The captured and
//     applied counters are written by different producers, so one table observed mid-update
//     can read negative; without GREATEST(...,0) that negative cancels a real backlog on
//     another table and the pipeline reports "caught up" while a table is stuck.
//   - A NULL from either column (a table registered but never counted) must coalesce to 0,
//     not poison the whole SUM into NULL — a NULL scan here would leave pending at its zero
//     value by accident, which is the right answer for the wrong reason and stops being
//     right the moment the column is used differently.
//
// RED against a query that omits the clamp: sqlmock's expectation below does not match and
// the helper returns (invalid, 0).
func TestLoadCDCLiveness_SumsTheCapturedMinusAppliedBacklog(t *testing.T) {
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer database.Close()

	pipelineID := "2cb685ed-4cf7-445b-9f77-071794d25423"

	mock.ExpectQuery(`GREATEST\(COALESCE\(total_events, 0\) - COALESCE\(applied_total_events, 0\), 0\)`).
		WithArgs(pipelineID).
		WillReturnRows(sqlmock.NewRows([]string{"max", "pending"}).
			AddRow(time.Now().Add(-2*time.Hour), int64(4213)))

	_, pending := loadCDCLiveness(database, pipelineID)

	if pending != 4213 {
		t.Errorf("pending = %d, want 4213 — the undrained backlog is what distinguishes "+
			"a wedged stream from a quiet one", pending)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

// TestLoadRuntimeDeps_IssuesDedupQueryAcrossExecutions locks the fix for the duplicate
// dependency-names bug. pipeline_dependencies holds one row PER EXECUTION
// (UNIQUE(pipeline_id, execution_id, kind, identifier), migration 049) and nothing ever
// deletes, so a long-running pipeline accumulates N rows per dependency and the Monitor tab
// renders the same source/sink/destination N times. The fix is DISTINCT ON (d.kind,
// d.identifier) ... ORDER BY d.kind, d.identifier, d.created_at DESC (newest registration
// per dependency). This expectation keys on the DISTINCT ON clause: against the old
// undeduped query it never matches, sqlmock errors, loadRuntimeDeps hits its empty branch
// and returns nil — the RED. Family-agnostic.
func TestLoadRuntimeDeps_IssuesDedupQueryAcrossExecutions(t *testing.T) {
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer database.Close()

	pipelineID := "2cb685ed-4cf7-445b-9f77-071794d25423"

	// The rows Postgres returns AFTER de-duplication — one per (kind, identifier).
	depRows := sqlmock.NewRows([]string{
		"kind", "identifier", "status", "last_checked_at", "last_healthy_at",
		"consecutive_failures", "last_error", "details",
	}).
		AddRow("source", "mysql@latest", "healthy", nil, nil, 0, "", []byte("{}")).
		AddRow("sink", "sink-2cb685ed", "healthy", nil, nil, 0, "", []byte("{}")).
		AddRow("destination", "postgresql@v1.0.0", "healthy", nil, nil, 0, "", []byte("{}"))

	mock.ExpectQuery(`DISTINCT ON \(d\.kind, d\.identifier\)`).
		WithArgs(pipelineID).
		WillReturnRows(depRows)

	deps, health := loadRuntimeDeps(database, pipelineID)

	if len(deps) != 3 {
		t.Fatalf("expected 3 deduped deps, got %d: %+v", len(deps), deps)
	}
	seen := map[string]bool{}
	for _, d := range deps {
		key := d.Kind + ":" + d.Identifier
		if seen[key] {
			t.Errorf("duplicate dependency survived de-dup: %s", key)
		}
		seen[key] = true
	}
	if health != "healthy" {
		t.Errorf("aggregate health = %q, want healthy", health)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

// TestGetPipelineRuntime_PausedPhaseDoesNotReportStaleStreamingMessage locks the fix for
// KI-CDC-PAUSE-STALE-PROGRESS-MESSAGE. Pause writes pipelines.status ONLY — the CDC pause
// handler (backend-orchestrator/cmd/orchestrator/main.go:1764) and the batch PausePipeline
// (pipelines.go:3320) both leave pipeline_progress untouched — so message stays frozen at the
// value the snapshot->streaming handoff stamped ('Streaming pipeline active',
// backend-temporal-adapter/internal/workflows/pipeline_status_activity.go:74) and the health
// banner rendered it next to a correct "Paused" pill.
//
// RED against the pre-fix source: rt.Message was copied verbatim from
// pipeline_progress.message (pipeline_runtime.go:139), so the response carried
// "Streaming pipeline active" and the message assertion below fails while the phase
// assertion passes — the exact two-sources-disagree shape.
//
// wsScopeMockDB / wsScopeRouterAsRole / gateRoleRows / wsScopePipeline / wsScopeUser /
// wsScopeWS come from the sibling workspace-scoping *_test.go files (same package).
func TestGetPipelineRuntime_PausedPhaseDoesNotReportStaleStreamingMessage(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)

	// Read gate: requirePipelineWorkspaceRole(WSViewer).
	mock.ExpectQuery(`SELECT wm\.role\s+FROM pipelines r`).
		WithArgs(wsScopePipeline, wsScopeUser, wsScopeWS).
		WillReturnRows(gateRoleRows("viewer"))
	// The authoritative row: a CDC pipeline the pause handler set to 'paused'.
	mock.ExpectQuery(`SELECT status, sync_mode, created_at, updated_at`).
		WithArgs(wsScopePipeline).
		WillReturnRows(sqlmock.NewRows([]string{"status", "sync_mode", "created_at", "updated_at"}).
			AddRow("paused", "cdc", time.Now(), time.Now()))
	// The progress row nobody rewrote — still on the pre-pause streaming tick.
	mock.ExpectQuery(`FROM pipeline_progress`).
		WithArgs(wsScopePipeline).
		WillReturnRows(sqlmock.NewRows([]string{
			"execution_id", "current_stage", "message",
			"progress_percent", "progress_current_step", "progress_total_steps",
			"blocking_reason_type", "blocking_reason_description", "updated_at",
		}).AddRow("55555555-5555-5555-5555-555555555555", "streaming", "Streaming pipeline active",
			100, 8, 8, nil, nil, time.Now()))
	// Liveness: MAX(last_applied_ts) plus the captured-minus-applied backlog. Both come
	// back from one multi-line query, so the pattern spans lines and the row carries two
	// columns; a paused pipeline has nothing applied recently and nothing pending.
	mock.ExpectQuery(`MAX\(last_applied_ts\)[\s\S]*FROM pipeline_run_table_stats`).
		WithArgs(wsScopePipeline).
		WillReturnRows(sqlmock.NewRows([]string{"max", "pending"}).AddRow(nil, int64(0)))
	mock.ExpectQuery(`DISTINCT ON \(d\.kind, d\.identifier\)`).
		WithArgs(wsScopePipeline).
		WillReturnRows(sqlmock.NewRows([]string{
			"kind", "identifier", "status", "last_checked_at", "last_healthy_at",
			"consecutive_failures", "last_error", "details",
		}).AddRow("source", "postgresql@v1.0.0", "healthy", nil, nil, 0, "", []byte("{}")))

	r := wsScopeRouterAsRole(http.MethodGet, "/pipelines/:id/runtime", GetPipelineRuntime, "viewer")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/pipelines/"+wsScopePipeline+"/runtime", nil))

	if w.Code != http.StatusOK {
		t.Fatalf("a viewer must read /runtime; got %d: %s", w.Code, w.Body.String())
	}
	var got PipelineRuntime
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal runtime response: %v (body=%s)", err, w.Body.String())
	}
	if got.Phase != "paused" {
		t.Fatalf("phase = %q, want paused (derived from pipelines.status)", got.Phase)
	}
	if got.Message != "Pipeline paused" {
		t.Errorf("message = %q, want \"Pipeline paused\" — a paused pipeline must not report the frozen pre-pause progress message", got.Message)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

// TestRuntimeMessage_OnlyRewritesThePausedPhase brackets the fix above: the substitution must
// fire ONLY for status='paused' (whose message is derived, because no pause writer touches
// pipeline_progress) and leave every other phase's progress-authored message alone. In
// particular 'stopped' also maps to phase "paused" (computeRuntimePhase:308) but StopPipeline
// already writes the more specific 'Cancelled by user' (pipelines.go:3241), and a HITL
// blocker's description must never be masked. Without the rawStatus/phase narrowing these
// cases regress to "Pipeline paused".
func TestRuntimeMessage_OnlyRewritesThePausedPhase(t *testing.T) {
	cases := []struct {
		name, phase, rawStatus, in, want string
	}{
		{"paused drops the stale streaming text", "paused", "paused", "Streaming pipeline active", "Pipeline paused"},
		{"paused with an empty progress row still reads paused", "paused", "PAUSED", "", "Pipeline paused"},
		{"stopped keeps the more specific cancel message", "paused", "stopped", "Cancelled by user", "Cancelled by user"},
		{"streaming message survives", "streaming", "completed", "Streaming pipeline active", "Streaming pipeline active"},
		{"HITL blocker description survives", "validating", "running", "Select the tables to sync", "Select the tables to sync"},
		{"failure message survives", "failed", "failed", "publication does not exist", "publication does not exist"},
	}
	for _, tc := range cases {
		if got := runtimeMessage(tc.phase, tc.rawStatus, tc.in); got != tc.want {
			t.Errorf("%s: runtimeMessage(%q, %q, %q) = %q, want %q", tc.name, tc.phase, tc.rawStatus, tc.in, got, tc.want)
		}
	}
}

// TestCDCLivenessPhase pins the precedence cdcLivenessPhase folds dep health and CDC
// event freshness in, which had to be reordered to land
// KI-CDC-DROPPED-SOURCE-TABLE-REPORTS-HEALTHY without the fix breaking its own repro.
//
// The repro is a CDC stream whose selected source table was dropped at the origin. It
// reported phase Idle (no CDC event for 5+ minutes, correctly) beside a green health
// badge (every probe asked "is the process up?", and it is). The fix makes the
// debezium_task dependency report degraded — but under the ORIGINAL ordering
// `case "degraded": return "streaming"` sat ABOVE the staleness check, so the same
// pipeline would have flipped from Idle to Streaming. Degrading the health while
// UPGRADING the phase is worse than the bug it fixes.
//
// RED against the pre-fix source: the (degraded, stale) case below returns
// "streaming" from the old switch.
//
// It then pins the SECOND reordering, for KI-CDC-QUIET-STREAM-REPORTS-IDLE. Staleness on
// its own was being read as "stalled", but a CDC stream over a low-traffic source is stale
// most of the time simply because nobody wrote anything — and the UI turned that into an
// Idle badge and a Resume button for a connector that had never stopped. Proven live:
// rows inserted at the source landed at the destination ~93 s later with no user action
// while the badge read Idle. PendingEvents (captured minus applied) is the discriminator,
// and the (healthy, stale, no backlog) case below is RED against the pre-fix source, which
// returned "idle" for it.
func TestCDCLivenessPhase(t *testing.T) {
	// 10 minutes stale, beyond the 300s bound, with nothing captured left to apply — the
	// quiet-source shape that the old code called stalled.
	stale := &RuntimeLiveness{StaleSeconds: 600}
	// Same staleness, but the source produced events the destination has not written.
	staleBacklogged := &RuntimeLiveness{StaleSeconds: 600, PendingEvents: 4213}
	fresh := &RuntimeLiveness{StaleSeconds: 12}

	cases := []struct {
		name      string
		depHealth string
		liveness  *RuntimeLiveness
		want      string
	}{
		// A dead required dependency is a failure regardless of freshness — it stays
		// the highest-priority branch, because "streaming" and "idle" both imply the
		// pipeline still exists as a going concern.
		{"unhealthy beats staleness", "unhealthy", stale, "failed"},
		{"unhealthy on a fresh stream is still failed", "unhealthy", fresh, "failed"},
		{"unhealthy with a backlog is still failed", "unhealthy", staleBacklogged, "failed"},

		// THE FIRST REPRO ASSERTION. Dropped source table: dependency degraded, no events
		// for 10 minutes because there is no table left to produce any. The badge must
		// stay Idle — the fix adds a reason, it does not invent movement. Note the backlog
		// is ZERO here (nothing is captured either), which is exactly why the quiet-stream
		// branch below has to require a healthy dep and cannot key on the counter alone.
		{"degraded and stale reports idle, not streaming", "degraded", stale, "idle"},

		// Degraded but still moving is what the dependency panel is for.
		{"degraded but fresh still streams", "degraded", fresh, "streaming"},

		// THE SECOND REPRO ASSERTION. Healthy deps, stale because the source is quiet,
		// and nothing captured is waiting: the stream is alive and caught up. Reporting
		// idle here is the button-lying bug.
		{"healthy and stale with no backlog is a quiet stream", "healthy", stale, "streaming"},

		// ...and its control. Same staleness, same green deps, but events were captured
		// and never applied: that IS a wedge, and it must still read idle. Without this
		// case the fix above would be indistinguishable from deleting the check.
		{"healthy and stale with an undrained backlog is idle", "healthy", staleBacklogged, "idle"},

		{"healthy and fresh streams", "healthy", fresh, "streaming"},

		// "unknown" = a legacy pipeline with no dependency manifest. There is no probe to
		// corroborate the empty backlog, so it keeps the old conservative answer.
		{"unknown deps and stale stays idle", "unknown", stale, "idle"},

		// No liveness row at all (nothing has ever written pipeline_run_table_stats
		// for this pipeline): absence of evidence is not staleness.
		{"no liveness row streams", "healthy", nil, "streaming"},
		{"no liveness row with a degraded dep still streams", "degraded", nil, "streaming"},
	}
	for _, tc := range cases {
		if got := cdcLivenessPhase(tc.depHealth, tc.liveness); got != tc.want {
			t.Errorf("%s: cdcLivenessPhase(%q, %+v) = %q, want %q", tc.name, tc.depHealth, tc.liveness, got, tc.want)
		}
	}
}
