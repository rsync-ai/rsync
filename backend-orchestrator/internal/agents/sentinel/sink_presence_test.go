package sentinel

// sink_presence_test.go — pins the sink-worker presence probe, and the two ways
// "we could not find out" gets mistaken for "everything is fine".
//
// The probe exists because the kafka-mcp-sink container keeps its worker registry
// in memory. When the container restarts, every registered worker is gone and no
// other signal notices: Debezium keeps capturing, the batch export keeps producing
// to Kafka, consumer lag on a dead group reads 0 on a quiet stream, and the
// pipeline stays 'running' with zero rows reaching the destination.
//
// Two gaps this file covers.
//
//  1. The CDC probe collapses THREE answers into two. sinkWorkerAbsent returns a
//     bool, so "the container says not_found" is one value and *everything else* —
//     running, stopped, crashed, a transport error, a body with no status field at
//     all — is the other. The caller then reads that bool as proof of health:
//
//     case sinkRespawnSkip:
//     if !absent { s.resolveLagIssue(ctx, sinkAbsentIssueID(pipelineID), ...) }
//
//     So a sink container that is throwing on every sink_status call (its generic
//     handler returns {"success": false, "error": ...} with NO status key —
//     connector.py:583) DELETEs the escalation that says its worker is missing.
//     The one condition under which the alarm matters most is the one that clears
//     it.
//
//  2. Batch pipelines start a sink worker too (executor.go:3842, "Step 1: Start
//     Kafka-MCP-Sink so it can consume while we export") and nothing probes it.
//     BatchSentinel has no MCP manager at all. A batch run whose sink worker
//     vanished surfaces only after the 20-minute stall threshold, as "no progress"
//     with no named cause — and the operator gets to work out for themselves that
//     the container restarted.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/rsync-ai/backend-orchestrator/internal/mcp"
)

// fakeSinkProbe stands in for mcp.NewClient(manager). It records the consumer
// group of every sink_status call, which is what test
// TestBatchSinkPresenceProbesTheGroupTheExecutorRegistered asserts on.
type fakeSinkProbe struct {
	respond   func(consumerGroup string) (*mcp.ExecuteResponse, error)
	gotGroups []string
}

func (f *fakeSinkProbe) ExecuteWithContext(_ context.Context, req mcp.ExecuteRequest) (*mcp.ExecuteResponse, error) {
	cfg, _ := req.Params["config"].(map[string]interface{})
	group, _ := cfg["consumer_group"].(string)
	f.gotGroups = append(f.gotGroups, group)
	if f.respond == nil {
		return nil, nil
	}
	return f.respond(group)
}

func staticSinkProbe(resp *mcp.ExecuteResponse, err error) *fakeSinkProbe {
	return &fakeSinkProbe{respond: func(string) (*mcp.ExecuteResponse, error) { return resp, err }}
}

func sinkStatusResp(status string) *mcp.ExecuteResponse {
	return &mcp.ExecuteResponse{Success: status != sinkStatusNotFound,
		Result: map[string]interface{}{"status": status}}
}

// TestProbeSinkPresence_TriStatesTheContainersAnswer — the whole point of the
// extraction. "unknown" must be its own value, not folded into "present".
//
// The container's vocabulary is fixed and small (connector.py sink_status):
// not_found, running, stopped, crashed. Anything outside it — including the
// status-less generic error body — means the probe learned nothing.
func TestProbeSinkPresence_TriStatesTheContainersAnswer(t *testing.T) {
	for _, tc := range []struct {
		name string
		resp *mcp.ExecuteResponse
		err  error
		want sinkPresence
	}{
		{
			// The only answer that means "this container holds no worker for that group".
			name: "not_found is absence",
			resp: sinkStatusResp(sinkStatusNotFound),
			want: sinkPresenceAbsent,
		},
		{
			name: "case and whitespace tolerated",
			resp: &mcp.ExecuteResponse{Result: map[string]interface{}{"status": " NOT_FOUND "}},
			want: sinkPresenceAbsent,
		},
		{
			name: "running worker is present",
			resp: sinkStatusResp("running"),
			want: sinkPresencePresent,
		},
		{
			// Registered but not alive: the container's own supervisor owns the respawn.
			// Present, because stepping in would fight that supervisor.
			name: "stopped worker is registered, so present",
			resp: sinkStatusResp("stopped"),
			want: sinkPresencePresent,
		},
		{
			// Terminal crash-loop; the circuit breaker tripped on purpose.
			name: "crashed worker is registered, so present",
			resp: sinkStatusResp("crashed"),
			want: sinkPresencePresent,
		},
		{
			// connector.py:583 — the generic except returns {"success": false,
			// "error": ...} with no status key at all. Today this reads as "not
			// absent", which the caller then treats as proof of health.
			name: "generic failure with no status is UNKNOWN, not present",
			resp: &mcp.ExecuteResponse{Success: false, Error: "Operation failed",
				Result: map[string]interface{}{"success": false}},
			want: sinkPresenceUnknown,
		},
		{
			name: "a status the container has never emitted is UNKNOWN",
			resp: sinkStatusResp("reticulating_splines"),
			want: sinkPresenceUnknown,
		},
		{
			name: "no result body is UNKNOWN",
			resp: &mcp.ExecuteResponse{Success: false, Error: "HTTP request failed"},
			want: sinkPresenceUnknown,
		},
		{
			name: "nil response is UNKNOWN",
			resp: nil,
			want: sinkPresenceUnknown,
		},
		{
			// The container is unreachable. We learned nothing about the worker —
			// least of all that it is running.
			name: "transport error is UNKNOWN",
			err:  context.DeadlineExceeded,
			want: sinkPresenceUnknown,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := probeSinkPresence(context.Background(), staticSinkProbe(tc.resp, tc.err), "sink-abcd1234")
			if got != tc.want {
				t.Errorf("probeSinkPresence = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestProbeSinkPresence_AsksAboutTheGroupItWasGiven — the probe must not re-derive
// a name. See handlers.ResolveSinkConsumerGroup for why: the executor mints three
// shapes and only one of them is "sink-<pid8>".
func TestProbeSinkPresence_AsksAboutTheGroupItWasGiven(t *testing.T) {
	probe := staticSinkProbe(sinkStatusResp("running"), nil)
	probeSinkPresence(context.Background(), probe, "sink-abcd1234-batch")

	if len(probe.gotGroups) != 1 || probe.gotGroups[0] != "sink-abcd1234-batch" {
		t.Errorf("probed groups = %v, want exactly [sink-abcd1234-batch]", probe.gotGroups)
	}
}

func newCDCSentinelForPresence(t *testing.T) (*CDCSentinel, sqlmock.Sqlmock, func()) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	s := &CDCSentinel{db: db, sinkRespawnState: map[string]*connRestartState{}}
	return s, mock, func() { _ = db.Close() }
}

// TestCDCSinkAbsence_UnknownMustNotResolveTheAlarm is gap 1.
//
// The assertion is inverted on purpose, and that inversion is the lesson from
// #730: with sqlmock, an UNEXPECTED Exec fails at the driver and resolveLagIssue
// swallows the error with a Debug log — so simply omitting the expectation asserts
// nothing at all. The only way to prove a statement did NOT run is to expect it and
// require ExpectationsWereMet to come back UNMET.
func TestCDCSinkAbsence_UnknownMustNotResolveTheAlarm(t *testing.T) {
	s, mock, done := newCDCSentinelForPresence(t)
	defer done()

	// The forbidden statement.
	mock.ExpectExec(`DELETE FROM sentinel_active_issues`).
		WithArgs(sinkAbsentIssueID("p1")).
		WillReturnResult(sqlmock.NewResult(0, 1))

	// The container is erroring: success=false, no status key. We learned nothing.
	probe := staticSinkProbe(&mcp.ExecuteResponse{Success: false, Error: "Operation failed",
		Result: map[string]interface{}{"success": false}}, nil)

	s.actOnSinkPresence(context.Background(), probe, nil, sinkPresenceTarget{
		pipelineID:    "p1",
		pipelineName:  "orders stream",
		dbType:        "postgresql",
		consumerGroup: "sink-abcd1234",
	}, time.Now())

	if err := mock.ExpectationsWereMet(); err == nil {
		t.Fatal("the sink-absent escalation was DELETED on a probe that answered nothing.\n\n" +
			"`if !absent { resolveLagIssue(...) }` treats every non-not_found answer as proof " +
			"the worker is back — including a container that is throwing on every sink_status " +
			"call. The alarm is cleared exactly when it is least safe to clear it.")
	}
}

// The positive control. Without it the test above would also pass if NOTHING ever
// resolved the issue, which would be a different bug of the same size.
func TestCDCSinkAbsence_PresentStillResolvesTheAlarm(t *testing.T) {
	s, mock, done := newCDCSentinelForPresence(t)
	defer done()

	mock.ExpectExec(`DELETE FROM sentinel_active_issues`).
		WithArgs(sinkAbsentIssueID("p1")).
		WillReturnResult(sqlmock.NewResult(0, 1))

	s.actOnSinkPresence(context.Background(), staticSinkProbe(sinkStatusResp("running"), nil), nil,
		sinkPresenceTarget{pipelineID: "p1", pipelineName: "orders stream",
			dbType: "postgresql", consumerGroup: "sink-abcd1234"}, time.Now())

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("a worker the container reports as running did not clear its absence alarm: %v", err)
	}
}

// TestDecideSinkRespawn_UnknownPreservesTheAttemptBudget — the same collapse, one
// layer down. A present worker resets the cap, which is right: a container that
// restarts again next month deserves a fresh budget. An UNKNOWN answer resetting
// it is not right — a container flapping between "absent" and "erroring" would
// refill its budget on every other tick and never reach the escalation that tells
// an operator to look.
func TestDecideSinkRespawn_UnknownPreservesTheAttemptBudget(t *testing.T) {
	now := time.Now()
	spent := connRestartState{attempts: 2, firstAttempt: now.Add(-time.Minute), lastAttempt: now.Add(-time.Minute)}

	got, st := decideSinkRespawn(sinkRespawnInputs{
		presence: sinkPresenceUnknown, now: now, st: spent,
		maxAttempts: 3, window: time.Hour, cooldown: time.Minute,
	})
	if got != sinkRespawnSkip {
		t.Fatalf("decision on unknown = %v, want skip", got)
	}
	if st.attempts != 2 {
		t.Errorf("attempts = %d, want 2 — an unknown answer refilled the respawn budget, so a "+
			"container that alternates absent/erroring never escalates", st.attempts)
	}
}

// ── Batch: gap 2 ──────────────────────────────────────────────────────────────

func batchSinkRows(pipelineID, name, execID, consumerGroup string) *sqlmock.Rows {
	return sqlmock.NewRows([]string{"id", "name", "execution_id", "consumer_group"}).
		AddRow(pipelineID, name, execID, consumerGroup)
}

// TestBatchSinkPresenceRaisesAnIssueWhenTheWorkerIsGone — the detection that does
// not exist today. Detect and escalate only: no restart, matching this file's
// stated scope. A batch run carries checkpoint and run-mode state that a blind
// restart can destroy.
func TestBatchSinkPresenceRaisesAnIssueWhenTheWorkerIsGone(t *testing.T) {
	s, mock, done := newBatchSentinel(t)
	defer done()

	mock.ExpectQuery(`FROM pipelines p`).
		WillReturnRows(batchSinkRows("p1", "nightly orders", "e1", "sink-abcd1234-batch"))
	mock.ExpectExec(`INSERT INTO sentinel_active_issues`).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(`SELECT id FROM sentinel_active_issues`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(batchSinkAbsentIssueID("p1")))

	s.probeBatchSinks(context.Background(), staticSinkProbe(sinkStatusResp(sinkStatusNotFound), nil))

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("a batch run whose sink worker is gone raised nothing: %v", err)
	}
}

// TestBatchSinkPresenceProbesTheGroupTheExecutorRegistered — batch is the shape
// that makes guessing actively wrong. The executor names a batch sink
// `sink-<pid8>-batch`; handlers.DerivedSinkConsumerGroup returns `sink-<pid8>`.
// Probing the derived name asks the container about a group it never created, the
// container correctly answers not_found, and every healthy batch run raises a
// false sink-absent alarm forever.
func TestBatchSinkPresenceProbesTheGroupTheExecutorRegistered(t *testing.T) {
	s, mock, done := newBatchSentinel(t)
	defer done()

	mock.ExpectQuery(`FROM pipelines p`).
		WillReturnRows(batchSinkRows("aaaabbbb-0000-4000-8000-000000000001", "nightly orders", "e1",
			"sink-aaaabbbb-batch"))
	mock.ExpectQuery(`SELECT id FROM sentinel_active_issues`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}))

	probe := staticSinkProbe(sinkStatusResp("running"), nil)
	s.probeBatchSinks(context.Background(), probe)

	if len(probe.gotGroups) != 1 || probe.gotGroups[0] != "sink-aaaabbbb-batch" {
		t.Errorf("probed groups = %v, want [sink-aaaabbbb-batch] — the manifest identifier, "+
			"not a name derived from the pipeline id", probe.gotGroups)
	}
}

// TestBatchSinkQueryReadsTheManifestAndNeverFallsBack — the query must JOIN the
// dependency manifest, not LEFT JOIN it. For CDC, falling back to the derived name
// is safe because it is right for the majority shape; for batch it is never right,
// so a pipeline with no manifest row must simply not be probed.
func TestBatchSinkQueryReadsTheManifestAndNeverFallsBack(t *testing.T) {
	q := runningBatchSinksQuery

	if !strings.Contains(q, "kafka_sink_worker") {
		t.Error("the batch sink probe no longer reads the dependency manifest — any other source " +
			"of the consumer group is a guess, and for batch the guess is always wrong")
	}
	if strings.Contains(q, "LEFT JOIN LATERAL (\n\t    SELECT dep.identifier") {
		t.Error("the manifest join is a LEFT JOIN: a pipeline with no registered sink would be " +
			"probed under an empty or derived group name instead of skipped")
	}
	// Same carve-outs the stall detector needs, for the same reason: a CDC pipeline
	// probed here would be double-covered by ensureSinkWorkerPresent.
	if !strings.Contains(q, "sync_mode IS DISTINCT FROM 'cdc'") || !strings.Contains(q, "cdc_mode IS NULL") {
		t.Error("the batch sink probe no longer excludes CDC pipelines")
	}
}

// TestBatchSinkPresenceUnknownDoesNotClearTheIssue — gap 1 again, in the shape it
// takes on the batch side.
//
// resolveStaleIssues DELETEs every open issue in the class that this tick did not
// re-mark as open. So a pipeline whose probe answered nothing must still be marked
// open, or an erroring sink container silently clears its own alarm on the very
// next tick — the #730 self-deleting-issue bug, rebuilt from different parts.
func TestBatchSinkPresenceUnknownDoesNotClearTheIssue(t *testing.T) {
	s, mock, done := newBatchSentinel(t)
	defer done()

	mock.ExpectQuery(`FROM pipelines p`).
		WillReturnRows(batchSinkRows("p1", "nightly orders", "e1", "sink-abcd1234-batch"))
	mock.ExpectQuery(`SELECT id FROM sentinel_active_issues`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(batchSinkAbsentIssueID("p1")))
	// The forbidden statement — see the CDC test above for why it is expected rather
	// than omitted.
	mock.ExpectExec(`DELETE FROM sentinel_active_issues`).
		WithArgs(batchSinkAbsentIssueID("p1")).
		WillReturnResult(sqlmock.NewResult(0, 1))

	s.probeBatchSinks(context.Background(), staticSinkProbe(nil, context.DeadlineExceeded))

	if err := mock.ExpectationsWereMet(); err == nil {
		t.Fatal("an unreachable sink container cleared its own sink-absent issue. " +
			"A probe that answered nothing must keep the issue open, not drop out of the " +
			"still-open set and let resolveStaleIssues delete it.")
	}
}

// The other direction: a worker that is genuinely back must clear the issue, or the
// health surface shows a problem that no longer exists.
func TestBatchSinkPresencePresentClearsTheIssue(t *testing.T) {
	s, mock, done := newBatchSentinel(t)
	defer done()

	mock.ExpectQuery(`FROM pipelines p`).
		WillReturnRows(batchSinkRows("p1", "nightly orders", "e1", "sink-abcd1234-batch"))
	mock.ExpectQuery(`SELECT id FROM sentinel_active_issues`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(batchSinkAbsentIssueID("p1")))
	mock.ExpectExec(`DELETE FROM sentinel_active_issues`).
		WithArgs(batchSinkAbsentIssueID("p1")).
		WillReturnResult(sqlmock.NewResult(0, 1))

	s.probeBatchSinks(context.Background(), staticSinkProbe(sinkStatusResp("running"), nil))

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("a recovered batch sink did not clear its absence issue: %v", err)
	}
}

// The issue id must carry the prefix resolveStaleIssues scopes on, and must not
// collide with the two existing batch classes. #730 was exactly this: an id formed
// at two call sites, drifting, and the resolver deleting every issue the detector
// had just raised.
func TestBatchSinkAbsentIDIsItsOwnClass(t *testing.T) {
	id := batchSinkAbsentIssueID("p1")
	if !strings.HasPrefix(id, batchSinkAbsentPrefix) {
		t.Fatalf("id %q does not carry prefix %q — resolveStaleIssues scopes by "+
			"`id LIKE prefix || '%%'` and would never see it", id, batchSinkAbsentPrefix)
	}
	for _, other := range []string{batchStallIssuePrefix, batchAckIssuePrefix} {
		if strings.HasPrefix(batchSinkAbsentPrefix, other) || strings.HasPrefix(other, batchSinkAbsentPrefix) {
			t.Errorf("prefix %q overlaps %q — one class's resolver would delete the other's issues",
				batchSinkAbsentPrefix, other)
		}
	}
}

// A sentinel with no MCP manager must be inert, not a nil-pointer panic:
// main.go starts the sentinel before the ServerManager exists, and every unit
// context leaves it nil.
func TestBatchSinkPresenceTickIsInertWithoutAnMCPManager(t *testing.T) {
	s, _, done := newBatchSentinel(t)
	defer done()

	// No query expectations: the tick must return before touching the database.
	s.sinkPresenceTick(context.Background())

	noDB := &BatchSentinel{stallThreshold: DefaultBatchStallThreshold}
	noDB.sinkPresenceTick(context.Background())
}

// TestBatchSinkQueryScopesTheManifestToTheRunItProbes — the startup race that made
// every zero-row batch run page a human (KI-BATCHSENTINEL-SINK-ABSENT-FALSE-POSITIVE).
//
// pipeline_dependencies accumulates one row per execution forever: upsertDependency
// conflicts on (pipeline_id, execution_id, kind, identifier), so a new row is minted
// each run and nothing ever deletes the old ones. The manifest LATERAL selected the
// newest row for the PIPELINE and never constrained it to the execution the outer
// query had already selected as `e.id` — so the probe paired THIS run's execution id
// with a PREVIOUS run's consumer group.
//
// The window is not a blip. api-gateway INSERTs the execution row with status
// 'running' at request time (handlers/pipelines.go:3009-3013), which satisfies both
// gating predicates of this query at t=0, while the sink is not registered until the
// end of Temporal stages 1-6 plus infra_preflight's container-health polling. Every
// tick in between probes a consumer group belonging to a finished run, the container
// correctly answers not_found, and the sentinel raises a CRITICAL "nothing is writing
// to the destination" alarm against a perfectly healthy run.
//
// Proven on prod 2026-08-18, pipeline 72efcfa0: alert at 10:25:36.241593 naming
// `sink-72efcfa0-batch` (the 2026-08-14 run's group, minted before the `rsync.`
// namespace prefix existed), 6.8s BEFORE this run registered `rsync.sink-72efcfa0-batch`
// at 10:25:43.016567.
func TestBatchSinkQueryScopesTheManifestToTheRunItProbes(t *testing.T) {
	q := runningBatchSinksQuery

	if !strings.Contains(q, "dep.execution_id = e.id") {
		t.Error("the manifest join does not constrain dep.execution_id to the execution the " +
			"outer query selected (e.id). The newest row for the PIPELINE wins, so during the " +
			"window between run-start and sink registration the probe asks the container about " +
			"a PREVIOUS run's consumer group, gets not_found, and raises a critical false alarm " +
			"on a healthy run.")
	}
}

// The fix must remove ONLY the provably-wrong case. upsertDependency writes a NULL
// execution_id when it is called with an empty execution id (dependency_manifest.go:31-34),
// and those rows are probed today; dropping them would be a silent loss of coverage
// rather than a fix. Scope to "this execution, or unscoped" — never to "this execution"
// alone.
func TestBatchSinkQueryStillProbesUnscopedManifestRows(t *testing.T) {
	q := runningBatchSinksQuery

	if !strings.Contains(q, "dep.execution_id IS NULL") {
		t.Error("execution-scoping dropped the NULL-execution manifest rows. upsertDependency " +
			"writes execution_id=NULL when executionID is empty, and those rows are the only " +
			"registration such a pipeline has — scoping them out disarms the probe entirely " +
			"instead of narrowing it.")
	}
}
