package workflows

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// Failed-run recording tests.
//
// The bug: a model run whose activity spent every retry without a result left no run
// history row and no badge change, because the gateway records inside the run endpoint
// and that endpoint never answered. The schedule page kept showing the last success.
//
// What is guarded here is the workflow half: the recording activity is called when —
// and only when — the run activity fails terminally, with a payload the gateway can
// dedupe; it is NOT called when replaying a history written before the change; and it
// never changes what the workflow itself decides. Every activity is a probe registered
// under the real name, so nothing here reaches a gateway.

// modelRunFailureProbe stands in for RecordModelRunFailureActivity.
type modelRunFailureProbe struct {
	mu    sync.Mutex
	calls []ModelRunFailureInput
	err   error
}

func (p *modelRunFailureProbe) record(_ context.Context, in ModelRunFailureInput) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, in)
	return p.err
}

func (p *modelRunFailureProbe) recorded() []ModelRunFailureInput {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]ModelRunFailureInput(nil), p.calls...)
}

var rfaStart = time.Date(2026, 9, 16, 10, 0, 0, 0, time.UTC)

func newScheduledModelRunFailureEnv(run func(context.Context, ScheduledModelRunInput) (modelRunActivityResult, error), rec *modelRunFailureProbe) *testsuite.TestWorkflowEnvironment {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetStartTime(rfaStart)
	env.RegisterWorkflow(ScheduledModelRunWorkflow)
	env.RegisterActivityWithOptions(run, activity.RegisterOptions{Name: "RunModelActivity"})
	env.RegisterActivityWithOptions(rec.record, activity.RegisterOptions{Name: "RecordModelRunFailureActivity"})
	return env
}

var rfaClockRun = ScheduledModelRunInput{ScheduleID: "sched-clock-1", SavedQueryID: "model-clock-1"}

func TestScheduledModelRun_ARunThatNeverReportedIsRecordedAsFailed(t *testing.T) {
	run := &modelRefreshProbe{err: errors.New("model run endpoint returned http 502: upstream unavailable")}
	rec := &modelRunFailureProbe{}
	env := newScheduledModelRunFailureEnv(run.run, rec)

	env.ExecuteWorkflow(ScheduledModelRunWorkflow, rfaClockRun)

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow never completed")
	}
	// The workflow's own outcome is unchanged: it still fails with the run's error.
	werr := env.GetWorkflowError()
	if werr == nil || !strings.Contains(werr.Error(), "model run failed") {
		t.Fatalf("workflow error = %v, want the run's own failure", werr)
	}
	if got := len(run.recorded()); got != 3 {
		t.Fatalf("run activity attempted %d times, want 3 — the record must follow the spent budget, not the first attempt", got)
	}

	calls := rec.recorded()
	if len(calls) != 1 {
		t.Fatalf("recording activity called %d times, want 1: %+v", len(calls), calls)
	}
	got := calls[0]
	if got.SavedQueryID != "model-clock-1" || got.ScheduleID != "sched-clock-1" {
		t.Errorf("recorded against model %q schedule %q, want model-clock-1 / sched-clock-1", got.SavedQueryID, got.ScheduleID)
	}
	if got.Trigger != "" {
		t.Errorf("clock run recorded with trigger %q, want empty (the clock door)", got.Trigger)
	}
	if got.UpstreamKind != "" || got.UpstreamID != "" || got.ExecutionID != "" || got.Depth != 0 || got.Coalesced != 0 {
		t.Errorf("clock run recorded with provenance %+v, want none", got)
	}
	if !got.StartedAt.Equal(rfaStart) {
		t.Errorf("started_at = %s, want the workflow time the run was scheduled (%s)", got.StartedAt, rfaStart)
	}
	for _, want := range []string{
		"did not return a result after several attempts",
		"It will run again at the next scheduled time.",
		"Details: model run endpoint returned http 502: upstream unavailable",
	} {
		if !strings.Contains(got.Error, want) {
			t.Errorf("recorded message %q does not contain %q", got.Error, want)
		}
	}
	for _, noise := range []string{"retryable:", "scheduledEventID"} {
		if strings.Contains(got.Error, noise) {
			t.Errorf("recorded message carries SDK wrapper text %q: %q", noise, got.Error)
		}
	}
}

func TestScheduledModelRun_AHistoryFromBeforeTheChangeRecordsNothing(t *testing.T) {
	run := &modelRefreshProbe{err: errors.New("model run endpoint returned http 502: upstream unavailable")}
	rec := &modelRunFailureProbe{}
	env := newScheduledModelRunFailureEnv(run.run, rec)
	env.OnGetVersion(recordModelRunFailureChangeID, workflow.DefaultVersion, 1).Return(workflow.DefaultVersion)

	env.ExecuteWorkflow(ScheduledModelRunWorkflow, rfaClockRun)

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow never completed")
	}
	if werr := env.GetWorkflowError(); werr == nil || !strings.Contains(werr.Error(), "model run failed") {
		t.Fatalf("workflow error = %v, want the run's own failure", werr)
	}
	// Denominator: the failure path was actually reached.
	if got := len(run.recorded()); got != 3 {
		t.Fatalf("run activity attempted %d times, want 3", got)
	}
	if calls := rec.recorded(); len(calls) != 0 {
		t.Fatalf("a DefaultVersion replay scheduled the recording activity %d times — that is a command the old history does not contain", len(calls))
	}
}

func TestScheduledModelRun_ASuccessfulRunRecordsNothing(t *testing.T) {
	run := &modelRefreshProbe{}
	rec := &modelRunFailureProbe{}
	env := newScheduledModelRunFailureEnv(run.run, rec)

	env.ExecuteWorkflow(ScheduledModelRunWorkflow, rfaClockRun)

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow never completed")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error = %v, want nil", err)
	}
	if got := len(run.recorded()); got != 1 {
		t.Fatalf("run activity called %d times, want 1", got)
	}
	if calls := rec.recorded(); len(calls) != 0 {
		t.Fatalf("a successful run called the recording activity: %+v", calls)
	}
}

// The gateway answers 200 with status "failed" for SQL the engine rejected, and has
// already written that row. Recording it again from here would show the failure twice.
func TestScheduledModelRun_AFailureTheGatewayReportedIsNotRecordedAgain(t *testing.T) {
	var runs atomic.Int32
	run := func(context.Context, ScheduledModelRunInput) (modelRunActivityResult, error) {
		runs.Add(1)
		return modelRunActivityResult{Status: "failed", Error: "relation does not exist"}, nil
	}
	rec := &modelRunFailureProbe{}
	env := newScheduledModelRunFailureEnv(run, rec)

	env.ExecuteWorkflow(ScheduledModelRunWorkflow, rfaClockRun)

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow never completed")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error = %v, want nil", err)
	}
	if runs.Load() != 1 {
		t.Fatalf("run activity called %d times, want 1", runs.Load())
	}
	if calls := rec.recorded(); len(calls) != 0 {
		t.Fatalf("a failure the gateway already recorded was recorded again: %+v", calls)
	}
}

func TestScheduledModelRun_ARecordThatCannotBeWrittenLeavesTheOutcomeAlone(t *testing.T) {
	run := &modelRefreshProbe{err: errors.New("model run request failed: connection refused")}
	rec := &modelRunFailureProbe{err: errors.New("failed-run record request failed: connection refused")}
	env := newScheduledModelRunFailureEnv(run.run, rec)

	env.ExecuteWorkflow(ScheduledModelRunWorkflow, rfaClockRun)

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow never completed")
	}
	werr := env.GetWorkflowError()
	if werr == nil || !strings.Contains(werr.Error(), "model run request failed") {
		t.Fatalf("workflow error = %v, want the run's own failure", werr)
	}
	if strings.Contains(werr.Error(), "failed-run record") {
		t.Fatalf("the record's failure leaked into the workflow outcome: %v", werr)
	}
	if got := len(rec.recorded()); got != 3 {
		t.Fatalf("recording activity attempted %d times, want exactly 3 (short and bounded)", got)
	}
}

func TestScheduledModelRun_ACancelledRunIsNotRecordedAsFailed(t *testing.T) {
	var runs atomic.Int32
	run := func(context.Context, ScheduledModelRunInput) (modelRunActivityResult, error) {
		runs.Add(1)
		return modelRunActivityResult{}, temporal.NewCanceledError("stopped")
	}
	rec := &modelRunFailureProbe{}
	env := newScheduledModelRunFailureEnv(run, rec)

	env.ExecuteWorkflow(ScheduledModelRunWorkflow, rfaClockRun)

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow never completed")
	}
	if werr := env.GetWorkflowError(); werr == nil {
		t.Fatal("workflow error = nil, want the cancellation to surface")
	}
	if runs.Load() == 0 {
		t.Fatal("run activity never ran")
	}
	if calls := rec.recorded(); len(calls) != 0 {
		t.Fatalf("a cancelled run was recorded as failed: %+v", calls)
	}
}

func newModelRefreshFailureEnv(run *modelRefreshProbe, rec *modelRunFailureProbe) *testsuite.TestWorkflowEnvironment {
	env := newModelRefreshEnv(run)
	env.SetStartTime(rfaStart)
	env.RegisterActivityWithOptions(rec.record, activity.RegisterOptions{Name: "RecordModelRunFailureActivity"})
	return env
}

func TestModelRefresh_ARebuildThatNeverReportedIsRecordedAndTheLoopCarriesOn(t *testing.T) {
	run := &modelRefreshProbe{err: errors.New("model run endpoint returned http 503: gateway restarting")}
	rec := &modelRunFailureProbe{}
	env := newModelRefreshFailureEnv(run, rec)

	signalAt(env, time.Millisecond, ModelRefreshRequest{
		ScheduleID: "sched-event-1", UpstreamKind: "pipeline", UpstreamID: "pipe-1", UpstreamRunID: "run-1", ExecutionID: "exec-1", Depth: 2,
	})
	env.ExecuteWorkflow(ModelRefreshWorkflow, ModelRefreshInput{SavedQueryID: "model-event-1"})

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow never completed")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("a spent retry budget failed the loop: %v — it must not, record or no record", err)
	}
	if got := len(run.recorded()); got != 3 {
		t.Fatalf("run activity attempted %d times, want 3", got)
	}
	calls := rec.recorded()
	if len(calls) != 1 {
		t.Fatalf("recording activity called %d times, want 1: %+v", len(calls), calls)
	}
	got := calls[0]
	if got.SavedQueryID != "model-event-1" || got.ScheduleID != "sched-event-1" {
		t.Errorf("recorded against model %q schedule %q, want model-event-1 / sched-event-1", got.SavedQueryID, got.ScheduleID)
	}
	if got.Trigger != ModelRefreshTrigger {
		t.Errorf("event rebuild recorded with trigger %q, want %q (the event door)", got.Trigger, ModelRefreshTrigger)
	}
	if want := rfaStart.Add(time.Millisecond); !got.StartedAt.Equal(want) {
		t.Errorf("started_at = %s, want the workflow time the rebuild was scheduled (%s)", got.StartedAt, want)
	}
	// The failed row names the upstream that woke the rebuild, exactly as the run input did.
	runs := run.recorded()
	if got.Depth != runs[0].Depth || got.UpstreamKind != runs[0].UpstreamKind || got.UpstreamID != runs[0].UpstreamID ||
		got.UpstreamRunID != runs[0].UpstreamRunID || got.ExecutionID != runs[0].ExecutionID || got.Coalesced != runs[0].Coalesced {
		t.Errorf("recorded provenance %+v differs from the run's own input %+v", got, runs[0])
	}
	if got.Depth != 2 || got.UpstreamKind != "pipeline" || got.UpstreamID != "pipe-1" || got.UpstreamRunID != "run-1" ||
		got.ExecutionID != "exec-1" || got.Coalesced != 1 {
		t.Errorf("recorded provenance = depth %d kind %q id %q run %q exec %q coalesced %d, want 2 pipeline pipe-1 run-1 exec-1 1",
			got.Depth, got.UpstreamKind, got.UpstreamID, got.UpstreamRunID, got.ExecutionID, got.Coalesced)
	}
	for _, want := range []string{
		"It will run again the next time something this model waits on finishes.",
		"Details: model run endpoint returned http 503: gateway restarting",
	} {
		if !strings.Contains(got.Error, want) {
			t.Errorf("recorded message %q does not contain %q", got.Error, want)
		}
	}
}

func TestModelRefresh_AHistoryFromBeforeTheChangeRecordsNothing(t *testing.T) {
	run := &modelRefreshProbe{err: errors.New("model run endpoint returned http 503: gateway restarting")}
	rec := &modelRunFailureProbe{}
	env := newModelRefreshFailureEnv(run, rec)
	env.OnGetVersion(recordModelRunFailureChangeID, workflow.DefaultVersion, 1).Return(workflow.DefaultVersion)

	signalAt(env, time.Millisecond, ModelRefreshRequest{ScheduleID: "sched-event-1", UpstreamKind: "pipeline", UpstreamID: "pipe-1"})
	env.ExecuteWorkflow(ModelRefreshWorkflow, ModelRefreshInput{SavedQueryID: "model-event-1"})

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow never completed")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error = %v, want nil", err)
	}
	if got := len(run.recorded()); got != 3 {
		t.Fatalf("run activity attempted %d times, want 3", got)
	}
	if calls := rec.recorded(); len(calls) != 0 {
		t.Fatalf("a DefaultVersion replay scheduled the recording activity %d times", len(calls))
	}
}

func TestModelRefresh_ASuccessfulRebuildRecordsNothing(t *testing.T) {
	run := &modelRefreshProbe{}
	rec := &modelRunFailureProbe{}
	env := newModelRefreshFailureEnv(run, rec)

	signalAt(env, time.Millisecond, ModelRefreshRequest{ScheduleID: "sched-event-1", UpstreamKind: "pipeline", UpstreamID: "pipe-1"})
	env.ExecuteWorkflow(ModelRefreshWorkflow, ModelRefreshInput{SavedQueryID: "model-event-1"})

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow never completed")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error = %v, want nil", err)
	}
	if got := len(run.recorded()); got != 1 {
		t.Fatalf("run activity called %d times, want 1", got)
	}
	if calls := rec.recorded(); len(calls) != 0 {
		t.Fatalf("a successful rebuild called the recording activity: %+v", calls)
	}
}

func TestDescribeModelRunFailure_ATimeoutSaysSoInPlainWords(t *testing.T) {
	err := fmt.Errorf("activity error: %w", temporal.NewTimeoutError(enumspb.TIMEOUT_TYPE_START_TO_CLOSE, nil))
	msg := describeModelRunFailure(err, "")
	if !strings.Contains(msg, "did not finish within its time limit") {
		t.Errorf("timeout described as %q, want it to say the time limit was reached", msg)
	}
	if !strings.Contains(msg, "It will run again at the next scheduled time.") {
		t.Errorf("timeout message %q does not say what happens next", msg)
	}
	// Control: a non-timeout error is not described as one.
	if other := describeModelRunFailure(errors.New("connection refused"), ""); strings.Contains(other, "time limit") {
		t.Errorf("a non-timeout error was described as a timeout: %q", other)
	}
}

func TestDescribeModelRunFailure_BoundsTheDetailOnARuneBoundary(t *testing.T) {
	long := strings.Repeat("é", modelRunFailureDetailMaxRunes+50)
	msg := describeModelRunFailure(errors.New(long), "")
	if !strings.HasSuffix(msg, "…") {
		t.Fatalf("an over-long detail was not bounded: %d bytes", len(msg))
	}
	detail := msg[strings.Index(msg, "Details: ")+len("Details: "):]
	if n := len([]rune(strings.TrimSuffix(detail, "…"))); n != modelRunFailureDetailMaxRunes {
		t.Errorf("detail kept %d runes, want %d", n, modelRunFailureDetailMaxRunes)
	}
	if strings.ContainsRune(msg, '�') {
		t.Error("the bound split a character")
	}
}

// The gateway keeps at most 1000 runes of the message (modelRunFailureErrorMaxRunes in
// api-gateway's saved_query_run_failure.go). A longest-possible message must fit, or the
// gateway's cut would fall on the sentences that say what happens next.
func TestDescribeModelRunFailure_TheLongestMessageFitsWhatTheGatewayKeeps(t *testing.T) {
	const gatewayKeeps = 1000
	long := errors.New(strings.Repeat("x", modelRunFailureDetailMaxRunes*3))
	for _, trigger := range []string{"", ModelRefreshTrigger} {
		msg := describeModelRunFailure(long, trigger)
		if n := len([]rune(msg)); n > gatewayKeeps {
			t.Errorf("trigger %q: longest message is %d runes, the gateway keeps %d", trigger, n, gatewayKeeps)
		}
		// Control: the detail really was at its bound, so this measured the longest case.
		if !strings.HasSuffix(msg, "…") {
			t.Errorf("trigger %q: detail was not cut, so this is not the longest message", trigger)
		}
	}
}

func TestDescribeModelRunFailure_ABlankApplicationMessageFallsBackToTheWholeError(t *testing.T) {
	blank := fmt.Errorf("model run endpoint returned http 502: %w", temporal.NewApplicationError("", "GatewayUnavailable"))
	msg := describeModelRunFailure(blank, "")
	if !strings.Contains(msg, "Details: model run endpoint returned http 502") {
		t.Errorf("an application error with a blank message lost the detail around it: %q", msg)
	}
	// Control: a non-blank innermost message is used on its own, without the wrapper.
	named := fmt.Errorf("activity error (scheduledEventID 5): %w", temporal.NewApplicationError("gateway restarting", "GatewayUnavailable"))
	ctl := describeModelRunFailure(named, "")
	if !strings.Contains(ctl, "Details: gateway restarting") || strings.Contains(ctl, "scheduledEventID") {
		t.Errorf("a non-blank application message was not used on its own: %q", ctl)
	}
}

// ---------------------------------------------------------------------------
// RecordModelRunFailureActivity against a stand-in gateway
// ---------------------------------------------------------------------------

// With API_GATEWAY_URL unset, the record must go where the run itself went. A record
// that defaults to any other address is lost exactly when the run reached the gateway.
func TestModelGatewayActivities_DefaultToTheSameGatewayAddress(t *testing.T) {
	t.Setenv("API_GATEWAY_URL", "")
	t.Setenv("INTERNAL_SERVICE_SECRET", rfaSecret)
	// Already cancelled: the HTTP client gives up before dialing, and its error names
	// the URL it would have called. Nothing leaves the process.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	const base = "http://api-gateway:8080/api/v1/internal/explorer/models/model-9/"

	_, runErr := RunModelActivity(ctx, ScheduledModelRunInput{SavedQueryID: "model-9", ScheduleID: "sched-9"})
	if runErr == nil || !strings.Contains(runErr.Error(), `"`+base+`run"`) {
		t.Errorf("run activity error = %v, want a call to %srun", runErr, base)
	}
	recErr := RecordModelRunFailureActivity(ctx, ModelRunFailureInput{SavedQueryID: "model-9", ScheduleID: "sched-9", Error: "x", StartedAt: rfaStartedAt})
	if recErr == nil || !strings.Contains(recErr.Error(), `"`+base+`run-failed"`) {
		t.Errorf("record activity error = %v, want a call to %srun-failed", recErr, base)
	}
}

const rfaSecret = "rf-adapter-unit-test-secret"

type rfaGateway struct {
	hits   atomic.Int32
	status int
	// reply is the response body; empty means a generic JSON body.
	reply  string
	mu     sync.Mutex
	method string
	path   string
	secret string
	ctype  string
	body   map[string]any
}

func newRFAGateway(t *testing.T, status int) *rfaGateway {
	t.Helper()
	return newRFAGatewayReplying(t, status, "")
}

func newRFAGatewayReplying(t *testing.T, status int, reply string) *rfaGateway {
	t.Helper()
	g := &rfaGateway{status: status, reply: reply}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		g.hits.Add(1)
		raw, _ := io.ReadAll(r.Body)
		g.mu.Lock()
		g.method, g.path = r.Method, r.URL.Path
		g.secret, g.ctype = r.Header.Get("X-Internal-Secret"), r.Header.Get("Content-Type")
		g.body = nil
		_ = json.Unmarshal(raw, &g.body)
		g.mu.Unlock()
		reply := g.reply
		if reply == "" {
			reply = `{"status":"stand-in"}`
		}
		w.WriteHeader(g.status)
		_, _ = w.Write([]byte(reply))
	}))
	t.Cleanup(srv.Close)
	t.Setenv("API_GATEWAY_URL", srv.URL)
	t.Setenv("INTERNAL_SERVICE_SECRET", rfaSecret)
	return g
}

var rfaStartedAt = time.Date(2026, 9, 16, 10, 0, 0, 123456789, time.UTC)

func TestRecordModelRunFailureActivity_PostsTheRecordToTheGateway(t *testing.T) {
	g := newRFAGateway(t, http.StatusOK)
	err := RecordModelRunFailureActivity(context.Background(), ModelRunFailureInput{
		SavedQueryID: "model-9", ScheduleID: "sched-9", Error: "The model rebuild could not be completed.", StartedAt: rfaStartedAt,
	})
	if err != nil {
		t.Fatalf("activity error = %v, want nil on 200", err)
	}
	if g.hits.Load() != 1 {
		t.Fatalf("gateway hit %d times, want 1", g.hits.Load())
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.method != http.MethodPost || g.path != "/api/v1/internal/explorer/models/model-9/run-failed" {
		t.Errorf("called %s %s, want POST /api/v1/internal/explorer/models/model-9/run-failed", g.method, g.path)
	}
	if g.secret != rfaSecret {
		t.Error("the internal secret header was not sent")
	}
	if g.ctype != "application/json" {
		t.Errorf("content type = %q, want application/json", g.ctype)
	}
	if g.body["schedule_id"] != "sched-9" || g.body["error"] != "The model rebuild could not be completed." {
		t.Errorf("body = %v, want schedule_id and error carried verbatim", g.body)
	}
	for _, k := range []string{"trigger", "depth", "upstream_kind", "upstream_id", "upstream_run_id", "execution_id", "coalesced"} {
		if _, has := g.body[k]; has {
			t.Errorf("clock-path body carries %q: %v", k, g.body)
		}
	}
	s, _ := g.body["started_at"].(string)
	parsed, perr := time.Parse(time.RFC3339Nano, s)
	if perr != nil || !parsed.Equal(rfaStartedAt) {
		t.Errorf("started_at = %q, want %s to the nanosecond", s, rfaStartedAt.Format(time.RFC3339Nano))
	}
}

func TestRecordModelRunFailureActivity_SendsTheEventDoorTrigger(t *testing.T) {
	g := newRFAGateway(t, http.StatusOK)
	if err := RecordModelRunFailureActivity(context.Background(), ModelRunFailureInput{
		SavedQueryID: "model-9", ScheduleID: "sched-9", Trigger: ModelRefreshTrigger, Error: "x", StartedAt: rfaStartedAt,
		Depth: 2, UpstreamKind: "pipeline", UpstreamID: "pipe-9", UpstreamRunID: "run-9", ExecutionID: "exec-9", Coalesced: 3,
	}); err != nil {
		t.Fatalf("activity error = %v", err)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.body["trigger"] != ModelRefreshTrigger {
		t.Errorf("trigger = %v, want %q", g.body["trigger"], ModelRefreshTrigger)
	}
	// The same keys, and the same JSON number types, the gateway decodes on the run endpoint.
	want := map[string]any{
		"depth": float64(2), "upstream_kind": "pipeline", "upstream_id": "pipe-9",
		"upstream_run_id": "run-9", "execution_id": "exec-9", "coalesced": float64(3),
	}
	for k, v := range want {
		if g.body[k] != v {
			t.Errorf("body[%q] = %v, want %v", k, g.body[k], v)
		}
	}
}

func TestRecordModelRunFailureActivity_ClassifiesTheGatewaysAnswer(t *testing.T) {
	cases := []struct {
		name         string
		status       int
		reply        string
		wantErr      bool
		nonRetryable bool
	}{
		{"ok", http.StatusOK, "", false, false},
		// The handler's own answer for a model or schedule deleted after the run
		// started (saved_query_schedules.go): nothing to attach to, done.
		{"handler not_found, model gone", http.StatusNotFound, `{"status":"not_found","reason":"model no longer exists"}`, false, false},
		{"handler not_found, schedule gone", http.StatusNotFound, `{"status":"not_found","reason":"no such schedule for this model"}`, false, false},
		// A 404 the handler did not write — a router with no such route answers in plain
		// text — recorded nothing and must never read as recorded.
		{"router 404, plain text", http.StatusNotFound, "404 page not found", true, false},
		{"404 with some other JSON", http.StatusNotFound, `{"error":"route not found"}`, true, false},
		{"gone", http.StatusGone, "", true, true},
		{"bad request", http.StatusBadRequest, "", true, true},
		{"unauthorized", http.StatusUnauthorized, "", true, true},
		{"request timeout", http.StatusRequestTimeout, "", true, false},
		{"too many requests", http.StatusTooManyRequests, "", true, false},
		{"internal server error", http.StatusInternalServerError, "", true, false},
		{"service unavailable", http.StatusServiceUnavailable, "", true, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g := newRFAGatewayReplying(t, tc.status, tc.reply)
			err := RecordModelRunFailureActivity(context.Background(), ModelRunFailureInput{
				SavedQueryID: "model-9", ScheduleID: "sched-9", Error: "x", StartedAt: rfaStartedAt,
			})
			if g.hits.Load() != 1 {
				t.Fatalf("gateway hit %d times, want 1", g.hits.Load())
			}
			if (err != nil) != tc.wantErr {
				t.Fatalf("http %d: error = %v, want error %v", tc.status, err, tc.wantErr)
			}
			if err == nil {
				return
			}
			var appErr *temporal.ApplicationError
			isNonRetryable := errors.As(err, &appErr) && appErr.NonRetryable()
			if isNonRetryable != tc.nonRetryable {
				t.Errorf("http %d: non-retryable = %v, want %v (%v)", tc.status, isNonRetryable, tc.nonRetryable, err)
			}
		})
	}
}

func TestRecordModelRunFailureActivity_WithoutTheSecretCallsNothing(t *testing.T) {
	g := newRFAGateway(t, http.StatusOK)
	t.Setenv("INTERNAL_SERVICE_SECRET", "  ")

	err := RecordModelRunFailureActivity(context.Background(), ModelRunFailureInput{SavedQueryID: "model-9", ScheduleID: "sched-9", Error: "x", StartedAt: rfaStartedAt})
	var appErr *temporal.ApplicationError
	if err == nil || !errors.As(err, &appErr) || !appErr.NonRetryable() {
		t.Fatalf("error = %v, want a non-retryable configuration error", err)
	}
	if g.hits.Load() != 0 {
		t.Fatalf("gateway hit %d times without a secret, want 0", g.hits.Load())
	}

	// Control: the same stand-in is reachable once the secret is present.
	t.Setenv("INTERNAL_SERVICE_SECRET", rfaSecret)
	if err := RecordModelRunFailureActivity(context.Background(), ModelRunFailureInput{SavedQueryID: "model-9", ScheduleID: "sched-9", Error: "x", StartedAt: rfaStartedAt}); err != nil {
		t.Fatalf("control call failed: %v", err)
	}
	if g.hits.Load() != 1 {
		t.Fatalf("control: gateway hit %d times, want 1", g.hits.Load())
	}
}

// ---------------------------------------------------------------------------
// Replay against histories written before the change
// ---------------------------------------------------------------------------
// OnGetVersion above proves what the code does WHEN GetVersion answers DefaultVersion.
// These prove that it does answer that for a real pre-change history, and that the new
// failure path then emits no command the history lacks.
//
// The two fixtures are CONSTRUCTED, not captured: no stack at hand has a model run that
// failed terminally on the old code. They follow the event layout of the captured
// model_freshness_pre_query_history.json (same SDK version, task queue, id scheme — an
// activity or timer id is the event id of its scheduled event, which the command matcher
// compares). Each ends on the workflow's closing event, not on WorkflowTaskStarted: a
// history that stops mid-task would make the failure path the live task, where GetVersion
// answers the new version and nothing would be tested. The proof that they are not
// vacuous is the mutation run — removing the GetVersion gate fails both with a
// nondeterminism error.

func TestScheduledModelRun_ReplaysAFailedRunFromBeforeTheChange(t *testing.T) {
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflow(ScheduledModelRunWorkflow)
	if err := replayer.ReplayWorkflowHistoryFromJSONFile(nil, "testdata/scheduled_model_run_failed_pre_record_history.json"); err != nil {
		t.Fatalf("a model run that failed before this change does not replay: %v", err)
	}
}

func TestModelRefresh_ReplaysAFailedRebuildFromBeforeTheChange(t *testing.T) {
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflow(ModelRefreshWorkflow)
	if err := replayer.ReplayWorkflowHistoryFromJSONFile(nil, "testdata/model_refresh_failed_pre_record_history.json"); err != nil {
		t.Fatalf("a model refresh loop whose rebuild failed before this change does not replay: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Replay against histories written WITH the change
// ---------------------------------------------------------------------------
// The other direction: a run that failed and was recorded on this code must keep
// replaying on every later build, so the change id and the recording path stay fixed.
// These two fixtures are CAPTURED, not constructed: each workflow ran against a
// throwaway local Temporal 1.22.4 server, with the run endpoint answering 502 and the
// failed-run endpoint answering 200, and the history was read back from the server.
// They were captured before the record carried an event run's provenance. The replayer
// matches commands (activity type and id), not activity inputs, so that difference is
// invisible to it, and what these pin is the command sequence: the version marker, then
// the recording activity. The fixture check below is the denominator: a fixture without
// the version marker and the recording activity would replay without testing anything.
// Mutation evidence: dropping the recording activity (gate kept) fails both.

func requireRecordedFailureHistory(t *testing.T, path string) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()
	hist, err := client.HistoryFromJSON(f, client.HistoryJSONOptions{})
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	marker, scheduled := false, false
	for _, ev := range hist.GetEvents() {
		if m := ev.GetMarkerRecordedEventAttributes(); m != nil && m.GetMarkerName() == "Version" {
			var id string
			if p := m.GetDetails()["change-id"].GetPayloads(); len(p) == 1 && json.Unmarshal(p[0].GetData(), &id) == nil && id == "record-model-run-failure" {
				marker = true
			}
		}
		if a := ev.GetActivityTaskScheduledEventAttributes(); a != nil && a.GetActivityType().GetName() == "RecordModelRunFailureActivity" {
			scheduled = marker
		}
	}
	if !marker || !scheduled {
		t.Fatalf("%s is not a recorded-failure history: version marker %v, recording activity after it %v", path, marker, scheduled)
	}
}

func TestScheduledModelRun_ReplaysAFailedRunRecordedByThisChange(t *testing.T) {
	const path = "testdata/scheduled_model_run_failed_recorded_history.json"
	requireRecordedFailureHistory(t, path)
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflow(ScheduledModelRunWorkflow)
	if err := replayer.ReplayWorkflowHistoryFromJSONFile(nil, path); err != nil {
		t.Fatalf("a model run whose failure was recorded does not replay: %v", err)
	}
}

func TestModelRefresh_ReplaysAFailedRebuildRecordedByThisChange(t *testing.T) {
	const path = "testdata/model_refresh_failed_recorded_history.json"
	requireRecordedFailureHistory(t, path)
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflow(ModelRefreshWorkflow)
	if err := replayer.ReplayWorkflowHistoryFromJSONFile(nil, path); err != nil {
		t.Fatalf("a model refresh loop whose failed rebuild was recorded does not replay: %v", err)
	}
}
