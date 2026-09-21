package handlers

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"go.temporal.io/api/serviceerror"
)

// ============================================================================
// The two doors of the internal model-run endpoint
// ============================================================================
// These tests exist because the failure mode is silent in both directions. The clock
// door and the event door partition schedule_type between them, and neither an overlap
// nor a gap produces an error anywhere: an overlap rebuilds a model twice per pipeline
// completion and writes two successful-looking rows, a gap leaves a schedule that never
// fires and reports "skipped" forever. Nothing at runtime notices either one.

func TestModelRunScheduleLookup_DoorsAreExactComplements(t *testing.T) {
	clock, _ := modelRunScheduleLookup(false)
	event, _ := modelRunScheduleLookup(true)

	if !strings.Contains(clock, "s.schedule_type != $3") {
		t.Errorf("clock door does not exclude the event schedule_type:\n%s", clock)
	}
	if !strings.Contains(event, "s.schedule_type = $3") {
		t.Errorf("event door does not select the event schedule_type:\n%s", event)
	}
	// Nothing but the operator may differ in the predicate, or the two doors are no
	// longer complements over the same column.
	if strings.Contains(event, "s.schedule_type != $3") {
		t.Errorf("event door still carries the clock door's operator:\n%s", event)
	}
	if strings.Contains(clock, "s.schedule_type = $3") {
		t.Errorf("clock door still carries the event door's operator:\n%s", clock)
	}
}

func TestModelRunScheduleLookup_ScheduleTypeIsBoundNotInterpolated(t *testing.T) {
	// The value travels as $3. A literal in the SQL would be a second place the
	// schedule_type is spelled, and the one the endpoint's own guard cannot see.
	for _, tc := range []struct {
		name  string
		event bool
	}{{"clock", false}, {"event", true}} {
		q, _ := modelRunScheduleLookup(tc.event)
		if strings.Contains(q, scheduleAfterUpstream) {
			t.Errorf("%s door interpolates the schedule type literal:\n%s", tc.name, q)
		}
		if !strings.Contains(q, "$3") {
			t.Errorf("%s door does not bind the schedule type as $3:\n%s", tc.name, q)
		}
	}
}

func TestModelRunScheduleLookup_RunsAreRecordedUnderTheDoorTheyCameThrough(t *testing.T) {
	if _, tr := modelRunScheduleLookup(false); tr != triggerScheduled {
		t.Errorf("clock door records trigger %q, want %q", tr, triggerScheduled)
	}
	if _, tr := modelRunScheduleLookup(true); tr != triggerTriggered {
		t.Errorf("event door records trigger %q, want %q", tr, triggerTriggered)
	}
}

func TestModelRunScheduleLookup_EventDoorRechecksTenancyAtRunTime(t *testing.T) {
	// The fire path already checks this when it looks the models up. With Temporal in the
	// path the rebuild can run much later than that lookup, so the predicate has to be
	// re-evaluated against the rows as they stand at run time.
	//
	// The signal does not say which upstream fired, and a schedule may wait on several, so
	// the check reads "no upstream of this schedule sits outside the model's workspace"
	// rather than joining the one that completed.
	event, _ := modelRunScheduleLookup(true)
	for _, want := range []string{
		"JOIN saved_queries sq ON sq.id = s.saved_query_id",
		"FROM saved_query_schedule_upstreams u",
		"COALESCE(up.workspace_id, um.workspace_id) IS DISTINCT FROM sq.workspace_id",
	} {
		if !strings.Contains(event, want) {
			t.Errorf("event door is missing %q:\n%s", want, event)
		}
	}
	// Stated as an absence, not a presence. One upstream in the wrong workspace has to
	// stop the rebuild; it must not be outvoted by the ones still in the right one.
	if !strings.Contains(event, "NOT EXISTS") {
		t.Errorf("event door's tenancy check is not stated as an absence:\n%s", event)
	}

	// The clock door must not carry it: a cadence schedule has no upstream rows at all, so
	// the subquery would cost a lookup that can only ever come back empty.
	clock, _ := modelRunScheduleLookup(false)
	if strings.Contains(clock, "saved_query_schedule_upstreams") {
		t.Errorf("clock door reads upstream rows, which a cadence schedule never has:\n%s", clock)
	}
}

func TestModelRunScheduleLookup_PausedScheduleIsLoadedNotFilteredOut(t *testing.T) {
	// Both doors load a paused row so the caller's status check can answer "schedule is
	// paused". Filtering it out here would report the far less useful "no schedule".
	for _, tc := range []struct {
		name  string
		event bool
	}{{"clock", false}, {"event", true}} {
		q, _ := modelRunScheduleLookup(tc.event)
		if !strings.Contains(q, "s.status != 'deleted'") {
			t.Errorf("%s door does not exclude deleted schedules:\n%s", tc.name, q)
		}
		if strings.Contains(q, "s.status = 'active'") {
			t.Errorf("%s door filters on active, hiding why a paused schedule did not fire:\n%s", tc.name, q)
		}
	}
}

func TestModelRunScheduleLookup_BothDoorsMatchOnBothIDs(t *testing.T) {
	// A tick naming a schedule that belongs to a different saved query must execute
	// nothing, whichever door it arrives at.
	for _, tc := range []struct {
		name  string
		event bool
	}{{"clock", false}, {"event", true}} {
		q, _ := modelRunScheduleLookup(tc.event)
		if !strings.Contains(q, "s.saved_query_id = $1") || !strings.Contains(q, "s.schedule_id = $2") {
			t.Errorf("%s door does not match on both ids:\n%s", tc.name, q)
		}
	}
}

// ============================================================================
// Handing rebuilds to Temporal, and what happens when that is not possible
// ============================================================================

// pipelineSource is the completing-pipeline case, which is what most of these tests are
// about. Depth stays 0: a pipeline is where a chain starts, never a link inside one.
func pipelineSource(pipelineID, executionID string) modelRefreshSource {
	return modelRefreshSource{
		Kind:        upstreamKindPipeline,
		ID:          pipelineID,
		ExecutionID: executionID,
	}
}

// fanOutOf is the value the fire path builds: one completion, its occurrence, and every
// model waiting on it.
func fanOutOf(src modelRefreshSource, targets ...modelRefreshTarget) upstreamFanOut {
	return upstreamFanOut{Occurrence: fanOutOccurrence(src), Source: src, Targets: targets}
}

func TestUpstreamFanOutWorkflowID_IsKeyedOnTheCompletion(t *testing.T) {
	// The id is what makes a redelivered completion land on a workflow that already
	// exists instead of rebuilding everything a second time, so it must depend on the
	// upstream AND on which completion of it this is.
	a := upstreamFanOutWorkflowID(fanOutOf(pipelineSource("p1", "e1")))
	if a != upstreamFanOutWorkflowID(fanOutOf(pipelineSource("p1", "e1"))) {
		t.Error("one completion produced two different workflow ids, so a redelivery would fan out again")
	}
	if a == upstreamFanOutWorkflowID(fanOutOf(pipelineSource("p1", "e2"))) {
		t.Error("two completions of one pipeline share an id, so the second would be refused as a duplicate")
	}
	if a == upstreamFanOutWorkflowID(fanOutOf(pipelineSource("p2", "e1"))) {
		t.Error("two different upstreams share one workflow id")
	}
	// The kind is in the id because a pipeline and a model can hold the same uuid in
	// principle, and two unrelated fan-outs collapsing into one is silent.
	model := modelRefreshSource{Kind: upstreamKindModel, ID: "p1", ExecutionID: "e1"}
	if a == upstreamFanOutWorkflowID(fanOutOf(model)) {
		t.Error("a pipeline and a model with the same id share one workflow id")
	}
}

func TestFanOutOccurrence_APipelineCompletionIsNamedByItsExecution(t *testing.T) {
	// The execution id is the only stable name a completion has, and using it is what
	// makes the fan-out idempotent under Kafka redelivery.
	if got := fanOutOccurrence(pipelineSource("p1", "e1")); got != "e1" {
		t.Errorf("occurrence = %q, want the completing execution's id", got)
	}
}

func TestFanOutOccurrence_EveryModelHopGetsAFreshName(t *testing.T) {
	// A model hop carries the execution id of the pipeline that STARTED the chain, so it
	// is the same value on every hop and on every later rebuild of that model. Reusing it
	// would give successive rebuilds one workflow id, and the second would be refused as
	// a duplicate — the chain would stop after one hop with nothing reporting it.
	hop := modelRefreshSource{Kind: upstreamKindModel, ID: "m0", ExecutionID: "", Depth: 1}
	first, second := fanOutOccurrence(hop), fanOutOccurrence(hop)
	if first == "" || second == "" {
		t.Fatal("a model hop got an empty occurrence, so every hop would share one workflow id")
	}
	if first == second {
		t.Errorf("two hops of the same model got the same occurrence %q; the second would be refused as a duplicate", first)
	}
}

func TestFanOutOccurrence_AModelHopBelowAPipelineIsNotNamedByTheRootExecution(t *testing.T) {
	// The execution id is now threaded down every model hop, so the rule cannot be "use
	// the execution id when there is one": two rebuilds of this model in the same chain
	// family would then share a workflow id and the second would be refused silently.
	a := modelRefreshSource{Kind: upstreamKindModel, ID: "m0", ExecutionID: "e1", RunID: "r1", Depth: 1}
	b := modelRefreshSource{Kind: upstreamKindModel, ID: "m0", ExecutionID: "e1", RunID: "r2", Depth: 1}
	if got := fanOutOccurrence(a); got == "e1" {
		t.Fatal("a model hop was named by the root pipeline execution; every rebuild of it would collide")
	}
	if fanOutOccurrence(a) != "r1" || fanOutOccurrence(b) != "r2" {
		t.Errorf("occurrences %q/%q, want each hop named by its own run row", fanOutOccurrence(a), fanOutOccurrence(b))
	}
	noRow := modelRefreshSource{Kind: upstreamKindModel, ID: "m0", ExecutionID: "e1", Depth: 1}
	if x, y := fanOutOccurrence(noRow), fanOutOccurrence(noRow); x == "e1" || x == y {
		t.Errorf("a hop with no run row got %q then %q, want a fresh name each time", x, y)
	}
}

func TestProvenanceFor_DepthIsHopsFromTheRootWhateverStartedTheChain(t *testing.T) {
	// The chain counter starts at 0 below a pipeline and at 1 below a model, so the raw
	// counter would record the same position two different ways.
	cases := []struct {
		name string
		src  modelRefreshSource
		want int
	}{
		{"pipeline -> B", modelRefreshSource{Kind: upstreamKindPipeline, ID: "p", ExecutionID: "e1"}, 1},
		{"pipeline -> B -> C", modelRefreshSource{Kind: upstreamKindModel, ID: "b", ExecutionID: "e1", Depth: 1}, 2},
		{"model A -> B", modelRefreshSource{Kind: upstreamKindModel, ID: "a", Depth: 1}, 1},
		{"model A -> B -> C", modelRefreshSource{Kind: upstreamKindModel, ID: "b", Depth: 2}, 2},
	}
	for _, tc := range cases {
		if got := provenanceFor(tc.src, 3); got.Depth != tc.want || got.Coalesced != 3 || got.UpstreamID != tc.src.ID {
			t.Errorf("%s: provenance %+v, want depth %d", tc.name, got, tc.want)
		}
	}
}

func TestRunProvenance_AMalformedPayloadCostsTheProvenanceNotTheRow(t *testing.T) {
	good := "11111111-1111-1111-1111-111111111111"
	if p := (runProvenance{UpstreamKind: "table", UpstreamID: good}).sanitized(); p != (runProvenance{}) {
		t.Errorf("unknown kind kept: %+v", p)
	}
	if p := (runProvenance{UpstreamKind: upstreamKindModel, UpstreamID: "nope"}).sanitized(); p != (runProvenance{}) {
		t.Errorf("non-uuid upstream kept: %+v", p)
	}
	p := (runProvenance{UpstreamKind: upstreamKindModel, UpstreamID: good, UpstreamRunID: "x", Depth: 2}).sanitized()
	if p.UpstreamID != good || p.UpstreamRunID != "" || p.Depth != 2 {
		t.Errorf("a bad run id should drop only itself: %+v", p)
	}
}

func TestStartUpstreamFanOut_NoTemporalMeansEverythingRunsLocally(t *testing.T) {
	// A deployment with no Temporal is a supported mode, not a fault. Every target has
	// to come back for the in-process path, or the trigger stops working entirely there.
	targets := []modelRefreshTarget{{ModelID: "m1"}, {ModelID: "m2"}}
	local := startUpstreamFanOut(context.Background(), nil, fanOutOf(pipelineSource("p1", "e1"), targets...))
	if len(local) != len(targets) {
		t.Fatalf("got %d local targets, want all %d", len(local), len(targets))
	}
}

func TestStartUpstreamFanOut_AStartedFanOutDoesNotAlsoRunLocally(t *testing.T) {
	// The whole point of the handover: a fan-out Temporal accepted must not also run on
	// the goroutine path, or every completion would rebuild each model twice.
	var seen []upstreamFanOut
	dispatch := func(_ context.Context, f upstreamFanOut) error {
		seen = append(seen, f)
		return nil
	}
	targets := []modelRefreshTarget{{ModelID: "m1"}, {ModelID: "m2"}}
	local := startUpstreamFanOut(context.Background(), dispatch, fanOutOf(pipelineSource("p1", "e1"), targets...))
	if len(local) != 0 {
		t.Errorf("got %d local targets after a successful fan-out, want 0", len(local))
	}
	// One call for the whole batch, not one per model — that is the change: the gateway
	// can die the instant it returns and both models are still signalled.
	if len(seen) != 1 {
		t.Fatalf("dispatch was called %d times, want exactly one call carrying the whole batch", len(seen))
	}
	if len(seen[0].Targets) != 2 {
		t.Errorf("the one call carried %d targets, want both", len(seen[0].Targets))
	}
}

func TestStartUpstreamFanOut_AFailedStartSendsTheWholeBatchToTheLocalPath(t *testing.T) {
	// There is one call now, so a half-dispatched batch is not a state that can exist.
	// Every target comes back, because dropping any of them means a model that silently
	// stops updating.
	dispatch := func(context.Context, upstreamFanOut) error { return errors.New("temporal unavailable") }
	targets := []modelRefreshTarget{{ModelID: "m1"}, {ModelID: "m2"}, {ModelID: "m3"}}
	local := startUpstreamFanOut(context.Background(), dispatch, fanOutOf(pipelineSource("p1", "e1"), targets...))
	if len(local) != len(targets) {
		t.Fatalf("local targets = %v, want all %d back for the in-process path", local, len(targets))
	}
}

func TestStartUpstreamFanOut_ACompletionAlreadyFannedOutIsNotRebuiltLocally(t *testing.T) {
	// Temporal refusing the start because this exact completion already has a fan-out is
	// "already done", not "did not happen". Falling back here would undo the duplicate
	// suppression the workflow id buys, and would do it on the path with no coalescing —
	// so a redelivered Kafka message would rebuild every model a second time.
	dispatch := func(context.Context, upstreamFanOut) error {
		return &serviceerror.WorkflowExecutionAlreadyStarted{}
	}
	targets := []modelRefreshTarget{{ModelID: "m1"}, {ModelID: "m2"}}
	local := startUpstreamFanOut(context.Background(), dispatch, fanOutOf(pipelineSource("p1", "e1"), targets...))
	if len(local) != 0 {
		t.Fatalf("got %d local targets for a completion that was already fanned out, want 0", len(local))
	}
}

func TestStartUpstreamFanOut_AWrappedAlreadyStartedIsStillRecognised(t *testing.T) {
	// The SDK wraps service errors on the way out. Matching on the type rather than on a
	// message keeps the distinction working through that wrapping; a string match would
	// pass here and quietly fall back in production.
	dispatch := func(context.Context, upstreamFanOut) error {
		return fmt.Errorf("start workflow: %w", &serviceerror.WorkflowExecutionAlreadyStarted{})
	}
	local := startUpstreamFanOut(context.Background(), dispatch,
		fanOutOf(pipelineSource("p1", "e1"), modelRefreshTarget{ModelID: "m1"}))
	if len(local) != 0 {
		t.Fatalf("a wrapped already-started error fell back to the local path: %v", local)
	}
}

func TestStartUpstreamFanOut_FallbackKeepsTheIdentityTheRunNeeds(t *testing.T) {
	// The fallback rebuild runs as run_as_user_id and audits against schedule_id, so a
	// target that comes back has to come back whole, not just as a model id. It is also
	// why run_as_user_id stays out of the workflow argument and lives only here.
	dispatch := func(context.Context, upstreamFanOut) error { return errors.New("nope") }
	want := modelRefreshTarget{ScheduleID: "s1", ModelID: "m1", RunAsUserID: "u1"}
	local := startUpstreamFanOut(context.Background(), dispatch, fanOutOf(pipelineSource("p1", "e1"), want))
	if len(local) != 1 || local[0] != want {
		t.Fatalf("local = %v, want %v", local, want)
	}
}

func TestStartUpstreamFanOut_TheWholeSourceReachesTemporalNotJustTheID(t *testing.T) {
	// Depth is what stops a rebuild chain, and it only bounds anything if it survives every
	// hop. This is the hop where it is easiest to lose: the dispatcher is the last place
	// the source is a Go value before it becomes a workflow argument.
	var got upstreamFanOut
	dispatch := func(_ context.Context, f upstreamFanOut) error {
		got = f
		return nil
	}
	want := modelRefreshSource{
		Kind:        upstreamKindModel,
		ID:          "m0",
		ExecutionID: "e9",
		Depth:       3,
	}
	startUpstreamFanOut(context.Background(), dispatch, fanOutOf(want, modelRefreshTarget{ModelID: "m1"}))
	if got.Source != want {
		t.Fatalf("dispatch saw %+v, want %+v", got.Source, want)
	}
}

// ============================================================================
// Which upstreams a completion wakes
// ============================================================================

func TestDownstreamModelLookup_EachKindReadsItsOwnColumn(t *testing.T) {
	// The two upstream columns are two different foreign keys. A query that matched a
	// model id against the pipeline column would find nothing and report it as "no
	// downstream models", which is indistinguishable from a correct empty result.
	pipeline := downstreamModelLookup(upstreamKindPipeline)
	model := downstreamModelLookup(upstreamKindModel)

	if !strings.Contains(pipeline, "u.upstream_pipeline_id = $1::uuid") {
		t.Errorf("pipeline lookup does not match on the pipeline column:\n%s", pipeline)
	}
	if strings.Contains(pipeline, "u.upstream_saved_query_id = $1") {
		t.Errorf("pipeline lookup also matches the model column:\n%s", pipeline)
	}
	if !strings.Contains(model, "u.upstream_saved_query_id = $1::uuid") {
		t.Errorf("model lookup does not match on the model column:\n%s", model)
	}
	if strings.Contains(model, "u.upstream_pipeline_id = $1") {
		t.Errorf("model lookup also matches the pipeline column:\n%s", model)
	}
	// The join follows the column, or the workspace check below reads the wrong table.
	if !strings.Contains(pipeline, "JOIN pipelines up ON up.id = u.upstream_pipeline_id") {
		t.Errorf("pipeline lookup joins the wrong producer table:\n%s", pipeline)
	}
	if !strings.Contains(model, "JOIN saved_queries up ON up.id = u.upstream_saved_query_id") {
		t.Errorf("model lookup joins the wrong producer table:\n%s", model)
	}
}

func TestDownstreamModelLookup_UnknownKindDoesNotFallThroughToEverything(t *testing.T) {
	// Only two kinds exist and the column is chosen, not bound, so the failure mode worth
	// pinning is a third value quietly selecting one of the two. It resolves to the
	// pipeline column, which is the safe default: a model id never matches a pipeline id,
	// so the result is empty rather than someone else's schedules.
	q := downstreamModelLookup("something-else")
	if !strings.Contains(q, "u.upstream_pipeline_id = $1::uuid") {
		t.Errorf("an unrecognized kind picked neither column:\n%s", q)
	}
}

func TestDownstreamModelLookup_BothKindsCheckTenancyAndPauseAtFireTime(t *testing.T) {
	// Neither is trusted from create time. The schedule was authorized against the
	// creator's workspace when it was written and an upstream can be moved afterwards; and
	// a paused event schedule has no Temporal schedule to have been paused, so this
	// predicate IS the pause for this path.
	for _, kind := range []string{upstreamKindPipeline, upstreamKindModel} {
		q := downstreamModelLookup(kind)
		for _, want := range []string{
			"sq.workspace_id = up.workspace_id",
			"s.status = 'active'",
			"s.schedule_type = 'after_upstream'",
		} {
			if !strings.Contains(q, want) {
				t.Errorf("%s lookup is missing %q:\n%s", kind, want, q)
			}
		}
	}
}

// ============================================================================
// What continues a rebuild chain, and what stops one
// ============================================================================
// Both rules below are invisible when broken. A chain that propagates from a failed run
// still writes successful-looking rows for everything downstream, and a chain that never
// hits its bound just keeps rebuilding — neither raises an error anywhere.

func TestNextChainSource_OnlyASucceededRunPropagates(t *testing.T) {
	// A model whose rebuild failed still holds the data it held before. Waking its
	// consumers would rebuild them against a snapshot that is one refresh stale while
	// their run history reports a fresh trigger — the confusion the trigger exists to
	// avoid. "skipped" is the same case: nothing was written, so nothing changed.
	for _, status := range []string{"failed", "skipped", "", "SUCCEEDED"} {
		if src := nextChainSource(&modelRunResult{Status: status}, "m1", "r1", "e1", 0); src != nil {
			t.Errorf("status %q propagated to depth %d, want no propagation", status, src.Depth)
		}
	}
	if src := nextChainSource(nil, "m1", "r1", "e1", 0); src != nil {
		t.Error("a nil result propagated; a run that produced no result has nothing to pass on")
	}
	if src := nextChainSource(&modelRunResult{Status: "succeeded"}, "m1", "r1", "e1", 0); src == nil {
		t.Fatal("a succeeded run did not propagate; the chain would stop at every model")
	}
}

func TestNextChainSource_TheModelBecomesTheUpstreamAndTheDepthAdvances(t *testing.T) {
	// The hop is what makes a model an upstream of its own consumers, and the +1 is the
	// only thing that ever moves the chain toward its bound.
	src := nextChainSource(&modelRunResult{Status: "succeeded"}, "m1", "r1", "e1", 3)
	if src == nil {
		t.Fatal("no source")
	}
	if src.Kind != upstreamKindModel || src.ID != "m1" {
		t.Errorf("next hop is %s/%s, want the model that just finished", src.Kind, src.ID)
	}
	if src.ExecutionID != "e1" {
		t.Errorf("execution id = %q, want the pipeline execution that started the chain carried along", src.ExecutionID)
	}
	if src.RunID != "r1" {
		t.Errorf("run id = %q, want the finished run's own history row, so the next hop can point back at it", src.RunID)
	}
	if src.Depth != 4 {
		t.Errorf("depth = %d, want 4; a hop that does not advance the depth can never reach the bound", src.Depth)
	}
}

func TestChainDepthBound_ARingTerminates(t *testing.T) {
	// Two models naming each other, walked the way the fire path walks them: each
	// rebuild produces the source for the next. checkUpstreamCycle refuses this ring at
	// write time; this is the backstop for the one that got stored anyway, and the only
	// thing standing between it and an unbounded rebuild loop.
	src := *nextChainSource(&modelRunResult{Status: "succeeded"}, "a", "r0", "e1", 0)
	hops := 0
	for !chainDepthExceeded(src.Depth) {
		hops++
		if hops > 1000 {
			t.Fatal("the ring never terminated: the depth bound does not stop a chain")
		}
		next := "a"
		if src.ID == "a" {
			next = "b"
		}
		src = *nextChainSource(&modelRunResult{Status: "succeeded"}, next, "r", src.ExecutionID, src.Depth)
	}
	// The first hop enters at depth 1, so the bound admits depths 1..maxTriggerChainDepth
	// and refuses the one after. A bound that is off by one here is still a bound; a
	// bound that never fires is the bug.
	if hops != maxTriggerChainDepth {
		t.Errorf("the ring ran %d rebuilds before stopping, want %d", hops, maxTriggerChainDepth)
	}
}

func TestChainDepthBound_ADepthWithinTheBoundIsNotRefused(t *testing.T) {
	// The other half: a bound that refuses everything stops the trigger working at all,
	// and looks identical in a log to one that is simply never reached.
	for depth := 0; depth <= maxTriggerChainDepth; depth++ {
		if chainDepthExceeded(depth) {
			t.Errorf("depth %d was refused, want it allowed up to %d", depth, maxTriggerChainDepth)
		}
	}
	if !chainDepthExceeded(maxTriggerChainDepth + 1) {
		t.Errorf("depth %d was allowed, want it refused", maxTriggerChainDepth+1)
	}
}
