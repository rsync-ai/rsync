package workflows

import (
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/testsuite"
)

// Explorer workflow query tests.
//
// These guard a surface with an unusual failure mode: every way it can be wrong looks
// exactly like the feature working. A handler registered one line too low answers
// nothing for the whole stretch the workflow spends blocked, which is nearly all of it,
// and the gateway renders that as "no detail available" — the same thing it renders for
// a model that genuinely has no run. A handler that reports the wrong number reports it
// confidently. Nothing errors, nothing alerts, and the page still fills in.
//
// So each test below pins one claim the handlers make, and each was checked by mutating
// the handler to break that claim and confirming the test fails. The registrations
// themselves are checked by census at the bottom rather than by eye, because the failure
// of a missing one is silence.

func queryModelRefreshState(env *testsuite.TestWorkflowEnvironment) (ModelRefreshState, error) {
	val, err := env.QueryWorkflow(ModelRefreshStateQuery)
	if err != nil {
		return ModelRefreshState{}, err
	}
	var st ModelRefreshState
	if err := val.Get(&st); err != nil {
		return ModelRefreshState{}, err
	}
	return st, nil
}

func queryFreshnessSweepState(env *testsuite.TestWorkflowEnvironment) (FreshnessSweepState, error) {
	val, err := env.QueryWorkflow(FreshnessSweepStateQuery)
	if err != nil {
		return FreshnessSweepState{}, err
	}
	var st FreshnessSweepState
	if err := val.Get(&st); err != nil {
		return FreshnessSweepState{}, err
	}
	return st, nil
}

func queryFanOutState(env *testsuite.TestWorkflowEnvironment) (FanOutState, error) {
	val, err := env.QueryWorkflow(FanOutStateQuery)
	if err != nil {
		return FanOutState{}, err
	}
	var st FanOutState
	if err := val.Get(&st); err != nil {
		return FanOutState{}, err
	}
	return st, nil
}

// The placement guard, and the reason this file exists.
//
// ModelRefreshWorkflow blocks on ch.Receive within a few statements of starting and then
// sits there for as long as the burst lasts, which on a healthy model is the entire run.
// A handler registered below that line exists only in the slivers between signals, so
// every well-behaved model in the workspace would answer "unknown queryType" and the
// gateway would report detail_available:false for all of them. The feature would be
// broken in exactly the case it is meant to serve, and nothing would say so.
func TestModelRefreshStateIsQueryableBeforeTheFirstSignalArrives(t *testing.T) {
	probe := &modelRefreshProbe{}
	env := newModelRefreshEnv(probe)

	var st ModelRefreshState
	var queryErr error
	env.RegisterDelayedCallback(func() {
		st, queryErr = queryModelRefreshState(env)
	}, time.Millisecond)
	signalAt(env, 2*time.Millisecond, ModelRefreshRequest{
		ScheduleID: "sched-1", UpstreamKind: "pipeline", UpstreamID: "pipe-1",
	})

	env.ExecuteWorkflow(ModelRefreshWorkflow, ModelRefreshInput{SavedQueryID: "model-1"})

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow never completed")
	}
	if queryErr != nil {
		t.Fatalf("querying a model that had not yet been signalled failed: %v — the handler is "+
			"registered below the blocking receive, so it does not exist while the loop waits", queryErr)
	}
	if st.SavedQueryID != "model-1" {
		t.Errorf("the state named model %q, want model-1", st.SavedQueryID)
	}
	if st.Phase != ModelRefreshPhaseWaiting {
		t.Errorf("a loop parked on its signal channel reported phase %q, want %q",
			st.Phase, ModelRefreshPhaseWaiting)
	}
	if st.Current != nil {
		t.Errorf("a loop that had rebuilt nothing reported a rebuild in flight: %+v", st.Current)
	}
	if st.RefreshesPerRun != modelRefreshesPerRun {
		t.Errorf("the run bound was reported as %d, want %d — the gateway shows it beside "+
			"refreshes_this_run to say how close this run is to handing over",
			st.RefreshesPerRun, modelRefreshesPerRun)
	}
}

// Depth is the one field a coalesced rebuild does not take from the latest request, and
// the handler has to obey the same rule the loop does. A batch can mix a shallow chain
// with a deep one, so reporting the latest request's depth would let the answer launder
// an eight-hop chain into a one-hop one — reintroducing through the observability surface
// the exact bug the batch-max loop was written to prevent, and making the bound look
// respected while it is being exceeded.
func TestModelRefreshStateReportsTheBatchMaxDepthNotTheLatest(t *testing.T) {
	probe := &modelRefreshProbe{}
	env := newModelRefreshEnv(probe)

	var st ModelRefreshState
	var queryErr error
	probe.onCall = func(n int) {
		switch n {
		case 1:
			// Deep first, then shallow. The last to arrive is the one a naive handler
			// reports, so this ordering is what makes the bug visible.
			env.SignalWorkflow(ModelRefreshSignalName, ModelRefreshRequest{
				ScheduleID: "sched-a", UpstreamKind: "model", UpstreamID: "model-0", Depth: 7,
			})
			env.SignalWorkflow(ModelRefreshSignalName, ModelRefreshRequest{
				ScheduleID: "sched-b", UpstreamKind: "pipeline", UpstreamID: "pipe-1", Depth: 2,
			})
		case 2:
			st, queryErr = queryModelRefreshState(env)
		}
	}

	signalAt(env, time.Millisecond, ModelRefreshRequest{
		ScheduleID: "sched-1", UpstreamKind: "model", UpstreamID: "model-0", Depth: 5,
	})
	env.ExecuteWorkflow(ModelRefreshWorkflow, ModelRefreshInput{SavedQueryID: "model-1"})

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow never completed")
	}
	if queryErr != nil {
		t.Fatalf("querying during the coalesced rebuild failed: %v", queryErr)
	}
	if st.Phase != ModelRefreshPhaseRebuilding {
		t.Fatalf("a loop inside the rebuild activity reported phase %q, want %q",
			st.Phase, ModelRefreshPhaseRebuilding)
	}
	if st.Current == nil {
		t.Fatal("a loop inside the rebuild activity reported no rebuild in flight")
	}
	if st.Current.Depth != 7 {
		t.Errorf("the coalesced rebuild was reported at depth %d, want 7 — the deepest request "+
			"in the batch, not the last one, which carried 2", st.Current.Depth)
	}
	if st.Current.Coalesced != 2 {
		t.Errorf("the rebuild was reported as standing in for %d completions, want 2 — this is "+
			"the number saved_query_runs will under-report by", st.Current.Coalesced)
	}
	if st.Current.ScheduleID != "sched-b" {
		t.Errorf("the rebuild named schedule %q, want sched-b — every field except depth does "+
			"come from the last request", st.Current.ScheduleID)
	}
}

// How far behind a model is cannot be read from the loop's own queue slice: the loop
// drains the signal channel into that slice and empties it into the batch without
// blocking in between, so no query can ever observe it holding anything. Reading the
// channel is the only honest answer, and it is the only place the backlog exists at all —
// coalesced completions write no run row, no audit row and no log line of their own.
func TestModelRefreshStateCountsTheCompletionsItHasNotYetRebuilt(t *testing.T) {
	// A failing activity, purely to hold the rebuild open across the mock clock: the
	// retry backoff is the only stretch of a rebuild a delayed callback can land in.
	probe := &modelRefreshProbe{err: errors.New("the gateway is down")}
	env := newModelRefreshEnv(probe)

	probe.onCall = func(n int) {
		if n != 1 {
			return
		}
		env.SignalWorkflow(ModelRefreshSignalName, ModelRefreshRequest{
			ScheduleID: "sched-a", UpstreamKind: "pipeline", UpstreamID: "pipe-1",
		})
		env.SignalWorkflow(ModelRefreshSignalName, ModelRefreshRequest{
			ScheduleID: "sched-b", UpstreamKind: "pipeline", UpstreamID: "pipe-2",
		})
	}

	var st ModelRefreshState
	var queryErr error
	// Between the second and third attempt: the rebuild is still in flight and both
	// completions have long since been delivered.
	env.RegisterDelayedCallback(func() {
		st, queryErr = queryModelRefreshState(env)
	}, 8*time.Second)

	signalAt(env, time.Millisecond, ModelRefreshRequest{
		ScheduleID: "sched-1", UpstreamKind: "pipeline", UpstreamID: "pipe-1",
	})
	env.ExecuteWorkflow(ModelRefreshWorkflow, ModelRefreshInput{SavedQueryID: "model-1"})

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow never completed")
	}
	if queryErr != nil {
		t.Fatalf("querying mid-rebuild failed: %v", queryErr)
	}
	if st.Phase != ModelRefreshPhaseRebuilding {
		t.Fatalf("a loop inside the rebuild activity reported phase %q, want %q",
			st.Phase, ModelRefreshPhaseRebuilding)
	}
	if st.QueuedCompletions != 2 {
		t.Errorf("%d completions were reported as owed a rebuild, want 2 — they arrived during "+
			"the rebuild and exist nowhere but this number", st.QueuedCompletions)
	}
}

// The loop deliberately survives a spent retry budget: a later completion of the upstream
// carries fresher data than a replay of this one, and failing here would take the
// buffered completions down with it. That decision costs one thing — the failure becomes
// a single log line, after which the loop is indistinguishable from a healthy idle one.
// This field is where that difference is kept.
func TestModelRefreshStateSurfacesARebuildThatExhaustedItsRetries(t *testing.T) {
	probe := &modelRefreshProbe{err: errors.New("the engine rejected the query")}
	env := newModelRefreshEnv(probe)

	signalAt(env, time.Millisecond, ModelRefreshRequest{
		ScheduleID: "sched-1", UpstreamKind: "pipeline", UpstreamID: "pipe-1",
	})
	env.ExecuteWorkflow(ModelRefreshWorkflow, ModelRefreshInput{SavedQueryID: "model-1"})

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow never completed")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("a spent retry budget failed the workflow: %v — it must not", err)
	}
	st, err := queryModelRefreshState(env)
	if err != nil {
		t.Fatalf("querying after the failed rebuild failed: %v", err)
	}
	if st.Phase != ModelRefreshPhaseWaiting {
		t.Errorf("after the rebuild gave up the loop reported phase %q, want %q",
			st.Phase, ModelRefreshPhaseWaiting)
	}
	if st.Current != nil {
		t.Errorf("after the rebuild gave up the loop still reported one in flight: %+v", st.Current)
	}
	if !strings.Contains(st.LastError, "the engine rejected the query") {
		t.Errorf("the failure the loop swallowed was reported as %q, want it to carry the "+
			"activity error — otherwise a model that has failed every rebuild for a day reads "+
			"exactly like one with nothing to do", st.LastError)
	}
	if st.RefreshesThisRun != 1 {
		t.Errorf("%d refreshes were counted, want 1 — a failed rebuild is still a rebuild this "+
			"run spent against its history bound", st.RefreshesThisRun)
	}
}

// MODEL_FRESHNESS_SWEEP_SECONDS is read by the gateway and passed in as input, and the
// singleton is started with USE_EXISTING — so changing it reaches a run that is already
// going only if that run is terminated first. Until then the environment says one thing
// and the sweep does another, and this answer is the only place the disagreement is
// visible. Reporting the compiled-in default instead would make the two always agree.
func TestFreshnessSweepStateReportsTheResolvedIntervalNotTheDefault(t *testing.T) {
	probe := &freshnessSweepProbe{}
	env := newFreshnessEnv(probe)

	var st FreshnessSweepState
	var queryErr error
	env.RegisterDelayedCallback(func() {
		st, queryErr = queryFreshnessSweepState(env)
	}, 5*time.Second)
	env.RegisterDelayedCallback(func() {
		env.CancelWorkflow()
	}, 6*time.Second)

	env.ExecuteWorkflow(ModelFreshnessWorkflow, ModelFreshnessInput{IntervalSeconds: 17})

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow never completed")
	}
	if queryErr != nil {
		t.Fatalf("querying the sweep during its first interval failed: %v — the handler is "+
			"registered below the first Sleep, so it does not exist for the whole first "+
			"interval, which is exactly when an operator asks whether the sweep came up", queryErr)
	}
	if st.EffectiveIntervalSeconds != 17 {
		t.Errorf("the running sweep reported a %ds interval, want the 17 it was started with — "+
			"reporting %d would be reporting the compiled-in default",
			st.EffectiveIntervalSeconds, int(ModelFreshnessSweepInterval/time.Second))
	}
	if st.SweptThisRun {
		t.Error("the sweep reported having swept before its first interval elapsed")
	}
	if st.TicksPerRun != modelFreshnessTicksPerRun {
		t.Errorf("the run bound was reported as %d, want %d", st.TicksPerRun, modelFreshnessTicksPerRun)
	}
}

// The sweep swallows its activity error on purpose — it is the thing that notices when
// nothing is happening, so it has to be the last thing to stop. And the success log is
// suppressed unless something changed. Between them, a sweep that has failed every tick
// for a day and a healthy quiet one produce identical logs and an identically empty
// saved_query_freshness_breaches. These fields are the only place that difference exists.
func TestFreshnessSweepStateSurfacesTheErrorTheSweepSwallows(t *testing.T) {
	probe := &freshnessSweepProbe{err: errors.New("the gateway refused the connection")}
	env := newFreshnessEnv(probe)

	var st FreshnessSweepState
	var queryErr error
	env.RegisterDelayedCallback(func() {
		st, queryErr = queryFreshnessSweepState(env)
	}, 30*time.Second)
	env.RegisterDelayedCallback(func() {
		env.CancelWorkflow()
	}, 31*time.Second)

	env.ExecuteWorkflow(ModelFreshnessWorkflow, ModelFreshnessInput{IntervalSeconds: 5})

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow never completed")
	}
	if queryErr != nil {
		t.Fatalf("querying the sweep after a failed tick failed: %v", queryErr)
	}
	if !st.SweptThisRun {
		t.Fatal("the sweep reported no tick at all after several intervals had elapsed")
	}
	if st.LastSweepOK {
		t.Error("a tick whose activity exhausted its retries was reported as OK")
	}
	if !strings.Contains(st.LastError, "the gateway refused the connection") {
		t.Errorf("the swallowed sweep error was reported as %q, want it to carry the activity "+
			"error — it is swallowed everywhere else", st.LastError)
	}
}

// A sweep that ticked once an hour ago and a sweep that ticked a second ago are the same
// workflow in the same RUNNING state. The counter and the timestamp are what separate a
// live loop from one wedged between ticks, which is the question the admin health row
// exists to answer.
func TestFreshnessSweepStateAdvancesAcrossTicks(t *testing.T) {
	probe := &freshnessSweepProbe{}
	env := newFreshnessEnv(probe)

	var afterOne, afterTwo FreshnessSweepState
	var errOne, errTwo error
	env.RegisterDelayedCallback(func() {
		afterOne, errOne = queryFreshnessSweepState(env)
	}, 7*time.Second)
	env.RegisterDelayedCallback(func() {
		afterTwo, errTwo = queryFreshnessSweepState(env)
	}, 12*time.Second)
	env.RegisterDelayedCallback(func() {
		env.CancelWorkflow()
	}, 13*time.Second)

	env.ExecuteWorkflow(ModelFreshnessWorkflow, ModelFreshnessInput{IntervalSeconds: 5})

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow never completed")
	}
	if errOne != nil || errTwo != nil {
		t.Fatalf("querying the sweep failed: %v / %v", errOne, errTwo)
	}
	if afterOne.TicksThisRun != 1 {
		t.Errorf("after one interval the sweep reported %d ticks, want 1", afterOne.TicksThisRun)
	}
	if afterTwo.TicksThisRun != 2 {
		t.Errorf("after two intervals the sweep reported %d ticks, want 2 — a counter that does "+
			"not advance is indistinguishable from a wedged loop", afterTwo.TicksThisRun)
	}
	if !afterOne.LastSweepOK {
		t.Error("a successful tick was reported as failed")
	}
	if afterOne.LastResult.Scanned != 3 || afterOne.LastResult.Opened != 1 {
		t.Errorf("the tick reported scanned=%d opened=%d, want the 3 and 1 the sweep returned — "+
			"the success log is suppressed when nothing changed, so this is where the numbers live",
			afterOne.LastResult.Scanned, afterOne.LastResult.Opened)
	}
	if afterOne.LastSweepAt == "" {
		t.Error("a completed tick reported no timestamp")
	}
	if afterTwo.LastSweepAt == afterOne.LastSweepAt {
		t.Errorf("both ticks reported the same time %q — the timestamp is how an operator sees "+
			"the loop is still moving", afterTwo.LastSweepAt)
	}
}

// The fan-out awaits its children strictly in dispatch order, so a child that finished
// out of turn is still unconfirmed as far as this workflow knows. Calling that "running"
// would be inventing a fact the fan-out does not have; a caller that needs the truth
// about one child composes the prefix with the model id and asks the child itself.
func TestFanOutStateReportsAModelAsNotYetConfirmedRatherThanRunning(t *testing.T) {
	probe := &fanOutProbe{hold: 10 * time.Second}
	env := newFanOutEnv(probe)

	var st FanOutState
	var queryErr error
	env.RegisterDelayedCallback(func() {
		st, queryErr = queryFanOutState(env)
	}, time.Second)

	env.ExecuteWorkflow(UpstreamFanOutWorkflow, pipelineFanOut(
		UpstreamFanOutTarget{ScheduleID: "s1", SavedQueryID: "m1"},
		UpstreamFanOutTarget{ScheduleID: "s2", SavedQueryID: "m2"},
		UpstreamFanOutTarget{ScheduleID: "s3", SavedQueryID: "m3"},
	))

	if !env.IsWorkflowCompleted() {
		t.Fatal("fan-out never completed")
	}
	if queryErr != nil {
		t.Fatalf("querying the fan-out while its children were in flight failed: %v — the "+
			"handler is registered after the dispatch loop, so the fan-out is unobservable for "+
			"exactly the stretch in which starting a child can itself be slow", queryErr)
	}
	if st.Total != 3 {
		t.Errorf("the fan-out reported %d dispatches, want 3", st.Total)
	}
	if len(st.Confirmed) != 0 {
		t.Errorf("the fan-out confirmed %v before any child had returned", st.Confirmed)
	}
	if len(st.NotYetConfirmed) != 3 {
		t.Errorf("the fan-out reported %d models as not yet confirmed, want 3: %v",
			len(st.NotYetConfirmed), st.NotYetConfirmed)
	}
	if st.ChildWorkflowIDPrefix == "" {
		t.Fatal("the fan-out reported no child workflow id prefix — without it a caller cannot " +
			"address any individual child")
	}
	// The prefix is only useful if it composes into a real child id the same way the
	// dispatch loop builds one.
	want := modelRefreshDispatchWorkflowID(st.ChildWorkflowIDPrefix, "m2")
	if !strings.HasSuffix(want, ":m2") {
		t.Errorf("composing the prefix with a model id produced %q, which is not a dispatch "+
			"child id", want)
	}
}

// A model whose dispatch failed and a model whose dispatch has not been reached are
// opposite facts: one is owed a rebuild nobody will deliver, the other is fine. Collapsing
// them is the one thing this answer must not do — a half-delivered fan-out is precisely
// what this workflow exists to make visible.
func TestFanOutStateCountsAFailedDeliverySeparatelyFromOneNotYetConfirmed(t *testing.T) {
	probe := &fanOutProbe{failFor: "m2"}
	env := newFanOutEnv(probe)

	env.ExecuteWorkflow(UpstreamFanOutWorkflow, pipelineFanOut(
		UpstreamFanOutTarget{ScheduleID: "s1", SavedQueryID: "m1"},
		UpstreamFanOutTarget{ScheduleID: "s2", SavedQueryID: "m2"},
		UpstreamFanOutTarget{ScheduleID: "s3", SavedQueryID: "m3"},
	))

	if !env.IsWorkflowCompleted() {
		t.Fatal("fan-out never completed")
	}
	st, err := queryFanOutState(env)
	if err != nil {
		t.Fatalf("querying the settled fan-out failed: %v", err)
	}
	if strings.Join(st.Failed, ",") != "m2" {
		t.Errorf("the fan-out reported failures %v, want just m2", st.Failed)
	}
	if strings.Join(st.Confirmed, ",") != "m1,m3" {
		t.Errorf("the fan-out reported confirmations %v, want m1 and m3 — one unreachable model "+
			"must not take the others with it", st.Confirmed)
	}
	if len(st.NotYetConfirmed) != 0 {
		t.Errorf("a settled fan-out reported %v as not yet confirmed — a model that failed is "+
			"settled, not outstanding", st.NotYetConfirmed)
	}
}

// The census.
//
// A handler that was never registered fails silently: the query returns "unknown
// queryType", the gateway renders that as no-detail-available, and no test that does not
// name the workflow would notice. Walking the package is how a fourth workflow gaining a
// query name without a registration, or a registration quietly moving to a workflow that
// does not own it, becomes a failure rather than a gap.
func TestEveryExplorerQueryNameIsRegisteredOnTheWorkflowThatOwnsIt(t *testing.T) {
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", func(fi fs.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatalf("could not parse the workflows package: %v", err)
	}

	// Constant name -> the workflow function that registers it.
	registeredIn := map[string]string{}
	upserts := []string{}
	for _, pkg := range pkgs {
		for _, file := range pkg.Files {
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok {
					continue
				}
				ast.Inspect(fn, func(n ast.Node) bool {
					call, ok := n.(*ast.CallExpr)
					if !ok {
						return true
					}
					sel, ok := call.Fun.(*ast.SelectorExpr)
					if !ok {
						return true
					}
					pkgIdent, ok := sel.X.(*ast.Ident)
					if !ok || pkgIdent.Name != "workflow" {
						return true
					}
					switch sel.Sel.Name {
					case "SetQueryHandler":
						if len(call.Args) < 2 {
							return true
						}
						name, ok := call.Args[1].(*ast.Ident)
						if !ok {
							t.Errorf("%s registers a query under a literal rather than a shared "+
								"constant; the gateway spells these names again and only a "+
								"constant keeps the two in step", fn.Name.Name)
							return true
						}
						registeredIn[name.Name] = fn.Name.Name
					case "UpsertTypedSearchAttributes", "UpsertSearchAttributes":
						upserts = append(upserts, fn.Name.Name)
					}
					return true
				})
			}
		}
	}

	// The denominator, asserted before anything is concluded from it. A parse that found
	// no registrations at all would otherwise satisfy every check below by vacuity.
	if len(registeredIn) < 3 {
		t.Fatalf("the census found %d query registrations in this package, want at least 3 — "+
			"finding none is how this test passes while the feature is absent: %v",
			len(registeredIn), registeredIn)
	}

	for constName, wantWorkflow := range map[string]string{
		"ModelRefreshStateQuery":   "ModelRefreshWorkflow",
		"FreshnessSweepStateQuery": "ModelFreshnessWorkflow",
		"FanOutStateQuery":         "UpstreamFanOutWorkflow",
	} {
		got, ok := registeredIn[constName]
		if !ok {
			t.Errorf("%s is a query name nothing registers a handler for; every caller would "+
				"get \"unknown queryType\", which the gateway cannot tell apart from a workflow "+
				"with no detail to give", constName)
			continue
		}
		if got != wantWorkflow {
			t.Errorf("%s is registered on %s, want %s", constName, got, wantWorkflow)
		}
	}

	// Search attributes are not an option on this deployment and adding one would not
	// fail loudly. The visibility store is Temporal's STANDARD schema (DB=postgresql in
	// every compose file and the Helm chart), whose executions_visibility table has no
	// search_attributes column at all. Registering a custom attribute against it exits 0
	// and lists successfully; only the filters fail. Worse, an Upsert IS a workflow
	// command, so adding one to a running workflow fails replay with TMPRL1100 — and
	// under the default BlockWorkflow panic policy the server retries that task forever
	// while the run stays RUNNING, with no DLQ and no alert. A model would simply stop
	// refreshing.
	if len(upserts) > 0 {
		t.Errorf("search attributes are upserted in %v; this deployment runs Temporal's standard "+
			"visibility schema, which has no column to put them in", upserts)
	}
}
