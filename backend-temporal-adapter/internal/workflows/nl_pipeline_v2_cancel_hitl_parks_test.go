package workflows

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

// Cancel coverage for every HITL park other than table selection (that one lives
// in nl_pipeline_v2_cancel_table_selection_test.go).
//
// Each scenario drives the REAL NLPipelineWorkflowV2 into one park with activity
// fakes, then sends the "cancel" signal StopPipeline sends. Two timings matter:
//
//   - during the park: the wait must end at the cancel, and the cancelled branch
//     must return before hitlTimeoutMessage reads state.WaitReason (the cancel
//     handler's Transition(StateCancelled) has set it to nil);
//   - while the park is being announced: the cancel lands before the wait starts,
//     so the call itself must not read state.WaitReason.TimeoutAt either.
//
// A control runs the same scenario with no cancel and must end at the 24h
// deadline as failed, so a scenario that silently stopped reaching its park
// cannot pass the cancel tests by accident.

type hitlParkScenario struct {
	name string
	// input builds the workflow input; fast-rerun inputs skip the agentic phases.
	input func() NLPipelineWorkflowV2Input
	// executor answers the n-th (1-based) executor dispatch.
	executor func(n int) ExecutorNativeResult
	// resolverErr, when set, is what ConnectorResolverActivityV2 fails with.
	resolverErr error
	// park identifies the PIPELINE_WAITING announcement as "stage/blocking_type".
	park string
	// waitingFor is the phrase the park's timeout message names.
	waitingFor string
	// wantCalls are activities that must have run for this park to be the one reached.
	wantCalls []string
	// executorCalls is how many times the executor must have been dispatched.
	executorCalls int
}

type hitlParkHarness struct {
	sc hitlParkScenario

	mu           sync.Mutex
	calls        []string
	executorN    int
	parks        []string
	domainEvents []string
	statusWrites []string
	cleanupCalls int

	// onWaiting, when set, runs once inside the StateUpdateActivity that announces
	// a park, before that activity returns.
	onWaiting func()
}

func (h *hitlParkHarness) record(name string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls = append(h.calls, name)
}

func (h *hitlParkHarness) intent(_ context.Context, _ NLPipelineWorkflowV2Input) (map[string]interface{}, error) {
	h.record("IntentActivityV2")
	return map[string]interface{}{"source_type": "source-db", "destination_type": "dest-warehouse"}, nil
}

func (h *hitlParkHarness) resolver(_ context.Context, _ NLPipelineWorkflowV2Input, _ *WorkflowState) (map[string]interface{}, error) {
	h.record("ConnectorResolverActivityV2")
	if h.sc.resolverErr != nil {
		return nil, h.sc.resolverErr
	}
	return map[string]interface{}{
		"resolved_connectors": map[string]interface{}{"source": "source-db", "destination": "dest-warehouse"},
	}, nil
}

func (h *hitlParkHarness) connectionValidation(_ context.Context, _ ConnectionValidationRequest) (*ConnectionValidationResult, error) {
	h.record("ConnectionValidationActivityV2")
	return &ConnectionValidationResult{
		AllConnectionsValid: false,
		MissingConnections: []MissingConnection{
			{Type: "dest-warehouse", Direction: "destination", Message: "no destination connection configured"},
		},
	}, nil
}

func (h *hitlParkHarness) connectionValidator(_ context.Context, _ NLPipelineWorkflowV2Input, _ *WorkflowState) error {
	h.record("ConnectionValidatorActivityV2")
	return nil
}

func (h *hitlParkHarness) executor(_ context.Context, _ map[string]interface{}) (ExecutorNativeResult, error) {
	h.mu.Lock()
	h.executorN++
	n := h.executorN
	h.calls = append(h.calls, "ExecutorNativeActivity")
	h.mu.Unlock()
	return h.sc.executor(n), nil
}

func (h *hitlParkHarness) planRepair(_ context.Context, _, _ string, _ []string) (SchemaRepairPlan, error) {
	h.record("PlanSchemaDriftRepairActivity")
	return SchemaRepairPlan{
		Safe:             false,
		RequiresApproval: true,
		Reason:           "a column type change needs review",
		DDL:              []string{"ALTER TABLE orders ALTER COLUMN total TYPE numeric"},
	}, nil
}

func (h *hitlParkHarness) fetchTokenID(_ context.Context, _ string) (string, error) {
	h.record("FetchConnectionOAuthTokenIDActivity")
	return "", nil
}

func (h *hitlParkHarness) cleanup(_ context.Context, _, _, _ string, _ []string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.cleanupCalls++
	return nil
}

func (h *hitlParkHarness) stateUpdate(_ context.Context, in StateUpdateInput) error {
	h.mu.Lock()
	var hook func()
	if in.EventType == "PIPELINE_WAITING" && h.onWaiting != nil {
		hook, h.onWaiting = h.onWaiting, nil
	}
	h.mu.Unlock()
	if hook != nil {
		hook()
	}
	return nil
}

func (h *hitlParkHarness) pipelineStatus(_ context.Context, _ string, _ string, status string, _ string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.statusWrites = append(h.statusWrites, status)
	return nil
}

func (h *hitlParkHarness) domainEvent(_ context.Context, event map[string]interface{}) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	eventType, _ := event["event_type"].(string)
	h.domainEvents = append(h.domainEvents, eventType)
	if eventType == "PIPELINE_WAITING" {
		stage, _ := event["stage"].(string)
		blocking := ""
		if br, ok := event["blocking_reason"].(map[string]interface{}); ok {
			blocking, _ = br["type"].(string)
		}
		h.parks = append(h.parks, stage+"/"+blocking)
	}
	return nil
}

type hitlParkResult struct {
	err          error
	elapsed      time.Duration
	calls        []string
	executorN    int
	parks        []string
	domainEvents []string
	statusWrites []string
	cleanupCalls int
}

var hitlParkStart = time.Date(2026, 9, 2, 8, 0, 0, 0, time.UTC)

// runHITLPark runs the scenario. setup may register callbacks, version overrides
// or an onWaiting hook before the workflow starts.
func runHITLPark(t *testing.T, sc hitlParkScenario, setup func(env *testsuite.TestWorkflowEnvironment, h *hitlParkHarness)) hitlParkResult {
	t.Helper()
	h := &hitlParkHarness{sc: sc}

	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetStartTime(hitlParkStart)
	env.RegisterWorkflowWithOptions(NLPipelineWorkflowV2, workflow.RegisterOptions{Name: NLPipelineWorkflowV2Name})
	for name, fn := range map[string]interface{}{
		"IntentActivityV2":                    h.intent,
		"ConnectorResolverActivityV2":         h.resolver,
		"ConnectionValidationActivityV2":      h.connectionValidation,
		"ConnectionValidatorActivityV2":       h.connectionValidator,
		"ExecutorNativeActivity":              h.executor,
		"PlanSchemaDriftRepairActivity":       h.planRepair,
		"FetchConnectionOAuthTokenIDActivity": h.fetchTokenID,
		"CleanupPartialDataActivityV2":        h.cleanup,
		"StateUpdateActivity":                 h.stateUpdate,
		"UpdatePipelineStatusActivity":        h.pipelineStatus,
		"EmitDomainEventActivity":             h.domainEvent,
	} {
		env.RegisterActivityWithOptions(fn, activity.RegisterOptions{Name: name})
	}
	if setup != nil {
		setup(env, h)
	}

	env.ExecuteWorkflow(NLPipelineWorkflowV2, sc.input())
	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	return hitlParkResult{
		err:          env.GetWorkflowError(),
		elapsed:      env.Now().Sub(hitlParkStart),
		calls:        append([]string(nil), h.calls...),
		executorN:    h.executorN,
		parks:        append([]string(nil), h.parks...),
		domainEvents: append([]string(nil), h.domainEvents...),
		statusWrites: append([]string(nil), h.statusWrites...),
		cleanupCalls: h.cleanupCalls,
	}
}

// requireReachedPark is the denominator: the run announced exactly the park the
// scenario is about, via the activities that lead there.
func requireReachedPark(t *testing.T, sc hitlParkScenario, r hitlParkResult) {
	t.Helper()
	if len(r.parks) != 1 || r.parks[0] != sc.park {
		t.Fatalf("expected the run to announce exactly the %q park, got %v — the test proves nothing", sc.park, r.parks)
	}
	for _, want := range sc.wantCalls {
		if !containsString(r.calls, want) {
			t.Fatalf("%s never ran (calls %v), so this is not the %q park", want, r.calls, sc.park)
		}
	}
	if r.executorN != sc.executorCalls {
		t.Fatalf("executor dispatched %d times, want %d (calls %v)", r.executorN, sc.executorCalls, r.calls)
	}
}

// requireStoppedByUser: a user cancel ends the run as stopped with no error, no
// failure event and no partial-data cleanup.
func requireStoppedByUser(t *testing.T, r hitlParkResult) {
	t.Helper()
	if r.err != nil {
		t.Fatalf("a user cancel must not end the run with an error, got: %v", r.err)
	}
	if len(r.statusWrites) != 1 || r.statusWrites[0] != "stopped" {
		t.Fatalf("expected exactly one terminal status write of \"stopped\", got %v", r.statusWrites)
	}
	if containsString(r.domainEvents, "STAGE_FAILED") {
		t.Fatalf("a user cancel must not emit STAGE_FAILED (events %v)", r.domainEvents)
	}
	if r.cleanupCalls != 0 {
		t.Fatalf("a user cancel must not run partial-data cleanup, ran %d times", r.cleanupCalls)
	}
}

func agenticInput() NLPipelineWorkflowV2Input {
	return NLPipelineWorkflowV2Input{
		PipelineID:              "pipe-park-1",
		ExecutionID:             "exec-park-1",
		UserID:                  "user-1",
		Message:                 "copy my orders into the warehouse",
		SkipConnectorValidation: true,
		ExecutorDispatch:        ExecutorDispatchTemporal,
	}
}

func fastRerunParkInput() NLPipelineWorkflowV2Input {
	return NLPipelineWorkflowV2Input{
		PipelineID:              "pipe-park-1",
		ExecutionID:             "exec-park-1",
		UserID:                  "user-1",
		Message:                 "copy my orders into the warehouse",
		SourceConnectionID:      "src-1",
		DestinationConnectionID: "dst-1",
		SelectedTables:          []string{"orders"},
		ExecutorDispatch:        ExecutorDispatchTemporal,
	}
}

func executorFails(errText string) func(int) ExecutorNativeResult {
	return func(int) ExecutorNativeResult {
		return ExecutorNativeResult{Status: "failed", Error: errText}
	}
}

func noExecutor(int) ExecutorNativeResult {
	return ExecutorNativeResult{Status: "failed", Error: "the executor must not run in this scenario"}
}

var hitlParkScenarios = []hitlParkScenario{
	{
		name:  "connector generation",
		input: agenticInput,
		resolverErr: toTemporalError(NewPolicyError(PolicyCodeMissingConnectors, "a connector is missing", map[string]interface{}{
			"missing_connectors": []string{"dest-warehouse"},
		})),
		executor:   noExecutor,
		park:       "capability_resolver/connector_generation",
		waitingFor: "a generated connector",
		wantCalls:  []string{"IntentActivityV2", "ConnectorResolverActivityV2"},
	},
	{
		name:  "connection configuration during connector resolution",
		input: agenticInput,
		resolverErr: toTemporalError(NewPolicyError(PolicyCodeMissingConnections, "a connection is missing", map[string]interface{}{
			"missing_connections": []interface{}{map[string]interface{}{"connector_type": "dest-warehouse", "direction": "destination"}},
		})),
		executor:   noExecutor,
		park:       "connection_validation/connection_config",
		waitingFor: "connections to be configured",
		wantCalls:  []string{"IntentActivityV2", "ConnectorResolverActivityV2"},
	},
	{
		name:       "connection configuration before planning",
		input:      agenticInput,
		executor:   noExecutor,
		park:       "connection_validation/connection_config",
		waitingFor: "connections to be configured",
		wantCalls:  []string{"IntentActivityV2", "ConnectorResolverActivityV2", "ConnectionValidationActivityV2"},
	},
	{
		name:          "executor connection fix",
		input:         fastRerunParkInput,
		executor:      executorFails("invalid configuration: destination host is missing"),
		park:          "executor/connection_config",
		waitingFor:    "connections to be fixed",
		wantCalls:     []string{"ExecutorNativeActivity"},
		executorCalls: 1,
	},
	{
		name:          "executor connection fix after a failed token refresh",
		input:         fastRerunParkInput,
		executor:      executorFails("401 unauthorized from destination"),
		park:          "executor/connection_config",
		waitingFor:    "connections to be fixed",
		wantCalls:     []string{"ExecutorNativeActivity", "FetchConnectionOAuthTokenIDActivity"},
		executorCalls: 1,
	},
	{
		name:          "schema change approval",
		input:         fastRerunParkInput,
		executor:      executorFails("schema drift detected on orders"),
		park:          "executor/approval",
		waitingFor:    "the schema change to be approved",
		wantCalls:     []string{"ExecutorNativeActivity", "PlanSchemaDriftRepairActivity"},
		executorCalls: 1,
	},
}

func sendCancel(env *testsuite.TestWorkflowEnvironment) {
	env.SignalWorkflow(SignalCancel, map[string]interface{}{"action": "stop"})
}

// The user cancels while the run is parked: it ends at the cancel, as stopped.
func TestNLPipelineV2_CancelDuringEveryHITLParkEndsRunAsStopped(t *testing.T) {
	for _, sc := range hitlParkScenarios {
		sc := sc
		t.Run(sc.name, func(t *testing.T) {
			r := runHITLPark(t, sc, func(env *testsuite.TestWorkflowEnvironment, _ *hitlParkHarness) {
				env.RegisterDelayedCallback(func() { sendCancel(env) }, time.Hour)
			})
			requireReachedPark(t, sc, r)
			requireStoppedByUser(t, r)
			if r.elapsed >= 2*time.Hour {
				t.Fatalf("run ended %s after start; a cancel at 1h must not wait for the 24h deadline", r.elapsed)
			}
		})
	}
}

// The cancel lands while the park is still being announced, so state.WaitReason
// is already nil when the workflow reaches the wait.
func TestNLPipelineV2_CancelWhileAnnouncingEveryHITLParkEndsRunAsStopped(t *testing.T) {
	for _, sc := range hitlParkScenarios {
		sc := sc
		t.Run(sc.name, func(t *testing.T) {
			r := runHITLPark(t, sc, func(env *testsuite.TestWorkflowEnvironment, h *hitlParkHarness) {
				// SignalWorkflow only queues a callback, queued before this activity's
				// completion, so the workflow sees the cancel first.
				h.onWaiting = func() { sendCancel(env) }
			})
			requireReachedPark(t, sc, r)
			requireStoppedByUser(t, r)
			if r.elapsed >= time.Hour {
				t.Fatalf("run ended %s after start; it must end at the cancel", r.elapsed)
			}
		})
	}
}

// Control: with no cancel the same scenarios sit in their park until the 24h
// deadline and fail with a message naming what they waited for.
func TestNLPipelineV2_EveryHITLParkWithoutCancelTimesOutAsFailed(t *testing.T) {
	for _, sc := range hitlParkScenarios {
		sc := sc
		t.Run(sc.name, func(t *testing.T) {
			r := runHITLPark(t, sc, nil)
			requireReachedPark(t, sc, r)
			if r.err == nil || !strings.Contains(r.err.Error(), "waiting for "+sc.waitingFor) {
				t.Fatalf("expected the park's timeout error naming %q, got: %v", sc.waitingFor, r.err)
			}
			if len(r.statusWrites) != 1 || r.statusWrites[0] != "failed" {
				t.Fatalf("expected exactly one terminal status write of \"failed\", got %v", r.statusWrites)
			}
			if r.elapsed < 24*time.Hour {
				t.Fatalf("run ended %s after start, before the 24h park deadline", r.elapsed)
			}
		})
	}
}

// waitForConnectionFixAndValidate reports "connections fixed" with a nil error.
// A cancel must therefore come back non-nil, or the executor loop would carry on
// as if repaired. At the iteration cap that is not harmless: the loop top
// continues the cancelled run as a brand-new workflow before it ever checks the
// cancel. The executor asks for 24 continuation chunks first so the connection
// fix park is entered on the 25th and last iteration.
func TestNLPipelineV2_CancelDuringConnectionFixAtLoopCapDoesNotContinueAsNew(t *testing.T) {
	sc := hitlParkScenario{
		name:  "executor connection fix at the loop cap",
		input: fastRerunParkInput,
		executor: func(n int) ExecutorNativeResult {
			if n < 25 {
				return ExecutorNativeResult{Status: "needs_continuation", Output: map[string]interface{}{"chunk": n}}
			}
			return ExecutorNativeResult{Status: "failed", Error: "invalid configuration: destination host is missing"}
		},
		park:          "executor/connection_config",
		wantCalls:     []string{"ExecutorNativeActivity"},
		executorCalls: 25,
	}
	r := runHITLPark(t, sc, func(env *testsuite.TestWorkflowEnvironment, _ *hitlParkHarness) {
		env.RegisterDelayedCallback(func() { sendCancel(env) }, time.Hour)
	})
	requireReachedPark(t, sc, r)
	var can *workflow.ContinueAsNewError
	if errors.As(r.err, &can) {
		t.Fatalf("a cancelled run was continued as new (%v)", r.err)
	}
	requireStoppedByUser(t, r)
}

// Replay safety for the executor-recovery callers. Before hitl-wait-honours-cancel
// a cancel during this park did not end it, and when the user then configured
// connections anyway the recovery helper returned workflow.ErrCanceled, which its
// caller handled as a failed recovery: partial-data cleanup, STAGE_FAILED, and the
// cancel error. Those commands are in such histories, so on DefaultVersion the
// caller must still take that path; only new runs stop cleanly.
func TestNLPipelineV2_CancelDuringConnectionFixOnDefaultVersionKeepsRecordedFailurePath(t *testing.T) {
	for _, sc := range hitlParkScenarios {
		if sc.park != "executor/connection_config" {
			continue
		}
		sc := sc
		t.Run(sc.name, func(t *testing.T) {
			r := runHITLPark(t, sc, func(env *testsuite.TestWorkflowEnvironment, _ *hitlParkHarness) {
				env.OnGetVersion(hitlWaitHonoursCancelVersion, workflow.DefaultVersion, 1).Return(workflow.DefaultVersion)
				env.RegisterDelayedCallback(func() { sendCancel(env) }, time.Hour)
				env.RegisterDelayedCallback(func() {
					env.SignalWorkflow(SignalConnectionsConfigured, ConnectionsConfiguredPayload{
						SourceConnectionID:      "src-2",
						DestinationConnectionID: "dst-2",
					})
				}, 2*time.Hour)
			})
			requireReachedPark(t, sc, r)
			if r.elapsed < 2*time.Hour || r.elapsed >= 24*time.Hour {
				t.Fatalf("on DefaultVersion the park must ignore the 1h cancel and end on the 2h signal, ended after %s", r.elapsed)
			}
			var canceled *temporal.CanceledError
			if !errors.As(r.err, &canceled) {
				t.Fatalf("expected the recorded cancel error, got: %v", r.err)
			}
			if r.cleanupCalls != 1 {
				t.Fatalf("expected the recorded partial-data cleanup to run once, ran %d times", r.cleanupCalls)
			}
			if !containsString(r.domainEvents, "STAGE_FAILED") {
				t.Fatalf("expected the recorded STAGE_FAILED event, got %v", r.domainEvents)
			}
			if containsString(r.calls, "ConnectionValidatorActivityV2") {
				t.Fatalf("a cancelled run must not validate the new connections (calls %v)", r.calls)
			}
			if fmt.Sprint(r.statusWrites) != "[stopped]" {
				t.Fatalf("expected exactly one terminal status write of \"stopped\", got %v", r.statusWrites)
			}
		})
	}
}
