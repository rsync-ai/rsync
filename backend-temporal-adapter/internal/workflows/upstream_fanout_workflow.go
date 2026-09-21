package workflows

import (
	"context"
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// ============================================================================
// Upstream Fan-Out — one completion, one workflow, one child per model
// ============================================================================
// Migration 095 gave a model an event trigger and #982 made the REBUILD durable:
// ModelRefreshWorkflow owns the work, keyed "model-refresh:<saved_query_id>", and a
// completion arriving mid-rebuild coalesces into the run already doing it. #983 let a
// model wait on another model, so one completion can wake several.
//
// What stayed undurable was the FAN-OUT itself. The api-gateway looked up every model
// waiting on the thing that just finished and then, on a detached goroutine, signalled
// them one at a time. A gateway that restarted after the third of nine signals left six
// models unrebuilt, with no record that they were owed anything: the goroutine died with
// the process, the in-process fallback died with it too, and the next rebuild of those
// six was whenever their upstream happened to run again.
//
// So the gateway now makes ONE call — start this workflow, with the whole target list as
// its argument — and the fan-out becomes Temporal's problem the moment that call returns.
// The gateway can die immediately afterwards and all nine models are still signalled.
//
// WHY A CHILD PER MODEL RATHER THAN N ACTIVITIES IN THIS WORKFLOW.
// Each model's dispatch is independently retryable, independently visible, and
// independently terminable — a child execution named after the model is what an operator
// looks at when one model out of thirty stops updating. ParentClosePolicy ABANDON means a
// child still retrying outlives a parent that failed or was terminated, so cancelling a
// fan-out cannot half-deliver it.
//
// WHY THE CHILD IS NOT ModelRefreshWorkflow ITSELF.
// That is the shape this looks like it should have, and it does not work. A refresh loop
// must be reached by signal-with-start on the id "model-refresh:<saved_query_id>" — that
// atomicity is what makes two completions coalesce instead of the second losing the
// model's advisory run lock and being dropped (#982). ExecuteChildWorkflow cannot express
// signal-with-start; it starts a workflow, and a second parent starting a child on the
// same id collides. Worse, ModelRefreshWorkflow takes every unit of work from its signal
// channel and has no run timeout, so a child started without a signal would block on
// ch.Receive forever. Signal-with-start is a client-side primitive, so the only place a
// workflow can perform one is inside an activity — which is exactly what the child does.

// modelRefreshDispatchHorizon is how long one child keeps trying to deliver its signal.
//
// Ten minutes covers a Temporal frontend that is rate-limiting or briefly unhappy. It is
// deliberately not unbounded: past that, the honest answer is that this completion did
// not reach the model, and a child that retried for a day would still be trying when the
// upstream ran again and made it moot.
const modelRefreshDispatchHorizon = 10 * time.Minute

// UpstreamFanOutTarget is one model waiting on the completion.
//
// run_as_user_id is deliberately absent. The gateway's internal run endpoint re-resolves
// it from the schedule row on every call, so carrying it here would freeze a permission
// into a Temporal argument, where it would outlive the demotion that revoked it — the
// same reason ScheduledModelRunWorkflow passes no identity either.
type UpstreamFanOutTarget struct {
	ScheduleID   string `json:"schedule_id"`
	SavedQueryID string `json:"saved_query_id"`
}

// UpstreamFanOutInput is one completion and everything waiting on it.
//
// Every field is a wire contract with the argument the api-gateway builds by hand in
// saved_query_schedules.go; the two modules share no types.
type UpstreamFanOutInput struct {
	// UpstreamKind is "pipeline" or "model".
	UpstreamKind string `json:"upstream_kind"`
	UpstreamID   string `json:"upstream_id"`
	// ExecutionID identifies the pipeline execution that started the chain. Empty for a
	// model-sourced fan-out, which has no execution of its own.
	ExecutionID string `json:"execution_id,omitempty"`
	// UpstreamRunID is the upstream model's own saved_query_runs row, carried to the
	// rebuild so its history can point back at the run that woke it. Empty for a pipeline.
	UpstreamRunID string `json:"upstream_run_id,omitempty"`
	// Depth is how many model-to-model hops preceded this completion. It is passed down
	// to every child and into every signal because the gateway's depth bound is the only
	// thing that stops a rebuild ring, and a bound reset at any hop stops nothing.
	Depth   int                    `json:"depth,omitempty"`
	Targets []UpstreamFanOutTarget `json:"targets"`
}

// ModelRefreshDispatchInput is one child: deliver one completion to one model.
type ModelRefreshDispatchInput struct {
	SavedQueryID  string `json:"saved_query_id"`
	ScheduleID    string `json:"schedule_id"`
	UpstreamKind  string `json:"upstream_kind"`
	UpstreamID    string `json:"upstream_id"`
	ExecutionID   string `json:"execution_id,omitempty"`
	UpstreamRunID string `json:"upstream_run_id,omitempty"`
	Depth         int    `json:"depth,omitempty"`
}

// ModelRefreshWorkflowID is the one place a refresh loop's id is spelled. One workflow
// per model — that is what makes two completions coalesce rather than race.
func ModelRefreshWorkflowID(savedQueryID string) string {
	return "model-refresh:" + savedQueryID
}

// modelRefreshDispatchWorkflowID names one child.
//
// Derived from the parent's own id, which already carries the completion that caused it,
// so two completions of the same upstream give the same model two different children and
// neither is refused as a duplicate of the other.
func modelRefreshDispatchWorkflowID(parentWorkflowID, savedQueryID string) string {
	return parentWorkflowID + ":" + savedQueryID
}

// UpstreamFanOutWorkflow starts one dispatch child per downstream model and waits for
// all of them.
//
// It waits for completion rather than only for the children to start — the departure from
// ScheduledPipelineRunWorkflow, which starts its child and returns. There the child is a
// pipeline run that can take hours and whose outcome the wrapper has no use for. Here the
// child is one signal with a ten-minute ceiling, and its outcome is the only thing the
// fan-out is about: waiting is what makes "this completion reached four of five models"
// a fact in one history instead of something to be reassembled from five.
func UpstreamFanOutWorkflow(ctx workflow.Context, input UpstreamFanOutInput) error {
	logger := workflow.GetLogger(ctx)
	info := workflow.GetInfo(ctx)

	type dispatch struct {
		savedQueryID string
		future       workflow.ChildWorkflowFuture
	}

	var started []dispatch
	var confirmed, failed []string

	// Registered before the first child is started. Placing it after the dispatch loop
	// would leave the fan-out unobservable for exactly the stretch in which starting a
	// child can itself be slow, which is one of the things worth observing.
	//
	// NotYetConfirmed is never "still running". The await loop below walks `started` in
	// order, so a child that finished out of turn stays in that list until the loop
	// reaches it. This reports what this fan-out has confirmed, and nothing about child
	// state — a caller that needs the truth about one child composes
	// ChildWorkflowIDPrefix + ":" + saved_query_id and asks it directly.
	if err := workflow.SetQueryHandler(ctx, FanOutStateQuery, func() (FanOutState, error) {
		st := FanOutState{
			UpstreamKind:          input.UpstreamKind,
			UpstreamID:            input.UpstreamID,
			ExecutionID:           input.ExecutionID,
			Depth:                 input.Depth,
			Total:                 len(started),
			Confirmed:             append([]string(nil), confirmed...),
			Failed:                append([]string(nil), failed...),
			ChildWorkflowIDPrefix: info.WorkflowExecution.ID,
		}
		settled := make(map[string]bool, len(confirmed)+len(failed))
		for _, id := range confirmed {
			settled[id] = true
		}
		for _, id := range failed {
			settled[id] = true
		}
		for _, d := range started {
			if !settled[d.savedQueryID] {
				st.NotYetConfirmed = append(st.NotYetConfirmed, d.savedQueryID)
			}
		}
		return st, nil
	}); err != nil {
		logger.Error("could not register the upstream fan-out query handler; this fan-out runs on unobserved",
			"upstream_id", input.UpstreamID, "error", err)
	}

	seen := make(map[string]bool, len(input.Targets))
	for _, t := range input.Targets {
		if t.SavedQueryID == "" {
			// Nothing to address. Skipping is right rather than failing the fan-out: the
			// other targets are unaffected, and a child with no model would fail anyway.
			logger.Warn("upstream fan-out: target has no saved_query_id, skipping",
				"upstream_id", input.UpstreamID, "schedule_id", t.ScheduleID)
			continue
		}
		if seen[t.SavedQueryID] {
			// Two targets naming one model would collide on the child id and the second
			// start would be refused, failing a fan-out that had in fact delivered
			// everything. Collapsing here is also simply correct — one completion owes a
			// model one rebuild however many rows say so.
			logger.Warn("upstream fan-out: model listed twice for one completion, dispatching once",
				"upstream_id", input.UpstreamID, "saved_query_id", t.SavedQueryID)
			continue
		}
		seen[t.SavedQueryID] = true

		cctx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			WorkflowID: modelRefreshDispatchWorkflowID(info.WorkflowExecution.ID, t.SavedQueryID),
			TaskQueue:  info.TaskQueueName,
			// The child outlives its parent. A parent that fails or is terminated must not
			// take a still-retrying dispatch down with it, because that is exactly the
			// half-delivered fan-out this workflow exists to make impossible.
			ParentClosePolicy: enumspb.PARENT_CLOSE_POLICY_ABANDON,
		})
		started = append(started, dispatch{
			savedQueryID: t.SavedQueryID,
			future: workflow.ExecuteChildWorkflow(cctx, ModelRefreshDispatchWorkflow, ModelRefreshDispatchInput{
				SavedQueryID:  t.SavedQueryID,
				ScheduleID:    t.ScheduleID,
				UpstreamKind:  input.UpstreamKind,
				UpstreamID:    input.UpstreamID,
				ExecutionID:   input.ExecutionID,
				UpstreamRunID: input.UpstreamRunID,
				Depth:         input.Depth,
			}),
		})
	}

	if len(started) == 0 {
		logger.Info("upstream fan-out: nothing to dispatch",
			"upstream_kind", input.UpstreamKind, "upstream_id", input.UpstreamID)
		return nil
	}

	logger.Info("⚡ upstream fan-out started",
		"upstream_kind", input.UpstreamKind,
		"upstream_id", input.UpstreamID,
		"execution_id", input.ExecutionID,
		"depth", input.Depth,
		"models", len(started))

	// Started in one pass above and awaited in a second, so the children run in parallel.
	// Awaiting inside the first loop would serialize the starts and make a fan-out to
	// thirty models thirty round trips deep.
	for _, d := range started {
		if err := d.future.Get(ctx, nil); err != nil {
			// One model's dispatch failing must not abandon the others' — they have already
			// been started, and this loop only collects what happened.
			logger.Error("upstream fan-out: a model could not be signalled",
				"saved_query_id", d.savedQueryID, "upstream_id", input.UpstreamID, "error", err)
			failed = append(failed, d.savedQueryID)
			continue
		}
		// Kept apart from failed deliberately. Collapsing the two would turn a fan-out
		// that reached four models of five into one that reads as still working, which is
		// the half-delivered failure this workflow exists to make visible.
		confirmed = append(confirmed, d.savedQueryID)
	}
	if len(failed) > 0 {
		return fmt.Errorf("upstream %s %s: %d of %d model refreshes could not be dispatched: %v",
			input.UpstreamKind, input.UpstreamID, len(failed), len(started), failed)
	}

	logger.Info("✅ upstream fan-out delivered", "upstream_id", input.UpstreamID, "models", len(started))
	return nil
}

// ModelRefreshDispatchWorkflow delivers one completion to one model's refresh loop.
//
// A workflow wrapping a single activity looks like ceremony, and the ceremony is the
// point: it is a durable, named, independently retrying execution per model, which is
// what lets the parent close on a fixed budget while a model whose signal is being
// rate-limited keeps trying under ParentClosePolicy ABANDON.
func ModelRefreshDispatchWorkflow(ctx workflow.Context, input ModelRefreshDispatchInput) error {
	actx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		// One RPC. A signal-with-start that has not answered in thirty seconds is not
		// going to; retrying is cheaper than waiting.
		StartToCloseTimeout: 30 * time.Second,
		// The retry budget for the whole dispatch. MaximumAttempts is left unset — the
		// horizon, not a count, is what says how long this completion is still worth
		// delivering, and a count would expire in seconds under a short backoff.
		ScheduleToCloseTimeout: modelRefreshDispatchHorizon,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    30 * time.Second,
		},
	})
	return workflow.ExecuteActivity(actx, SignalModelRefreshActivity, input).Get(ctx, nil)
}

// SignalModelRefreshActivity signal-with-starts the model's refresh loop.
//
// This is the one operation in the fan-out that has to be an activity: signal-with-start
// is atomic only on the client, and that atomicity is what makes a completion arriving
// mid-rebuild coalesce into the run already working instead of being refused by the
// model's advisory run lock and dropped.
//
// The workflow argument and signal payload are typed here, not hand-built maps, because
// unlike the api-gateway this code lives in the same module as ModelRefreshWorkflow.
// Moving the signal to this side removed the gateway's hand-built payload, and with it
// the class of bug where one end renames a field and nothing fails until a rebuild
// silently arrives with depth 0.
func SignalModelRefreshActivity(ctx context.Context, input ModelRefreshDispatchInput) error {
	if input.SavedQueryID == "" {
		// Retrying cannot fix a dispatch with no model. Fail the child now rather than
		// burning the full horizon on it.
		return temporal.NewNonRetryableApplicationError(
			"model refresh dispatch carries no saved_query_id", "InvalidDispatch", nil)
	}

	tc := activityTemporalClient()
	if tc == nil {
		// Retryable on purpose. A worker booted before SetTemporalClient ran would
		// otherwise drop every dispatch it was handed, silently.
		return fmt.Errorf("temporal client is not configured on this worker; cannot signal %s",
			ModelRefreshWorkflowID(input.SavedQueryID))
	}

	id := ModelRefreshWorkflowID(input.SavedQueryID)
	_, err := tc.SignalWithStartWorkflow(ctx, id, ModelRefreshSignalName,
		ModelRefreshRequest{
			ScheduleID:    input.ScheduleID,
			UpstreamKind:  input.UpstreamKind,
			UpstreamID:    input.UpstreamID,
			ExecutionID:   input.ExecutionID,
			UpstreamRunID: input.UpstreamRunID,
			Depth:         input.Depth,
		},
		client.StartWorkflowOptions{
			ID: id,
			// The queue this activity is already running on, so a fan-out cannot be
			// dispatched onto a queue no worker polls.
			TaskQueue: activity.GetInfo(ctx).TaskQueue,
			// No run timeout on purpose. The refresh loop lives as long as completions keep
			// arriving and ends itself when they stop; a run timeout would kill a rebuild
			// that happened to still be in flight when the clock ran out.
		},
		ModelRefreshWorkflow,
		ModelRefreshInput{SavedQueryID: input.SavedQueryID},
	)
	return err
}
