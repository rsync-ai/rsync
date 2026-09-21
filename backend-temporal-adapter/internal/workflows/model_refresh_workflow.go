package workflows

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// ============================================================================
// Model Refresh Workflow — the after-pipeline trigger, made durable
// ============================================================================
// The event trigger from migration 095 ("rebuild this model after that pipeline
// runs") used to be a detached goroutine inside the api-gateway, started from the
// event projector's consume loop. That works, and it still runs on any deployment
// with no Temporal, but it loses three things a rebuild wants:
//
//   - Durability. A gateway restart mid-rebuild drops the work with one log line.
//     Nothing retries it; the next completion of the upstream pipeline is the only
//     retry, and that may be a day away.
//   - Serialization that keeps the work. Two completions of the same pipeline in
//     quick succession started two goroutines, and the second lost the model's
//     advisory run lock (acquireModelRunLock, api-gateway saved_query_models.go).
//     That refusal is correct — it is what stops two DROP/CREATE sequences sharing
//     one scratch table — but the refused completion is then simply gone: the run
//     returns "skipped", nothing queues it, and the model keeps serving data from
//     before that completion until the pipeline happens to finish again.
//   - A record. One log line is the whole history of an event rebuild.
//
// One workflow per model, keyed "model-refresh:<saved_query_id>", fixes all three
// at once: signal-with-start means a completion arriving while a rebuild is in
// flight is delivered to the run already doing the work instead of being refused
// by the run lock and dropped.
//
// COALESCING IS CORRECT HERE, and that is a property of what a model is rather than
// a shortcut. A rebuild reads the current state of the upstream tables and replaces
// the target wholesale — it is not an increment applied to a delta. So collapsing
// three completions that landed inside one rebuild into one further rebuild produces
// exactly what running three would have, and produces it sooner.
//
// EVERY UNIT OF WORK COMES FROM THE SIGNAL CHANNEL, never from the workflow argument.
// A signal-with-start that actually starts the run buffers the signal and delivers it
// to this loop; treating the argument as work too would rebuild twice for the first
// completion and only the first. The argument names the model and nothing else.

// ModelRefreshSignalName is the signal an api-gateway sends when something this model
// is triggered by has completed.
const ModelRefreshSignalName = "model_refresh_requested"

// ModelRefreshTrigger is the value sent to the gateway's internal run endpoint to
// select the event door rather than the clock door. It is the schedule_type of the
// rows this workflow is allowed to act on (migration 100, which renamed it from
// after_pipeline when a model became a thing another model can wait on).
//
// The two modules share no types, so this string is a wire contract with the gateway's
// scheduleAfterUpstream and has to be changed on both sides together.
const ModelRefreshTrigger = "after_upstream"

// modelRefreshIdleWindow is how long the loop waits for another completion before
// concluding the burst is over and finishing.
//
// It is not a debounce — the rebuild has already happened by the time this starts.
// It exists so a burst that straddles the end of a rebuild still collapses: a
// thirty-table nightly load emits completions over a minute or two, and without it
// each late completion pays a fresh workflow start.
const modelRefreshIdleWindow = 30 * time.Second

// modelRefreshesPerRun bounds one run's history before continuing as new. A refresh
// is a handful of events, so this is a long way below any history limit; it is here
// so a model wired to a minutely pipeline cannot grow an unbounded history over
// weeks of never going idle.
const modelRefreshesPerRun = 100

// ModelRefreshRequest is one signal: something this model waits on finished.
//
// The upstream and execution ids are carried for the workflow history rather than for
// the rebuild, which does not depend on them — the model reads whatever the tables now
// hold. They are what makes the history answer "which run of what caused this rebuild",
// which the log line it replaces could not.
//
// Every field is a wire contract with the gateway's signal payload, which is built by
// hand in saved_query_schedules.go because the two modules share no types. A signal sent
// by a gateway older than migration 100 carries pipeline_id instead of upstream_kind and
// upstream_id, so it deserializes to empty strings and depth 0 — the rebuild still
// happens, and only the history line is thinner.
type ModelRefreshRequest struct {
	ScheduleID string `json:"schedule_id"`
	// UpstreamKind is "pipeline" or "model".
	UpstreamKind string `json:"upstream_kind"`
	UpstreamID   string `json:"upstream_id"`
	ExecutionID  string `json:"execution_id"`
	// UpstreamRunID is the upstream model's run row; empty for a pipeline, and from a
	// gateway that predates migration 104.
	UpstreamRunID string `json:"upstream_run_id,omitempty"`
	// Depth is how many model-to-model hops the chain has already taken. Carried across
	// the Temporal hop rather than recomputed because the gateway's depth bound is the
	// only thing stopping a rebuild ring, and a bound that reset at every signal would
	// stop nothing at all.
	Depth int `json:"depth,omitempty"`
}

// ModelRefreshInput is the workflow argument. It names the model and carries the
// queue handed over at continue-as-new; it is never itself a unit of work.
type ModelRefreshInput struct {
	SavedQueryID string `json:"saved_query_id"`
	// Carried is what the previous run had buffered when it hit its history bound.
	// The Go SDK does not move unhandled signals across continue-as-new, so they are
	// drained and passed explicitly — dropping them would lose exactly the completions
	// that arrived during the busiest possible moment.
	Carried []ModelRefreshRequest `json:"carried,omitempty"`
}

// ModelRefreshWorkflow rebuilds one model, once per burst of upstream completions,
// for as long as completions keep arriving.
func ModelRefreshWorkflow(ctx workflow.Context, input ModelRefreshInput) error {
	logger := workflow.GetLogger(ctx)
	ch := workflow.GetSignalChannel(ctx, ModelRefreshSignalName)

	// Same options as the clock path, and the same activity: a rebuild is a rebuild
	// whichever door woke it, and two sets of timeouts for one operation would be two
	// things to keep in step.
	actx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 35 * time.Minute,
		HeartbeatTimeout:    35 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    5 * time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    time.Minute,
			// Three attempts covers a gateway restart or a brief network fault. A
			// failing query is not retried at all: the endpoint answers 200 with status
			// "failed" for anything the engine rejected on its merits.
			MaximumAttempts: 3,
		},
	})

	pending := append([]ModelRefreshRequest(nil), input.Carried...)
	done := 0
	phase := ModelRefreshPhaseWaiting
	var current *ModelRefreshCurrent
	lastErr := ""

	// Registered before ch.Receive, which this loop reaches within a few statements and
	// sits in almost all of the time. A handler registered after it would exist only in
	// the brief windows between signals, so every healthy model would report that its
	// state is unavailable — the opposite of what the handler is for.
	//
	// It reads the loop's own locals rather than maintaining a parallel copy. A second
	// copy is a second thing that can drift from what the loop is actually doing, and the
	// value of this answer is entirely that it is the loop's own.
	if err := workflow.SetQueryHandler(ctx, ModelRefreshStateQuery, func() (ModelRefreshState, error) {
		st := ModelRefreshState{
			SavedQueryID: input.SavedQueryID,
			Phase:        phase,
			// ch.Len(), not len(pending). The loop drains the channel into `pending` and
			// empties `pending` into `batch` without blocking in between, so `pending` is
			// empty at every instant a query can run — reporting it would be a field that
			// is structurally always zero. The channel is where a backlog actually sits.
			// Len() reads a buffer length: no command, nothing consumed, replay-safe.
			QueuedCompletions: ch.Len() + len(pending),
			RefreshesThisRun:  done,
			RefreshesPerRun:   modelRefreshesPerRun,
			LastError:         lastErr,
		}
		if current != nil {
			c := *current
			st.Current = &c
		}
		return st, nil
	}); err != nil {
		// Logged, not returned. A rebuild that runs unobserved is far better than one
		// that does not run because its introspection could not be set up.
		logger.Error("could not register the model refresh query handler; this loop runs on unobserved",
			"saved_query_id", input.SavedQueryID, "error", err)
	}

	for {
		if len(pending) == 0 {
			// Block. On a fresh run this is where the signal-with-start's own signal
			// arrives; there is no other way in.
			var req ModelRefreshRequest
			ch.Receive(ctx, &req)
			pending = append(pending, req)
		}
		drainModelRefreshRequests(ch, &pending)

		batch := pending
		pending = nil
		// The most recent request wins. Every request in a batch names the same
		// schedule — one live schedule per saved query is a unique index, not a
		// convention (085_saved_query_models.sql) — so this picks a spelling, not a
		// behaviour, and stays correct if that ever stops being true.
		latest := batch[len(batch)-1]

		// Depth is the one field the latest request does NOT get to decide. A batch can
		// mix a shallow chain with a deep one — two producers of the same model, reached
		// by routes of different length — and taking the latest would let coalescing
		// launder a chain that is eight hops deep into one that reports zero. The bound
		// is only worth having if collapsing requests cannot lower it.
		depth := latest.Depth
		for _, req := range batch {
			if req.Depth > depth {
				depth = req.Depth
			}
		}

		logger.Info("🔁 rebuilding model after upstream completion",
			"saved_query_id", input.SavedQueryID,
			"schedule_id", latest.ScheduleID,
			"upstream_kind", latest.UpstreamKind,
			"upstream_id", latest.UpstreamID,
			"execution_id", latest.ExecutionID,
			"depth", depth,
			"coalesced", len(batch))

		// The batch max, never latest.Depth — the same rule the depth computation above
		// exists for. A handler reporting the laundered number would reintroduce through
		// a new door the exact bug that loop was written to avoid.
		current = &ModelRefreshCurrent{
			ScheduleID:   latest.ScheduleID,
			UpstreamKind: latest.UpstreamKind,
			UpstreamID:   latest.UpstreamID,
			ExecutionID:  latest.ExecutionID,
			Depth:        depth,
			Coalesced:    len(batch),
			StartedAt:    workflow.Now(ctx).UTC().Format(time.RFC3339),
		}
		phase = ModelRefreshPhaseRebuilding

		var result modelRunActivityResult
		// Provenance is the latest request's, like the ids: the history row names one
		// upstream, and the coalesced count says how many more this rebuild absorbed.
		runInput := ScheduledModelRunInput{
			ScheduleID:    latest.ScheduleID,
			SavedQueryID:  input.SavedQueryID,
			Trigger:       ModelRefreshTrigger,
			Depth:         depth,
			UpstreamKind:  latest.UpstreamKind,
			UpstreamID:    latest.UpstreamID,
			UpstreamRunID: latest.UpstreamRunID,
			ExecutionID:   latest.ExecutionID,
			Coalesced:     len(batch),
		}
		// Workflow time: the failure record below is deduped on it, so it has to be the
		// same instant on every replay and every delivery.
		runStartedAt := workflow.Now(ctx)
		if err := workflow.ExecuteActivity(actx, RunModelActivity, runInput).Get(ctx, &result); err != nil {
			// Retries are spent. Do NOT fail the workflow: a later completion of the
			// upstream pipeline is a better retry than a replay of this one, because it
			// carries fresher data — and failing here would take the buffered requests
			// down with it.
			logger.Error("model refresh failed after retries",
				"saved_query_id", input.SavedQueryID, "error", err)
			// The loop deliberately survives a spent retry budget, so without this the
			// failure is one log line and the next query reports a model that looks idle
			// and well.
			lastErr = err.Error()
			// And the query only reaches someone who asks Temporal. The schedule page
			// reads run history, which the gateway writes only for runs that reached it;
			// this one never came back. Best-effort: the loop carries on either way.
			recordModelRunFailure(ctx, runInput, runStartedAt, err)
		} else {
			switch result.Status {
			case "succeeded":
				logger.Info("✅ model rebuilt",
					"saved_query_id", input.SavedQueryID, "target_table", result.TargetTable)
			case "skipped":
				// Includes every auto-pause. The trigger is already inert by the time this
				// returns, so this is the last rebuild — say why in the history, which is
				// where an operator looks when a model stops updating.
				logger.Warn("⏭️  model refresh skipped",
					"saved_query_id", input.SavedQueryID,
					"skip_reason", result.SkipReason,
					"reason", firstNonEmpty(result.AutoPauseReason, result.Error, result.Reason))
			default:
				logger.Warn("❌ model refresh failed",
					"saved_query_id", input.SavedQueryID, "error", result.Error)
			}
		}
		done++
		phase = ModelRefreshPhaseWaiting
		current = nil

		if !waitForModelRefresh(ctx, ch, &pending) {
			// Nothing more arrived. Finishing is the right end state rather than parking
			// forever: the next completion signal-with-starts a fresh run, and an open
			// workflow per model with no work to do is a cost with no reader.
			logger.Info("💤 model refresh loop idle, finishing",
				"saved_query_id", input.SavedQueryID, "refreshes", done)
			return nil
		}

		if done >= modelRefreshesPerRun {
			drainModelRefreshRequests(ch, &pending)
			return workflow.NewContinueAsNewError(ctx, ModelRefreshWorkflow, ModelRefreshInput{
				SavedQueryID: input.SavedQueryID,
				Carried:      pending,
			})
		}
	}
}

// drainModelRefreshRequests moves every already-delivered signal onto the queue
// without blocking. This is the coalescing: everything that landed while a rebuild
// was running becomes one further rebuild.
func drainModelRefreshRequests(ch workflow.ReceiveChannel, pending *[]ModelRefreshRequest) {
	for {
		var req ModelRefreshRequest
		if !ch.ReceiveAsync(&req) {
			return
		}
		*pending = append(*pending, req)
	}
}

// waitForModelRefresh waits out the idle window. It reports whether another
// completion arrived — false means the burst is over.
func waitForModelRefresh(ctx workflow.Context, ch workflow.ReceiveChannel, pending *[]ModelRefreshRequest) bool {
	// The timer is cancelled on the way out so a run that ends on a signal does not
	// leave a pending timer holding the workflow open for the rest of the window.
	timerCtx, cancelTimer := workflow.WithCancel(ctx)
	defer cancelTimer()

	arrived := false
	sel := workflow.NewSelector(ctx)
	sel.AddReceive(ch, func(c workflow.ReceiveChannel, _ bool) {
		var req ModelRefreshRequest
		c.Receive(ctx, &req)
		*pending = append(*pending, req)
		arrived = true
	})
	sel.AddFuture(workflow.NewTimer(timerCtx, modelRefreshIdleWindow), func(workflow.Future) {})
	sel.Select(ctx)

	if arrived {
		drainModelRefreshRequests(ch, pending)
	}
	return arrived
}
