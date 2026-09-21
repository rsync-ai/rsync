package workflows

import (
	"testing"

	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// The replay guard.
//
// The freshness sweep is a singleton started with USE_EXISTING and bounded at 500 ticks,
// so at a minute an interval one run lives about eight hours. Deploying this change does
// not restart it — the run that was going keeps going, and the new code replays its
// existing history to catch up. Anything in that change that emits a command would
// diverge from what the history records.
//
// What makes that worth a checked-in fixture rather than a code review is the failure
// mode. The default WorkflowPanicPolicy is BlockWorkflow, and neither worker in this repo
// overrides it. A non-deterministic workflow task fails once, is ignored on every later
// attempt, times out, and is retried by the server forever while the run sits RUNNING.
// There is no DLQ, no failure callback and no alert. The staleness monitor would simply
// stop, which is the most reassuring possible way for a staleness monitor to be broken.
//
// workflow.SetQueryHandler is safe here precisely because it emits no command and writes
// no history event. workflow.UpsertTypedSearchAttributes — the other way this feature
// could have been built — is a command, and replacing the handler with one makes this
// test fail with TMPRL1100. That is one of the two reasons the feature is queries.
//
// The fixture is the first 25 events of the real singleton as it was running before this
// change, truncated so the last event is WorkflowTaskStarted: ending on a
// WorkflowTaskCompleted would make the replayer compare commands for a task the workflow
// has not finished producing, and the test would fail for unmodified code. It covers the
// workflow's whole shape — the start, the first sleep, two sweep attempts and the sleeps
// between them. The activity fails in it because the stack it was taken from has no
// INTERNAL_SERVICE_SECRET; that is the more interesting path to replay anyway, since it
// is the one the new state assignments sit on.
func TestTheFreshnessSweepReplaysCleanAgainstAPreChangeHistory(t *testing.T) {
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(ModelFreshnessWorkflow, workflow.RegisterOptions{
		// Spelled out because the history records a type name and this test is about
		// that name, not because the registration would otherwise miss. It would not:
		// getFunctionName returns the SHORT Go name, so a plain RegisterWorkflow already
		// keys this under "ModelFreshnessWorkflow", and passing a Name additionally
		// records an alias FROM the short function name TO it — which is why registering
		// under a name absent from the fixture still resolves and still replays. Do not
		// read a passing test here as evidence that the name matched; the proof that
		// this guard is not vacuous is the mutation run, where a second workflow
		// registered under this name, a missing fixture file and one extra command
		// emitted before the first timer each fail it.
		Name: "ModelFreshnessWorkflow",
	})

	err := replayer.ReplayWorkflowHistoryFromJSONFile(nil, "testdata/model_freshness_pre_query_history.json")
	if err != nil {
		t.Fatalf("the sweep does not replay against the history it was running on: %v\n\n"+
			"A running singleton would wedge on this: the task fails, is ignored on retry, "+
			"times out, and the server retries it forever with the run still RUNNING and "+
			"nothing reporting a fault.", err)
	}
}
