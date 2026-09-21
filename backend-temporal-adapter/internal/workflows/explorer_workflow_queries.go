package workflows

// ============================================================================
// Explorer workflow queries — asking a running workflow what it is doing
// ============================================================================
//
// Everything the explorer knows about a rebuild, it learns after the rebuild ends.
// saved_query_runs cannot hold an in-flight attempt: its status CHECK admits only
// succeeded, failed and skipped, finished_at is NOT NULL, and the gateway writes the
// row once with started_at and finished_at together. So "is this model rebuilding
// right now" has no answer in Postgres, and neither do the two questions behind it —
// what is buffered waiting on the rebuild in flight, and which upstream caused it.
//
// A query handler answers from the workflow's own memory. It is the only mechanism
// that can: the facts are process state, not a record, and they are gone the moment
// the run ends.
//
// WHY QUERIES AND NOT SEARCH ATTRIBUTES. The obvious alternative — tag each run with
// its workspace and model and filter in the Temporal UI — cannot work on any surface
// this repo ships. All four set DB=postgresql, which selects Temporal's STANDARD
// visibility schema: executions_visibility has twelve columns and no search_attributes
// among them. A custom attribute registers with exit 0, prints "Search attributes have
// been added", appears in `temporal operator search-attribute list`, and every filter
// naming it fails with "filter by 'X' not supported for standard visibility". Custom
// attributes on SQL visibility need the advanced schema (DB=postgres12), which is a
// migration against every existing volume and needs btree_gin, blocked by default on
// Azure Flexible Server, Cloud SQL and RDS. Nobody needs UI filtering that much.
//
// None of that would matter if the mechanism were merely useless. It is worse: an
// unregistered key set at start time fails StartWorkflowExecution outright, so one
// missed registration stops every scheduled rebuild, and the symptom is staleness
// days later. A query handler has no registration step at all — a query type exists
// because a worker registered a handler in-process.
//
// THREE RULES, EACH LOAD-BEARING.
//
//  1. REGISTER AT THE TOP, before any blocking call. A handler registered after the
//     first block exists only in the windows between blocks, which for
//     ModelRefreshWorkflow — parked in ch.Receive almost always — means close to
//     never. The guard is TestModelRefreshStateIsQueryableBeforeTheFirstSignalArrives,
//     not review.
//
//  2. READ ONLY, AND NEVER BLOCK. The SDK forces read-only for the duration of a
//     handler, so a mutation is refused rather than silently applied. A panic is the
//     real hazard: it surfaces as a failed query, which the gateway renders as
//     "unreachable" — a hole that looks like a network fault.
//
//  3. NO COMMAND, EVER. SetQueryHandler appends nothing to the decision state machine
//     and writes no history event, so adding one to a workflow mid-flight replays
//     clean. This matters more here than anywhere else in the repo: the freshness
//     singleton continues as new every 500 ticks of a 60-second interval, so there is
//     no deploy window in which it is not mid-run. workflow.UpsertTypedSearchAttributes
//     would have been a command, and under the default BlockWorkflow panic policy —
//     which neither worker in this repo overrides — a non-deterministic workflow task
//     fails, is ignored on later attempts, times out, and is retried by the server
//     forever while the run sits RUNNING. No DLQ, no failure callback, no alert. A
//     model would simply stop refreshing. That is the same shape
//     SweepModelFreshnessActivity already calls out as the most reassuring possible
//     way for a staleness monitor to be broken, and it would have been introduced by
//     the observability feature.

// The query names. Each is a wire contract with the api-gateway, which spells them
// again in saved_query_explorer_running.go because the two modules share no types —
// the same arrangement ModelRefreshTrigger already lives under. A census test on each
// side asserts the set it knows is the set that exists.
const (
	// ModelRefreshStateQuery asks one model's refresh loop what it is doing.
	ModelRefreshStateQuery = "model_refresh_state"
	// FreshnessSweepStateQuery asks the singleton sweep whether it is alive, on what
	// interval, and what happened on its last tick.
	FreshnessSweepStateQuery = "freshness_sweep_state"
	// FanOutStateQuery asks one fan-out which models it has confirmed.
	FanOutStateQuery = "fanout_state"
)

// ModelRefreshCurrent describes the rebuild in flight.
//
// Coalesced is the size of the batch this rebuild is standing in for, which is the
// number saved_query_runs will under-report by when the row is finally written.
type ModelRefreshCurrent struct {
	ScheduleID   string `json:"schedule_id"`
	UpstreamKind string `json:"upstream_kind"`
	UpstreamID   string `json:"upstream_id"`
	ExecutionID  string `json:"execution_id,omitempty"`
	Depth        int    `json:"depth"`
	Coalesced    int    `json:"coalesced"`
	StartedAt    string `json:"started_at"`
}

// Phases of a refresh loop. Two, because the loop has exactly two states: parked on
// the signal channel, or inside the rebuild activity.
const (
	ModelRefreshPhaseWaiting    = "waiting"
	ModelRefreshPhaseRebuilding = "rebuilding"
)

// ModelRefreshState is the answer to ModelRefreshStateQuery.
type ModelRefreshState struct {
	SavedQueryID string `json:"saved_query_id"`
	// Phase is waiting or rebuilding. Waiting is the overwhelmingly common answer and
	// is not a fault: the loop finishes when a burst ends, so most models have no run
	// at all, and the gateway reports that as idle rather than querying anything.
	Phase string `json:"phase"`
	// QueuedCompletions is how many upstream completions have been delivered and not
	// yet rebuilt. It is read from the signal channel itself rather than from the
	// loop's own slice, which is the only honest source: the loop drains the channel
	// and empties the slice in the same unblocked stretch, so a query can never
	// observe the slice holding anything. Three completions that coalesce into one
	// rebuild write ONE row to saved_query_runs and the other two leave no trace
	// anywhere — not a run, not an audit row, not a log line of their own. This
	// number and Current.Coalesced are the only places that backlog exists.
	QueuedCompletions int `json:"queued_completions"`
	// Current is nil while waiting.
	Current *ModelRefreshCurrent `json:"current"`
	// RefreshesThisRun resets at continue-as-new, which is why RefreshesPerRun is
	// reported beside it: the pair says how close this run is to handing over.
	RefreshesThisRun int `json:"refreshes_this_run"`
	RefreshesPerRun  int `json:"refreshes_per_run"`
	// LastError is the last rebuild failure this run survived. The loop deliberately
	// does not fail on a spent retry budget — that would take the buffered queue down
	// with it — so without this the failure is a log line and nothing more.
	LastError string `json:"last_error,omitempty"`
}

// FreshnessSweepLastResult is what the last completed sweep counted.
type FreshnessSweepLastResult struct {
	Scanned  int `json:"scanned"`
	Opened   int `json:"opened"`
	Resolved int `json:"resolved"`
	Failed   int `json:"failed"`
}

// FreshnessSweepState is the answer to FreshnessSweepStateQuery.
//
// Two gaps, and both make a dead monitor look like a healthy system.
//
// The interval is frozen into a USE_EXISTING singleton's argument, so changing
// MODEL_FRESHNESS_SWEEP_SECONDS and restarting the adapter changes nothing: the
// existing run keeps its old interval and no error is raised anywhere. The compose
// file and the running sweep can disagree forever. EffectiveIntervalSeconds is the
// number actually in use.
//
// And the activity error is deliberately swallowed so a failing gateway cannot end
// the sweep, while the success line is suppressed unless something changed. So a sweep
// that has failed every tick for a day and a healthy quiet one are byte-identical in
// the logs AND in Postgres — saved_query_freshness_breaches is empty in both cases.
// LastSweepOK and LastError are the only place the difference exists.
type FreshnessSweepState struct {
	// EffectiveIntervalSeconds is the resolved value, not the default and not the
	// environment's current opinion.
	EffectiveIntervalSeconds int `json:"effective_interval_seconds"`
	TicksThisRun             int `json:"ticks_this_run"`
	TicksPerRun              int `json:"ticks_per_run"`
	// SweptThisRun is false between a continue-as-new and the first tick after it. In
	// that window every field below is a zero value, and without this flag "has not
	// swept yet in this run" is indistinguishable from "has never swept".
	SweptThisRun bool `json:"swept_this_run"`
	// LastSweepAt comes from workflow.Now, never time.Now: reading the wall clock in
	// workflow code is non-deterministic and a replay would produce a different value.
	LastSweepAt string                   `json:"last_sweep_at,omitempty"`
	LastSweepOK bool                     `json:"last_sweep_ok"`
	LastError   string                   `json:"last_error,omitempty"`
	LastResult  FreshnessSweepLastResult `json:"last_result"`
}

// FanOutState is the answer to FanOutStateQuery.
//
// NotYetConfirmed is named for what it is. The await loop walks `started` in order, so
// a child that finished out of turn stays in this list until the loop reaches it: this
// is what THIS fan-out has confirmed, never a claim about child state. Calling the
// field still_running would be a lie the operator would act on.
//
// Failed and NotYetConfirmed must never be collapsed. A half-delivered fan-out is the
// failure that looks like nothing at all — some models rebuild, one does not, and
// Postgres shows only that one model has a stale last run, which is indistinguishable
// from an upstream that never ran.
type FanOutState struct {
	UpstreamKind string `json:"upstream_kind"`
	UpstreamID   string `json:"upstream_id"`
	ExecutionID  string `json:"execution_id,omitempty"`
	Depth        int    `json:"depth"`
	// Total is how many children were started, after skipping targets with no model
	// and collapsing a model listed twice.
	Total           int      `json:"total"`
	Confirmed       []string `json:"confirmed"`
	Failed          []string `json:"failed"`
	NotYetConfirmed []string `json:"not_yet_confirmed"`
	// ChildWorkflowIDPrefix lets a caller compose <prefix>:<saved_query_id> and ask a
	// child directly. Returned rather than mirroring child state here, because a second
	// copy of a fact is a second thing that can be wrong.
	ChildWorkflowIDPrefix string `json:"child_workflow_id_prefix"`
}
