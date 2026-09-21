package handlers

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"api-gateway/internal/db"
	"api-gateway/internal/security"

	"github.com/gin-gonic/gin"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"

	log "github.com/sirupsen/logrus"
)

// ============================================================================
// What the explorer is doing RIGHT NOW
// ============================================================================
//
// Everything else this subsystem reports is a record of something that finished.
// saved_query_runs cannot hold an in-flight rebuild — its status CHECK admits only
// succeeded, failed and skipped, finished_at is NOT NULL, and the row is written once
// at the end with started_at and finished_at together. So the three questions an
// operator actually asks during an incident have no answer anywhere in Postgres: is
// this model rebuilding, what is queued behind the rebuild, and which upstream caused
// it.
//
// The refresh loop holds all three in memory and answers a Temporal query. This route
// is the only place that answer surfaces.
//
// THE SECURITY PROPERTY, AND WHY IT IS STRUCTURAL RATHER THAN CHECKED.
//
// A workflow id is not a workspace-scoped object. Temporal has no notion of this
// product's tenancy, so any endpoint that lets a caller name a workflow is an IDOR with
// extra steps: "model-refresh:<someone else's model>" would answer perfectly. This
// route therefore takes NO path parameter and reads no id from the request at all. The
// only ids it can ever query are derived, by modelRefreshWorkflowID, from rows the
// caller's own workspace and visibility predicate already returned. There is nothing to
// validate because there is nothing to supply.
//
// The same property is what keeps the freshness singleton out of reach. Its id is the
// constant "model-freshness-sweep", which is not in the image of modelRefreshWorkflowID
// for any input — every derived id carries the "model-refresh:" prefix. The singleton is
// cross-workspace by nature and is reported on the admin health route instead, behind
// the admin gate, where it belongs.
//
// ListWorkflowExecutions is never called. It would answer across the whole namespace and
// the filtering would then be this handler's problem, which is exactly the arrangement
// the previous paragraph exists to avoid.

// The query names. The adapter spells them in
// backend-temporal-adapter/internal/workflows/explorer_workflow_queries.go; the two
// modules share no types, so this is a second spelling of a wire contract and
// TestTheGatewaySpellsEveryExplorerQueryNameTheAdapterRegisters reads the adapter's
// source to hold them equal.
const (
	queryModelRefreshState   = "model_refresh_state"
	queryFreshnessSweepState = "freshness_sweep_state"
	queryFanOutState         = "fanout_state"
)

const (
	// modelRefreshWorkflowIDPrefix mirrors ModelRefreshWorkflowID in the adapter. The
	// prefix is load-bearing twice over: it is what makes one workflow per model (and so
	// what makes two completions coalesce rather than race), and it is what keeps every
	// id this handler can construct inside its own namespace.
	modelRefreshWorkflowIDPrefix = "model-refresh:"

	// freshnessSweepWorkflowID is here to be excluded, not to be queried. Naming it
	// gives TestTheFreshnessSingletonIsNeverReachableFromAWorkspaceListing something to
	// assert against.
	freshnessSweepWorkflowID = "model-freshness-sweep"
)

func modelRefreshWorkflowID(savedQueryID string) string {
	return modelRefreshWorkflowIDPrefix + savedQueryID
}

// The four answers. They are four because they are four different facts about the
// world, and an operator acts differently on each one. Collapsing any pair into "not
// running" is the failure this whole route exists to prevent: a model nobody is
// rebuilding and a model whose worker cannot be reached look identical in every other
// surface the product has.
const (
	// runningStateIdle — no refresh loop is open for this model. Either one never
	// started or the last one finished when its burst ended. Both are normal.
	runningStateIdle = "idle"
	// runningStateRunning — a loop is open. Detail may still be missing; see
	// DetailAvailable.
	runningStateRunning = "running"
	// runningStateUnreachable — the question could not be asked. This is a fault in the
	// asking, not an answer about the model.
	runningStateUnreachable = "unreachable"
	// runningStateUnknown — nothing was asked, because there is no Temporal client.
	runningStateUnknown = "unknown"
)

const (
	// runningModelQueryLimit caps the fan-out. One page load costs one query per model,
	// and those queries land on pipeline-workflows — the same task queue that carries
	// real rebuilds. Fifty is a ceiling on the blast radius of somebody leaving the page
	// open, not a page size anybody asked for.
	runningModelQueryLimit = 50

	// runningModelQueryTimeout is per query. A query is answered by a worker in memory;
	// if it has not come back in two seconds the worker is wedged or gone, which is
	// itself the answer.
	runningModelQueryTimeout = 2 * time.Second

	// runningModelQueryConcurrency bounds how many of those land at once.
	runningModelQueryConcurrency = 8
)

// modelRefreshCurrentView mirrors ModelRefreshCurrent in the adapter.
type modelRefreshCurrentView struct {
	ScheduleID   string `json:"schedule_id"`
	UpstreamKind string `json:"upstream_kind"`
	UpstreamID   string `json:"upstream_id"`
	ExecutionID  string `json:"execution_id,omitempty"`
	Depth        int    `json:"depth"`
	Coalesced    int    `json:"coalesced"`
	StartedAt    string `json:"started_at"`
}

// modelRefreshStateView mirrors ModelRefreshState in the adapter.
type modelRefreshStateView struct {
	SavedQueryID      string                   `json:"saved_query_id"`
	Phase             string                   `json:"phase"`
	QueuedCompletions int                      `json:"queued_completions"`
	Current           *modelRefreshCurrentView `json:"current"`
	RefreshesThisRun  int                      `json:"refreshes_this_run"`
	RefreshesPerRun   int                      `json:"refreshes_per_run"`
	LastError         string                   `json:"last_error,omitempty"`
}

// freshnessSweepStateView mirrors FreshnessSweepState in the adapter.
type freshnessSweepStateView struct {
	EffectiveIntervalSeconds int    `json:"effective_interval_seconds"`
	TicksThisRun             int    `json:"ticks_this_run"`
	TicksPerRun              int    `json:"ticks_per_run"`
	SweptThisRun             bool   `json:"swept_this_run"`
	LastSweepAt              string `json:"last_sweep_at,omitempty"`
	LastSweepOK              bool   `json:"last_sweep_ok"`
	LastError                string `json:"last_error,omitempty"`
	LastResult               struct {
		Scanned  int `json:"scanned"`
		Opened   int `json:"opened"`
		Resolved int `json:"resolved"`
		Failed   int `json:"failed"`
	} `json:"last_result"`
}

// runningModelRow is one authorized model. It is the ONLY source of workflow ids in
// this file; nothing derived from the request reaches modelRefreshWorkflowID.
type runningModelRow struct {
	SavedQueryID string
	Name         string
	ScheduleID   string
}

// RunningModelWork is one row of the answer.
type RunningModelWork struct {
	SavedQueryID string `json:"saved_query_id"`
	Name         string `json:"name"`
	ScheduleID   string `json:"schedule_id,omitempty"`
	// WorkflowID is returned because an operator with Temporal access will want it, and
	// because echoing the id we actually asked about makes the derivation auditable from
	// the response alone.
	WorkflowID string `json:"workflow_id"`
	State      string `json:"state"`
	// DetailAvailable distinguishes "running, and here is what it is doing" from
	// "running, and the worker would not say". The second happens when a worker on an
	// older build has no handler for this query type, which is precisely the state a
	// half-finished deploy is in.
	DetailAvailable bool                   `json:"detail_available"`
	Detail          *modelRefreshStateView `json:"detail,omitempty"`
	// Message says why, for every row that is not a plain running-with-detail.
	Message string `json:"message,omitempty"`
}

// modelRefreshQuerier is the seam. Everything below it is Temporal; everything above it
// is testable without one. rejected is the server refusing to answer because the run is
// closed, which is a different fact from an error and must not arrive as one.
type modelRefreshQuerier func(ctx context.Context, workflowID string) (detail *modelRefreshStateView, rejected bool, err error)

// modelRefreshVerdict is the classified outcome of one query.
type modelRefreshVerdict struct {
	State           string
	DetailAvailable bool
	Detail          *modelRefreshStateView
	Message         string
}

// classifyModelRefreshQuery maps one query outcome onto a row.
//
// Five inputs, four states. Two of the inputs — never started, and finished cleanly —
// are the same fact to an operator and share idle; the other three must stay apart.
//
// The rejected branch is why the query is issued with QUERY_REJECT_CONDITION_NOT_OPEN
// at all. Temporal will happily answer a query against a CLOSED run by replaying its
// history, and that answer is a real ModelRefreshState carrying the phase and counts
// from whenever the loop ended. Without the reject condition, a model that finished
// rebuilding an hour ago would render as running with an hour-old batch underneath it.
func classifyModelRefreshQuery(detail *modelRefreshStateView, rejected bool, err error) modelRefreshVerdict {
	switch {
	case rejected:
		return modelRefreshVerdict{
			State:   runningStateIdle,
			Message: "no open refresh loop; the last run has closed",
		}

	case err == nil:
		if detail == nil {
			// A query that succeeded and decoded to nothing. Not idle — the run is open —
			// and not a transport fault either.
			return modelRefreshVerdict{
				State:   runningStateRunning,
				Message: "the refresh loop answered with no state",
			}
		}
		return modelRefreshVerdict{State: runningStateRunning, DetailAvailable: true, Detail: detail}

	case isNotFound(err):
		// No execution by that id has ever existed. The overwhelmingly common case: a
		// loop lives only as long as a burst of upstream completions, so most models
		// have no workflow at all most of the time.
		return modelRefreshVerdict{State: runningStateIdle, Message: "no refresh loop has run for this model"}

	case isQueryFailed(err):
		// The server reached a worker and the worker refused. That is only possible for
		// an OPEN execution, so this is a running model whose detail is unavailable —
		// an older build with no handler for this query type, or a handler that panicked.
		// Reporting it as unreachable would blame the network for a deploy.
		return modelRefreshVerdict{
			State:   runningStateRunning,
			Message: "the refresh loop is running but did not answer this query: " + err.Error(),
		}

	default:
		return modelRefreshVerdict{State: runningStateUnreachable, Message: err.Error()}
	}
}

// isNotFound and isQueryFailed match on type, through wrapping. The SDK wraps service
// errors on the way out, so a string match would pass a unit test and misclassify in
// production — the same trap TestStartUpstreamFanOut_AWrappedAlreadyStartedIsStillRecognised
// was written for.
func isNotFound(err error) bool {
	var nf *serviceerror.NotFound
	return errors.As(err, &nf)
}

func isQueryFailed(err error) bool {
	var qf *serviceerror.QueryFailed
	return errors.As(err, &qf)
}

// listRunningModelWork asks about every row it is given, and about nothing else.
//
// A nil querier is not an error: it means this gateway has no Temporal client, which
// makes every row's state genuinely unknown rather than idle. Rendering "idle" there
// would be the worst available lie — it is the reassuring answer, and it would be
// produced by the one condition under which nothing can be reassuring.
func listRunningModelWork(ctx context.Context, rows []runningModelRow, query modelRefreshQuerier) []RunningModelWork {
	out := make([]RunningModelWork, len(rows))
	for i, r := range rows {
		out[i] = RunningModelWork{
			SavedQueryID: r.SavedQueryID,
			Name:         r.Name,
			ScheduleID:   r.ScheduleID,
			WorkflowID:   modelRefreshWorkflowID(r.SavedQueryID),
		}
	}

	if query == nil {
		for i := range out {
			out[i].State = runningStateUnknown
			out[i].Message = "orchestration is not available to this gateway; nothing was asked"
		}
		return out
	}

	sem := make(chan struct{}, runningModelQueryConcurrency)
	var wg sync.WaitGroup
	for i := range out {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			detail, rejected, err := query(ctx, out[i].WorkflowID)
			v := classifyModelRefreshQuery(detail, rejected, err)
			out[i].State = v.State
			out[i].DetailAvailable = v.DetailAvailable
			out[i].Detail = v.Detail
			out[i].Message = v.Message
		}(i)
	}
	wg.Wait()
	return out
}

// temporalModelRefreshQuerier binds the seam to a live client. Nil client in, nil
// querier out — the caller turns that into "unknown", never into "idle".
func temporalModelRefreshQuerier(tc client.Client) modelRefreshQuerier {
	if tc == nil {
		return nil
	}
	return func(ctx context.Context, workflowID string) (*modelRefreshStateView, bool, error) {
		qctx, cancel := context.WithTimeout(ctx, runningModelQueryTimeout)
		defer cancel()

		resp, err := tc.QueryWorkflowWithOptions(qctx, &client.QueryWorkflowWithOptionsRequest{
			WorkflowID: workflowID,
			QueryType:  queryModelRefreshState,
			// See classifyModelRefreshQuery: without this a closed run answers from
			// history and stale state renders as live.
			QueryRejectCondition: enumspb.QUERY_REJECT_CONDITION_NOT_OPEN,
		})
		if err != nil {
			return nil, false, err
		}
		if resp.QueryRejected != nil {
			return nil, true, nil
		}
		if resp.QueryResult == nil {
			return nil, false, nil
		}
		var st modelRefreshStateView
		if err := resp.QueryResult.Get(&st); err != nil {
			return nil, false, fmt.Errorf("decode %s: %w", queryModelRefreshState, err)
		}
		return &st, false, nil
	}
}

// runningModelWorkQuery is the authorized set: models in the caller's workspace that
// the caller may see and that have a schedule capable of starting a refresh loop.
//
// Only after_upstream schedules are listed. A clock schedule runs through a Temporal
// Schedule and never produces a ModelRefreshWorkflow, so including one would add a row
// that is idle by construction and read as a model that has stopped.
//
// The predicate is the same pair ListModelFreshness and ListSavedQuerySchedules carry:
// workspace, then visibility. A private model belonging to another member must not
// become visible just because it happens to be rebuilding.
func runningModelWorkQuery() string {
	return `
		SELECT sq.id::text, sq.name, s.schedule_id
		FROM saved_query_schedules s
		JOIN saved_queries sq ON sq.id = s.saved_query_id
		WHERE sq.workspace_id = $1
		  AND (sq.visibility = 'workspace' OR sq.created_by = $2)
		  AND s.status != 'deleted'
		  AND s.schedule_type = $3
		ORDER BY sq.name
		LIMIT $4
	`
}

// ListRunningModelWork reports what the workspace's refresh loops are doing.
// GET /api/v1/explorer/running
//
// Read-only in the strongest sense available: a Temporal query cannot signal, cancel,
// terminate or reset anything, and the handler holds no write path of any kind.
func ListRunningModelWork(c *gin.Context) {
	userID, ok := resolveUserID(c)
	if !ok {
		return
	}
	if _, ok := requireWorkspaceRole(c, security.WSViewer); !ok {
		return
	}
	workspaceID, ok := resolveActiveWorkspace(c)
	if !ok {
		return
	}
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "database not available"})
		return
	}

	rows, err := database.QueryContext(c.Request.Context(), runningModelWorkQuery(),
		workspaceID, userID, scheduleAfterUpstream, runningModelQueryLimit)
	if err != nil {
		log.WithError(err).Error("list running model work")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list running model work"})
		return
	}
	defer rows.Close()

	models := make([]runningModelRow, 0)
	for rows.Next() {
		var r runningModelRow
		if err := rows.Scan(&r.SavedQueryID, &r.Name, &r.ScheduleID); err != nil {
			log.WithError(err).Error("scan running model work")
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list running model work"})
			return
		}
		models = append(models, r)
	}
	if err := rows.Err(); err != nil {
		log.WithError(err).Error("iterate running model work")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list running model work"})
		return
	}

	tc := getTemporalClient()
	work := listRunningModelWork(c.Request.Context(), models, temporalModelRefreshQuerier(tc))

	// 200 with temporal_available false, not 503. The list itself is a real answer — it
	// says which models have an after_upstream schedule — and the caller needs to be able
	// to tell "nothing is rebuilding" from "I could not find out".
	c.JSON(http.StatusOK, gin.H{
		"models":             work,
		"count":              len(work),
		"limit":              runningModelQueryLimit,
		"temporal_available": tc != nil,
	})
}

// ============================================================================
// The freshness sweep, on the admin health route
// ============================================================================
//
// The sweep is the thing that notices when nothing is happening. Two properties make it
// the single worst subject in this product for silent failure, and both are invisible
// everywhere else:
//
// It swallows its activity error on purpose, so that a failing gateway cannot end the
// sweep — and it suppresses its success log unless something changed. A sweep that has
// failed every tick for a day and a healthy quiet one are byte-identical in the logs AND
// in Postgres, because saved_query_freshness_breaches is empty in both cases. That is
// not hypothetical: the stack this change's replay fixture was taken from had been
// failing every tick for hours with INTERNAL_SERVICE_SECRET unset, and nothing anywhere
// said so.
//
// And its interval is frozen into a USE_EXISTING singleton's argument, so editing
// MODEL_FRESHNESS_SWEEP_SECONDS and restarting the adapter changes nothing until the run
// is terminated. The compose file and the running sweep can disagree indefinitely with no
// error raised. EffectiveIntervalSeconds is the number actually in use.
//
// Admin-gated, and the singleton's id is a constant here rather than anything derived,
// because the sweep is cross-workspace by nature: the thing that has to notice a model
// nobody is watching cannot be scoped to the session of somebody watching.
func checkFreshnessSweep() serviceHealth {
	const service = "explorer-freshness-sweep"
	start := time.Now()

	tc := getTemporalClient()
	if tc == nil {
		return serviceHealth{
			Service: service, Status: "unknown", LatencyMs: 0,
			Error: "orchestration is not available to this gateway; the sweep was not asked",
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), runningModelQueryTimeout)
	defer cancel()

	resp, err := tc.QueryWorkflowWithOptions(ctx, &client.QueryWorkflowWithOptionsRequest{
		WorkflowID: freshnessSweepWorkflowID,
		QueryType:  queryFreshnessSweepState,
		// A closed sweep must read as down, not as whatever it was doing when it closed.
		QueryRejectCondition: enumspb.QUERY_REJECT_CONDITION_NOT_OPEN,
	})
	latency := time.Since(start).Milliseconds()

	switch {
	case err != nil && isNotFound(err):
		// The singleton is started at adapter boot and continues as new forever. Absent
		// means nothing is watching for stale models at all.
		return serviceHealth{Service: service, Status: "down", LatencyMs: latency,
			Error: "no freshness sweep is running; nothing is watching for stale models"}

	case err != nil && isQueryFailed(err):
		// Running, but this build's worker would not answer. Up, with the gap named.
		return serviceHealth{Service: service, Status: "up", LatencyMs: latency,
			Error:  "the sweep is running but did not answer this query: " + err.Error(),
			Detail: map[string]any{"detail_available": false}}

	case err != nil:
		return serviceHealth{Service: service, Status: "unknown", LatencyMs: latency, Error: err.Error()}

	case resp.QueryRejected != nil:
		return serviceHealth{Service: service, Status: "down", LatencyMs: latency,
			Error: "the freshness sweep has stopped; nothing is watching for stale models"}
	}

	var st freshnessSweepStateView
	if resp.QueryResult == nil {
		return serviceHealth{Service: service, Status: "up", LatencyMs: latency,
			Error:  "the sweep answered with no state",
			Detail: map[string]any{"detail_available": false}}
	}
	if err := resp.QueryResult.Get(&st); err != nil {
		return serviceHealth{Service: service, Status: "unknown", LatencyMs: latency,
			Error: fmt.Sprintf("decode %s: %v", queryFreshnessSweepState, err)}
	}

	h := serviceHealth{Service: service, Status: "up", LatencyMs: latency, Detail: map[string]any{
		"detail_available":           true,
		"effective_interval_seconds": st.EffectiveIntervalSeconds,
		"ticks_this_run":             st.TicksThisRun,
		"ticks_per_run":              st.TicksPerRun,
		"swept_this_run":             st.SweptThisRun,
		"last_sweep_at":              st.LastSweepAt,
		"last_sweep_ok":              st.LastSweepOK,
		"last_result":                st.LastResult,
	}}

	// Alive and failing every tick is its own status. Reporting it as up because the
	// workflow is running would reproduce, on the health route, exactly the blindness
	// this row was added to remove.
	if st.SweptThisRun && !st.LastSweepOK {
		h.Status = "degraded"
		h.Error = "the sweep is running and its last tick failed: " + st.LastError
	}
	return h
}
