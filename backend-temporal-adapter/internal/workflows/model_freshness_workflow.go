package workflows

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// ============================================================================
// Model freshness — the durable timer
// ============================================================================
//
// Every other explorer workflow is woken by something happening. ScheduledModelWorkflow
// is woken by a clock tick Temporal owns; ModelRefreshWorkflow is woken by a signal from
// a run that completed. Neither can be woken by a rebuild that never came, and that is
// exactly the failure a freshness deadline is for.
//
// This is the shape Airflow arrived at after removing SLAs in 3.1: the defect they cite
// is that an SLA was only evaluated when a run FINISHED, so a run that never completed
// was never flagged. Deadline Alerts replaced it with a precomputed expiry checked on a
// heartbeat. The same reasoning applies one notch earlier here — the model this feature
// exists to catch is one whose run never STARTED, because its last upstream was deleted
// and its after_upstream schedule is now an active trigger with an empty set.
//
// A per-model timer would be the more elegant shape and it cannot work, for that same
// reason: a per-model timer has to be armed by something, and the models most worth
// catching are the ones nothing is firing for. So: one singleton sweep, looking at the
// whole table on an interval, which needs no event to exist.
//
// It does NOT rebuild anything. Dagster deprecated the coupling that let freshness drive
// auto-materialization, and dbt's warn_after/error_after never had it — freshness is an
// observation. Here a rebuild is a DROP and CREATE of a user's table, and doing that
// unattended because a clock expired is a larger decision than the operator made when
// they typed a number into a deadline field.

// ModelFreshnessSweepInterval is how often the sweep looks.
//
// 60 seconds, equal to migration 101's floor on freshness_deadline_seconds, and the two
// have to agree: the interval is the resolution of every answer this feature gives, so a
// deadline the checker could not observe on time must not be settable. Dagster's
// freshness daemon runs at the same order.
const ModelFreshnessSweepInterval = 60 * time.Second

// modelFreshnessTicksPerRun bounds one run's history before continuing as new.
//
// A tick is a timer plus an activity, so this is a few thousand events — comfortably
// under any history limit, and it caps the history of a workflow that by design never
// ends. Without it the singleton would accumulate forever, and the failure would not
// appear until the sweep had been running for months.
const modelFreshnessTicksPerRun = 500

// ModelFreshnessWorkflowID is the singleton's fixed id. Fixed rather than generated
// because the whole point is that exactly one of these exists: starting it at every
// adapter boot with a USE_EXISTING conflict policy then makes boot idempotent, and a
// second adapter replica joins the existing sweep instead of doubling it.
const ModelFreshnessWorkflowID = "model-freshness-sweep"

// ModelFreshnessInput is the workflow argument.
type ModelFreshnessInput struct {
	// IntervalSeconds overrides ModelFreshnessSweepInterval. Carried in the argument
	// rather than read from the environment inside the workflow, because reading the
	// environment in workflow code is non-deterministic and a replay would take a
	// different path than the original execution.
	//
	// A consequence worth knowing: the singleton is started with USE_EXISTING, so
	// changing the interval on a running sweep needs the workflow terminated, not just
	// the adapter restarted.
	IntervalSeconds int `json:"interval_seconds,omitempty"`

	// Ticks already spent, carried across continue-as-new so the bound is on the run
	// and not on the wall clock.
	Ticks int `json:"ticks,omitempty"`
}

// ModelFreshnessSweepResult is what the gateway reports back about one sweep.
type ModelFreshnessSweepResult struct {
	Scanned  int `json:"scanned"`
	Opened   int `json:"opened"`
	Resolved int `json:"resolved"`
	Failed   int `json:"failed"`
}

// ModelFreshnessWorkflow sleeps, sweeps, and repeats, forever.
func ModelFreshnessWorkflow(ctx workflow.Context, input ModelFreshnessInput) error {
	logger := workflow.GetLogger(ctx)

	interval := ModelFreshnessSweepInterval
	if input.IntervalSeconds > 0 {
		interval = time.Duration(input.IntervalSeconds) * time.Second
	}

	// Registered before the first Sleep, which is this loop's only blocking call. After
	// it, the handler would not exist for the whole first interval — precisely when an
	// operator who has just restarted the adapter is asking whether the sweep came up.
	//
	// The interval reported is the RESOLVED one. The singleton is started with
	// USE_EXISTING, so a changed MODEL_FRESHNESS_SWEEP_SECONDS reaches a run that is
	// already going only if that run is terminated first. Until then the environment and
	// the sweep disagree and nothing anywhere says so.
	state := &FreshnessSweepState{
		EffectiveIntervalSeconds: int(interval / time.Second),
		TicksPerRun:              modelFreshnessTicksPerRun,
	}
	if err := workflow.SetQueryHandler(ctx, FreshnessSweepStateQuery, func() (FreshnessSweepState, error) {
		return *state, nil
	}); err != nil {
		// Logged, not returned, for the same reason the activity error below is: this
		// sweep is what notices when nothing is happening, so it has to be the last thing
		// to stop. Losing introspection is a defect; losing the staleness monitor because
		// introspection failed would be the feature breaking the thing it reports on.
		logger.Error("could not register the freshness sweep query handler; the sweep runs on unobserved",
			"error", err)
	}

	actx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		// The sweep is one indexed query and a handful of small writes. A minute is
		// generous; anything longer than the interval itself would let sweeps overlap,
		// which the idempotent writes survive but which would still mean the timer had
		// stopped meaning what it says.
		StartToCloseTimeout: time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    5 * time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    30 * time.Second,
			// Three attempts covers a gateway restart. Beyond that, giving up and
			// waiting for the next tick is better than retrying into the next interval:
			// the sweep derives everything from current state, so a skipped tick costs
			// detection latency and nothing else. Nothing is lost by not catching up.
			MaximumAttempts: 3,
		},
	})

	ticks := input.Ticks
	state.TicksThisRun = ticks
	for {
		// Sleep FIRST. On the very first run the adapter has just booted, which usually
		// means the gateway is still coming up beside it — an immediate sweep would spend
		// all three retries on connection refusals before the first interval had even
		// elapsed.
		if err := workflow.Sleep(ctx, interval); err != nil {
			// Cancellation. Returning the error is what lets an operator stop the
			// singleton; swallowing it would make it unstoppable short of a terminate.
			return err
		}

		var res ModelFreshnessSweepResult
		err := workflow.ExecuteActivity(actx, SweepModelFreshnessActivity).Get(ctx, &res)

		// Recorded before it is acted on, and recorded on both paths. The error below is
		// swallowed so a failing gateway cannot end the sweep, and the success line is
		// suppressed unless something changed — so a sweep that has failed every tick for
		// a day and a healthy quiet one produce identical logs and an identically empty
		// saved_query_freshness_breaches. These four fields are the only place that
		// difference exists.
		state.SweptThisRun = true
		state.LastSweepAt = workflow.Now(ctx).UTC().Format(time.RFC3339)
		state.LastSweepOK = err == nil
		state.LastError = ""
		state.LastResult = FreshnessSweepLastResult{}
		if err != nil {
			state.LastError = err.Error()
		} else {
			state.LastResult = FreshnessSweepLastResult{
				Scanned: res.Scanned, Opened: res.Opened,
				Resolved: res.Resolved, Failed: res.Failed,
			}
		}

		if err != nil {
			// Logged, not returned. A failing gateway must not end the sweep — it is the
			// thing that notices when nothing is happening, so it has to be the last
			// thing to stop.
			logger.Warn("⚠️  model freshness sweep failed; will retry on the next tick", "error", err)
		} else if res.Opened > 0 || res.Resolved > 0 {
			// Quiet when nothing changed. At this interval a line per tick would be
			// 1,440 lines a day saying nothing, which is how the lines that say
			// something get missed.
			logger.Info("🕒 model freshness swept",
				"scanned", res.Scanned, "opened", res.Opened,
				"resolved", res.Resolved, "failed", res.Failed)
		}

		ticks++
		state.TicksThisRun = ticks
		if ticks >= modelFreshnessTicksPerRun {
			return workflow.NewContinueAsNewError(ctx, ModelFreshnessWorkflow, ModelFreshnessInput{
				IntervalSeconds: input.IntervalSeconds,
				Ticks:           0,
			})
		}
	}
}

// SweepModelFreshnessActivity asks the gateway to check every model carrying a deadline.
//
// HTTP for the same reason RunModelActivity is: the gateway owns the database the
// explorer's tables live in, and putting a second writer of saved_query_freshness_breaches
// in another service would mean two copies of the rule for telling a rebuild from a
// widened deadline.
func SweepModelFreshnessActivity(ctx context.Context) (*ModelFreshnessSweepResult, error) {
	secret := strings.TrimSpace(os.Getenv("INTERNAL_SERVICE_SECRET"))
	if secret == "" {
		// Fail loudly. A silent misconfiguration here looks exactly like a workspace
		// where nothing is ever stale, which is the most reassuring possible way for a
		// staleness monitor to be broken.
		return nil, fmt.Errorf("INTERNAL_SERVICE_SECRET is not configured; the freshness sweep cannot authenticate")
	}

	apiGatewayURL := strings.TrimSpace(os.Getenv("API_GATEWAY_URL"))
	if apiGatewayURL == "" {
		apiGatewayURL = "http://api-gateway:8080"
	}
	url := apiGatewayURL + "/api/v1/internal/explorer/freshness/sweep"

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader("{}"))
	if err != nil {
		return nil, fmt.Errorf("failed to build freshness sweep request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Internal-Secret", secret)

	client := &http.Client{Timeout: 45 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("freshness sweep request failed: %w", err)
	}
	defer resp.Body.Close()

	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("freshness sweep endpoint returned http %d: %s", resp.StatusCode, truncateForLog(string(raw), 400))
	}

	var out ModelFreshnessSweepResult
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("failed to decode freshness sweep response: %w", err)
	}
	return &out, nil
}
