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
// Scheduled Model Run Workflow
// ============================================================================
// Fires one rebuild of a saved-query model (migration 085). Deliberately much
// thinner than ScheduledPipelineRunWorkflow, and the difference is worth stating
// because the asymmetry looks like an omission otherwise.
//
// The pipeline wrapper has to do work here — overlap guard, execution record,
// child workflow — because a pipeline run is a distributed job spanning several
// services. A model rebuild is one SQL statement sequence against one connection.
// The only component that can decrypt that connection's credentials is the API
// gateway, so the gateway runs it, and this workflow's entire job is to be the
// thing Temporal can schedule: call the endpoint, report what happened.
//
// The overlap guard lives in the schedule's SKIP policy rather than in an activity
// for the same reason — with one action there is nothing for a guard to arbitrate.
//
// Authorization is NOT here. The endpoint re-resolves the schedule's run_as_user_id
// against workspace_members and re-classifies the current sql_text on every call.
// Passing a user identity through the workflow would mean storing a permission in a
// Temporal schedule argument, where it would be frozen at create time and outlive
// any demotion.

// ScheduledModelRunInput is the input to the model run wrapper workflow.
type ScheduledModelRunInput struct {
	ScheduleID   string `json:"schedule_id"`
	SavedQueryID string `json:"saved_query_id"`
}

// modelRunActivityResult mirrors the endpoint's response envelope.
type modelRunActivityResult struct {
	Status          string `json:"status"`
	Error           string `json:"error,omitempty"`
	TargetTable     string `json:"target_table,omitempty"`
	AutoPauseReason string `json:"auto_pause_reason,omitempty"`
	Reason          string `json:"reason,omitempty"`
}

// ScheduledModelRunWorkflow is the wrapper workflow a model's Temporal schedule runs.
func ScheduledModelRunWorkflow(ctx workflow.Context, input ScheduledModelRunInput) error {
	logger := workflow.GetLogger(ctx)
	logger.Info("🕐 ScheduledModelRunWorkflow started",
		"schedule_id", input.ScheduleID,
		"saved_query_id", input.SavedQueryID)

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		// A rebuild is a bulk warehouse operation. This must exceed the gateway's own
		// modelRunTimeout (30m) or Temporal would abandon an activity whose statement
		// is still running server-side, and the retry below would then stack a second
		// rebuild on top of the first.
		StartToCloseTimeout: 35 * time.Minute,
		HeartbeatTimeout:    35 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    5 * time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    time.Minute,
			// Three attempts covers a gateway restart or a brief network fault. It does
			// NOT cover a failing query: the endpoint answers 200 with status "failed"
			// for anything the engine rejected, so a broken model is recorded once and
			// never retried in a tight loop.
			MaximumAttempts: 3,
		},
	})

	var result modelRunActivityResult
	if err := workflow.ExecuteActivity(ctx, RunModelActivity, input).Get(ctx, &result); err != nil {
		logger.Error("model run activity failed", "error", err, "saved_query_id", input.SavedQueryID)
		return fmt.Errorf("model run failed: %w", err)
	}

	switch result.Status {
	case "succeeded":
		logger.Info("✅ model rebuilt", "saved_query_id", input.SavedQueryID, "target_table", result.TargetTable)
	case "skipped":
		// Includes every auto-pause. The schedule is already paused by the time this
		// returns, so this tick is the last one — say why in the workflow history,
		// which is where an operator looks when a schedule stops.
		logger.Warn("⏭️  model run skipped",
			"saved_query_id", input.SavedQueryID,
			"reason", firstNonEmpty(result.AutoPauseReason, result.Error, result.Reason))
	default:
		// The engine rejected the SQL. Not a workflow failure: retrying would not
		// change the answer, and the failure is already recorded on the saved query.
		logger.Warn("❌ model run failed", "saved_query_id", input.SavedQueryID, "error", result.Error)
	}

	return nil
}

// RunModelActivity calls the API gateway's internal model-run endpoint.
//
// It is HTTP rather than a direct database write because the rebuild needs the
// connection's decrypted credentials, and the gateway is the only service holding the
// key. Keeping the crypto boundary in one place is worth an extra hop.
func RunModelActivity(ctx context.Context, input ScheduledModelRunInput) (*modelRunActivityResult, error) {
	secret := strings.TrimSpace(os.Getenv("INTERNAL_SERVICE_SECRET"))
	if secret == "" {
		// Fail loudly rather than calling an endpoint that will 503 anyway. A silent
		// misconfiguration here would look exactly like a schedule that never fires.
		return nil, fmt.Errorf("INTERNAL_SERVICE_SECRET is not configured; scheduled model runs cannot authenticate")
	}

	apiGatewayURL := strings.TrimSpace(os.Getenv("API_GATEWAY_URL"))
	if apiGatewayURL == "" {
		// 8080 is the gateway's in-container port. 5001 is only the host-side mapping,
		// and is unreachable from inside the compose network.
		apiGatewayURL = "http://api-gateway:8080"
	}
	url := fmt.Sprintf("%s/api/v1/internal/explorer/models/%s/run", apiGatewayURL, input.SavedQueryID)

	body, err := json.Marshal(map[string]string{"schedule_id": input.ScheduleID})
	if err != nil {
		return nil, fmt.Errorf("failed to encode model run request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(string(body)))
	if err != nil {
		return nil, fmt.Errorf("failed to build model run request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Internal-Secret", secret)

	// Slightly beyond the gateway's modelRunTimeout so the timeout that fires first is
	// the one that can actually cancel the statement.
	client := &http.Client{Timeout: 32 * time.Minute}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("model run request failed: %w", err)
	}
	defer resp.Body.Close()

	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		// Retryable by design: a non-200 here means the gateway itself could not handle
		// the call. Anything about the SQL comes back as 200 with a status field.
		return nil, fmt.Errorf("model run endpoint returned http %d: %s", resp.StatusCode, truncateForLog(string(raw), 400))
	}

	var out modelRunActivityResult
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("failed to decode model run response: %w", err)
	}
	return &out, nil
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return "unspecified"
}

func truncateForLog(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "…"
}
