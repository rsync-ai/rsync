package workflows

import (
	"context"
	"encoding/json"
	"errors"
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
// Recording a model run that never reported a result
// ============================================================================
// The gateway writes the run history row and the saved query's last-run badge
// itself, inside the run endpoint. That covers every outcome the gateway gets to
// decide — succeeded, skipped, and "failed" for SQL the engine rejected — and it
// leaves exactly one gap: a run whose activity spent its whole retry budget
// without a 200 ever coming back. A gateway that was down, a request that timed
// out, a 5xx: in every one of those the endpoint never reached the code that
// records, so the only trace was a failed Temporal workflow nobody looks at, and
// the schedule page kept showing the last success as if nothing had happened.
//
// This closes that gap from the one place that knows the run failed. It is a
// separate endpoint rather than a retry of the run endpoint because the run
// endpoint REBUILDS; the only thing wanted here is the record.

// recordModelRunFailureChangeID gates the recording call. Both workflows that run
// a model gained a new activity on their failure path, and a history written by
// the code before that has no such activity in it: without the gate, replaying a
// run that failed before this change would schedule a command the history does
// not contain and wedge the workflow task.
const recordModelRunFailureChangeID = "record-model-run-failure"

// modelRunFailureDetailMaxRunes bounds the detail sent to the gateway. The gateway
// scrubs the whole message and keeps at most 1000 runes of it
// (modelRunFailureErrorMaxRunes), so the detail is cut here to leave the sentences
// around it intact: a cut made there would take "what happens next" with it.
const modelRunFailureDetailMaxRunes = 600

// ModelRunFailureInput is the input to RecordModelRunFailureActivity.
type ModelRunFailureInput struct {
	SavedQueryID string `json:"saved_query_id"`
	ScheduleID   string `json:"schedule_id"`
	// Trigger is empty for the clock path and ModelRefreshTrigger for the event path,
	// the same selector the run endpoint takes, so the gateway resolves the schedule
	// through the same door the run itself went through.
	Trigger string `json:"trigger,omitempty"`
	// Error is the plain-words message the schedule page shows.
	Error string `json:"error"`
	// StartedAt is workflow time when the run activity was scheduled. Deterministic,
	// so every retry and every replay sends the same value, and the gateway uses it
	// with the schedule id to write one row however many times this is delivered.
	StartedAt time.Time `json:"started_at"`

	// Provenance of an event-path run, copied from the run's own input so the failed
	// row names the upstream that woke it, like a row the run endpoint writes does.
	// Sent only with a Trigger, as RunModelActivity sends it.
	Depth         int    `json:"depth,omitempty"`
	UpstreamKind  string `json:"upstream_kind,omitempty"`
	UpstreamID    string `json:"upstream_id,omitempty"`
	UpstreamRunID string `json:"upstream_run_id,omitempty"`
	ExecutionID   string `json:"execution_id,omitempty"`
	Coalesced     int    `json:"coalesced,omitempty"`
}

// recordModelRunFailure reports a run activity that failed terminally. It is
// best-effort by construction: it returns nothing, and a failure to record is a log
// line — the caller's outcome is decided by the run, never by its record.
func recordModelRunFailure(ctx workflow.Context, run ScheduledModelRunInput, startedAt time.Time, runErr error) {
	// A cancelled run was stopped on purpose, not failed; and a cancelled context
	// could not schedule the recording activity anyway. Checked before GetVersion so
	// a cancellation writes no version marker either.
	if runErr == nil || temporal.IsCanceledError(runErr) {
		return
	}
	if workflow.GetVersion(ctx, recordModelRunFailureChangeID, workflow.DefaultVersion, 1) == workflow.DefaultVersion {
		return
	}

	logger := workflow.GetLogger(ctx)
	rctx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout:    20 * time.Second,
		ScheduleToCloseTimeout: 2 * time.Minute,
		// Short and bounded. This runs after a run that already failed, often because
		// the gateway was unreachable; a long retry here would only hold the workflow
		// open against the same outage.
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    2 * time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    10 * time.Second,
			MaximumAttempts:    3,
		},
	})
	input := ModelRunFailureInput{
		SavedQueryID:  run.SavedQueryID,
		ScheduleID:    run.ScheduleID,
		Trigger:       run.Trigger,
		Error:         describeModelRunFailure(runErr, run.Trigger),
		StartedAt:     startedAt.UTC(),
		Depth:         run.Depth,
		UpstreamKind:  run.UpstreamKind,
		UpstreamID:    run.UpstreamID,
		UpstreamRunID: run.UpstreamRunID,
		ExecutionID:   run.ExecutionID,
		Coalesced:     run.Coalesced,
	}
	if err := workflow.ExecuteActivity(rctx, RecordModelRunFailureActivity, input).Get(rctx, nil); err != nil {
		logger.Warn("could not record the failed model run; the schedule page will not show it",
			"saved_query_id", run.SavedQueryID, "schedule_id", run.ScheduleID, "error", err)
	}
}

// describeModelRunFailure turns the run activity's terminal error into the message
// a person reads on the schedule page: what happened, and what happens next.
func describeModelRunFailure(runErr error, trigger string) string {
	next := "It will run again at the next scheduled time."
	if trigger == ModelRefreshTrigger {
		next = "It will run again the next time something this model waits on finishes."
	}

	if temporal.IsTimeoutError(runErr) {
		return "The model rebuild did not finish within its time limit, so it was stopped and no result came back. " +
			next + " If this keeps happening, check whether the query has become slower or its database is overloaded."
	}

	// The innermost application message, not Error(): the wrappers around it add
	// activity ids and "(type: …, retryable: …)", which mean nothing to the reader.
	detail := runErr.Error()
	var appErr *temporal.ApplicationError
	if errors.As(runErr, &appErr) && strings.TrimSpace(appErr.Message()) != "" {
		detail = appErr.Message()
	}

	msg := "The model rebuild could not be completed: the service that runs it did not return a result after several attempts. " +
		next + " If a successful run appears at the same time, the rebuild did finish and only its report was lost."
	if d := strings.TrimSpace(detail); d != "" {
		msg += " Details: " + truncateRunes(d, modelRunFailureDetailMaxRunes)
	}
	return msg
}

// truncateRunes cuts on a rune boundary, so a bound never splits a character into
// bytes the database would refuse.
func truncateRunes(s string, max int) string {
	n := 0
	for i := range s {
		if n == max {
			return s[:i] + "…"
		}
		n++
	}
	return s
}

// RecordModelRunFailureActivity calls the API gateway's internal failed-run endpoint.
//
// HTTP for the same reason RunModelActivity is: the gateway owns the saved query's
// tables and the rules for which schedule a record may be attached to.
func RecordModelRunFailureActivity(ctx context.Context, input ModelRunFailureInput) error {
	secret := strings.TrimSpace(os.Getenv("INTERNAL_SERVICE_SECRET"))
	if secret == "" {
		// Not retryable: the environment will not change between attempts.
		return temporal.NewNonRetryableApplicationError(
			"INTERNAL_SERVICE_SECRET is not configured; the failed model run cannot be recorded",
			"InternalSecretMissing", nil)
	}

	apiGatewayURL := strings.TrimSpace(os.Getenv("API_GATEWAY_URL"))
	if apiGatewayURL == "" {
		apiGatewayURL = "http://api-gateway:8080"
	}
	url := fmt.Sprintf("%s/api/v1/internal/explorer/models/%s/run-failed", apiGatewayURL, input.SavedQueryID)

	payload := map[string]any{
		"schedule_id": input.ScheduleID,
		"error":       input.Error,
		"started_at":  input.StartedAt.UTC().Format(time.RFC3339Nano),
	}
	if input.Trigger != "" {
		payload["trigger"] = input.Trigger
		payload["depth"] = input.Depth
		payload["upstream_kind"] = input.UpstreamKind
		payload["upstream_id"] = input.UpstreamID
		payload["upstream_run_id"] = input.UpstreamRunID
		payload["execution_id"] = input.ExecutionID
		payload["coalesced"] = input.Coalesced
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to encode failed-run record: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(string(body)))
	if err != nil {
		return fmt.Errorf("failed to build failed-run record request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Internal-Secret", secret)

	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed-run record request failed: %w", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))

	switch code := resp.StatusCode; {
	case code >= 200 && code < 300:
		return nil
	case code == http.StatusNotFound && gatewaySaidNotFound(raw):
		// The model or its schedule was deleted after the run started. There is
		// nothing left to attach a record to, and no retry will create it.
		return nil
	case code == http.StatusNotFound:
		// A 404 without the handler's not_found answer did not come from the handler:
		// a gateway that predates this route, or something in front of it mid-rollout.
		// Nothing was recorded, so this is never success. Retryable, and a log line
		// once the attempts are spent.
		return fmt.Errorf("failed-run record endpoint was not found (http 404): %s", truncateForLog(string(raw), 400))
	case code >= 400 && code < 500 && code != http.StatusRequestTimeout && code != http.StatusTooManyRequests:
		return temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("failed-run record endpoint refused the call with http %d: %s", code, truncateForLog(string(raw), 400)),
			"ModelRunFailureRecordRefused", nil)
	default:
		return fmt.Errorf("failed-run record endpoint returned http %d: %s", code, truncateForLog(string(raw), 400))
	}
}

// gatewaySaidNotFound reports whether a 404 body is the failed-run handler's own
// answer for a deleted model or schedule, {"status":"not_found",...}. Any other 404
// (a router's plain "404 page not found") means the handler never ran.
func gatewaySaidNotFound(raw []byte) bool {
	var body struct {
		Status string `json:"status"`
	}
	return json.Unmarshal(raw, &body) == nil && body.Status == "not_found"
}
