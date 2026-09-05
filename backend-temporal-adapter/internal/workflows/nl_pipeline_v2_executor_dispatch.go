package workflows

import (
	"fmt"
	"strings"
)

// ==============================================================================
// EXECUTOR DISPATCH — shared contracts (Phase 3: Temporal-native executor)
// ==============================================================================
// ⛔ PARKED — dead scaffolding. The "temporal" dispatch path below is fully built
// and unit-tested but NEVER taken at runtime: nothing populates
// NLPipelineWorkflowV2Input.ExecutorDispatch (api-gateway RunPipeline +
// scheduled_run_workflow build the input without it), so it is always "" and the
// workflow always uses the correlation path. The orchestrator's native worker is
// default-OFF (EXECUTOR_NATIVE_WORKER=false everywhere). Code is retained so the
// design stays reviewable; removal is tracked in BACKLOG.md and the status is
// recorded in CAPABILITIES.md. Do not build on this path without finishing the
// cutover described in CAPABILITIES.
//
// V2 executor dispatch supports two paths, selected per-workflow by
// NLPipelineWorkflowV2Input.ExecutorDispatch:
//
//   "correlation" (default) — ExecutorActivityV2 writes the task to the Redis
//       correlation store and the orchestrator's ExecutorWorker Redis poller
//       picks it up. Reply via Redis BRPOP.
//
//   "temporal" — the workflow invokes "ExecutorNativeActivity" (registered by a
//       Temporal worker hosted inside the orchestrator process, on the
//       executor-tasks task queue) which calls ExecutorWorker.Execute directly.
//       No Redis hop.
//
// Both paths feed the SAME task payload and the SAME result classification, so
// HITL (table selection), self-healing (auth/config/schema-drift) and
// success/running semantics are identical regardless of dispatch. The two
// helpers below are the shared pieces that guarantee that equivalence.

// ExecutorTaskQueue is the Temporal task queue the orchestrator-hosted native
// executor worker polls. The workflow targets it only when ExecutorDispatch is
// "temporal".
const ExecutorTaskQueue = "executor-tasks"

// ExecutorDispatchCorrelation and ExecutorDispatchTemporal are the accepted
// values of NLPipelineWorkflowV2Input.ExecutorDispatch. Empty string == default
// (correlation), so existing/in-flight workflows are unaffected.
const (
	ExecutorDispatchCorrelation = "correlation"
	ExecutorDispatchTemporal    = "temporal"
)

// ExecutorNativeResult is the wire contract returned by ExecutorNativeActivity
// (orchestrator side). It mirrors the fields the correlation path reads off a
// correlation.Response so the two paths classify identically. JSON tags MUST
// stay in lockstep with the orchestrator's matching struct.
type ExecutorNativeResult struct {
	Status string                 `json:"status"`
	Output map[string]interface{} `json:"output,omitempty"`
	Error  string                 `json:"error,omitempty"`
}

// buildExecutorTaskMap builds the executor task payload from workflow input +
// cached state. PURE / DETERMINISTIC: it copies fields only — no time, no
// randomness, no I/O — so it is safe to call from a workflow (Temporal-native
// path) as well as from an activity (correlation path). Trace-context values are
// passed in by the caller (computed deterministically from input) rather than
// derived here, to keep this function free of any nondeterministic helper.
func buildExecutorTaskMap(input NLPipelineWorkflowV2Input, state *WorkflowState, traceID, traceparent, tracestate string) map[string]interface{} {
	task := map[string]interface{}{
		"pipeline_id":  input.PipelineID,
		"execution_id": input.ExecutionID,
		// user_id threads tenant identity onto the executor task so the orchestrator's
		// getConnectionConfigForTask resolves connection config via the tenant-scoped
		// GetForUser path instead of the tenant-blind fallback (KI-NLCHAT-TENANT-BLIND-FETCH).
		"user_id":                   input.UserID,
		"type":                      "executor",
		"trace_id":                  traceID,
		"traceparent":               traceparent,
		"tracestate":                tracestate,
		"user_request":              input.Message, // enable plan-less table inference
		"pipeline_name":             input.PipelineName,
		"dataset":                   input.Dataset,
		"run_mode":                  input.RunMode,
		"plan":                      state.CachedPlan,
		"validation":                state.CachedValidation,
		"source_connection_id":      state.SourceConnectionID,
		"destination_connection_id": state.DestinationConnectionID,
	}

	// Best-effort: propagate HITL metadata (e.g. UI-provided transforms).
	if state != nil && state.TableSelectionMetadata != nil {
		task["metadata"] = state.TableSelectionMetadata
	}
	// Connector version snapshots (versioned execution).
	if state.SourceConnectorSnapshot != nil {
		task["source"] = map[string]interface{}{
			"type":    state.SourceConnectorSnapshot.Type,
			"version": state.SourceConnectorSnapshot.Version,
		}
	}
	if state.DestinationConnectorSnapshot != nil {
		task["destination"] = map[string]interface{}{
			"type":    state.DestinationConnectorSnapshot.Type,
			"version": state.DestinationConnectorSnapshot.Version,
		}
	}
	if len(state.SelectedTables) > 0 {
		task["selected_tables"] = state.SelectedTables
	}
	return task
}

// classifyExecutorResponse maps an executor agent response (status/error/output)
// into either a success output map or a structured *ActivityError. PURE /
// DETERMINISTIC (string heuristics only) so both the correlation activity and
// the Temporal-native workflow branch classify outcomes identically.
//
// Returning a raw *ActivityError (rather than a Temporal-wrapped error) lets the
// workflow consume it directly via unwrapV2ActivityError (which matches
// *ActivityError through errors.As), while the activity path wraps it with
// toTemporalError before returning across the activity boundary.
//
// NOTE: status "running" is a SUCCESS for streaming/CDC pipelines (Debezium +
// kafka-mcp-sink started). It is intentionally not treated as a failure.
func classifyExecutorResponse(status, errStr string, output map[string]interface{}, correlationID string, state *WorkflowState) (map[string]interface{}, *ActivityError) {
	if status == "success" || status == "completed" || status == "running" {
		return output, nil
	}

	if status == "needs_continuation" {
		// Large-table chunk done, durable checkpoint saved, more rows remain.
		// Surface as a POLICY error (NOT a Temporal retry, NOT a deterministic
		// fail) so the workflow loop deterministically re-dispatches the executor
		// to resume from the checkpoint. Consumes no repair/replan budget.
		return nil, NewPolicyError(PolicyCodeNeedsContinuation, errStr, output)
	}

	if status == "waiting_for_table_selection" {
		return nil, NewPolicyError(PolicyCodeTableSelectionRequired, errStr, output)
	}

	// Convert common operational failures into POLICY errors so the workflow can heal/resume.
	meta := map[string]interface{}{}
	for k, v := range output {
		meta[k] = v
	}
	meta["agent_status"] = status
	meta["agent_error"] = errStr
	meta["correlation_id"] = correlationID
	if state != nil {
		meta["source_connection_id"] = state.SourceConnectionID
		meta["destination_connection_id"] = state.DestinationConnectionID
	}

	// Heuristic classification (beta): auth expiry, invalid config, schema drift.
	if looksLikeAuthError(errStr, meta) {
		// If we can't even identify an oauth_token_id, user likely needs to reconnect.
		code := PolicyCodeAuthRefreshFailed
		if v, ok := meta["oauth_token_id"]; ok && strings.TrimSpace(fmt.Sprint(v)) == "" {
			code = PolicyCodeAuthReauthRequired
		}
		return nil, NewPolicyError(code, errStr, meta)
	}
	if looksLikeInvalidConfig(errStr, meta) {
		return nil, NewPolicyError(PolicyCodeInvalidConfiguration, errStr, meta)
	}
	if looksLikeSchemaDrift(errStr, meta) {
		return nil, NewPolicyError(PolicyCodeSchemaDriftDetected, errStr, meta)
	}

	return nil, NewDeterministicError("EXECUTION_FAILED", errStr, meta)
}
