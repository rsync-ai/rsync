package workflows

import (
	"errors"
	"fmt"
	"strings"
	"time"

	workflowtypes "github.com/rsync-ai/backend-temporal-adapter/internal/workflows/types"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// ==============================================================================
// NLPIPELINEWORKFLOW V2 (Phase 0 - New Workflow Type)
// ==============================================================================
// This is a new workflow implementation that fixes the architectural issues:
// 1. Temporal is the sole state authority (no Kafka-driven state)
// 2. HITL is implemented as Temporal wait states
// 3. Intent is cached and never re-executed on resume
// 4. Activities use request/reply pattern (no KafkaAdapter signals)
// 5. All errors are typed and deterministically handled

const (
	// NLPipelineWorkflowV2Name is the registered Temporal workflow type name.
	// "V2" is vestigial: there is NO V1 NL-pipeline workflow (no
	// NLPipelineWorkflow / nl_pipeline.go exists) — this is the sole, current
	// path. Do NOT rename it to drop "V2": the string is the registered Temporal
	// workflow type, so renaming breaks in-flight and replayed workflows.
	NLPipelineWorkflowV2Name = "NLPipelineWorkflowV2"

	// Signal names (keep existing names for API compatibility)
	SignalConnectionsConfigured = "connections_configured"
	SignalConnectorGenerated    = "connector_generated"
	SignalTablesSelected        = "tables_selected"
	SignalIntentUpdated         = "intent_updated"
	SignalApprovalGiven         = "approval_given"
	SignalCancel                = "cancel"
	SignalPause                 = "pause"
	SignalResume                = "resume"
)

// executorReplanEligible decides whether a failed executor activity should trigger an
// auto-replan. Auto-replan (beta): only genuine deterministic executor business failures
// (DETERMINISTIC / EXECUTION_FAILED) within the replan budget are eligible. Transient/infra
// errors are not replannable, and once the budget is exhausted we fall through to the existing
// terminal-failure path — so prior behavior is preserved, just deferred by N replans. Pure and
// side-effect free so the gate can be unit-tested without a workflow environment.
func executorReplanEligible(execErr *ActivityError, ok bool, replanAttempts, maxReplanAttempts int) bool {
	return ok && execErr != nil &&
		execErr.Type == ErrTypeDeterministic && execErr.Code == "EXECUTION_FAILED" &&
		replanAttempts < maxReplanAttempts
}

// unwrapV2ActivityError extracts our typed *ActivityError from a Temporal activity failure.
// We support two paths:
//   - Preferred: V2 activities wrap typed errors as a Temporal ApplicationError of type v2ActivityErrorTemporalType
//     with ActivityError as details.
//   - Fallback: parse the first "[TYPE:CODE]" fragment from the error string (works with legacy/older runs).
func unwrapV2ActivityError(err error) (*ActivityError, bool) {
	if err == nil {
		return nil, false
	}

	// If it somehow already is our typed error.
	var direct *ActivityError
	if errors.As(err, &direct) && direct != nil {
		return direct, true
	}

	// Preferred: extract from Temporal ApplicationError details.
	//
	// NOTE: in practice Temporal often returns *temporal.ActivityError (and friends) which may not
	// reliably participate in Go's errors.As/Unwrap chain across versions. We defensively walk the
	// cause chain via common interfaces to find an ApplicationError and decode ActivityError details.
	cur := err
	for i := 0; i < 10 && cur != nil; i++ {
		var appErr *temporal.ApplicationError
		if errors.As(cur, &appErr) && appErr != nil {
			if appErr.Type() == v2ActivityErrorTemporalType {
				var v ActivityError
				if derr := appErr.Details(&v); derr == nil {
					return &v, true
				}
			}
		}

		// Try standard unwrapping first
		if u, ok := cur.(interface{ Unwrap() error }); ok {
			next := u.Unwrap()
			if next == nil || next == cur {
				break
			}
			cur = next
			continue
		}

		// Some Temporal errors expose Cause() instead of Unwrap()
		if c, ok := cur.(interface{ Cause() error }); ok {
			next := c.Cause()
			if next == nil || next == cur {
				break
			}
			cur = next
			continue
		}

		break
	}

	// Fallback: parse error string containing "[POLICY:CODE]" etc.
	msg := err.Error()
	open := strings.Index(msg, "[")
	close := strings.Index(msg, "]")
	if open >= 0 && close > open {
		inner := strings.TrimSpace(msg[open+1 : close])
		parts := strings.SplitN(inner, ":", 2)
		if len(parts) == 2 {
			et := ErrorType(strings.TrimSpace(parts[0]))
			code := strings.TrimSpace(parts[1])
			if et == ErrTypePolicy || et == ErrTypeTransient || et == ErrTypeInfra || et == ErrTypeDeterministic {
				return &ActivityError{
					Type:    et,
					Code:    code,
					Message: strings.TrimSpace(msg[close+1:]),
				}, true
			}
		}
	}

	return nil, false
}

// NLPipelineWorkflowV2Input is the input to the V2 workflow
// V2: Minimal input - workflow handles ALL intelligence
type NLPipelineWorkflowV2Input struct {
	PipelineID  string `json:"pipeline_id"`
	ExecutionID string `json:"execution_id"`
	UserID      string `json:"user_id"`
	Message     string `json:"message"` // Raw user message - workflow parses intent

	// Optional execution context for stable storage paths + run-mode semantics.
	// These are provided by the API Gateway on /pipelines/:id/run.
	PipelineName string `json:"pipeline_name,omitempty"`
	Dataset      string `json:"dataset,omitempty"`
	RunMode      string `json:"run_mode,omitempty"` // "resume" or "reload"

	// Optional distributed tracing context (W3C Trace Context).
	// If provided, Temporal activities should propagate it to Kafka commands/events so
	// downstream agents can join the same trace.
	TraceID     string `json:"trace_id,omitempty"`    // 32-char hex (OTel)
	Traceparent string `json:"traceparent,omitempty"` // W3C trace context
	Tracestate  string `json:"tracestate,omitempty"`

	// Connection IDs (optional but critical for end-to-end execution).
	// If provided, they are propagated to ALL activities (esp. executor).
	SourceConnectionID      string `json:"source_connection_id,omitempty"`
	DestinationConnectionID string `json:"destination_connection_id,omitempty"`

	// Optional: immutable connector snapshots (versioned routing)
	SourceConnectorSnapshot      *ConnectorSnapshot `json:"source_connector_snapshot,omitempty"`
	DestinationConnectorSnapshot *ConnectorSnapshot `json:"destination_connector_snapshot,omitempty"`

	// Optional: saved table selection (sticky per pipeline). If provided, executor can run
	// without pausing for HITL table selection.
	SelectedTables []string `json:"selected_tables,omitempty"`

	// Optional execution hints
	BatchSize int `json:"batch_size,omitempty"`
	// Feature flag fields
	SkipConnectorValidation bool `json:"skip_connector_validation,omitempty"`
	SkipApproval            bool `json:"skip_approval,omitempty"`

	// ExecutorDispatch selects how the executor stage is dispatched (Phase 3):
	//   "" / "correlation" (default) — Redis correlation store + orchestrator poller.
	//   "temporal"                   — native Temporal activity on the executor-tasks
	//                                  task queue (no Redis hop).
	// Plumbed through the workflow input (not os.Getenv) so the choice is
	// deterministic/replayable; in-flight workflows finish on the path they started.
	ExecutorDispatch string `json:"executor_dispatch,omitempty"`
}

// Signal payload schemas (Phase C - HITL Contract)
type ConnectionsConfiguredPayload struct {
	PipelineID              string    `json:"pipeline_id"`
	SourceConnectionID      string    `json:"source_connection_id,omitempty"`
	DestinationConnectionID string    `json:"destination_connection_id,omitempty"`
	Timestamp               time.Time `json:"timestamp"`
}

type TablesSelectedPayload struct {
	PipelineID     string                 `json:"pipeline_id"`
	SelectedTables []string               `json:"selected_tables"`
	Timestamp      time.Time              `json:"timestamp"`
	Metadata       map[string]interface{} `json:"metadata,omitempty"`
}

type ConnectorGeneratedPayload struct {
	PipelineID  string    `json:"pipeline_id"`
	ConnectorID string    `json:"connector_id"`
	Timestamp   time.Time `json:"timestamp"`
}

type IntentUpdatedPayload struct {
	PipelineID string    `json:"pipeline_id"`
	NewMessage string    `json:"new_message"`
	Timestamp  time.Time `json:"timestamp"`
}

type ApprovalGivenPayload struct {
	PipelineID string    `json:"pipeline_id"`
	Approved   bool      `json:"approved"`
	Comment    string    `json:"comment,omitempty"`
	Timestamp  time.Time `json:"timestamp"`
}

// NLPipelineWorkflowV2 is the V2 implementation with explicit state machine
func NLPipelineWorkflowV2(ctx workflow.Context, input NLPipelineWorkflowV2Input) error {
	logger := workflow.GetLogger(ctx)
	// Store trace context into workflow context so helper functions can attach it to domain events.
	traceID := input.TraceID
	if traceID == "" {
		// Fallback: stable business identifier
		traceID = input.ExecutionID
	}
	ctx = workflow.WithValue(ctx, "trace_id", traceID)
	ctx = workflow.WithValue(ctx, "traceparent", input.Traceparent)
	ctx = workflow.WithValue(ctx, "tracestate", input.Tracestate)

	logger.Info("🚀 NLPipelineWorkflowV2 started",
		"pipeline_id", input.PipelineID,
		"execution_id", input.ExecutionID,
		"trace_id", traceID,
		"source_connection_id", input.SourceConnectionID,
		"destination_connection_id", input.DestinationConnectionID,
		"batch_size", input.BatchSize,
		"version", "V2")

	// Setup activity options EARLY.
	// IMPORTANT: We must apply ActivityOptions before executing any activity.
	// Otherwise Temporal will reject activity commands with:
	//   BadScheduleActivityAttributes: A valid StartToClose or ScheduleToCloseTimeout is not set on command.
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		HeartbeatTimeout:    30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    time.Minute,
			MaximumAttempts:    3,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	// Initialize workflow state
	state := &WorkflowState{
		CurrentState:   StateInit,
		StateHistory:   []StateTransition{},
		RepairAttempts: map[string]int{},
	}

	paused := false
	cancelled := false

	// Keep pipelines.status in sync with the workflow outcome (completed/failed).
	// We do this in a deterministic defer so EVERY exit path updates the DB row once.
	defer func() {
		var finalStatus string
		switch state.CurrentState {
		case StateCompleted:
			finalStatus = "completed"
		case StateFailed:
			finalStatus = "failed"
		case StateCancelled:
			finalStatus = "stopped"
		default:
			return
		}

		// Best-effort error message from the last transition reason.
		errMsg := ""
		if finalStatus == "failed" && len(state.StateHistory) > 0 {
			errMsg = state.StateHistory[len(state.StateHistory)-1].Reason
		}

		_ = workflow.ExecuteActivity(ctx, UpdatePipelineStatusActivity, input.PipelineID, input.ExecutionID, finalStatus, errMsg).Get(ctx, nil)
	}()

	// Seed connection IDs from workflow input (if provided).
	// These are propagated to planning/validation/execution tasks.
	state.SourceConnectionID = input.SourceConnectionID
	state.DestinationConnectionID = input.DestinationConnectionID

	// Seed connector snapshots from workflow input (if provided).
	// These drive versioned MCP routing at execution time.
	state.SourceConnectorSnapshot = input.SourceConnectorSnapshot
	state.DestinationConnectorSnapshot = input.DestinationConnectorSnapshot

	// Seed selected tables from workflow input (if provided).
	// This allows scheduled/manual runs to reuse a previously confirmed table selection.
	if len(input.SelectedTables) > 0 {
		state.SelectedTables = input.SelectedTables
	}

	// Fast re-run optimization:
	// If the pipeline is already configured (connections + selected tables),
	// skip the expensive agentic phases (intent/resolver/planner/validator) and
	// jump straight to execution.
	fastRerun := state.SourceConnectionID != "" && state.DestinationConnectionID != "" && len(state.SelectedTables) > 0

	// Build an initial execution plan (drives fully data-driven UI).
	// NOTE: Must be deterministic for Temporal replay → use workflow.Now(ctx), not time.Now().
	planBuilder := NewExecutionPlanBuilder(
		input.PipelineID,
		workflow.GetInfo(ctx).WorkflowExecution.ID,
		"batch",
	)
	if fastRerun {
		state.ExecutionPlan = planBuilder.AddExecuteOnlyStages().BuildAt(workflow.Now(ctx))
		logger.Info("⚡ Fast rerun enabled: skipping agentic planning phases",
			"selected_tables", len(state.SelectedTables),
			"source_connection_id", state.SourceConnectionID,
			"destination_connection_id", state.DestinationConnectionID,
		)
	} else {
		state.ExecutionPlan = planBuilder.AddStandardStages().BuildAt(workflow.Now(ctx))
	}

	// Emit PIPELINE_STARTED/CREATED event
	_ = emitDomainEvent(ctx, map[string]interface{}{
		"schema_version": 2,
		"event_type":     "PIPELINE_CREATED",
		"pipeline_id":    input.PipelineID,
		"execution_id":   input.ExecutionID,
		"state":          "running",
		"status":         "processing",
		"timestamp":      workflow.Now(ctx).Format(time.RFC3339),
	})

	// Setup signal channels
	connectionsConfiguredCh := workflow.GetSignalChannel(ctx, SignalConnectionsConfigured)
	connectorGeneratedCh := workflow.GetSignalChannel(ctx, SignalConnectorGenerated)
	tablesSelectedCh := workflow.GetSignalChannel(ctx, SignalTablesSelected)
	intentUpdatedCh := workflow.GetSignalChannel(ctx, SignalIntentUpdated)
	approvalGivenCh := workflow.GetSignalChannel(ctx, SignalApprovalGiven)
	cancelCh := workflow.GetSignalChannel(ctx, SignalCancel)
	pauseCh := workflow.GetSignalChannel(ctx, SignalPause)
	resumeCh := workflow.GetSignalChannel(ctx, SignalResume)

	// Cancellation context
	childCtx, cancelFn := workflow.WithCancel(ctx)
	defer cancelFn()

	// Listen for cancel signal
	workflow.Go(ctx, func(gCtx workflow.Context) {
		cancelCh.Receive(gCtx, nil)
		logger.Info("❌ Cancel signal received")
		cancelled = true
		_ = state.Transition(StateCancelled, workflow.Now(ctx), "Cancelled by user")
		cancelFn()
	})

	// Listen for pause/resume (best-effort; pause takes effect between activities).
	workflow.Go(ctx, func(gCtx workflow.Context) {
		for {
			// Wait for pause
			pauseCh.Receive(gCtx, nil)
			if cancelled {
				return
			}
			paused = true

			// Emit a control-plane event (best-effort) for UI + audit.
			_ = emitDomainEvent(gCtx, map[string]interface{}{
				"schema_version": 2,
				"event_type":     "CONTROL_PLANE",
				"pipeline_id":    input.PipelineID,
				"execution_id":   input.ExecutionID,
				"action":         "pause",
				"state":          "waiting",
				"status":         "waiting_for_user",
				"timestamp":      workflow.Now(gCtx).Format(time.RFC3339),
			})

			// Mark pipeline_progress as waiting (so UI shows paused state).
			_ = workflow.ExecuteActivity(gCtx, StateUpdateActivity, StateUpdateInput{
				PipelineID:  input.PipelineID,
				ExecutionID: input.ExecutionID,
				Stage:       "", // keep current stage
				StageGroup:  "",
				Status:      "waiting_for_user",
				EventType:   "CONTROL_PLANE",
				Summary:     "Paused by user",
				Metadata: map[string]interface{}{
					"paused":               true,
					"blocking_reason_type": "paused_by_user",
				},
			}).Get(gCtx, nil)

			// Wait for resume
			resumeCh.Receive(gCtx, nil)
			if cancelled {
				return
			}
			paused = false

			_ = emitDomainEvent(gCtx, map[string]interface{}{
				"schema_version": 2,
				"event_type":     "CONTROL_PLANE",
				"pipeline_id":    input.PipelineID,
				"execution_id":   input.ExecutionID,
				"action":         "resume",
				"state":          "running",
				"status":         "processing",
				"timestamp":      workflow.Now(gCtx).Format(time.RFC3339),
			})

			_ = workflow.ExecuteActivity(gCtx, StateUpdateActivity, StateUpdateInput{
				PipelineID:  input.PipelineID,
				ExecutionID: input.ExecutionID,
				Status:      "processing",
				EventType:   "CONTROL_PLANE",
				Summary:     "Resumed",
				Metadata: map[string]interface{}{
					"paused": false,
				},
			}).Get(gCtx, nil)
		}
	})

	awaitIfPaused := func() error {
		if cancelled {
			return workflow.ErrCanceled
		}
		if !paused {
			return nil
		}
		// Block until resume or cancel.
		_ = workflow.Await(ctx, func() bool { return !paused || cancelled })
		if cancelled {
			return workflow.ErrCanceled
		}
		return nil
	}

	if !fastRerun {
		// ==========================================================================
		// PHASE 1: INTENT RESOLUTION
		// ==========================================================================
		if err := awaitIfPaused(); err != nil {
			return err
		}
		if err := state.Transition(StateIntentResolved, workflow.Now(ctx), "Starting intent resolution"); err != nil {
			return err
		}
		_ = emitStageEvent(ctx, input.PipelineID, input.ExecutionID, "intent", "STAGE_STARTED", state.ExecutionPlan, nil)

		var intentResult map[string]interface{}
		err := workflow.ExecuteActivity(ctx, IntentActivityV2, input).Get(ctx, &intentResult)
		if err != nil {
			if cancelled {
				return nil
			}
			_ = emitStageEvent(ctx, input.PipelineID, input.ExecutionID, "intent", "STAGE_FAILED", state.ExecutionPlan, map[string]interface{}{
				"error_message": err.Error(),
			})
			_ = state.Transition(StateFailed, workflow.Now(ctx), fmt.Sprintf("Intent failed: %v", err))
			return err
		}
		// Extract source/dest types from intent for UI display
		intentSrcType := ""
		intentDstType := ""
		if intentResult != nil {
			for _, key := range []string{"source_type", "source_category", "source_connector"} {
				if v, ok := intentResult[key].(string); ok && v != "" {
					intentSrcType = v
					break
				}
			}
			for _, key := range []string{"destination_type", "destination_category", "destination_connector"} {
				if v, ok := intentResult[key].(string); ok && v != "" {
					intentDstType = v
					break
				}
			}
			// Nested intent payload
			if nested, ok := intentResult["intent"].(map[string]interface{}); ok {
				for _, key := range []string{"source_type", "source_category", "source_connector"} {
					if v, ok := nested[key].(string); ok && v != "" && intentSrcType == "" {
						intentSrcType = v
						break
					}
				}
				for _, key := range []string{"destination_type", "destination_category", "destination_connector"} {
					if v, ok := nested[key].(string); ok && v != "" && intentDstType == "" {
						intentDstType = v
						break
					}
				}
			}
		}
		intentMeta := map[string]interface{}{}
		if intentSrcType != "" {
			intentMeta["source_connector_type"] = intentSrcType
		}
		if intentDstType != "" {
			intentMeta["destination_connector_type"] = intentDstType
		}
		_ = emitStageEvent(ctx, input.PipelineID, input.ExecutionID, "intent", "STAGE_COMPLETED", state.ExecutionPlan, intentMeta)
		state.CachedIntent = intentResult
		logger.Info("✅ Intent resolved and cached", "result", intentResult)

		// Update state in DB (authoritative write)
		stateMetaForIntent := map[string]interface{}{
			"execution_plan": state.ExecutionPlan,
		}
		if intentSrcType != "" {
			stateMetaForIntent["source_connector_type"] = intentSrcType
		}
		if intentDstType != "" {
			stateMetaForIntent["destination_connector_type"] = intentDstType
		}
		_ = workflow.ExecuteActivity(ctx, StateUpdateActivity, StateUpdateInput{
			PipelineID:  input.PipelineID,
			ExecutionID: input.ExecutionID,
			Stage:       "intent",
			StageGroup:  "understanding",
			// Per-stage completion: the overall pipeline is still running. Writing
			// "completed" here clobbers pipeline_progress.status and makes the UI
			// flash a premature "Completed/100%" badge mid-pipeline. Only a real
			// PIPELINE_COMPLETED event sets the overall status to "completed".
			Status:    "processing",
			EventType: "STAGE_COMPLETED",
			Summary:   "Intent resolved successfully",
			Metadata:  stateMetaForIntent,
		}).Get(ctx, nil)

		// ==========================================================================
		// PHASE 2: CONNECTOR RESOLUTION (with HITL loop)
		// ==========================================================================
		if err := state.Transition(StateConnectorResolved, workflow.Now(ctx), "Starting connector resolution"); err != nil {
			return err
		}
		_ = emitStageEvent(ctx, input.PipelineID, input.ExecutionID, "capability_resolver", "STAGE_STARTED", state.ExecutionPlan, nil)

		// Connector resolution loop (may enter wait states)
		for {
			if err := awaitIfPaused(); err != nil {
				if cancelled {
					return nil
				}
				return err
			}
			// Check for intent update signal
			var intentUpdate IntentUpdatedPayload
			if intentUpdatedCh.ReceiveAsync(&intentUpdate) {
				logger.Info("🔄 Intent updated, invalidating cache")
				state.CachedIntent = nil
				// Re-execute intent
				err = workflow.ExecuteActivity(childCtx, IntentActivityV2, NLPipelineWorkflowV2Input{
					PipelineID:  input.PipelineID,
					ExecutionID: input.ExecutionID,
					UserID:      input.UserID,
					Message:     intentUpdate.NewMessage,
					// Preserve connection IDs across intent updates
					SourceConnectionID:      input.SourceConnectionID,
					DestinationConnectionID: input.DestinationConnectionID,
					BatchSize:               input.BatchSize,
				}).Get(childCtx, &intentResult)
				if err != nil {
					if cancelled {
						return nil
					}
					_ = state.Transition(StateFailed, workflow.Now(ctx), fmt.Sprintf("Intent re-execution failed: %v", err))
					return err
				}
				state.CachedIntent = intentResult
			}

			var connectorResult map[string]interface{}
			err = workflow.ExecuteActivity(childCtx, ConnectorResolverActivityV2, input, state).Get(childCtx, &connectorResult)
			if err != nil {
				if cancelled {
					return nil
				}
				// Handle typed errors
				if activityErr, ok := unwrapV2ActivityError(err); ok {
					switch activityErr.Type {
					case ErrTypePolicy:
						// Enter wait state based on policy error code
						if activityErr.Code == PolicyCodeMissingConnectors {
							if err := state.Transition(StateWaitingForConnectorGeneration, workflow.Now(ctx), "Missing connectors"); err != nil {
								return err
							}
							state.SetWaitReason(&WaitReason{
								Type:              WaitReasonConnectorGeneration,
								WaitingSince:      workflow.Now(ctx),
								TimeoutAt:         workflow.Now(ctx).Add(24 * time.Hour),
								MissingConnectors: getMissingConnectors(activityErr.Metadata),
							})
							logger.Info("⏳ Waiting for connector generation", "missing", state.WaitReason.MissingConnectors)

							// Emit V2 PIPELINE_WAITING event for connector generation
							// IMPORTANT: include the activity metadata (resolved_connectors, required_connectors, etc)
							// so the UI can show which connectors are available vs missing.
							waitDetails := map[string]interface{}{}
							for k, v := range activityErr.Metadata {
								waitDetails[k] = v
							}
							waitDetails["missing_connectors"] = state.WaitReason.MissingConnectors
							_ = emitPipelineWaitingEvent(ctx, input.PipelineID, input.ExecutionID, "capability_resolver", "connector_generation", state.ExecutionPlan, waitDetails)

							// Wait for connector generation signal
							var connectorPayload ConnectorGeneratedPayload
							if !awaitHITLSignal(ctx, connectorGeneratedCh, &connectorPayload, state.WaitReason.TimeoutAt) {
								msg := hitlTimeoutMessage("a generated connector", state.WaitReason.TimeoutAt, state.WaitReason.WaitingSince)
								_ = emitStageEvent(ctx, input.PipelineID, input.ExecutionID, "capability_resolver", "STAGE_FAILED", state.ExecutionPlan, map[string]interface{}{
									"error_message": msg,
								})
								_ = state.Transition(StateFailed, workflow.Now(ctx), msg)
								return errors.New(msg)
							}
							logger.Info("✅ Connector generated signal received", "connector_id", connectorPayload.ConnectorID)

							// Clear wait reason and retry
							state.ClearWaitReason()
							if err := state.Transition(StateConnectorResolved, workflow.Now(ctx), "Retrying after connector generation"); err != nil {
								return err
							}
							continue

						} else if activityErr.Code == PolicyCodeMissingConnections {
							if err := state.Transition(StateWaitingForConnectionConfiguration, workflow.Now(ctx), "Missing connections"); err != nil {
								return err
							}
							state.SetWaitReason(&WaitReason{
								Type:         WaitReasonConnectionConfiguration,
								WaitingSince: workflow.Now(ctx),
								TimeoutAt:    workflow.Now(ctx).Add(24 * time.Hour),
								Context:      activityErr.Metadata,
							})
							logger.Info("⏳ Waiting for connection configuration")

							// Emit V2 PIPELINE_WAITING event with proper schema
							// IMPORTANT: stage id must match the execution plan stage key to avoid creating a duplicate/phantom stage in the UI.
							_ = emitPipelineWaitingEvent(ctx, input.PipelineID, input.ExecutionID, "connection_validation", "connection_config", state.ExecutionPlan, activityErr.Metadata)

							// Wait for connections configured signal (with validation loop)
							// P0-3: Cap validation attempts to prevent infinite loops
							const maxConnectionValidationAttempts = 3
							validationAttempts := 0

							for {
								// Check max attempts
								if validationAttempts >= maxConnectionValidationAttempts {
									_ = emitStageEvent(ctx, input.PipelineID, input.ExecutionID, "connection_validation", "STAGE_FAILED", state.ExecutionPlan, map[string]interface{}{
										"error_message": fmt.Sprintf("Connection validation failed after %d attempts", maxConnectionValidationAttempts),
									})
									_ = state.Transition(StateFailed, workflow.Now(ctx), fmt.Sprintf("Connection validation failed after %d attempts", maxConnectionValidationAttempts))
									return fmt.Errorf("connection validation failed after %d attempts - user provided invalid connections repeatedly", maxConnectionValidationAttempts)
								}

								validationAttempts++
								logger.Info("⏳ Waiting for connection configuration", "attempt", validationAttempts, "max_attempts", maxConnectionValidationAttempts)

								var connectionPayload ConnectionsConfiguredPayload
								if !awaitHITLSignal(ctx, connectionsConfiguredCh, &connectionPayload, state.WaitReason.TimeoutAt) {
									msg := hitlTimeoutMessage("connections to be configured", state.WaitReason.TimeoutAt, state.WaitReason.WaitingSince)
									_ = emitStageEvent(ctx, input.PipelineID, input.ExecutionID, "connection_validation", "STAGE_FAILED", state.ExecutionPlan, map[string]interface{}{
										"error_message": msg,
									})
									_ = state.Transition(StateFailed, workflow.Now(ctx), msg)
									return errors.New(msg)
								}
								logger.Info("✅ Connections configured signal received",
									"source", connectionPayload.SourceConnectionID,
									"dest", connectionPayload.DestinationConnectionID,
									"attempt", validationAttempts)

								// Store connection IDs
								state.SourceConnectionID = connectionPayload.SourceConnectionID
								state.DestinationConnectionID = connectionPayload.DestinationConnectionID

								// Validate connections (Phase C3)
								err := workflow.ExecuteActivity(ctx, ConnectionValidatorActivityV2, input, state).Get(ctx, nil)
								if err == nil {
									// Valid connections, proceed
									state.ClearWaitReason()
									if err := state.Transition(StateConnectorResolved, workflow.Now(ctx), "Connections validated"); err != nil {
										return err
									}
									break
								}

								// Invalid connections, stay in wait state
								logger.Warn("⚠️ Invalid connections provided, staying in wait state",
									"error", err,
									"attempt", validationAttempts,
									"remaining_attempts", maxConnectionValidationAttempts-validationAttempts)
								// Loop back to wait for corrected connections
							}
							continue
						}

					case ErrTypeTransient:
						// Retry via Temporal's automatic retry
						return fmt.Errorf("transient error in connector resolution: %w", err)

					case ErrTypeInfra:
						_ = emitStageEvent(ctx, input.PipelineID, input.ExecutionID, "capability_resolver", "STAGE_FAILED", state.ExecutionPlan, map[string]interface{}{
							"error_message": err.Error(),
						})
						_ = state.Transition(StateFailed, workflow.Now(ctx), fmt.Sprintf("Infrastructure failure: %v", err))
						return err

					case ErrTypeDeterministic:
						_ = emitStageEvent(ctx, input.PipelineID, input.ExecutionID, "capability_resolver", "STAGE_FAILED", state.ExecutionPlan, map[string]interface{}{
							"error_message": err.Error(),
						})
						_ = state.Transition(StateFailed, workflow.Now(ctx), fmt.Sprintf("Deterministic failure: %v", err))
						return err
					}
				}
				// Unknown error
				_ = emitStageEvent(ctx, input.PipelineID, input.ExecutionID, "capability_resolver", "STAGE_FAILED", state.ExecutionPlan, map[string]interface{}{
					"error_message": err.Error(),
				})
				_ = state.Transition(StateFailed, workflow.Now(ctx), fmt.Sprintf("Connector resolution failed: %v", err))
				return err
			}

			// Success - cache and break loop
			// Extract resolved connector types to show in UI
			resolverMeta := map[string]interface{}{}
			if rc, ok := connectorResult["resolved_connectors"].(map[string]interface{}); ok {
				if t, ok := rc["source"].(string); ok && t != "" {
					resolverMeta["source_connector_type"] = t
				}
				if t, ok := rc["destination"].(string); ok && t != "" {
					resolverMeta["destination_connector_type"] = t
				}
			}
			if intentSrcType != "" && resolverMeta["source_connector_type"] == nil {
				resolverMeta["source_connector_type"] = intentSrcType
			}
			if intentDstType != "" && resolverMeta["destination_connector_type"] == nil {
				resolverMeta["destination_connector_type"] = intentDstType
			}
			_ = emitStageEvent(ctx, input.PipelineID, input.ExecutionID, "capability_resolver", "STAGE_COMPLETED", state.ExecutionPlan, resolverMeta)
			state.CachedConnectorResolution = connectorResult

			// PHASE 2.4 FIX: Extract connection IDs from resolver result
			if sourceConnID, ok := connectorResult["source_connection_id"].(string); ok && sourceConnID != "" {
				state.SourceConnectionID = sourceConnID
				logger.Info("✅ Source connection ID set from resolver", "id", sourceConnID)
			}
			if destConnID, ok := connectorResult["destination_connection_id"].(string); ok && destConnID != "" {
				state.DestinationConnectionID = destConnID
				logger.Info("✅ Destination connection ID set from resolver", "id", destConnID)
			}

			logger.Info("✅ Connectors resolved and cached", "result", connectorResult)
			break
		}

		// ==========================================================================
		// PHASE 2.5: CONNECTOR AVAILABILITY CHECK (filesystem) + OPTIONAL AUTO-GENERATION
		// ==========================================================================
		if !input.SkipConnectorValidation {
			_ = emitStageEvent(ctx, input.PipelineID, input.ExecutionID, "connector_check", "STAGE_STARTED", state.ExecutionPlan, nil)

			// Derive connector types from cached intent or resolution output.
			sourceType := ""
			destType := ""

			if state.CachedIntent != nil {
				if v, ok := state.CachedIntent["source_type"].(string); ok && v != "" {
					sourceType = v
				}
				if v, ok := state.CachedIntent["source_category"].(string); ok && v != "" && sourceType == "" {
					sourceType = v
				}
				if v, ok := state.CachedIntent["source_connector"].(string); ok && v != "" && sourceType == "" {
					sourceType = v
				}
				if v, ok := state.CachedIntent["destination_type"].(string); ok && v != "" {
					destType = v
				}
				if v, ok := state.CachedIntent["destination_category"].(string); ok && v != "" && destType == "" {
					destType = v
				}
				if v, ok := state.CachedIntent["destination_connector"].(string); ok && v != "" && destType == "" {
					destType = v
				}

				// Nested intent payload (IntentWorker returns Output.intent)
				if nested, ok := state.CachedIntent["intent"].(map[string]interface{}); ok {
					if v, ok := nested["source_type"].(string); ok && v != "" && sourceType == "" {
						sourceType = v
					}
					if v, ok := nested["source_category"].(string); ok && v != "" && sourceType == "" {
						sourceType = v
					}
					if v, ok := nested["destination_type"].(string); ok && v != "" && destType == "" {
						destType = v
					}
					if v, ok := nested["destination_category"].(string); ok && v != "" && destType == "" {
						destType = v
					}
				}
			}

			// Prefer resolved connector IDs (canonical names, e.g. "aws-s3") when available.
			if rc, ok := state.CachedConnectorResolution["resolved_connectors"].(map[string]interface{}); ok {
				if t, ok := rc["source"].(string); ok && t != "" {
					sourceType = t
				}
				if t, ok := rc["destination"].(string); ok && t != "" {
					destType = t
				}
			}

			if sourceType == "" || destType == "" {
				// Fallback: inspect capability resolver output (resolved_connectors.{source|destination}.type)
				if rc, ok := state.CachedConnectorResolution["resolved_connectors"].(map[string]interface{}); ok {
					if sourceType == "" {
						if t, ok := rc["source"].(string); ok && t != "" {
							sourceType = t
						} else if src, ok := rc["source"].(map[string]interface{}); ok {
							if t, ok := src["type"].(string); ok && t != "" {
								sourceType = t
							}
						}
					}
					if destType == "" {
						if t, ok := rc["destination"].(string); ok && t != "" {
							destType = t
						} else if dst, ok := rc["destination"].(map[string]interface{}); ok {
							if t, ok := dst["type"].(string); ok && t != "" {
								destType = t
							}
						}
					}
				}
			}

			if sourceType == "" || destType == "" {
				logger.Warn("⚠️ Connector types missing; skipping connector availability check", "source_type", sourceType, "dest_type", destType)
				_ = emitStageEvent(ctx, input.PipelineID, input.ExecutionID, "connector_check", "STAGE_COMPLETED", state.ExecutionPlan, map[string]interface{}{
					"skipped_reason": "connector types missing",
				})
			} else {
				var checkResult ConnectorCheckResult
				checkReq := ConnectorCheckRequest{
					CorrelationID:   fmt.Sprintf("%s-%s", input.PipelineID, input.ExecutionID),
					SourceType:      sourceType,
					DestinationType: destType,
				}

				err = workflow.ExecuteActivity(childCtx, ConnectorAvailabilityActivityV2, checkReq).Get(childCtx, &checkResult)
				if err != nil {
					_ = emitStageEvent(ctx, input.PipelineID, input.ExecutionID, "connector_check", "STAGE_FAILED", state.ExecutionPlan, map[string]interface{}{
						"error_message": err.Error(),
					})
					_ = state.Transition(StateFailed, workflow.Now(ctx), fmt.Sprintf("Connector availability check failed: %v", err))
					return err
				}

				// Auto-generate missing connectors when possible.
				if !checkResult.AllConnectorsAvailable && len(checkResult.MissingConnectors) > 0 {
					for _, missing := range checkResult.MissingConnectors {
						if !missing.CanGenerate {
							_ = emitStageEvent(ctx, input.PipelineID, input.ExecutionID, "connector_check", "STAGE_FAILED", state.ExecutionPlan, map[string]interface{}{
								"error_message": fmt.Sprintf("Missing connector: %s", missing.Type),
							})
							_ = state.Transition(StateFailed, workflow.Now(ctx), fmt.Sprintf("Missing connector: %s", missing.Type))
							return fmt.Errorf("missing connector %s: %s", missing.Type, missing.Message)
						}

						_ = emitStageEvent(ctx, input.PipelineID, input.ExecutionID, "connector_generation", "STAGE_STARTED", state.ExecutionPlan, map[string]interface{}{
							"connector_type": missing.Type,
						})

						// Longer timeout for generation.
						genOpts := workflow.ActivityOptions{
							StartToCloseTimeout: 3 * time.Minute,
							HeartbeatTimeout:    30 * time.Second,
							RetryPolicy: &temporal.RetryPolicy{
								InitialInterval:    time.Second,
								BackoffCoefficient: 2.0,
								MaximumInterval:    time.Minute,
								MaximumAttempts:    3,
							},
						}
						genCtx := workflow.WithActivityOptions(ctx, genOpts)

						var genResult GenerateConnectorResult
						genReq := GenerateConnectorRequest{
							CorrelationID: fmt.Sprintf("%s-%s", input.PipelineID, input.ExecutionID),
							ConnectorType: missing.Type,
						}
						genErr := workflow.ExecuteActivity(genCtx, GenerateConnectorActivityV2, genReq).Get(genCtx, &genResult)
						if genErr != nil || !genResult.Success {
							errMsg := ""
							if genErr != nil {
								errMsg = genErr.Error()
							} else {
								errMsg = "generation returned success=false"
							}
							_ = emitStageEvent(ctx, input.PipelineID, input.ExecutionID, "connector_generation", "STAGE_FAILED", state.ExecutionPlan, map[string]interface{}{
								"error_message":  errMsg,
								"connector_type": missing.Type,
							})
							_ = emitStageEvent(ctx, input.PipelineID, input.ExecutionID, "connector_check", "STAGE_FAILED", state.ExecutionPlan, map[string]interface{}{
								"error_message": fmt.Sprintf("Connector generation failed for %s", missing.Type),
							})
							_ = state.Transition(StateFailed, workflow.Now(ctx), fmt.Sprintf("Connector generation failed for %s: %v", missing.Type, genErr))
							if genErr != nil {
								return genErr
							}
							return fmt.Errorf("connector generation failed for %s", missing.Type)
						}

						_ = emitStageEvent(ctx, input.PipelineID, input.ExecutionID, "connector_generation", "STAGE_COMPLETED", state.ExecutionPlan, map[string]interface{}{
							"connector_type": missing.Type,
							"connector_path": genResult.ConnectorPath,
						})
					}

					// Re-check after generation.
					var recheck ConnectorCheckResult
					err = workflow.ExecuteActivity(childCtx, ConnectorAvailabilityActivityV2, checkReq).Get(childCtx, &recheck)
					if err != nil {
						_ = emitStageEvent(ctx, input.PipelineID, input.ExecutionID, "connector_check", "STAGE_FAILED", state.ExecutionPlan, map[string]interface{}{
							"error_message": err.Error(),
						})
						_ = state.Transition(StateFailed, workflow.Now(ctx), fmt.Sprintf("Connector re-check failed: %v", err))
						return err
					}
					if !recheck.AllConnectorsAvailable {
						_ = emitStageEvent(ctx, input.PipelineID, input.ExecutionID, "connector_check", "STAGE_FAILED", state.ExecutionPlan, map[string]interface{}{
							"error_message": "connectors still missing after generation",
						})
						_ = state.Transition(StateFailed, workflow.Now(ctx), "Missing connectors after generation")
						return fmt.Errorf("connectors still missing after generation")
					}
				}

				_ = emitStageEvent(ctx, input.PipelineID, input.ExecutionID, "connector_check", "STAGE_COMPLETED", state.ExecutionPlan, map[string]interface{}{
					"source_type":      sourceType,
					"destination_type": destType,
				})
			}
		}

		// ==========================================================================
		// PHASE 2.6: CONNECTION VALIDATION (ensure IDs exist BEFORE planning/execution)
		// ==========================================================================
		_ = emitStageEvent(ctx, input.PipelineID, input.ExecutionID, "connection_validation", "STAGE_STARTED", state.ExecutionPlan, nil)

		// Derive connector types for connection lookup (same logic as connector check).
		sourceTypeForConn := ""
		destTypeForConn := ""
		if state.CachedIntent != nil {
			if v, ok := state.CachedIntent["source_type"].(string); ok && v != "" {
				sourceTypeForConn = v
			}
			if v, ok := state.CachedIntent["source_category"].(string); ok && v != "" && sourceTypeForConn == "" {
				sourceTypeForConn = v
			}
			if v, ok := state.CachedIntent["source_connector"].(string); ok && v != "" && sourceTypeForConn == "" {
				sourceTypeForConn = v
			}
			if v, ok := state.CachedIntent["destination_type"].(string); ok && v != "" {
				destTypeForConn = v
			}
			if v, ok := state.CachedIntent["destination_category"].(string); ok && v != "" && destTypeForConn == "" {
				destTypeForConn = v
			}
			if v, ok := state.CachedIntent["destination_connector"].(string); ok && v != "" && destTypeForConn == "" {
				destTypeForConn = v
			}

			if nested, ok := state.CachedIntent["intent"].(map[string]interface{}); ok {
				if v, ok := nested["source_type"].(string); ok && v != "" && sourceTypeForConn == "" {
					sourceTypeForConn = v
				}
				if v, ok := nested["source_category"].(string); ok && v != "" && sourceTypeForConn == "" {
					sourceTypeForConn = v
				}
				if v, ok := nested["destination_type"].(string); ok && v != "" && destTypeForConn == "" {
					destTypeForConn = v
				}
				if v, ok := nested["destination_category"].(string); ok && v != "" && destTypeForConn == "" {
					destTypeForConn = v
				}
			}
		}

		// Prefer resolved connector IDs (canonical names) when available.
		if rc, ok := state.CachedConnectorResolution["resolved_connectors"].(map[string]interface{}); ok {
			if t, ok := rc["source"].(string); ok && t != "" {
				sourceTypeForConn = t
			}
			if t, ok := rc["destination"].(string); ok && t != "" {
				destTypeForConn = t
			}
		}
		if sourceTypeForConn == "" || destTypeForConn == "" {
			if rc, ok := state.CachedConnectorResolution["resolved_connectors"].(map[string]interface{}); ok {
				if sourceTypeForConn == "" {
					if t, ok := rc["source"].(string); ok && t != "" {
						sourceTypeForConn = t
					} else if src, ok := rc["source"].(map[string]interface{}); ok {
						if t, ok := src["type"].(string); ok && t != "" {
							sourceTypeForConn = t
						}
					}
				}
				if destTypeForConn == "" {
					if t, ok := rc["destination"].(string); ok && t != "" {
						destTypeForConn = t
					} else if dst, ok := rc["destination"].(map[string]interface{}); ok {
						if t, ok := dst["type"].(string); ok && t != "" {
							destTypeForConn = t
						}
					}
				}
			}
		}

		var connValidation ConnectionValidationResult
		connReq := ConnectionValidationRequest{
			CorrelationID:           fmt.Sprintf("%s-%s", input.PipelineID, input.ExecutionID),
			UserID:                  input.UserID,
			SourceType:              sourceTypeForConn,
			DestinationType:         destTypeForConn,
			SourceConnectionID:      state.SourceConnectionID,
			DestinationConnectionID: state.DestinationConnectionID,
			UserRequest:             input.Message,
		}
		err = workflow.ExecuteActivity(childCtx, ConnectionValidationActivityV2, connReq).Get(childCtx, &connValidation)
		if err != nil {
			_ = emitStageEvent(ctx, input.PipelineID, input.ExecutionID, "connection_validation", "STAGE_FAILED", state.ExecutionPlan, map[string]interface{}{
				"error_message": err.Error(),
			})
			_ = state.Transition(StateFailed, workflow.Now(ctx), fmt.Sprintf("Connection validation activity failed: %v", err))
			return err
		}

		if !connValidation.AllConnectionsValid {
			// Enter wait state and request user configuration.
			if err := state.Transition(StateWaitingForConnectionConfiguration, workflow.Now(ctx), "Missing connections"); err != nil {
				_ = emitStageEvent(ctx, input.PipelineID, input.ExecutionID, "connection_validation", "STAGE_FAILED", state.ExecutionPlan, map[string]interface{}{
					"error_message": err.Error(),
				})
				return err
			}
			state.SetWaitReason(&WaitReason{
				Type:         WaitReasonConnectionConfiguration,
				WaitingSince: workflow.Now(ctx),
				TimeoutAt:    workflow.Now(ctx).Add(24 * time.Hour),
				Context: map[string]interface{}{
					"missing_connections": connValidation.MissingConnections,
					"source_type":         sourceTypeForConn,
					"destination_type":    destTypeForConn,
				},
			})
			_ = emitPipelineWaitingEvent(ctx, input.PipelineID, input.ExecutionID, "connection_validation", "connection_config", state.ExecutionPlan, map[string]interface{}{
				"missing_connections": connValidation.MissingConnections,
				"source_type":         sourceTypeForConn,
				"destination_type":    destTypeForConn,
				// Optional UX helpers (additive; older UI ignores unknown fields)
				"user_request": input.Message,
				"suggestions":  connValidation.Suggestions,
			})

			// Wait for connections configured signal and validate.
			const maxConnectionValidationAttempts = 3
			validationAttempts := 0
			for {
				if validationAttempts >= maxConnectionValidationAttempts {
					_ = emitStageEvent(ctx, input.PipelineID, input.ExecutionID, "connection_validation", "STAGE_FAILED", state.ExecutionPlan, map[string]interface{}{
						"error_message": fmt.Sprintf("Connection validation failed after %d attempts", maxConnectionValidationAttempts),
					})
					_ = state.Transition(StateFailed, workflow.Now(ctx), fmt.Sprintf("Connection validation failed after %d attempts", maxConnectionValidationAttempts))
					return fmt.Errorf("connection validation failed after %d attempts", maxConnectionValidationAttempts)
				}
				validationAttempts++

				var connectionPayload ConnectionsConfiguredPayload
				if !awaitHITLSignal(ctx, connectionsConfiguredCh, &connectionPayload, state.WaitReason.TimeoutAt) {
					msg := hitlTimeoutMessage("connections to be configured", state.WaitReason.TimeoutAt, state.WaitReason.WaitingSince)
					_ = emitStageEvent(ctx, input.PipelineID, input.ExecutionID, "connection_validation", "STAGE_FAILED", state.ExecutionPlan, map[string]interface{}{
						"error_message": msg,
					})
					_ = state.Transition(StateFailed, workflow.Now(ctx), msg)
					return errors.New(msg)
				}

				state.SourceConnectionID = connectionPayload.SourceConnectionID
				state.DestinationConnectionID = connectionPayload.DestinationConnectionID

				// Validate connections via existing validator agent.
				if err := awaitIfPaused(); err != nil {
					if cancelled {
						return nil
					}
					return err
				}
				validateErr := workflow.ExecuteActivity(ctx, ConnectionValidatorActivityV2, input, state).Get(ctx, nil)
				if validateErr == nil {
					state.ClearWaitReason()
					_ = state.Transition(StateConnectorResolved, workflow.Now(ctx), "Connections validated")
					break
				}
			}
		} else {
			// Update state with validated IDs (either explicit or looked up).
			state.SourceConnectionID = connValidation.SourceConnectionID
			state.DestinationConnectionID = connValidation.DestinationConnectionID
		}

		// Ensure subsequent activities see validated IDs.
		input.SourceConnectionID = state.SourceConnectionID
		input.DestinationConnectionID = state.DestinationConnectionID

		_ = emitStageEvent(ctx, input.PipelineID, input.ExecutionID, "connection_validation", "STAGE_COMPLETED", state.ExecutionPlan, map[string]interface{}{
			"source_connection_id":      state.SourceConnectionID,
			"destination_connection_id": state.DestinationConnectionID,
		})

		// Persist resolved connector types so downstream stages (and the frontend) can access them.
		if state.SourceConnectorType == "" && sourceTypeForConn != "" {
			state.SourceConnectorType = sourceTypeForConn
		}
		if state.DestinationConnectorType == "" && destTypeForConn != "" {
			state.DestinationConnectorType = destTypeForConn
		}

		// ==========================================================================
		// PHASE 3: PLANNING
		// ==========================================================================
		if err := awaitIfPaused(); err != nil {
			if cancelled {
				return nil
			}
			return err
		}
		if err := state.Transition(StatePlanned, workflow.Now(ctx), "Starting planning"); err != nil {
			return err
		}
		_ = emitStageEvent(ctx, input.PipelineID, input.ExecutionID, "planner", "STAGE_STARTED", state.ExecutionPlan, nil)

		var planResult map[string]interface{}
		err = workflow.ExecuteActivity(childCtx, PlannerActivityV2, input, state).Get(childCtx, &planResult)
		if err != nil {
			if cancelled {
				return nil
			}
			_ = emitStageEvent(ctx, input.PipelineID, input.ExecutionID, "planner", "STAGE_FAILED", state.ExecutionPlan, map[string]interface{}{
				"error_message": err.Error(),
			})
			_ = state.Transition(StateFailed, workflow.Now(ctx), fmt.Sprintf("Planning failed: %v", err))
			return err
		}
		_ = emitStageEvent(ctx, input.PipelineID, input.ExecutionID, "planner", "STAGE_COMPLETED", state.ExecutionPlan, map[string]interface{}{
			"result_summary": "Plan created",
		})
		state.CachedPlan = planResult
		logger.Info("✅ Plan created and cached")

		_ = workflow.ExecuteActivity(ctx, StateUpdateActivity, StateUpdateInput{
			PipelineID:  input.PipelineID,
			ExecutionID: input.ExecutionID,
			Stage:       "planner",
			StageGroup:  "planning",
			// Per-stage completion — keep overall status "processing" (see intent note).
			Status:    "processing",
			EventType: "STAGE_COMPLETED",
			Summary:   "Plan created",
			Metadata: map[string]interface{}{
				"execution_plan":             state.ExecutionPlan,
				"source_connector_type":      state.SourceConnectorType,
				"destination_connector_type": state.DestinationConnectorType,
				"run_mode":                   input.RunMode,
			},
		}).Get(ctx, nil)

		// ==========================================================================
		// PHASE 3b: COST ESTIMATION (advisory — never blocks the pipeline)
		// ==========================================================================
		costOpts := workflow.ActivityOptions{
			StartToCloseTimeout: 5 * time.Minute,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
		}
		var costEstimate map[string]interface{}
		_ = workflow.ExecuteActivity(
			workflow.WithActivityOptions(ctx, costOpts),
			CostEstimatorActivityV2, input, state,
		).Get(ctx, &costEstimate)
		if costEstimate != nil {
			state.CachedCostEstimate = costEstimate
			logger.Info("💰 Cost estimate received",
				"risk_level", costEstimate["risk_level"],
				"estimated_rows", costEstimate["estimated_rows"],
			)
		}

		// ==========================================================================
		// PHASE 4: VALIDATION
		// ==========================================================================
		if err := awaitIfPaused(); err != nil {
			if cancelled {
				return nil
			}
			return err
		}
		if err := state.Transition(StateValidated, workflow.Now(ctx), "Starting validation"); err != nil {
			return err
		}
		_ = emitStageEvent(ctx, input.PipelineID, input.ExecutionID, "validator", "STAGE_STARTED", state.ExecutionPlan, nil)

		var validationResult map[string]interface{}
		err = workflow.ExecuteActivity(childCtx, ValidatorActivityV2, input, state).Get(childCtx, &validationResult)
		if err != nil {
			if cancelled {
				return nil
			}
			_ = emitStageEvent(ctx, input.PipelineID, input.ExecutionID, "validator", "STAGE_FAILED", state.ExecutionPlan, map[string]interface{}{
				"error_message": err.Error(),
			})
			_ = state.Transition(StateFailed, workflow.Now(ctx), fmt.Sprintf("Validation failed: %v", err))
			return err
		}
		_ = emitStageEvent(ctx, input.PipelineID, input.ExecutionID, "validator", "STAGE_COMPLETED", state.ExecutionPlan, map[string]interface{}{
			"result_summary": "Plan validated",
		})
		state.CachedValidation = validationResult
		logger.Info("✅ Plan validated")

		_ = workflow.ExecuteActivity(ctx, StateUpdateActivity, StateUpdateInput{
			PipelineID:  input.PipelineID,
			ExecutionID: input.ExecutionID,
			Stage:       "validator",
			StageGroup:  "validating",
			// Per-stage completion — keep overall status "processing" (see intent note).
			Status:    "processing",
			EventType: "STAGE_COMPLETED",
			Summary:   "Plan validated",
			Metadata: map[string]interface{}{
				"execution_plan": state.ExecutionPlan,
			},
		}).Get(ctx, nil)
	} else {
		// Fast path: advance workflow FSM through planning states without executing agent activities.
		// This keeps Temporal state machine guards satisfied and allows us to enter execution directly.
		now := workflow.Now(ctx)
		if err := state.Transition(StateIntentResolved, now, "Fast rerun: skipping intent resolution"); err != nil {
			return err
		}
		if err := state.Transition(StateConnectorResolved, now, "Fast rerun: skipping connector resolution"); err != nil {
			return err
		}
		if err := state.Transition(StatePlanned, now, "Fast rerun: skipping planner"); err != nil {
			return err
		}
		if err := state.Transition(StateValidated, now, "Fast rerun: skipping validator"); err != nil {
			return err
		}
	}

	// ==========================================================================
	// PHASE 5: EXECUTION (DAG or Linear)
	// ==========================================================================
	if err := awaitIfPaused(); err != nil {
		if cancelled {
			return nil
		}
		return err
	}

	// Check if the plan contains a workflow graph (DAG execution mode)
	if HasWorkflowGraph(state.CachedPlan) {
		// DAG Execution Path
		logger.Info("🔀 Plan contains workflow_graph, using DAG execution mode")

		graph, err := ExtractWorkflowGraph(state.CachedPlan)
		if err != nil {
			_ = state.Transition(StateFailed, workflow.Now(ctx), fmt.Sprintf("Failed to parse workflow_graph: %v", err))
			return err
		}

		// Store graph in state
		state.WorkflowGraph = graph

		// Transition to DAG executing state
		if err := state.Transition(StateDAGExecuting, workflow.Now(ctx), "Starting DAG execution"); err != nil {
			return err
		}

		// Convert graph to execution plan for UI
		planOpts := workflowtypes.GraphToExecutionPlanOptions{
			PipelineID: input.PipelineID,
			WorkflowID: workflow.GetInfo(ctx).WorkflowExecution.ID,
			Mode:       "dag",
			Now:        workflow.Now(ctx),
		}
		dagPlan, err := workflowtypes.GraphToExecutionPlan(graph, planOpts)
		if err != nil {
			logger.Warn("Failed to convert graph to execution plan", "error", err)
		} else {
			state.ExecutionPlan = dagPlan
		}

		// Create and run the DAG scheduler
		scheduler := NewDAGScheduler(ctx, graph, state, input)
		if err := scheduler.Execute(); err != nil {
			_ = state.Transition(StateFailed, workflow.Now(ctx), fmt.Sprintf("DAG execution failed: %v", err))
			return err
		}

		// DAG completed successfully
		_ = emitPipelineCompletedEvent(ctx, input.PipelineID, input.ExecutionID, map[string]interface{}{
			"execution_plan": state.ExecutionPlan,
			"is_dag":         true,
		})
		if err := state.Transition(StateCompleted, workflow.Now(ctx), "DAG pipeline completed successfully"); err != nil {
			return err
		}

		_ = workflow.ExecuteActivity(ctx, StateUpdateActivity, StateUpdateInput{
			PipelineID:  input.PipelineID,
			ExecutionID: input.ExecutionID,
			Stage:       "completed",
			StageGroup:  "executing",
			Status:      "completed",
			EventType:   "PIPELINE_COMPLETED",
			Summary:     "DAG pipeline completed successfully",
			Metadata: map[string]interface{}{
				"execution_plan": state.ExecutionPlan,
				"is_dag":         true,
			},
		}).Get(ctx, nil)

		logger.Info("🎉 DAG pipeline completed successfully", "graph_id", graph.GraphID)
		return nil
	}

	// Linear Execution Path (existing behavior)
	if err := state.Transition(StateExecuting, workflow.Now(ctx), "Starting execution"); err != nil {
		return err
	}
	_ = emitStageEvent(ctx, input.PipelineID, input.ExecutionID, "executor", "STAGE_STARTED", state.ExecutionPlan, nil)

	// Executor can legitimately take a long time (large tables, networked destinations).
	// Use a dedicated ActivityOptions with a longer StartToClose timeout.
	execOpts := workflow.ActivityOptions{
		StartToCloseTimeout: 45 * time.Minute,
		HeartbeatTimeout:    30 * time.Second, // activity will poll+heartbeat while waiting for correlated response
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    time.Minute,
			MaximumAttempts:    3,
		},
	}
	execCtx := workflow.WithActivityOptions(ctx, execOpts)

	var executionResult map[string]interface{}
	var err error
	// Beta: bounded healing loop around executor failures (observe → diagnose → fix → validate → resume).
	const maxExecutorRepairAttempts = 3
	const maxExecutorLoopIterations = 25
	// maxExecutorReplanAttempts bounds corrective re-plans triggered by a deterministic
	// execution failure. After this many replans the workflow falls back to the existing
	// terminal-failure path unchanged. Kept at 1 for v1 (one corrective attempt, then fail).
	const maxExecutorReplanAttempts = 1
	loopIterations := 0

	// compensate triggers best-effort partial-data cleanup after executor has started writing.
	// It must never block or fail the workflow — call before every StateFailed transition in the executor loop.
	// compensatedForCurrentPlan guards against double-cleanup: it runs at most once per plan version.
	// The replan path re-arms it (sets false) after installing a new plan so the new plan's partial
	// writes can be cleaned independently. Existing paths call compensate() once then return, so the
	// guard is a no-op for them.
	compensatedForCurrentPlan := false
	compensate := func() {
		if compensatedForCurrentPlan {
			return
		}
		compensatedForCurrentPlan = true
		cleanupOpts := workflow.ActivityOptions{
			StartToCloseTimeout: 2 * time.Minute,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 2},
		}
		_ = workflow.ExecuteActivity(
			workflow.WithActivityOptions(ctx, cleanupOpts),
			CleanupPartialDataActivityV2,
			input.PipelineID, input.ExecutionID, state.DestinationConnectionID, state.SelectedTables,
		).Get(ctx, nil)
	}

	emitRepairEvent := func(eventType string, meta map[string]interface{}) {
		// Best-effort: do not fail workflow correctness on telemetry.
		_ = emitDomainEvent(ctx, map[string]interface{}{
			"schema_version": 2,
			"event_type":     eventType, // e.g. REPAIR_STARTED / REPAIR_APPLIED / REPAIR_FAILED
			"pipeline_id":    input.PipelineID,
			"execution_id":   input.ExecutionID,
			"stage":          "executor",
			"timestamp":      workflow.Now(ctx).Format(time.RFC3339),
			"metadata":       mergeMetadata(meta, map[string]interface{}{"execution_plan": state.ExecutionPlan}),
		})
	}

	bumpExecutorRepairAttempt := func(reason string, policyCode string, extra map[string]interface{}) (attempt int, ok bool) {
		if state.RepairAttempts == nil {
			state.RepairAttempts = map[string]int{}
		}
		state.RepairAttempts["executor"] = state.RepairAttempts["executor"] + 1
		attempt = state.RepairAttempts["executor"]
		state.LastFailure = map[string]interface{}{
			"stage":        "executor",
			"policy_code":  policyCode,
			"reason":       reason,
			"attempt":      attempt,
			"max_attempts": maxExecutorRepairAttempts,
			"at":           workflow.Now(ctx).Format(time.RFC3339),
		}
		if extra != nil {
			for k, v := range extra {
				state.LastFailure[k] = v
			}
		}

		if attempt > maxExecutorRepairAttempts {
			emitRepairEvent("REPAIR_FAILED", mergeMetadata(extra, map[string]interface{}{
				"reason":       reason,
				"policy_code":  policyCode,
				"attempt":      attempt,
				"max_attempts": maxExecutorRepairAttempts,
			}))
			return attempt, false
		}

		emitRepairEvent("REPAIR_STARTED", mergeMetadata(extra, map[string]interface{}{
			"reason":       reason,
			"policy_code":  policyCode,
			"attempt":      attempt,
			"max_attempts": maxExecutorRepairAttempts,
		}))
		return attempt, true
	}

	// Shared helper: enter connection configuration wait + validate on resume.
	waitForConnectionFixAndValidate := func(waitDetails map[string]interface{}) error {
		if waitDetails == nil {
			waitDetails = map[string]interface{}{}
		}
		// Add UX hints so the frontend can render the connection configurator reliably.
		waitDetails["request_type"] = "connection_config"
		waitDetails["action_needed"] = "connection_config"
		waitDetails["button_text"] = "Configure connections"
		waitDetails["source_connection_id"] = state.SourceConnectionID
		waitDetails["destination_connection_id"] = state.DestinationConnectionID

		// Provide connector type hints if available.
		if _, ok := waitDetails["source_type"]; !ok {
			if state.CachedIntent != nil {
				if v, ok := state.CachedIntent["source_type"].(string); ok && v != "" {
					waitDetails["source_type"] = v
				} else if nested, ok := state.CachedIntent["intent"].(map[string]interface{}); ok {
					if v, ok := nested["source_type"].(string); ok && v != "" {
						waitDetails["source_type"] = v
					}
				}
			}
		}
		if _, ok := waitDetails["destination_type"]; !ok {
			if state.CachedIntent != nil {
				if v, ok := state.CachedIntent["destination_type"].(string); ok && v != "" {
					waitDetails["destination_type"] = v
				} else if nested, ok := state.CachedIntent["intent"].(map[string]interface{}); ok {
					if v, ok := nested["destination_type"].(string); ok && v != "" {
						waitDetails["destination_type"] = v
					}
				}
			}
		}

		// Enter wait state.
		if err := state.Transition(StateWaitingForConnectionConfiguration, workflow.Now(ctx), "Waiting for connection fix (executor recovery)"); err != nil {
			return err
		}
		state.SetWaitReason(&WaitReason{
			Type:         WaitReasonConnectionConfiguration,
			WaitingSince: workflow.Now(ctx),
			TimeoutAt:    workflow.Now(ctx).Add(24 * time.Hour),
			Context:      waitDetails,
		})

		_ = emitPipelineWaitingEvent(ctx, input.PipelineID, input.ExecutionID, "executor", "connection_config", state.ExecutionPlan, waitDetails)

		// Validate connections with bounded attempts.
		const maxValidationAttempts = 3
		for attempt := 1; attempt <= maxValidationAttempts; attempt++ {
			var connectionPayload ConnectionsConfiguredPayload
			if !awaitHITLSignal(ctx, connectionsConfiguredCh, &connectionPayload, state.WaitReason.TimeoutAt) {
				msg := hitlTimeoutMessage("connections to be fixed", state.WaitReason.TimeoutAt, state.WaitReason.WaitingSince)
				_ = emitStageEvent(ctx, input.PipelineID, input.ExecutionID, "executor", "STAGE_FAILED", state.ExecutionPlan, map[string]interface{}{
					"error_message": msg,
				})
				_ = state.Transition(StateFailed, workflow.Now(ctx), msg)
				return errors.New(msg)
			}
			state.SourceConnectionID = connectionPayload.SourceConnectionID
			state.DestinationConnectionID = connectionPayload.DestinationConnectionID
			input.SourceConnectionID = state.SourceConnectionID
			input.DestinationConnectionID = state.DestinationConnectionID

			if err := awaitIfPaused(); err != nil {
				return err
			}

			validateErr := workflow.ExecuteActivity(ctx, ConnectionValidatorActivityV2, input, state).Get(ctx, nil)
			if validateErr == nil {
				state.ClearWaitReason()
				_ = state.Transition(StateExecuting, workflow.Now(ctx), "Connections fixed and validated (executor recovery)")
				emitRepairEvent("REPAIR_APPLIED", map[string]interface{}{
					"kind":         "connection_fix",
					"attempt":      attempt,
					"max_attempts": maxValidationAttempts,
				})
				return nil
			}

			// Stay in wait state; emit updated waiting event with validation failure.
			_ = emitPipelineWaitingEvent(ctx, input.PipelineID, input.ExecutionID, "executor", "connection_config", state.ExecutionPlan, mergeMetadata(waitDetails, map[string]interface{}{
				"validation_errors": map[string]interface{}{"connection": validateErr.Error()},
				"attempt":           attempt,
				"max_attempts":      maxValidationAttempts,
			}))
		}

		return fmt.Errorf("connection validation failed after %d attempts", maxValidationAttempts)
	}

	for {
		loopIterations++
		if loopIterations > maxExecutorLoopIterations {
			// Guard against unbounded history growth. Prefer ContinueAsNew in a way that allows fast rerun.
			next := input
			next.SourceConnectionID = state.SourceConnectionID
			next.DestinationConnectionID = state.DestinationConnectionID
			next.SourceConnectorSnapshot = state.SourceConnectorSnapshot
			next.DestinationConnectorSnapshot = state.DestinationConnectorSnapshot
			if len(state.SelectedTables) > 0 {
				next.SelectedTables = state.SelectedTables
			}
			logger.Warn("Executor loop iterations exceeded; continuing as new",
				"iterations", loopIterations,
				"pipeline_id", input.PipelineID,
				"execution_id", input.ExecutionID,
			)
			return workflow.NewContinueAsNewError(ctx, NLPipelineWorkflowV2Name, next)
		}

		if err := awaitIfPaused(); err != nil {
			if cancelled {
				return nil
			}
			return err
		}
		if input.ExecutorDispatch == ExecutorDispatchTemporal {
			// Phase 3: native Temporal dispatch on the executor-tasks queue. The
			// orchestrator-hosted ExecutorNativeActivity calls ExecutorWorker.Execute
			// directly (no Redis hop). Classification is SHARED with the correlation
			// path via classifyExecutorResponse, so HITL / self-healing / running
			// semantics are identical. buildExecutorTaskMap + traceContextFromInput are
			// deterministic, so calling them here is replay-safe.
			nativeOpts := execOpts
			nativeOpts.TaskQueue = ExecutorTaskQueue
			nativeCtx := workflow.WithActivityOptions(ctx, nativeOpts)
			tID, tParent, tState := traceContextFromInput(input)
			taskMap := buildExecutorTaskMap(input, state, tID, tParent, tState)
			var nr ExecutorNativeResult
			if nerr := workflow.ExecuteActivity(nativeCtx, "ExecutorNativeActivity", taskMap).Get(nativeCtx, &nr); nerr != nil {
				// Infra/transient failure crossing the activity boundary (already
				// retried per RetryPolicy). Hand to the healing loop unchanged.
				err = nerr
			} else {
				var aerr *ActivityError
				executionResult, aerr = classifyExecutorResponse(nr.Status, nr.Error, nr.Output, "", state)
				if aerr != nil {
					// Raw *ActivityError — unwrapV2ActivityError matches it via errors.As.
					err = aerr
				} else {
					err = nil
				}
			}
		} else {
			err = workflow.ExecuteActivity(execCtx, ExecutorActivityV2, input, state).Get(execCtx, &executionResult)
		}
		if err == nil {
			break
		}

		if cancelled {
			return nil
		}

		// ---- Chunked continuation (large-table resume) ----
		// The executor finished a bounded chunk of a large table, persisted its
		// durable Postgres checkpoint, and signalled more rows remain. Re-dispatch
		// the executor to resume from the checkpoint WITHOUT consuming any repair
		// or replan budget. The loopIterations cap + ContinueAsNew at the top of
		// this loop bound workflow history, so an arbitrarily large table
		// completes across as many continuation iterations / ContinueAsNew
		// generations as needed.
		if activityErr, ok := unwrapV2ActivityError(err); ok &&
			activityErr.Type == ErrTypePolicy && activityErr.Code == PolicyCodeNeedsContinuation {
			logger.Info("Executor chunk complete, more data remains — re-dispatching to resume from checkpoint",
				"pipeline_id", input.PipelineID,
				"execution_id", input.ExecutionID,
				"iteration", loopIterations,
			)
			continue
		}

		// Handle typed errors from executor for HITL + self-healing (beta)
		if activityErr, ok := unwrapV2ActivityError(err); ok && activityErr.Type == ErrTypePolicy {
			// ---- HITL: table selection (existing) ----
			if activityErr.Code == PolicyCodeTableSelectionRequired {
				// Enter wait state for table selection
				if err := state.Transition(StateWaitingForTableSelection, workflow.Now(ctx), "Table selection required"); err != nil {
					return err
				}

				// Normalize wait details so frontend can render a generic, connector-agnostic table selector.
				// (Some executor implementations only return available_tables; UI expects request_type/action_needed/button_text.)
				waitDetails := activityErr.Metadata
				if waitDetails == nil {
					waitDetails = map[string]interface{}{}
				}
				waitDetails["request_type"] = "table_selection"
				waitDetails["action_needed"] = "table_selection"
				waitDetails["button_text"] = "Select Tables"
				// Include connection IDs so the UI can perform post-discovery schema fetches
				// (e.g. suggestions/transforms review) without re-running earlier stages.
				if state.SourceConnectionID != "" {
					waitDetails["source_connection_id"] = state.SourceConnectionID
				}
				if state.DestinationConnectionID != "" {
					waitDetails["destination_connection_id"] = state.DestinationConnectionID
				}
				if _, ok := waitDetails["source_type"]; !ok {
					if state.CachedIntent != nil {
						if v, ok := state.CachedIntent["source_type"].(string); ok && v != "" {
							waitDetails["source_type"] = v
						} else if nested, ok := state.CachedIntent["intent"].(map[string]interface{}); ok {
							if v, ok := nested["source_type"].(string); ok && v != "" {
								waitDetails["source_type"] = v
							}
						}
					}
				}

				state.SetWaitReason(&WaitReason{
					Type:         WaitReasonTableSelection,
					WaitingSince: workflow.Now(ctx),
					TimeoutAt:    workflow.Now(ctx).Add(24 * time.Hour),
					Context:      waitDetails,
				})

				_ = emitPipelineWaitingEvent(ctx, input.PipelineID, input.ExecutionID, "executor", "table_selection", state.ExecutionPlan, waitDetails)

				// Wait for tables_selected signal
				var tablePayload TablesSelectedPayload
				if !awaitHITLSignal(ctx, tablesSelectedCh, &tablePayload, state.WaitReason.TimeoutAt) {
					msg := hitlTimeoutMessage("tables to be selected", state.WaitReason.TimeoutAt, state.WaitReason.WaitingSince)
					_ = emitStageEvent(ctx, input.PipelineID, input.ExecutionID, "executor", "STAGE_FAILED", state.ExecutionPlan, map[string]interface{}{
						"error_message": msg,
					})
					_ = state.Transition(StateFailed, workflow.Now(ctx), msg)
					return errors.New(msg)
				}
				logger.Info("✅ Tables selected signal received", "count", len(tablePayload.SelectedTables))

				// Store selection in workflow state and retry execution
				state.SelectedTables = tablePayload.SelectedTables
				// NOTE: If metadata is omitted, this intentionally clears any previously stored metadata
				// so we don't accidentally reuse stale transforms across HITL retries.
				state.TableSelectionMetadata = tablePayload.Metadata

				state.ClearWaitReason()
				if err := state.Transition(StateExecuting, workflow.Now(ctx), "Retrying execution after table selection"); err != nil {
					return err
				}
				continue
			}

			// ---- Beta: executor-time connection fix ----
			if activityErr.Code == PolicyCodeInvalidConfiguration || activityErr.Code == PolicyCodeAuthReauthRequired {
				if _, ok := bumpExecutorRepairAttempt("Executor requires connection fix", activityErr.Code, activityErr.Metadata); !ok {
					compensate()
					_ = emitStageEvent(ctx, input.PipelineID, input.ExecutionID, "executor", "STAGE_FAILED", state.ExecutionPlan, map[string]interface{}{
						"error_message": err.Error(),
					})
					_ = state.Transition(StateFailed, workflow.Now(ctx), fmt.Sprintf("Execution failed (max repairs exceeded): %v", err))
					return err
				}

				waitDetails := mergeMetadata(activityErr.Metadata, map[string]interface{}{
					"attempt":      state.RepairAttempts["executor"],
					"max_attempts": maxExecutorRepairAttempts,
				})
				// If we have no directional errors, add a generic one.
				if _, ok := waitDetails["validation_errors"]; !ok {
					waitDetails["validation_errors"] = map[string]interface{}{"connection": activityErr.Message}
				}

				if werr := waitForConnectionFixAndValidate(waitDetails); werr != nil {
					compensate()
					_ = emitStageEvent(ctx, input.PipelineID, input.ExecutionID, "executor", "STAGE_FAILED", state.ExecutionPlan, map[string]interface{}{
						"error_message": werr.Error(),
					})
					_ = state.Transition(StateFailed, workflow.Now(ctx), fmt.Sprintf("Execution failed during recovery: %v", werr))
					return werr
				}

				// Retry executor after successful validation.
				continue
			}

			// ---- Beta: auth expiry auto-refresh (best-effort) ----
			if activityErr.Code == PolicyCodeAuthRefreshFailed {
				if _, ok := bumpExecutorRepairAttempt("Attempting OAuth refresh (executor recovery)", activityErr.Code, activityErr.Metadata); !ok {
					compensate()
					_ = emitStageEvent(ctx, input.PipelineID, input.ExecutionID, "executor", "STAGE_FAILED", state.ExecutionPlan, map[string]interface{}{
						"error_message": err.Error(),
					})
					_ = state.Transition(StateFailed, workflow.Now(ctx), fmt.Sprintf("Execution failed (max repairs exceeded): %v", err))
					return err
				}

				// Prefer destination token refresh; fall back to source.
				destTok := ""
				srcTok := ""
				_ = workflow.ExecuteActivity(ctx, FetchConnectionOAuthTokenIDActivity, state.DestinationConnectionID).Get(ctx, &destTok)
				_ = workflow.ExecuteActivity(ctx, FetchConnectionOAuthTokenIDActivity, state.SourceConnectionID).Get(ctx, &srcTok)
				destTok = strings.TrimSpace(destTok)
				srcTok = strings.TrimSpace(srcTok)

				refreshed := false
				if destTok != "" {
					var refreshResp map[string]interface{}
					if rerr := workflow.ExecuteActivity(ctx, RefreshOAuthTokenActivity, destTok).Get(ctx, &refreshResp); rerr == nil {
						refreshed = true
					}
				}
				if !refreshed && srcTok != "" {
					var refreshResp map[string]interface{}
					if rerr := workflow.ExecuteActivity(ctx, RefreshOAuthTokenActivity, srcTok).Get(ctx, &refreshResp); rerr == nil {
						refreshed = true
					}
				}

				if refreshed {
					// Validate connections and retry.
					_ = workflow.ExecuteActivity(ctx, ConnectionValidatorActivityV2, input, state).Get(ctx, nil)
					emitRepairEvent("REPAIR_APPLIED", map[string]interface{}{
						"kind": "oauth_refresh",
					})
					continue
				}

				// Refresh not possible or failed: fall back to connection fix HITL.
				waitDetails := mergeMetadata(activityErr.Metadata, map[string]interface{}{
					"attempt":      state.RepairAttempts["executor"],
					"max_attempts": maxExecutorRepairAttempts,
					"validation_errors": map[string]interface{}{
						"auth": activityErr.Message,
					},
				})
				if werr := waitForConnectionFixAndValidate(waitDetails); werr != nil {
					compensate()
					_ = emitStageEvent(ctx, input.PipelineID, input.ExecutionID, "executor", "STAGE_FAILED", state.ExecutionPlan, map[string]interface{}{
						"error_message": werr.Error(),
					})
					_ = state.Transition(StateFailed, workflow.Now(ctx), fmt.Sprintf("Execution failed during auth recovery: %v", werr))
					return werr
				}
				continue
			}

			// ---- Beta: schema drift -> plan -> (auto-apply safe DDL) -> validate -> resume ----
			if activityErr.Code == PolicyCodeSchemaDriftDetected || activityErr.Code == PolicyCodeSchemaApprovalRequired {
				if _, ok := bumpExecutorRepairAttempt("Schema drift detected; planning repair", activityErr.Code, activityErr.Metadata); !ok {
					compensate()
					_ = emitStageEvent(ctx, input.PipelineID, input.ExecutionID, "executor", "STAGE_FAILED", state.ExecutionPlan, map[string]interface{}{
						"error_message": err.Error(),
					})
					_ = state.Transition(StateFailed, workflow.Now(ctx), fmt.Sprintf("Execution failed (max repairs exceeded): %v", err))
					return err
				}

				// Plan repair (Postgres→Postgres safe auto-DDL; otherwise approval required).
				var plan SchemaRepairPlan
				planErr := workflow.ExecuteActivity(ctx, PlanSchemaDriftRepairActivity, state.SourceConnectionID, state.DestinationConnectionID, state.SelectedTables).Get(ctx, &plan)
				if planErr != nil {
					compensate()
					emitRepairEvent("REPAIR_FAILED", map[string]interface{}{"kind": "schema_plan", "error": planErr.Error()})
					_ = emitStageEvent(ctx, input.PipelineID, input.ExecutionID, "executor", "STAGE_FAILED", state.ExecutionPlan, map[string]interface{}{
						"error_message": planErr.Error(),
					})
					_ = state.Transition(StateFailed, workflow.Now(ctx), fmt.Sprintf("Schema repair planning failed: %v", planErr))
					return planErr
				}

				// Persist lightweight plan snapshot in workflow state.
				state.LastRepairPlan = map[string]interface{}{
					"safe":              plan.Safe,
					"requires_approval": plan.RequiresApproval,
					"reason":            plan.Reason,
					"ddl":               plan.DDL,
					"changes":           plan.Changes,
				}

				// Auto-apply when safe.
				if plan.Safe && !plan.RequiresApproval && len(plan.DDL) > 0 {
					if aerr := workflow.ExecuteActivity(ctx, ApplySchemaDDLActivity, state.DestinationConnectionID, plan.DDL).Get(ctx, nil); aerr == nil {
						var validation SchemaRepairValidation
						_ = workflow.ExecuteActivity(ctx, ValidateSchemaRepairActivity, state.SourceConnectionID, state.DestinationConnectionID, state.SelectedTables).Get(ctx, &validation)
						if validation.Valid {
							emitRepairEvent("REPAIR_APPLIED", map[string]interface{}{"kind": "schema_auto_ddl"})
							continue
						}
						// Fall through to approval gate if validation still reports drift.
						plan = validation.RepairPlan
					} else {
						// Auto-apply failed; require approval / manual intervention.
						plan.RequiresApproval = true
						plan.Safe = false
						plan.Reason = fmt.Sprintf("Auto-apply failed: %v", aerr)
					}
				}

				// Approval gate (either risky plan or auto-apply failed / not supported).
				if terr := state.Transition(StateWaitingForApproval, workflow.Now(ctx), "Approval required (schema drift)"); terr != nil {
					return terr
				}

				waitDetails := mergeMetadata(activityErr.Metadata, map[string]interface{}{
					"request_type":      "approval",
					"action_needed":     "approval",
					"button_text":       "Approve",
					"attempt":           state.RepairAttempts["executor"],
					"max_attempts":      maxExecutorRepairAttempts,
					"reason":            plan.Reason,
					"repair_plan":       plan,
					"requires_approval": true,
					"proposed_ddl":      plan.DDL,
				})
				state.SetWaitReason(&WaitReason{
					Type:         WaitReasonApproval,
					WaitingSince: workflow.Now(ctx),
					TimeoutAt:    workflow.Now(ctx).Add(24 * time.Hour),
					Context:      waitDetails,
				})
				_ = emitPipelineWaitingEvent(ctx, input.PipelineID, input.ExecutionID, "executor", "approval", state.ExecutionPlan, waitDetails)

				var approval ApprovalGivenPayload
				if !awaitHITLSignal(ctx, approvalGivenCh, &approval, state.WaitReason.TimeoutAt) {
					msg := hitlTimeoutMessage("the schema change to be approved", state.WaitReason.TimeoutAt, state.WaitReason.WaitingSince)
					emitRepairEvent("REPAIR_FAILED", map[string]interface{}{"kind": "schema_approval", "reason": msg})
					_ = emitStageEvent(ctx, input.PipelineID, input.ExecutionID, "executor", "STAGE_FAILED", state.ExecutionPlan, map[string]interface{}{
						"error_message": msg,
					})
					_ = state.Transition(StateFailed, workflow.Now(ctx), msg)
					return errors.New(msg)
				}
				if !approval.Approved {
					msg := strings.TrimSpace(approval.Comment)
					if msg == "" {
						msg = "Schema repair was not approved"
					}
					emitRepairEvent("REPAIR_FAILED", map[string]interface{}{"kind": "schema_approval", "reason": msg})
					_ = emitStageEvent(ctx, input.PipelineID, input.ExecutionID, "executor", "STAGE_FAILED", state.ExecutionPlan, map[string]interface{}{
						"error_message": msg,
					})
					_ = state.Transition(StateFailed, workflow.Now(ctx), msg)
					return errors.New(msg)
				}

				// Apply + validate after approval.
				if aerr := workflow.ExecuteActivity(ctx, ApplySchemaDDLActivity, state.DestinationConnectionID, plan.DDL).Get(ctx, nil); aerr != nil {
					emitRepairEvent("REPAIR_FAILED", map[string]interface{}{"kind": "schema_apply", "error": aerr.Error()})
					_ = emitStageEvent(ctx, input.PipelineID, input.ExecutionID, "executor", "STAGE_FAILED", state.ExecutionPlan, map[string]interface{}{
						"error_message": aerr.Error(),
					})
					_ = state.Transition(StateFailed, workflow.Now(ctx), fmt.Sprintf("Schema repair apply failed: %v", aerr))
					return aerr
				}

				var validation SchemaRepairValidation
				if verr := workflow.ExecuteActivity(ctx, ValidateSchemaRepairActivity, state.SourceConnectionID, state.DestinationConnectionID, state.SelectedTables).Get(ctx, &validation); verr != nil || !validation.Valid {
					msg := "Schema repair validation failed"
					if verr != nil {
						msg = verr.Error()
					} else if strings.TrimSpace(validation.Reason) != "" {
						msg = validation.Reason
					}
					emitRepairEvent("REPAIR_FAILED", map[string]interface{}{"kind": "schema_validate", "error": msg})
					_ = emitStageEvent(ctx, input.PipelineID, input.ExecutionID, "executor", "STAGE_FAILED", state.ExecutionPlan, map[string]interface{}{
						"error_message": msg,
					})
					_ = state.Transition(StateFailed, workflow.Now(ctx), msg)
					return errors.New(msg)
				}

				state.ClearWaitReason()
				_ = state.Transition(StateExecuting, workflow.Now(ctx), "Schema repaired; retrying execution")
				emitRepairEvent("REPAIR_APPLIED", map[string]interface{}{"kind": "schema_ddl"})
				continue
			}
		}

		// ---- Auto-replan (beta): a deterministic execution failure may be fixable by
		// re-planning with the failure as context. Bounded by maxExecutorReplanAttempts.
		// Only genuine executor business failures (DETERMINISTIC / EXECUTION_FAILED) are
		// eligible — transient/infra errors are not replannable and fall straight through.
		// Once the replan budget is exhausted we fall through to the existing terminal-failure
		// path below unchanged, so prior behavior is preserved (just deferred by N replans).
		if execErr, ok := unwrapV2ActivityError(err); executorReplanEligible(execErr, ok, state.ReplanAttempts, maxExecutorReplanAttempts) {

			state.ReplanAttempts++
			// Record the failure deterministically so PlannerActivityV2 can pass it as error_context.
			state.LastFailure = map[string]interface{}{
				"stage":       "executor",
				"policy_code": execErr.Code,
				"reason":      execErr.Message,
				"replan":      state.ReplanAttempts,
				"max_replans": maxExecutorReplanAttempts,
				"at":          workflow.Now(ctx).Format(time.RFC3339),
			}
			for k, v := range execErr.Metadata {
				state.LastFailure[k] = v
			}

			emitRepairEvent("REPAIR_STARTED", map[string]interface{}{
				"kind":        "replan",
				"replan":      state.ReplanAttempts,
				"max_replans": maxExecutorReplanAttempts,
				"reason":      execErr.Message,
			})

			// Clean up any partial writes from the failed plan BEFORE installing a new one.
			compensate()

			// Re-invoke the planner with the failure context (state.LastFailure).
			var replanResult map[string]interface{}
			replanErr := workflow.ExecuteActivity(childCtx, PlannerActivityV2, input, state).Get(childCtx, &replanResult)
			if replanErr != nil {
				if cancelled {
					return nil
				}
				// Replan itself failed → fall through to the terminal-failure path below.
				emitRepairEvent("REPAIR_FAILED", map[string]interface{}{
					"kind":  "replan",
					"error": replanErr.Error(),
				})
			} else {
				// Install the corrected plan and re-arm compensation for the new plan version,
				// then retry execution with the new plan.
				state.CachedPlan = replanResult
				compensatedForCurrentPlan = false
				emitRepairEvent("REPAIR_APPLIED", map[string]interface{}{
					"kind":   "replan",
					"replan": state.ReplanAttempts,
				})
				continue
			}
		}

		// Non-HITL error: fail — compensate any partial writes first
		compensate()
		_ = emitStageEvent(ctx, input.PipelineID, input.ExecutionID, "executor", "STAGE_FAILED", state.ExecutionPlan, map[string]interface{}{
			"error_message": err.Error(),
		})
		_ = state.Transition(StateFailed, workflow.Now(ctx), fmt.Sprintf("Execution failed: %v", err))
		return err
	}
	_ = emitStageEvent(ctx, input.PipelineID, input.ExecutionID, "executor", "STAGE_COMPLETED", state.ExecutionPlan, map[string]interface{}{
		"result_summary": "Pipeline executed",
	})

	// Streaming pipelines: executor starts long-running components and returns status=running.
	// We must NOT mark the pipeline as completed here; instead, persist "running" and exit the workflow
	// without transitioning to StateCompleted (so pipelines.status isn't overwritten).
	pipelineType := ""
	if v, ok := executionResult["pipeline_type"].(string); ok {
		pipelineType = strings.TrimSpace(strings.ToLower(v))
	}
	// Per-stage completion — keep the overall status non-terminal (see intent note
	// at the understanding stage above). StateUpdateInput.Status is the status of
	// the whole RUN, not of the stage that just finished; the stage's own outcome
	// is recorded separately, from EventType, by stateForEventType() in
	// activities.go. Writing "completed" here made stateUpdateActivity stamp
	// pipeline_progress.status = 'completed' while the run was still executing —
	// the same defect PR #736 fixed on the orchestrator side. Only the real
	// PIPELINE_COMPLETED below sets the overall status to "completed".
	execStatus := "processing"
	execSummary := "Pipeline executed"
	if pipelineType == "streaming" {
		// Not a violation: a streaming run genuinely IS running from here on, and
		// this is its final status — the streaming branch returns below without
		// ever emitting PIPELINE_COMPLETED, so nothing later would set it.
		execStatus = "running"
		execSummary = "Streaming pipeline started"
	}

	_ = workflow.ExecuteActivity(ctx, StateUpdateActivity, StateUpdateInput{
		PipelineID:  input.PipelineID,
		ExecutionID: input.ExecutionID,
		Stage:       "executor",
		StageGroup:  "executing",
		Status:      execStatus,
		EventType:   "STAGE_COMPLETED",
		Summary:     execSummary,
		Metadata: map[string]interface{}{
			"execution_plan": state.ExecutionPlan,
			"pipeline_type":  pipelineType,
		},
	}).Get(ctx, nil)

	if pipelineType == "streaming" {
		// Keep workflow state as EXECUTING so deferred status updater does not mark pipelines.status as completed.
		_ = state.Transition(StateExecuting, workflow.Now(ctx), "Streaming pipeline started")
		logger.Info("🚀 Streaming pipeline started; workflow exiting (pipeline continues running)", "pipeline_id", input.PipelineID, "execution_id", input.ExecutionID)
		// Close the execution row explicitly — "streaming_active" closes executions.status
		// without touching pipelines.status (CDC sentinel owns the pipeline from here).
		// Without this the row stays stuck at status='running' forever (zombie execution).
		_ = workflow.ExecuteActivity(ctx, UpdatePipelineStatusActivity,
			input.PipelineID, input.ExecutionID, "streaming_active", "",
		).Get(ctx, nil)
		return nil
	}

	// ==========================================================================
	// COMPLETION
	// ==========================================================================
	_ = emitPipelineCompletedEvent(ctx, input.PipelineID, input.ExecutionID, mergeMetadata(executionResult, map[string]interface{}{
		"execution_plan": state.ExecutionPlan,
	}))
	if err := state.Transition(StateCompleted, workflow.Now(ctx), "Pipeline completed successfully"); err != nil {
		return err
	}

	_ = workflow.ExecuteActivity(ctx, StateUpdateActivity, StateUpdateInput{
		PipelineID:  input.PipelineID,
		ExecutionID: input.ExecutionID,
		Stage:       "completed",
		StageGroup:  "executing",
		Status:      "completed",
		EventType:   "PIPELINE_COMPLETED",
		Summary:     "Pipeline completed successfully",
		Metadata: map[string]interface{}{
			"execution_plan": state.ExecutionPlan,
		},
	}).Get(ctx, nil)

	logger.Info("🎉 Pipeline completed successfully", "result", executionResult)
	return nil
}

func traceContextFromWorkflowCtx(ctx workflow.Context, executionID string) (traceID string, traceparent string, tracestate string) {
	// Best-effort: fallback to execution_id (stable business identifier)
	traceID = executionID
	if v := ctx.Value("trace_id"); v != nil {
		if s, ok := v.(string); ok && s != "" {
			traceID = s
		}
	}
	if v := ctx.Value("traceparent"); v != nil {
		if s, ok := v.(string); ok {
			traceparent = s
		}
	}
	if v := ctx.Value("tracestate"); v != nil {
		if s, ok := v.(string); ok {
			tracestate = s
		}
	}
	return traceID, traceparent, tracestate
}

// hitlWaitTimeoutVersion gates enforcement of the HITL park deadline.
//
// Versioned because enforcement adds a timer, and a timer is a command: replaying
// a workflow that is ALREADY parked under the timer-free code must keep taking the
// old path or the replay diverges from its recorded history. Workflows parked
// before this change therefore stay parked — they are no worse off than they were,
// and the operator-facing recovery for them is unchanged.
const hitlWaitTimeoutVersion = "hitl-wait-timeout"

// awaitHITLSignal blocks until the signal arrives or the park deadline passes,
// reporting true only when a payload was actually received.
//
// This is the sole place WaitReason.TimeoutAt is enforced. Every HITL park must go
// through it (TestNoBareHITLChannelReceives holds that line): the deadline was
// computed and persisted at all six park sites long before anything read it back,
// so a park whose signal never arrived blocked for the life of the workflow. That
// is not a hypothetical — it is the "pipeline hangs at table selection" report, and
// the zombie sweeper cannot clean up after it because it deliberately skips
// pipeline_progress.status='waiting_for_user'.
func awaitHITLSignal(ctx workflow.Context, ch workflow.ReceiveChannel, valuePtr interface{}, deadline time.Time) bool {
	if workflow.GetVersion(ctx, hitlWaitTimeoutVersion, workflow.DefaultVersion, 1) == workflow.DefaultVersion {
		ch.Receive(ctx, valuePtr)
		return true
	}

	// A resumed workflow can re-enter a park whose deadline already passed; arming
	// a negative-duration timer here would fire immediately anyway, but returning
	// early keeps the intent legible and skips a pointless command.
	remaining := deadline.Sub(workflow.Now(ctx))
	if remaining <= 0 {
		return false
	}

	// Cancel the timer on the receive path so a park that ends normally does not
	// leave a pending timer behind for the rest of the workflow's life.
	timerCtx, cancelTimer := workflow.WithCancel(ctx)
	defer cancelTimer()

	received := false
	selector := workflow.NewSelector(ctx)
	selector.AddReceive(ch, func(c workflow.ReceiveChannel, _ bool) {
		c.Receive(ctx, valuePtr)
		received = true
	})
	selector.AddFuture(workflow.NewTimer(timerCtx, remaining), func(workflow.Future) {})
	selector.Select(ctx)

	return received
}

// hitlTimeoutMessage renders the user-facing reason a park expired.
//
// The park already knows what it was waiting for, so say that rather than emitting
// a generic timeout: before this existed the only trace a stuck pipeline left was
// the sweeper's "zombie: execution timed out with no end_time", which describes the
// janitor rather than anything the user could act on.
func hitlTimeoutMessage(waiting string, deadline time.Time, waitingSince time.Time) string {
	waited := deadline.Sub(waitingSince).Round(time.Minute)
	return fmt.Sprintf("Timed out after %s waiting for %s. The pipeline was paused for input that never arrived — resolve it and run the pipeline again.", waited, waiting)
}

// emitPipelineWaitingEvent emits a V2-compliant PIPELINE_WAITING event
func emitPipelineWaitingEvent(ctx workflow.Context, pipelineID, executionID, stage, blockingType string, plan *workflowtypes.ExecutionPlan, details map[string]interface{}) error {
	now := workflow.Now(ctx)
	traceID, traceparent, tracestate := traceContextFromWorkflowCtx(ctx, executionID)

	if details == nil {
		details = map[string]interface{}{}
	}

	// Persist the blocking reason type into metadata so the authoritative state writer
	// (StateUpdateActivity → pipeline_progress) can populate blocking_reason_* columns.
	// This keeps /api/v1/pipelines/:id/state consistent even if projector lags.
	if _, ok := details["blocking_reason_type"]; !ok && blockingType != "" {
		details["blocking_reason_type"] = blockingType
	}

	// Update dynamic execution plan stage state
	updateExecutionPlanForWaiting(plan, stage, now, details)

	// Ensure execution_plan is always present for the UI
	details = mergeMetadata(details, map[string]interface{}{
		"execution_plan": plan,
	})

	// Extract user-friendly description based on blocking type
	description := "Waiting for user input"
	switch blockingType {
	case "connection_config":
		// Build a sentence like "Select a MySQL source and a PostgreSQL
		// destination connection to continue." instead of dumping the raw
		// Go map literal `[map[connector_type:mysql direction:source] ...]`.
		description = formatMissingConnectionsDescription(details["missing_connections"])
	case "connector_generation":
		description = "Connector generation required"
		// missing_connectors can be []interface{} or []string depending on producer
		switch v := details["missing_connectors"].(type) {
		case []interface{}:
			if len(v) > 0 {
				description = fmt.Sprintf("Connector generation required for %v", v)
			}
		case []string:
			if len(v) > 0 {
				description = fmt.Sprintf("Connector generation required for %v", v)
			}
		}
	case "table_selection":
		description = "Table selection required"
		// available_tables can be []interface{} or []map[string]interface{}
		switch v := details["available_tables"].(type) {
		case []interface{}:
			if len(v) > 0 {
				description = fmt.Sprintf("Select table(s) to sync (%d available)", len(v))
			}
		case []map[string]interface{}:
			if len(v) > 0 {
				description = fmt.Sprintf("Select table(s) to sync (%d available)", len(v))
			}
		}
	}

	// Build V2-compliant event
	event := map[string]interface{}{
		"schema_version": 2,
		"event_type":     "PIPELINE_WAITING",
		"pipeline_id":    pipelineID,
		"execution_id":   executionID,
		"trace_id":       traceID,
		"stage":          stage,
		"status":         "waiting_for_user",
		"message":        description,
		"metadata":       details, // Persist execution_plan + wait details
		"state":          "waiting",
		"blocking_reason": map[string]interface{}{
			"type":        blockingType,
			"description": description,
			"details":     details,
		},
		"timestamp": now.Format(time.RFC3339),
	}
	if traceparent != "" {
		event["traceparent"] = traceparent
	}
	if tracestate != "" {
		event["tracestate"] = tracestate
	}

	// Emit domain event (Kafka → UI/WebSocket and best-effort projector)
	if err := emitDomainEvent(ctx, event); err != nil {
		return err
	}

	// Authoritative state write for /api/v1/pipelines/:id/state consumers.
	// Best-effort: don't fail the workflow if state write is unavailable.
	_ = workflow.ExecuteActivity(ctx, StateUpdateActivity, StateUpdateInput{
		PipelineID:  pipelineID,
		ExecutionID: executionID,
		Stage:       stage,
		StageGroup:  stageGroupForStage(stage),
		Status:      "waiting_for_user",
		EventType:   "PIPELINE_WAITING",
		Summary:     description,
		Metadata:    details,
	}).Get(ctx, nil)

	return nil
}

// emitStageEvent emits a V2-compliant stage lifecycle event (STAGE_STARTED, STAGE_COMPLETED, STAGE_FAILED)
func emitStageEvent(ctx workflow.Context, pipelineID, executionID, stage, eventType string, plan *workflowtypes.ExecutionPlan, metadata map[string]interface{}) error {
	now := workflow.Now(ctx)
	traceID, traceparent, tracestate := traceContextFromWorkflowCtx(ctx, executionID)

	// Update dynamic execution plan stage state
	updateExecutionPlanForStageEvent(plan, stage, eventType, now, metadata)

	// Ensure execution_plan is always present for the UI
	metadata = mergeMetadata(metadata, map[string]interface{}{
		"execution_plan": plan,
	})

	stageState := "running"
	switch eventType {
	case "STAGE_STARTED":
		stageState = "running"
	case "STAGE_COMPLETED":
		stageState = "succeeded"
	case "STAGE_FAILED":
		stageState = "failed"
	}

	stageGroup := stageGroupForStage(stage)

	status := "processing"
	if eventType == "STAGE_FAILED" {
		status = "failed"
	}

	event := map[string]interface{}{
		"schema_version": 2,
		"event_type":     eventType,
		"pipeline_id":    pipelineID,
		"execution_id":   executionID,
		"trace_id":       traceID,
		"stage":          stage,
		"stage_group":    stageGroup,
		"state":          stageState,
		"status":         status,
		"timestamp":      now.Format(time.RFC3339),
		"metadata":       metadata,
	}
	if traceparent != "" {
		event["traceparent"] = traceparent
	}
	if tracestate != "" {
		event["tracestate"] = tracestate
	}

	if err := emitDomainEvent(ctx, event); err != nil {
		return err
	}

	// Best-effort authoritative write (keeps /state consistent even if projector lags).
	summary := fmt.Sprintf("%s %s", stage, strings.ToLower(strings.ReplaceAll(eventType, "STAGE_", "")))
	_ = workflow.ExecuteActivity(ctx, StateUpdateActivity, StateUpdateInput{
		PipelineID:  pipelineID,
		ExecutionID: executionID,
		Stage:       stage,
		StageGroup:  stageGroup,
		Status:      status,
		EventType:   eventType,
		Summary:     summary,
		Metadata:    metadata,
	}).Get(ctx, nil)

	return nil
}

// formatMissingConnectionsDescription turns the missing-connections payload
// (a slice of structs/maps with `connector_type` + `direction`) into a human
// sentence the chat surface can show without exposing Go map syntax.
//
// Falls back to a generic message if the input shape is unexpected — this is
// purely cosmetic, not load-bearing.
func formatMissingConnectionsDescription(raw interface{}) string {
	if raw == nil {
		return "Connection configuration required"
	}
	items, ok := raw.([]interface{})
	if !ok {
		// Try []string fallback
		if ss, ok := raw.([]string); ok && len(ss) > 0 {
			return "Connection configuration required: " + strings.Join(ss, ", ")
		}
		return "Connection configuration required"
	}
	if len(items) == 0 {
		return "Connection configuration required"
	}

	parts := make([]string, 0, len(items))
	for _, it := range items {
		m, ok := it.(map[string]interface{})
		if !ok {
			continue
		}
		ct, _ := m["connector_type"].(string)
		dir, _ := m["direction"].(string)
		ct = strings.TrimSpace(ct)
		dir = strings.TrimSpace(dir)
		if ct == "" && dir == "" {
			continue
		}
		// Capitalize the connector for display ("mysql" → "MySQL"); fall back to the raw value.
		ctDisplay := ct
		switch strings.ToLower(ct) {
		case "mysql":
			ctDisplay = "MySQL"
		case "postgresql", "postgres":
			ctDisplay = "PostgreSQL"
		case "mongodb":
			ctDisplay = "MongoDB"
		case "bigquery":
			ctDisplay = "BigQuery"
		case "snowflake":
			ctDisplay = "Snowflake"
		case "aws-s3", "s3":
			ctDisplay = "S3"
		}
		switch strings.ToLower(dir) {
		case "source":
			parts = append(parts, "a "+ctDisplay+" source")
		case "destination":
			parts = append(parts, "a "+ctDisplay+" destination")
		default:
			parts = append(parts, ctDisplay)
		}
	}

	switch len(parts) {
	case 0:
		return "Connection configuration required"
	case 1:
		return "Select " + parts[0] + " connection to continue."
	case 2:
		return "Select " + parts[0] + " and " + parts[1] + " connection to continue."
	default:
		return "Select " + strings.Join(parts[:len(parts)-1], ", ") + " and " + parts[len(parts)-1] + " connections to continue."
	}
}

func mergeMetadata(primary map[string]interface{}, extra map[string]interface{}) map[string]interface{} {
	if primary == nil && extra == nil {
		return nil
	}
	out := map[string]interface{}{}
	for k, v := range extra {
		out[k] = v
	}
	for k, v := range primary {
		out[k] = v
	}
	return out
}

func findPlanStage(plan *workflowtypes.ExecutionPlan, stageID string) *workflowtypes.ExecutionStage {
	if plan == nil {
		return nil
	}
	for i := range plan.Stages {
		if plan.Stages[i].ID == stageID {
			return &plan.Stages[i]
		}
	}
	return nil
}

func ensurePlanStage(plan *workflowtypes.ExecutionPlan, stageID string) *workflowtypes.ExecutionStage {
	if plan == nil {
		return nil
	}
	if st := findPlanStage(plan, stageID); st != nil {
		return st
	}

	// Unknown stage → add a generic representation so UI still renders it.
	order := len(plan.Stages) + 1
	plan.Stages = append(plan.Stages, workflowtypes.ExecutionStage{
		ID:                stageID,
		DisplayName:       stageID,
		Description:       "Running stage",
		Icon:              "📍",
		Color:             "blue",
		Group:             stageGroupForStage(stageID),
		Type:              "custom",
		Status:            "pending",
		Progress:          0,
		Order:             order,
		Dependencies:      []string{},
		EstimatedDuration: 0,
		ActualDuration:    0,
	})
	return &plan.Stages[len(plan.Stages)-1]
}

func updateExecutionPlanForStageEvent(plan *workflowtypes.ExecutionPlan, stageID, eventType string, now time.Time, metadata map[string]interface{}) {
	if plan == nil {
		return
	}

	stage := ensurePlanStage(plan, stageID)
	if stage == nil {
		return
	}

	switch eventType {
	case "STAGE_STARTED":
		stage.Status = "running"
		stage.Progress = 0
		if stage.StartedAt == nil {
			stage.StartedAt = &now
		}
		stage.ErrorMessage = ""

	case "STAGE_COMPLETED":
		stage.Status = "complete"
		stage.Progress = 100
		stage.CompletedAt = &now
		if stage.StartedAt == nil {
			stage.StartedAt = &now
		}
		stage.SetActualDuration(now.Sub(*stage.StartedAt))

		// Heuristic: if connector_check completed and nothing ran connector_generation, mark it skipped.
		if stageID == "connector_check" {
			if cg := findPlanStage(plan, "connector_generation"); cg != nil {
				if cg.Status == "pending" {
					cg.Status = "skipped"
					cg.Progress = 0
				}
			}
		}

	case "STAGE_FAILED":
		stage.Status = "failed"
		stage.CompletedAt = &now
		if stage.StartedAt == nil {
			stage.StartedAt = &now
		}
		stage.SetActualDuration(now.Sub(*stage.StartedAt))
		if metadata != nil {
			if em, ok := metadata["error_message"].(string); ok && em != "" {
				stage.ErrorMessage = em
			} else if em, ok := metadata["error"].(string); ok && em != "" {
				stage.ErrorMessage = em
			}
		}
	}
}

func updateExecutionPlanForWaiting(plan *workflowtypes.ExecutionPlan, stageID string, now time.Time, details map[string]interface{}) {
	if plan == nil {
		return
	}
	stage := ensurePlanStage(plan, stageID)
	if stage == nil {
		return
	}
	stage.Status = "waiting"
	if stage.StartedAt == nil {
		stage.StartedAt = &now
	}
	// Keep progress as-is (or 0 if unset)
	if stage.Progress < 0 {
		stage.Progress = 0
	}
	_ = details
}

// emitPipelineCompletedEvent emits pipeline completion event
func emitPipelineCompletedEvent(ctx workflow.Context, pipelineID, executionID string, result map[string]interface{}) error {
	event := map[string]interface{}{
		"schema_version": 2,
		"event_type":     "PIPELINE_COMPLETED",
		"pipeline_id":    pipelineID,
		"execution_id":   executionID,
		"state":          "succeeded",
		"status":         "completed",
		"timestamp":      workflow.Now(ctx).Format(time.RFC3339),
		"metadata":       result,
	}

	return emitDomainEvent(ctx, event)
}

// stageGroupForStage maps stage to stage_group (UI lane)
func stageGroupForStage(stage string) string {
	switch stage {
	case "intent":
		return "understanding"
	case "capability_resolver", "connection_validator", "connection_validation", "connector_check", "connector_generation", "resolver":
		return "connecting"
	case "discovery":
		return "discovering"
	case "planner":
		return "planning"
	case "cost_estimation", "policy_check", "validator":
		return "validating"
	case "executor":
		return "executing"
	default:
		return "planning"
	}
}

func getMissingConnectors(metadata map[string]interface{}) []string {
	if metadata == nil {
		return []string{}
	}

	// Fast path: already []string
	if missing, ok := metadata["missing_connectors"].([]string); ok && len(missing) > 0 {
		return missing
	}

	// Common path when decoded from JSON into map[string]interface{}: []interface{} of strings
	if raw, ok := metadata["missing_connectors"].([]interface{}); ok && len(raw) > 0 {
		out := make([]string, 0, len(raw))
		for _, v := range raw {
			if s, ok := v.(string); ok {
				s = strings.TrimSpace(s)
				if s != "" {
					out = append(out, s)
				}
			}
		}
		return out
	}

	// Fallback: comma-separated string
	if s, ok := metadata["missing_connectors"].(string); ok {
		s = strings.TrimSpace(s)
		if s == "" {
			return []string{}
		}
		parts := strings.Split(s, ",")
		out := make([]string, 0, len(parts))
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p != "" {
				out = append(out, p)
			}
		}
		return out
	}

	return []string{}
}
