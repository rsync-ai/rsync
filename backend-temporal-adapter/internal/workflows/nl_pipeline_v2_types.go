package workflows

import (
	"fmt"
	"time"

	workflowtypes "github.com/rsync-ai/backend-temporal-adapter/internal/workflows/types"
)

// ==============================================================================
// WORKFLOW STATE MODEL V2 (Phase 0 - Explicit State Machine)
// ==============================================================================
// This file defines the explicit state model for NLPipelineWorkflowV2 to ensure
// Temporal is the sole authority for pipeline state and prevent re-entry loops.

// PipelineState represents the explicit state of a pipeline workflow
type PipelineState string

const (
	StateInit                              PipelineState = "INIT"
	StateIntentResolved                    PipelineState = "INTENT_RESOLVED"
	StateConnectorResolved                 PipelineState = "CONNECTOR_RESOLVED"
	StateWaitingForConnectionConfiguration PipelineState = "WAITING_FOR_CONNECTION_CONFIGURATION"
	StateWaitingForConnectorGeneration     PipelineState = "WAITING_FOR_CONNECTOR_GENERATION"
	StateWaitingForTableSelection          PipelineState = "WAITING_FOR_TABLE_SELECTION"
	StateWaitingForApproval                PipelineState = "WAITING_FOR_APPROVAL"
	StatePlanned                           PipelineState = "PLANNED"
	StateValidated                         PipelineState = "VALIDATED"
	StateExecuting                         PipelineState = "EXECUTING"
	StateCompleted                         PipelineState = "COMPLETED"
	StateFailed                            PipelineState = "FAILED"
	StateCancelled                         PipelineState = "CANCELLED"

	// DAG-specific states
	StateDAGExecuting           PipelineState = "DAG_EXECUTING"            // Running DAG nodes
	StateWaitingForNodeInput    PipelineState = "WAITING_FOR_NODE_INPUT"   // Waiting for NL input for a node
)

// WaitReasonType represents why a workflow is waiting
type WaitReasonType string

const (
	WaitReasonConnectionConfiguration WaitReasonType = "CONNECTION_CONFIGURATION"
	WaitReasonConnectorGeneration     WaitReasonType = "CONNECTOR_GENERATION"
	WaitReasonTableSelection          WaitReasonType = "TABLE_SELECTION"
	WaitReasonApproval                WaitReasonType = "APPROVAL"
	WaitReasonOther                   WaitReasonType = "OTHER"

	// DAG-specific wait reasons
	WaitReasonNodeInput WaitReasonType = "NODE_INPUT" // Waiting for NL input for a specific node
)

// WaitReason provides context for why a workflow is waiting
type WaitReason struct {
	Type              WaitReasonType         `json:"type"`
	Context           map[string]interface{} `json:"context"`
	WaitingSince      time.Time              `json:"waiting_since"`
	TimeoutAt         time.Time              `json:"timeout_at"`
	MissingConnectors []string               `json:"missing_connectors,omitempty"`
	RequiredApproval  string                 `json:"required_approval,omitempty"`

	// DAG-specific fields
	NodeID       string `json:"node_id,omitempty"`       // Node ID that requires input
	NodePrompt   string `json:"node_prompt,omitempty"`   // Prompt to show the user
}

// WorkflowState tracks the complete state of a pipeline workflow execution
// This struct is stored in workflow memory and persisted via Temporal's event history
// All fields must be deterministically computable for proper Temporal replay
type WorkflowState struct {
	// Current workflow state
	CurrentState PipelineState `json:"current_state"`
	WaitReason   *WaitReason   `json:"wait_reason,omitempty"`

	// Repair / healing context (beta)
	// NOTE: keep these lightweight; anything heavy should live in pipeline_progress.metadata via StateUpdateActivity.
	RepairAttempts map[string]int         `json:"repair_attempts,omitempty"`  // stage -> attempts
	ReplanAttempts int                    `json:"replan_attempts,omitempty"`  // corrective re-plans triggered by executor failures
	LastFailure    map[string]interface{} `json:"last_failure,omitempty"`     // sanitized snapshot of last failure
	LastDiagnosis  map[string]interface{} `json:"last_diagnosis,omitempty"`   // last classification output
	LastRepairPlan map[string]interface{} `json:"last_repair_plan,omitempty"` // last proposed fix

	// Cached results (to prevent re-execution on resume)
	CachedIntent              map[string]interface{} `json:"cached_intent,omitempty"`
	CachedConnectorResolution map[string]interface{} `json:"cached_connector_resolution,omitempty"`
	CachedPlan                map[string]interface{} `json:"cached_plan,omitempty"`
	CachedCostEstimate        map[string]interface{} `json:"cached_cost_estimate,omitempty"`
	CachedValidation          map[string]interface{} `json:"cached_validation,omitempty"`

	// Dynamic execution plan (drives fully data-driven UI)
	ExecutionPlan *workflowtypes.ExecutionPlan `json:"execution_plan,omitempty"`

	// DAG workflow graph (for DAG execution mode)
	// Set when planner returns a workflow_graph in the plan
	WorkflowGraph *workflowtypes.WorkflowGraph `json:"workflow_graph,omitempty"`

	// DAG node outputs (maps node_id -> output data or artifact_ref)
	// Used to pass data between nodes during DAG execution
	NodeOutputs map[string]map[string]interface{} `json:"node_outputs,omitempty"`

	// Connection IDs (set once, never change)
	SourceConnectionID      string `json:"source_connection_id,omitempty"`
	DestinationConnectionID string `json:"destination_connection_id,omitempty"`

	// Connector Snapshots (versioned connectors, immutable once set)
	SourceConnectorSnapshot      *ConnectorSnapshot `json:"source_connector_snapshot,omitempty"`
	DestinationConnectorSnapshot *ConnectorSnapshot `json:"destination_connector_snapshot,omitempty"`

	// Selected tables/resources (optional; set via HITL when user selects)
	SelectedTables []string `json:"selected_tables,omitempty"`

	// HITL metadata captured during table selection (e.g., UI-provided transform suggestions).
	// This is propagated into the executor activity/task so the data plane can apply transforms.
	TableSelectionMetadata map[string]interface{} `json:"table_selection_metadata,omitempty"`

	// Metadata (immutable after set)
	SourceConnectorType      string `json:"source_connector_type,omitempty"`
	DestinationConnectorType string `json:"destination_connector_type,omitempty"`

	// State transition history (for debugging/audit)
	StateHistory []StateTransition `json:"state_history,omitempty"`

	// Retry counters (for intelligent backoff)
	IntentRetries       int `json:"intent_retries,omitempty"`
	ConnectorRetries    int `json:"connector_retries,omitempty"`
	ConnectionRetries   int `json:"connection_retries,omitempty"`
	PlannerRetries      int `json:"planner_retries,omitempty"`
}

// ConnectorSnapshot represents a versioned connector snapshot (immutable)
type ConnectorSnapshot struct {
	Type       string    `json:"type"`        // Connector type (e.g., "mysql", "aws_s3")
	Version    string    `json:"version"`     // Semantic version (e.g., "v1.1.0" or "latest")
	Path       string    `json:"path"`        // Filesystem path to connector
	Image      string    `json:"image"`       // Docker image name with tag
	Hash       string    `json:"hash"`        // SHA256 hash for integrity
	ResolvedAt time.Time `json:"resolved_at"` // When version was resolved
}

// StateTransition records a state change for audit/debugging
type StateTransition struct {
	From      PipelineState `json:"from"`
	To        PipelineState `json:"to"`
	Timestamp time.Time     `json:"timestamp"`
	Reason    string        `json:"reason,omitempty"`
}

// Transition validates and records a state transition
// Returns error if transition is invalid (FSM guard)
func (ws *WorkflowState) Transition(to PipelineState, workflowTime time.Time, reason string) error {
	from := ws.CurrentState

	// Validate transition
	if !isValidTransition(from, to) {
		return fmt.Errorf("invalid state transition: %s -> %s", from, to)
	}

	// Record transition
	ws.StateHistory = append(ws.StateHistory, StateTransition{
		From:      from,
		To:        to,
		Timestamp: workflowTime,
		Reason:    reason,
	})

	// Update current state
	ws.CurrentState = to

	// Clear wait reason if leaving wait state
	if !isWaitState(to) {
		ws.WaitReason = nil
	}

	return nil
}

// SetWaitReason sets the wait reason when entering a wait state
func (ws *WorkflowState) SetWaitReason(reason *WaitReason) {
	ws.WaitReason = reason
}

// ClearWaitReason clears the wait reason when resuming
func (ws *WorkflowState) ClearWaitReason() {
	ws.WaitReason = nil
}

// isValidTransition checks if a state transition is allowed
func isValidTransition(from, to PipelineState) bool {
	// Terminal states can't transition
	if from == StateCompleted || from == StateFailed || from == StateCancelled {
		return false
	}

	// Cancellation is allowed from any non-terminal state.
	if to == StateCancelled {
		return true
	}

	// Allow all forward transitions
	validTransitions := map[PipelineState][]PipelineState{
		StateInit: {
			StateIntentResolved,
			StateFailed,
		},
		StateIntentResolved: {
			StateConnectorResolved,
			StateFailed,
		},
		StateConnectorResolved: {
			StateWaitingForConnectionConfiguration,
			StateWaitingForConnectorGeneration,
			StatePlanned, // Skip wait states if all configured
			StateFailed,
		},
		StateWaitingForConnectionConfiguration: {
			StateConnectorResolved, // Retry validation after user configures
			StateExecuting,         // Executor-time recovery: retry execution after user fixes config/credentials
			StateFailed,
		},
		StateWaitingForConnectorGeneration: {
			StateConnectorResolved, // Retry after connectors generated
			StateFailed,
		},
		StateWaitingForTableSelection: {
			StateExecuting, // Retry execution after user selects tables
			StateFailed,
		},
		StateWaitingForApproval: {
			StatePlanned,   // Continue after approval
			StateValidated, // Skip to validation if approved
			StateExecuting, // Skip to execution if approved
			StateFailed,    // Reject
		},
		StatePlanned: {
			StateWaitingForApproval,
			StateValidated, // Skip approval if not required
			StateFailed,
		},
		StateValidated: {
			StateExecuting,
			StateDAGExecuting, // DAG execution mode
			StateFailed,
		},
		StateExecuting: {
			// Executor-time recovery: allow workflows to enter HITL waits for config/approval.
			StateWaitingForConnectionConfiguration,
			StateWaitingForTableSelection,
			StateWaitingForApproval,
			StateCompleted,
			StateFailed,
		},
		// DAG-specific transitions
		StateDAGExecuting: {
			StateWaitingForNodeInput, // Node needs user input
			StateCompleted,
			StateFailed,
		},
		StateWaitingForNodeInput: {
			StateDAGExecuting, // Resume after user provides input
			StateFailed,
		},
	}

	allowed, exists := validTransitions[from]
	if !exists {
		return false
	}

	for _, allowedState := range allowed {
		if allowedState == to {
			return true
		}
	}

	return false
}

// isWaitState returns true if the state is a wait state
func isWaitState(state PipelineState) bool {
	return state == StateWaitingForConnectionConfiguration ||
		state == StateWaitingForConnectorGeneration ||
		state == StateWaitingForTableSelection ||
		state == StateWaitingForApproval ||
		state == StateWaitingForNodeInput // DAG HITL wait state
}

// IsTerminalState returns true if the state is terminal
func (ws *WorkflowState) IsTerminalState() bool {
	return ws.CurrentState == StateCompleted || ws.CurrentState == StateFailed || ws.CurrentState == StateCancelled
}

// IsWaitingState returns true if the workflow is waiting for user input
func (ws *WorkflowState) IsWaitingState() bool {
	return isWaitState(ws.CurrentState)
}
