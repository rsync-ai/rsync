package workers

import (
	"context"
	"fmt"
	"time"
)

// ==============================================================================
// AGENT WORKER TYPES
// ==============================================================================
// This package defines the stateless agent workers driven by the Temporal
// adapter through the Redis correlation store.
//
// Key Principles:
// 1. Workers are STATELESS (can be killed and restarted)
// 2. Workers claim requests the Temporal adapter writes to the Redis
//    correlation store (each worker's startRedisPoller)
// 3. Workers write results back to that store (RouteResult), where the waiting
//    Temporal activity reads them
// 4. Workers are GENERIC (work with ANY connector via MCP)
// 5. Workers are AUTONOMOUS (make decisions, not just execute scripts)
// ==============================================================================

// Task represents a task sent to a worker from Temporal (via the correlation store)
type Task struct {
	TaskID        string                 `json:"task_id"`
	WorkflowID    string                 `json:"workflow_id"` // Temporal workflow ID
	StepID        string                 `json:"step_id"`     // Temporal activity ID (used for idempotency)
	TaskType      string                 `json:"task_type"`
	PipelineID    string                 `json:"pipeline_id"`
	ExecutionID   string                 `json:"execution_id"` // Pipeline execution ID
	UserID        string                 `json:"user_id"`
	Payload       map[string]interface{} `json:"payload"`
	Context       map[string]interface{} `json:"context"`
	AssignedAt    time.Time              `json:"assigned_at"`
	TimeoutSec    int                    `json:"timeout_sec"`
	RetryCount    int                    `json:"retry_count"`
	TraceID       string                 `json:"trace_id"`
	ChunkID       string                 `json:"chunk_id,omitempty"`       // NEW: For chunked operations (Phase 2.2)
	CorrelationID string                 `json:"correlation_id,omitempty"` // V2: For correlation store routing
}

// IdempotencyKey generates a deterministic idempotency key for this task
// Format: {pipeline_id}-{execution_id}-{step_id}[-{chunk_id}]
func (t *Task) IdempotencyKey() string {
	if t.ChunkID == "" {
		return fmt.Sprintf("%s-%s-%s", t.PipelineID, t.ExecutionID, t.StepID)
	}
	return fmt.Sprintf("%s-%s-%s-%s", t.PipelineID, t.ExecutionID, t.StepID, t.ChunkID)
}

// TaskResult represents a result sent back to Temporal (via the correlation store)
type TaskResult struct {
	TaskID      string                 `json:"task_id"`
	WorkflowID  string                 `json:"workflow_id"` // Temporal workflow ID
	StepID      string                 `json:"step_id"`     // Temporal activity ID
	Status      string                 `json:"status"`      // "success", "failure", "retry"
	Output      map[string]interface{} `json:"output"`
	Error       string                 `json:"error,omitempty"`
	NextAction  string                 `json:"next_action,omitempty"`
	CompletedAt time.Time              `json:"completed_at"`
	TraceID     string                 `json:"trace_id"`
}

// Worker is the interface that all agent workers must implement
type Worker interface {
	// Execute processes a task and returns a result
	// This is the ONLY method workers need to implement
	Execute(ctx context.Context, task Task) TaskResult

	// GetWorkerType returns the type of worker (e.g., "intent", "resolver")
	GetWorkerType() string

	// Start begins claiming requests from the Redis correlation store
	Start() error

	// Stop gracefully shuts down the worker
	Stop() error
}
