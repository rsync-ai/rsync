package handlers

import (
	"context"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"api-gateway/internal/db"
	"api-gateway/internal/kafka"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// StatusManager manages pipeline status updates
type StatusManager struct {
	mu sync.RWMutex
}

// NewStatusManager creates a new status manager
func NewStatusManager() *StatusManager {
	return &StatusManager{}
}

// UpdatePipelineStatus updates the status of a pipeline in the database
func (sm *StatusManager) UpdatePipelineStatus(id string, status string, result map[string]interface{}) {
	// Database update
	database := db.GetDB()
	if database == nil {
		log.Printf("Database not connected, cannot update pipeline %s", id)
		return
	}

	// Simple status update
	query := "UPDATE pipelines SET status = $1, updated_at = $2 WHERE id = $3"
	_, err := database.Exec(query, status, time.Now(), id)
	if err != nil {
		log.Printf("Failed to update pipeline %s status: %v", id, err)
		return
	}

	// Update stats if provided
	if result != nil {
		if rows, ok := result["rows_processed"].(float64); ok {
			if _, err := database.Exec("UPDATE pipelines SET rows_processed = $1 WHERE id = $2", int64(rows), id); err != nil {
				log.Printf("Failed to update pipeline %s rows_processed (ignored): %v", id, err)
			}
		}
		if cost, ok := result["cost"].(float64); ok {
			if _, err := database.Exec("UPDATE pipelines SET cost = $1 WHERE id = $2", cost, id); err != nil {
				log.Printf("Failed to update pipeline %s cost (ignored): %v", id, err)
			}
		}
	}

	log.Printf("✓ Updated pipeline %s status to %s", id, status)
}

// HandleAgentResponse processes an agent response and updates pipeline status
// Now accepts context for trace propagation
func (sm *StatusManager) HandleAgentResponse(ctx context.Context, response kafka.AgentResponse) error {
	// Create a child span for processing the agent response
	tracer := otel.Tracer("status-manager")
	ctx, span := tracer.Start(ctx, "status.handle-agent-response",
		trace.WithAttributes(
			attribute.String("agent.name", response.Agent),
			attribute.String("agent.status", response.Status),
		),
	)
	defer span.End()

	// Map agent status to pipeline status
	pipelineStatus := sm.mapAgentStatusToPipelineStatus(response)

	// Determine the pipeline ID (prefer PipelineID field, fallback to TraceID for backwards compatibility)
	pipelineID := response.PipelineID
	if pipelineID == "" {
		pipelineID = response.TraceID
	}

	span.SetAttributes(
		attribute.String("pipeline.id", pipelineID),
		attribute.String("pipeline.status", pipelineStatus),
	)

	// Update the pipeline
	sm.UpdatePipelineStatus(pipelineID, pipelineStatus, response.Result)

	return nil
}

// mapAgentStatusToPipelineStatus determines pipeline status from agent response.
//
// The executor in particular publishes several different response.Status
// values across its lifecycle:
//   - "success" / "completed" — task finished successfully
//   - "failed" / "error"      — task failed
//   - "cancelled"             — task was cancelled
//   - "waiting_for_*"         — HITL pause (table_selection, etc.)
//   - "running" / ""          — still in flight (heartbeat / progress event)
//
// The previous implementation collapsed every non-"error" executor response
// onto "completed", which had two visible bugs:
//   1. A "failed" executor response (response.Status="failed") was mapped
//      to "completed" because the check only matched "error".
//   2. A "waiting_for_table_selection" response (HITL pause) also mapped
//      to "completed" — the api-gateway told callers the pipeline was done
//      while the executor was actually still waiting on user input.
//   3. Any pre-final executor progress event also flipped status to
//      "completed" prematurely.
//
// Map the executor's actual status values, and treat anything we don't
// recognize as "processing" (in flight) so callers don't get false success.
func (sm *StatusManager) mapAgentStatusToPipelineStatus(response kafka.AgentResponse) string {
	// If there's an error, mark as failed
	if response.Status == "error" {
		return "failed"
	}

	// Map based on which agent completed
	switch response.Agent {
	case "planner":
		return "processing" // Pipeline is being processed
	case "validator":
		return "validating"
	case "decomposer":
		return "planning"
	case "transformer":
		return "transforming"
	case "executor":
		// Honour the executor's own status field. Only treat a real
		// success terminal as "completed".
		switch response.Status {
		case "success", "completed", "succeeded":
			return "completed"
		case "failed", "error":
			return "failed"
		case "silent_drop_detected", "silent_partial_drop_detected":
			// Phase 1 post-flight verifier. The executor finished without
			// raising but counts disagree (or source had data the
			// destination didn't receive). Surface as "failed" — never
			// let this look like a successful run.
			return "failed"
		case "credential_check_failed":
			// Phase 2.5 — unknown credential failure. Bubble up as failed
			// so it's not mistaken for success.
			return "failed"
		case "cancelled", "canceled":
			return "cancelled"
		default:
			if strings.HasPrefix(response.Status, "waiting_for") || response.Status == "waiting" || response.Status == "paused" {
				// HITL pause (table selection, approval, etc.). Surface as the
				// canonical waiting_for_user — the value the Temporal projector,
				// the /state endpoint, and both list projections agree on — so a
				// parked run never masquerades as 'running' or falls back to
				// 'pending' on any surface.
				return "waiting_for_user"
			}
			// Heartbeat, progress, or unknown intermediate state — keep
			// the pipeline marked as running until the executor sends a
			// terminal status.
			return "running"
		}
	case "policy":
		// Policy agent might block the pipeline
		if violation, ok := response.Result["violation"].(bool); ok && violation {
			return "blocked"
		}
		return "processing"
	default:
		return "processing"
	}
}

