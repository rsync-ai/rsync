package handlers

import (
	"database/sql"
	"net/http"
	"strconv"
	"strings"
	"time"

	"api-gateway/internal/db"
	"api-gateway/internal/security"

	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
)

type ExecutionSummary struct {
	ExecutionID string     `json:"execution_id"`
	PipelineID  string     `json:"pipeline_id"`
	Status      string     `json:"status"`
	StartTime   time.Time  `json:"start_time"`
	EndTime     *time.Time `json:"end_time,omitempty"`
	DurationMs  *int64     `json:"duration_ms,omitempty"`
	EventCount  int        `json:"event_count"`
	ErrorCount  int        `json:"error_count"`
	RetryCount  int        `json:"retry_count"`
	// Optional (may be unavailable depending on deployment)
	RowsProcessed  *int64 `json:"rows_processed,omitempty"`
	BytesProcessed *int64 `json:"bytes_processed,omitempty"`
}

type PipelineTrends struct {
	PipelineID       string             `json:"pipeline_id"`
	TotalRuns        int                `json:"total_runs"`
	SuccessRate      float64            `json:"success_rate"`
	AvgDurationMs    *int64             `json:"avg_duration_ms,omitempty"`
	RecentExecutions []ExecutionSummary `json:"recent_executions"`
}

// ComparePipelineRuns compares two pipeline executions
// GET /api/v1/pipelines/:id/compare?execution_a=...&execution_b=...
func ComparePipelineRuns(c *gin.Context) {
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}

	pipelineID, ok := requireUUIDParam(c, "id", "invalid_pipeline_id", "Invalid pipeline ID format")
	if !ok {
		return
	}
	// Gate before parameter validation: a pipeline outside the active workspace
	// must 404 rather than answer 400, which would otherwise distinguish "exists
	// but you sent bad params" from "does not exist".
	if _, ok := requirePipelineWorkspaceRole(c, pipelineID, security.WSViewer); !ok {
		return
	}

	execA := strings.TrimSpace(c.Query("execution_a"))
	execB := strings.TrimSpace(c.Query("execution_b"))

	if execA == "" || execB == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "missing_parameters",
			"message": "Both execution_a and execution_b are required",
		})
		return
	}

	// Fetch summaries for both executions
	summaryA, err := getExecutionSummary(database, pipelineID, execA)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{
			"error":   "execution_a_not_found",
			"message": err.Error(),
		})
		return
	}

	summaryB, err := getExecutionSummary(database, pipelineID, execB)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{
			"error":   "execution_b_not_found",
			"message": err.Error(),
		})
		return
	}

	// Calculate deltas
	comparison := map[string]interface{}{
		"execution_a": summaryA,
		"execution_b": summaryB,
		"deltas": map[string]interface{}{
			"duration_delta_ms":       calculateDelta(summaryA.DurationMs, summaryB.DurationMs),
			"duration_change_percent": calculatePercentChange(summaryA.DurationMs, summaryB.DurationMs),
			"event_count_delta":       summaryB.EventCount - summaryA.EventCount,
			"error_count_delta":       summaryB.ErrorCount - summaryA.ErrorCount,
			"retry_count_delta":       summaryB.RetryCount - summaryA.RetryCount,
			"rows_delta":              calculateDelta(summaryA.RowsProcessed, summaryB.RowsProcessed),
			"bytes_delta":             calculateDelta(summaryA.BytesProcessed, summaryB.BytesProcessed),
		},
	}

	c.JSON(http.StatusOK, comparison)
}

// GetPipelineTrends returns historical trends for a pipeline
// GET /api/v1/pipelines/:id/trends?limit=10
func GetPipelineTrends(c *gin.Context) {
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}

	pipelineID, ok := requireUUIDParam(c, "id", "invalid_pipeline_id", "Invalid pipeline ID format")
	if !ok {
		return
	}
	if _, ok := requirePipelineWorkspaceRole(c, pipelineID, security.WSViewer); !ok {
		return
	}

	limit := 10
	if s := strings.TrimSpace(c.Query("limit")); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v > 0 && v <= 50 {
			limit = v
		}
	}

	// Trend data is derived from the canonical event store (pipeline_run_events),
	// which is guaranteed to exist for monitored pipelines.
	//
	// 1) Identify recent execution_ids for this pipeline.
	args := []interface{}{pipelineID}

	execListQuery := `
		SELECT e.execution_id::text, MAX(e.received_at) as last_seen
		FROM pipeline_run_events e
		WHERE e.pipeline_id = $1 AND e.execution_id IS NOT NULL
	`
	execListQuery += " GROUP BY e.execution_id ORDER BY last_seen DESC LIMIT " + strconv.Itoa(limit)

	rows, err := database.Query(execListQuery, args...)
	if err != nil {
		log.WithError(err).Error("Failed to fetch pipeline trends (execution list)")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch trends"})
		return
	}
	defer rows.Close()

	executionIDs := make([]string, 0, limit)
	for rows.Next() {
		var execID string
		var _lastSeen time.Time
		if err := rows.Scan(&execID, &_lastSeen); err != nil {
			log.WithError(err).Warn("Failed to scan execution id")
			continue
		}
		if strings.TrimSpace(execID) != "" {
			executionIDs = append(executionIDs, execID)
		}
	}

	// 2) Total distinct runs
	totalRunsQuery := `
		SELECT COUNT(DISTINCT e.execution_id)
		FROM pipeline_run_events e
		WHERE e.pipeline_id = $1 AND e.execution_id IS NOT NULL
	`
	totalArgs := []interface{}{pipelineID}
	var totalRuns int
	_ = database.QueryRow(totalRunsQuery, totalArgs...).Scan(&totalRuns)

	// 3) Build summaries
	executions := make([]ExecutionSummary, 0, len(executionIDs))
	successCount := 0
	for _, execID := range executionIDs {
		summary, err := getExecutionSummary(database, pipelineID, execID)
		if err != nil {
			continue
		}
		executions = append(executions, *summary)
		if summary.Status == "completed" || summary.Status == "success" {
			successCount++
		}
	}

	// Calculate success rate
	successRate := 0.0
	if totalRuns > 0 {
		successRate = float64(successCount) / float64(totalRuns)
	}

	// Calculate average duration
	var avgDurationMs *int64
	durationCount := 0
	var totalDuration int64
	for _, exec := range executions {
		if exec.DurationMs != nil {
			totalDuration += *exec.DurationMs
			durationCount++
		}
	}
	if durationCount > 0 {
		avg := totalDuration / int64(durationCount)
		avgDurationMs = &avg
	}

	c.JSON(http.StatusOK, PipelineTrends{
		PipelineID:       pipelineID,
		TotalRuns:        totalRuns,
		SuccessRate:      successRate,
		AvgDurationMs:    avgDurationMs,
		RecentExecutions: executions,
	})
}

// Helper: fetch summary for a single execution. Takes no user id: both callers
// have already proven the pipeline belongs to the caller's active workspace, and
// re-filtering by created_by here would hide a teammate's runs from a legitimate
// workspace member.
func getExecutionSummary(database *sql.DB, pipelineID, executionID string) (*ExecutionSummary, error) {
	// Derive summary from the canonical event store for this execution_id.
	query := `
		SELECT
			MIN(COALESCE(e.occurred_at, e.received_at)) as start_time,
			MAX(COALESCE(e.occurred_at, e.received_at)) FILTER (WHERE e.event_type IN ('PIPELINE_COMPLETED','PIPELINE_FAILED')) as end_time,
			COUNT(*) as event_count,
			COUNT(*) FILTER (WHERE e.severity = 'error') as error_count,
			COUNT(*) FILTER (WHERE (e.payload->>'retry' = 'true' OR e.event_type ILIKE '%RETRY%')) as retry_count,
			BOOL_OR(e.event_type = 'PIPELINE_COMPLETED') as has_completed,
			BOOL_OR(e.event_type = 'PIPELINE_FAILED') as has_failed
		FROM pipeline_run_events e
		WHERE e.pipeline_id = $1 AND e.execution_id = $2::uuid
	`

	args := []interface{}{pipelineID, executionID}

	var (
		startTime    sql.NullTime
		endTime      sql.NullTime
		eventCount   int
		errorCount   int
		retryCount   int
		hasCompleted bool
		hasFailed    bool
	)

	if err := database.QueryRow(query, args...).Scan(
		&startTime, &endTime, &eventCount, &errorCount, &retryCount, &hasCompleted, &hasFailed,
	); err != nil {
		return nil, err
	}

	status := "running"
	if hasFailed {
		status = "failed"
	} else if hasCompleted {
		status = "completed"
	}

	summary := &ExecutionSummary{
		ExecutionID: executionID,
		PipelineID:  pipelineID,
		Status:      status,
		EventCount:  eventCount,
		ErrorCount:  errorCount,
		RetryCount:  retryCount,
	}

	if startTime.Valid {
		summary.StartTime = startTime.Time
	} else {
		summary.StartTime = time.Now().UTC()
	}
	if endTime.Valid {
		summary.EndTime = &endTime.Time
		dur := endTime.Time.Sub(summary.StartTime).Milliseconds()
		summary.DurationMs = &dur
	}

	return summary, nil
}

func calculateDelta(a, b *int64) *int64 {
	if a == nil || b == nil {
		return nil
	}
	delta := *b - *a
	return &delta
}

func calculatePercentChange(a, b *int64) *float64 {
	if a == nil || b == nil || *a == 0 {
		return nil
	}
	change := (float64(*b) - float64(*a)) / float64(*a) * 100
	return &change
}
