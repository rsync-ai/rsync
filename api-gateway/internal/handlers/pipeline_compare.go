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
	PipelineID string `json:"pipeline_id"`
	// TotalRuns counts every run this pipeline ever had; the rest of the fields
	// describe only the RecentExecutions window (?limit=, default 10).
	TotalRuns int `json:"total_runs"`
	// SuccessRate is SucceededRuns / FinishedRuns over the window. A run still in
	// flight has no outcome yet, so it counts toward neither side.
	SuccessRate float64 `json:"success_rate"`
	// FinishedRuns is how many runs in the window reached completed or failed;
	// SucceededRuns is how many of those completed. They let a client say
	// "9 of the last 10 finished runs succeeded" instead of a bare percentage.
	FinishedRuns     int                `json:"finished_runs"`
	SucceededRuns    int                `json:"succeeded_runs"`
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

// trendRunsWhere selects the rows that belong to a run. A CDC pipeline's stream
// stats carry execution_id = pipeline_id, a key that stays stable across
// restarts, so that id is the stream and not a run. Counted as one, it became
// the newest "run", and the comparison card compared the latest run against it
// ("— → —").
const trendRunsWhere = `e.pipeline_id = $1 AND e.execution_id IS NOT NULL AND e.execution_id <> e.pipeline_id
	`

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
		WHERE ` + trendRunsWhere
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
		WHERE ` + trendRunsWhere
	totalArgs := []interface{}{pipelineID}
	var totalRuns int
	_ = database.QueryRow(totalRunsQuery, totalArgs...).Scan(&totalRuns)

	// 3) Build summaries
	executions := make([]ExecutionSummary, 0, len(executionIDs))
	for _, execID := range executionIDs {
		summary, err := getExecutionSummary(database, pipelineID, execID)
		if err != nil {
			continue
		}
		executions = append(executions, *summary)
	}

	trends := summarizeTrendWindow(executions)
	trends.PipelineID = pipelineID
	trends.TotalRuns = totalRuns
	c.JSON(http.StatusOK, trends)
}

// summarizeTrendWindow computes the success rate and average duration over one
// window of runs. Both sides of the rate come from the same window: the old code
// divided the window's successes by every run the pipeline ever had, so a
// pipeline with 100 runs whose last 10 all succeeded reported 10%. Runs still in
// flight are left out of both sides — they have no outcome yet.
func summarizeTrendWindow(executions []ExecutionSummary) PipelineTrends {
	t := PipelineTrends{RecentExecutions: executions}
	var totalDuration int64
	durationCount := 0
	for _, exec := range executions {
		switch exec.Status {
		case "completed", "success":
			t.FinishedRuns++
			t.SucceededRuns++
		case "failed":
			t.FinishedRuns++
		}
		if exec.DurationMs != nil {
			totalDuration += *exec.DurationMs
			durationCount++
		}
	}
	if t.FinishedRuns > 0 {
		t.SuccessRate = float64(t.SucceededRuns) / float64(t.FinishedRuns)
	}
	if durationCount > 0 {
		avg := totalDuration / int64(durationCount)
		t.AvgDurationMs = &avg
	}
	return t
}

// trendStatusFromExecution maps executionStatusSQL's answer onto the trends
// vocabulary (completed, failed, cancelled, running).
func trendStatusFromExecution(s string) string {
	switch s {
	case "completed", "success":
		return "completed"
	case "failed", "error", "silent_drop_detected", "silent_partial_drop_detected", "credential_check_failed":
		return "failed"
	case "cancelled":
		return "cancelled"
	}
	return "running"
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
	// COUNT(*) above counts ROWS, and every stage transition has two
	// (KI-EVENTS-DUAL-ID-NAMESPACE-DUPES), so a run read as twice as eventful and,
	// on a failed stage, twice as erroneous. Recount from per-producer groups.
	// Fail-soft: the raw counts are still an answer, just an inflated one.
	if groups, gerr := loadRunEventGroups(database, pipelineID, executionID); gerr == nil {
		eventCount, errorCount = logicalRunEventCounts(groups)
	} else {
		log.WithError(gerr).WithField("execution_id", executionID).Warn("trends: event group recount failed; event/error counts include both producers' copies")
	}

	status := "running"
	if hasFailed {
		status = "failed"
	} else if hasCompleted {
		status = "completed"
	}
	// The events cannot say a run failed: PIPELINE_FAILED has no producer, and
	// PIPELINE_COMPLETED is emitted before the postflight check that can still
	// fail the run (KI-SILENTDROP-COMPLETED-EVENT). executions.status is the
	// terminal field, read the way the executions endpoints read it. A run with
	// no executions row keeps the events' answer.
	var execStatus string
	err := database.QueryRow(`SELECT `+executionStatusSQL+executionFromSQL+`
		WHERE e.id = $1::uuid AND e.pipeline_id = $2::uuid`, executionID, pipelineID).Scan(&execStatus)
	if err == nil {
		status = trendStatusFromExecution(execStatus)
	} else if err != sql.ErrNoRows {
		log.WithError(err).WithField("execution_id", executionID).Warn("trends: executions status lookup failed; using the event-derived status")
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

// runEventGroup is one (event_type, stage, producer) bucket of a run's events.
type runEventGroup struct {
	EventType string
	StageID   string
	Producer  string // payload schema_version: "1" orchestrator workers, "2" the V2 workflow
	Count     int
	Errors    int
}

func loadRunEventGroups(database *sql.DB, pipelineID, executionID string) ([]runEventGroup, error) {
	rows, err := database.Query(`
		SELECT COALESCE(e.event_type,''), COALESCE(e.stage_id,''), COALESCE(e.payload->>'schema_version',''),
			COUNT(*), COUNT(*) FILTER (WHERE e.severity = 'error')
		FROM pipeline_run_events e
		WHERE e.pipeline_id = $1 AND e.execution_id = $2::uuid
		GROUP BY 1, 2, 3
	`, pipelineID, executionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []runEventGroup
	for rows.Next() {
		var g runEventGroup
		if err := rows.Scan(&g.EventType, &g.StageID, &g.Producer, &g.Count, &g.Errors); err != nil {
			return nil, err
		}
		out = append(out, g)
	}
	return out, rows.Err()
}

// dualProducerLifecycleTypes are the transitions BOTH the orchestrator's stage
// workers and the V2 workflow report, each from where it stands, with different
// payloads and no shared id (KI-EVENTS-DUAL-ID-NAMESPACE-DUPES).
var dualProducerLifecycleTypes = map[string]bool{
	"STAGE_STARTED":   true,
	"STAGE_COMPLETED": true,
	"STAGE_FAILED":    true,
	// Both park the run on HITL: the workflow via emitPipelineWaitingEvent, the
	// orchestrator's resolver/validator/executor workers on their own.
	"PIPELINE_WAITING": true,
}

// canonicalRunEventStage folds the two producers' names for one stage; it mirrors
// canonicalStageId in frontend/src/lib/pipeline/eventNormalizer.ts.
func canonicalRunEventStage(stage string) string {
	s := strings.ToLower(strings.TrimSpace(stage))
	switch s {
	case "resolver":
		return "capability_resolver"
	case "connection_validator":
		return "connection_validation"
	}
	return s
}

// logicalRunEventCounts counts each stage transition once however many producers
// reported it. Every producer reports each transition it observes, so the number
// of transitions of one (type, stage) is the MOST any single producer reported —
// not the sum. MAX, not DISTINCT or a time window: the V2 workflow re-dispatches
// the executor (chunked continuation, HITL repair, retries) and each dispatch's
// STAGE_COMPLETED is a real, separate transition the orchestrator reports and the
// workflow does not, so collapsing by key would undercount them. Everything else
// is counted as stored.
func logicalRunEventCounts(groups []runEventGroup) (events, errors int) {
	type key struct{ typ, stage string }
	type maxes struct{ count, errors int }
	lifecycle := map[key]map[string]*maxes{}
	for _, g := range groups {
		typ := strings.ToUpper(strings.TrimSpace(g.EventType))
		if !dualProducerLifecycleTypes[typ] || strings.TrimSpace(g.StageID) == "" {
			events += g.Count
			errors += g.Errors
			continue
		}
		k := key{typ, canonicalRunEventStage(g.StageID)}
		if lifecycle[k] == nil {
			lifecycle[k] = map[string]*maxes{}
		}
		// Two stage names can fold into one producer's bucket; they add.
		m := lifecycle[k][g.Producer]
		if m == nil {
			m = &maxes{}
			lifecycle[k][g.Producer] = m
		}
		m.count += g.Count
		m.errors += g.Errors
	}
	for _, byProducer := range lifecycle {
		best := maxes{}
		for _, m := range byProducer {
			if m.count > best.count {
				best.count = m.count
			}
			if m.errors > best.errors {
				best.errors = m.errors
			}
		}
		events += best.count
		errors += best.errors
	}
	return events, errors
}
