package handlers

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"api-gateway/internal/config"
	"api-gateway/internal/db"
	"api-gateway/internal/security"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

// ============================================================================
// Data Structures
// ============================================================================

// MonitoringOverview aggregates all monitoring layers for a pipeline
type MonitoringOverview struct {
	PipelineID          string                 `json:"pipeline_id"`
	TimeRange           TimeRange              `json:"time_range"`
	AgentReasoning      *AgentReasoningSummary `json:"agent_reasoning,omitempty"`
	AgentReasoningError string                 `json:"agent_reasoning_error,omitempty"`
	DataPlane           *DataPlaneSummary      `json:"data_plane,omitempty"`
	DataPlaneError      string                 `json:"data_plane_error,omitempty"`
	Infrastructure      *InfrastructureSummary `json:"infrastructure,omitempty"`
	InfrastructureError string                 `json:"infrastructure_error,omitempty"`
	Correlations        []Correlation          `json:"correlations,omitempty"`
	Links               MonitoringLinks        `json:"links"`
}

// TimeRange represents the query time window
type TimeRange struct {
	Since time.Time `json:"since"`
	Until time.Time `json:"until"`
}

// AgentReasoningSummary summarizes agent decision-making
type AgentReasoningSummary struct {
	TotalDecisions   int             `json:"total_decisions"`
	HighConfidence   int             `json:"high_confidence"`   // >= 0.9
	MediumConfidence int             `json:"medium_confidence"` // 0.7-0.89
	LowConfidence    int             `json:"low_confidence"`    // < 0.7
	AvgConfidence    float64         `json:"avg_confidence"`
	RecentDecisions  []AgentDecision `json:"recent_decisions,omitempty"`
}

// AgentDecision represents a single agent decision event
type AgentDecision struct {
	EventID    string    `json:"event_id"`
	Timestamp  time.Time `json:"timestamp"`
	StageID    string    `json:"stage_id,omitempty"`
	Decision   string    `json:"decision"`
	Rationale  string    `json:"rationale,omitempty"`
	Confidence *float64  `json:"confidence,omitempty"`
	TraceID    string    `json:"trace_id,omitempty"`
}

// DataPlaneSummary summarizes pipeline data movement
type DataPlaneSummary struct {
	TotalRowsProcessed  int64      `json:"total_rows_processed"`
	TotalBytesProcessed int64      `json:"total_bytes_processed"`
	AvgThroughput       float64    `json:"avg_throughput_rows_per_sec,omitempty"`
	CDCLagMs            *int64     `json:"cdc_lag_ms,omitempty"`
	ErrorCount          int        `json:"error_count"`
	LastMetricTime      *time.Time `json:"last_metric_time,omitempty"`
}

// InfrastructureSummary summarizes system health
type InfrastructureSummary struct {
	ComponentsMonitored int               `json:"components_monitored"`
	HealthyComponents   int               `json:"healthy_components"`
	DegradedComponents  int               `json:"degraded_components"`
	UnhealthyComponents int               `json:"unhealthy_components"`
	ActiveIssues        int               `json:"active_issues"`
	CriticalIssues      int               `json:"critical_issues"`
	RecentIssues        []SentinelIssue   `json:"recent_issues,omitempty"`
	ComponentHealth     []ComponentHealth `json:"component_health,omitempty"`
}

// ComponentHealth represents health of a single component
type ComponentHealth struct {
	ComponentID       string                 `json:"component_id"`
	ComponentType     string                 `json:"component_type"`
	Status            string                 `json:"status"`
	LastHeartbeat     time.Time              `json:"last_heartbeat"`
	MessagesProcessed int64                  `json:"messages_processed"`
	ErrorCount        int64                  `json:"error_count"`
	ConsumerLag       int64                  `json:"consumer_lag,omitempty"`
	LastError         string                 `json:"last_error,omitempty"`
	Metadata          map[string]interface{} `json:"metadata,omitempty"`
	UpdatedAt         time.Time              `json:"updated_at"`
}

// SentinelIssue represents an active or recent issue
type SentinelIssue struct {
	ID              string                 `json:"id"`
	Type            string                 `json:"type"`
	Severity        string                 `json:"severity"`
	ComponentID     string                 `json:"component_id"`
	ComponentType   string                 `json:"component_type"`
	Description     string                 `json:"description"`
	DetectedAt      time.Time              `json:"detected_at"`
	ResolvedAt      *time.Time             `json:"resolved_at,omitempty"`
	OccurrenceCount int                    `json:"occurrence_count"`
	LastOccurrence  time.Time              `json:"last_occurrence"`
	Metadata        map[string]interface{} `json:"metadata,omitempty"`
}

// Correlation links agent decisions to infrastructure events
type Correlation struct {
	AgentDecision AgentDecision  `json:"agent_decision"`
	InfraEvent    *SentinelIssue `json:"infra_event,omitempty"`
	Confidence    float64        `json:"confidence"`
	Method        string         `json:"method"` // trace_id_match, time_window_semantic
}

// MonitoringLinks provides deep-links to external tools
type MonitoringLinks struct {
	SigNozTrace     string `json:"signoz_trace,omitempty"`
	SigNozMetrics   string `json:"signoz_metrics,omitempty"`
	SigNozDashboard string `json:"signoz_dashboard,omitempty"`
}

// PaginatedIssuesResponse wraps paginated Sentinel issues
type PaginatedIssuesResponse struct {
	Issues     []SentinelIssue `json:"issues"`
	HasMore    bool            `json:"has_more"`
	NextCursor string          `json:"next_cursor,omitempty"`
	Total      int             `json:"total,omitempty"`
}

// PaginatedHealthResponse wraps paginated component health
type PaginatedHealthResponse struct {
	Components []ComponentHealth `json:"components"`
	HasMore    bool              `json:"has_more"`
	NextCursor string            `json:"next_cursor,omitempty"`
	Total      int               `json:"total,omitempty"`
}

// ============================================================================
// Handlers
// ============================================================================

// GetPipelineMonitoringOverview returns aggregated monitoring data for a pipeline
// GET /api/v1/pipelines/:id/monitoring/overview?since=ISO8601&until=ISO8601&range=last_hour
func GetPipelineMonitoringOverview(c *gin.Context) {
	features := config.GetFeatures()
	if features == nil || !features.MonitoringOverview {
		c.JSON(http.StatusNotFound, gin.H{"error": "Monitoring overview not enabled"})
		return
	}

	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}

	pipelineID := c.Param("id")
	if _, err := uuid.Parse(pipelineID); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Pipeline not found"})
		return
	}

	// RBAC: read gate scoped to the caller's ACTIVE workspace, same as every other
	// /pipelines/:id* handler. Membership alone is not enough — a user who belongs
	// to two workspaces must not read workspace A's table/run metrics while
	// workspace B is active, which is what the old membership-only check allowed.
	if _, ok := requirePipelineWorkspaceRole(c, pipelineID, security.WSViewer); !ok {
		return
	}

	// Parse time range
	timeRange := parseTimeRange(c)

	overview := MonitoringOverview{
		PipelineID: pipelineID,
		TimeRange:  timeRange,
		Links:      generateMonitoringLinks(features, pipelineID, ""),
	}

	// Fetch agent reasoning summary (graceful degradation)
	if reasoning, err := fetchAgentReasoning(database, pipelineID, timeRange); err != nil {
		overview.AgentReasoningError = err.Error()
		log.Warnf("Failed to fetch agent reasoning for pipeline %s: %v", pipelineID, err)
	} else {
		overview.AgentReasoning = reasoning
	}

	// Fetch data plane summary (graceful degradation)
	if dataPlane, err := fetchDataPlaneSummary(database, pipelineID, timeRange); err != nil {
		overview.DataPlaneError = err.Error()
		log.Warnf("Failed to fetch data plane metrics for pipeline %s: %v", pipelineID, err)
	} else {
		overview.DataPlane = dataPlane
	}

	// Fetch infrastructure summary (graceful degradation)
	if infra, err := fetchInfrastructureSummary(database, timeRange); err != nil {
		overview.InfrastructureError = err.Error()
		log.Warnf("Failed to fetch infrastructure summary: %v", err)
	} else {
		overview.Infrastructure = infra
	}

	// Compute correlations if we have both agent decisions and infra issues
	if overview.AgentReasoning != nil && overview.Infrastructure != nil {
		overview.Correlations = computeCorrelations(
			overview.AgentReasoning.RecentDecisions,
			overview.Infrastructure.RecentIssues,
		)
	}

	c.JSON(http.StatusOK, overview)
}

// GetSentinelIssues returns paginated Sentinel issues
// GET /api/v1/monitoring/sentinel/issues?limit=20&before_detected_at=ISO8601&severity=critical&component_type=kafka_consumer
func GetSentinelIssues(c *gin.Context) {
	features := config.GetFeatures()
	if features == nil || !features.MonitoringInfra {
		c.JSON(http.StatusNotFound, gin.H{"error": "Infrastructure monitoring not enabled"})
		return
	}

	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}

	// RBAC: power_user or admin required
	userRole := security.GetUserRole(c)
	if userRole != security.RolePowerUser && userRole != security.RoleAdmin {
		c.JSON(http.StatusForbidden, gin.H{"error": "Insufficient permissions"})
		return
	}

	// Parse pagination params
	limit := 20
	if s := c.Query("limit"); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v > 0 && v <= 100 {
			limit = v
		}
	}

	var beforeDetectedAt *time.Time
	if s := c.Query("before_detected_at"); s != "" {
		if t, err := time.Parse(time.RFC3339, s); err == nil {
			beforeDetectedAt = &t
		}
	}

	// Parse filters
	severity := c.Query("severity")
	componentType := c.Query("component_type")
	componentID := c.Query("component_id")
	resolved := c.Query("resolved") // "true", "false", or empty (all)

	// Build query
	args := []interface{}{}
	argIdx := 1
	query := `
		SELECT 
			id, type, severity, component_id, component_type, 
			description, detected_at, resolved_at, occurrence_count, 
			last_occurrence, metadata
		FROM sentinel_active_issues
		WHERE 1=1
	`

	if beforeDetectedAt != nil {
		query += fmt.Sprintf(" AND detected_at < $%d", argIdx)
		args = append(args, *beforeDetectedAt)
		argIdx++
	}

	if severity != "" {
		query += fmt.Sprintf(" AND severity = $%d", argIdx)
		args = append(args, severity)
		argIdx++
	}

	if componentType != "" {
		query += fmt.Sprintf(" AND component_type = $%d", argIdx)
		args = append(args, componentType)
		argIdx++
	}

	if componentID != "" {
		query += fmt.Sprintf(" AND component_id = $%d", argIdx)
		args = append(args, componentID)
		argIdx++
	}

	if resolved == "true" {
		query += " AND resolved_at IS NOT NULL"
	} else if resolved == "false" {
		query += " AND resolved_at IS NULL"
	}

	query += fmt.Sprintf(" ORDER BY detected_at DESC LIMIT $%d", argIdx)
	args = append(args, limit+1) // Fetch one extra to check has_more

	rows, err := database.Query(query, args...)
	if err != nil {
		log.Errorf("Failed to query sentinel issues: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch issues"})
		return
	}
	defer rows.Close()

	issues := make([]SentinelIssue, 0, limit)
	for rows.Next() {
		var issue SentinelIssue
		var resolvedAt sql.NullTime
		var metadataBytes []byte

		if err := rows.Scan(
			&issue.ID, &issue.Type, &issue.Severity, &issue.ComponentID, &issue.ComponentType,
			&issue.Description, &issue.DetectedAt, &resolvedAt, &issue.OccurrenceCount,
			&issue.LastOccurrence, &metadataBytes,
		); err != nil {
			log.Errorf("Failed to scan issue row: %v", err)
			continue
		}

		if resolvedAt.Valid {
			issue.ResolvedAt = &resolvedAt.Time
		}

		if len(metadataBytes) > 0 {
			json.Unmarshal(metadataBytes, &issue.Metadata)
		}

		issues = append(issues, issue)
	}

	hasMore := len(issues) > limit
	if hasMore {
		issues = issues[:limit]
	}

	var nextCursor string
	if hasMore && len(issues) > 0 {
		nextCursor = issues[len(issues)-1].DetectedAt.Format(time.RFC3339)
	}

	c.JSON(http.StatusOK, PaginatedIssuesResponse{
		Issues:     issues,
		HasMore:    hasMore,
		NextCursor: nextCursor,
	})
}

// GetSentinelHealth returns current health status of all components
// GET /api/v1/monitoring/sentinel/health?component_type=kafka_consumer&status=unhealthy
func GetSentinelHealth(c *gin.Context) {
	features := config.GetFeatures()
	if features == nil || !features.MonitoringInfra {
		c.JSON(http.StatusNotFound, gin.H{"error": "Infrastructure monitoring not enabled"})
		return
	}

	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}

	// RBAC: authenticated user (aggregated, non-sensitive)
	// No additional checks needed

	// Parse filters
	componentType := c.Query("component_type")
	status := c.Query("status")

	args := []interface{}{}
	argIdx := 1
	query := `
		SELECT 
			component_id, component_type, status, last_heartbeat,
			messages_processed, error_count, consumer_lag, last_error,
			metadata, updated_at
		FROM sentinel_component_health
		WHERE 1=1
	`

	if componentType != "" {
		query += fmt.Sprintf(" AND component_type = $%d", argIdx)
		args = append(args, componentType)
		argIdx++
	}

	if status != "" {
		query += fmt.Sprintf(" AND status = $%d", argIdx)
		args = append(args, status)
		argIdx++
	}

	query += " ORDER BY updated_at DESC"

	rows, err := database.Query(query, args...)
	if err != nil {
		log.Errorf("Failed to query component health: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch health data"})
		return
	}
	defer rows.Close()

	components := make([]ComponentHealth, 0)
	for rows.Next() {
		var comp ComponentHealth
		var lastError sql.NullString
		var metadataBytes []byte

		if err := rows.Scan(
			&comp.ComponentID, &comp.ComponentType, &comp.Status, &comp.LastHeartbeat,
			&comp.MessagesProcessed, &comp.ErrorCount, &comp.ConsumerLag, &lastError,
			&metadataBytes, &comp.UpdatedAt,
		); err != nil {
			// A row that will not scan is a broken read, not a component to omit. Skipping
			// it produced the worst possible answer: a 200 whose Total agreed with the
			// truncated list, so the response was internally consistent and externally
			// wrong, and the caller had no way to tell a healthy fleet from an unreadable
			// one. Same shape as the admin_settings.go half of F-260, fixed in #752.
			log.Errorf("Failed to scan health row: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch health data"})
			return
		}

		if lastError.Valid {
			comp.LastError = lastError.String
		}

		if len(metadataBytes) > 0 {
			// Not fatal, unlike a scan failure: metadata is JSONB, so Postgres has already
			// guaranteed it parses, and the only way to land here is a stored value that
			// is valid JSON but not an object. That costs this one component its metadata,
			// not the caller's ability to trust the status column — but it must not pass
			// silently, which is what dropping the error did.
			if err := json.Unmarshal(metadataBytes, &comp.Metadata); err != nil {
				log.Errorf("Failed to decode health metadata for component %s: %v", comp.ComponentID, err)
			}
		}

		components = append(components, comp)
	}

	// rows.Next() returning false does not distinguish "done" with "failed": a connection
	// dropped mid-iteration ends the loop exactly like a complete read. Without this check
	// a database problem was reported as an empty-but-200 list of zero unhealthy
	// components — the most reassuring possible rendering of an outage.
	if err := rows.Err(); err != nil {
		log.Errorf("Failed to iterate health rows: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch health data"})
		return
	}

	// HasMore is false because the query above is genuinely unpaginated — no LIMIT, no
	// cursor — so this really is every row matching the filter, and Total is a count of
	// that whole set rather than of whatever survived scanning. Both statements depend on
	// the two error returns above; if a LIMIT is ever added here, these become lies and
	// need a cursor and a COUNT(*) to stay true.
	c.JSON(http.StatusOK, PaginatedHealthResponse{
		Components: components,
		HasMore:    false,
		Total:      len(components),
	})
}

// ============================================================================
// Helper Functions
// ============================================================================

func parseTimeRange(c *gin.Context) TimeRange {
	now := time.Now()

	// Check for preset ranges
	if rangeStr := c.Query("range"); rangeStr != "" {
		switch rangeStr {
		case "last_hour":
			return TimeRange{Since: now.Add(-1 * time.Hour), Until: now}
		case "last_6h":
			return TimeRange{Since: now.Add(-6 * time.Hour), Until: now}
		case "last_24h":
			return TimeRange{Since: now.Add(-24 * time.Hour), Until: now}
		case "last_7d":
			return TimeRange{Since: now.Add(-7 * 24 * time.Hour), Until: now}
		}
	}

	// Check for explicit since/until
	since := now.Add(-1 * time.Hour) // default: last hour
	until := now

	if s := c.Query("since"); s != "" {
		if t, err := time.Parse(time.RFC3339, s); err == nil {
			since = t
		}
	}

	if s := c.Query("until"); s != "" {
		if t, err := time.Parse(time.RFC3339, s); err == nil {
			until = t
		}
	}

	return TimeRange{Since: since, Until: until}
}

// canAccessPipeline is GONE on purpose. It gated on workspace MEMBERSHIP only —
// "does the caller belong to the workspace that owns this pipeline" — and never
// on the caller's ACTIVE workspace, so a user in two workspaces could read
// workspace A's pipeline while B was active. Its three former call sites
// (monitoring overview, table stats, schedule list) now use
// requirePipelineWorkspaceRole, which binds (resource, user, active workspace)
// in one query and 404s rather than confirming a foreign resource exists.
// Do not reintroduce a membership-only pipeline gate.

func fetchAgentReasoning(database *sql.DB, pipelineID string, timeRange TimeRange) (*AgentReasoningSummary, error) {
	query := `
		SELECT 
			event_id, occurred_at, stage_id, trace_id, payload
		FROM pipeline_run_events
		WHERE pipeline_id = $1
		  AND occurred_at BETWEEN $2 AND $3
		  AND event_type IN ('AGENT_DECISION', 'PLANNER_DECISION', 'DECISION_MADE')
		ORDER BY occurred_at DESC
		LIMIT 20
	`

	rows, err := database.Query(query, pipelineID, timeRange.Since, timeRange.Until)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	decisions := make([]AgentDecision, 0)
	var totalConfidence float64
	var confidenceCount int
	highConf, medConf, lowConf := 0, 0, 0

	for rows.Next() {
		var decision AgentDecision
		var occurredAt sql.NullTime
		var stageID, traceID sql.NullString
		var payloadBytes []byte

		if err := rows.Scan(&decision.EventID, &occurredAt, &stageID, &traceID, &payloadBytes); err != nil {
			continue
		}

		if occurredAt.Valid {
			decision.Timestamp = occurredAt.Time
		}
		if stageID.Valid {
			decision.StageID = stageID.String
		}
		if traceID.Valid {
			decision.TraceID = traceID.String
		}

		// Parse payload for decision details
		var payload map[string]interface{}
		if len(payloadBytes) > 0 {
			json.Unmarshal(payloadBytes, &payload)

			if d, ok := payload["decision"].(string); ok {
				decision.Decision = d
			}
			if r, ok := payload["rationale"].(string); ok {
				decision.Rationale = r
			}
			if c, ok := payload["confidence"].(float64); ok {
				decision.Confidence = &c
				totalConfidence += c
				confidenceCount++

				if c >= 0.9 {
					highConf++
				} else if c >= 0.7 {
					medConf++
				} else {
					lowConf++
				}
			}
		}

		decisions = append(decisions, decision)
	}

	avgConfidence := 0.0
	if confidenceCount > 0 {
		avgConfidence = totalConfidence / float64(confidenceCount)
	}

	return &AgentReasoningSummary{
		TotalDecisions:   len(decisions),
		HighConfidence:   highConf,
		MediumConfidence: medConf,
		LowConfidence:    lowConf,
		AvgConfidence:    avgConfidence,
		RecentDecisions:  decisions,
	}, nil
}

func fetchDataPlaneSummary(database *sql.DB, pipelineID string, timeRange TimeRange) (*DataPlaneSummary, error) {
	query := `
		SELECT 
			occurred_at,
			payload
		FROM pipeline_run_events
		WHERE pipeline_id = $1
		  AND occurred_at BETWEEN $2 AND $3
		  AND event_type = 'DATA_PLANE_METRICS'
		ORDER BY occurred_at DESC
	`

	rows, err := database.Query(query, pipelineID, timeRange.Since, timeRange.Until)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// DATA_PLANE_METRICS events are cumulative counters; for an overview we want the
	// latest/highest totals, not a sum across events (which would over-count).
	var totalRows, totalBytes int64
	var errorCount int
	var lastMetricTime *time.Time
	var cdcLagMs *int64
	throughputSamples := make([]float64, 0)

	for rows.Next() {
		var occurredAt time.Time
		var payloadBytes []byte
		if err := rows.Scan(&occurredAt, &payloadBytes); err != nil {
			continue
		}

		var payload map[string]interface{}
		if len(payloadBytes) > 0 {
			_ = json.Unmarshal(payloadBytes, &payload)

			// First (newest) event time becomes the overview's lastMetricTime.
			if lastMetricTime == nil {
				lastMetricTime = &occurredAt
			}

			// Support both shapes:
			// - schema_version=2 domain event: payload.metadata.{rows_processed,bytes_processed,...}
			// - schema_version=1 progress event: payload.metadata.{rows_processed,bytes_processed,metrics{...}}
			// - legacy: payload.{rows_processed,bytes_processed,...}
			meta, _ := payload["metadata"].(map[string]interface{})

			// rows/bytes (prefer explicit counters)
			if r, ok := meta["rows_processed"].(float64); ok {
				if int64(r) > totalRows {
					totalRows = int64(r)
				}
			} else if r, ok := payload["rows_processed"].(float64); ok {
				if int64(r) > totalRows {
					totalRows = int64(r)
				}
			} else if metrics, ok := meta["metrics"].(map[string]interface{}); ok {
				// Some emitters report row counts as records_read/records_written
				if r, ok := metrics["records_read"].(float64); ok && int64(r) > totalRows {
					totalRows = int64(r)
				} else if r, ok := metrics["records_written"].(float64); ok && int64(r) > totalRows {
					totalRows = int64(r)
				}
			}

			if b, ok := meta["bytes_processed"].(float64); ok {
				if int64(b) > totalBytes {
					totalBytes = int64(b)
				}
			} else if b, ok := payload["bytes_processed"].(float64); ok {
				if int64(b) > totalBytes {
					totalBytes = int64(b)
				}
			} else if metrics, ok := meta["metrics"].(map[string]interface{}); ok {
				if b, ok := metrics["bytes_read"].(float64); ok && int64(b) > totalBytes {
					totalBytes = int64(b)
				} else if b, ok := metrics["bytes_written"].(float64); ok && int64(b) > totalBytes {
					totalBytes = int64(b)
				}
			}

			// errors (best-effort)
			if e, ok := meta["error_count"].(float64); ok {
				errorCount += int(e)
			} else if metrics, ok := meta["metrics"].(map[string]interface{}); ok {
				if e, ok := metrics["errors"].(float64); ok {
					errorCount += int(e)
				}
			} else if e, ok := payload["error_count"].(float64); ok {
				errorCount += int(e)
			}

			// throughput samples (optional)
			if t, ok := meta["throughput"].(float64); ok {
				throughputSamples = append(throughputSamples, t)
			} else if t, ok := payload["throughput"].(float64); ok {
				throughputSamples = append(throughputSamples, t)
			}

			// CDC lag (optional)
			if lag, ok := meta["cdc_lag_ms"].(float64); ok {
				lagVal := int64(lag)
				cdcLagMs = &lagVal
			} else if lag, ok := payload["cdc_lag_ms"].(float64); ok {
				lagVal := int64(lag)
				cdcLagMs = &lagVal
			}
		}
	}

	avgThroughput := 0.0
	if len(throughputSamples) > 0 {
		sum := 0.0
		for _, t := range throughputSamples {
			sum += t
		}
		avgThroughput = sum / float64(len(throughputSamples))
	}

	return &DataPlaneSummary{
		TotalRowsProcessed:  totalRows,
		TotalBytesProcessed: totalBytes,
		AvgThroughput:       avgThroughput,
		CDCLagMs:            cdcLagMs,
		ErrorCount:          errorCount,
		LastMetricTime:      lastMetricTime,
	}, nil
}

func fetchInfrastructureSummary(database *sql.DB, timeRange TimeRange) (*InfrastructureSummary, error) {
	// Count components by status
	var total, healthy, degraded, unhealthy int
	err := database.QueryRow(`
		SELECT 
			COUNT(*) as total,
			COUNT(*) FILTER (WHERE status = 'healthy') as healthy,
			COUNT(*) FILTER (WHERE status = 'degraded') as degraded,
			COUNT(*) FILTER (WHERE status IN ('unhealthy', 'dead')) as unhealthy
		FROM sentinel_component_health
	`).Scan(&total, &healthy, &degraded, &unhealthy)
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}

	// Count active issues
	var activeIssues, criticalIssues int
	err = database.QueryRow(`
		SELECT 
			COUNT(*) as active,
			COUNT(*) FILTER (WHERE severity = 'critical') as critical
		FROM sentinel_active_issues
		WHERE resolved_at IS NULL
	`).Scan(&activeIssues, &criticalIssues)
	if err != nil && err != sql.ErrNoRows {
		// Non-fatal, Sentinel might not be running
		activeIssues = 0
		criticalIssues = 0
	}

	// Fetch recent issues
	recentIssues := make([]SentinelIssue, 0)
	rows, err := database.Query(`
		SELECT 
			id, type, severity, component_id, component_type,
			description, detected_at, resolved_at, occurrence_count,
			last_occurrence, metadata
		FROM sentinel_active_issues
		WHERE detected_at >= $1
		ORDER BY detected_at DESC
		LIMIT 10
	`, timeRange.Since)

	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var issue SentinelIssue
			var resolvedAt sql.NullTime
			var metadataBytes []byte

			if err := rows.Scan(
				&issue.ID, &issue.Type, &issue.Severity, &issue.ComponentID, &issue.ComponentType,
				&issue.Description, &issue.DetectedAt, &resolvedAt, &issue.OccurrenceCount,
				&issue.LastOccurrence, &metadataBytes,
			); err == nil {
				if resolvedAt.Valid {
					issue.ResolvedAt = &resolvedAt.Time
				}
				if len(metadataBytes) > 0 {
					json.Unmarshal(metadataBytes, &issue.Metadata)
				}
				recentIssues = append(recentIssues, issue)
			}
		}
	}

	return &InfrastructureSummary{
		ComponentsMonitored: total,
		HealthyComponents:   healthy,
		DegradedComponents:  degraded,
		UnhealthyComponents: unhealthy,
		ActiveIssues:        activeIssues,
		CriticalIssues:      criticalIssues,
		RecentIssues:        recentIssues,
	}, nil
}

func computeCorrelations(decisions []AgentDecision, issues []SentinelIssue) []Correlation {
	correlations := make([]Correlation, 0)

	for _, decision := range decisions {
		for _, issue := range issues {
			// Strategy 1: Exact trace_id match (confidence 1.0)
			if decision.TraceID != "" && issue.Metadata != nil {
				if issueTraceID, ok := issue.Metadata["trace_id"].(string); ok && issueTraceID == decision.TraceID {
					correlations = append(correlations, Correlation{
						AgentDecision: decision,
						InfraEvent:    &issue,
						Confidence:    1.0,
						Method:        "trace_id_match",
					})
					continue
				}
			}

			// Strategy 2: Time window + semantic matching
			timeDiff := math.Abs(float64(decision.Timestamp.Sub(issue.DetectedAt)))
			if timeDiff <= float64(60*time.Second) { // ±60s window
				confidence := 0.7 + (0.2 * (1.0 - timeDiff/float64(60*time.Second)))

				// Boost confidence if decision rationale mentions component
				if decision.Rationale != "" && strings.Contains(
					strings.ToLower(decision.Rationale),
					strings.ToLower(issue.ComponentType),
				) {
					confidence += 0.1
				}

				if confidence >= 0.7 {
					correlations = append(correlations, Correlation{
						AgentDecision: decision,
						InfraEvent:    &issue,
						Confidence:    confidence,
						Method:        "time_window_semantic",
					})
				}
			}
		}
	}

	return correlations
}

func generateMonitoringLinks(features *config.FeatureFlags, pipelineID, traceID string) MonitoringLinks {
	links := MonitoringLinks{}

	if features == nil || !features.SigNozDeepLinks {
		return links
	}

	if traceID != "" {
		links.SigNozTrace = features.GetSigNozTraceURL(traceID)
	}

	links.SigNozMetrics = features.GetSigNozMetricsURL("/dashboard")
	links.SigNozDashboard = features.GetSigNozMetricsURL("/")

	return links
}
