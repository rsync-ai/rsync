package handlers

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"api-gateway/internal/db"
	"api-gateway/internal/security"

	"github.com/gin-gonic/gin"
)

// PipelineAlert is one open (or recently resolved) Sentinel finding about ONE
// pipeline: source-side WAL/binlog lag (cdc-lag-<id>), sink-side Kafka drain lag
// (cdc-sink-lag-<id>), or a connector that is down (cdc-connector-down-<id>).
//
// Deliberately a narrower shape than SentinelIssue: no component_type, because
// every row here is about the pipeline in the URL, and nothing on this route can
// return an infrastructure component.
type PipelineAlert struct {
	ID              string                 `json:"id"`
	Type            string                 `json:"type"`
	Severity        string                 `json:"severity"`
	Description     string                 `json:"description"`
	DetectedAt      time.Time              `json:"detected_at"`
	ResolvedAt      *time.Time             `json:"resolved_at,omitempty"`
	OccurrenceCount int                    `json:"occurrence_count"`
	LastOccurrence  time.Time              `json:"last_occurrence"`
	Metadata        map[string]interface{} `json:"metadata,omitempty"`
}

const pipelineAlertsDefaultLimit = 20
const pipelineAlertsMaxLimit = 100

// GetPipelineAlerts lists the Sentinel's findings for one pipeline.
//
// GET /api/v1/pipelines/:id/alerts?resolved=false&limit=20
//
// WHY THIS EXISTS RATHER THAN REUSING /monitoring/sentinel/issues
//
// The pipeline page already had a lag panel, and it read
// GET /api/v1/monitoring/sentinel/issues. That route is shaped for the ADMIN
// infrastructure view and carries two gates this one must not:
//
//  1. FEATURE_MONITORING_INFRA, default false (config/features.go). With the flag
//     unset the route 404s, and CDCLagAlertsPanel treats 404 as "not enabled here"
//     and hides itself. So on a default deployment the Sentinel would detect a
//     stalled sink, write the issue, and nobody looking at the pipeline would ever
//     be told.
//  2. A PLATFORM role of power_user or admin (monitoring.go GetSentinelIssues).
//     An ordinary workspace member — the person who owns the pipeline and is on
//     call for it — got 403 and, again, a silently hidden panel.
//
// Neither gate is wrong for what that route is: sentinel_component_health and the
// infrastructure issue classes name Kafka topics and containers across every
// workspace, which is why admin/health is admin-only. The split this route
// establishes is the product line: INFRASTRUCTURE monitoring is admin, and a
// PIPELINE's own health — its source, its destination, its consumers, whether data
// is moving — belongs to the people who own that pipeline.
//
// So: no feature flag, and the ordinary pipeline IDOR gate (Viewer, the same floor
// every other pipeline GET uses) instead of a platform role. The tenant scope is
// requirePipelineWorkspaceRole rather than the sentinelIssueTenantPredicate
// subquery, because the pipeline id is in the URL — there is nothing to filter, only
// one resource to authorize, and that helper is the repo's single IDOR chokepoint.
//
// Rows are matched on component_id = <pipeline id>, which is how emitCDCIssue keys
// every pipeline-scoped issue. component_type is additionally constrained to the two
// pipeline types so that a future component whose id happened to collide with a
// pipeline uuid could not leak onto this route.
func GetPipelineAlerts(c *gin.Context) {
	pipelineID, ok := requireUUIDParam(c, "id", "invalid_pipeline_id", "Invalid pipeline ID format")
	if !ok {
		return
	}
	if _, ok := requirePipelineWorkspaceRole(c, pipelineID, security.WSViewer); !ok {
		return
	}

	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}

	limit := pipelineAlertsDefaultLimit
	if s := c.Query("limit"); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v > 0 && v <= pipelineAlertsMaxLimit {
			limit = v
		}
	}

	// resolved: "false" (default view — what is wrong now), "true", or absent for both.
	// The default is deliberately ALL rather than open-only: a caller that wants the
	// open ones asks for them, and a silent default filter is how a history view ends
	// up quietly empty.
	query := `
		SELECT id, type, severity, description, detected_at, resolved_at,
		       occurrence_count, last_occurrence, metadata
		FROM sentinel_active_issues
		WHERE component_id = $1
		  AND component_type IN ('cdc_pipeline', 'batch_pipeline')
	`
	args := []interface{}{pipelineID}
	switch c.Query("resolved") {
	case "false":
		query += " AND resolved_at IS NULL"
	case "true":
		query += " AND resolved_at IS NOT NULL"
	}
	query += " ORDER BY detected_at DESC LIMIT $2"
	args = append(args, limit)

	rows, err := database.Query(query, args...)
	if err != nil {
		respondError(c, http.StatusInternalServerError, "pipeline_alerts_query_failed",
			"Failed to read this pipeline's alerts", err)
		return
	}
	defer rows.Close()

	alerts := make([]PipelineAlert, 0, limit)
	for rows.Next() {
		var a PipelineAlert
		var resolvedAt sql.NullTime
		var metadataBytes []byte
		if err := rows.Scan(&a.ID, &a.Type, &a.Severity, &a.Description, &a.DetectedAt,
			&resolvedAt, &a.OccurrenceCount, &a.LastOccurrence, &metadataBytes); err != nil {
			// A row we cannot read is a fault, not an absence: answering 200 with a
			// short list would tell the operator everything is fine. This is the same
			// reasoning GetSentinelHealth applies to its scan errors.
			respondError(c, http.StatusInternalServerError, "pipeline_alerts_scan_failed",
				"Failed to read this pipeline's alerts", err)
			return
		}
		if resolvedAt.Valid {
			a.ResolvedAt = &resolvedAt.Time
		}
		if len(metadataBytes) > 0 {
			// Best-effort: a malformed metadata blob must not hide the alert itself,
			// which carries its meaning in `description` and `severity`.
			_ = json.Unmarshal(metadataBytes, &a.Metadata)
		}
		alerts = append(alerts, a)
	}
	if err := rows.Err(); err != nil {
		respondError(c, http.StatusInternalServerError, "pipeline_alerts_query_failed",
			"Failed to read this pipeline's alerts", err)
		return
	}

	// A pipeline with no alerts answers an empty list, not 404 — "nothing is wrong"
	// is a valid, useful answer and the UI renders it as the all-clear.
	c.JSON(http.StatusOK, gin.H{"alerts": alerts})
}
