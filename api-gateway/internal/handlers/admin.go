package handlers

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"api-gateway/internal/db"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// =============================================================================
// Admin APIs (role-gated via AdminRoleMiddleware)
// =============================================================================

type adminPipelineRow struct {
	ID             string    `json:"id"`
	Name           string    `json:"name"`
	Status         string    `json:"status"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
	CreatedBy      string    `json:"created_by"`
	CreatedByEmail string    `json:"created_by_email"`
}

type adminExecutionRow struct {
	ID             string     `json:"id"`
	PipelineID     string     `json:"pipeline_id"`
	PipelineName   string     `json:"pipeline_name"`
	CreatedByEmail string     `json:"created_by_email"`
	Status         string     `json:"status"`
	StartTime      time.Time  `json:"start_time"`
	EndTime        *time.Time `json:"end_time,omitempty"`
	ErrorMessage   *string    `json:"error_message,omitempty"`
}

// AdminOverview returns high-level counts + recent failures.
// GET /api/v1/admin/overview
func AdminOverview(c *gin.Context) {
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}

	var usersCount, pipelinesCount, executionsCount, connectionsCount int64
	if err := database.QueryRow(`
		SELECT
			(SELECT COUNT(*) FROM users)      AS users,
			(SELECT COUNT(*) FROM pipelines)  AS pipelines,
			-- id <> pipeline_id drops the synthetic CDC audit rows (permanent FK
			-- anchors written by the streaming audit persisters, one per CDC
			-- pipeline, never a run). See GetPipelineStats for the full note.
			(SELECT COUNT(*) FROM executions WHERE id <> pipeline_id) AS executions,
			(SELECT COUNT(*) FROM connections) AS connections
	`).Scan(&usersCount, &pipelinesCount, &executionsCount, &connectionsCount); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to load counts"})
		return
	}

	// Recent failures (last 24h)
	rows, err := database.Query(`
		SELECT
			e.id,
			e.pipeline_id,
			COALESCE(p.name, '') AS pipeline_name,
			COALESCE(u.email, '') AS created_by_email,
			e.status,
			e.start_time,
			e.end_time,
			e.error_message
		FROM executions e
		LEFT JOIN pipelines p ON p.id = e.pipeline_id
		LEFT JOIN users u ON u.id = p.created_by
		WHERE e.status IN ('failed', 'error')
		  AND e.start_time > NOW() - INTERVAL '24 hours'
		ORDER BY e.start_time DESC
		LIMIT 20
	`)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to load recent failures"})
		return
	}
	defer rows.Close()

	failures := make([]adminExecutionRow, 0, 20)
	for rows.Next() {
		var r adminExecutionRow
		if err := rows.Scan(
			&r.ID,
			&r.PipelineID,
			&r.PipelineName,
			&r.CreatedByEmail,
			&r.Status,
			&r.StartTime,
			&r.EndTime,
			&r.ErrorMessage,
		); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to parse recent failures"})
			return
		}
		failures = append(failures, r)
	}
	if err := rows.Err(); err != nil {
		respondError(c, http.StatusInternalServerError, "recent_failures_iter_failed", "Failed to load recent failures", err)
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"counts": gin.H{
			"users":       usersCount,
			"pipelines":   pipelinesCount,
			"executions":  executionsCount,
			"connections": connectionsCount,
		},
		"recent_failed_executions": failures,
	})
}

// AdminListPipelines lists pipelines across all users.
// GET /api/v1/admin/pipelines?limit=50&offset=0&q=
func AdminListPipelines(c *gin.Context) {
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}

	limit := 50
	if s := strings.TrimSpace(c.Query("limit")); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v > 0 && v <= 200 {
			limit = v
		}
	}
	offset := 0
	if s := strings.TrimSpace(c.Query("offset")); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v >= 0 {
			offset = v
		}
	}
	q := strings.TrimSpace(c.Query("q"))

	args := []any{}
	where := "WHERE 1=1"
	if q != "" {
		args = append(args, "%"+q+"%")
		where += " AND (p.name ILIKE $1 OR p.id::text ILIKE $1 OR u.email ILIKE $1)"
	}

	var total int64
	countQuery := `
		SELECT COUNT(*)
		FROM pipelines p
		LEFT JOIN users u ON u.id = p.created_by
	` + "\n" + where
	if err := database.QueryRow(countQuery, args...).Scan(&total); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to count pipelines"})
		return
	}

	// Append pagination args
	argsList := append([]any{}, args...)
	argsList = append(argsList, limit, offset)
	limitParam := len(args) + 1
	offsetParam := len(args) + 2

	listQuery := `
		SELECT
			p.id,
			p.name,
			p.status,
			p.created_at,
			p.updated_at,
			COALESCE(p.created_by::text, '') AS created_by,
			COALESCE(u.email, '') AS created_by_email
		FROM pipelines p
		LEFT JOIN users u ON u.id = p.created_by
	` + "\n" + where + "\n" +
		"ORDER BY p.created_at DESC " +
		"LIMIT $" + strconv.Itoa(limitParam) + " OFFSET $" + strconv.Itoa(offsetParam)

	rows, err := database.Query(listQuery, argsList...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to list pipelines"})
		return
	}
	defer rows.Close()

	out := make([]adminPipelineRow, 0, limit)
	for rows.Next() {
		var r adminPipelineRow
		if err := rows.Scan(&r.ID, &r.Name, &r.Status, &r.CreatedAt, &r.UpdatedAt, &r.CreatedBy, &r.CreatedByEmail); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to parse pipelines"})
			return
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		respondError(c, http.StatusInternalServerError, "list_pipelines_iter_failed", "Failed to list pipelines", err)
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"data":   out,
		"total":  total,
		"limit":  limit,
		"offset": offset,
	})
}

// AdminListExecutions lists executions across all users.
// GET /api/v1/admin/executions?limit=50&offset=0&status=&q=
func AdminListExecutions(c *gin.Context) {
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}

	limit := 50
	if s := strings.TrimSpace(c.Query("limit")); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v > 0 && v <= 200 {
			limit = v
		}
	}
	offset := 0
	if s := strings.TrimSpace(c.Query("offset")); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v >= 0 {
			offset = v
		}
	}
	status := strings.TrimSpace(c.Query("status"))
	q := strings.TrimSpace(c.Query("q"))

	args := []any{}
	// e.id <> e.pipeline_id drops the synthetic CDC audit rows — see
	// GetPipelineStats. Both the COUNT and the LIST below build off this string,
	// so they stay consistent with each other.
	where := "WHERE e.id <> e.pipeline_id"
	argIdx := 1

	if status != "" {
		where += " AND e.status = $" + strconv.Itoa(argIdx)
		args = append(args, status)
		argIdx++
	}
	if q != "" {
		where += " AND (e.id::text ILIKE $" + strconv.Itoa(argIdx) + " OR e.pipeline_id::text ILIKE $" + strconv.Itoa(argIdx) + " OR COALESCE(p.name,'') ILIKE $" + strconv.Itoa(argIdx) + " OR COALESCE(u.email,'') ILIKE $" + strconv.Itoa(argIdx) + ")"
		args = append(args, "%"+q+"%")
		argIdx++
	}

	var total int64
	countQuery := `
		SELECT COUNT(*)
		FROM executions e
		LEFT JOIN pipelines p ON p.id = e.pipeline_id
		LEFT JOIN users u ON u.id = p.created_by
	` + "\n" + where
	if err := database.QueryRow(countQuery, args...).Scan(&total); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to count executions"})
		return
	}

	argsList := append([]any{}, args...)
	argsList = append(argsList, limit, offset)
	limitParam := len(args) + 1
	offsetParam := len(args) + 2

	listQuery := `
		SELECT
			e.id,
			COALESCE(e.pipeline_id::text, '') AS pipeline_id,
			COALESCE(p.name, '') AS pipeline_name,
			COALESCE(u.email, '') AS created_by_email,
			e.status,
			e.start_time,
			e.end_time,
			e.error_message
		FROM executions e
		LEFT JOIN pipelines p ON p.id = e.pipeline_id
		LEFT JOIN users u ON u.id = p.created_by
	` + "\n" + where + "\n" +
		"ORDER BY e.start_time DESC " +
		"LIMIT $" + strconv.Itoa(limitParam) + " OFFSET $" + strconv.Itoa(offsetParam)

	rows, err := database.Query(listQuery, argsList...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to list executions"})
		return
	}
	defer rows.Close()

	out := make([]adminExecutionRow, 0, limit)
	for rows.Next() {
		var r adminExecutionRow
		if err := rows.Scan(
			&r.ID,
			&r.PipelineID,
			&r.PipelineName,
			&r.CreatedByEmail,
			&r.Status,
			&r.StartTime,
			&r.EndTime,
			&r.ErrorMessage,
		); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to parse executions"})
			return
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		respondError(c, http.StatusInternalServerError, "list_executions_iter_failed", "Failed to list executions", err)
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"data":   out,
		"total":  total,
		"limit":  limit,
		"offset": offset,
	})
}

// AdminGetPipelineEventsRaw returns raw event payloads for debugging.
// POST /api/v1/admin/pipelines/:id/events/raw?execution_id=&limit=
// Body: { "justification": "..." }
func AdminGetPipelineEventsRaw(c *gin.Context) {
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}

	pipelineIDStr := strings.TrimSpace(c.Param("id"))
	pid, err := uuid.Parse(pipelineIDStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_pipeline_id"})
		return
	}

	execIDStr := strings.TrimSpace(c.Query("execution_id"))
	var execID *uuid.UUID
	if execIDStr != "" {
		v, err := uuid.Parse(execIDStr)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_execution_id"})
			return
		}
		execID = &v
	}

	limit := 50
	if s := strings.TrimSpace(c.Query("limit")); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v > 0 && v <= 200 {
			limit = v
		}
	}

	var req struct {
		Justification string `json:"justification"`
	}
	if err := c.ShouldBindJSON(&req); err != nil || strings.TrimSpace(req.Justification) == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "justification_required",
			"message": "You must provide a justification for accessing raw events",
		})
		return
	}
	req.Justification = strings.TrimSpace(req.Justification)

	adminEmail := c.GetString("admin_user_email")

	// Fetch events (unscoped across users; admin-only)
	args := []any{pid}
	query := `
		SELECT
			e.event_id,
			e.event_type,
			e.execution_id,
			e.seq,
			e.received_at,
			e.payload
		FROM pipeline_run_events e
		WHERE e.pipeline_id = $1
	`
	if execID != nil {
		query += " AND e.execution_id = $2"
		args = append(args, *execID)
	}
	// Order by event TIME. seq is NULL on every pipeline-scoped event (the
	// projector assigns one only when the event has an execution_id) and on
	// every healer-written row, so `seq DESC NULLS LAST` sorted 21% of prod's
	// events below the window this LIMIT returns — including every
	// SENTINEL_ALERT. Kept identical to the user-facing endpoint's ordering in
	// pipeline_events.go, because an admin reading raw events to explain an
	// incident must not be shown a *different* stream than the customer.
	query += ` ORDER BY COALESCE(e.occurred_at, e.received_at) DESC,
	                   COALESCE(e.seq, 0) DESC,
	                   e.event_id DESC LIMIT ` + strconv.Itoa(limit)

	rows, err := database.Query(query, args...)
	success := err == nil
	var errMsg string
	if err != nil {
		errMsg = err.Error()
	}

	// Always audit the access attempt (success/failure)
	logAudit(c, "view_raw_events", "pipeline", pid.String(), map[string]interface{}{
		"admin_email":   adminEmail,
		"pipeline_id":   pid.String(),
		"execution_id":  execIDStr,
		"justification": req.Justification,
		"limit":         limit,
		"ip_address":    c.ClientIP(),
		"user_agent":    c.GetHeader("User-Agent"),
		"success":       success,
		"error_message": errMsg,
	})

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch raw events"})
		return
	}
	defer rows.Close()

	type rawEvent struct {
		EventID     string          `json:"event_id"`
		EventType   string          `json:"event_type"`
		ExecutionID *string         `json:"execution_id,omitempty"`
		Seq         *int64          `json:"seq,omitempty"`
		ReceivedAt  time.Time       `json:"received_at"`
		Payload     json.RawMessage `json:"payload"`
	}

	out := make([]rawEvent, 0, limit)
	for rows.Next() {
		var (
			eventID     string
			eventType   string
			execUUID    *uuid.UUID
			seq         *int64
			receivedAt  time.Time
			payloadJSON []byte
		)
		if err := rows.Scan(&eventID, &eventType, &execUUID, &seq, &receivedAt, &payloadJSON); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to parse raw events"})
			return
		}
		var execOut *string
		if execUUID != nil {
			s := execUUID.String()
			execOut = &s
		}
		// payload is JSONB; pass through as raw JSON.
		out = append(out, rawEvent{
			EventID:     eventID,
			EventType:   eventType,
			ExecutionID: execOut,
			Seq:         seq,
			ReceivedAt:  receivedAt,
			Payload:     json.RawMessage(payloadJSON),
		})
	}
	if err := rows.Err(); err != nil {
		respondError(c, http.StatusInternalServerError, "raw_events_iter_failed", "Failed to fetch raw events", err)
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"pipeline_id":  pid.String(),
		"execution_id": execIDStr,
		"events":       out,
		"limit":        limit,
	})
}
