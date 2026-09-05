package handlers

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"api-gateway/internal/audit"
	"api-gateway/internal/db"
	"api-gateway/internal/security"

	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
)

type PipelineRunEvent struct {
	PipelineID  string         `json:"pipeline_id"`
	ExecutionID *string        `json:"execution_id,omitempty"`
	EventID     string         `json:"event_id"`
	Seq         *int64         `json:"seq,omitempty"`
	EventType   string         `json:"event_type"`
	StageID     *string        `json:"stage_id,omitempty"`
	StageGroup  *string        `json:"stage_group,omitempty"`
	Severity    *string        `json:"severity,omitempty"`
	TraceID     *string        `json:"trace_id,omitempty"`
	OccurredAt  *time.Time     `json:"occurred_at,omitempty"`
	ReceivedAt  time.Time      `json:"received_at"`
	Payload     map[string]any `json:"payload"`
}

// pipelineEventsCursor is a position in the run-event stream: the sort key of
// the last row already delivered. Zero value = start of stream.
type pipelineEventsCursor struct {
	TS      *time.Time
	Seq     int64
	EventID string
	// LegacySeq carries a `before_seq=` query param from a client that predates
	// the time-ordered cursor. Honoured only when TS is nil.
	LegacySeq *int64
}

func (c pipelineEventsCursor) isSet() bool { return c.TS != nil }

// cursorFromRow derives the cursor that points just past the given row. It must
// mirror the ORDER BY in buildPipelineEventsQuery exactly, or pagination will
// skip or repeat rows.
func cursorFromRow(eventID string, seq sql.NullInt64, occurredAt sql.NullTime, receivedAt time.Time) pipelineEventsCursor {
	ts := receivedAt
	if occurredAt.Valid {
		ts = occurredAt.Time
	}
	return pipelineEventsCursor{TS: &ts, Seq: seq.Int64, EventID: eventID}
}

// buildPipelineEventsQuery renders the run-event page query and its arguments.
//
// Split out of GetPipelineEvents so the SQL can be exercised against a real
// PostgreSQL planner (pipeline_events_pg_test.go). Row ORDER is the whole
// contract of this endpoint and sqlmock cannot check it: it matches statements
// as strings and never executes them.
func buildPipelineEventsQuery(
	pipelineID, execID string, eventTypes []string, cur pipelineEventsCursor, limit int,
) (string, []any) {
	args := []any{pipelineID}
	argIdx := 2

	query := `
		SELECT
			e.pipeline_id::text,
			CASE WHEN e.execution_id IS NULL THEN NULL ELSE e.execution_id::text END as execution_id,
			e.event_id,
			e.seq,
			e.event_type,
			e.stage_id,
			e.stage_group,
			e.severity,
			e.trace_id,
			e.occurred_at,
			e.received_at,
			e.payload
		FROM pipeline_run_events e
		WHERE e.pipeline_id = $1
	`

	if execID != "" {
		query += " AND e.execution_id = $" + strconv.Itoa(argIdx) + "::uuid"
		args = append(args, execID)
		argIdx++
	}

	if len(eventTypes) > 0 {
		placeholders := make([]string, 0, len(eventTypes))
		for _, t := range eventTypes {
			placeholders = append(placeholders, "$"+strconv.Itoa(argIdx))
			args = append(args, t)
			argIdx++
		}
		query += " AND e.event_type IN (" + strings.Join(placeholders, ",") + ")"
	}

	switch {
	case cur.isSet():
		// Keyset pagination on the exact ORDER BY tuple below. Row-wise `<` is
		// the only form that cannot skip or repeat a row, and every component
		// must be NOT NULL for it to evaluate — a NULL anywhere in a row-wise
		// comparison yields NULL, i.e. "not less than", i.e. the row is served
		// again on the next page. That is precisely how the old seq-only
		// cursor looped forever on NULL-seq rows.
		query += " AND (COALESCE(e.occurred_at, e.received_at), COALESCE(e.seq, 0), e.event_id) < ($" +
			strconv.Itoa(argIdx) + "::timestamptz, $" + strconv.Itoa(argIdx+1) + "::bigint, $" +
			strconv.Itoa(argIdx+2) + ")"
		args = append(args, *cur.TS, cur.Seq, cur.EventID)
		argIdx += 3
	case cur.LegacySeq != nil:
		// DEPRECATED. A `before_seq=` from a browser still running a bundle
		// that predates the time cursor. Kept verbatim so those clients page
		// exactly as they did before this change rather than re-reading page 1.
		query += " AND (e.seq IS NULL OR e.seq < $" + strconv.Itoa(argIdx) + ")"
		args = append(args, *cur.LegacySeq)
		argIdx++
	}

	// Order by event TIME, not by seq.
	//
	// seq is not a stream position: the projector assigns one only when the
	// event carries an execution_id (event_projector.go:598), and the healer
	// writes its rows with raw SQL that has no seq column at all
	// (heal/worker.go:901, :942 · heal/executors.go:325). On prod that left
	// 343 of 1614 events (21%) with seq NULL — every SENTINEL_ALERT, every
	// DATA_PLANE_METRICS, every healer decision and verdict. Sorting those
	// last is not a cosmetic ordering choice: this endpoint serves a fixed
	// window (60 rows in MonitorTab), so "last" means "not in the response".
	// The alarms and the healer's own conclusions were the rows being dropped.
	//
	// COALESCE handles the healer shape, which supplies received_at only.
	// The seq and event_id components are tiebreakers that make the sort a
	// total order, which is what the keyset cursor above needs to be exact.
	query += ` ORDER BY COALESCE(e.occurred_at, e.received_at) DESC,
	                   COALESCE(e.seq, 0) DESC,
	                   e.event_id DESC LIMIT ` + strconv.Itoa(limit)

	return query, args
}

// GetPipelineEvents returns replayable, redacted run events for a pipeline.
// GET /api/v1/pipelines/:id/events?execution_id=...&limit=...&before_seq=...&event_types=a,b,c
func GetPipelineEvents(c *gin.Context) {
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}

	pipelineID, ok := requireUUIDParam(c, "id", "invalid_pipeline_id", "Invalid pipeline ID format")
	if !ok {
		return
	}
	// Gate on the ACTIVE workspace. The previous `p.created_by = $n` predicate was
	// workspace-blind: the creator kept full read access to this pipeline's event
	// stream — execution ids, trace ids, stage ids, data-plane metrics — from a
	// DIFFERENT active workspace, and a teammate in the pipeline's own workspace
	// got an empty list instead of the run they share.
	if _, ok := requirePipelineWorkspaceRole(c, pipelineID, security.WSViewer); !ok {
		return
	}

	execID := strings.TrimSpace(c.Query("execution_id"))
	eventTypesRaw := strings.TrimSpace(c.Query("event_types"))
	beforeSeqRaw := strings.TrimSpace(c.Query("before_seq"))

	limit := 100
	if s := strings.TrimSpace(c.Query("limit")); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v > 0 && v <= 500 {
			limit = v
		}
	}

	// Cursor. `before_ts` + `before_seq` + `before_event_id` is the keyset
	// triple echoed back from a previous page's next_cursor. `before_seq` on
	// its own is the deprecated pre-time-cursor form.
	var cursor pipelineEventsCursor
	if v, err := strconv.ParseInt(beforeSeqRaw, 10, 64); err == nil && beforeSeqRaw != "" {
		cursor.Seq = v
		cursor.LegacySeq = &v
	}
	if s := strings.TrimSpace(c.Query("before_ts")); s != "" {
		if ts, err := time.Parse(time.RFC3339Nano, s); err == nil {
			cursor.TS = &ts
			cursor.EventID = strings.TrimSpace(c.Query("before_event_id"))
			cursor.LegacySeq = nil
		}
	}

	// Parse event_types filter
	var eventTypes []string
	if eventTypesRaw != "" {
		for _, t := range strings.Split(eventTypesRaw, ",") {
			tt := strings.TrimSpace(t)
			if tt != "" {
				eventTypes = append(eventTypes, tt)
			}
		}
	}

	// Tenancy is already proven by the gate above, so the query filters on the
	// pipeline id alone — no join back to pipelines for a creator predicate.
	query, args := buildPipelineEventsQuery(pipelineID, execID, eventTypes, cursor, limit)

	rows, err := database.Query(query, args...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch pipeline events"})
		return
	}
	defer rows.Close()

	events := make([]PipelineRunEvent, 0, limit)
	var nextCursor pipelineEventsCursor
	for rows.Next() {
		var (
			pid          string
			executionID  sql.NullString
			eventID      string
			seq          sql.NullInt64
			eventType    string
			stageID      sql.NullString
			stageGroup   sql.NullString
			severity     sql.NullString
			traceID      sql.NullString
			occurredAt   sql.NullTime
			receivedAt   time.Time
			payloadBytes []byte
		)

		if err := rows.Scan(
			&pid,
			&executionID,
			&eventID,
			&seq,
			&eventType,
			&stageID,
			&stageGroup,
			&severity,
			&traceID,
			&occurredAt,
			&receivedAt,
			&payloadBytes,
		); err != nil {
			log.WithError(err).WithField("event_type", eventType).Warn("Failed to scan event row")
			continue
		}

		// Unmarshal JSONB payload
		var payload map[string]any
		if len(payloadBytes) > 0 {
			if err := json.Unmarshal(payloadBytes, &payload); err != nil {
				log.WithError(err).WithField("event_id", eventID).Error("GetPipelineEvents: Failed to unmarshal payload")
				payload = make(map[string]any)
			}
		} else {
			payload = make(map[string]any)
		}

		var execPtr *string
		if executionID.Valid {
			v := executionID.String
			execPtr = &v
		}
		var seqPtr *int64
		if seq.Valid {
			v := seq.Int64
			seqPtr = &v
		}
		var stagePtr *string
		if stageID.Valid {
			v := stageID.String
			stagePtr = &v
		}
		var groupPtr *string
		if stageGroup.Valid {
			v := stageGroup.String
			groupPtr = &v
		}
		var sevPtr *string
		if severity.Valid {
			v := severity.String
			sevPtr = &v
		}
		var tracePtr *string
		if traceID.Valid {
			v := traceID.String
			tracePtr = &v
		}
		var occurredPtr *time.Time
		if occurredAt.Valid {
			v := occurredAt.Time
			occurredPtr = &v
		}

		events = append(events, PipelineRunEvent{
			PipelineID:  pid,
			ExecutionID: execPtr,
			EventID:     eventID,
			Seq:         seqPtr,
			EventType:   eventType,
			StageID:     stagePtr,
			StageGroup:  groupPtr,
			Severity:    sevPtr,
			TraceID:     tracePtr,
			OccurredAt:  occurredPtr,
			ReceivedAt:  receivedAt,
			Payload:     payload,
		})
		nextCursor = cursorFromRow(eventID, seq, occurredAt, receivedAt)
	}

	// The cursor is derived server-side and echoed back verbatim, so a client
	// never has to know the sort key. Only meaningful on a full page: a short
	// page is the end of the stream.
	var nextCursorJSON any
	if len(events) == limit && nextCursor.TS != nil {
		nextCursorJSON = gin.H{
			"before_ts":       nextCursor.TS.Format(time.RFC3339Nano),
			"before_seq":      nextCursor.Seq,
			"before_event_id": nextCursor.EventID,
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"pipeline_id":              pipelineID,
		"execution_id":             execID,
		"events":                   events,
		"limit":                    limit,
		"has_more":                 len(events) == limit,
		"next_cursor":              nextCursorJSON,
		"redaction_policy_version": security.RedactionPolicyVersion,
	})
}

// GetPipelineEventsRaw returns less-redacted events for power users/admins
// Requires: power_user or admin role
// POST /api/v1/pipelines/:id/events/raw
// Body: { "justification": "debugging prod issue #123" }
func GetPipelineEventsRaw(c *gin.Context) {
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}

	pipelineID, ok := requireUUIDParam(c, "id", "invalid_pipeline_id", "Invalid pipeline ID format")
	if !ok {
		return
	}
	userID, ok := resolveUserID(c)
	if !ok {
		return
	}

	// Tenancy before RBAC: a pipeline outside the active workspace must 404 no
	// matter what role the caller holds, so power_user/admin can't read raw
	// payloads out of a workspace they are not currently acting in.
	if _, ok := requirePipelineWorkspaceRole(c, pipelineID, security.WSViewer); !ok {
		return
	}

	userRole := security.GetUserRole(c)

	// Enforce RBAC
	if !security.CanViewRawEvents(userRole) {
		c.JSON(http.StatusForbidden, gin.H{
			"error":   "insufficient_permissions",
			"message": "Raw events require power_user or admin role",
		})
		return
	}

	// Require justification
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

	// Audit log
	audit.LogRawEventsAccess(
		userID,
		pipelineID,
		req.Justification,
		c.ClientIP(),
		c.GetHeader("User-Agent"),
	)

	// Fetch events (same query as GetPipelineEvents, but return raw payloads)
	execID := strings.TrimSpace(c.Query("execution_id"))
	limit := 50
	if s := strings.TrimSpace(c.Query("limit")); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v > 0 && v <= 200 {
			limit = v
		}
	}

	args := []any{pipelineID}
	argIdx := 2

	query := `
		SELECT
			e.event_id,
			e.event_type,
			e.payload
		FROM pipeline_run_events e
		WHERE e.pipeline_id = $1
	`

	if execID != "" {
		query += " AND e.execution_id = $" + strconv.Itoa(argIdx) + "::uuid"
		args = append(args, execID)
		argIdx++
	}

	// Same time ordering as the redacted endpoint above. This is the
	// justification-gated raw-payload view — the one an operator opens when the
	// redacted stream did not explain the incident — so it must not be the
	// view that hides the alert.
	query += ` ORDER BY COALESCE(e.occurred_at, e.received_at) DESC,
	                   COALESCE(e.seq, 0) DESC,
	                   e.event_id DESC LIMIT ` + strconv.Itoa(limit)

	rows, err := database.Query(query, args...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch raw events"})
		return
	}
	defer rows.Close()

	rawEvents := make([]map[string]any, 0, limit)
	for rows.Next() {
		var (
			eventID      string
			eventType    string
			payloadBytes []byte
		)

		if err := rows.Scan(&eventID, &eventType, &payloadBytes); err != nil {
			log.WithError(err).Warn("Failed to scan raw event row")
			continue
		}

		var payload map[string]any
		if len(payloadBytes) > 0 {
			if err := json.Unmarshal(payloadBytes, &payload); err != nil {
				log.WithError(err).WithField("event_id", eventID).Error("Failed to unmarshal raw payload")
				payload = make(map[string]any)
			}
		} else {
			payload = make(map[string]any)
		}

		rawEvents = append(rawEvents, map[string]any{
			"event_id":   eventID,
			"event_type": eventType,
			"payload":    payload, // Raw, less-redacted payload
		})
	}

	c.JSON(http.StatusOK, gin.H{
		"pipeline_id":   pipelineID,
		"execution_id":  execID,
		"raw_events":    rawEvents,
		"limit":         limit,
		"justification": req.Justification,
		"accessed_by":   userID,
		"accessed_at":   time.Now().UTC().Format(time.RFC3339),
		"warning":       "This endpoint returns less-redacted data. Handle with care.",
	})
}
