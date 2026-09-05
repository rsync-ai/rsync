package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"time"

	"api-gateway/internal/db"
	"api-gateway/internal/security"

	"github.com/gin-gonic/gin"
	"github.com/rsync-ai/shared/naming"
	log "github.com/sirupsen/logrus"
)

type cachedSchemaPayload struct {
	Tables []struct {
		Name   string `json:"name"`
		Schema string `json:"schema,omitempty"`
	} `json:"tables"`
}

func normalizeTableKeyForLookup(raw string) string {
	s := strings.TrimSpace(raw)
	s = strings.Trim(s, "\"`'")
	// Preserve dots for qualified keys; just normalize whitespace.
	return strings.ToLower(strings.TrimSpace(s))
}

func validateAndQualifySelectedTablesFromCache(ctx context.Context, sourceConnectionID string, tables []string) (qualified []string, missing []string, ambiguous map[string][]string, ok bool) {
	if schemaCache == nil || sourceConnectionID == "" || len(tables) == 0 {
		return nil, nil, nil, false
	}
	b, err := schemaCache.Get(ctx, sourceConnectionID)
	if err != nil || b == nil {
		return nil, nil, nil, false
	}

	var payload cachedSchemaPayload
	if err := json.Unmarshal(b, &payload); err != nil {
		return nil, nil, nil, false
	}
	if len(payload.Tables) == 0 {
		return nil, nil, nil, false
	}

	exact := make(map[string]string, len(payload.Tables)) // lower -> canonical
	byUnqualified := make(map[string][]string, 256)       // table -> []qualified
	for _, t := range payload.Tables {
		name := strings.TrimSpace(t.Name)
		if name == "" {
			continue
		}
		schema := strings.TrimSpace(t.Schema)
		canonical := name
		if schema != "" {
			canonical = schema + "." + name
		}
		key := normalizeTableKeyForLookup(canonical)
		exact[key] = canonical
		byUnqualified[strings.ToLower(name)] = append(byUnqualified[strings.ToLower(name)], canonical)
	}

	out := make([]string, 0, len(tables))
	miss := make([]string, 0)
	amb := make(map[string][]string)

	for _, raw := range tables {
		in := strings.TrimSpace(raw)
		if in == "" {
			continue
		}
		lk := normalizeTableKeyForLookup(in)
		if strings.Contains(lk, ".") {
			if canon, ok := exact[lk]; ok {
				out = append(out, canon)
			} else {
				miss = append(miss, in)
			}
			continue
		}

		cands := byUnqualified[strings.ToLower(in)]
		if len(cands) == 1 {
			out = append(out, cands[0])
		} else if len(cands) == 0 {
			miss = append(miss, in)
		} else {
			// Ambiguous across schemas/databases; require qualification.
			sort.Strings(cands)
			amb[in] = cands
		}
	}

	// Deduplicate, preserve order.
	seen := make(map[string]struct{}, len(out))
	dedup := make([]string, 0, len(out))
	for _, t := range out {
		k := normalizeTableKeyForLookup(t)
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		dedup = append(dedup, t)
	}

	return dedup, miss, amb, true
}

func isWorkflowAlreadyCompletedError(err error) bool {
	if err == nil {
		return false
	}
	// Temporal SDK returns: "workflow execution already completed"
	return strings.Contains(strings.ToLower(err.Error()), "workflow execution already completed")
}

func isWorkflowNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	// Temporal can surface various not-found strings depending on server/persistence layer.
	return strings.Contains(msg, "workflow execution not found") ||
		strings.Contains(msg, "not found") ||
		strings.Contains(msg, "sql: no rows in result set")
}

// bestEffortMarkWorkflowEnded updates DB rows so the UI doesn't keep showing stale HITL state.
func bestEffortMarkWorkflowEnded(database *sql.DB, pipelineID, executionID, finalStatus, message string) {
	if database == nil || pipelineID == "" {
		return
	}

	stageState := "failed"
	switch finalStatus {
	case "completed":
		stageState = "succeeded"
	case "stopped":
		stageState = "failed"
	case "failed":
		stageState = "failed"
	}

	// Clear HITL blocking fields so UI doesn't keep showing table selection modal.
	if _, err := database.Exec(`
		UPDATE pipeline_progress
		SET status = $1,
		    stage_state = $2,
		    blocking_reason_type = NULL,
		    blocking_reason_description = NULL,
		    blocking_reason_estimated_seconds = NULL,
		    message = $3,
		    updated_at = NOW()
		WHERE pipeline_id = $4
	`, finalStatus, stageState, message, pipelineID); err != nil {
		log.WithError(err).WithField("pipeline_id", pipelineID).Warn("bestEffortMarkWorkflowEnded: pipeline_progress update failed (ignored)")
	}

	if executionID != "" {
		if _, err := database.Exec(`
			UPDATE executions
			SET status = $1,
			    end_time = NOW(),
			    error_message = $2
			WHERE id = $3
		`, finalStatus, message, executionID); err != nil {
			log.WithError(err).WithFields(log.Fields{
				"pipeline_id":  pipelineID,
				"execution_id": executionID,
				"final_status": finalStatus,
			}).Warn("bestEffortMarkWorkflowEnded: executions update failed (ignored)")
		}
	}

	// Keep pipelines.status in sync with the workflow outcome (best-effort).
	if _, err := database.Exec(`UPDATE pipelines SET status = $1, updated_at = NOW() WHERE id = $2`, finalStatus, pipelineID); err != nil {
		log.WithError(err).WithField("pipeline_id", pipelineID).Warn("bestEffortMarkWorkflowEnded: pipelines status update failed (ignored)")
	}
}

// ResumeConnectionsRequest signals Temporal that required connections are configured.
// Supports either:
// - source_connection_id + destination_connection_id (preferred)
// - direction + connection_id (one-at-a-time)
type ResumeConnectionsRequest struct {
	ExecutionID string `json:"execution_id,omitempty"`

	SourceConnectionID      string `json:"source_connection_id,omitempty"`
	DestinationConnectionID string `json:"destination_connection_id,omitempty"`

	Direction    string `json:"direction,omitempty"`     // source|destination
	ConnectionID string `json:"connection_id,omitempty"` // connection record id

	// Optional analytics/debug metadata (safe additive field)
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

type ResumeConnectorsRequest struct {
	ExecutionID string `json:"execution_id,omitempty"`
	// Optional metadata, useful for auditing/debugging
	Connectors []string               `json:"connectors,omitempty"`
	Metadata   map[string]interface{} `json:"metadata,omitempty"`
}

type ResumeTablesRequest struct {
	ExecutionID    string                 `json:"execution_id,omitempty"`
	SelectedTables []string               `json:"selected_tables"` // Table names selected by user
	Metadata       map[string]interface{} `json:"metadata,omitempty"`
	// DestinationConfig is the user's confirmed destination namespace mapping,
	// collected in the same first-run HITL step as table selection (PR-C). It is
	// pipeline-scoped and persisted before the workflow is signalled. nil = the
	// caller is not changing the mapping; the server still resolves and locks the
	// pipeline's own seeded namespace (see serverSideFirstRunNamespace) so the
	// first-run collision probe does not depend on the client sending anything.
	DestinationConfig *DestinationConfig `json:"destination_config,omitempty"`
}

// ResumeNodeInputRequest is the request to provide NL input for a DAG node
type ResumeNodeInputRequest struct {
	ExecutionID string                 `json:"execution_id,omitempty"`
	NodeID      string                 `json:"node_id"`                // ID of the node waiting for input
	Message     string                 `json:"message"`                // User's NL response
	ConfigPatch map[string]interface{} `json:"config_patch,omitempty"` // Optional: pre-interpreted config
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
}

func resolveExecutionIDForPipeline(database *sql.DB, pipelineID string) (string, error) {
	var execID sql.NullString

	// Prefer pipeline_progress (authoritative for UI)
	err := database.QueryRow(`
		SELECT execution_id
		FROM pipeline_progress
		WHERE pipeline_id = $1
	`, pipelineID).Scan(&execID)
	if err == nil && execID.Valid && execID.String != "" {
		return execID.String, nil
	}

	// Fallback: latest execution row
	err = database.QueryRow(`
		SELECT id
		FROM executions
		WHERE pipeline_id = $1
		ORDER BY start_time DESC
		LIMIT 1
	`, pipelineID).Scan(&execID)
	if err == nil && execID.Valid && execID.String != "" {
		return execID.String, nil
	}

	if err != nil {
		return "", err
	}
	return "", sql.ErrNoRows
}

// ResumePipelineConnections signals the running workflow to continue after user configures connections.
// Signal: connections_configured
func ResumePipelineConnections(c *gin.Context) {
	pipelineID, ok := requireUUIDParam(c, "id", "invalid_pipeline_id", "Invalid pipeline ID format")
	if !ok {
		return
	}
	if _, ok := requirePipelineWorkspaceRole(c, pipelineID, security.WSMember); !ok {
		return
	}
	if temporalClient == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "temporal_client_not_configured"})
		return
	}

	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}

	var req ResumeConnectionsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_request", "message": err.Error()})
		return
	}

	execID := req.ExecutionID
	if execID == "" {
		var err error
		execID, err = resolveExecutionIDForPipeline(database, pipelineID)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "execution_not_found"})
			return
		}
	}

	payload := map[string]interface{}{
		"pipeline_id": pipelineID,
		"timestamp":   time.Now().UTC().Format(time.RFC3339),
	}

	// Prefer explicit IDs
	if req.SourceConnectionID != "" {
		payload["source_connection_id"] = req.SourceConnectionID
	}
	if req.DestinationConnectionID != "" {
		payload["destination_connection_id"] = req.DestinationConnectionID
	}

	// One-at-a-time fallback
	if req.ConnectionID != "" && (req.Direction == "source" || req.Direction == "destination") {
		if req.Direction == "source" {
			payload["source_connection_id"] = req.ConnectionID
		} else {
			payload["destination_connection_id"] = req.ConnectionID
		}
		payload["direction"] = req.Direction
	}

	if payload["source_connection_id"] == nil && payload["destination_connection_id"] == nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "missing_connection_ids",
			"message": "Provide source_connection_id and/or destination_connection_id (or direction+connection_id)",
		})
		return
	}

	// Connection-tenancy guard (P2e): the supplied connection ids must belong to
	// the caller's ACTIVE workspace. requirePipelineWorkspaceRole above only proves
	// the caller's role on the pipeline, not access to the connections being wired
	// in — without this a
	// member could signal a connection from another workspace into the run.
	// Validating the payload map covers both the explicit-id and the
	// direction+connection_id fallback in one place.
	activeWS := activeWorkspaceID(c)
	for _, key := range []string{"source_connection_id", "destination_connection_id"} {
		if s, ok := payload[key].(string); ok && s != "" && !connectionInWorkspace(database, s, activeWS) {
			c.JSON(http.StatusBadRequest, gin.H{
				"error":   "connection_not_in_workspace",
				"message": key + " does not belong to the active workspace",
			})
			return
		}
	}

	// Best-effort audit log for intent suggestion usage (only if metadata provided)
	if req.Metadata != nil {
		logAudit(c, "intent_resolution", "pipeline", pipelineID, map[string]interface{}{
			"metadata":                  req.Metadata,
			"source_connection_id":      payload["source_connection_id"],
			"destination_connection_id": payload["destination_connection_id"],
		})
		// Also forward metadata to workflow signal for optional downstream use.
		payload["metadata"] = req.Metadata
	}

	err := signalPipelineWorkflowWithFallback(c.Request.Context(), pipelineID, execID, "connections_configured", payload)
	if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"pipeline_id":  pipelineID,
			"execution_id": execID,
		}).Error("Failed to signal workflow: connections_configured")
		if isWorkflowAlreadyCompletedError(err) {
			msg := "This pipeline run is no longer active (workflow already completed). Please run the pipeline again."
			bestEffortMarkWorkflowEnded(database, pipelineID, execID, "failed", msg)
			c.JSON(http.StatusConflict, gin.H{"error": "workflow_already_completed", "message": msg, "execution_id": execID})
			return
		}
		if isWorkflowNotFoundError(err) {
			msg := "This pipeline run is no longer active (workflow not found). Please run the pipeline again."
			bestEffortMarkWorkflowEnded(database, pipelineID, execID, "failed", msg)
			c.JSON(http.StatusConflict, gin.H{"error": "workflow_not_found", "message": msg, "execution_id": execID})
			return
		}
		log.Errorf("failed to signal Temporal workflow: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed_to_signal_workflow", "message": "workflow signaling failed; please try again"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success":      true,
		"pipeline_id":  pipelineID,
		"execution_id": execID,
		"signal":       "connections_configured",
	})
}

// ResumePipelineConnectors signals the running workflow to continue after connectors are generated.
// Signal: connector_generated
func ResumePipelineConnectors(c *gin.Context) {
	pipelineID, ok := requireUUIDParam(c, "id", "invalid_pipeline_id", "Invalid pipeline ID format")
	if !ok {
		return
	}
	if _, ok := requirePipelineWorkspaceRole(c, pipelineID, security.WSMember); !ok {
		return
	}
	if temporalClient == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "temporal_client_not_configured"})
		return
	}

	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}

	var req ResumeConnectorsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_request", "message": err.Error()})
		return
	}

	execID := req.ExecutionID
	if execID == "" {
		var err error
		execID, err = resolveExecutionIDForPipeline(database, pipelineID)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "execution_not_found"})
			return
		}
	}

	payload := map[string]interface{}{
		"pipeline_id": pipelineID,
		"timestamp":   time.Now().UTC().Format(time.RFC3339),
	}
	if len(req.Connectors) > 0 {
		payload["connectors"] = req.Connectors
	}
	if req.Metadata != nil {
		payload["metadata"] = req.Metadata
	}

	err := signalPipelineWorkflowWithFallback(c.Request.Context(), pipelineID, execID, "connector_generated", payload)
	if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"pipeline_id":  pipelineID,
			"execution_id": execID,
		}).Error("Failed to signal workflow: connector_generated")
		if isWorkflowAlreadyCompletedError(err) {
			msg := "This pipeline run is no longer active (workflow already completed). Please run the pipeline again."
			bestEffortMarkWorkflowEnded(database, pipelineID, execID, "failed", msg)
			c.JSON(http.StatusConflict, gin.H{"error": "workflow_already_completed", "message": msg, "execution_id": execID})
			return
		}
		if isWorkflowNotFoundError(err) {
			msg := "This pipeline run is no longer active (workflow not found). Please run the pipeline again."
			bestEffortMarkWorkflowEnded(database, pipelineID, execID, "failed", msg)
			c.JSON(http.StatusConflict, gin.H{"error": "workflow_not_found", "message": msg, "execution_id": execID})
			return
		}
		log.Errorf("failed to signal Temporal workflow: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed_to_signal_workflow", "message": "workflow signaling failed; please try again"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success":      true,
		"pipeline_id":  pipelineID,
		"execution_id": execID,
		"signal":       "connector_generated",
	})
}

// ResumePipelineTables signals the running workflow to continue after user selects tables.
// Signal: tables_selected
func ResumePipelineTables(c *gin.Context) {
	pipelineID, ok := requireUUIDParam(c, "id", "invalid_pipeline_id", "Invalid pipeline ID format")
	if !ok {
		return
	}
	if _, ok := requirePipelineWorkspaceRole(c, pipelineID, security.WSMember); !ok {
		return
	}
	if temporalClient == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "temporal_client_not_configured"})
		return
	}

	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}

	var req ResumeTablesRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_request", "message": err.Error()})
		return
	}

	tables := normalizeSelectedTables(req.SelectedTables)

	// Resolve the pipeline's source connection up front — needed to expand any
	// "select entire database" ("*") / "select entire namespace" ("<ns>.*")
	// sentinel into an explicit, namespace-qualified list before the empty-check
	// and cache validation below.
	var sourceConnID sql.NullString
	if err := database.QueryRow(`SELECT source_connection_id FROM pipelines WHERE id = $1::uuid`, pipelineID).Scan(&sourceConnID); err != nil && err != sql.ErrNoRows {
		log.WithContext(c.Request.Context()).WithError(err).Warn("HITL: could not fetch source_connection_id for table validation")
	}

	sentinelExpanded := false
	if hasSelectionSentinel(tables) {
		connID := strings.TrimSpace(sourceConnID.String)
		if !sourceConnID.Valid || connID == "" {
			c.JSON(http.StatusBadRequest, gin.H{
				"error":   "cannot_resolve_all_tables",
				"message": "Cannot expand a whole-database selection without a source connection.",
			})
			return
		}
		resolved, expanded, rerr := resolveSelectionSentinels(
			c.Request.Context(), connID, tables,
			func(_ context.Context, id string, max int) ([]TableMetadata, error) {
				return discoverConnectionTablesForResolve(c, id, max)
			},
		)
		if rerr != nil {
			log.WithContext(c.Request.Context()).WithError(rerr).Warn("HITL: failed to resolve select-all sentinel")
			c.JSON(http.StatusBadGateway, gin.H{
				"error":   "table_resolution_failed",
				"message": "Could not enumerate tables for a whole-database selection. Please retry or select tables explicitly.",
			})
			return
		}
		tables = resolved
		sentinelExpanded = expanded
	}

	if len(tables) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "no_tables_selected",
			"message": "At least one table must be selected",
		})
		return
	}

	// Explicit destination schema-mode directive (multi-schema mirror). The
	// table-selection HITL sends schema_mode="preserve" (with an empty namespace)
	// when the selection spans multiple source schemas and the user left the
	// destination namespace blank. Persist it to the top-level
	// config.destination_schema_mode key that the executor reads FIRST
	// (preserveSourceSchemaLayout), so each source schema is mirrored at the
	// destination even when the SEEDED destination namespace is not an engine
	// default (e.g. SQL Server / Snowflake seed the source slug — merely omitting
	// destination_config would leave that non-default seed in place and silently
	// FLATTEN, colliding same-named tables across schemas → data loss, PR #549).
	// Best-effort: a persistence failure falls back to the auto policy.
	//
	// Recorded BEFORE the preserve/mirror rule below can nil the config out, because
	// "the caller sent nothing" and "the caller sent a mirror directive we dropped"
	// are opposite instructions that both leave req.DestinationConfig == nil. The
	// first wants the server to resolve the seeded namespace; the second must be left
	// alone, since re-attaching one namespace would flatten a multi-schema pipeline.
	clientSentDestinationConfig := req.DestinationConfig != nil

	if req.DestinationConfig != nil {
		mode := strings.ToLower(strings.TrimSpace(req.DestinationConfig.SchemaMode))
		if mode == "preserve" || mode == "mirror" || mode == "flatten" {
			if _, err := database.Exec(`
				UPDATE pipelines
				SET config = jsonb_set(COALESCE(config, '{}'::jsonb), '{destination_schema_mode}', to_jsonb($1::text), true),
				    updated_at = NOW()
				WHERE id = $2::uuid
			`, mode, pipelineID); err != nil {
				log.WithError(err).WithField("pipeline_id", pipelineID).Warn("ResumeTables: failed to persist destination_schema_mode (ignored)")
			}
			// A blank namespace paired with a preserve/mirror directive carries no
			// single target to validate or lock — drop the empty destination_config
			// so the namespace machinery below (which requires a real name) is
			// skipped. A flatten directive always carries a real namespace, so it
			// falls through to normal persistence.
			if (mode == "preserve" || mode == "mirror") && strings.TrimSpace(req.DestinationConfig.Namespace) == "" {
				req.DestinationConfig = nil
			}
		}
	}

	// Validate the destination namespace mapping (PR-C) BEFORE claiming/signalling
	// the workflow, so an invalid name fails fast without locking the pipeline into
	// a pending HITL claim. The authoritative re-validation + existence/privilege
	// probe still happens in the pre-migration assessment.
	if req.DestinationConfig != nil {
		if reason := naming.ValidateNamespace(req.DestinationConfig.Namespace); reason != "" {
			c.JSON(http.StatusUnprocessableEntity, gin.H{
				"error":  "invalid_destination_namespace",
				"reason": reason,
			})
			return
		}
	}

	// Validate table existence best-effort using cached connection schema (if available).
	// This prevents "table not found" failures much later in the executor stage when the user
	// typed a wrong/ambiguous table name in the manual input flow.
	// Skipped when the list was sentinel-expanded: those names come straight from
	// discovery (authoritative) and would spuriously fail against the 100-table cache.
	if !sentinelExpanded && sourceConnID.Valid && strings.TrimSpace(sourceConnID.String) != "" {
		if qualified, missing, ambiguous, used := validateAndQualifySelectedTablesFromCache(
			c.Request.Context(),
			strings.TrimSpace(sourceConnID.String),
			tables,
		); used {
			if len(missing) > 0 || len(ambiguous) > 0 {
				msg := "Some selected tables were not found in the source connection. Use fully-qualified names like <db>.<table>."
				resp := gin.H{
					// Keep `error` as user-facing message (frontend extractErrorMessage prefers it).
					"error":      msg,
					"message":    msg,
					"error_code": "invalid_tables",
				}
				if len(missing) > 0 {
					resp["missing_tables"] = missing
				}
				if len(ambiguous) > 0 {
					resp["ambiguous_tables"] = ambiguous
				}
				c.JSON(http.StatusBadRequest, resp)
				return
			}
			// If we can unambiguously qualify, prefer qualified names for downstream determinism.
			if len(qualified) > 0 {
				tables = qualified
			}
		}
	}

	execID := req.ExecutionID
	if execID == "" {
		var err error
		execID, err = resolveExecutionIDForPipeline(database, pipelineID)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "execution_not_found"})
			return
		}
	}

	// -------------------------------------------------------------------------
	// Atomic idempotency claim.
	//
	// The previous read-then-check-then-signal pattern had a TOCTOU window:
	// two concurrent requests (double-click, retry storm, ack lost) could
	// both pass the "already signaled?" check before either had written
	// the claim, then both signal Temporal — the workflow resumes twice
	// and DB-to-DB pipelines double-write at the destination.
	//
	// We collapse the check + claim into a single UPDATE that only writes
	// hitl_tables_selected when it isn't already set, then key all
	// subsequent decisions on RowsAffected:
	//   - rows == 1 → we won the race; proceed to signal.
	//   - rows == 0 → someone else claimed first; load their claim and
	//     return 200 "already_submitted" if their tables match ours, or
	//     409 Conflict if a different selection is already in flight.
	// -------------------------------------------------------------------------
	claimPayload, marshalErr := json.Marshal(map[string]interface{}{
		"execution_id":    execID,
		"selected_tables": tables,
		"timestamp":       time.Now().UTC().Format(time.RFC3339),
		"status":          "pending",
	})
	if marshalErr != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed_to_serialize_claim"})
		return
	}

	res, claimErr := database.ExecContext(c.Request.Context(), `
		UPDATE pipeline_progress
		SET metadata = jsonb_set(COALESCE(metadata, '{}'::jsonb), '{hitl_tables_selected}', $1::jsonb, true),
		    stage_last_heartbeat_at = NOW(),
		    updated_at = NOW()
		WHERE pipeline_id = $2
		  AND COALESCE(metadata->'hitl_tables_selected', 'null'::jsonb) = 'null'::jsonb
	`, string(claimPayload), pipelineID)
	if claimErr != nil {
		log.WithContext(c.Request.Context()).WithError(claimErr).Error("HITL tables: atomic claim UPDATE failed")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "claim_failed", "message": claimErr.Error()})
		return
	}
	rowsClaimed, _ := res.RowsAffected()

	if rowsClaimed == 0 {
		// Someone else claimed first. Load their claim to decide whether
		// this is a benign retry (same tables) or a real conflict.
		var meta sql.NullString
		if err := database.QueryRow(`SELECT metadata FROM pipeline_progress WHERE pipeline_id = $1`, pipelineID).Scan(&meta); err != nil && err != sql.ErrNoRows {
			log.WithContext(c.Request.Context()).WithError(err).Warn("HITL tables: failed to read claim after losing race")
		}
		var existingTables []string
		var existingExec string
		if meta.Valid && strings.TrimSpace(meta.String) != "" {
			var metaObj map[string]interface{}
			if err := json.Unmarshal([]byte(meta.String), &metaObj); err == nil && metaObj != nil {
				if v, ok := metaObj["hitl_tables_selected"].(map[string]interface{}); ok && v != nil {
					existingExec, _ = v["execution_id"].(string)
					if raw, ok := v["selected_tables"].([]interface{}); ok && raw != nil {
						for _, it := range raw {
							if s, ok := it.(string); ok {
								s = strings.TrimSpace(s)
								if s != "" {
									existingTables = append(existingTables, s)
								}
							}
						}
					}
				}
			}
		}
		existingTables = normalizeSelectedTables(existingTables)

		sameExec := strings.TrimSpace(existingExec) == strings.TrimSpace(execID)
		sameTables := false
		if sameExec && len(existingTables) == len(tables) {
			cmpA := append([]string{}, existingTables...)
			cmpB := append([]string{}, tables...)
			sort.Strings(cmpA)
			sort.Strings(cmpB)
			sameTables = true
			for i := range cmpA {
				if cmpA[i] != cmpB[i] {
					sameTables = false
					break
				}
			}
		}

		if sameExec && sameTables {
			// Benign double-click: same execution, same selection. Treat
			// as success without re-signaling.
			c.JSON(http.StatusOK, gin.H{
				"success":         true,
				"pipeline_id":     pipelineID,
				"execution_id":    execID,
				"signal":          "tables_selected",
				"status":          "already_submitted",
				"tables_count":    len(tables),
				"selected_tables": tables,
			})
			return
		}

		// Different selection already in flight. We deliberately do NOT
		// overwrite the prior claim — the workflow has already received
		// (or is about to receive) the first signal. Changing the
		// selection at this point would require a workflow-level reset
		// flow that does not yet exist; surface the conflict instead.
		c.JSON(http.StatusConflict, gin.H{
			"error":           "table_selection_in_flight",
			"message":         "A table selection is already being applied for this pipeline. Wait for it to complete before submitting a new one.",
			"execution_id":    existingExec,
			"selected_tables": existingTables,
		})
		return
	}

	// Persist table selection to the pipeline so future manual/scheduled runs can reuse it
	// (and won't require table selection again unless the user changes it).
	// We store this under pipelines.config.selected_tables (jsonb).
	if b, err := json.Marshal(tables); err == nil {
		if _, err := database.Exec(`
			UPDATE pipelines
			SET config = jsonb_set(COALESCE(config, '{}'::jsonb), '{selected_tables}', $1::jsonb, true),
			    updated_at = NOW()
			WHERE id = $2::uuid
		`, string(b), pipelineID); err != nil {
			log.WithError(err).WithField("pipeline_id", pipelineID).Warn("ResumeTables: failed to persist selected_tables (ignored)")
		}
	}

	// Persist the user's confirmed destination mapping (PR-C) in the same step.
	// Pipeline-scoped only — never the shared connection. Validated above.
	//
	// First-run namespace resolution (table-level collision → rsync_ prefix) +
	// lock. The resolved namespace is FROZEN on first run so reloads /
	// incremental / scheduled runs reuse the same namespace + tables and never
	// re-prompt or re-prefix (Airbyte stream-prefix / Fivetran schema-pin
	// semantics). Engine-agnostic: PG schema and MySQL database both go through
	// resolveFirstRunNamespace.
	//
	// A caller that sends NO mapping still gets the probe: the collision check is a
	// data-safety property, not a UI feature, and gating it on the request body meant
	// whole surfaces skipped it (PipelineMonitoringPanel resumes first-run table
	// selection with only execution_id + selected_tables). serverSideFirstRunNamespace
	// falls back to the pipeline's own persisted mapping — except where probing a
	// single namespace would itself be wrong, i.e. when source schemas are being
	// mirrored.
	if cfg := req.DestinationConfig; cfg != nil {
		lockFirstRunNamespace(c.Request.Context(), database, c.GetString("workspace_id"), pipelineID, *cfg, tables, "ResumeTables")
	} else {
		persisted, schemaMode := pipelineDestinationState(database, pipelineID)
		if seeded, probe := serverSideFirstRunNamespace(clientSentDestinationConfig, persisted, schemaMode); probe {
			lockFirstRunNamespace(c.Request.Context(), database, c.GetString("workspace_id"), pipelineID, seeded, tables, "ResumeTables")
		}
	}

	payload := map[string]interface{}{
		"pipeline_id":     pipelineID,
		"selected_tables": tables,
		"timestamp":       time.Now().UTC().Format(time.RFC3339),
	}
	if req.Metadata != nil {
		payload["metadata"] = req.Metadata
	}

	signalErr := signalPipelineWorkflowWithFallback(c.Request.Context(), pipelineID, execID, "tables_selected", payload)
	if signalErr != nil {
		// Release the claim so the caller can retry — without this, a
		// transient Temporal blip would permanently lock the pipeline
		// into "table_selection_in_flight" 409s.
		if _, releaseErr := database.Exec(`
			UPDATE pipeline_progress
			SET metadata = metadata - 'hitl_tables_selected',
			    updated_at = NOW()
			WHERE pipeline_id = $1
			  AND metadata->'hitl_tables_selected'->>'status' = 'pending'
		`, pipelineID); releaseErr != nil {
			log.WithError(releaseErr).WithField("pipeline_id", pipelineID).Warn("ResumeTables: failed to release claim after signal failure (ignored)")
		}

		log.WithError(signalErr).WithFields(log.Fields{
			"pipeline_id":  pipelineID,
			"execution_id": execID,
		}).Error("Failed to signal workflow: tables_selected")
		if isWorkflowAlreadyCompletedError(signalErr) {
			msg := "This pipeline run is no longer active (workflow already completed). Please run the pipeline again."
			bestEffortMarkWorkflowEnded(database, pipelineID, execID, "failed", msg)
			c.JSON(http.StatusConflict, gin.H{"error": "workflow_already_completed", "message": msg, "execution_id": execID})
			return
		}
		if isWorkflowNotFoundError(signalErr) {
			msg := "This pipeline run is no longer active (workflow not found). Please run the pipeline again."
			bestEffortMarkWorkflowEnded(database, pipelineID, execID, "failed", msg)
			c.JSON(http.StatusConflict, gin.H{"error": "workflow_not_found", "message": msg, "execution_id": execID})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed_to_signal_workflow", "message": signalErr.Error()})
		return
	}

	// Signal landed — promote the claim from "pending" to "submitted" and
	// clear the HITL blocking_reason so the UI doesn't keep showing the
	// table-selection modal while Temporal propagates to the next stage.
	if b, marshalErr := json.Marshal(map[string]interface{}{
		"execution_id":    execID,
		"selected_tables": tables,
		"timestamp":       time.Now().UTC().Format(time.RFC3339),
		"status":          "submitted",
	}); marshalErr == nil {
		if _, err := database.Exec(`
			UPDATE pipeline_progress
			SET status = 'processing',
			    blocking_reason_type = NULL,
			    blocking_reason_description = NULL,
			    blocking_reason_estimated_seconds = NULL,
			    message = 'Resuming after table selection...',
			    stage_last_heartbeat_at = NOW(),
			    metadata = jsonb_set(COALESCE(metadata, '{}'::jsonb), '{hitl_tables_selected}', $1::jsonb, true),
			    updated_at = NOW()
			WHERE pipeline_id = $2
		`, string(b), pipelineID); err != nil {
			log.WithError(err).WithField("pipeline_id", pipelineID).Warn("ResumeTables: failed to mark HITL submitted (ignored)")
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"success":         true,
		"pipeline_id":     pipelineID,
		"execution_id":    execID,
		"signal":          "tables_selected",
		"tables_count":    len(tables),
		"selected_tables": tables,
	})
}

// lockFirstRunNamespace resolves a pipeline's destination namespace against the live
// destination and FREEZES the result, so reloads / incremental / scheduled runs reuse
// the same namespace and never re-prompt or re-prefix. Returns the namespace that is
// now locked.
//
// Two callers, and both are required for the lock to actually hold:
//   - "ResumeTables" — the table-selection HITL, where the user picks the tables.
//   - "run-boundary" — lockNamespaceForRun, reached from the executor once the final
//     table set is known. A pipeline whose prompt NAMES its tables never parks at the
//     HITL, so before that caller existed it wrote to the seeded namespace with no
//     probe at all (KI-NSLOCK-PROBE-UNREACHABLE-WITHOUT-HITL). Reachability is part of
//     the safety property: a probe only the HITL can trigger is not a probe.
//
// Idempotent by the destination_namespace_locked guard below, so the two callers may
// both fire for the same pipeline; the second one reuses the first one's answer.
//
// Already locked → reuse the locked value verbatim; a second resolve could move a
// pipeline's data mid-life, which is worse than any collision it might avoid.
//
// Fail-soft throughout: a missing destination connection, a failed probe, or a failed
// write all degrade to "lock the chosen namespace unverified". Blocking the resume
// would strand the pipeline in its HITL park over a control-plane hiccup.
//
// That fail-soft is UNBACKED -- this comment used to claim a real collision still
// surfaces at write time via the connector's _rsync_pipelines ownership gate. It does
// not: drop_table co-registers the reloading pipeline and falls through to DROP, the
// pipeline_id-equality refusal having been removed in PR #121. Ownership is answered
// by the control-plane query added in #762. See ensureDestinationNamespaceLocked in
// backend-orchestrator/internal/agents/executor/namespace_lock.go for the full note.
// Returns (resolved namespace, relocation or nil, checkpoints cleared by that
// relocation). The second and third values are for the caller that has to report
// them over the wire; both HITL call sites ignore them.
func lockFirstRunNamespace(ctx context.Context, database *sql.DB, workspaceID, pipelineID string, cfg DestinationConfig, tables []string, caller string) (string, *namespaceRelocation, int64) {
	if locked, lockedNS := destinationNamespaceLock(database, pipelineID); locked && strings.TrimSpace(lockedNS) != "" {
		cfg.Namespace = lockedNS
		log.WithFields(log.Fields{"pipeline_id": pipelineID, "namespace": lockedNS, "caller": caller}).
			Info("namespace lock: destination namespace already locked; reusing")
		if err := persistResolvedDestinationConfig(database, pipelineID, cfg); err != nil {
			log.WithError(err).WithFields(log.Fields{"pipeline_id": pipelineID, "caller": caller}).Warn("namespace lock: failed to re-persist locked destination_config (ignored)")
		}
		// No relocation to report: the lock is one-way, so a pipeline that is
		// already locked was either never moved or was moved on the run that set
		// the lock. Reporting one here would clear checkpoints on every run.
		return lockedNS, nil, 0
	}

	// First run: resolve against live destination, then lock.
	destConnID, destType := destinationConnIDAndType(database, pipelineID)
	chosen := cfg.Namespace
	resolved := cfg.Namespace
	var relocated *namespaceRelocation
	if destConnID != "" && destType != "" {
		resolved, relocated = resolveFirstRunNamespace(ctx, database, workspaceID, destConnID, destType, pipelineID, cfg.Namespace, tables)
	} else {
		log.WithFields(log.Fields{"pipeline_id": pipelineID, "caller": caller}).Warn("namespace lock: missing destination conn/type; locking chosen namespace without collision probe")
	}
	cfg.Namespace = resolved
	if err := persistResolvedDestinationConfig(database, pipelineID, cfg); err != nil {
		log.WithError(err).WithFields(log.Fields{"pipeline_id": pipelineID, "caller": caller}).Warn("namespace lock: failed to persist resolved destination_config (ignored)")
	}
	// A relocated pipeline's resume checkpoints were written against the namespace
	// it just left, and carry no namespace of their own — left in place they resume
	// "already complete" and the new namespace is never even created.
	cleared := clearRelocatedCheckpoints(ctx, database, pipelineID, relocated)
	// Tell the user when the lock moved them off the destination they picked —
	// the lock is permanent, so this is their only chance to find out.
	notifyNamespaceRelocation(ctx, database, pipelineID, relocated)
	// The one line that says the probe RAN, and who made it run. Reachability was
	// the whole defect, so it needs its own signature in prod logs, separate from
	// the resolution decision (which resolveFirstRunNamespace logs).
	log.WithFields(log.Fields{
		"pipeline_id": pipelineID,
		"caller":      caller,
		"chosen":      chosen,
		"resolved":    resolved,
		"relocated":   relocated != nil,
		"table_count": len(tables),
		// 0 on a relocation is meaningful too: it says the pipeline had never run,
		// which is the case where nothing was stranded.
		"checkpoints_cleared": cleared,
	}).Info("namespace lock: first-run namespace resolved and locked")
	return resolved, relocated, cleared
}

// serverSideFirstRunNamespace decides what to resolve + lock when the caller sent no
// destination_config, and returns the mapping to use.
//
// The probe used to run only for callers that sent one, which made a data-safety
// check a property of the client rather than of the API: a resume carrying just
// {execution_id, selected_tables} landed in the seeded namespace with no collision
// check at all, silently merging into tables that were already there.
//
// It stands down in exactly one situation — when source schemas are being MIRRORED to
// the destination. There is no single namespace to probe then, and attaching one would
// flatten every source schema into it, colliding same-named tables across schemas
// (the PR #549 data loss). Mirroring is signalled two ways, and both must be honoured:
// the request carried a preserve/mirror directive that the caller-side rule already
// dropped (clientSentConfig, config now nil), or the directive is sticky on the
// pipeline from an earlier run (schemaMode).
func serverSideFirstRunNamespace(clientSentConfig bool, persisted *DestinationConfig, schemaMode string) (DestinationConfig, bool) {
	if clientSentConfig {
		return DestinationConfig{}, false
	}
	switch strings.ToLower(strings.TrimSpace(schemaMode)) {
	case "preserve", "mirror":
		return DestinationConfig{}, false
	}
	if persisted == nil || strings.TrimSpace(persisted.Namespace) == "" {
		return DestinationConfig{}, false
	}

	// SchemaMode is a request-time directive and never belongs in the stored object;
	// persisted values are read back through a type that has no such field, but copy
	// the fields explicitly so that stays true if the shapes ever converge.
	return DestinationConfig{
		Namespace:         strings.TrimSpace(persisted.Namespace),
		NamespaceKind:     persisted.NamespaceKind,
		CreateIfNotExists: persisted.CreateIfNotExists,
	}, true
}

// pipelineDestinationState reads the mapping a pipeline was created with plus any
// sticky schema-mode directive, for the server-side first-run probe.
//
// Fail-soft: any error returns (nil, "") — which serverSideFirstRunNamespace reads as
// "nothing to probe", leaving behaviour exactly as it was before this path existed.
func pipelineDestinationState(database *sql.DB, pipelineID string) (*DestinationConfig, string) {
	if database == nil || strings.TrimSpace(pipelineID) == "" {
		return nil, ""
	}
	var configJSON []byte
	var mode sql.NullString
	err := database.QueryRow(`
		SELECT COALESCE(config, '{}'::jsonb)::text, config->>'destination_schema_mode'
		FROM pipelines WHERE id = $1::uuid
	`, pipelineID).Scan(&configJSON, &mode)
	if err != nil {
		return nil, ""
	}

	dc := extractDestinationConfigFromConfigJSON(configJSON)
	if dc == nil {
		return nil, mode.String
	}
	return &DestinationConfig{
		Namespace:         dc.Namespace,
		NamespaceKind:     dc.NamespaceKind,
		CreateIfNotExists: dc.CreateIfNotExists,
	}, mode.String
}

// ==============================================================================
// DAG NODE INPUT HITL HANDLER
// ==============================================================================

// ResumePipelineNodeInput signals the running DAG workflow to continue after user provides NL input for a node.
// Signal: node_input_provided
//
// This endpoint is called when a DAG workflow is waiting for user input on a specific node.
// The user's natural language response is sent to the workflow, which will interpret it
// and apply the configuration updates to the node.
//
// POST /api/v1/pipelines/:id/node-input
func ResumePipelineNodeInput(c *gin.Context) {
	pipelineID, ok := requireUUIDParam(c, "id", "invalid_pipeline_id", "Invalid pipeline ID format")
	if !ok {
		return
	}
	if _, ok := requirePipelineWorkspaceRole(c, pipelineID, security.WSMember); !ok {
		return
	}
	if temporalClient == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "temporal_client_not_configured"})
		return
	}

	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}

	var req ResumeNodeInputRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_request", "message": err.Error()})
		return
	}

	// Validate required fields
	if req.NodeID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "missing_node_id",
			"message": "node_id is required",
		})
		return
	}
	if req.Message == "" && req.ConfigPatch == nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "missing_input",
			"message": "Either message or config_patch must be provided",
		})
		return
	}

	execID := req.ExecutionID
	if execID == "" {
		var err error
		execID, err = resolveExecutionIDForPipeline(database, pipelineID)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "execution_not_found"})
			return
		}
	}

	// -------------------------------------------------------------------------
	// Idempotency guard (similar to table selection)
	// -------------------------------------------------------------------------
	alreadySignaled := false
	{
		var meta sql.NullString
		if err := database.QueryRow(`SELECT metadata FROM pipeline_progress WHERE pipeline_id = $1`, pipelineID).Scan(&meta); err != nil && err != sql.ErrNoRows {
			log.WithContext(c.Request.Context()).WithError(err).Warn("HITL: idempotency guard DB query failed — proceeding without dedup protection")
		}
		if meta.Valid && strings.TrimSpace(meta.String) != "" {
			var metaObj map[string]interface{}
			if err := json.Unmarshal([]byte(meta.String), &metaObj); err == nil && metaObj != nil {
				if v, ok := metaObj["hitl_node_input_submitted"].(map[string]interface{}); ok && v != nil {
					existingExec, _ := v["execution_id"].(string)
					existingNode, _ := v["node_id"].(string)
					if strings.TrimSpace(existingExec) == strings.TrimSpace(execID) &&
						strings.TrimSpace(existingNode) == strings.TrimSpace(req.NodeID) {
						alreadySignaled = true
					}
				}
			}
		}
	}
	if alreadySignaled {
		// Idempotent re-submission: do not signal Temporal again, but refresh heartbeat
		// so the UI doesn't mark the pipeline as stale while resuming.
		if _, err := database.Exec(`
			UPDATE pipeline_progress
			SET stage_last_heartbeat_at = NOW(),
			    updated_at = NOW()
			WHERE pipeline_id = $1
		`, pipelineID); err != nil {
			log.WithError(err).WithField("pipeline_id", pipelineID).Warn("ResumePipelineNodeInput: heartbeat update failed (ignored)")
		}
		c.JSON(http.StatusOK, gin.H{
			"success":      true,
			"pipeline_id":  pipelineID,
			"execution_id": execID,
			"node_id":      req.NodeID,
			"signal":       "node_input_provided",
			"status":       "already_submitted",
		})
		return
	}

	// Build signal payload
	payload := map[string]interface{}{
		"pipeline_id":  pipelineID,
		"execution_id": execID,
		"node_id":      req.NodeID,
		"message":      req.Message,
		"timestamp":    time.Now().UTC().Format(time.RFC3339),
	}
	if req.ConfigPatch != nil {
		payload["config_patch"] = req.ConfigPatch
	}
	if req.Metadata != nil {
		payload["metadata"] = req.Metadata
	}

	// Signal the workflow
	err := signalPipelineWorkflowWithFallback(c.Request.Context(), pipelineID, execID, "node_input_provided", payload)
	if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"pipeline_id":  pipelineID,
			"execution_id": execID,
			"node_id":      req.NodeID,
		}).Error("Failed to signal workflow: node_input_provided")
		if isWorkflowAlreadyCompletedError(err) {
			msg := "This pipeline run is no longer active (workflow already completed). Please run the pipeline again."
			bestEffortMarkWorkflowEnded(database, pipelineID, execID, "failed", msg)
			c.JSON(http.StatusConflict, gin.H{"error": "workflow_already_completed", "message": msg, "execution_id": execID})
			return
		}
		if isWorkflowNotFoundError(err) {
			msg := "This pipeline run is no longer active (workflow not found). Please run the pipeline again."
			bestEffortMarkWorkflowEnded(database, pipelineID, execID, "failed", msg)
			c.JSON(http.StatusConflict, gin.H{"error": "workflow_not_found", "message": msg, "execution_id": execID})
			return
		}
		log.Errorf("failed to signal Temporal workflow: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed_to_signal_workflow", "message": "workflow signaling failed; please try again"})
		return
	}

	// Best-effort: mark HITL as submitted
	if b, err := json.Marshal(map[string]interface{}{
		"execution_id": execID,
		"node_id":      req.NodeID,
		"timestamp":    time.Now().UTC().Format(time.RFC3339),
	}); err == nil {
		if _, err := database.Exec(`
			UPDATE pipeline_progress
			SET status = 'processing',
			    blocking_reason_type = NULL,
			    blocking_reason_description = NULL,
			    blocking_reason_estimated_seconds = NULL,
			    message = 'Processing node input...',
			    -- Bump heartbeat so UI doesn't mark the pipeline as stale while resuming.
			    stage_last_heartbeat_at = NOW(),
			    metadata = jsonb_set(COALESCE(metadata, '{}'::jsonb), '{hitl_node_input_submitted}', $1::jsonb, true),
			    updated_at = NOW()
			WHERE pipeline_id = $2
		`, string(b), pipelineID); err != nil {
			log.WithError(err).WithField("pipeline_id", pipelineID).Warn("ResumePipelineNodeInput: failed to mark HITL submitted (ignored)")
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"success":      true,
		"pipeline_id":  pipelineID,
		"execution_id": execID,
		"node_id":      req.NodeID,
		"signal":       "node_input_provided",
	})
}
