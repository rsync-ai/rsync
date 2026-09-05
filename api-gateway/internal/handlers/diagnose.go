package handlers

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"api-gateway/internal/db"
	"api-gateway/internal/security"
	"api-gateway/internal/telemetry"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/rsync-ai/backend-orchestrator/pkg/llmscrub"
	log "github.com/sirupsen/logrus"
)

// DiagnoseResponse mirrors llm-service's response shape so the UI can render
// it without two parsing layers.
type DiagnoseResponse struct {
	Summary          string   `json:"summary"`
	RootCause        string   `json:"root_cause"`
	SuggestedAction  string   `json:"suggested_action"`
	Confidence       string   `json:"confidence"`
	EvidencePointers []string `json:"evidence_pointers,omitempty"`
	Raw              string   `json:"raw,omitempty"`
	// Diagnostic context surfaced alongside the LLM verdict so on-call can
	// verify or override quickly.
	Evidence map[string]interface{} `json:"evidence,omitempty"`
}

// DiagnosePipeline gathers structured evidence about a misbehaving pipeline
// and asks llm-service for a root-cause hypothesis.
//
// POST /api/v1/pipelines/:id/diagnose
//
// Read-only — never mutates pipeline state. The model is asked to *explain*,
// never to act. Caller is the on-call engineer or a UI button.
func DiagnosePipeline(c *gin.Context) {
	pipelineID := c.Param("id")
	if _, err := uuid.Parse(pipelineID); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Pipeline not found"})
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

	evidence, ok := gatherDiagnosisEvidence(database, pipelineID)
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "Pipeline not found"})
		return
	}

	ctx := c.Request.Context()

	// Phase 3b: pull the verbatim SigNoz log lines for the stalled/failed stage
	// (fail-soft — attaches nothing if SigNoz is unavailable).
	if flow, ok := evidence["flow"].(map[string]interface{}); ok {
		enrichFlowWithSigNozLogs(ctx, flow)
	}

	// When inserted_rows == 0, pull sink write errors by pipeline_id so the LLM
	// sees the actual error text (e.g. missing PK / ON CONFLICT constraint) and
	// can give an actionable diagnosis instead of a generic "silent drop" report.
	enrichEvidenceWithSinkErrors(ctx, pipelineID, evidence)

	verdict, status, err := callDiagnoseLLM(ctx, pipelineID, evidence)
	if err != nil {
		// Even if the LLM call fails we still return the evidence — that's
		// useful by itself for on-call.
		log.Warnf("diagnose: LLM call failed for %s: %v", pipelineID, err)
		c.JSON(http.StatusBadGateway, gin.H{
			"error":    "diagnose_unavailable",
			"message":  err.Error(),
			"evidence": evidence,
		})
		return
	}
	verdict.Evidence = evidence
	c.JSON(status, verdict)
}

// gatherDiagnosisEvidence joins everything we'd want a human on-call to see:
// the canonical runtime view (mode/phase/health/deps), the underlying raw
// pipeline_progress, the latest execution outcome, the most recent error
// messages, and a small slice of recent monitoring events. The shape is
// intentionally flat-ish so the LLM can reason about it without nested
// gymnastics.
//
// Returns (evidence, exists). exists=false means the pipeline doesn't exist.
func gatherDiagnosisEvidence(database *sql.DB, pipelineID string) (map[string]interface{}, bool) {
	ev := map[string]interface{}{
		"pipeline_id":   pipelineID,
		"gathered_at":   time.Now().UTC().Format(time.RFC3339),
	}

	// 1) Pipeline base info — joined to connections so the LLM sees the
	//    actual connector types, not just the abstract connection IDs.
	var name, status, syncMode sql.NullString
	var srcType, dstType sql.NullString
	var srcConnID, dstConnID sql.NullString
	var createdAt, updatedAt time.Time
	err := database.QueryRow(`
		SELECT p.name, p.status, p.sync_mode,
		       p.source_connection_id, p.destination_connection_id,
		       sc.connector_type, dc.connector_type,
		       p.created_at, p.updated_at
		FROM pipelines p
		LEFT JOIN connections sc ON sc.id = p.source_connection_id
		LEFT JOIN connections dc ON dc.id = p.destination_connection_id
		WHERE p.id = $1
	`, pipelineID).Scan(&name, &status, &syncMode, &srcConnID, &dstConnID, &srcType, &dstType, &createdAt, &updatedAt)
	if err == sql.ErrNoRows {
		return nil, false
	}
	if err != nil {
		log.Warnf("diagnose: pipelines query failed: %v", err)
		return nil, false
	}
	ev["pipeline"] = map[string]interface{}{
		"name":                      name.String,
		"status":                    status.String,
		"sync_mode":                 syncMode.String,
		"source_connection_id":      srcConnID.String,
		"destination_connection_id": dstConnID.String,
		"source_type":               srcType.String,
		"destination_type":          dstType.String,
		"created_at":                createdAt.Format(time.RFC3339),
		"updated_at":                updatedAt.Format(time.RFC3339),
	}

	// 2) Latest pipeline_progress snapshot
	var execID, currentStage, message, blockType, blockDesc sql.NullString
	var pPercent, pStep, pTotal sql.NullInt32
	var pUpdatedAt sql.NullTime
	_ = database.QueryRow(`
		SELECT execution_id, current_stage, message,
		       progress_percent, progress_current_step, progress_total_steps,
		       blocking_reason_type, blocking_reason_description, updated_at
		FROM pipeline_progress WHERE pipeline_id = $1
	`, pipelineID).Scan(&execID, &currentStage, &message,
		&pPercent, &pStep, &pTotal, &blockType, &blockDesc, &pUpdatedAt)
	if execID.Valid {
		ev["execution_id"] = execID.String
	}
	// Free-text progress fields can carry executor error text (and with it row
	// values) — scrub before they enter LLM-bound evidence (privacy contract).
	progress := map[string]interface{}{
		"current_stage":       currentStage.String,
		"message":             llmscrub.Scrub(message.String),
		"percent":             pPercent.Int32,
		"step":                pStep.Int32,
		"total_steps":         pTotal.Int32,
		"blocking_reason":     blockType.String,
		"blocking_description": llmscrub.Scrub(blockDesc.String),
	}
	if pUpdatedAt.Valid {
		progress["updated_at"] = pUpdatedAt.Time.Format(time.RFC3339)
		progress["seconds_since_update"] = int64(time.Since(pUpdatedAt.Time).Seconds())
	}
	ev["progress"] = progress

	// 3) Dependency manifest + observed health (uses the same loader as /runtime).
	// LastError and Details carry free-form probe/recovery error text (arbitrary
	// %v errors, raw Docker output) — scrub before LLM exposure like every other
	// free-text field in this evidence map.
	deps, depAggregate := loadRuntimeDeps(database, pipelineID)
	for i := range deps {
		deps[i].LastError = llmscrub.Scrub(deps[i].LastError)
		for k, v := range deps[i].Details {
			if s, ok := v.(string); ok {
				deps[i].Details[k] = llmscrub.Scrub(s)
			}
		}
	}
	ev["dependencies"] = deps
	ev["dependency_health_aggregate"] = depAggregate

	// 4) Latest execution outcome
	var lastExecID, lastExecStatus, lastExecError sql.NullString
	var lastExecStarted sql.NullTime
	_ = database.QueryRow(`
		SELECT id, status, error_message, start_time
		FROM executions WHERE pipeline_id = $1 ORDER BY start_time DESC LIMIT 1
	`, pipelineID).Scan(&lastExecID, &lastExecStatus, &lastExecError, &lastExecStarted)
	if lastExecID.Valid {
		ev["latest_execution"] = map[string]interface{}{
			"id":     lastExecID.String,
			"status": lastExecStatus.String,
			// Database error text may embed failed row values — scrub before
			// LLM exposure. enrichEvidenceWithSinkErrors matches on the scrubbed
			// text; its trigger substrings (silent_drop, dest write failed) are
			// preserved by Scrub.
			"error_message": llmscrub.Scrub(lastExecError.String),
			"started_at":    nullTimeOrEmpty(lastExecStarted),
		}
	}

	// 5) Last 30 events from pipeline_events (best-effort; table name may differ).
	//    We keep payload short to stay under the LLM context budget.
	ev["recent_events"] = recentPipelineEvents(database, pipelineID, 30)

	// 6) Table-level summary (works for both CDC and batch). For CDC, reads
	//    Debezium-applied event counts from pipeline_run_table_stats; for batch,
	//    reads inserted_rows. Last event timestamp is critical for "is the
	//    stream actually flowing?" diagnoses.
	var totalEvents, insertedRows sql.NullInt64
	var lastEventAt sql.NullTime
	_ = database.QueryRow(`
		SELECT COALESCE(SUM(COALESCE(total_events, 0)), 0),
		       COALESCE(SUM(COALESCE(inserted_rows, 0)), 0),
		       MAX(last_event_ts)
		FROM pipeline_run_table_stats WHERE pipeline_id = $1
	`, pipelineID).Scan(&totalEvents, &insertedRows, &lastEventAt)
	tableStats := map[string]interface{}{
		"total_events":  totalEvents.Int64,
		"inserted_rows": insertedRows.Int64,
	}
	if lastEventAt.Valid {
		tableStats["last_event_at"] = lastEventAt.Time.Format(time.RFC3339)
		tableStats["seconds_since_last_event"] = int64(time.Since(lastEventAt.Time).Seconds())
	}
	ev["table_stats_summary"] = tableStats

	// 7) Deterministic flow verification — fold pipeline_run_events onto the
	//    canonical stage sequence to report which stage the run reached and where
	//    it stalled. This is the single most useful triage fact and is computed
	//    without an LLM guess (the LLM then explains *why*, not *where*).
	ev["flow"] = computePipelineFlow(database, pipelineID, execID.String, syncMode.String)

	return ev, true
}

func recentPipelineEvents(database *sql.DB, pipelineID string, limit int) []map[string]interface{} {
	out := []map[string]interface{}{}
	// pipeline_run_events is the Temporal-projected event stream. We pull the
	// fields most useful for a 2am diagnosis (type, severity, stage_id) but
	// skip raw payload to keep the LLM context small — the LLM rarely needs
	// the inner payload for triage.
	rows, err := database.Query(`
		SELECT event_type,
		       COALESCE(stage_id, ''),
		       COALESCE(stage_group, ''),
		       COALESCE(severity, ''),
		       COALESCE(occurred_at, received_at)
		FROM pipeline_run_events WHERE pipeline_id = $1
		ORDER BY COALESCE(occurred_at, received_at) DESC LIMIT $2
	`, pipelineID, limit)
	if err != nil {
		log.Debugf("diagnose: pipeline_run_events query failed: %v", err)
		return out
	}
	defer rows.Close()
	for rows.Next() {
		var eventType, stageID, stageGroup, severity string
		var ts time.Time
		if err := rows.Scan(&eventType, &stageID, &stageGroup, &severity, &ts); err != nil {
			continue
		}
		out = append(out, map[string]interface{}{
			"type":        eventType,
			"stage_id":    stageID,
			"stage_group": stageGroup,
			"severity":    severity,
			"at":          ts.Format(time.RFC3339),
		})
	}
	return out
}

func nullTimeOrEmpty(t sql.NullTime) string {
	if !t.Valid {
		return ""
	}
	return t.Time.Format(time.RFC3339)
}

func callDiagnoseLLM(ctx context.Context, pipelineID string, evidence map[string]interface{}) (*DiagnoseResponse, int, error) {
	llmURL := strings.TrimSpace(os.Getenv("LLM_SERVICE_URL"))
	if llmURL == "" {
		llmURL = "http://llm-service:5000"
	}
	llmURL = strings.TrimRight(llmURL, "/") + "/v1/diagnose/pipeline"

	body, err := json.Marshal(map[string]interface{}{
		"pipeline_id": pipelineID,
		"evidence":    evidence,
	})
	if err != nil {
		return nil, http.StatusInternalServerError, fmt.Errorf("encode evidence: %w", err)
	}

	client := &http.Client{Timeout: 30 * time.Second}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, llmURL, bytes.NewReader(body))
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	for k, v := range telemetry.InjectTraceToHeaders(ctx) {
		httpReq.Header.Set(k, v)
	}

	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, http.StatusBadGateway, fmt.Errorf("llm-service unreachable: %w", err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return nil, resp.StatusCode, fmt.Errorf("llm-service returned %d: %s", resp.StatusCode, truncate(string(respBody), 300))
	}
	var verdict DiagnoseResponse
	if err := json.Unmarshal(respBody, &verdict); err != nil {
		return nil, http.StatusBadGateway, fmt.Errorf("decode llm-service response: %w", err)
	}
	return &verdict, http.StatusOK, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
