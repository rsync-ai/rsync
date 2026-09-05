package handlers

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"

	"api-gateway/internal/db"

	"github.com/gin-gonic/gin"
	"github.com/rsync-ai/backend-orchestrator/pkg/diagnose"
	log "github.com/sirupsen/logrus"
)

// Chat assistant: "why did my pipeline fail?" flow.
//
// Composes Phase 3 (Diagnoser) + Phase 5 (intent routing). The user types
// something like "why did pipeline c5168d6d fail" or "what went wrong with
// my last run" and the assistant:
//
//  1. Resolves which execution they're asking about (explicit id, latest
//     for a named pipeline, or "user's most recent failed run").
//  2. Reads the execution row + per-table stats from Postgres.
//  3. Builds a diagnose.Signal and runs the rule-based diagnoser.
//  4. Returns a typed, UI-renderable response: human-readable explanation
//     + closed-enum action + deep_link + confidence.
//
// The endpoint deliberately does no LLM call. Phase 3's rule classifier is
// the source of truth; an LLM ELI5 layer can wrap this later without
// changing the response shape.

// diagnoseMatchers — keyword/regex patterns that trigger the diagnose
// flow. Ordered most-specific first so the first hit wins. Confidence
// comes from which pattern matched (specific id > pipeline name > "last
// failed"), not a probability.
var (
	// "why did pipeline c5168d6d-... fail" / "diagnose execution c5168d6d-..."
	// / "check issues on <id>". The UUID may be a pipeline id OR an execution id.
	reDiagWithUUID = regexp.MustCompile(`(?i)\b(?:diagnose|why|fail|failure|fix|debug|check|issue|problem|wrong|troubleshoot|what.+wrong|wrong.+with)\b.*?\b([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\b`)
	// "why did my last (run|pipeline|execution) fail"
	reDiagLast = regexp.MustCompile(`(?i)\b(?:why|what.+wrong|diagnose|fix|check).*\b(?:last|latest|recent|previous)\b.*\b(?:run|execution|pipeline|sync)\b`)
	// catch-all: starts with "diagnose"/"troubleshoot" or "why" near "fail".
	reDiagGeneric = regexp.MustCompile(`(?i)\b(?:diagnose|troubleshoot|fix.+(?:pipeline|sync|run)|why.+(?:fail|broken|stuck|not work))`)
	// Natural-language "something is wrong" phrasings a real self-serve user
	// actually types instead of the word "diagnose": "check issues", "what's
	// wrong", "not working", "0 rows", "stuck". These route to a diagnosis of the
	// user's most relevant (failed or running) pipeline. Kept broad on purpose —
	// the resolver degrades gracefully when there is no pipeline to inspect. Word
	// boundaries keep "network"/"working" from tripping the "not work" clause.
	reDiagBroad = regexp.MustCompile(`(?i)(check\s+(?:the\s+|for\s+|any\s+|my\s+)?(?:issue|problem|error|status)|what(?:'?s| is| went| is going)?\s+(?:wrong|the\s+issue|the\s+problem|happening|going\s+on)|\bnot\s+work|isn'?t\s+work|does\s*n'?t\s+work|no\s+(?:data|rows|records)|zero\s+(?:rows|records|data)|\b0\s+rows|\bstuck\b|\bhang(?:ing|s)?\b|\bbroken\b|(?:pipeline|sync|run|transfer|load|it|this)\s+(?:is\s+|are\s+|keeps?\s+)?(?:fail|error|issue|problem|broken|stuck|not\s+work))`)
)

// maybeHandleDiagnoseCommand returns (response, true) when the message
// is a diagnose request the handler took ownership of.
//
// Scoped to the caller's ACTIVE workspace, not to created_by: "diagnose my last
// run" must mean the last run of the workspace the user is looking at. Scoping
// by creator both named the previous tenant's pipeline after a switch and hid a
// teammate's failing run from the person asking about it.
func (h *ChatHandler) maybeHandleDiagnoseCommand(
	ctx context.Context, message, workspaceID, traceID string,
) (ChatMessageResponse, bool) {
	msg := strings.TrimSpace(message)
	if msg == "" {
		return ChatMessageResponse{}, false
	}
	if workspaceID == "" {
		return ChatMessageResponse{}, false
	}

	database := db.GetDB()
	if database == nil {
		return ChatMessageResponse{}, false
	}

	// Pattern 1: explicit UUID in the message. Highest confidence — the user
	// told us exactly what to look at. The id may be a pipeline id OR an
	// execution id; resolve to the owning pipeline either way.
	if m := reDiagWithUUID.FindStringSubmatch(msg); len(m) == 2 {
		pipelineID, execID, name := resolveDiagnosisTargetFromID(database, m[1], workspaceID)
		if pipelineID == "" {
			return ChatMessageResponse{
				Message:   fmt.Sprintf("I couldn't find a pipeline or execution `%s` for your account. Double-check the id?", m[1]),
				Type:      "text",
				TraceID:   traceID,
				Timestamp: time.Now().Format(time.RFC3339),
			}, true
		}
		return h.buildDiagnosisResponse(ctx, database, pipelineID, execID, name, workspaceID, traceID, "explicit-id"), true
	}

	// Pattern 2: a diagnosis-shaped question without an id — the common
	// self-serve case. Covers "why did my last run fail", "diagnose",
	// "check issues", "what's wrong", "0 rows", "stuck", "not working".
	// Resolve the user's most relevant pipeline (failed OR running first) and
	// run the rich diagnosis. `explicit` = the user clearly asked to diagnose,
	// so we own the reply even when there's nothing to look at; a vague broad
	// match with no pipeline falls back to the general chat router.
	explicit := reDiagLast.MatchString(msg) || reDiagGeneric.MatchString(msg)
	if explicit || reDiagBroad.MatchString(msg) {
		pipelineID, execID, name := findMostRelevantPipeline(database, workspaceID)
		if pipelineID == "" {
			if explicit {
				return ChatMessageResponse{
					Message:   "I couldn't find a pipeline to diagnose yet — create and run one first, or paste its id and I'll dig in.",
					Type:      "text",
					TraceID:   traceID,
					Timestamp: time.Now().Format(time.RFC3339),
				}, true
			}
			return ChatMessageResponse{}, false // let the LLM router handle it
		}
		return h.buildDiagnosisResponse(ctx, database, pipelineID, execID, name, workspaceID, traceID, "auto"), true
	}

	return ChatMessageResponse{}, false
}

// resolveDiagnosisTargetFromID accepts a UUID the user pasted and resolves the
// owning pipeline — the UUID may be a pipeline id OR an execution id. Returns
// ("","","") when neither matches in the active workspace (tenant-safe: scoped
// to workspace_id). execID is the pasted execution id, or the pipeline's latest.
func resolveDiagnosisTargetFromID(database *sql.DB, id, workspaceID string) (pipelineID, execID, pipelineName string) {
	var pName sql.NullString
	// Try as a pipeline id first.
	if err := database.QueryRow(`
		SELECT p.id, COALESCE(p.name,'')
		FROM pipelines p
		WHERE p.id = $1 AND p.workspace_id = $2
	`, id, workspaceID).Scan(&pipelineID, &pName); err == nil && pipelineID != "" {
		pipelineName = pName.String
		_ = database.QueryRow(`SELECT id FROM executions WHERE pipeline_id = $1 ORDER BY start_time DESC LIMIT 1`, pipelineID).Scan(&execID)
		return pipelineID, execID, pipelineName
	}
	// Otherwise try as an execution id.
	if err := database.QueryRow(`
		SELECT e.pipeline_id, e.id, COALESCE(p.name,'')
		FROM executions e
		JOIN pipelines p ON p.id = e.pipeline_id
		WHERE e.id = $1 AND p.workspace_id = $2
	`, id, workspaceID).Scan(&pipelineID, &execID, &pName); err == nil && pipelineID != "" {
		return pipelineID, execID, pName.String
	}
	return "", "", ""
}

// findMostRelevantPipeline picks the pipeline a user most likely wants
// diagnosed when they don't name one: a failed/errored pipeline first, then a
// running/streaming one (which may be silently stuck), then the most recently
// updated. Crucially this includes LIVE pipelines (status running, no terminal
// execution) — the #1 real-world "0 rows / stuck" case the old execution-only
// lookup could never surface. execID is the latest execution if any (may be "").
func findMostRelevantPipeline(database *sql.DB, workspaceID string) (pipelineID, execID, pipelineName string) {
	var pName sql.NullString
	err := database.QueryRow(`
		SELECT p.id, COALESCE(p.name,'')
		FROM pipelines p
		WHERE p.workspace_id = $1
		ORDER BY
			CASE
				WHEN lower(COALESCE(p.status,'')) IN ('failed','error','silent_drop_detected','silent_partial_drop_detected') THEN 0
				WHEN lower(COALESCE(p.status,'')) IN ('running','streaming','active','processing','pending','stopped') THEN 1
				ELSE 2
			END,
			p.updated_at DESC
		LIMIT 1
	`, workspaceID).Scan(&pipelineID, &pName)
	if err != nil || pipelineID == "" {
		return "", "", ""
	}
	pipelineName = pName.String
	_ = database.QueryRow(`SELECT id FROM executions WHERE pipeline_id = $1 ORDER BY start_time DESC LIMIT 1`, pipelineID).Scan(&execID)
	return pipelineID, execID, pipelineName
}

// buildDiagnosisResponse runs the RICH diagnosis for a pipeline: it gathers the
// same evidence the on-call POST /pipelines/:id/diagnose endpoint uses (progress
// + blocking reason, dependency health, latest execution error, sink write
// errors, SigNoz logs, deterministic flow), asks llm-service for a root-cause
// hypothesis, AND runs the deterministic rule diagnoser as a fast, always-
// available fallback. Works for failed AND live (running/stuck) pipelines.
//
// Privacy: the LLM only ever sees the SCRUBBED evidence (gatherDiagnosisEvidence
// scrubs every free-text field). The raw error is used solely by the in-process
// rule matcher and never leaves the box.
func (h *ChatHandler) buildDiagnosisResponse(
	ctx context.Context, database *sql.DB,
	pipelineID, execID, pipelineName, workspaceID, traceID, matchSource string,
) ChatMessageResponse {
	now := time.Now().Format(time.RFC3339)

	// Tenancy guard (defense in depth — resolvers already scoped to the workspace).
	if !diagnosisPipelineInWorkspace(database, pipelineID, workspaceID) {
		return ChatMessageResponse{
			Message:   "I couldn't find that pipeline for your account.",
			Type:      "text",
			TraceID:   traceID,
			Timestamp: now,
		}
	}

	// Rich evidence — reads pipeline_progress (incl. blocking reason),
	// dependency health, latest execution error, table stats, and the
	// deterministic flow. Works for live pipelines, not just terminal runs.
	evidence, ok := gatherDiagnosisEvidence(database, pipelineID)
	if !ok {
		return ChatMessageResponse{
			Message:   "I couldn't load that pipeline to diagnose it.",
			Type:      "text",
			TraceID:   traceID,
			Timestamp: now,
		}
	}
	if flow, ok := evidence["flow"].(map[string]interface{}); ok {
		enrichFlowWithSigNozLogs(ctx, flow)
	}
	// Surface the real sink write error (missing PK / ON CONFLICT, etc.) when
	// rows were read but none landed.
	enrichEvidenceWithSinkErrors(ctx, pipelineID, evidence)

	if pipelineName == "" {
		pipelineName = evidenceStr(evidence, "pipeline", "name")
	}
	if execID == "" {
		if eid, ok := evidence["execution_id"].(string); ok {
			execID = eid
		}
	}

	// Deterministic rule verdict from the RAW error (local only — never sent to
	// the LLM). Gives a failure category + a UI action button instantly, and is
	// the fallback answer when llm-service is unavailable.
	signal := diagnosisSignalFromEvidence(database, pipelineID, evidence)
	dx := diagnose.New().Diagnose(signal)

	// Map the diagnosis to a UI button. Closed enum — the UI validates the
	// action string before rendering, so a bogus value renders nothing.
	actionLabel, deepLink := actionToButton(dx.SuggestedAction, pipelineID, execID)

	// LLM root-cause hypothesis over the SCRUBBED evidence (fail-soft: if
	// llm-service is down we still answer with the rule + evidence).
	var llm *DiagnoseResponse
	if v, _, err := callDiagnoseLLM(ctx, pipelineID, evidence); err == nil {
		llm = v
	} else {
		log.WithError(err).Warn("chat diagnose: LLM hypothesis unavailable; using rule + evidence")
	}

	data := map[string]interface{}{
		"execution_id":                execID,
		"pipeline_id":                 pipelineID,
		"pipeline_name":               pipelineName,
		"status":                      evidenceStr(evidence, "pipeline", "status"),
		"category":                    string(dx.Category),
		"action":                      string(dx.SuggestedAction),
		"action_label":                actionLabel,
		"deep_link":                   deepLink,
		"confidence":                  dx.Confidence,
		"rationale":                   dx.Rationale,
		"match_source":                matchSource,
		"source_rows":                 signal.SourceRowCount,
		"landed_rows":                 signal.WrittenRows,
		"flow":                        evidence["flow"],
		"progress":                    evidence["progress"],
		"dependency_health_aggregate": evidence["dependency_health_aggregate"],
		"table_stats_summary":         evidence["table_stats_summary"],
	}
	if llm != nil {
		data["llm_summary"] = llm.Summary
		data["llm_root_cause"] = llm.RootCause
		data["llm_suggested_action"] = llm.SuggestedAction
		data["llm_confidence"] = llm.Confidence
		data["evidence_pointers"] = llm.EvidencePointers
	}

	return ChatMessageResponse{
		Message:   composeDiagnosisBody(llm, dx.Rationale, signal, evidence),
		Type:      "diagnosis",
		TraceID:   traceID,
		Timestamp: now,
		Data:      data,
	}
}

// diagnosisPipelineInWorkspace confirms the pipeline lives in the caller's
// active workspace.
func diagnosisPipelineInWorkspace(database *sql.DB, pipelineID, workspaceID string) bool {
	var one int
	return database.QueryRow(`SELECT 1 FROM pipelines WHERE id = $1 AND workspace_id = $2`, pipelineID, workspaceID).Scan(&one) == nil
}

// diagnosisSignalFromEvidence builds the rule diagnoser's Signal from the RAW
// (unscrubbed) latest-execution error plus the pipeline's connector types and
// row counts. The raw error is used only for local rule matching — it is never
// forwarded to the LLM, which sees the scrubbed evidence map instead.
func diagnosisSignalFromEvidence(database *sql.DB, pipelineID string, evidence map[string]interface{}) diagnose.Signal {
	var rawErr, execStatus, srcType, dstType sql.NullString
	_ = database.QueryRow(`
        SELECT e.error_message, e.status,
               COALESCE(src.connector_type,''), COALESCE(dst.connector_type,'')
        FROM executions e
        JOIN pipelines p ON p.id = e.pipeline_id
        LEFT JOIN connections src ON src.id = p.source_connection_id
        LEFT JOIN connections dst ON dst.id = p.destination_connection_id
        WHERE e.pipeline_id = $1
        ORDER BY e.start_time DESC LIMIT 1
    `, pipelineID).Scan(&rawErr, &execStatus, &srcType, &dstType)

	rawErrS := rawErr.String
	status := execStatus.String
	switch {
	case strings.HasPrefix(rawErrS, "silent_drop_detected"):
		status = "silent_drop_detected"
	case strings.HasPrefix(rawErrS, "silent_partial_drop_detected"):
		status = "silent_partial_drop_detected"
	case strings.HasPrefix(rawErrS, "waiting_for_credential_reauth"):
		status = "waiting_for_credential_reauth"
	case strings.HasPrefix(rawErrS, "waiting_for_credential_scope"):
		status = "waiting_for_credential_scope"
	}
	if status == "" {
		status = evidenceStr(evidence, "pipeline", "status")
	}
	src := srcType.String
	if src == "" {
		src = evidenceStr(evidence, "pipeline", "source_type")
	}
	dst := dstType.String
	if dst == "" {
		dst = evidenceStr(evidence, "pipeline", "destination_type")
	}

	var readRows, landedRows sql.NullInt64
	_ = database.QueryRow(`
        SELECT COALESCE(SUM(COALESCE(read_rows,0)),0),
               COALESCE(SUM(COALESCE(inserted_rows,0) + COALESCE(applied_inserts,0)),0)
        FROM pipeline_run_table_stats WHERE pipeline_id = $1
    `, pipelineID).Scan(&readRows, &landedRows)

	return diagnose.Signal{
		ErrorMessage:    rawErrS,
		ExecutorStatus:  status,
		Stage:           "executor",
		SourceType:      src,
		DestinationType: dst,
		WrittenRows:     landedRows.Int64,
		SourceRowCount:  readRows.Int64,
	}
}

// composeDiagnosisBody writes the human-facing chat message. Prefers the LLM's
// plain-language root cause; falls back to the deterministic rule rationale plus
// the most telling evidence (blocking stage, row counts) when the LLM is down.
func composeDiagnosisBody(llm *DiagnoseResponse, ruleRationale string, signal diagnose.Signal, evidence map[string]interface{}) string {
	if llm != nil && strings.TrimSpace(llm.RootCause) != "" {
		var b strings.Builder
		if s := strings.TrimSpace(llm.Summary); s != "" {
			b.WriteString(s)
			b.WriteString("\n\n")
		}
		b.WriteString("**Likely cause:** ")
		b.WriteString(strings.TrimSpace(llm.RootCause))
		if a := strings.TrimSpace(llm.SuggestedAction); a != "" {
			b.WriteString("\n\n**What to do:** ")
			b.WriteString(a)
		}
		return b.String()
	}

	var b strings.Builder
	b.WriteString("Here's what I found from the pipeline's runtime state:\n")
	if r := strings.TrimSpace(ruleRationale); r != "" {
		b.WriteString("- ")
		b.WriteString(r)
		b.WriteString("\n")
	}
	if blk := evidenceStr(evidence, "progress", "blocking_reason"); blk != "" {
		b.WriteString("- Blocked at: ")
		b.WriteString(blk)
		if desc := evidenceStr(evidence, "progress", "blocking_description"); desc != "" {
			b.WriteString(" — ")
			b.WriteString(desc)
		}
		b.WriteString("\n")
	}
	b.WriteString(fmt.Sprintf("- Rows: %d read → %d landed\n", signal.SourceRowCount, signal.WrittenRows))
	return b.String()
}

// evidenceStr safely reads evidence[outer][inner] as a string.
func evidenceStr(evidence map[string]interface{}, outer, inner string) string {
	if m, ok := evidence[outer].(map[string]interface{}); ok {
		if s, ok := m[inner].(string); ok {
			return s
		}
	}
	return ""
}

// actionToButton maps the diagnose.Action enum to a UI-renderable
// (label, deep_link) pair. Returns ("", "") for actions that should
// not surface a button (escalate, no_op).
func actionToButton(a diagnose.Action, pipelineID, execID string) (string, string) {
	switch a {
	case diagnose.ActionRefreshAuth:
		// Take them to the pipeline page where they can re-auth the source.
		return "Reconnect source credential", fmt.Sprintf("/pipelines/%s", pipelineID)
	case diagnose.ActionRegenerateConnector:
		return "Regenerate connector (HITL)", fmt.Sprintf("/pipelines/%s", pipelineID)
	case diagnose.ActionBackoffRetry:
		return "Retry pipeline", fmt.Sprintf("/pipelines/%s", pipelineID)
	case diagnose.ActionRequestUserConfig:
		return "Fix pipeline configuration", fmt.Sprintf("/pipelines/%s", pipelineID)
	case diagnose.ActionEscalate, diagnose.ActionNoOp:
		return "", ""
	}
	return "", ""
}

// DiagnoseExecutionByID is a thin HTTP wrapper exposing the same logic
// as a REST endpoint. The chat handler is the primary surface, but
// having a JSON endpoint makes it easy to wire from the Execution
// Detail page's "Diagnose" button without going through chat.
func DiagnoseExecutionByID(c *gin.Context) {
	id := c.Param("id")
	workspaceID := activeWorkspaceID(c)
	if id == "" || workspaceID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing id or workspace"})
		return
	}
	database := db.GetDB()
	pipelineID, execID, name := resolveDiagnosisTargetFromID(database, id, workspaceID)
	if pipelineID == "" {
		c.JSON(http.StatusNotFound, gin.H{"error": "pipeline or execution not found"})
		return
	}
	h := &ChatHandler{}
	traceID, _ := c.Get("trace_id")
	traceIDStr, _ := traceID.(string)
	resp := h.buildDiagnosisResponse(c.Request.Context(), database, pipelineID, execID, name, workspaceID, traceIDStr, "rest-api")
	c.JSON(http.StatusOK, resp)
}
