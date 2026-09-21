package handlers

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"api-gateway/internal/db"
	"api-gateway/internal/security"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type UpdateCDCTablesRequest struct {
	Tables []string `json:"tables" binding:"required"`
	// If true, trigger a backfill (Debezium ad-hoc snapshot) for newly added tables.
	// This is DMS-like behavior: "load existing rows, then keep streaming".
	BackfillNewlyAdded bool `json:"backfill_newly_added"`
	// BackfillMode can be "incremental" (default) or "blocking". Only used when BackfillNewlyAdded=true.
	BackfillMode string `json:"backfill_mode"`
}

func kafkaConnectURL() string {
	v := strings.TrimSpace(os.Getenv("KAFKA_CONNECT_URL"))
	if v == "" {
		return "http://kafka-connect:8083"
	}
	return v
}

// UpdatePipelineCDCTables updates Debezium table.include.list for a pipeline's connector.
// POST /api/v1/pipelines/:id/cdc/tables
func UpdatePipelineCDCTables(c *gin.Context) {
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}

	pipelineID := strings.TrimSpace(c.Param("id"))
	if _, err := uuid.Parse(pipelineID); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Pipeline not found"})
		return
	}

	// RBAC: mutating a pipeline's CDC table selection requires at least `member`
	// in the active workspace — membership alone is not enough (viewers are
	// read-only). requireResourceRole proves the pipeline lives in the active
	// workspace and the caller holds >= member there.
	if _, ok := requirePipelineWorkspaceRole(c, pipelineID, security.WSMember); !ok {
		return
	}

	var req UpdateCDCTablesRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		respondError(c, http.StatusBadRequest, "invalid_request", "Invalid request payload", err)
		return
	}
	tables := make([]string, 0, len(req.Tables))
	for _, t := range req.Tables {
		v := strings.TrimSpace(t)
		if v == "" {
			continue
		}
		tables = append(tables, v)
	}
	if len(tables) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "tables must be non-empty"})
		return
	}
	// Keep the selection as the user expressed it: resolution below replaces
	// `tables` with the expansion, and the sentinel is what the auto-pickup
	// watcher needs to re-apply later.
	rawTables := append([]string(nil), tables...)

	// Expand any "select entire database" ("*") / "select entire namespace"
	// ("<ns>.*") sentinel into an explicit list before diffing/persisting so the
	// Debezium include-list and the SQL Server/Oracle per-table CDC provisioning
	// receive real table names, never a raw sentinel.
	if resolved, _, rerr := resolveSelectionForPipeline(c, pipelineSourceConnectionID(pipelineID), tables); rerr != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": "table_resolution_failed", "message": rerr.Error()})
		return
	} else {
		tables = resolved
	}

	// Read previous persisted selection (best-effort) to compute "newly added tables".
	prevTables := []string{}
	prevText := ""
	// jsonb -> text yields e.g. ["db.table"].
	if err := database.QueryRow(
		`SELECT COALESCE(config->'selected_tables','[]'::jsonb)::text FROM pipelines WHERE id = $1::uuid`,
		pipelineID,
	).Scan(&prevText); err == nil && strings.TrimSpace(prevText) != "" {
		_ = json.Unmarshal([]byte(prevText), &prevTables)
	}
	prevSet := make(map[string]struct{}, len(prevTables))
	for _, t := range prevTables {
		v := strings.TrimSpace(t)
		if v != "" {
			prevSet[v] = struct{}{}
		}
	}
	newTables := make([]string, 0)
	for _, t := range tables {
		if _, ok := prevSet[t]; !ok {
			newTables = append(newTables, t)
		}
	}

	// Find connector name (best-effort heuristics).
	//
	// A pipeline that never finished CDC provisioning — e.g. one blocked on day
	// one by a table without a PRIMARY KEY, `cdc_resources` empty — genuinely has
	// no Debezium connector, and this lookup legitimately comes back empty. That
	// must NOT sink the whole request: unchecking the offending table is exactly
	// the corrective action such a pipeline needs, and hard-failing 404 here threw
	// the corrected list away before it could be persisted, leaving no
	// self-service recovery path at all (KI-CDC-EDIT-TABLES-NO-CONNECTOR).
	//
	// So the two failure modes are separated: "no connector exists yet" saves the
	// selection and reports pending_provision, while "Kafka Connect could not be
	// reached" still fails — in that case we cannot tell whether a live connector
	// exists, and persisting a list we never pushed would silently desync it.
	connectorName, connErr := findDebeziumConnectorName(pipelineID)
	switch {
	case connErr == nil:
		// Live connector: fall through to the normal update path below.
	case errors.Is(connErr, errCDCConnectorNotProvisioned):
		if err := persistSelectedTables(database, pipelineID, tables); err != nil {
			respondError(c, http.StatusInternalServerError, "persist_failed",
				"Failed to save the CDC table selection", err)
			return
		}
		if err := persistTableSelectionRule(database, pipelineID, rawTables); err != nil {
			log.WithError(err).WithField("pipeline_id", pipelineID).Warn("Failed to persist table_selection_rule (ignored)")
		}
		log.WithFields(log.Fields{
			"pipeline_id": pipelineID,
			"tables":      len(tables),
		}).Info("CDC table selection saved ahead of connector provisioning")
		resp := gin.H{
			"success":           true,
			"pipeline_id":       pipelineID,
			"pending_provision": true,
			"tables":            tables,
			"new_tables":        newTables,
			"message":           "Table selection saved. This pipeline has no Debezium connector yet, so the list will be applied when CDC is provisioned.",
		}
		if req.BackfillNewlyAdded {
			// Nothing to snapshot: the connector that would run the ad-hoc
			// snapshot doesn't exist. Initial provisioning loads these tables.
			resp["backfill"] = gin.H{
				"requested": true,
				"success":   false,
				"tables":    newTables,
				"error":     "no Debezium connector yet — newly added tables load during initial CDC provisioning",
			}
		}
		c.JSON(http.StatusOK, resp)
		return
	default:
		respondError(c, http.StatusBadGateway, "connect_unreachable",
			"Kafka Connect is unreachable, so the CDC table list was not changed", connErr)
		return
	}

	// IMPORTANT: Route updates via orchestrator so it can enforce P0 safety guards
	// (e.g., hard PK validation for relational destinations).
	{
		status, body, perr := pushCDCTableList(c.Request.Context(), pipelineID, tables)
		if perr != nil {
			respondError(c, http.StatusBadGateway, "orchestrator_unreachable", "Orchestrator is unreachable", perr)
			return
		}
		if status < 200 || status >= 300 {
			log.WithFields(log.Fields{
				"pipeline_id":    pipelineID,
				"connector_name": connectorName,
				"status_code":    status,
			}).Warn("Orchestrator rejected CDC table update")
			c.Data(status, "application/json", body)
			return
		}
	}

	// Persist selection on the pipeline as well (so scheduled/manual runs can reuse it consistently).
	// (Non-blocking here: the Debezium update above is the authoritative side-effect.)
	if err := persistSelectedTables(database, pipelineID, tables); err != nil {
		log.WithError(err).WithField("pipeline_id", pipelineID).Warn("Failed to persist selected_tables (ignored)")
	}
	// Record the RULE behind the selection, not just its expansion, so the CDC
	// auto-pickup watcher keeps this pipeline current. rawTables is the request
	// as sent (pre-expansion): an exact list writes an empty rule, which is how
	// narrowing a whole-database pipeline turns auto-pickup back off.
	if err := persistTableSelectionRule(database, pipelineID, rawTables); err != nil {
		log.WithError(err).WithField("pipeline_id", pipelineID).Warn("Failed to persist table_selection_rule (ignored)")
	}

	// Optionally trigger backfill for newly added tables.
	backfill := gin.H{
		"requested": req.BackfillNewlyAdded,
		"mode":      strings.TrimSpace(req.BackfillMode),
		"tables":    newTables,
		"success":   false,
	}
	if req.BackfillNewlyAdded && len(newTables) > 0 {
		backfill = requestCDCBackfill(c.Request.Context(), pipelineID, newTables, req.BackfillMode)
	} else if req.BackfillNewlyAdded && len(newTables) == 0 {
		// Nothing new to backfill.
		backfill["success"] = true
	}

	// Best-effort: restart sink worker so newly-added table topics are applied (not just captured).
	// Without this, the sink consumer group may remain subscribed to the old topic list and
	// "Applied Inserts" stays at 0 for new tables.
	sinkRestart := restartCDCSink(c.Request.Context(), pipelineID)

	c.JSON(http.StatusOK, gin.H{
		"success":        true,
		"pipeline_id":    pipelineID,
		"connector_name": connectorName,
		"tables":         tables,
		"new_tables":     newTables,
		"backfill":       backfill,
		"sink_restart":   sinkRestart,
		"message":        "CDC tables updated (Debezium connector will restart automatically).",
	})
}

// persistSelectedTables writes the pipeline's desired CDC table list to
// `config.selected_tables`. Split out because it is now reached from two places
// with different severities: after a successful Debezium push it is a best-effort
// mirror of an already-applied change, while on the pending-provision path it is
// the entire point of the request and its failure must surface.
func persistSelectedTables(database *sql.DB, pipelineID string, tables []string) error {
	b, err := json.Marshal(tables)
	if err != nil {
		return fmt.Errorf("failed to encode table selection: %w", err)
	}
	if _, err := database.Exec(`
		UPDATE pipelines
		SET config = jsonb_set(COALESCE(config, '{}'::jsonb), '{selected_tables}', $1::jsonb, true),
		    updated_at = NOW()
		WHERE id = $2::uuid
	`, string(b), pipelineID); err != nil {
		return err
	}
	return nil
}

// errCDCConnectorNotProvisioned means Kafka Connect answered normally and simply
// has no connector for this pipeline — distinct from "Kafka Connect could not be
// reached", where the answer is unknown. Callers must not conflate the two: the
// first is a legitimate state for a pipeline that never provisioned, the second
// is an outage.
var errCDCConnectorNotProvisioned = errors.New("debezium connector not provisioned for pipeline")

func findDebeziumConnectorName(pipelineID string) (string, error) {
	connectURL := kafkaConnectURL()
	httpClient := &http.Client{Timeout: 10 * time.Second}

	// GET /connectors -> []string
	resp, err := httpClient.Get(connectURL + "/connectors")
	if err != nil {
		return "", fmt.Errorf("failed to reach kafka connect: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("kafka connect error (status %d): %s", resp.StatusCode, string(body))
	}

	var names []string
	if err := json.NewDecoder(resp.Body).Decode(&names); err != nil {
		return "", fmt.Errorf("failed to decode connector list: %w", err)
	}

	short := pipelineID
	if len(short) > 8 {
		short = short[:8]
	}

	for _, n := range names {
		ln := strings.ToLower(strings.TrimSpace(n))
		if ln == "" {
			continue
		}
		// Our planner commonly uses: cdc-{tenant}-{pipeline_id}
		if strings.Contains(ln, strings.ToLower(pipelineID)) || strings.Contains(ln, strings.ToLower(short)) {
			return n, nil
		}
	}

	return "", errCDCConnectorNotProvisioned
}

// The three calls below are the side-effects of changing a CDC table list, in
// the order they must happen: push the list (the orchestrator revalidates and
// rewrites Debezium's table.include.list), snapshot whatever is newly added,
// then restart the sink so it subscribes to the new topics. They are extracted
// because the CDC auto-pickup watcher performs the very same sequence on a
// schedule; keeping two copies is how a fix lands in one path and not the other.

// pushCDCTableList sends the desired table list to the orchestrator, which owns
// the P0 safety guards (hard PK validation for relational destinations) before
// it touches the connector. The orchestrator's status and body are returned
// verbatim so a caller can forward its error to the user, or read the
// structured rejection (see cdcMissingPrimaryKeyTables) and react to it.
func pushCDCTableList(ctx context.Context, pipelineID string, tables []string) (int, []byte, error) {
	payload, _ := json.Marshal(gin.H{
		"pipeline_id": pipelineID,
		"tables":      tables,
	})
	url := fmt.Sprintf("%s/api/v1/cdc/tables", orchestratorBaseURL())
	r, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(payload))
	if err != nil {
		return 0, nil, err
	}
	r.Header.Set("Content-Type", "application/json")
	setInternalServiceSecret(r)
	resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(r)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return resp.StatusCode, body, nil
}

// cdcMissingPrimaryKeyTables reads the orchestrator's structured PK rejection
// ({"error":"missing_primary_key","tables":[…]}) and returns the offending
// tables. A rejection in any other shape returns nil: the caller must then treat
// the failure as opaque rather than guess which table caused it.
func cdcMissingPrimaryKeyTables(status int, body []byte) []string {
	if status != http.StatusBadRequest {
		return nil
	}
	var parsed struct {
		Error  string   `json:"error"`
		Tables []string `json:"tables"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil
	}
	if parsed.Error != "missing_primary_key" {
		return nil
	}
	out := make([]string, 0, len(parsed.Tables))
	for _, t := range parsed.Tables {
		if s := strings.TrimSpace(t); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// requestCDCBackfill asks the orchestrator for an ad-hoc Debezium snapshot of
// the given tables — the "load what is already there, then keep streaming"
// half of adding a table to a running CDC pipeline. The returned map is the
// caller's `backfill` report; a failure is described in it rather than
// returned, because the table list has already been applied by then and the
// caller must not undo it.
func requestCDCBackfill(ctx context.Context, pipelineID string, tables []string, mode string) gin.H {
	mode = strings.TrimSpace(mode)
	if mode == "" {
		mode = "incremental"
	}
	result := gin.H{
		"requested": true,
		"mode":      mode,
		"tables":    tables,
		"success":   false,
	}
	payload, _ := json.Marshal(gin.H{
		"tables": tables,
		"mode":   mode,
	})
	url := fmt.Sprintf("%s/api/v1/cdc/pipelines/%s/backfill", orchestratorBaseURL(), pipelineID)
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		result["error"] = err.Error()
		return result
	}
	r.Header.Set("Content-Type", "application/json")
	setInternalServiceSecret(r)
	resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(r)
	if err != nil {
		result["error"] = err.Error()
		return result
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	result["status_code"] = resp.StatusCode
	result["response"] = json.RawMessage(body)
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		result["success"] = true
	} else {
		result["error"] = fmt.Sprintf("orchestrator backfill failed (status %d)", resp.StatusCode)
	}
	return result
}

// restartCDCSink restarts the pipeline's sink worker so newly added table
// topics are actually consumed. Without it the sink's consumer group stays
// subscribed to the old topic list and "Applied Inserts" sits at 0 for every
// new table while Debezium happily captures them.
func restartCDCSink(ctx context.Context, pipelineID string) gin.H {
	result := gin.H{
		"requested": true,
		"success":   false,
	}
	url := fmt.Sprintf("%s/api/v1/cdc/pipelines/%s/sink/restart", orchestratorBaseURL(), pipelineID)
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader([]byte(`{}`)))
	if err != nil {
		result["error"] = err.Error()
		return result
	}
	r.Header.Set("Content-Type", "application/json")
	setInternalServiceSecret(r)
	resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(r)
	if err != nil {
		result["error"] = err.Error()
		return result
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	result["status_code"] = resp.StatusCode
	result["response"] = json.RawMessage(body)
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		result["success"] = true
	} else {
		result["error"] = fmt.Sprintf("orchestrator sink restart failed (status %d)", resp.StatusCode)
	}
	return result
}
