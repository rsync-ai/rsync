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
	// BackfillMode can be "incremental" or "blocking"; empty lets the orchestrator
	// pick the engine's default (incremental; blocking on MongoDB, which refuses
	// incremental). Only used when BackfillNewlyAdded=true.
	BackfillMode string `json:"backfill_mode"`
}

// cdcStreamingOnlyTablesKey is the pipelines.config key listing the CDC tables that
// were added without loading their existing rows: only changes made after the add
// reach the destination. UpdatePipelineCDCTables and BackfillPipelineCDCTables keep
// it (updateCDCStreamingOnlyTables); the table-stats response reports it as
// load_mode "streaming_only".
const cdcStreamingOnlyTablesKey = "cdc_streaming_only_tables"

// Snapshot request sources the orchestrator records on cdc_snapshot_requests.source.
const (
	cdcSnapshotSourceTableEdit  = "table_edit"
	cdcSnapshotSourceAutoPickup = "auto_pickup"
)

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
	tableSet := make(map[string]struct{}, len(tables))
	for _, t := range tables {
		tableSet[t] = struct{}{}
		if _, ok := prevSet[t]; !ok {
			newTables = append(newTables, t)
		}
	}
	removedTables := make([]string, 0)
	for _, t := range prevTables {
		v := strings.TrimSpace(t)
		if _, ok := tableSet[v]; v != "" && !ok {
			removedTables = append(removedTables, v)
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
	// (e.g., hard PK validation for database destinations, MongoDB included).
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
			c.Data(browserStatusForUpstream(status), "application/json", body)
			return
		}
		// BUG #6: the diff above is against the SAVED list, which lags the connector
		// whenever a save failed or the auto-pickup watcher added tables. When the
		// orchestrator read the connector's live include-list, its diff is the truth.
		if added, removed, ok := cdcTableListLiveDiff(body); ok {
			newTables, removedTables = added, removed
		}
	}

	warnings := make([]string, 0)

	// Persist selection on the pipeline as well (so scheduled/manual runs can reuse it consistently).
	// Not fatal: the list is already live on the connector and must not be undone, but the
	// caller must hear that the saved copy is stale.
	if err := persistSelectedTables(database, pipelineID, tables); err != nil {
		log.WithError(err).WithField("pipeline_id", pipelineID).Error("CDC table list applied to the connector but selected_tables could not be saved")
		warnings = append(warnings, "The table list was applied to the connector but could not be saved; "+
			"save it again, or the pipeline's saved selection stays on the previous list.")
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
		backfill = requestCDCBackfill(c.Request.Context(), pipelineID, newTables, req.BackfillMode, cdcSnapshotSourceTableEdit)
	} else if req.BackfillNewlyAdded && len(newTables) == 0 {
		// Nothing new to backfill.
		backfill["success"] = true
	}

	// BUG #5/#9: record which tables stream without their existing rows. An added table
	// whose backfill was not requested — or was refused — is streaming-only; a backfilled
	// or removed table is not (a re-added one starts over).
	streamingOnly := newTables
	drop := removedTables
	if ok, _ := backfill["success"].(bool); ok && req.BackfillNewlyAdded {
		streamingOnly = nil
		drop = append(append([]string(nil), removedTables...), newTables...)
	}
	if err := updateCDCStreamingOnlyTables(database, pipelineID, streamingOnly, drop); err != nil {
		log.WithError(err).WithField("pipeline_id", pipelineID).Warn("Failed to update cdc_streaming_only_tables (ignored)")
	}
	// BUG #5: a table removed and re-added missed every change made in between, and
	// streaming alone never repairs that. Its old stats row is the proof it streamed before.
	for _, t := range cdcTablesWithStatsRows(database, pipelineID, streamingOnly) {
		warnings = append(warnings, t+" was streamed before and removed; changes made while it was removed are not streamed, "+
			"so without loading its existing rows those rows stay missing or stale at the destination.")
	}

	// Best-effort: restart sink worker so newly-added table topics are applied (not just captured).
	// Without this, the sink consumer group may remain subscribed to the old topic list and
	// "Applied Inserts" stays at 0 for new tables.
	sinkRestart := restartCDCSink(c.Request.Context(), pipelineID)

	paused := pipelineIsPaused(database, pipelineID)
	c.JSON(http.StatusOK, gin.H{
		"success":        true,
		"pipeline_id":    pipelineID,
		"connector_name": connectorName,
		"tables":         tables,
		"new_tables":     newTables,
		"removed_tables": removedTables,
		"backfill":       backfill,
		"sink_restart":   sinkRestart,
		"warnings":       warnings,
		"paused":         paused,
		"message":        cdcTableEditMessage(newTables, removedTables, paused),
	})
}

// cdcTableListLiveDiff reads the orchestrator's update-tables answer. ok is true only
// when it read the connector's live include-list (live_list_read); added/removed are
// then its diff against that list. Otherwise the caller keeps its saved-list diff.
func cdcTableListLiveDiff(body []byte) (added, removed []string, ok bool) {
	var ack struct {
		LiveListRead bool     `json:"live_list_read"`
		Added        []string `json:"added"`
		Removed      []string `json:"removed"`
	}
	if err := json.Unmarshal(body, &ack); err != nil || !ack.LiveListRead {
		return nil, nil, false
	}
	return trimmedNonEmpty(ack.Added), trimmedNonEmpty(ack.Removed), true
}

// trimmedNonEmpty returns the trimmed non-empty entries, never nil (a JSON [] not null).
func trimmedNonEmpty(in []string) []string {
	out := make([]string, 0, len(in))
	for _, v := range in {
		if s := strings.TrimSpace(v); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// cdcTableEditMessage is the one-line outcome of a table edit (BUG #16/#17): removals
// used to go unmentioned, and a paused pipeline was told its connector would restart.
func cdcTableEditMessage(added, removed []string, paused bool) string {
	parts := make([]string, 0, 2)
	if len(added) > 0 {
		parts = append(parts, fmt.Sprintf("%d added", len(added)))
	}
	if len(removed) > 0 {
		parts = append(parts, fmt.Sprintf("%d removed and no longer streamed", len(removed)))
	}
	msg := "CDC tables updated"
	if len(parts) > 0 {
		msg += ": " + strings.Join(parts, ", ")
	}
	msg += "."
	switch {
	case paused && len(added) > 0:
		msg += " The pipeline is paused: the added tables start streaming when it is resumed."
	case !paused:
		msg += " The Debezium connector restarts to apply the change."
	}
	return msg
}

// pipelineIsPaused reports pipelines.status = 'paused'. Best-effort: false on error.
func pipelineIsPaused(database *sql.DB, pipelineID string) bool {
	var status string
	if err := database.QueryRow(
		`SELECT COALESCE(status, '') FROM pipelines WHERE id = $1::uuid`, pipelineID,
	).Scan(&status); err != nil {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(status), "paused")
}

// cdcTablesWithStatsRows returns those of tables that already have a
// pipeline_run_table_stats row for this pipeline, in the order given. Best-effort:
// nil on error.
func cdcTablesWithStatsRows(database *sql.DB, pipelineID string, tables []string) []string {
	if len(tables) == 0 {
		return nil
	}
	b, err := json.Marshal(tables)
	if err != nil {
		return nil
	}
	rows, err := database.Query(`
		SELECT DISTINCT qualified_name
		FROM pipeline_run_table_stats
		WHERE pipeline_id = $1::uuid
		  AND qualified_name IN (SELECT jsonb_array_elements_text($2::jsonb))
	`, pipelineID, string(b))
	if err != nil {
		log.WithError(err).WithField("pipeline_id", pipelineID).Warn("Failed to look up earlier CDC stats rows (ignored)")
		return nil
	}
	defer rows.Close()
	seen := make(map[string]bool, len(tables))
	for rows.Next() {
		var qn string
		if rows.Scan(&qn) == nil {
			seen[qn] = true
		}
	}
	out := make([]string, 0, len(seen))
	for _, t := range tables {
		if seen[t] {
			out = append(out, t)
		}
	}
	return out
}

// updateCDCStreamingOnlyTables adds `add` to, and takes `drop` out of,
// pipelines.config->'cdc_streaming_only_tables' in ONE statement, so two concurrent
// edits cannot lose each other's change the way a Go read-modify-write could. A table
// in both lists ends up out. A non-array value is treated as empty.
func updateCDCStreamingOnlyTables(database *sql.DB, pipelineID string, add, drop []string) error {
	if len(add) == 0 && len(drop) == 0 {
		return nil
	}
	addJSON, err := json.Marshal(trimmedNonEmpty(add))
	if err != nil {
		return err
	}
	dropJSON, err := json.Marshal(trimmedNonEmpty(drop))
	if err != nil {
		return err
	}
	_, err = database.Exec(`
		UPDATE pipelines
		SET config = jsonb_set(COALESCE(config, '{}'::jsonb), '{`+cdcStreamingOnlyTablesKey+`}', COALESCE((
				SELECT jsonb_agg(t ORDER BY t)
				FROM (
					SELECT jsonb_array_elements_text(CASE
						WHEN jsonb_typeof(config->'`+cdcStreamingOnlyTablesKey+`') = 'array'
						THEN config->'`+cdcStreamingOnlyTablesKey+`' ELSE '[]'::jsonb END) AS t
					UNION
					SELECT jsonb_array_elements_text($1::jsonb)
				) s
				WHERE t NOT IN (SELECT jsonb_array_elements_text($2::jsonb))
			), '[]'::jsonb), true),
		    updated_at = NOW()
		WHERE id = $3::uuid
	`, string(addJSON), string(dropJSON), pipelineID)
	return err
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
// the P0 safety guards (hard PK validation for database destinations, MongoDB included) before
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
//
// An empty mode is sent as empty: the orchestrator owns the default, because it
// depends on the engine — incremental, but blocking on MongoDB, which refuses an
// incremental request (orchestrator backfillModes). Filling in "incremental"
// here made every MongoDB backfill of newly added collections a refusal.
//
// source is recorded on the orchestrator's cdc_snapshot_requests row (table_edit,
// auto_pickup); empty leaves the orchestrator's default.
func requestCDCBackfill(ctx context.Context, pipelineID string, tables []string, mode string, source string) gin.H {
	mode = strings.ToLower(strings.TrimSpace(mode))
	result := gin.H{
		"requested": true,
		"mode":      mode,
		"tables":    tables,
		"success":   false,
	}
	reqBody := gin.H{"tables": tables}
	if mode != "" {
		reqBody["mode"] = mode
	}
	if s := strings.TrimSpace(source); s != "" {
		reqBody["source"] = s
	}
	payload, _ := json.Marshal(reqBody)
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
		// Report the mode that actually ran, not the (possibly empty) request, and the
		// request the orchestrator queued (GET …/cdc/snapshot-requests tracks it).
		var ack struct {
			SnapshotMode string `json:"snapshot_mode"`
			RequestID    string `json:"request_id"`
			Status       string `json:"status"`
		}
		if json.Unmarshal(body, &ack) == nil {
			if ack.SnapshotMode != "" {
				result["mode"] = ack.SnapshotMode
			}
			if ack.RequestID != "" {
				result["request_id"] = ack.RequestID
			}
			if ack.Status != "" {
				result["status"] = ack.Status
			}
		}
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
