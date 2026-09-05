package handlers

import (
	"bytes"
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
		payload, _ := json.Marshal(gin.H{
			"pipeline_id": pipelineID,
			"tables":      tables,
		})
		url := fmt.Sprintf("%s/api/v1/cdc/tables", orchestratorBaseURL())
		httpClient := &http.Client{Timeout: 30 * time.Second}
		r, err := http.NewRequestWithContext(c.Request.Context(), http.MethodPut, url, bytes.NewReader(payload))
		if err != nil {
			respondError(c, http.StatusInternalServerError, "request_build_failed", "Failed to build request", err)
			return
		}
		r.Header.Set("Content-Type", "application/json")
		setInternalServiceSecret(r)
		resp, err := httpClient.Do(r)
		if err != nil {
			respondError(c, http.StatusBadGateway, "orchestrator_unreachable", "Orchestrator is unreachable", err)
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			body, _ := io.ReadAll(resp.Body)
			log.WithFields(log.Fields{
				"pipeline_id":    pipelineID,
				"connector_name": connectorName,
				"status_code":    resp.StatusCode,
			}).Warn("Orchestrator rejected CDC table update")
			c.Data(resp.StatusCode, "application/json", body)
			return
		}
	}

	// Persist selection on the pipeline as well (so scheduled/manual runs can reuse it consistently).
	// (Non-blocking here: the Debezium update above is the authoritative side-effect.)
	if err := persistSelectedTables(database, pipelineID, tables); err != nil {
		log.WithError(err).WithField("pipeline_id", pipelineID).Warn("Failed to persist selected_tables (ignored)")
	}

	// Optionally trigger backfill for newly added tables.
	backfill := gin.H{
		"requested": req.BackfillNewlyAdded,
		"mode":      strings.TrimSpace(req.BackfillMode),
		"tables":    newTables,
		"success":   false,
	}
	if req.BackfillNewlyAdded && len(newTables) > 0 {
		mode := strings.TrimSpace(req.BackfillMode)
		if mode == "" {
			mode = "incremental"
		}
		backfill["mode"] = mode

		payload, _ := json.Marshal(gin.H{
			"tables": newTables,
			"mode":   mode,
		})
		url := fmt.Sprintf("%s/api/v1/cdc/pipelines/%s/backfill", orchestratorBaseURL(), pipelineID)
		httpClient := &http.Client{Timeout: 30 * time.Second}
		r, err := http.NewRequestWithContext(c.Request.Context(), http.MethodPost, url, bytes.NewReader(payload))
		if err == nil {
			r.Header.Set("Content-Type", "application/json")
			setInternalServiceSecret(r)
			resp, err2 := httpClient.Do(r)
			if err2 == nil && resp != nil {
				defer resp.Body.Close()
				body, _ := io.ReadAll(resp.Body)
				backfill["status_code"] = resp.StatusCode
				backfill["response"] = json.RawMessage(body)
				if resp.StatusCode >= 200 && resp.StatusCode < 300 {
					backfill["success"] = true
				} else {
					backfill["error"] = fmt.Sprintf("orchestrator backfill failed (status %d)", resp.StatusCode)
				}
			} else if err2 != nil {
				backfill["error"] = err2.Error()
			}
		} else {
			backfill["error"] = err.Error()
		}
	} else if req.BackfillNewlyAdded && len(newTables) == 0 {
		// Nothing new to backfill.
		backfill["success"] = true
	}

	// Best-effort: restart sink worker so newly-added table topics are applied (not just captured).
	// Without this, the sink consumer group may remain subscribed to the old topic list and
	// "Applied Inserts" stays at 0 for new tables.
	sinkRestart := gin.H{
		"requested": true,
		"success":   false,
	}
	{
		url := fmt.Sprintf("%s/api/v1/cdc/pipelines/%s/sink/restart", orchestratorBaseURL(), pipelineID)
		httpClient := &http.Client{Timeout: 30 * time.Second}
		r, err := http.NewRequestWithContext(c.Request.Context(), http.MethodPost, url, bytes.NewReader([]byte(`{}`)))
		if err == nil {
			r.Header.Set("Content-Type", "application/json")
			setInternalServiceSecret(r)
			resp, err2 := httpClient.Do(r)
			if err2 == nil && resp != nil {
				defer resp.Body.Close()
				body, _ := io.ReadAll(resp.Body)
				sinkRestart["status_code"] = resp.StatusCode
				sinkRestart["response"] = json.RawMessage(body)
				if resp.StatusCode >= 200 && resp.StatusCode < 300 {
					sinkRestart["success"] = true
				} else {
					sinkRestart["error"] = fmt.Sprintf("orchestrator sink restart failed (status %d)", resp.StatusCode)
				}
			} else if err2 != nil {
				sinkRestart["error"] = err2.Error()
			}
		} else {
			sinkRestart["error"] = err.Error()
		}
	}

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
