package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/rsync-ai/backend-orchestrator/internal/cdc"
	log "github.com/sirupsen/logrus"
)

type CDCRecoverRequest struct {
	Operator string `json:"operator" binding:"required"`
	Reason   string `json:"reason" binding:"required"`

	// SnapshotMode controls Debezium snapshot.mode:
	// - "initial": full snapshot then stream
	// - "recovery": resume a partial snapshot
	// - "never": streaming only
	SnapshotMode string `json:"snapshot_mode"`

	// ResetOffsets attempts to reset Kafka Connect stored offsets for the connector.
	// This is required when forcing a re-snapshot after binlog/WAL position loss.
	ResetOffsets bool `json:"reset_offsets"`

	// DryRun returns validations + planned actions without side effects.
	DryRun bool `json:"dry_run"`
}

type connectorStatusPayload struct {
	Name      string `json:"name"`
	Connector struct {
		State string `json:"state"`
	} `json:"connector"`
	Tasks []struct {
		ID    int    `json:"id"`
		State string `json:"state"`
	} `json:"tasks"`
}

// RecoverCDCPipeline is an operator-initiated, guarded recovery endpoint.
//
// POST /api/v1/cdc/pipelines/:pipeline_id/recover
func RecoverCDCPipeline(db *sql.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		if strings.ToLower(strings.TrimSpace(os.Getenv("CDC_RECOVERY_ENABLED"))) != "true" {
			c.JSON(http.StatusForbidden, gin.H{
				"success": false,
				"error":   "cdc_recovery_disabled",
				"message": "CDC recovery endpoint is disabled (set CDC_RECOVERY_ENABLED=true to enable).",
			})
			return
		}

		pipelineID := strings.TrimSpace(c.Param("pipeline_id"))
		if pipelineID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"success": false, "error": "pipeline_id is required"})
			return
		}

		// SECURITY (tenant isolation): gate cross-tenant CDC recovery/re-provision.
		if !assertPipelineOwnerForHandlers(c, db, pipelineID) {
			return
		}

		var req CDCRecoverRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"success": false, "error": err.Error()})
			return
		}

		snapshotMode := normalizeSnapshotMode(req.SnapshotMode)
		if snapshotMode == "" {
			c.JSON(http.StatusBadRequest, gin.H{
				"success": false,
				"error":   "invalid_snapshot_mode",
				"message": "snapshot_mode must be one of: initial, recovery, never",
			})
			return
		}

		ctx, cancel := context.WithTimeout(c.Request.Context(), 90*time.Second)
		defer cancel()

		connectorName, err := findConnectorName(ctx, db, pipelineID)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"success": false, "error": err.Error()})
			return
		}

		connectURL := strings.TrimRight(getKafkaConnectURL(), "/")
		status, statusCode, statusErr := getConnectorStatus(ctx, connectURL, connectorName)
		if statusErr != nil {
			c.JSON(http.StatusBadGateway, gin.H{
				"success": false,
				"error":   statusErr.Error(),
				"result": gin.H{
					"connector_name": connectorName,
					"status_code":    statusCode,
				},
			})
			return
		}

		connState := strings.ToUpper(strings.TrimSpace(status.Connector.State))
		taskFailed := false
		for _, t := range status.Tasks {
			if strings.ToUpper(strings.TrimSpace(t.State)) == "FAILED" {
				taskFailed = true
				break
			}
		}
		if !req.DryRun && !(connState == "FAILED" || taskFailed) {
			c.JSON(http.StatusBadRequest, gin.H{
				"success": false,
				"error":   "invalid_state",
				"message": "Recovery is only allowed when connector or a task is in FAILED state (use restart for RUNNING/PAUSED).",
				"result": gin.H{
					"connector_state": connState,
					"tasks":           status.Tasks,
				},
			})
			return
		}

		connCfg, err := fetchKafkaConnectConfig(ctx, connectURL, connectorName)
		if err != nil {
			c.JSON(http.StatusBadGateway, gin.H{"success": false, "error": err.Error()})
			return
		}

		dbType := inferDebeziumDatabaseType(connCfg)
		defaultDB, defaultSchema := inferDefaultDBAndSchema(connCfg)
		sourceConnID, err := findPipelineSourceConnectionID(ctx, db, pipelineID)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"success": false, "error": err.Error()})
			return
		}

		tables := parseTableIncludeList(connCfg)
		if len(tables) == 0 {
			// Fallback: allow operator to proceed, but warn loudly.
			log.WithFields(log.Fields{
				"pipeline_id":    pipelineID,
				"connector_name": connectorName,
			}).Warn("CDC recover: table.include.list empty; skipping PK validation")
		}

		requiresPK, destType, _ := pipelineDestinationRequiresPKValidation(ctx, db, pipelineID)
		if requiresPK && len(tables) > 0 {
			switch dbType {
			case "mysql":
				mgr := cdc.NewMySQLManager(db)
				missing, verr := mgr.ValidateTablesHavePrimaryKeys(ctx, sourceConnID, defaultDB, tables)
				if verr != nil {
					c.JSON(http.StatusBadRequest, gin.H{"success": false, "error": verr.Error()})
					return
				}
				if len(missing) > 0 {
					c.JSON(http.StatusBadRequest, gin.H{
						"success": false,
						"error":   "missing_primary_key",
						"message": fmt.Sprintf("CDC recovery blocked: destination %q requires PKs for upsert/delete correctness.", destType),
						"tables":  missing,
					})
					return
				}
			case "postgresql":
				mgr := cdc.NewPostgreSQLManager(db)
				missing, verr := mgr.ValidateTablesHavePrimaryKeys(ctx, sourceConnID, defaultSchema, tables)
				if verr != nil {
					c.JSON(http.StatusBadRequest, gin.H{"success": false, "error": verr.Error()})
					return
				}
				if len(missing) > 0 {
					c.JSON(http.StatusBadRequest, gin.H{
						"success": false,
						"error":   "missing_primary_key",
						"message": fmt.Sprintf("CDC recovery blocked: destination %q requires PKs for upsert/delete correctness.", destType),
						"tables":  missing,
					})
					return
				}
			default:
				c.JSON(http.StatusBadRequest, gin.H{
					"success": false,
					"error":   "cdc_pk_validation_unsupported",
					"message": fmt.Sprintf("PK validation is not supported for Debezium connector type %q", dbType),
				})
				return
			}
		}

		plan := gin.H{
			"connector_name":  connectorName,
			"connector_state": connState,
			"db_type":         dbType,
			"snapshot_mode":   snapshotMode,
			"reset_offsets":   req.ResetOffsets,
			"table_count":     len(tables),
		}

		// Postgres source: ensure publication/slot are provisioned and align config.
		if dbType == "postgresql" {
			dbName := strings.TrimSpace(fmt.Sprint(connCfg["database.dbname"]))
			rcfg := cdc.CDCResourceConfig{
				PipelineID:   pipelineID,
				ConnectionID: sourceConnID,
				DatabaseType: "postgresql",
				Database:     dbName,
			}
			pgMgr := cdc.NewPostgreSQLManager(db)
			if !req.DryRun {
				if _, perr := pgMgr.ProvisionResources(ctx, rcfg, tables); perr != nil {
					c.JSON(http.StatusBadRequest, gin.H{"success": false, "error": fmt.Sprintf("failed to provision postgresql CDC resources: %v", perr)})
					return
				}
			}
			connCfg["publication.name"] = cdc.GenerateResourceName(rcfg, "publication")
			connCfg["slot.name"] = cdc.GenerateResourceName(rcfg, "replication_slot")
			connCfg["publication.autocreate.mode"] = "disabled"
			plan["publication_name"] = connCfg["publication.name"]
			plan["slot_name"] = connCfg["slot.name"]
		}

		// Update snapshot mode.
		connCfg["snapshot.mode"] = snapshotMode

		if req.DryRun {
			recoveryID := uuid.NewString()
			c.JSON(http.StatusOK, gin.H{
				"success":     true,
				"dry_run":     true,
				"recovery_id": recoveryID,
				"pipeline_id": pipelineID,
				"plan":        plan,
			})
			return
		}

		recoveryID := uuid.NewString()
		reason := strings.TrimSpace(req.Reason)
		log.WithFields(log.Fields{
			"recovery_id":    recoveryID,
			"pipeline_id":    pipelineID,
			"connector_name": connectorName,
			"operator":       strings.TrimSpace(req.Operator),
			"snapshot_mode":  snapshotMode,
			"reset_offsets":  req.ResetOffsets,
			"reason":         reason,
		}).Warn("CDC recovery requested")

		// Pause connector (best-effort) to stabilize task state for offset reset.
		_, _ = putConnectNoBody(ctx, connectURL, fmt.Sprintf("/connectors/%s/pause", connectorName))

		offsetReset := gin.H{"attempted": req.ResetOffsets, "success": false}
		if req.ResetOffsets {
			if err := deleteConnectorOffsets(ctx, connectURL, connectorName); err != nil {
				c.JSON(http.StatusBadRequest, gin.H{
					"success":     false,
					"error":       "offset_reset_failed",
					"message":     err.Error(),
					"pipeline_id": pipelineID,
					"connector":   connectorName,
				})
				return
			}
			offsetReset["success"] = true
		}

		// Apply updated config (snapshot.mode + optional pg publication/slot alignment).
		if err := putKafkaConnectConfig(ctx, connectURL, connectorName, connCfg); err != nil {
			c.JSON(http.StatusBadGateway, gin.H{"success": false, "error": err.Error()})
			return
		}

		// Restart + resume.
		_, _ = postConnectNoBody(ctx, connectURL, fmt.Sprintf("/connectors/%s/restart?includeTasks=true&onlyFailed=false", connectorName))
		_, _ = putConnectNoBody(ctx, connectURL, fmt.Sprintf("/connectors/%s/resume", connectorName))

		c.JSON(http.StatusOK, gin.H{
			"success":     true,
			"recovery_id": recoveryID,
			"pipeline_id": pipelineID,
			"connector":   connectorName,
			"plan":        plan,
			"offsets":     offsetReset,
			"message":     "CDC recovery triggered (connector restarted).",
		})
	}
}

func normalizeSnapshotMode(v string) string {
	s := strings.ToLower(strings.TrimSpace(v))
	if s == "" {
		return "initial"
	}
	switch s {
	case "initial", "recovery":
		return s
	case "never", "none", "streaming_only", "streaming-only":
		return "never"
	default:
		return ""
	}
}

func parseTableIncludeList(connCfg map[string]interface{}) []string {
	raw := strings.TrimSpace(fmt.Sprint(connCfg["table.include.list"]))
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		t := strings.TrimSpace(p)
		if t != "" {
			out = append(out, t)
		}
	}
	return out
}

func getConnectorStatus(ctx context.Context, connectURL, connectorName string) (*connectorStatusPayload, int, error) {
	u := fmt.Sprintf("%s/connectors/%s/status", strings.TrimRight(connectURL, "/"), connectorName)
	httpClient := &http.Client{Timeout: 10 * time.Second}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, 0, err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, resp.StatusCode, fmt.Errorf("kafka connect status error (status %d): %s", resp.StatusCode, string(body))
	}
	var payload connectorStatusPayload
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, resp.StatusCode, err
	}
	return &payload, resp.StatusCode, nil
}

func deleteConnectorOffsets(ctx context.Context, connectURL, connectorName string) error {
	// Kafka Connect supports DELETE /connectors/{name}/offsets on modern versions.
	u := fmt.Sprintf("%s/connectors/%s/offsets", strings.TrimRight(connectURL, "/"), connectorName)
	httpClient := &http.Client{Timeout: 15 * time.Second}
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, u, nil)
	if err != nil {
		return err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	body, _ := io.ReadAll(resp.Body)
	return fmt.Errorf("offset reset not supported or failed (status %d): %s", resp.StatusCode, string(body))
}

func putConnectNoBody(ctx context.Context, connectURL, path string) (int, error) {
	u := strings.TrimRight(connectURL, "/") + path
	httpClient := &http.Client{Timeout: 10 * time.Second}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, u, nil)
	if err != nil {
		return 0, err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	return resp.StatusCode, nil
}

func postConnectNoBody(ctx context.Context, connectURL, path string) (int, error) {
	u := strings.TrimRight(connectURL, "/") + path
	httpClient := &http.Client{Timeout: 10 * time.Second}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, nil)
	if err != nil {
		return 0, err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	return resp.StatusCode, nil
}
