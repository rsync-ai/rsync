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

	"github.com/rsync-ai/backend-orchestrator/internal/cdc"
	"github.com/rsync-ai/backend-orchestrator/internal/mcp"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

// CDCProvisionRequest represents a request to provision CDC resources
type CDCProvisionRequest struct {
	PipelineID   string   `json:"pipeline_id" binding:"required"`
	ConnectionID string   `json:"connection_id" binding:"required"`
	DatabaseType string   `json:"database_type" binding:"required"`
	Database     string   `json:"database" binding:"required"`
	Tables       []string `json:"tables"`
}

// CDCProvisionResponse represents the response from CDC provisioning
type CDCProvisionResponse struct {
	Success   bool              `json:"success"`
	Resources []cdc.CDCResource `json:"resources,omitempty"`
	Errors    []string          `json:"errors,omitempty"`
}

// CDCCleanupRequest represents a request to cleanup CDC resources
type CDCCleanupRequest struct {
	PipelineID string `json:"pipeline_id" binding:"required"`
}

// CDCUpdateTablesRequest represents a request to update CDC connector tables
type CDCUpdateTablesRequest struct {
	PipelineID string   `json:"pipeline_id" binding:"required"`
	Tables     []string `json:"tables" binding:"required"`
}

// CDCBackfillRequest represents a request to trigger a Debezium backfill (ad-hoc snapshot)
// for one or more tables.
type CDCBackfillRequest struct {
	Tables []string `json:"tables" binding:"required"`
	// Mode can be "incremental" (default) or "blocking".
	Mode string `json:"mode"`
}

// ProvisionCDCResources provisions CDC resources for a pipeline
func ProvisionCDCResources(db *sql.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req CDCProvisionRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		// SECURITY (tenant isolation): requirePrincipal only authenticates. The
		// CDC managers fetch this connection's decrypted config with no tenant
		// predicate, so verify the caller owns req.ConnectionID before its
		// credentials are used to connect to the source DB.
		if !assertConnectionOwner(c, db, req.ConnectionID) {
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		log.WithFields(log.Fields{
			"pipeline_id":   req.PipelineID,
			"connection_id": req.ConnectionID,
			"database_type": req.DatabaseType,
		}).Info("Provisioning CDC resources")

		config := cdc.CDCResourceConfig{
			PipelineID:   req.PipelineID,
			ConnectionID: req.ConnectionID,
			DatabaseType: req.DatabaseType,
			Database:     req.Database,
		}

		var resources []cdc.CDCResource
		var err error

		// Dispatch through the CDC source-provider registry (internal/cdc/provider.go).
		// Adding a new source database = registering one provider, not editing
		// this switch. Unregistered types return the same "unsupported" error.
		mgr, ok := cdc.NewProvider(req.DatabaseType, db)
		if !ok {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "Unsupported database type for CDC: " + req.DatabaseType,
			})
			return
		}
		resources, err = mgr.ProvisionResources(ctx, config, req.Tables)

		if err != nil {
			log.WithError(err).Error("Failed to provision CDC resources")
			c.JSON(http.StatusInternalServerError, gin.H{
				"success": false,
				"errors":  []string{err.Error()},
			})
			return
		}

		log.WithFields(log.Fields{
			"pipeline_id":    req.PipelineID,
			"resource_count": len(resources),
		}).Info("Successfully provisioned CDC resources")

		c.JSON(http.StatusOK, CDCProvisionResponse{
			Success:   true,
			Resources: resources,
		})
	}
}

// CleanupCDCResources cleans up CDC resources for a pipeline.
// Also stops any kafka-mcp-sink worker associated with the pipeline so the
// per-pipeline sink process doesn't linger after the pipeline is deleted.
func CleanupCDCResources(db *sql.DB, mcpManager *mcp.ServerManager) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req CDCCleanupRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		// SECURITY (tenant isolation): gate cross-tenant CDC teardown.
		if !assertPipelineOwnerForHandlers(c, db, req.PipelineID) {
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		log.WithField("pipeline_id", req.PipelineID).Info("Cleaning up CDC resources")

		// Best-effort: stop the kafka-mcp-sink worker for this pipeline.
		// The orchestrator only starts the worker (start_sink); without an
		// explicit stop_sink call on delete, the subprocess inside the sink
		// MCP container lingers until the container restarts.
		if mcpManager != nil && strings.TrimSpace(req.PipelineID) != "" {
			// Teardown must target the group the sink actually registered, or the
			// subprocess survives the delete and keeps holding the destination.
			consumerGroup := ResolveSinkConsumerGroup(ctx, db, req.PipelineID)
			client := mcp.NewClient(mcpManager)
			if stopResp, _ := client.ExecuteWithContext(ctx, mcp.ExecuteRequest{
				Connector: "kafka-mcp-sink",
				Operation: "stop_sink",
				Config:    map[string]string{},
				Params: map[string]interface{}{
					"config": map[string]interface{}{
						"consumer_group": consumerGroup,
					},
				},
			}); stopResp != nil && !stopResp.Success {
				log.WithFields(log.Fields{
					"pipeline_id":     req.PipelineID,
					"consumer_group":  consumerGroup,
					"stop_sink_error": stopResp.Error,
				}).Debug("stop_sink on cleanup returned failure (likely no worker running; continuing)")
			}
		}

		errors := []string{}

		// ALWAYS delete the Debezium connector first — independent of the
		// cdc_resources table (which is frequently empty for MySQL pipelines,
		// so gating teardown on it silently skipped connector deletion). Doing
		// this before the per-DB managers also lets the PG replication slot go
		// inactive so it can be dropped (otherwise DROP_REPLICATION_SLOT fails
		// on the still-active slot held by a running connector).
		if strings.TrimSpace(req.PipelineID) != "" {
			if err := deleteDebeziumConnector(ctx, db, req.PipelineID); err != nil {
				log.WithError(err).WithField("pipeline_id", req.PipelineID).
					Warn("Debezium connector delete on cleanup failed (continuing)")
				errors = append(errors, "connector delete: "+err.Error())
			}
		}

		// Get all resources to determine which managers to use
		resources, err := cdc.GetCDCResources(ctx, db, req.PipelineID)
		if err != nil {
			log.WithError(err).Error("Failed to get CDC resources for cleanup")
			c.JSON(http.StatusInternalServerError, gin.H{
				"success": false,
				"errors":  []string{err.Error()},
			})
			return
		}

		// Group by database type. Always include every registered CDC family so
		// slot/publication/capture-instance cleanup runs even when cdc_resources
		// has no rows for this pipeline (the common case for MySQL pipelines).
		// Each provider's CleanupResources is idempotent/best-effort: if there is
		// nothing for this pipeline it no-ops, so running all of them is safe.
		dbTypes := map[string]bool{}
		for _, t := range cdc.RegisteredDBTypes() {
			dbTypes[t] = true
		}
		for _, res := range resources {
			dbTypes[res.DatabaseType] = true
		}

		// Cleanup for each database type via the shared provider registry.
		for dbType := range dbTypes {
			mgr, ok := cdc.NewProvider(dbType, db)
			if !ok {
				continue
			}
			if err := mgr.CleanupResources(ctx, req.PipelineID); err != nil {
				errors = append(errors, fmt.Sprintf("%s cleanup: %s", mgr.Family(), err.Error()))
			}
		}

		if len(errors) > 0 {
			log.WithField("errors", errors).Warn("CDC cleanup completed with errors")
			c.JSON(http.StatusOK, gin.H{
				"success": false,
				"errors":  errors,
				"message": "Cleanup completed with errors",
			})
			return
		}

		log.WithField("pipeline_id", req.PipelineID).Info("Successfully cleaned up CDC resources")

		c.JSON(http.StatusOK, gin.H{
			"success": true,
			"message": "CDC resources cleaned up successfully",
		})
	}
}

// UpdateCDCTables updates the table.include.list for a CDC connector
func UpdateCDCTables(db *sql.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req CDCUpdateTablesRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		// SECURITY (tenant isolation): gate cross-tenant connector table edits.
		if !assertPipelineOwnerForHandlers(c, db, req.PipelineID) {
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		log.WithFields(log.Fields{
			"pipeline_id": req.PipelineID,
			"tables":      req.Tables,
		}).Info("Updating CDC connector table list")

		// Find the Debezium connector name for this pipeline
		connectorName, err := findConnectorName(ctx, db, req.PipelineID)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{
				"success": false,
				"error":   "Connector not found for pipeline: " + err.Error(),
			})
			return
		}

		// P0 guard: For relational destinations, CDC tables MUST have PKs.
		// We validate at table-update time to prevent silently adding non-upsertable tables.
		if requiresPK, _, derr := pipelineDestinationRequiresPKValidation(ctx, db, req.PipelineID); derr == nil && requiresPK {
			kafkaConnectURL := strings.TrimRight(getKafkaConnectURL(), "/")
			connCfg, cfgErr := fetchKafkaConnectConfig(ctx, kafkaConnectURL, connectorName)
			if cfgErr != nil {
				c.JSON(http.StatusBadGateway, gin.H{"success": false, "error": cfgErr.Error()})
				return
			}

			sourceConnID, scErr := findPipelineSourceConnectionID(ctx, db, req.PipelineID)
			if scErr != nil {
				c.JSON(http.StatusNotFound, gin.H{"success": false, "error": scErr.Error()})
				return
			}

			dbType := inferDebeziumDatabaseType(connCfg)
			defaultDB, defaultSchema := inferDefaultDBAndSchema(connCfg)

			// Dispatch PK validation through the provider registry. The provider
			// selects which default namespace (database vs schema) qualifies
			// unqualified table names for its family.
			mgr, ok := cdc.NewProvider(dbType, db)
			if !ok {
				c.JSON(http.StatusBadRequest, gin.H{
					"success": false,
					"error":   "cdc_pk_validation_unsupported",
					"message": fmt.Sprintf("PK validation is not supported for Debezium connector type %q", dbType),
				})
				return
			}
			namespace := mgr.PrimaryKeyNamespace(defaultDB, defaultSchema)
			missing, verr := mgr.ValidateTablesHavePrimaryKeys(ctx, sourceConnID, namespace, req.Tables)
			if verr != nil {
				c.JSON(http.StatusBadRequest, gin.H{"success": false, "error": verr.Error()})
				return
			}
			if len(missing) > 0 {
				c.JSON(http.StatusBadRequest, gin.H{
					"success": false,
					"error":   "missing_primary_key",
					"message": "CDC requires PRIMARY KEY for relational destinations (upsert/delete). Add PKs or remove these tables.",
					"tables":  missing,
				})
				return
			}
		}

		// Update connector configuration via Kafka Connect API
		kafkaConnectURL := getKafkaConnectURL()
		err = updateConnectorTableList(ctx, kafkaConnectURL, connectorName, req.Tables)
		if err != nil {
			log.WithError(err).Error("Failed to update CDC connector table list")
			c.JSON(http.StatusInternalServerError, gin.H{
				"success": false,
				"error":   err.Error(),
			})
			return
		}

		log.WithFields(log.Fields{
			"pipeline_id":    req.PipelineID,
			"connector_name": connectorName,
			"tables":         req.Tables,
		}).Info("Successfully updated CDC connector table list")

		c.JSON(http.StatusOK, gin.H{
			"success":        true,
			"connector_name": connectorName,
			"tables":         req.Tables,
			"message":        "Connector table list updated successfully. Connector will restart automatically.",
		})
	}
}

func pipelineDestinationRequiresPKValidation(ctx context.Context, db *sql.DB, pipelineID string) (bool, string, error) {
	// Only enforce PKs when destination is a relational DB (upsert/delete semantics).
	var destConnectorType sql.NullString
	err := db.QueryRowContext(ctx, `
		SELECT c.connector_type
		FROM pipelines p
		JOIN connections c ON c.id = p.destination_connection_id
		WHERE p.id = $1::uuid
	`, pipelineID).Scan(&destConnectorType)
	if err != nil {
		return false, "", err
	}
	dest := strings.ToLower(strings.TrimSpace(destConnectorType.String))
	if dest == "postgres" {
		dest = "postgresql"
	}
	return dest == "postgresql" || dest == "mysql", dest, nil
}

func inferDebeziumDatabaseType(connCfg map[string]interface{}) string {
	class := strings.ToLower(strings.TrimSpace(fmt.Sprint(connCfg["connector.class"])))
	switch {
	case strings.Contains(class, "debezium") && strings.Contains(class, "mysql"):
		return "mysql"
	case strings.Contains(class, "debezium") && strings.Contains(class, "postgres"):
		return "postgresql"
	case strings.Contains(class, "debezium") && strings.Contains(class, "sqlserver"):
		return "sqlserver"
	}
	// Fallback: infer from presence of typical properties.
	if v := strings.TrimSpace(fmt.Sprint(connCfg["database.dbname"])); v != "" {
		return "postgresql"
	}
	if v := strings.TrimSpace(fmt.Sprint(connCfg["database.include.list"])); v != "" {
		return "mysql"
	}
	// SQL Server Debezium uses database.names (plural) rather than a dbname/
	// include.list key.
	if v := strings.TrimSpace(fmt.Sprint(connCfg["database.names"])); v != "" {
		return "sqlserver"
	}
	return ""
}

func inferDefaultDBAndSchema(connCfg map[string]interface{}) (string, string) {
	// MySQL default db
	dbName := strings.TrimSpace(fmt.Sprint(connCfg["database.include.list"]))
	if dbName != "" {
		parts := strings.Split(dbName, ",")
		if len(parts) > 0 {
			dbName = strings.TrimSpace(parts[0])
		}
	}

	// Postgres default schema: Debezium doesn't always set an explicit include list.
	// Use connector param "schema" if present; otherwise default to public.
	schema := strings.TrimSpace(fmt.Sprint(connCfg["schema"]))
	if schema == "" {
		schema = "public"
	}
	return dbName, schema
}

// findConnectorName finds the Debezium connector name for a pipeline
func findConnectorName(ctx context.Context, db *sql.DB, pipelineID string) (string, error) {
	// Try to find connector name from cdc_resources table
	var connectorName sql.NullString
	err := db.QueryRowContext(ctx, `
		SELECT resource_name
		FROM cdc_resources
		WHERE pipeline_id = $1::uuid
		  AND resource_type IN ('connector', 'debezium_connector')
		  AND status = 'active'
		ORDER BY created_at DESC
		LIMIT 1
	`, pipelineID).Scan(&connectorName)

	if err == nil && connectorName.Valid {
		return connectorName.String, nil
	}

	// Fallback 1: best-effort search in Kafka Connect connector list (works across naming schemes).
	if name, err := findConnectorNameInKafkaConnect(ctx, pipelineID); err == nil && strings.TrimSpace(name) != "" {
		return name, nil
	}

	// Fallback 2: stable legacy guess.
	if len(pipelineID) >= 8 {
		return fmt.Sprintf("cdc-%s", pipelineID[:8]), nil
	}

	return "", fmt.Errorf("could not determine connector name for pipeline %s", pipelineID)
}

func findConnectorNameInKafkaConnect(ctx context.Context, pipelineID string) (string, error) {
	connectURL := strings.TrimRight(getKafkaConnectURL(), "/")
	httpClient := &http.Client{Timeout: 10 * time.Second}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, connectURL+"/connectors", nil)
	if err != nil {
		return "", err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("kafka connect error (status %d)", resp.StatusCode)
	}

	var names []string
	if err := json.NewDecoder(resp.Body).Decode(&names); err != nil {
		return "", err
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
		if strings.Contains(ln, strings.ToLower(pipelineID)) || (short != "" && strings.Contains(ln, strings.ToLower(short))) {
			return n, nil
		}
	}
	return "", fmt.Errorf("connector not found in kafka connect")
}

// deleteDebeziumConnector removes the pipeline's Debezium connector from Kafka
// Connect (and its schema-history topic is left to the reaper). This is the
// single most important teardown step: without it the connector keeps a binlog
// reader / replication slot open on the source DB forever after the pipeline is
// gone (the orphan leak that exhausts the source and stalls future pipelines).
// It MUST run before the per-DB managers so the PG replication slot becomes
// inactive and can actually be dropped. Best-effort: errors are returned for
// logging but never block the rest of cleanup.
func deleteDebeziumConnector(ctx context.Context, db *sql.DB, pipelineID string) error {
	name, err := findConnectorName(ctx, db, pipelineID)
	if err != nil || strings.TrimSpace(name) == "" {
		// Nothing resolvable to delete — not an error for cleanup purposes.
		return nil
	}
	connectURL := strings.TrimRight(getKafkaConnectURL(), "/")
	httpClient := &http.Client{Timeout: 15 * time.Second}
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, connectURL+"/connectors/"+name, nil)
	if err != nil {
		return err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// 204 = deleted, 404 = already gone — both are success for teardown.
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("delete connector %s: kafka connect status %d", name, resp.StatusCode)
	}
	log.WithFields(log.Fields{"pipeline_id": pipelineID, "connector": name, "status": resp.StatusCode}).
		Info("Deleted Debezium connector on cleanup")
	return nil
}

// getKafkaConnectURL returns the Kafka Connect URL from environment or default
func getKafkaConnectURL() string {
	url := strings.TrimSpace(os.Getenv("KAFKA_CONNECT_URL"))
	if url == "" {
		return "http://kafka-connect:8083"
	}
	return url
}

// updateConnectorTableList updates the table.include.list for a Debezium connector
func updateConnectorTableList(ctx context.Context, kafkaConnectURL, connectorName string, tables []string) error {
	// Build table.include.list (format: db1.table1,db1.table2,...)
	tableIncludeList := strings.Join(tables, ",")

	// Fetch current connector config
	getURL := fmt.Sprintf("%s/connectors/%s/config", kafkaConnectURL, connectorName)
	httpClient := &http.Client{Timeout: 10 * time.Second}

	resp, err := httpClient.Get(getURL)
	if err != nil {
		return fmt.Errorf("failed to fetch connector config: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to fetch connector config (status %d): %s", resp.StatusCode, string(body))
	}

	var config map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&config); err != nil {
		return fmt.Errorf("failed to decode connector config: %w", err)
	}

	// Update table.include.list
	config["table.include.list"] = tableIncludeList

	// Send updated config back to Kafka Connect
	putURL := fmt.Sprintf("%s/connectors/%s/config", kafkaConnectURL, connectorName)
	configBytes, err := json.Marshal(config)
	if err != nil {
		return fmt.Errorf("failed to marshal updated config: %w", err)
	}

	putReq, err := http.NewRequestWithContext(ctx, http.MethodPut, putURL, bytes.NewReader(configBytes))
	if err != nil {
		return fmt.Errorf("failed to create PUT request: %w", err)
	}
	putReq.Header.Set("Content-Type", "application/json")

	putResp, err := httpClient.Do(putReq)
	if err != nil {
		return fmt.Errorf("failed to update connector config: %w", err)
	}
	defer putResp.Body.Close()

	if putResp.StatusCode != http.StatusOK && putResp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(putResp.Body)
		return fmt.Errorf("failed to update connector config (status %d): %s", putResp.StatusCode, string(body))
	}

	// Kafka Connect will automatically restart the connector when config changes
	log.Infof("Successfully updated connector %s table list to: %s", connectorName, tableIncludeList)

	return nil
}

// BackfillCDCTables triggers an ad-hoc snapshot for the requested tables using Debezium signaling.
// Currently supported for MySQL Debezium connectors (signal via <db>.debezium_signal).
func BackfillCDCTables(db *sql.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		pipelineID := strings.TrimSpace(c.Param("pipeline_id"))
		if pipelineID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "pipeline_id is required"})
			return
		}

		// SECURITY (tenant isolation): gate cross-tenant CDC backfill signaling.
		if !assertPipelineOwnerForHandlers(c, db, pipelineID) {
			return
		}

		var req CDCBackfillRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		// Sanitize tables
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

		mode := strings.ToLower(strings.TrimSpace(req.Mode))
		if mode == "" {
			mode = "incremental"
		}
		if mode != "incremental" && mode != "blocking" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "mode must be 'incremental' or 'blocking'"})
			return
		}

		ctx, cancel := context.WithTimeout(c.Request.Context(), 60*time.Second)
		defer cancel()

		connectorName, err := findConnectorName(ctx, db, pipelineID)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "connector not found for pipeline: " + err.Error()})
			return
		}

		kafkaConnectURL := strings.TrimRight(getKafkaConnectURL(), "/")
		connCfg, err := fetchKafkaConnectConfig(ctx, kafkaConnectURL, connectorName)
		if err != nil {
			c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
			return
		}

		connectorClass := strings.ToLower(strings.TrimSpace(fmt.Sprint(connCfg["connector.class"])))
		if !strings.Contains(connectorClass, "mysql") {
			c.JSON(http.StatusBadRequest, gin.H{
				"error":           "cdc_backfill_not_supported",
				"message":         "CDC backfill is currently supported for MySQL Debezium connectors only",
				"connector_class": connectorClass,
			})
			return
		}

		dbName := deriveDebeziumDatabaseName(connCfg, tables)
		if dbName == "" {
			c.JSON(http.StatusBadRequest, gin.H{
				"error":   "db_name_missing",
				"message": "Could not determine database name for Debezium signaling (expected database.include.list or qualified table names)",
			})
			return
		}

		// Ensure connector has signaling configured.
		signalCollection := fmt.Sprintf("%s.debezium_signal", dbName)
		needsUpdate := false
		if v := strings.TrimSpace(fmt.Sprint(connCfg["signal.data.collection"])); v != signalCollection {
			connCfg["signal.data.collection"] = signalCollection
			needsUpdate = true
		}
		if v := strings.TrimSpace(fmt.Sprint(connCfg["signal.enabled.channels"])); v == "" {
			connCfg["signal.enabled.channels"] = "source"
			needsUpdate = true
		}
		if needsUpdate {
			if err := putKafkaConnectConfig(ctx, kafkaConnectURL, connectorName, connCfg); err != nil {
				c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
				return
			}
		}

		// Find source connection ID (to connect to MySQL for signaling).
		sourceConnID, err := findPipelineSourceConnectionID(ctx, db, pipelineID)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
			return
		}

		// P0 guard: for relational destinations, ensure PKs exist before emitting snapshot signals.
		if requiresPK, _, derr := pipelineDestinationRequiresPKValidation(ctx, db, pipelineID); derr == nil && requiresPK {
			mgr := cdc.NewMySQLManager(db)
			missing, verr := mgr.ValidateTablesHavePrimaryKeys(ctx, sourceConnID, dbName, tables)
			if verr != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": verr.Error()})
				return
			}
			if len(missing) > 0 {
				c.JSON(http.StatusBadRequest, gin.H{
					"error":   "missing_primary_key",
					"message": "CDC requires PRIMARY KEY for relational destinations (upsert/delete). Add PKs or remove these tables.",
					"tables":  missing,
				})
				return
			}
		}

		// Ensure debezium_signal table exists and emit execute-snapshot signal.
		mgr := cdc.NewMySQLManager(db)
		if err := mgr.EnsureDebeziumSignalTable(ctx, sourceConnID, dbName); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error":   "signal_table_unavailable",
				"message": err.Error(),
			})
			return
		}

		collections := normalizeDebeziumCollections(dbName, tables)
		payload := map[string]interface{}{
			"data-collections": collections,
			"type":             mode,
		}
		b, _ := json.Marshal(payload)

		signalID := uuid.NewString()
		if err := mgr.UpsertDebeziumSignal(ctx, sourceConnID, dbName, signalID, "execute-snapshot", string(b)); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error":   "signal_emit_failed",
				"message": err.Error(),
			})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"success":          true,
			"pipeline_id":      pipelineID,
			"connector_name":   connectorName,
			"db":               dbName,
			"signal_table":     signalCollection,
			"signal_id":        signalID,
			"snapshot_mode":    mode,
			"data_collections": collections,
			"message":          "CDC backfill triggered (Debezium ad-hoc snapshot).",
		})
	}
}

func fetchKafkaConnectConfig(ctx context.Context, kafkaConnectURL, connectorName string) (map[string]interface{}, error) {
	getURL := fmt.Sprintf("%s/connectors/%s/config", kafkaConnectURL, connectorName)
	httpClient := &http.Client{Timeout: 10 * time.Second}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, getURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to build kafka connect request: %w", err)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch connector config: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to fetch connector config (status %d): %s", resp.StatusCode, string(body))
	}

	var cfg map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&cfg); err != nil {
		return nil, fmt.Errorf("failed to decode connector config: %w", err)
	}
	return cfg, nil
}

func putKafkaConnectConfig(ctx context.Context, kafkaConnectURL, connectorName string, cfg map[string]interface{}) error {
	putURL := fmt.Sprintf("%s/connectors/%s/config", kafkaConnectURL, connectorName)
	configBytes, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("failed to marshal updated config: %w", err)
	}
	httpClient := &http.Client{Timeout: 10 * time.Second}
	putReq, err := http.NewRequestWithContext(ctx, http.MethodPut, putURL, bytes.NewReader(configBytes))
	if err != nil {
		return fmt.Errorf("failed to create PUT request: %w", err)
	}
	putReq.Header.Set("Content-Type", "application/json")
	putResp, err := httpClient.Do(putReq)
	if err != nil {
		return fmt.Errorf("failed to update connector config: %w", err)
	}
	defer putResp.Body.Close()
	if putResp.StatusCode != http.StatusOK && putResp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(putResp.Body)
		return fmt.Errorf("failed to update connector config (status %d): %s", putResp.StatusCode, string(body))
	}
	return nil
}

func findPipelineSourceConnectionID(ctx context.Context, db *sql.DB, pipelineID string) (string, error) {
	var sourceConnID sql.NullString
	err := db.QueryRowContext(ctx, `SELECT source_connection_id::text FROM pipelines WHERE id = $1::uuid`, pipelineID).Scan(&sourceConnID)
	if err != nil || !sourceConnID.Valid || strings.TrimSpace(sourceConnID.String) == "" {
		return "", fmt.Errorf("source_connection_id not found for pipeline")
	}
	return strings.TrimSpace(sourceConnID.String), nil
}

func deriveDebeziumDatabaseName(connCfg map[string]interface{}, tables []string) string {
	// Prefer Debezium config if present.
	if v := strings.TrimSpace(fmt.Sprint(connCfg["database.include.list"])); v != "" {
		// database.include.list can be comma-separated.
		parts := strings.Split(v, ",")
		if len(parts) > 0 && strings.TrimSpace(parts[0]) != "" {
			return strings.TrimSpace(parts[0])
		}
	}
	// Fallback: infer from first qualified table name.
	if len(tables) > 0 {
		if i := strings.Index(tables[0], "."); i > 0 {
			return strings.TrimSpace(tables[0][:i])
		}
	}
	return ""
}

func normalizeDebeziumCollections(dbName string, tables []string) []string {
	out := make([]string, 0, len(tables))
	for _, t := range tables {
		v := strings.TrimSpace(t)
		if v == "" {
			continue
		}
		// For MySQL Debezium, collections are in the form: <db>.<table>
		if !strings.Contains(v, ".") && dbName != "" {
			v = fmt.Sprintf("%s.%s", dbName, v)
		}
		out = append(out, v)
	}
	return out
}
