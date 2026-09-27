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
	"slices"
	"strings"
	"time"

	"github.com/rsync-ai/backend-orchestrator/internal/agents/executor"
	"github.com/rsync-ai/backend-orchestrator/internal/cdc"
	"github.com/rsync-ai/backend-orchestrator/internal/cdcsnapshot"
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
	// Mode can be "incremental" or "blocking". Empty picks the connector's
	// default: incremental where it is allowed, blocking on MongoDB
	// (backfillModes).
	Mode string `json:"mode"`
	// Source says who asked: "resnapshot" (the Re-snapshot card), "table_edit"
	// (Edit tables loading newly added tables) or "auto_pickup" (the CDC table
	// watcher). Empty reads as "resnapshot".
	Source string `json:"source"`
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

// cdcCleanupBudgets gives each phase of CleanupCDCResources its own time.
//
// The phases used to share one 60s context, with the sink stops first. A stop that never
// answered spent the time the connector delete and the slot drop needed, and api-gateway
// stops waiting after 30s and deletes the pipeline row; after that the slot drop cannot
// find its cdc_resources rows (pipeline_id is ON DELETE SET NULL) and the slot leaks until
// the reconciler. Separate budgets bound each phase alone.
//
// Together they stay under that 30s wait with room for the reply. api-gateway gives up on
// a slower answer, reports "did not run (orchestrator unreachable)" and deletes the row, so
// whatever this handler found after the wait is never reported.
// TestDeleteBudgetsFitTheGatewayWaits reads the wait from api-gateway and holds the sum.
type cdcCleanupBudgets struct {
	resolve   time.Duration // reading which sink workers to stop
	connector time.Duration // Debezium connector delete
	sources   time.Duration // per-database slot / publication / capture cleanup
	sinkStop  time.Duration // every stop_sink call
}

var defaultCDCCleanupBudgets = cdcCleanupBudgets{
	resolve:   2 * time.Second,
	connector: 8 * time.Second,
	sources:   12 * time.Second,
	sinkStop:  5 * time.Second,
}

// cdcSourceCleanup removes a pipeline's source-database CDC resources and returns one
// error per failure.
type cdcSourceCleanup func(ctx context.Context, db *sql.DB, pipelineID string) []string

// CleanupCDCResources cleans up CDC resources for a pipeline.
// Also stops any kafka-mcp-sink worker associated with the pipeline so the
// per-pipeline sink process doesn't linger after the pipeline is deleted.
func CleanupCDCResources(db *sql.DB, mcpManager *mcp.ServerManager) gin.HandlerFunc {
	return cleanupCDCResources(db, newSinkStopExecutor(mcpManager), cleanupCDCSources, defaultCDCCleanupBudgets)
}

// cleanupCDCResources is CleanupCDCResources with the sink service, the source cleanup and
// the phase budgets passed in, so tests can replace them.
//
// PHASE ORDER. Its only caller is api-gateway DeletePipeline (runCDCCleanupSync), which
// deletes the pipeline row once this returns or its 30s wait runs out. So:
//
//  1. Sink worker names are read first, while the pipeline row and its manifest rows
//     still exist; they cascade away with the row.
//  2. The Debezium connector is deleted, so the replication slot goes inactive.
//  3. Source cleanup drops the slot / publication. It reads cdc_resources by
//     pipeline_id, so it has to finish before the row delete clears that column.
//  4. Sink workers are stopped last. They only write what the connector already
//     produced, and the Kafka teardown after the row delete stops them again.
func cleanupCDCResources(db *sql.DB, sinks sinkStopExecutor, cleanupSources cdcSourceCleanup, budgets cdcCleanupBudgets) gin.HandlerFunc {
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

		log.WithField("pipeline_id", req.PipelineID).Info("Cleaning up CDC resources")

		errors := []string{}

		// Phase 1: which kafka-mcp-sink workers to stop. The orchestrator only starts
		// them (start_sink); without an explicit stop_sink on delete the subprocess
		// inside the sink container keeps consuming and writing until it restarts.
		// Lower-cased because the derived names and the id8 check compare against
		// pipelines.id::text.
		var sinkGroups []string
		if sinks != nil && strings.TrimSpace(req.PipelineID) != "" {
			resolveCtx, cancelResolve := context.WithTimeout(context.Background(), budgets.resolve)
			defer cancelResolve()
			groups, warnings := sinkGroupsForCleanup(resolveCtx, db, strings.ToLower(strings.TrimSpace(req.PipelineID)))
			sinkGroups = groups
			errors = append(errors, warnings...)
		}

		// Phase 2: ALWAYS delete the Debezium connector — independent of the
		// cdc_resources table (which is frequently empty for MySQL pipelines,
		// so gating teardown on it silently skipped connector deletion). Doing
		// this before the per-DB managers also lets the PG replication slot go
		// inactive so it can be dropped (otherwise DROP_REPLICATION_SLOT fails
		// on the still-active slot held by a running connector).
		if strings.TrimSpace(req.PipelineID) != "" {
			connectorCtx, cancelConnector := context.WithTimeout(context.Background(), budgets.connector)
			defer cancelConnector()
			if err := deleteDebeziumConnector(connectorCtx, db, req.PipelineID); err != nil {
				log.WithError(err).WithField("pipeline_id", req.PipelineID).
					Warn("Debezium connector delete on cleanup failed (continuing)")
				errors = append(errors, "connector delete: "+err.Error())
			}
		}

		// Phase 3: source-database resources.
		sourcesCtx, cancelSources := context.WithTimeout(context.Background(), budgets.sources)
		defer cancelSources()
		errors = append(errors, cleanupSources(sourcesCtx, db, req.PipelineID)...)

		// Phase 4: stop the sink workers. A failure is reported, not logged and
		// dropped: the worker may still be writing to the destination.
		sinkStopCtx, cancelSinkStop := context.WithTimeout(context.Background(), budgets.sinkStop)
		defer cancelSinkStop()
		errors = append(errors, stopSinkWorkers(sinkStopCtx, sinks, req.PipelineID, sinkGroups)...)

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

// cleanupCDCSources runs every registered CDC provider's cleanup for a pipeline.
//
// A failed cdc_resources read is returned as an error rather than ending the request with
// a 500: the handler still has to stop the sink workers after this. api-gateway reports a
// 500 and a success:false the same way, as a delete warning.
func cleanupCDCSources(ctx context.Context, db *sql.DB, pipelineID string) []string {
	// Get all resources to determine which managers to use
	resources, err := cdc.GetCDCResources(ctx, db, pipelineID)
	if err != nil {
		log.WithError(err).Error("Failed to get CDC resources for cleanup")
		return []string{err.Error()}
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
	var errs []string
	for dbType := range dbTypes {
		mgr, ok := cdc.NewProvider(dbType, db)
		if !ok {
			continue
		}
		if err := mgr.CleanupResources(ctx, pipelineID); err != nil {
			errs = append(errs, fmt.Sprintf("%s cleanup: %s", mgr.Family(), err.Error()))
		}
	}
	return errs
}

// UpdateCDCTables updates the table.include.list for a CDC connector. reaper
// (nil-safe) deletes the Kafka topics of removed tables once the sink has
// applied them (#20).
func UpdateCDCTables(db *sql.DB, reaper *RemovedTopicReaper) gin.HandlerFunc {
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
					"message": missingPrimaryKeyMessage,
					"tables":  missing,
				})
				return
			}
		}

		// Update connector configuration via Kafka Connect API
		kafkaConnectURL := getKafkaConnectURL()
		change, err := updateConnectorTableList(ctx, kafkaConnectURL, connectorName, req.Tables)
		if err != nil {
			log.WithError(err).Error("Failed to update CDC connector table list")
			c.JSON(http.StatusInternalServerError, gin.H{
				"success": false,
				"error":   err.Error(),
			})
			return
		}

		// The diff is taken against the list the connector actually held, not the
		// caller's idea of it (#6): a table edit whose earlier persist failed would
		// otherwise re-announce old additions and miss real removals.
		added, removed := diffTableLists(change.Previous, change.Written)
		reaper.Readd(change.Config, added)
		reaper.Schedule(req.PipelineID, connectorName, change.Config, removed)

		log.WithFields(log.Fields{
			"pipeline_id":    req.PipelineID,
			"connector_name": connectorName,
			"tables":         req.Tables,
			"added":          added,
			"removed":        removed,
		}).Info("Successfully updated CDC connector table list")

		previous := []string{}
		for _, p := range change.Previous {
			if plain := unescapeIncludeEntry(p); plain != "" {
				previous = append(previous, plain)
			}
		}
		c.JSON(http.StatusOK, gin.H{
			"success":         true,
			"connector_name":  connectorName,
			"tables":          req.Tables,
			"live_list_read":  true,
			"previous_tables": tableNamesAsRequested(change.Config, req.Tables, previous),
			"added":           tableNamesAsRequested(change.Config, req.Tables, added),
			"removed":         tableNamesAsRequested(change.Config, req.Tables, removed),
			"message":         "Connector table list updated successfully. Connector will restart automatically.",
		})
	}
}

// missingPrimaryKeyMessage is the refusal text for a keyless table headed to a
// database destination. CDC auto-pickup keys on the "missing_primary_key" error
// code, not on this text.
const missingPrimaryKeyMessage = "CDC to a database destination (PostgreSQL, MySQL or MongoDB) needs a PRIMARY KEY on every table: the destination upserts and deletes on it. Add a PRIMARY KEY or remove these tables."

func pipelineDestinationRequiresPKValidation(ctx context.Context, db *sql.DB, pipelineID string) (bool, string, error) {
	// Enforce PKs for every DATABASE destination: relational (upsert/delete on the
	// key) and MongoDB, where the sink upserts on the key too — a keyless table
	// there either gets a guessed key (rows sharing it replace each other) or is
	// inserted blind (every re-snapshot duplicates it). Object storage is
	// append-only, so a keyless table there is only a warning. The product rule:
	// keyless → blocked for any DB destination (the executor's hard-block and
	// assessor.CDCBlocksWithoutPrimaryKey carry the same list).
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
	return cdcDestinationRequiresPrimaryKeys(dest), normalizeCDCDestType(dest), nil
}

// normalizeCDCDestType folds destination connector_type aliases the same way the
// executor's normalizeDBType does (postgres→postgresql, mariadb→mysql).
func normalizeCDCDestType(dest string) string {
	switch dest {
	case "postgres":
		return "postgresql"
	case "mariadb":
		return "mysql"
	default:
		return dest
	}
}

// cdcDestinationRequiresPrimaryKeys is the destination list of the keyless-table
// block: every database destination rsync upserts into.
func cdcDestinationRequiresPrimaryKeys(dest string) bool {
	switch normalizeCDCDestType(strings.ToLower(strings.TrimSpace(dest))) {
	case "postgresql", "mysql", "mongodb":
		return true
	default:
		return false
	}
}

func inferDebeziumDatabaseType(connCfg map[string]interface{}) string {
	// MongoDB first: its config carries none of the relational keys below, and the
	// database.dbname fallback reads a missing key as "<nil>" — so a MongoDB
	// connector used to come back as "postgresql".
	if isMongoDebeziumConfig(connCfg) {
		return "mongodb"
	}
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

// isMongoDebeziumConfig reports whether a Kafka Connect config is a Debezium
// MongoDB source connector.
func isMongoDebeziumConfig(config map[string]interface{}) bool {
	class := strings.ToLower(connectorConfigString(config, "connector.class"))
	if strings.Contains(class, "mongodb") {
		return true
	}
	return class == "" && connectorConfigString(config, "collection.include.list") != ""
}

// qualifyMongoCollections turns bare collection names into "db.collection"
// when the connector captures exactly one database, as the connector create
// path does (debezium connector.py). Qualified names pass through unchanged.
func qualifyMongoCollections(config map[string]interface{}, tables []string) []string {
	dbs := splitCommaList(connectorConfigString(config, "database.include.list"))
	out := make([]string, 0, len(tables))
	for _, t := range tables {
		tt := strings.TrimSpace(t)
		if tt == "" {
			continue
		}
		if !strings.Contains(tt, ".") && len(dbs) == 1 {
			tt = dbs[0] + "." + tt
		}
		out = append(out, tt)
	}
	return out
}

// tableListChange is what updateConnectorTableList replaced: the include list
// the connector held before the write (nil when it had none, i.e. captured
// everything), the entries written, and the config as written.
type tableListChange struct {
	Previous []string
	Written  []string
	Config   map[string]interface{}
}

// updateConnectorTableList updates the captured table list of a Debezium
// connector: table.include.list for relational sources, collection.include.list
// for MongoDB.
func updateConnectorTableList(ctx context.Context, kafkaConnectURL, connectorName string, tables []string) (tableListChange, error) {
	// Build table.include.list (format: db1.table1,db1.table2,...)
	tableIncludeList := strings.Join(tables, ",")

	// Fetch current connector config
	getURL := fmt.Sprintf("%s/connectors/%s/config", kafkaConnectURL, connectorName)
	httpClient := &http.Client{Timeout: 10 * time.Second}

	resp, err := httpClient.Get(getURL)
	if err != nil {
		return tableListChange{}, fmt.Errorf("failed to fetch connector config: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return tableListChange{}, fmt.Errorf("failed to fetch connector config (status %d): %s", resp.StatusCode, string(body))
	}

	var config map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&config); err != nil {
		return tableListChange{}, fmt.Errorf("failed to decode connector config: %w", err)
	}

	// The MongoDB connector ignores table.include.list and captures
	// collection.include.list, which the sink respawn also reads
	// (connectorIncludeList). Writing only table.include.list left Debezium on
	// the old collections while a respawned sink followed the new ones (#26).
	change := tableListChange{}
	if isMongoDebeziumConfig(config) {
		change.Previous = includeListEntries(config, "collection.include.list")
		change.Written = qualifyMongoCollections(config, tables)
		tableIncludeList = strings.Join(change.Written, ",")
		config["collection.include.list"] = tableIncludeList
		delete(config, "table.include.list")
	} else {
		change.Previous = includeListEntries(config, "table.include.list")
		change.Written = splitCommaList(tableIncludeList)
		config["table.include.list"] = tableIncludeList
	}
	change.Config = config

	// Send updated config back to Kafka Connect
	putURL := fmt.Sprintf("%s/connectors/%s/config", kafkaConnectURL, connectorName)
	configBytes, err := json.Marshal(config)
	if err != nil {
		return tableListChange{}, fmt.Errorf("failed to marshal updated config: %w", err)
	}

	putReq, err := http.NewRequestWithContext(ctx, http.MethodPut, putURL, bytes.NewReader(configBytes))
	if err != nil {
		return tableListChange{}, fmt.Errorf("failed to create PUT request: %w", err)
	}
	putReq.Header.Set("Content-Type", "application/json")

	putResp, err := httpClient.Do(putReq)
	if err != nil {
		return tableListChange{}, fmt.Errorf("failed to update connector config: %w", err)
	}
	defer putResp.Body.Close()

	if putResp.StatusCode != http.StatusOK && putResp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(putResp.Body)
		return tableListChange{}, fmt.Errorf("failed to update connector config (status %d): %s", putResp.StatusCode, string(body))
	}

	// Kafka Connect will automatically restart the connector when config changes
	log.Infof("Successfully updated connector %s table list to: %s", connectorName, tableIncludeList)

	return change, nil
}

// includeListEntries returns the entries of an include-list property, or nil
// when the connector has none (it captures every table).
func includeListEntries(config map[string]interface{}, key string) []string {
	entries := splitCommaList(connectorConfigString(config, key))
	if len(entries) == 0 {
		return nil
	}
	return entries
}

// diffTableLists compares an include list before and after an edit the way
// Debezium matches it (regular expressions, case-insensitive). added are the
// written entries the previous list did not capture; removed are the previous
// entries, unescaped, that the new list no longer captures.
func diffTableLists(previous, written []string) (added, removed []string) {
	added, removed = []string{}, []string{}
	for _, w := range written {
		plain := unescapeIncludeEntry(w)
		if plain != "" && !cdcsnapshot.IncludeListCaptures(previous, plain) {
			added = append(added, plain)
		}
	}
	for _, p := range previous {
		plain := unescapeIncludeEntry(p)
		if plain != "" && !cdcsnapshot.IncludeListCaptures(written, plain) {
			removed = append(removed, plain)
		}
	}
	return added, removed
}

// unescapeIncludeEntry turns an include-list entry rsync wrote ("public\.users")
// back into the table name it stands for.
func unescapeIncludeEntry(e string) string {
	return strings.TrimSpace(strings.ReplaceAll(e, `\`, ""))
}

// tableNamesAsRequested renders connector-side names the way the caller named
// its tables: a MongoDB connector qualifies bare collections with its single
// database, so when every requested name was bare the "db." prefix goes again.
func tableNamesAsRequested(config map[string]interface{}, requested, names []string) []string {
	if !isMongoDebeziumConfig(config) {
		return names
	}
	dbs := splitCommaList(connectorConfigString(config, "database.include.list"))
	if len(dbs) != 1 {
		return names
	}
	for _, t := range requested {
		if strings.Contains(strings.TrimSpace(t), ".") {
			return names
		}
	}
	out := make([]string, 0, len(names))
	for _, n := range names {
		out = append(out, strings.TrimPrefix(n, dbs[0]+"."))
	}
	return out
}

// cdcSignalProducer is the slice of the Kafka manager the backfill needs. It is
// an interface so the handler can be tested without a broker, and so the
// orchestrator keeps exactly one Kafka client.
type cdcSignalProducer interface {
	EnsureSignalTopic(topic string) error
	ProduceWithContext(ctx context.Context, topic string, key, value []byte) error
}

// BackfillCDCTables triggers an ad-hoc Debezium snapshot for the requested tables.
//
// There are two signalling channels, and which one a connector has is decided
// when it is created:
//
//   - Kafka signal channel (every PostgreSQL-family and MongoDB connector, plus
//     MySQL connectors created with snapshot_strategy=incremental): the signal is
//     a Kafka message, so NOTHING is written to the customer's source database.
//     Preferred whenever the connector has it. MongoDB accepts BLOCKING snapshots
//     only (backfillModes).
//   - Source signal table (<db>.debezium_signal): MySQL only, because it needs a
//     writable signal table in the source.
//
// Engines with neither are refused with cdc_backfill_not_supported.
//
// Over the Kafka channel the request is QUEUED (cdc_snapshot_requests), not
// sent: the cdcsnapshot dispatcher sends it once the connector's running task
// captures every requested table. Sent straight away, a signal right after Edit
// tables reached the OLD task — which does not capture the new table and drops
// the signal — or was lost in the restart. requests may be nil (or its table
// missing): the signal is then sent directly, as before.
func BackfillCDCTables(db *sql.DB, signals cdcSignalProducer, requests *cdcsnapshot.Store) gin.HandlerFunc {
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

		// An empty mode is resolved once the connector is known: the default
		// depends on the engine (backfillModes).
		mode := strings.ToLower(strings.TrimSpace(req.Mode))
		if mode != "" && mode != "incremental" && mode != "blocking" {
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

		// Find the source connection up front: both channels validate primary keys
		// against the source before signalling.
		sourceConnID, err := findPipelineSourceConnectionID(ctx, db, pipelineID)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
			return
		}

		dbType := inferDebeziumDatabaseType(connCfg)
		defaultDB, defaultSchema := inferDefaultDBAndSchema(connCfg)

		// Prefer the Kafka signal channel when the connector has one: it works for
		// every engine that wires it, and it writes nothing to the source.
		channel := backfillSignalChannel(connCfg)

		// One destination lookup serves the primary-key gate and the object
		// storage rules. A failed lookup keeps the old fail-open behaviour: no
		// PK gate, no object-storage rules.
		requiresPK, destType, destErr := pipelineDestinationRequiresPKValidation(ctx, db, pipelineID)
		if destErr != nil {
			log.WithError(destErr).WithField("pipeline_id", pipelineID).Warn("CDC backfill: could not read the destination type")
		}
		objectStorage := destErr == nil && isObjectStorageDest(destType)
		if channel == backfillChannelKafka && objectStorage {
			// Object storage re-snapshots are BLOCKING only: the folder is emptied
			// and re-written in one pass with streaming paused, and a blocking
			// snapshot marks its last row, so the request is seen to finish. An
			// incremental one interleaves with live changes and never says it is
			// done (PostgreSQL sends no end marker for it).
			mode = "blocking"
		}
		if modes := backfillModesFor(connCfg, channel, objectStorage); len(modes) > 0 {
			if mode == "" {
				mode = modes[0]
			}
			if !slices.Contains(modes, mode) {
				c.JSON(http.StatusBadRequest, gin.H{
					"error":           "cdc_backfill_mode_not_supported",
					"message":         backfillModeNotSupportedMessage(mode),
					"modes":           modes,
					"connector_class": connectorClass,
				})
				return
			}
		}
		if channel == backfillChannelKafka {
			signalTopic := connCfgString(connCfg, "signal.kafka.topic")
			if signals == nil {
				c.JSON(http.StatusServiceUnavailable, gin.H{
					"error":   "signal_channel_unavailable",
					"message": "The connector signals over Kafka but this orchestrator has no Kafka producer",
				})
				return
			}
			if halt := backfillMissingPKs(ctx, c, db, requiresPK, sourceConnID, dbType, defaultDB, defaultSchema, tables); halt {
				return
			}

			collections := normalizeDebeziumCollections(pkNamespaceFor(dbType, defaultDB, defaultSchema), tables)
			if dbType == "mongodb" {
				// "db.collection", qualified exactly as the Edit tables include-list
				// update qualifies it (updateConnectorTableList), so the signal names
				// the collections the connector captures.
				collections = qualifyMongoCollections(connCfg, tables)
			}
			cleansFolder := objectStorage && executor.PipelineCleansFolderOnResnapshot(ctx, db, pipelineID, destType)
			source := cdcsnapshot.NormalizeSource(req.Source)
			notBefore := time.Now()
			if source != cdcsnapshot.SourceResnapshot {
				// Edit tables and auto-pickup have just rewritten the include list;
				// give Connect time to begin restarting the task before the
				// dispatcher starts judging whether the new one is running.
				notBefore = notBefore.Add(15 * time.Second)
			}
			queued, qerr := requests.Insert(ctx, cdcsnapshot.Request{
				PipelineID: pipelineID, ConnectorName: connectorName, Mode: mode, Tables: collections,
				Source: source, CleansFolder: cleansFolder, NotBefore: notBefore,
			})
			if qerr == nil {
				log.WithFields(log.Fields{
					"pipeline_id": pipelineID,
					"request_id":  queued.ID,
					"source":      source,
					"tables":      len(collections),
				}).Info("📸 CDC backfill queued; the snapshot dispatcher sends it once the connector runs with these tables")
				c.JSON(http.StatusOK, gin.H{
					"success":          true,
					"status":           queued.Status,
					"request_id":       queued.ID,
					"pipeline_id":      pipelineID,
					"connector_name":   connectorName,
					"signal_channel":   "kafka",
					"signal_topic":     signalTopic,
					"snapshot_mode":    mode,
					"data_collections": collections,
					"cleans_folder":    cleansFolder,
					"message":          backfillQueuedMessage(cleansFolder),
				})
				return
			}
			if !errors.Is(qerr, cdcsnapshot.ErrUnavailable) {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "snapshot_request_failed", "message": qerr.Error()})
				return
			}
			// No queue (migration 113 not applied, or no store wired): send now.

			// Per the Debezium signalling contract the message KEY is the
			// connector's topic.prefix; the connector name is the prefix here, and
			// the explicit property wins when present.
			key := cdcsnapshot.SignalKey(connCfg, connectorName)
			value, merr := buildExecuteSnapshotSignal(mode, collections)
			if merr != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "signal_encode_failed", "message": merr.Error()})
				return
			}
			if terr := signals.EnsureSignalTopic(signalTopic); terr != nil {
				// Non-fatal: the produce below is the authoritative delivery check
				// (broker auto-create may still succeed).
				log.WithError(terr).WithField("topic", signalTopic).Warn("⚠️  CDC backfill: could not ensure the signal topic exists (producing anyway)")
			}
			if perr := signals.ProduceWithContext(ctx, signalTopic, []byte(key), value); perr != nil {
				c.JSON(http.StatusBadGateway, gin.H{
					"error":   "signal_emit_failed",
					"message": perr.Error(),
				})
				return
			}
			log.WithFields(log.Fields{
				"pipeline_id":  pipelineID,
				"signal_topic": signalTopic,
				"tables":       len(collections),
			}).Info("📸 CDC backfill triggered over the Kafka signal channel")
			c.JSON(http.StatusOK, gin.H{
				"success":          true,
				"status":           cdcsnapshot.StatusSent,
				"pipeline_id":      pipelineID,
				"connector_name":   connectorName,
				"signal_channel":   "kafka",
				"signal_topic":     signalTopic,
				"snapshot_mode":    mode,
				"data_collections": collections,
				"message":          "CDC backfill triggered (Debezium ad-hoc snapshot over the Kafka signal channel).",
			})
			return
		}

		if channel == backfillChannelNone {
			c.JSON(http.StatusBadRequest, gin.H{
				"error":           "cdc_backfill_not_supported",
				"message":         backfillNotSupportedMessage,
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

		// P0 guard: for database destinations (relational and MongoDB), ensure PKs exist before emitting snapshot signals.
		if requiresPK {
			mgr := cdc.NewMySQLManager(db)
			missing, verr := mgr.ValidateTablesHavePrimaryKeys(ctx, sourceConnID, dbName, tables)
			if verr != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": verr.Error()})
				return
			}
			if len(missing) > 0 {
				c.JSON(http.StatusBadRequest, gin.H{
					"error":   "missing_primary_key",
					"message": missingPrimaryKeyMessage,
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

// The channel a re-snapshot of a connector would go through. Decided once, by
// backfillSignalChannel, for both BackfillCDCTables (which acts on it) and
// GetCDCBackfillCapability (which lets the UI say so before anyone clicks) — two
// copies of this rule would drift, and the UI would offer what the POST refuses.
const (
	backfillChannelKafka  = "kafka"  // Kafka signal channel wired at connector creation
	backfillChannelSource = "source" // MySQL source signal table, wired on demand
	backfillChannelNone   = ""       // neither → cdc_backfill_not_supported
)

// backfillNotSupportedMessage is what both Re-snapshot and "backfill newly added
// tables" show when the connector has no channel. Every PostgreSQL-family and
// MongoDB connector now gets a Kafka signal channel when it is created, whatever
// its snapshot strategy (executor.go cdcSignalTopicFor); before that only the
// PostgreSQL incremental strategy (>= CDC_INCREMENTAL_SNAPSHOT_MIN_ROWS rows)
// wired one, so every smaller pipeline was refused. A refusal therefore now means
// a connector created before that change — which recreating the pipeline fixes —
// or an engine with no channel at all. The handler cannot tell which, so the
// message names both without claiming either.
const backfillNotSupportedMessage = "This pipeline's CDC connector has no signal channel, so it cannot be asked to re-read tables: " +
	"neither Re-snapshot nor the backfill of newly added tables can run. " +
	"PostgreSQL and MongoDB pipelines get a Kafka signal channel when their connector is created, but connectors created before that was added have none; recreating the pipeline adds it. " +
	"MySQL pipelines use a signal table instead, and other sources have no re-snapshot path."

// backfillModes lists the snapshot modes BackfillCDCTables accepts for this
// connector, the default first; nil when there is no channel at all. The one
// rule for both the POST (which enforces it) and GetCDCBackfillCapability (which
// lets the UI offer only these).
//
// MongoDB over the Kafka channel is blocking only. Debezium's incremental
// snapshot for MongoDB has no read-only mode: it writes low/high watermark
// documents into a signal collection in the source (signal.data.collection),
// which rsync never configures. Without one the signal fails inside the
// connector (Debezium 3.1.3: a NullPointerException in emitWindowOpen) while the
// task stays RUNNING, so a request accepted here would silently load nothing;
// configuring one would mean writing to the customer's database. A blocking snapshot
// pauses streaming, re-reads the requested collections, then resumes from the
// stored resume token, and writes nothing.
func backfillModes(connCfg map[string]interface{}, channel string) []string {
	switch {
	case channel == backfillChannelNone:
		return nil
	case channel == backfillChannelKafka && isMongoDebeziumConfig(connCfg) &&
		connCfgString(connCfg, "signal.data.collection") == "":
		return []string{"blocking"}
	default:
		return []string{"incremental", "blocking"}
	}
}

// backfillModesFor is backfillModes for a known destination: object storage
// takes BLOCKING re-snapshots only (see BackfillCDCTables).
func backfillModesFor(connCfg map[string]interface{}, channel string, objectStorage bool) []string {
	modes := backfillModes(connCfg, channel)
	if objectStorage && channel == backfillChannelKafka && slices.Contains(modes, "blocking") {
		return []string{"blocking"}
	}
	return modes
}

// isObjectStorageDest reports a destination that appends files rather than
// upserting rows, so re-reading a table there adds a second copy of it.
func isObjectStorageDest(dest string) bool {
	switch strings.ReplaceAll(strings.ToLower(strings.TrimSpace(dest)), "_", "-") {
	case "gcs", "aws-s3", "s3", "azure-blob", "minio":
		return true
	}
	return false
}

// backfillQueuedMessage is the answer to a queued re-snapshot.
func backfillQueuedMessage(cleansFolder bool) string {
	msg := "Queued: the snapshot starts once the CDC connector is running with these tables, usually within a minute."
	if cleansFolder {
		msg += " Each table's existing files in the destination folder are deleted when its snapshot starts, then written again."
	}
	return msg
}

// backfillModeNotSupportedMessage explains a refused mode. Only MongoDB refuses
// one today (incremental), so the message says why in its terms.
func backfillModeNotSupportedMessage(mode string) string {
	if mode == "incremental" {
		return "MongoDB pipelines re-read collections with a blocking snapshot only: change streaming for this pipeline pauses " +
			"while the requested collections are read again, then resumes where it stopped. An incremental snapshot is not " +
			"offered because it would write watermark documents into your MongoDB database. Retry with mode \"blocking\"."
	}
	return fmt.Sprintf("Snapshot mode %q is not supported for this pipeline's CDC connector.", mode)
}

// backfillSignalChannel reports which channel BackfillCDCTables would use for
// this connector config.
func backfillSignalChannel(connCfg map[string]interface{}) string {
	if connCfgString(connCfg, "signal.kafka.topic") != "" &&
		strings.Contains(strings.ToLower(connCfgString(connCfg, "signal.enabled.channels")), "kafka") {
		return backfillChannelKafka
	}
	if strings.Contains(strings.ToLower(connCfgString(connCfg, "connector.class")), "mysql") {
		return backfillChannelSource
	}
	return backfillChannelNone
}

// GetCDCBackfillCapability answers "can this pipeline be re-snapshotted?"
// without doing it. Read-only: one Kafka Connect config GET, no signal, no
// write to the source. The UI calls it when the Re-snapshot card loads, so a
// pipeline with no signal channel says so up front instead of offering a mode
// choice and a button that can only ever return cdc_backfill_not_supported.
func GetCDCBackfillCapability(db *sql.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		pipelineID := strings.TrimSpace(c.Param("pipeline_id"))
		if pipelineID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "pipeline_id is required"})
			return
		}
		if !assertPipelineOwnerForHandlers(c, db, pipelineID) {
			return
		}

		ctx, cancel := context.WithTimeout(c.Request.Context(), 15*time.Second)
		defer cancel()

		connectorName, err := findConnectorName(ctx, db, pipelineID)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "connector not found for pipeline: " + err.Error()})
			return
		}
		connCfg, err := fetchKafkaConnectConfig(ctx, strings.TrimRight(getKafkaConnectURL(), "/"), connectorName)
		if err != nil {
			c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
			return
		}

		channel := backfillSignalChannel(connCfg)
		_, destType, destErr := pipelineDestinationRequiresPKValidation(ctx, db, pipelineID)
		objectStorage := destErr == nil && isObjectStorageDest(destType)
		modes := backfillModesFor(connCfg, channel, objectStorage)
		resp := gin.H{
			"destination_type": destType,
			"object_storage":   objectStorage,
			// Whether a re-snapshot first deletes each table's existing files.
			"cleans_folder": channel == backfillChannelKafka && objectStorage &&
				executor.PipelineCleansFolderOnResnapshot(ctx, db, pipelineID, destType),
			"pipeline_id":     pipelineID,
			"connector_name":  connectorName,
			"connector_class": strings.ToLower(connCfgString(connCfg, "connector.class")),
			"supported":       channel != backfillChannelNone,
			"signal_channel":  channel,
			// The modes the POST accepts, default first ([] when unsupported), so the
			// UI never offers one it would refuse.
			"modes": append([]string{}, modes...),
		}
		if len(modes) > 0 {
			resp["default_mode"] = modes[0]
		}
		if channel == backfillChannelNone {
			resp["error"] = "cdc_backfill_not_supported"
			resp["message"] = backfillNotSupportedMessage
		}
		c.JSON(http.StatusOK, resp)
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

// buildExecuteSnapshotSignal encodes the VALUE of a Debezium execute-snapshot
// signal for the Kafka signal channel. The shape is Debezium's, not ours: the
// snapshot type lives under "data", and Debezium silently ignores a signal it
// cannot parse — so getting this wrong means "no backfill" with no error
// anywhere. The message KEY is the connector's topic.prefix, supplied by the
// caller.
func buildExecuteSnapshotSignal(mode string, collections []string) ([]byte, error) {
	return cdcsnapshot.BuildExecuteSnapshotSignal(mode, collections)
}

// connCfgString reads a Kafka Connect config value as a trimmed string. A
// missing key must read as "" — fmt.Sprint(nil) yields "<nil>", which would make
// an absent signal topic look configured.
func connCfgString(connCfg map[string]interface{}, key string) string {
	v, ok := connCfg[key]
	if !ok || v == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(v))
}

// pkNamespaceFor picks the default namespace that qualifies an unqualified table
// name for an engine (database for MySQL, schema for PostgreSQL, …), falling
// back to the schema when the engine is unknown.
func pkNamespaceFor(dbType, defaultDB, defaultSchema string) string {
	if mgr, ok := cdc.NewProvider(dbType, nil); ok {
		return mgr.PrimaryKeyNamespace(defaultDB, defaultSchema)
	}
	if defaultSchema != "" {
		return defaultSchema
	}
	return defaultDB
}

// backfillMissingPKs applies the same primary-key policy as UpdateCDCTables
// before a snapshot is signalled: a relational destination needs a PK for
// upsert/delete. It answers the request itself on any failure and reports
// halt=true, so the caller just returns. The refusal shape is identical to
// UpdateCDCTables' — CDC auto-pickup reads {"error":"missing_primary_key",
// "tables":[…]} from both.
func backfillMissingPKs(ctx context.Context, c *gin.Context, db *sql.DB, requiresPK bool, sourceConnID, dbType, defaultDB, defaultSchema string, tables []string) (halt bool) {
	if !requiresPK {
		return false
	}
	mgr, ok := cdc.NewProvider(dbType, db)
	if !ok {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "cdc_pk_validation_unsupported",
			"message": fmt.Sprintf("PK validation is not supported for Debezium connector type %q", dbType),
		})
		return true
	}
	missing, verr := mgr.ValidateTablesHavePrimaryKeys(ctx, sourceConnID, mgr.PrimaryKeyNamespace(defaultDB, defaultSchema), tables)
	if verr != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": verr.Error()})
		return true
	}
	if len(missing) > 0 {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "missing_primary_key",
			"message": missingPrimaryKeyMessage,
			"tables":  missing,
		})
		return true
	}
	return false
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
