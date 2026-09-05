package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"

	"github.com/rsync-ai/backend-orchestrator/internal/kafka"
	"github.com/rsync-ai/backend-orchestrator/internal/mcp"
	"github.com/rsync-ai/shared/crypto"
)

// RestartCDCSink restarts the kafka-mcp-sink worker for a CDC pipeline so it picks up newly added Debezium topics.
// This is required because a long-lived Kafka consumer group won't automatically subscribe to new topics.
//
// POST /api/v1/cdc/pipelines/:pipeline_id/sink/restart
//
// This gin handler is a thin wrapper: it does param validation, the tenant-owner gate, and
// HTTP status/body mapping around restartCDCSinkWorker, which holds the actual stop_sink +
// start_sink logic so a background caller (the CDC Sentinel's autonomous wedge-recovery rung)
// can reuse the exact same restart path.
func RestartCDCSink(db *sql.DB, mcpManager *mcp.ServerManager) gin.HandlerFunc {
	return func(c *gin.Context) {
		pipelineID := strings.TrimSpace(c.Param("pipeline_id"))
		if pipelineID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "pipeline_id is required"})
			return
		}
		if _, err := uuid.Parse(pipelineID); err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "Pipeline not found"})
			return
		}

		// SECURITY (tenant isolation): gate cross-tenant CDC sink restart.
		if !assertPipelineOwnerForHandlers(c, db, pipelineID) {
			return
		}

		ctx, cancel := context.WithTimeout(c.Request.Context(), 60*time.Second)
		defer cancel()

		result, status, err := restartCDCSinkWorker(ctx, db, mcpManager, pipelineID)
		if err != nil {
			if result != nil {
				// Rich failure (stop/start were attempted) — preserve the detailed result body.
				c.JSON(status, gin.H{"success": false, "error": err.Error(), "result": result})
			} else {
				// Early failure — plain {"error": msg} body, same as before extraction.
				c.JSON(status, gin.H{"error": err.Error()})
			}
			return
		}
		c.JSON(status, gin.H{"success": true, "result": result})
	}
}

// RestartCDCSinkWorker performs the stop_sink+start_sink restart of a CDC pipeline's
// kafka-mcp-sink worker with NO gin/HTTP context, so a background caller (the CDC Sentinel's
// autonomous wedge-recovery rung) can reuse the exact same restart path as the manual
// edit-tables endpoint. It returns nil once start_sink is accepted, or an error if the restart
// could not be issued/accepted — which is all the Sentinel's attempt-cap bookkeeping needs.
func RestartCDCSinkWorker(ctx context.Context, db *sql.DB, mcpManager *mcp.ServerManager, pipelineID string) error {
	_, _, err := restartCDCSinkWorker(ctx, db, mcpManager, pipelineID)
	return err
}

// restartCDCSinkWorker is the shared, context-only core of the CDC sink restart. It returns:
//   - (successResult, http.StatusOK, nil) on success;
//   - (nil, <status>, err) for an early failure whose HTTP body is just {"error": msg};
//   - (richResult, http.StatusBadGateway, err) when start_sink itself fails, whose HTTP body is
//     {"success": false, "error": msg, "result": richResult}.
//
// The gin wrapper reproduces each of those response shapes exactly, so extracting this core is
// behavior-preserving for the existing endpoint.
func restartCDCSinkWorker(ctx context.Context, db *sql.DB, mcpManager *mcp.ServerManager, pipelineID string) (gin.H, int, error) {
	// Determine Debezium connector config (topic.prefix + table.include.list).
	connectorName, err := findConnectorName(ctx, db, pipelineID)
	if err != nil {
		return nil, http.StatusNotFound, fmt.Errorf("connector not found for pipeline: %s", err.Error())
	}

	connectURL := strings.TrimRight(getKafkaConnectURL(), "/")
	cfg, err := fetchKafkaConnectConfig(ctx, connectURL, connectorName)
	if err != nil {
		return nil, http.StatusBadGateway, err
	}

	topicPrefix := strings.TrimSpace(fmt.Sprint(cfg["topic.prefix"]))
	if topicPrefix == "" {
		return nil, http.StatusBadRequest, errors.New("missing topic.prefix in connector config")
	}

	tableIncludeList := strings.TrimSpace(fmt.Sprint(cfg["table.include.list"]))
	if tableIncludeList == "" {
		return nil, http.StatusBadRequest, errors.New("missing table.include.list in connector config")
	}

	tables := splitCommaList(tableIncludeList)
	topics := make([]string, 0, len(tables))
	seen := map[string]struct{}{}
	for _, t := range tables {
		tt := strings.TrimSpace(t)
		if tt == "" {
			continue
		}
		topic := tt
		if !strings.HasPrefix(topic, topicPrefix+".") {
			topic = topicPrefix + "." + tt
		}
		if _, ok := seen[topic]; ok {
			continue
		}
		seen[topic] = struct{}{}
		topics = append(topics, topic)
	}
	if len(topics) == 0 {
		return nil, http.StatusBadRequest, errors.New("no topics derived from table.include.list")
	}

	// Destination connection (decrypt config) so sink can write.
	destConnID, err := findPipelineDestinationConnectionID(ctx, db, pipelineID)
	if err != nil {
		return nil, http.StatusNotFound, err
	}
	destConnector, destVersion, destCfg, err := getDecryptedConnectionForSink(ctx, db, destConnID)
	if err != nil {
		return nil, http.StatusBadRequest, fmt.Errorf("failed to load destination config: %s", err.Error())
	}
	destConcreteVer := strings.TrimSpace(destVersion)
	if strings.TrimSpace(destConnector) != "" && mcpManager != nil {
		if v, rerr := mcpManager.ResolveConcreteVersion(destConnector, destVersion); rerr == nil && strings.TrimSpace(v) != "" {
			destConcreteVer = strings.TrimSpace(v)
		}
	}

	// Destination namespace (schema/db) from pipelines.config so CDC writes land
	// in <namespace>.<table> (mirrors the batch path's resolveDestinationNamespace).
	// Empty/"default" is safely ignored downstream (sink + connector skip it).
	var destNamespace string
	{
		var ns sql.NullString
		_ = db.QueryRowContext(ctx,
			`SELECT NULLIF(TRIM(COALESCE(config->>'destination_namespace','')), '') FROM pipelines WHERE id = $1::uuid`,
			pipelineID,
		).Scan(&ns)
		if ns.Valid {
			destNamespace = strings.TrimSpace(ns.String)
		}
	}

	// The group the sink ACTUALLY registered — not the derived default. This function
	// both stops and starts a worker, so a wrong name here does not fail quietly: the
	// stop hits nothing and the start registers a second worker under a group no probe
	// reads. See ResolveSinkConsumerGroup.
	consumerGroup := ResolveSinkConsumerGroup(ctx, db, pipelineID)
	client := mcp.NewClient(mcpManager)

	// Best-effort: stop current worker (may not exist if sink wasn't started yet).
	stopResp, _ := client.ExecuteWithContext(ctx, mcp.ExecuteRequest{
		Connector: "kafka-mcp-sink",
		Operation: "stop_sink",
		Config:    map[string]string{},
		Params: map[string]interface{}{
			"config": map[string]interface{}{
				"consumer_group": consumerGroup,
			},
		},
	})
	if stopResp != nil && !stopResp.Success {
		log.WithFields(log.Fields{
			"pipeline_id":     pipelineID,
			"consumer_group":  consumerGroup,
			"stop_sink_error": stopResp.Error,
		}).Warn("cdc sink restart: stop_sink returned failure (continuing)")
	}

	startResp, startErr := client.ExecuteWithContext(ctx, mcp.ExecuteRequest{
		Connector: "kafka-mcp-sink",
		Operation: "start_sink",
		Config:    map[string]string{},
		Params: map[string]interface{}{
			"config": map[string]interface{}{
				// Derived, not hardcoded: the restart path must reach the same
				// cluster the rest of the platform uses (see kafka.SinkBootstrapServers).
				"kafka_bootstrap_servers": kafka.SinkBootstrapServers(),
				"topics":                  topics,
				"consumer_group":          consumerGroup,
				"start_offset":            "earliest",
				"sink_mode":               "cdc",
				"pipeline_id":             pipelineID,
				// CDC stats use stable execution_id == pipeline_id.
				"execution_id":          pipelineID,
				"destination_connector": destConnector,
				"destination_version":   destConcreteVer,
				"destination_config":    destCfg,
				"destination_namespace": destNamespace,
			},
		},
	})
	if startErr != nil || startResp == nil || !startResp.Success {
		msg := ""
		if startErr != nil {
			msg = startErr.Error()
		} else if startResp != nil {
			msg = startResp.Error
		}
		return gin.H{
			"connector_name":   connectorName,
			"consumer_group":   consumerGroup,
			"topics":           topics,
			"destination":      destConnector,
			"destination_ver":  destConcreteVer,
			"stop_sink_result": stopResp,
		}, http.StatusBadGateway, fmt.Errorf("failed to restart sink: %s", msg)
	}

	return gin.H{
		"connector_name":      connectorName,
		"consumer_group":      consumerGroup,
		"topics":              topics,
		"destination":         destConnector,
		"destination_version": destConcreteVer,
		"stop_sink":           stopResp,
		"start_sink":          startResp,
	}, http.StatusOK, nil
}

func splitCommaList(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		v := strings.TrimSpace(p)
		if v != "" {
			out = append(out, v)
		}
	}
	return out
}

func findPipelineDestinationConnectionID(ctx context.Context, db *sql.DB, pipelineID string) (string, error) {
	var destConnID sql.NullString
	err := db.QueryRowContext(ctx, `SELECT destination_connection_id::text FROM pipelines WHERE id = $1::uuid`, pipelineID).Scan(&destConnID)
	if err != nil || !destConnID.Valid || strings.TrimSpace(destConnID.String) == "" {
		return "", fmt.Errorf("destination_connection_id not found for pipeline")
	}
	return strings.TrimSpace(destConnID.String), nil
}

func getDecryptedConnectionForSink(ctx context.Context, db *sql.DB, connectionID string) (connectorType string, connectorVersion string, cfg map[string]string, err error) {
	var encryptedConfig string
	var connType string
	var ctype string
	var cver sql.NullString
	if err := db.QueryRowContext(ctx, `SELECT type, connector_type, connector_version, config FROM connections WHERE id = $1`, connectionID).
		Scan(&connType, &ctype, &cver, &encryptedConfig); err != nil {
		return "", "", nil, err
	}
	connectorType = strings.TrimSpace(ctype)
	if cver.Valid {
		connectorVersion = strings.TrimSpace(cver.String)
	}
	if connectorVersion == "" {
		connectorVersion = "latest"
	}

	decryptedJSON, derr := crypto.DecryptString(strings.TrimSpace(encryptedConfig))
	if derr != nil {
		return "", "", nil, derr
	}
	var raw map[string]interface{}
	if uerr := json.Unmarshal([]byte(decryptedJSON), &raw); uerr != nil {
		return "", "", nil, uerr
	}
	out := make(map[string]string, len(raw)+1)
	for k, v := range raw {
		out[k] = fmt.Sprint(v)
	}
	// Helpful for downstream components that inspect config for versioning.
	out["connector_version"] = connectorVersion
	return connectorType, connectorVersion, out, nil
}
