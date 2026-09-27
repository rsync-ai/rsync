package handlers

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"

	"github.com/rsync-ai/backend-orchestrator/internal/kafka"
	"github.com/rsync-ai/backend-orchestrator/internal/mcp"
	"github.com/rsync-ai/backend-orchestrator/internal/utils"
)

// cdcStopSinkBudget bounds the best-effort sink stop so a slow sink service cannot
// hold the user's Stop open past the gateway's 15 s wait.
const cdcStopSinkBudget = 8 * time.Second

// StopCDCPipeline stops a CDC pipeline and keeps its position, for every Debezium
// source (the connector name is resolved, not assumed per database).
//
// PUT /api/v1/cdc/pipelines/:pipeline_id/stop
//
// Stop used to be batch-only: the gateway signalled the pipeline's latest execution
// (for CDC, the finished snapshot) and flipped the row to `stopped`, so the
// connector and sink kept streaming. The reconciler then reaped the connector, slot
// and publication of the "stopped" pipeline, and Start made a new slot at the
// current LSN while reusing the old offsets: every change made while stopped was
// lost.
//
// Now Stop parks the connector in Kafka Connect's STOPPED state (config and source
// offsets kept, no tasks), stops the sink workers (their consumer groups keep the
// committed offsets), and leaves the replication slot and publication alone, so
// Start resumes where Stop left off. A stopped PostgreSQL slot retains WAL; the WAL
// watchdog bounds it (cdc_wal_watchdog.go).
//
// Fails loud: a connector Kafka Connect refuses to stop answers 502 and the row
// keeps its status, so the UI never says stopped while the connector streams.
func StopCDCPipeline(db *sql.DB, mcpManager *mcp.ServerManager, tm *kafka.TopologyManager) gin.HandlerFunc {
	return stopCDCPipeline(db, newSinkStopExecutor(mcpManager), tm)
}

func stopCDCPipeline(db *sql.DB, sinks sinkStopExecutor, tm *kafka.TopologyManager) gin.HandlerFunc {
	return func(c *gin.Context) {
		pipelineID := strings.TrimSpace(c.Param("pipeline_id"))
		if pipelineID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "pipeline_id is required"})
			return
		}
		if !assertPipelineOwnerForHandlers(c, db, pipelineID) {
			return
		}

		ctx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
		defer cancel()

		connectorName, err := findConnectorName(ctx, db, pipelineID)
		if err != nil || strings.TrimSpace(connectorName) == "" {
			c.JSON(http.StatusBadRequest, gin.H{
				"success": false, "pipeline_id": pipelineID,
				"error": "cannot resolve the CDC connector for this pipeline",
			})
			return
		}

		outcome, httpStatus, errMsg := stopConnector(ctx, getKafkaConnectURL(), connectorName)
		if httpStatus != http.StatusOK {
			c.JSON(httpStatus, gin.H{
				"success": false, "pipeline_id": pipelineID, "connector_name": connectorName,
				"error": errMsg,
			})
			return
		}

		// Best effort: a sink left running only drains what was already captured,
		// and its group keeps the offsets either way.
		var warnings []string
		if sinks != nil {
			sinkCtx, sinkCancel := context.WithTimeout(c.Request.Context(), cdcStopSinkBudget)
			names := pipelineKafkaNames{id8: utils.SafeID8(pipelineID), uuid: pipelineID}
			if id8IsUnique(sinkCtx, db, pipelineID, names.id8) {
				warnings = stopSinkWorkers(sinkCtx, sinks, pipelineID, discoverSinkGroups(sinkCtx, tm, names))
			} else {
				warnings = []string{sharedID8TeardownWarning}
			}
			sinkCancel()
		}

		if _, err := db.ExecContext(c.Request.Context(),
			`UPDATE pipelines SET status = 'stopped', updated_at = NOW() WHERE id = $1`, pipelineID); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"success": false, "pipeline_id": pipelineID, "connector_name": connectorName,
				"error": "connector stopped but the pipeline status could not be saved: " + err.Error(),
			})
			return
		}

		log.WithFields(log.Fields{
			"pipeline_id": pipelineID, "connector": connectorName, "outcome": outcome, "warnings": len(warnings),
		}).Info("CDC pipeline stopped; slot, publication and offsets kept")
		c.JSON(http.StatusOK, gin.H{
			"success": true, "pipeline_id": pipelineID, "connector_name": connectorName,
			"result": gin.H{"connector": outcome}, "warnings": warnings,
		})
	}
}

// stopConnector parks a connector in STOPPED. It returns what happened ("stopped";
// "paused" on a Kafka Connect older than 3.5, which has no /stop; "absent"), and
// 200, or the status to answer and why.
func stopConnector(ctx context.Context, connectURL, connectorName string) (string, int, string) {
	code, err := putConnectNoBody(ctx, connectURL, "/connectors/"+connectorName+"/stop")
	if err != nil {
		return "", http.StatusServiceUnavailable, fmt.Sprintf("kafka connect unreachable: %v", err)
	}
	if code >= 200 && code < 300 {
		return "stopped", http.StatusOK, ""
	}
	if code != http.StatusNotFound {
		return "", http.StatusBadGateway, fmt.Sprintf("kafka connect refused the stop of %s (HTTP %d)", connectorName, code)
	}
	// 404 is either no such connector or a Connect without the /stop endpoint.
	_, statusCode, err := getConnectorStatus(ctx, connectURL, connectorName)
	switch {
	case err == nil:
		code, err := putConnectNoBody(ctx, connectURL, "/connectors/"+connectorName+"/pause")
		if err != nil {
			return "", http.StatusServiceUnavailable, fmt.Sprintf("kafka connect unreachable: %v", err)
		}
		if code >= 200 && code < 300 {
			return "paused", http.StatusOK, ""
		}
		return "", http.StatusBadGateway, fmt.Sprintf("kafka connect refused the pause of %s (HTTP %d)", connectorName, code)
	case statusCode == http.StatusNotFound:
		// Nothing streams; Start creates the connector again.
		return "absent", http.StatusOK, ""
	case statusCode == 0:
		return "", http.StatusServiceUnavailable, fmt.Sprintf("kafka connect unreachable: %v", err)
	default:
		return "", http.StatusBadGateway, fmt.Sprintf("kafka connect status of %s failed (HTTP %d)", connectorName, statusCode)
	}
}
