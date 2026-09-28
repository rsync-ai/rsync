package handlers

import (
	"context"
	"database/sql"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"

	"github.com/rsync-ai/backend-orchestrator/internal/kafka"
	"github.com/rsync-ai/backend-orchestrator/internal/mcp"
	"github.com/rsync-ai/backend-orchestrator/internal/utils"
)

// StopBatchSinkWorkers stops the kafka-mcp-sink worker(s) consuming a BATCH
// pipeline's data topic.
//
// PUT /api/v1/pipelines/:id/sink/stop  (":id", not ":pipeline_id": gin panics when
// two routes name the same path segment differently, and /pipelines/:id/run and
// the assessment routes already own it)
//
// A batch Stop used to reach only Temporal (cancel signal) and the pipelines row.
// Nothing told kafka-mcp-sink, whose worker for the pipeline's "sink-<id8>-batch"
// group runs until stop_sink or SIGTERM (connector.py start_sink/stop_sink), so it
// kept draining everything the run had already produced into the destination —
// ~8.5 min of writes after the user pressed Stop on prod. The CDC Stop already
// stops its sink workers (cdc_stop.go); this is the batch half, with the same
// helpers and budget.
//
// Only "-batch" groups are stopped: a batch Stop has no business touching a CDC
// or streaming worker. stop_sink sets intentional_stop, so the supervisor does not
// respawn the worker; the group keeps its committed offset, so nothing already in
// Kafka is lost — the next run's start_sink for the same pipeline-scoped group
// resumes from that offset.
//
// Best effort, always 200: the gateway has already cancelled the run and flipped
// the row, and a worker that could not be stopped is reported as a warning.
func StopBatchSinkWorkers(db *sql.DB, mcpManager *mcp.ServerManager, tm *kafka.TopologyManager) gin.HandlerFunc {
	return stopBatchSinkWorkers(db, newSinkStopExecutor(mcpManager), tm)
}

func stopBatchSinkWorkers(db *sql.DB, sinks sinkStopExecutor, tm *kafka.TopologyManager) gin.HandlerFunc {
	return func(c *gin.Context) {
		pipelineID := strings.TrimSpace(c.Param("id"))
		if pipelineID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "pipeline_id is required"})
			return
		}
		if !assertPipelineOwnerForHandlers(c, db, pipelineID) {
			return
		}

		var groups, warnings []string
		if sinks != nil {
			ctx, cancel := context.WithTimeout(c.Request.Context(), cdcStopSinkBudget)
			names := pipelineKafkaNames{id8: utils.SafeID8(pipelineID), uuid: pipelineID}
			if id8IsUnique(ctx, db, pipelineID, names.id8) {
				groups = batchSinkGroups(discoverSinkGroups(ctx, tm, names))
				warnings = stopSinkWorkers(ctx, sinks, pipelineID, groups)
			} else {
				warnings = []string{sharedID8TeardownWarning}
			}
			cancel()
		}

		log.WithFields(log.Fields{
			"pipeline_id": pipelineID, "groups": groups, "warnings": len(warnings),
		}).Info("batch stop: sink workers stopped; consumer-group offsets kept")
		c.JSON(http.StatusOK, gin.H{
			"success": true, "pipeline_id": pipelineID, "consumer_groups": groups, "warnings": warnings,
		})
	}
}

// batchSinkGroups keeps the batch-phase groups ("sink-<id8>-batch", bare or
// namespace-qualified) — the only ones a batch run's executor starts
// (executor.sinkConsumerGroup for a pipeline.<id> topic).
func batchSinkGroups(groups []string) []string {
	var out []string
	for _, g := range groups {
		if strings.HasSuffix(g, "-batch") {
			out = append(out, g)
		}
	}
	return out
}
