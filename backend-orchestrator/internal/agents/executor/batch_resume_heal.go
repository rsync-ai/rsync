package executor

import (
	"context"
	"database/sql"
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/rsync-ai/backend-orchestrator/internal/cdc"
)

// lostBatchExecutionsQuery finds earlier executions that still own a checkpoint of
// this pipeline and whose batches the sink negatively acked (rows_written = 0 with
// an error — the DLQ path) with no later positive ack for the same batch.
const lostBatchExecutionsQuery = `
	SELECT DISTINCT c.position->>'execution_id'
	FROM pipeline_checkpoints c
	WHERE c.pipeline_id = $1
	  AND COALESCE(c.position->>'execution_id', '') NOT IN ('', $2)
	  AND EXISTS (
	    SELECT 1 FROM pipeline_batch_acks n
	    WHERE n.pipeline_id = $1 AND n.execution_id::text = c.position->>'execution_id'
	      AND n.rows_written = 0 AND COALESCE(n.last_error, '') <> ''
	      AND NOT EXISTS (
	        SELECT 1 FROM pipeline_batch_acks p
	        WHERE p.pipeline_id = n.pipeline_id AND p.execution_id = n.execution_id
	          AND p.table_name = n.table_name AND p.batch_offset = n.batch_offset
	          AND p.rows_written > 0))`

// rewindCheckpointsBehindNegativeAcks is the resume-time half of B-RESUME-DLQ.
// The postflight rewinds a run's checkpoints only when it sees the loss itself
// (AckEvidencedDrop). A run that ended unverified (acks still in flight), was
// cancelled, or crashed never rewinds, and negative acks that land afterwards
// leave the lost batches behind the checkpoint: a Resume then reads nothing.
// Called once at the start of a non-reload run, this rewinds every such earlier
// execution so the Resume re-reads what the sink dropped. It is idempotent: a
// rewound checkpoint carries the earlier execution's id from run_start.
// Best-effort — a failure is logged and the run continues as before.
func rewindCheckpointsBehindNegativeAcks(ctx context.Context, db *sql.DB, pipelineID, executionID string) int64 {
	if db == nil || strings.TrimSpace(pipelineID) == "" {
		return 0
	}
	rows, err := db.QueryContext(ctx, lostBatchExecutionsQuery, pipelineID, executionID)
	if err != nil {
		log.WithError(err).WithField("pipeline_id", pipelineID).Warn("could not look for earlier runs with lost batches; a Resume may skip rows the sink dropped")
		return 0
	}
	var lost []string
	for rows.Next() {
		var id string
		if rows.Scan(&id) == nil && id != "" {
			lost = append(lost, id)
		}
	}
	rows.Close()
	var total int64
	for _, id := range lost {
		n, err := cdc.RewindCheckpointsOfExecution(ctx, db, pipelineID, id)
		if err != nil {
			log.WithError(err).WithFields(log.Fields{"pipeline_id": pipelineID, "lost_execution_id": id}).Error("could not rewind the checkpoints of an earlier run that lost batches; run a Reload to re-read every row")
			continue
		}
		total += n
	}
	if total > 0 {
		log.WithFields(log.Fields{"pipeline_id": pipelineID, "executions": lost, "checkpoints": total}).Warn("Rewound checkpoints of earlier runs the sink lost batches from; this run re-reads those rows")
	}
	return total
}
