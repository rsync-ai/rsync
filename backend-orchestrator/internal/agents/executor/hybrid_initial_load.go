package executor

import (
	"context"
	"errors"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/rsync-ai/backend-orchestrator/internal/cdcsnapshot"
)

// hybridLoadFailedMessage is the load's last_error when the batch fails. It is
// shown on the pipeline page, so it is a fixed sentence: the batch error itself
// can quote row values and stays in the run's error.
const hybridLoadFailedMessage = "the batch historical load failed and CDC was not started; see the pipeline's latest run"

// beginHybridInitialLoad records the batch historical load as the pipeline's
// initial load (cdc_snapshot_requests, source 'initial'), so the pipeline page
// shows "Full load in progress" and then "Load completed". A hybrid pipeline's
// connector streams from P and never snapshots, so no snapshot row records it
// the way the dispatcher records a Debezium load. Best-effort: nil when there is
// no queue, and the load runs either way.
func (a *Agent) beginHybridInitialLoad(ctx context.Context, task ExecutorTask, connectorName string) *cdcsnapshot.Request {
	if a.db == nil {
		return nil
	}
	r, err := cdcsnapshot.NewStore(a.db).BeginInitial(ctx, cdcsnapshot.Request{
		PipelineID:    task.PipelineID,
		ConnectorName: connectorName,
		Mode:          "blocking",
		Tables:        hybridTablesFromTask(task),
	})
	if err != nil {
		switch {
		case errors.Is(err, cdcsnapshot.ErrInitialLoadOpen):
			log.WithField("pipeline_id", task.PipelineID).
				Info("Hybrid CDC: another run of this pipeline is recording its initial load; this run's is not recorded")
		case !errors.Is(err, cdcsnapshot.ErrUnavailable):
			log.WithError(err).WithField("pipeline_id", task.PipelineID).
				Warn("⚠️ Hybrid CDC: could not record the initial load; the pipeline page will not show its progress")
		}
		return nil
	}
	return &r
}

// finishHybridInitialLoad closes what beginHybridInitialLoad recorded. It runs
// on a context the batch's cancellation cannot abort, so a stopped load is
// still closed rather than left "in progress".
func (a *Agent) finishHybridInitialLoad(ctx context.Context, load *cdcsnapshot.Request, ok bool) {
	if load == nil || a.db == nil {
		return
	}
	to, cause := cdcsnapshot.StatusCompleted, ""
	if !ok {
		to, cause = cdcsnapshot.StatusFailed, hybridLoadFailedMessage
	}
	fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	closed, err := cdcsnapshot.NewStore(a.db).Finish(fctx, *load, to, cause)
	switch {
	case err != nil:
		log.WithError(err).WithField("pipeline_id", load.PipelineID).
			Warn("⚠️ Hybrid CDC: could not close the recorded initial load")
	case !closed:
		// A newer run superseded this load, so the page shows that run's.
		log.WithFields(log.Fields{"pipeline_id": load.PipelineID, "request_id": load.ID, "outcome": to}).
			Warn("⚠️ Hybrid CDC: the recorded initial load was already closed by another run; this run's outcome is only in its run record")
	}
}
