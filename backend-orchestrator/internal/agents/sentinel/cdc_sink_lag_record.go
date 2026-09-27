package sentinel

import (
	"context"
	"time"

	log "github.com/sirupsen/logrus"
)

// recordSinkLag stores this tick's CDC sink drain reading in pipeline_sink_lag.
//
// checkSinkConsumerLag already reads the authoritative broker-side drain for every
// running CDC pipeline on each tick -- per-topic lag, the committed position, and
// whether the sink is still moving -- and before this function existed it kept none
// of it once the cdc-sink-lag-<id> decision was made. The pipeline page therefore
// showed lag from a DATA_PLANE_METRICS event that is only written when somebody
// loads GET /cdc/pipelines/:id/status, i.e. whenever the page was last opened; after
// a day without a visit the tile read "No reading" on a perfectly healthy stream.
// Writing the reading here makes it at most one tick old, watched or not.
//
// One row per pipeline, rewritten in place: this answers "right now", and the
// history of a lag reading belongs in monitoring events, not in a table the UI
// reads synchronously. The row is keyed on pipeline_id (not on a topic or a
// container, the way sentinel_component_health is) precisely so the read can be
// workspace-scoped -- infrastructure monitoring stays admin-only on admin/health,
// while the pipeline's own numbers reach the people who own the pipeline.
//
// Best-effort by construction. Every caller is mid-tick with an alarm decision still
// to make, and a pipeline whose lag row failed to write must still raise its alarm,
// so failures are logged and swallowed. A missing or stale row is a case the reader
// already has to handle (the writer only runs for running CDC pipelines, and only
// when the Kafka manager is wired in), and it renders as "no recent reading" rather
// than as a green zero -- which is the whole point of storing measured_at.
func (s *CDCSentinel) recordSinkLag(
	ctx context.Context,
	pipelineID string,
	consumerGroup string,
	totalLag int64,
	committed int64,
	moving bool,
	stalled bool,
	stalledFor time.Duration,
) {
	if s.db == nil {
		return
	}

	// stalledFor is only meaningful while stalled: observeSinkDrain returns the time
	// since the sink was last seen moving, which on a healthy sink is just the age of
	// the previous tick and would read as a stall duration to anyone displaying it.
	var stalledSeconds int64
	if stalled {
		stalledSeconds = int64(stalledFor.Seconds())
	}

	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO pipeline_sink_lag (
			pipeline_id, consumer_group, total_lag,
			committed, committed_moving, stalled, stalled_seconds, measured_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
		ON CONFLICT (pipeline_id) DO UPDATE SET
			consumer_group   = EXCLUDED.consumer_group,
			total_lag        = EXCLUDED.total_lag,
			committed        = EXCLUDED.committed,
			committed_moving = EXCLUDED.committed_moving,
			stalled          = EXCLUDED.stalled,
			stalled_seconds  = EXCLUDED.stalled_seconds,
			measured_at      = EXCLUDED.measured_at
	`, pipelineID, consumerGroup, totalLag,
		committed, moving, stalled, stalledSeconds); err != nil {
		log.WithError(err).WithFields(log.Fields{
			"pipeline_id":    pipelineID,
			"consumer_group": consumerGroup,
		}).Debug("🛡️ recordSinkLag: write failed")
	}
}
