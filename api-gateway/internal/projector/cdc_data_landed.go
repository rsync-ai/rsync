package projector

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

// "Run after pipeline" for CDC pipelines.
//
// A model scheduled to rebuild after a pipeline runs is fired by OnPipelineCompleted, on
// PIPELINE_COMPLETED. A CDC pipeline never completes — it streams until someone stops it
// — so for every CDC upstream that trigger could never fire, and the schedule sat there
// looking armed while the model went stale.
//
// The CDC equivalent of "the pipeline ran" is "rows landed in the destination", and the
// only producer that knows that is the kafka-mcp-sink: its TABLE_STATS carry cumulative
// applied counters that advance only after a successful destination write. So the
// trigger here is: a sink-sourced CDC TABLE_STATS event whose applied_total_events is
// higher than what pipeline_run_table_stats already held for that table.
//
// Deliberately excluded:
//   - source cdc_stats_consumer (the orchestrator's cdcstats agent). It counts what was
//     CAPTURED from the source topic, not what landed; rebuilding off it would rebuild
//     from rows the destination may not have yet.
//   - any event that did not advance the applied counter (an idle sink's periodic
//     re-emit, an out-of-order older event, a replay).
//
// Throttle and idempotency, which are the same mechanism here: the event timestamp is
// bucketed into N-minute windows (N = CDC_AFTER_PIPELINE_INTERVAL_MINUTES, default 15),
// and the trigger fires at most once per pipeline per window. The window id becomes the
// occurrence handed to FireModelsAfterPipeline in place of an execution id, so the
// Temporal fan-out workflow id is upstream-fanout:pipeline:<id>:cdc-window-<N>m-<window>
// — the same ALLOW_DUPLICATE_FAILED_ONLY dedupe that makes a batch completion fire once
// makes a CDC window fire once, across projector restarts and replays. The in-memory
// map below only saves the round trip to Temporal and the downstream-model lookup on
// every event in a busy window.
//
// It is gated on `stored` exactly like the batch hook, so a redelivered or replayed
// TABLE_STATS message fires nothing.
//
// Known limits, both inherent in a leading-edge window:
//   - rows that land later in a window that has already fired are only reflected once
//     rows land again in a later window;
//   - two fires can be seconds apart when landings straddle a window boundary.

// CDCAfterPipelineIntervalEnv names the env var that sets the CDC after-pipeline window.
const CDCAfterPipelineIntervalEnv = "CDC_AFTER_PIPELINE_INTERVAL_MINUTES"

const defaultCDCAfterPipelineInterval = 15 * time.Minute

// cdcAfterPipelineIntervalFromEnv reads CDC_AFTER_PIPELINE_INTERVAL_MINUTES. Unset means
// the default; anything that is not a positive whole number of minutes is logged and
// also means the default, rather than a window of zero that would fire on every event.
func cdcAfterPipelineIntervalFromEnv() time.Duration {
	v := strings.TrimSpace(os.Getenv(CDCAfterPipelineIntervalEnv))
	if v == "" {
		return defaultCDCAfterPipelineInterval
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		log.WithFields(log.Fields{"env": CDCAfterPipelineIntervalEnv, "value": truncateLabel(v)}).
			Warn("Event Projector: invalid CDC after-pipeline interval, using the default of 15 minutes")
		return defaultCDCAfterPipelineInterval
	}
	return time.Duration(n) * time.Minute
}

// appliedProgress is what one TABLE_STATS upsert learned about destination progress.
// The zero value means "nothing to act on".
type appliedProgress struct {
	pipelineID string
	// increased is true only for a kafka_mcp_sink CDC event whose applied_total_events
	// exceeded the value stored before this upsert, and only when the hook is set.
	increased bool
	// appliedAt is the event's timestamp (what the upsert writes to last_applied_ts).
	appliedAt string
}

// dataLandedWindowMinutes is the configured window, in whole minutes.
func (p *EventProjector) dataLandedWindowMinutes() int64 {
	minutes := int64(p.dataLandedInterval / time.Minute)
	if minutes <= 0 {
		minutes = int64(defaultCDCAfterPipelineInterval / time.Minute)
	}
	return minutes
}

// dataLandedWindow returns the window id for ts: floor(unix seconds / window seconds).
func dataLandedWindow(ts time.Time, minutes int64) int64 {
	secs := ts.Unix()
	size := minutes * 60
	w := secs / size
	if secs < 0 && secs%size != 0 {
		w-- // floor, not truncation, for pre-epoch timestamps
	}
	return w
}

// dataLandedOccurrence is the occurrence label for a window. It names the window size as
// well as the window, so changing the interval cannot collide with an earlier window id.
func dataLandedOccurrence(minutes, window int64) string {
	return fmt.Sprintf("cdc-window-%dm-%d", minutes, window)
}

// maybeFireDataLanded fires OnPipelineDataLanded for progress, at most once per pipeline
// per window. It reports whether it fired. The caller has already checked that the event
// was stored for the first time.
func (p *EventProjector) maybeFireDataLanded(progress appliedProgress) bool {
	hook := p.OnPipelineDataLanded
	if hook == nil || !progress.increased || strings.TrimSpace(progress.pipelineID) == "" {
		return false
	}
	ts, err := time.Parse(time.RFC3339, progress.appliedAt)
	if err != nil {
		log.WithFields(log.Fields{"pipeline_id": progress.pipelineID}).
			Debug("Event Projector: CDC rows landed but the TABLE_STATS timestamp is unparseable; not firing after-pipeline models")
		return false
	}

	minutes := p.dataLandedWindowMinutes()
	window := dataLandedWindow(ts, minutes)

	p.landedMu.Lock()
	if p.landedWindow == nil {
		p.landedWindow = map[string]int64{}
	}
	if last, ok := p.landedWindow[progress.pipelineID]; ok && window <= last {
		p.landedMu.Unlock()
		log.WithFields(log.Fields{"pipeline_id": progress.pipelineID, "window": window}).
			Debug("Event Projector: CDC rows landed; after-pipeline models already fired for this window")
		return false
	}
	p.landedWindow[progress.pipelineID] = window
	p.landedMu.Unlock()

	occurrence := dataLandedOccurrence(minutes, window)
	log.WithFields(log.Fields{
		"pipeline_id":    progress.pipelineID,
		"occurrence":     occurrence,
		"window_minutes": minutes,
	}).Info("Event Projector: CDC rows landed; firing after-pipeline models for this window")
	hook(p.ctx, progress.pipelineID, occurrence)
	return true
}
