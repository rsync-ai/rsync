package sentinel

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/rsync-ai/backend-orchestrator/internal/handlers"
)

// DefaultSinkStaleBound is wedge signal #2's threshold: how long the destination-apply marker
// (MAX(last_applied_ts)) may go without advancing before the sink is considered stale. It
// matches the UI liveness bound (StaleSeconds>300 → "idle") so both surfaces agree on when a
// CDC sink has stopped delivering. Overridable via CDC_SINK_STALE_BOUND.
const DefaultSinkStaleBound = 5 * time.Minute

// sinkAutoRestartEnabled reports whether the autonomous sink-restart rung is armed. It is an
// OPERATIONAL-SAFETY gate (mirrors CDC_RECOVERY_ENABLED), NOT an edition gate: it defaults OFF
// in every deployment — cloud and OSS alike — so turning it on is an explicit operator opt-in,
// never a cloud-vs-OSS behavior split. Only a case-insensitive, trimmed "true" arms it; any
// other value (including "1") leaves it disarmed so a stray value can't silently start
// restarting sinks.
func sinkAutoRestartEnabled() bool {
	return strings.EqualFold(strings.TrimSpace(os.Getenv("CDC_SINK_AUTORESTART_ENABLED")), "true")
}

// sinkStaleBound returns the destination-apply staleness threshold (wedge signal #2). A
// positive duration in CDC_SINK_STALE_BOUND overrides the default; a non-positive or
// unparseable value falls back to DefaultSinkStaleBound rather than arming a 0s hair-trigger.
func sinkStaleBound() time.Duration {
	if raw := strings.TrimSpace(os.Getenv("CDC_SINK_STALE_BOUND")); raw != "" {
		if d, err := time.ParseDuration(raw); err == nil && d > 0 {
			return d
		}
	}
	return DefaultSinkStaleBound
}

// loadSinkAppliedProgress reads the destination-apply progress marker for a CDC pipeline:
// MAX(last_applied_ts) from pipeline_run_table_stats scoped to mode='cdc'. This column is the
// sink's OWN post-apply timestamp (source="kafka_mcp_sink") — destination truth. It is
// deliberately NOT the source-side last_event_ts (Debezium ts_ms), which the independent
// cdcstats consumer keeps fresh while the sink is wedged; keying the wedge signal on the
// source column would make a dead sink look alive. Family-agnostic: mode='cdc' covers MySQL
// and PostgreSQL sources identically. A missing row / query error returns an invalid time,
// which deriveSinkProgressSignals treats as "no wedge evidence" (never fires on absence).
func loadSinkAppliedProgress(ctx context.Context, db *sql.DB, pipelineID string) sql.NullTime {
	var lastApplied sql.NullTime
	if err := db.QueryRowContext(ctx, `
		SELECT MAX(last_applied_ts) FROM pipeline_run_table_stats
		WHERE pipeline_id = $1 AND mode = 'cdc'
	`, pipelineID).Scan(&lastApplied); err != nil {
		log.WithError(err).WithField("pipeline_id", pipelineID).
			Debug("🛡️ sink auto-restart: last_applied_ts query failed (treating as unknown)")
	}
	return lastApplied
}

// sinkWedgedEscalationIssueID is the sentinel_active_issues key for the TERMINAL escalation the
// rung raises once the per-pipeline restart cap is exhausted. It is deliberately DISTINCT from
// the ongoing "cdc-sink-lag-<id>" alarm so the escalation never stomps (or auto-resolves) that
// alarm — it is its own operator-visible, deduped row.
func sinkWedgedEscalationIssueID(pipelineID string) string {
	return "cdc-sink-wedged-" + pipelineID
}

// maybeAutoRestartSink is Phase B: the autonomous wedge-recovery rung. checkSinkConsumerLag calls
// it every poll tick with the CORRECTED, topic-scoped lag verdict (Phase A) as `lagging`. It:
//
//  1. stays completely dormant unless CDC_SINK_AUTORESTART_ENABLED is armed (default off) — so the
//     default behavior in every deployment, cloud and OSS alike, is unchanged;
//  2. is a no-op until the MCP manager has been plumbed in (SetMCPManager), so it can't fire during
//     startup before the sink transport exists;
//  3. reads the destination-truth apply marker (MAX(last_applied_ts)) and derives the two non-lag
//     wedge signals (stale + flat) against the PREVIOUS tick's marker, held per pipeline;
//  4. delegates the actual FIRE/ESCALATE/SKIP decision to the pure decideSinkRestart, which fires
//     only on the genuine three-signal wedge (lagging AND stale AND flat) and enforces the same
//     per-pipeline attempt cap / rolling window / cooldown as handleFailedConnector.
//
// On FIRE it reuses the exact manual restart path (handlers.RestartCDCSinkWorker → stop_sink +
// start_sink). On ESCALATE (cap exhausted) it raises a distinct terminal issue and — unlike a
// failed SOURCE connector — does NOT stop the pipeline: a wedged sink sits on a healthy source
// slot, and dropping that slot would turn a recoverable stall into data loss. Family-agnostic:
// every signal keys on mode='cdc', so MySQL and PostgreSQL sources are handled identically.
func (s *CDCSentinel) maybeAutoRestartSink(ctx context.Context, pipelineID, pipelineName, dbType string, lagging bool) {
	if !sinkAutoRestartEnabled() {
		return
	}

	// The MCP manager is plumbed in post-construction (SetMCPManager); until then — and in any
	// unit context that never sets it — the rung is inert. Read it (and the shared maps) under mu.
	s.mu.Lock()
	mcpManager := s.mcpManager
	s.mu.Unlock()
	if mcpManager == nil {
		return
	}

	now := time.Now()

	// Signals #2 (stale) and #3 (flat) come from the destination-apply marker. Read the current
	// marker OUTSIDE the lock (it does DB I/O), then swap it against the previous tick's value.
	current := loadSinkAppliedProgress(ctx, s.db, pipelineID)

	s.mu.Lock()
	prev := s.sinkProgress[pipelineID]
	s.sinkProgress[pipelineID] = current
	st := s.sinkRestartState[pipelineID]
	if st == nil {
		st = &connRestartState{}
		s.sinkRestartState[pipelineID] = st
	}
	stCopy := *st
	s.mu.Unlock()

	stale, flat := deriveSinkProgressSignals(current, prev, now, sinkStaleBound())

	decision, newSt := decideSinkRestart(sinkRestartInputs{
		enabled:      true, // sinkAutoRestartEnabled() already asserted above
		lagging:      lagging,
		sinkStale:    stale,
		progressFlat: flat,
		now:          now,
		st:           stCopy,
		maxAttempts:  maxConnectorRestartAttempts(),
		window:       connectorRestartWindow(),
		cooldown:     connectorRestartCooldown(),
	})

	// Persist the updated bookkeeping (the entry is never deleted, so the pointer is still live;
	// re-init defensively in case a future change ever prunes the map).
	s.mu.Lock()
	if cur := s.sinkRestartState[pipelineID]; cur != nil {
		*cur = newSt
	} else {
		nc := newSt
		s.sinkRestartState[pipelineID] = &nc
	}
	s.mu.Unlock()

	// Must be the group the sink actually registered, not the derived default —
	// restarting "sink-<pid8>" for a streaming_only pipeline whose real group is
	// "sink-<pid8>-<eid8>" would stop nothing and report success.
	consumerGroup := s.resolveSinkConsumerGroup(ctx, pipelineID)

	switch decision {
	case sinkRestartSkip:
		return

	case sinkRestartFire:
		log.WithFields(log.Fields{
			"pipeline_id":    pipelineID,
			"consumer_group": consumerGroup,
			"attempt":        newSt.attempts,
			"max":            maxConnectorRestartAttempts(),
			"db_type":        dbType,
		}).Warn("🛡️ CDC sink wedged (real backlog + stale + flat destination progress) — auto-restarting sink worker")

		if err := handlers.RestartCDCSinkWorker(ctx, s.db, mcpManager, pipelineID); err != nil {
			// The attempt already counts toward the cap (decideSinkRestart recorded it before we
			// fired), so a failed restart still advances toward escalation rather than looping.
			log.WithError(err).WithFields(log.Fields{
				"pipeline_id":    pipelineID,
				"consumer_group": consumerGroup,
			}).Warn("🛡️ CDC sink auto-restart request failed (attempt counted; will escalate at cap)")
			return
		}
		log.WithFields(log.Fields{
			"pipeline_id":    pipelineID,
			"consumer_group": consumerGroup,
			"attempt":        newSt.attempts,
		}).Info("🛡️ CDC sink auto-restart issued — awaiting drain confirmation on the next poll")

	case sinkRestartEscalate:
		log.WithFields(log.Fields{
			"pipeline_id":    pipelineID,
			"consumer_group": consumerGroup,
			"attempts":       maxConnectorRestartAttempts(),
			"db_type":        dbType,
		}).Error("🛡️ CDC sink still wedged after max auto-restart attempts — escalating for manual intervention")

		// Distinct, deduped terminal issue. We do NOT markPipelineStopped here (unlike a failed
		// source connector): the source slot is healthy and streaming; only the destination sink
		// is stuck. Surfacing the wedge lets an operator intervene without losing the WAL position.
		s.emitLagIssue(ctx, sinkWedgedEscalationIssueID(pipelineID), "sink_wedged_escalation",
			pipelineID, pipelineName, dbType,
			fmt.Sprintf("CDC sink for this pipeline is still not draining Kafka after %d automatic restart attempts (consumer group: %s). The sink worker is likely permanently wedged — manual intervention required (the source connector is still healthy and capturing changes).",
				maxConnectorRestartAttempts(), consumerGroup),
			map[string]interface{}{
				"consumer_group": consumerGroup,
				"attempts":       maxConnectorRestartAttempts(),
				"terminal":       true,
			})
	}
}
