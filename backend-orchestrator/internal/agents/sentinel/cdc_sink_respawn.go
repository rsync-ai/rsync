package sentinel

import (
	"context"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/rsync-ai/backend-orchestrator/internal/handlers"
	"github.com/rsync-ai/backend-orchestrator/internal/mcp"
)

// The kafka-mcp-sink container keeps its worker registry in memory (self.workers). Its
// supervisor thread respawns a worker whose PROCESS died, but nothing survives the
// CONTAINER dying: on restart the registry is empty, every CDC and batch sink worker is
// gone, and no component notices. Debezium keeps capturing, the source slot/binlog stays
// current, the pipeline reads "running" — and zero rows reach the destination. That is
// exactly the shape this repo keeps producing: two pieces of code answer "is the sink
// running?" and disagree.
//
// This rung closes it with a DIFFERENT trigger from the wedge rung in
// cdc_sink_autorestart.go. That one infers a wedge from three noisy signals (lag + stale
// + flat) and is therefore gated behind CDC_SINK_AUTORESTART_ENABLED, off everywhere.
// This one fires on a FACT the sink container reports about itself: sink_status returns
// status="not_found", meaning the container holds no worker for this consumer group at
// all. Restarting a worker that provably does not exist cannot race a live worker, cannot
// reset an offset, and cannot lose data — so it needs no operator opt-in.
//
// Everything else stays fail-closed: a transport error, an unreachable container, or any
// status other than the literal "not_found" is treated as "unknown" and skipped.

// The probe itself — sinkStatusNotFound, the tri-state, and probeSinkPresence — lives in
// sink_presence.go, because the batch sentinel needs the same question answered the same
// way. Only the respawn decision below is CDC-specific.

// sinkRespawnGraceAfterStart is how long after the sentinel starts to leave absent workers
// alone. On a full-stack boot the orchestrator often comes up before the sink container has
// finished starting, and the CDC executor is itself about to issue start_sink; respawning
// during that window would race the normal startup path for no benefit.
const sinkRespawnGraceAfterStart = 2 * time.Minute

// sinkAbsentIssueID keys the operator-visible issue this rung raises once its attempt cap is
// exhausted. Distinct from cdc-sink-lag-* and cdc-sink-wedged-* so the three never stomp or
// auto-resolve each other.
func sinkAbsentIssueID(pipelineID string) string {
	return "cdc-sink-absent-" + pipelineID
}

// sinkRespawnDecision is the outcome of the absent-worker gate.
type sinkRespawnDecision int

const (
	// sinkRespawnSkip: the worker is present, unknown, still cooling down, or the cap was
	// already escalated.
	sinkRespawnSkip sinkRespawnDecision = iota
	// sinkRespawnFire: issue one bounded start_sink and record the attempt.
	sinkRespawnFire
	// sinkRespawnEscalate: the per-pipeline cap is exhausted inside the rolling window —
	// stop respawning and escalate terminally, exactly once.
	sinkRespawnEscalate
)

// sinkRespawnInputs is the side-effect-free input to the gate.
type sinkRespawnInputs struct {
	presence    sinkPresence // what the last sink_status call established
	now         time.Time
	st          connRestartState
	maxAttempts int
	window      time.Duration
	cooldown    time.Duration
}

// decideSinkRespawn is the pure absent-worker gate. Unlike decideSinkRestart it has no
// feature flag and only one signal, because the signal is a fact rather than an inference.
// It keeps the same attempt cap / rolling window / cooldown so a start_sink that keeps
// failing (bad destination config, sink container permanently broken) is retried a bounded
// number of times and then escalated once, instead of hammering the container every tick.
//
// A worker that comes back PRESENT clears the bookkeeping, so a later, unrelated container
// restart gets a fresh budget rather than inheriting an exhausted one. UNKNOWN does not:
// it is not evidence of recovery, and refilling the budget on it would let a container
// that alternates between "absent" and "erroring" reset its cap on every other tick and
// never reach the escalation that tells an operator to look.
func decideSinkRespawn(in sinkRespawnInputs) (sinkRespawnDecision, connRestartState) {
	st := in.st

	switch in.presence {
	case sinkPresencePresent:
		// Confirmed back: reset so the next genuine absence starts from a clean cap.
		return sinkRespawnSkip, connRestartState{}
	case sinkPresenceUnknown:
		// We learned nothing. Do nothing, and change nothing — including the budget.
		return sinkRespawnSkip, st
	}

	if !st.firstAttempt.IsZero() && in.now.Sub(st.firstAttempt) > in.window {
		st.attempts = 0
		st.firstAttempt = in.now
		st.escalated = false
	}
	if st.firstAttempt.IsZero() {
		st.firstAttempt = in.now
	}

	if st.attempts >= in.maxAttempts {
		if st.escalated {
			return sinkRespawnSkip, st
		}
		st.escalated = true
		return sinkRespawnEscalate, st
	}

	if !st.lastAttempt.IsZero() && in.now.Sub(st.lastAttempt) < in.cooldown {
		return sinkRespawnSkip, st
	}

	// Record BEFORE the caller acts: a start_sink that is accepted but whose worker dies
	// again immediately still burns an attempt, so a crash-loop escalates instead of looping.
	st.attempts++
	st.lastAttempt = in.now
	return sinkRespawnFire, st
}

// ensureSinkWorkerPresent asks the sink container whether it still holds a worker for this
// pipeline's consumer group and, if it definitively does not, re-issues the start. It runs
// for every running CDC pipeline on the source-lag tick, so a sink-container restart is
// healed within one poll interval instead of silently stalling every pipeline until someone
// notices the destination has stopped moving.
func (s *CDCSentinel) ensureSinkWorkerPresent(ctx context.Context, pipelineID, pipelineName, dbType string) {
	// The MCP manager is plumbed in post-construction (SetMCPManager); until then the rung
	// is inert, as it is in any unit context that never sets it.
	s.mu.Lock()
	mcpManager := s.mcpManager
	startedAt := s.startedAt
	s.mu.Unlock()
	if mcpManager == nil {
		return
	}

	now := time.Now()
	if !startedAt.IsZero() && now.Sub(startedAt) < sinkRespawnGraceAfterStart {
		return
	}

	// Must be the group the sink ACTUALLY registered, not the derived default — see
	// resolveSinkConsumerGroup. This probe asks the container "do you hold a worker for
	// this group?", so a wrong name is not a silent no-op here: the container correctly
	// answers "no", sinkWorkerAbsent goes true on a perfectly healthy pipeline, and the
	// rung re-issues start_sink on every source-lag tick until it hits the attempt cap
	// and escalates. That is the failure mode for any pipeline in cdc_mode
	// streaming_only|never, whose real group is sink-<pid8>-<eid8>.
	consumerGroup := s.resolveSinkConsumerGroup(ctx, pipelineID)

	s.actOnSinkPresence(ctx, mcp.NewClient(mcpManager), mcpManager, sinkPresenceTarget{
		pipelineID:    pipelineID,
		pipelineName:  pipelineName,
		dbType:        dbType,
		consumerGroup: consumerGroup,
	}, now)
}

// sinkPresenceTarget is the pipeline one probe+decision cycle is about. Bundled so the
// decision half below can be called with a test double for the probe without a
// nine-argument signature.
type sinkPresenceTarget struct {
	pipelineID    string
	pipelineName  string
	dbType        string
	consumerGroup string
}

// actOnSinkPresence is the decision half of ensureSinkWorkerPresent: probe, gate, act.
// Split out so the probe can be substituted — the gate's most important property is what
// it does with an answer it did not get, and that is unreachable through a live client.
//
// mgr is needed only on the respawn path (handlers.RestartCDCSinkWorker), which is why it
// is passed alongside the probe rather than derived from it.
func (s *CDCSentinel) actOnSinkPresence(
	ctx context.Context,
	probe sinkStatusProbe,
	mgr *mcp.ServerManager,
	t sinkPresenceTarget,
	now time.Time,
) {
	presence := probeSinkPresence(ctx, probe, t.consumerGroup)

	s.mu.Lock()
	st := s.sinkRespawnState[t.pipelineID]
	if st == nil {
		st = &connRestartState{}
		s.sinkRespawnState[t.pipelineID] = st
	}
	stCopy := *st
	s.mu.Unlock()

	decision, newSt := decideSinkRespawn(sinkRespawnInputs{
		presence:    presence,
		now:         now,
		st:          stCopy,
		maxAttempts: maxConnectorRestartAttempts(),
		window:      connectorRestartWindow(),
		cooldown:    connectorRestartCooldown(),
	})

	s.mu.Lock()
	if cur := s.sinkRespawnState[t.pipelineID]; cur != nil {
		*cur = newSt
	} else {
		nc := newSt
		s.sinkRespawnState[t.pipelineID] = &nc
	}
	s.mu.Unlock()

	switch decision {
	case sinkRespawnSkip:
		// ONLY a confirmed present worker clears the escalation. An unknown answer used
		// to land here too — and deleting a "your sink is missing" alarm because the
		// container was too broken to answer is the worst possible reading of silence.
		if presence == sinkPresencePresent {
			s.resolveLagIssue(ctx, sinkAbsentIssueID(t.pipelineID), t.pipelineID)
		}
		return

	case sinkRespawnFire:
		log.WithFields(log.Fields{
			"pipeline_id":    t.pipelineID,
			"consumer_group": t.consumerGroup,
			"attempt":        newSt.attempts,
			"max":            maxConnectorRestartAttempts(),
			"db_type":        t.dbType,
		}).Warn("🛡️ CDC sink worker is ABSENT from the sink container (container restarted?) — re-issuing start_sink")

		if err := handlers.RestartCDCSinkWorker(ctx, s.db, mgr, t.pipelineID); err != nil {
			log.WithError(err).WithFields(log.Fields{
				"pipeline_id":    t.pipelineID,
				"consumer_group": t.consumerGroup,
			}).Warn("🛡️ CDC sink respawn failed (attempt counted; will escalate at cap)")
			return
		}
		log.WithFields(log.Fields{
			"pipeline_id":    t.pipelineID,
			"consumer_group": t.consumerGroup,
			"attempt":        newSt.attempts,
		}).Info("🛡️ CDC sink respawned after absence — awaiting presence confirmation on the next poll")

	case sinkRespawnEscalate:
		log.WithFields(log.Fields{
			"pipeline_id":    t.pipelineID,
			"consumer_group": t.consumerGroup,
			"attempts":       maxConnectorRestartAttempts(),
			"db_type":        t.dbType,
		}).Error("🛡️ CDC sink worker still absent after max respawn attempts — escalating for manual intervention")

		// Do NOT stop the pipeline: the source connector is healthy and the WAL position is
		// intact. Surfacing the absence lets an operator fix the sink without losing it.
		s.emitLagIssue(ctx, sinkAbsentIssueID(t.pipelineID), "sink_absent",
			t.pipelineID, t.pipelineName, t.dbType,
			fmt.Sprintf("The kafka-mcp-sink container is not running a worker for this pipeline and could not be restarted automatically after %d attempts (consumer group: %s). Change events are being captured but NOTHING is reaching the destination — manual intervention required.",
				maxConnectorRestartAttempts(), t.consumerGroup),
			map[string]interface{}{
				"consumer_group": t.consumerGroup,
				"attempts":       maxConnectorRestartAttempts(),
				"terminal":       true,
			})
	}
}
