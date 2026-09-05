package sentinel

import (
	"database/sql"
	"time"
)

// deriveSinkProgressSignals turns the destination-apply progress marker into the two non-lag
// wedge signals fed to decideSinkRestart. lastApplied is MAX(last_applied_ts) from
// pipeline_run_table_stats — the sink's OWN post-apply timestamp (source="kafka_mcp_sink"),
// i.e. destination truth. It is deliberately NOT the source-side last_event_ts (Debezium
// ts_ms), which stays fresh while the sink is wedged. prevApplied is the marker observed on
// the previous tick.
//
//	stale = the destination has applied nothing for longer than staleBound (absolute)
//	flat  = the apply marker has not advanced since the previous tick (relative)
//
// Safety properties (all three keep the auto-restart gate from firing on non-wedges):
//   - a MISSING marker (never applied, or the sink→Kafka→projector telemetry path hasn't
//     produced a row yet) is neither stale nor flat — the gate can never fire on absence;
//   - the FIRST observation (no previous marker) is never flat, so a fire needs ≥2 ticks,
//     which also spaces the very first restart by a full poll interval;
//   - a marker that ADVANCED since the last tick is never flat even if its absolute age
//     exceeds the bound, so a slow-but-progressing sink is never restarted.
func deriveSinkProgressSignals(lastApplied, prevApplied sql.NullTime, now time.Time, staleBound time.Duration) (stale, flat bool) {
	if !lastApplied.Valid {
		return false, false
	}
	stale = now.Sub(lastApplied.Time) > staleBound
	if !prevApplied.Valid {
		return stale, false
	}
	flat = lastApplied.Time.Equal(prevApplied.Time)
	return stale, flat
}

// sinkRestartDecision is the outcome of the autonomous sink-restart wedge-gate.
type sinkRestartDecision int

const (
	// sinkRestartSkip: do nothing this tick — not a genuine wedge, feature disabled,
	// still cooling down, or the cap was already escalated.
	sinkRestartSkip sinkRestartDecision = iota
	// sinkRestartFire: perform one bounded sink restart and record the attempt.
	sinkRestartFire
	// sinkRestartEscalate: the per-pipeline attempt cap is exhausted inside the rolling
	// window — stop restarting and escalate terminally, exactly once.
	sinkRestartEscalate
)

// sinkRestartInputs is the full, side-effect-free input to the wedge-gate. The caller
// computes the three signal booleans — from the CORRECTED consumer lag (Phase A), the sink's
// own processing staleness, and destination progress (pipeline_run_table_stats) — plus the
// feature flag, then hands them here together with the current per-pipeline restart
// bookkeeping. Keeping this pure is what lets the safety policy be exhaustively unit-tested
// with no broker, DB, or HTTP.
type sinkRestartInputs struct {
	enabled      bool // CDC_SINK_AUTORESTART_ENABLED (default off, everywhere)
	lagging      bool // corrected consumer lag on our OWN topics > threshold
	sinkStale    bool // sink has processed nothing for longer than the stale bound
	progressFlat bool // destination progress (pipeline_run_table_stats) is not advancing
	now          time.Time
	st           connRestartState
	maxAttempts  int
	window       time.Duration
	cooldown     time.Duration
}

// decideSinkRestart is the pure wedge-gate. It returns the action to take and the restart
// state the caller must persist.
//
// It fires ONLY when the feature is enabled AND all three genuine-wedge signals agree — never
// on the Phase-A phantom (a huge reported lag while the sink is actually fresh and the
// destination is still advancing). Wiring a restart to that phantom would cluster-restart-loop
// every healthy sink, which is exactly the failure this gate exists to prevent. When it does
// act, it enforces the same attempt cap / rolling window / cooldown as handleFailedConnector so
// a permanently-wedged sink is restarted a bounded number of times, then escalated once.
func decideSinkRestart(in sinkRestartInputs) (sinkRestartDecision, connRestartState) {
	st := in.st

	// Dormant unless explicitly enabled, and only for a genuine three-signal wedge. This is
	// the guard that keeps the corrected-but-still-imperfect lag signal from ever, on its
	// own, triggering a restart.
	if !in.enabled || !(in.lagging && in.sinkStale && in.progressFlat) {
		return sinkRestartSkip, st
	}

	// Roll the window over once it has fully elapsed, then anchor a fresh window on first use.
	if !st.firstAttempt.IsZero() && in.now.Sub(st.firstAttempt) > in.window {
		st.attempts = 0
		st.firstAttempt = in.now
		st.escalated = false
	}
	if st.firstAttempt.IsZero() {
		st.firstAttempt = in.now
	}

	// Cap exhausted → terminal escalation, exactly once (dedup via st.escalated).
	if st.attempts >= in.maxAttempts {
		if st.escalated {
			return sinkRestartSkip, st
		}
		st.escalated = true
		return sinkRestartEscalate, st
	}

	// Space attempts out by the cooldown.
	if !st.lastAttempt.IsZero() && in.now.Sub(st.lastAttempt) < in.cooldown {
		return sinkRestartSkip, st
	}

	// Record the attempt BEFORE the caller acts, so a restart that is "accepted" but
	// immediately re-wedges still counts toward the cap (mirrors handleFailedConnector's
	// SENT-B note — otherwise a sink that accepts every restart would loop forever).
	st.attempts++
	st.lastAttempt = in.now
	return sinkRestartFire, st
}
