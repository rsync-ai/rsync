package sentinel

import (
	"database/sql"
	"testing"
	"time"
)

// nt is a tiny helper: a valid sql.NullTime at base+off.
func nt(base time.Time, off time.Duration) sql.NullTime {
	return sql.NullTime{Time: base.Add(off), Valid: true}
}

// TestDeriveSinkProgressSignals locks how the destination-apply marker
// (MAX(last_applied_ts) from pipeline_run_table_stats — the sink's own post-apply timestamp,
// NOT the source-side last_event_ts) becomes the two non-lag wedge signals fed to
// decideSinkRestart:
//
//	sinkStale    = destination has applied nothing for longer than staleBound (absolute)
//	progressFlat = the apply marker did not advance since the previous tick (relative)
//
// The safety-critical properties: a MISSING marker (never applied / telemetry not yet
// flowing) is neither stale nor flat, so the gate can never fire on absence of data; the
// FIRST observation (no previous marker) is never flat, so a fire needs at least two ticks;
// and a marker that ADVANCED since the last tick is never flat even if its absolute age
// exceeds the bound — a slow-but-progressing sink must not be restarted.
func TestDeriveSinkProgressSignals(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	const bound = 300 * time.Second
	absent := sql.NullTime{}

	cases := []struct {
		name        string
		lastApplied sql.NullTime
		prevApplied sql.NullTime
		wantStale   bool
		wantFlat    bool
	}{
		// Never applied / telemetry absent → never fire on absence alone.
		{"never_applied_absent", absent, absent, false, false},
		// Fresh apply that advanced since last tick → healthy.
		{"fresh_progressing", nt(base, -10*time.Second), nt(base, -70*time.Second), false, false},
		// Old apply, unchanged since last tick → genuine wedge (both true).
		{"stale_and_flat_wedge", nt(base, -600*time.Second), nt(base, -600*time.Second), true, true},
		// Old apply but this is the first observation → can't call it flat yet.
		{"stale_first_observation", nt(base, -600*time.Second), absent, true, false},
		// Beyond the absolute bound BUT the marker advanced since last tick → slow, not wedged.
		{"stale_absolute_but_advanced", nt(base, -350*time.Second), nt(base, -410*time.Second), true, false},
		// Fresh but unchanged since last tick (quiet source) → not stale, so it can't fire.
		{"fresh_but_flat_quiet_source", nt(base, -10*time.Second), nt(base, -10*time.Second), false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stale, flat := deriveSinkProgressSignals(tc.lastApplied, tc.prevApplied, base, bound)
			if stale != tc.wantStale {
				t.Errorf("stale = %v, want %v", stale, tc.wantStale)
			}
			if flat != tc.wantFlat {
				t.Errorf("flat = %v, want %v", flat, tc.wantFlat)
			}
		})
	}
}

// Phase B — the autonomous sink-restart wedge-gate.
//
// decideSinkRestart is the PURE decision core for whether the Sentinel should restart a
// per-pipeline CDC sink consumer. It exists so the safety-critical policy can be tested
// entirely offline, with NO Kafka broker, NO sink HTTP call, and NO control DB. The single
// most important property it locks: an auto-restart NEVER fires on the Phase-A phantom-lag
// input (huge reported lag while the sink is actually fresh and destination progress is
// advancing) — wiring a restart to that phantom would cluster-restart-loop every healthy
// sink. It also must stay dormant unless CDC_SINK_AUTORESTART_ENABLED is on, and must respect
// the same per-pipeline attempt cap / rolling window / cooldown as handleFailedConnector.

// baseSinkInputs returns a "genuine wedge, feature on, fresh state" input — every field set so
// that decideSinkRestart would FIRE — which individual cases then perturb one axis at a time.
func baseSinkInputs() sinkRestartInputs {
	return sinkRestartInputs{
		enabled:      true,
		lagging:      true, // corrected consumer lag > threshold (real backlog on our own topics)
		sinkStale:    true, // sink has not processed anything for > the stale bound
		progressFlat: true, // pipeline_run_table_stats destination progress is not advancing
		now:          time.Unix(1_700_000_000, 0),
		st:           connRestartState{},
		maxAttempts:  3,
		window:       24 * time.Hour,
		cooldown:     5 * time.Minute,
	}
}

// TestDecideSinkRestart_GateRequiresAllThreeSignalsAndFlag is the core safety table. The only
// combination that may FIRE is "flag on AND all three genuine-wedge signals true"; every other
// combination — most importantly the phantom-lag row (lagging but NOT stale and NOT flat) —
// must SKIP and must not touch the attempt counter.
func TestDecideSinkRestart_GateRequiresAllThreeSignalsAndFlag(t *testing.T) {
	cases := []struct {
		name         string
		enabled      bool
		lagging      bool
		sinkStale    bool
		progressFlat bool
		want         sinkRestartDecision
	}{
		// The Phase-A phantom: lag looks enormous, but the sink is fresh and the destination
		// is still advancing. This MUST NOT restart — it is the whole reason Phase B is gated.
		{"phantom_lag_fresh_sink_progressing", true, true, false, false, sinkRestartSkip},
		// Feature disabled → dormant no matter what the signals say.
		{"flag_off_even_on_genuine_wedge", false, true, true, true, sinkRestartSkip},
		// Any single missing signal → skip.
		{"no_backlog", true, false, true, true, sinkRestartSkip},
		{"sink_not_stale", true, true, false, true, sinkRestartSkip},
		{"caught_up_but_source_quiet", true, true, true, false, sinkRestartSkip},
		{"only_lagging", true, true, false, false, sinkRestartSkip},
		{"only_stale", true, false, true, false, sinkRestartSkip},
		{"only_flat", true, false, false, true, sinkRestartSkip},
		{"no_signals", true, false, false, false, sinkRestartSkip},
		// The one firing combination.
		{"genuine_wedge_flag_on", true, true, true, true, sinkRestartFire},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := baseSinkInputs()
			in.enabled = tc.enabled
			in.lagging = tc.lagging
			in.sinkStale = tc.sinkStale
			in.progressFlat = tc.progressFlat

			got, st := decideSinkRestart(in)
			if got != tc.want {
				t.Fatalf("decision = %v, want %v", got, tc.want)
			}
			// A skip must never advance the attempt counter.
			if tc.want == sinkRestartSkip && st.attempts != 0 {
				t.Errorf("skip must not record an attempt, got attempts=%d", st.attempts)
			}
			// A fire records exactly one attempt and stamps lastAttempt=now.
			if tc.want == sinkRestartFire {
				if st.attempts != 1 {
					t.Errorf("fire should set attempts=1, got %d", st.attempts)
				}
				if !st.lastAttempt.Equal(in.now) {
					t.Errorf("fire should stamp lastAttempt=now")
				}
			}
		})
	}
}

// TestDecideSinkRestart_RespectsCooldown: after a fire, a second genuine wedge within the
// cooldown window must skip; once past the cooldown it may fire again.
func TestDecideSinkRestart_RespectsCooldown(t *testing.T) {
	in := baseSinkInputs()
	in.st = connRestartState{attempts: 1, firstAttempt: in.now.Add(-1 * time.Minute), lastAttempt: in.now.Add(-1 * time.Minute)}

	if got, _ := decideSinkRestart(in); got != sinkRestartSkip {
		t.Fatalf("within cooldown: decision = %v, want skip", got)
	}

	in.now = in.now.Add(6 * time.Minute) // now past the 5m cooldown
	got, st := decideSinkRestart(in)
	if got != sinkRestartFire {
		t.Fatalf("past cooldown: decision = %v, want fire", got)
	}
	if st.attempts != 2 {
		t.Errorf("past cooldown fire should increment attempts to 2, got %d", st.attempts)
	}
}

// TestDecideSinkRestart_CapExhaustedEscalatesOnce: at the attempt cap the gate escalates
// exactly once (terminal), then stays quiet on subsequent ticks until the state is cleared.
func TestDecideSinkRestart_CapExhaustedEscalatesOnce(t *testing.T) {
	in := baseSinkInputs()
	in.st = connRestartState{attempts: 3, firstAttempt: in.now.Add(-1 * time.Hour), lastAttempt: in.now.Add(-10 * time.Minute)}

	got, st := decideSinkRestart(in)
	if got != sinkRestartEscalate {
		t.Fatalf("at cap: decision = %v, want escalate", got)
	}
	if !st.escalated {
		t.Errorf("escalate must mark state.escalated=true for dedup")
	}

	// Feed the escalated state back: must go quiet, not escalate again.
	in.st = st
	if got, _ := decideSinkRestart(in); got != sinkRestartSkip {
		t.Errorf("already-escalated: decision = %v, want skip", got)
	}
}

// TestDecideSinkRestart_WindowRolloverResets: a cap-exhausted state whose window has elapsed
// resets and is allowed to fire again (attempts back to 1, window re-anchored to now).
func TestDecideSinkRestart_WindowRolloverResets(t *testing.T) {
	in := baseSinkInputs()
	in.st = connRestartState{attempts: 3, firstAttempt: in.now.Add(-25 * time.Hour), lastAttempt: in.now.Add(-25 * time.Hour), escalated: true}

	got, st := decideSinkRestart(in)
	if got != sinkRestartFire {
		t.Fatalf("after window rollover: decision = %v, want fire", got)
	}
	if st.attempts != 1 {
		t.Errorf("rollover should reset attempts to 1, got %d", st.attempts)
	}
	if !st.firstAttempt.Equal(in.now) {
		t.Errorf("rollover should re-anchor firstAttempt to now")
	}
	if st.escalated {
		t.Errorf("rollover should clear the escalated flag")
	}
}
