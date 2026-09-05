package sentinel

import (
	"testing"
	"time"
)

// The container-vocabulary cases that used to live here as TestSinkWorkerAbsent moved to
// sink_presence_test.go (TestProbeSinkPresence_TriStatesTheContainersAnswer) along with the
// probe itself. They grew a third outcome on the way: sinkWorkerAbsent returned a bool, so
// "the container could not answer" and "the worker is present" were the same value, and the
// caller read both as healthy. The cases below are the DECISION half only.

func TestDecideSinkRespawnFiresOnAbsence(t *testing.T) {
	now := time.Now()
	got, st := decideSinkRespawn(sinkRespawnInputs{
		presence: sinkPresenceAbsent, now: now, maxAttempts: 3, window: time.Hour, cooldown: time.Minute,
	})
	if got != sinkRespawnFire {
		t.Fatalf("decision = %v, want fire", got)
	}
	// The attempt is recorded BEFORE the caller acts, so an accepted-but-immediately-dead
	// worker still burns budget instead of looping forever.
	if st.attempts != 1 {
		t.Errorf("attempts = %d, want 1 (recorded before the caller acts)", st.attempts)
	}
}

// The single most important safety property: only a definite not_found restarts anything.
// The Unknown half of that property is covered in sink_presence_test.go
// (TestDecideSinkRespawn_UnknownPreservesTheAttemptBudget), which also pins the part a
// bool could not express — that Unknown must not reset the budget the way Present does.
func TestDecideSinkRespawnSkipsWhenPresent(t *testing.T) {
	now := time.Now()
	got, _ := decideSinkRespawn(sinkRespawnInputs{
		presence: sinkPresencePresent, now: now, maxAttempts: 3, window: time.Hour, cooldown: time.Minute,
	})
	if got != sinkRespawnSkip {
		t.Errorf("decision = %v, want skip", got)
	}
}

// A worker that comes back must clear the budget, or an unrelated container restart weeks
// later inherits an exhausted cap and is never recovered.
func TestDecideSinkRespawnResetsWhenWorkerReturns(t *testing.T) {
	now := time.Now()
	exhausted := connRestartState{attempts: 3, firstAttempt: now, lastAttempt: now, escalated: true}

	_, st := decideSinkRespawn(sinkRespawnInputs{
		presence: sinkPresencePresent, now: now, st: exhausted, maxAttempts: 3, window: time.Hour, cooldown: time.Minute,
	})
	if st.attempts != 0 || st.escalated || !st.firstAttempt.IsZero() {
		t.Fatalf("state not reset on presence: %+v", st)
	}

	// ...and the next genuine absence gets a full budget again.
	got, _ := decideSinkRespawn(sinkRespawnInputs{
		presence: sinkPresenceAbsent, now: now.Add(time.Hour), st: st, maxAttempts: 3, window: time.Hour, cooldown: time.Minute,
	})
	if got != sinkRespawnFire {
		t.Errorf("decision after reset = %v, want fire", got)
	}
}

func TestDecideSinkRespawnRespectsCooldown(t *testing.T) {
	now := time.Now()
	st := connRestartState{attempts: 1, firstAttempt: now.Add(-10 * time.Second), lastAttempt: now.Add(-10 * time.Second)}

	got, _ := decideSinkRespawn(sinkRespawnInputs{
		presence: sinkPresenceAbsent, now: now, st: st, maxAttempts: 3, window: time.Hour, cooldown: time.Minute,
	})
	if got != sinkRespawnSkip {
		t.Errorf("decision inside cooldown = %v, want skip", got)
	}

	got, _ = decideSinkRespawn(sinkRespawnInputs{
		presence: sinkPresenceAbsent, now: now.Add(time.Minute), st: st, maxAttempts: 3, window: time.Hour, cooldown: time.Minute,
	})
	if got != sinkRespawnFire {
		t.Errorf("decision after cooldown = %v, want fire", got)
	}
}

// A sink that can never be started (bad destination config, broken container) must escalate
// once and then go quiet, not hammer the container on every poll tick forever.
func TestDecideSinkRespawnEscalatesOnceAtCap(t *testing.T) {
	now := time.Now()
	st := connRestartState{attempts: 3, firstAttempt: now.Add(-5 * time.Minute), lastAttempt: now.Add(-5 * time.Minute)}

	got, st := decideSinkRespawn(sinkRespawnInputs{
		presence: sinkPresenceAbsent, now: now, st: st, maxAttempts: 3, window: time.Hour, cooldown: time.Minute,
	})
	if got != sinkRespawnEscalate {
		t.Fatalf("decision at cap = %v, want escalate", got)
	}

	got, _ = decideSinkRespawn(sinkRespawnInputs{
		presence: sinkPresenceAbsent, now: now.Add(time.Minute), st: st, maxAttempts: 3, window: time.Hour, cooldown: time.Minute,
	})
	if got != sinkRespawnSkip {
		t.Errorf("second decision at cap = %v, want skip (escalate exactly once)", got)
	}
}

// Once the rolling window elapses the budget refreshes, so a container that restarts again
// tomorrow is recovered rather than left dead by yesterday's exhausted cap.
func TestDecideSinkRespawnRollsWindow(t *testing.T) {
	now := time.Now()
	st := connRestartState{attempts: 3, firstAttempt: now.Add(-2 * time.Hour), lastAttempt: now.Add(-2 * time.Hour), escalated: true}

	got, newSt := decideSinkRespawn(sinkRespawnInputs{
		presence: sinkPresenceAbsent, now: now, st: st, maxAttempts: 3, window: time.Hour, cooldown: time.Minute,
	})
	if got != sinkRespawnFire {
		t.Fatalf("decision after window elapsed = %v, want fire", got)
	}
	if newSt.attempts != 1 || newSt.escalated {
		t.Errorf("window did not roll cleanly: %+v", newSt)
	}
}

// The respawn rung must not be reachable through the wedge rung's flag, and vice versa:
// they answer different questions and share no budget.
func TestSinkRespawnIsNotGatedByAutoRestartFlag(t *testing.T) {
	t.Setenv("CDC_SINK_AUTORESTART_ENABLED", "")
	if sinkAutoRestartEnabled() {
		t.Fatal("precondition: wedge rung should be disarmed")
	}
	got, _ := decideSinkRespawn(sinkRespawnInputs{
		presence: sinkPresenceAbsent, now: time.Now(), maxAttempts: 3, window: time.Hour, cooldown: time.Minute,
	})
	if got != sinkRespawnFire {
		t.Errorf("decision = %v, want fire — the absent-worker rung must not ride the wedge flag", got)
	}
}
