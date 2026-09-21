package sentinel

import (
	"testing"
	"time"
)

// tick is one sentinel poll as the sink-lag alarm sees it.
type tick struct {
	at        time.Duration // since the first tick
	committed int64
	lag       bool
}

// runDrainTicks feeds ticks through observeSinkDrain the way checkSinkConsumerLag does and
// returns the alarm verdict of each one.
func runDrainTicks(t *testing.T, ticks []tick) []bool {
	t.Helper()
	t.Setenv("CDC_SINK_DRAIN_STALL_AFTER", "5m")
	s := &CDCSentinel{} // no map on purpose: a bare literal must not panic
	start := time.Date(2026, 9, 17, 10, 0, 0, 0, time.UTC)
	got := make([]bool, len(ticks))
	for i, tk := range ticks {
		got[i], _ = s.observeSinkDrain("p1", tk.committed, tk.lag, start.Add(tk.at))
	}
	return got
}

func assertAlarms(t *testing.T, got, want []bool) {
	t.Helper()
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("tick %d: alarm=%v, want %v (all: got %v, want %v)", i, got[i], want[i], got, want)
		}
	}
}

// The false alarm this replaces: a first load keeps the backlog far above the threshold for
// many minutes while the sink commits every tick.
func TestSinkDrain_BacklogThatIsDrainingNeverAlarms(t *testing.T) {
	var ticks []tick
	for m := 0; m <= 30; m++ {
		ticks = append(ticks, tick{at: time.Duration(m) * time.Minute, committed: int64(m) * 5000, lag: true})
	}
	got := runDrainTicks(t, ticks)
	assertAlarms(t, got, make([]bool, len(ticks)))
}

// A dead or wedged sink never commits, so a backlog that does not move must alarm once the
// stall window has passed, and keep alarming.
func TestSinkDrain_BacklogThatStopsMovingAlarmsAfterTheWindow(t *testing.T) {
	got := runDrainTicks(t, []tick{
		{at: 0, committed: 100, lag: true},                // first reading: starts the clock
		{at: 1 * time.Minute, committed: 100, lag: true},  // 1m flat
		{at: 4 * time.Minute, committed: 100, lag: true},  // 4m flat
		{at: 5 * time.Minute, committed: 100, lag: true},  // 5m flat: alarm
		{at: 10 * time.Minute, committed: 100, lag: true}, // still stuck: still alarms
	})
	assertAlarms(t, got, []bool{false, false, false, true, true})
}

// Once the sink commits again the alarm clears, and a later stall needs a full window again.
func TestSinkDrain_MovingAgainClearsTheAlarmAndRestartsTheClock(t *testing.T) {
	got := runDrainTicks(t, []tick{
		{at: 0, committed: 100, lag: true},
		{at: 6 * time.Minute, committed: 100, lag: true},  // stuck: alarm
		{at: 7 * time.Minute, committed: 900, lag: true},  // moved: clear
		{at: 11 * time.Minute, committed: 900, lag: true}, // 4m flat: not yet
		{at: 12 * time.Minute, committed: 900, lag: true}, // 5m flat: alarm
	})
	assertAlarms(t, got, []bool{false, true, false, false, true})
}

// A sink idle for hours on a quiet stream has a flat committed position. A burst that lands
// just before a tick must not be reported as hours of being stuck.
func TestSinkDrain_IdleSinkHitByABurstIsNotStuckYet(t *testing.T) {
	got := runDrainTicks(t, []tick{
		{at: 0, committed: 100, lag: false},
		{at: 3 * time.Hour, committed: 100, lag: false},
		{at: 3*time.Hour + time.Minute, committed: 100, lag: true}, // burst, not committed yet
		{at: 3*time.Hour + 6*time.Minute, committed: 100, lag: true},
	})
	assertAlarms(t, got, []bool{false, false, false, true})
}

// The first reading after an orchestrator restart says nothing about how long the sink has
// been stuck, and neither does a committed position that went backwards (group reset).
func TestSinkDrain_FirstReadingAndResetOnlyStartTheClock(t *testing.T) {
	got := runDrainTicks(t, []tick{
		{at: 0, committed: 5000, lag: true},
		{at: 6 * time.Minute, committed: 10, lag: true}, // reset backwards: restart clock
		{at: 7 * time.Minute, committed: 10, lag: true},
		{at: 11 * time.Minute, committed: 10, lag: true},
	})
	assertAlarms(t, got, []bool{false, false, false, true})
}

// Pipelines keep separate clocks: one stuck sink must not raise or clear another's alarm.
func TestSinkDrain_PipelinesAreTrackedSeparately(t *testing.T) {
	t.Setenv("CDC_SINK_DRAIN_STALL_AFTER", "5m")
	s := &CDCSentinel{}
	start := time.Date(2026, 9, 17, 10, 0, 0, 0, time.UTC)
	s.observeSinkDrain("stuck", 1, true, start)
	s.observeSinkDrain("busy", 1, true, start)
	if stuck, _ := s.observeSinkDrain("stuck", 1, true, start.Add(6*time.Minute)); !stuck {
		t.Fatal("stuck pipeline did not alarm after 6m without committing")
	}
	if busy, _ := s.observeSinkDrain("busy", 2000, true, start.Add(6*time.Minute)); busy {
		t.Fatal("a pipeline that committed alarmed because another pipeline was stuck")
	}
}
