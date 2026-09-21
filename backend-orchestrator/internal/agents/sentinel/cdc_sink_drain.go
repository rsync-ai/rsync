package sentinel

import "time"

// DefaultSinkDrainStallAfter is how long a CDC sink may sit on a backlog without committing
// before the sink-lag alarm fires. Overridable via CDC_SINK_DRAIN_STALL_AFTER (minimum 30s).
//
// The sink commits its Kafka offsets right after each destination write, so a working sink
// moves its committed position within seconds. Five minutes leaves room for one slow batch
// into a slow destination without calling it stuck.
const DefaultSinkDrainStallAfter = 5 * time.Minute

// sinkDrainState is what the sink-lag alarm remembers about one pipeline between ticks.
type sinkDrainState struct {
	committed int64
	// movingAt is the last time the sink was seen doing its job: its committed position
	// moved forward, or it had no backlog worth alarming about.
	movingAt time.Time
}

// decideSinkDrainAlarm says whether a sink with a backlog has stopped draining it.
//
// Lag on its own cannot say that. The first load of a large table puts a backlog far above
// the threshold on a perfectly healthy sink, and alarming on lag alone told the user rows
// were "NOT reaching the destination" while they were. What separates a busy sink from a
// stuck one is whether its committed position moves, so the alarm needs a backlog AND no
// movement for stallAfter.
//
// A sink that is dead, wedged, or replaying the same batch without committing never moves
// its committed position, so it still alarms. A sink that crashes but commits some batches
// between crashes is moving and does not alarm here.
//
// The first reading for a pipeline only starts the clock, and so does a committed position
// that went backwards (the group was reset or recreated), because neither says how long
// the sink has been stuck. Having no backlog also restarts the clock, so a burst that lands
// just before a tick on a sink idle for hours is not reported as hours of being stuck.
func decideSinkDrainAlarm(prev sinkDrainState, seen bool, committed int64, lagging bool, now time.Time, stallAfter time.Duration) (sinkDrainState, bool) {
	next := sinkDrainState{committed: committed, movingAt: prev.movingAt}
	if !seen || !lagging || committed != prev.committed {
		next.movingAt = now
		return next, false
	}
	return next, now.Sub(prev.movingAt) >= stallAfter
}

// observeSinkDrain records this tick's committed position for a pipeline and returns
// whether its sink-lag alarm should be raised, and for how long the sink has not moved.
func (s *CDCSentinel) observeSinkDrain(pipelineID string, committed int64, lagging bool, now time.Time) (bool, time.Duration) {
	stallAfter := walDurationFromEnv("CDC_SINK_DRAIN_STALL_AFTER", DefaultSinkDrainStallAfter)

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sinkDrain == nil {
		s.sinkDrain = make(map[string]sinkDrainState)
	}
	prev, seen := s.sinkDrain[pipelineID]
	next, alarm := decideSinkDrainAlarm(prev, seen, committed, lagging, now, stallAfter)
	s.sinkDrain[pipelineID] = next
	return alarm, now.Sub(next.movingAt)
}
