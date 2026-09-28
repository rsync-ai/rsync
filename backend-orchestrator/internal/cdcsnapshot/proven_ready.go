package cdcsnapshot

import "time"

// A CDC Reload's re-snapshot used to wait 10-15 s in the dispatcher after the
// executor had already watched the connector run for 10 s: the request was
// looked at only on the next 5 s tick, and its ReadyStable streak started from
// zero there. Hurry hands the dispatcher that proof so the signal goes out on
// the next tick, and wakes the loop so that tick is now.
//
// What stays checked: the dispatcher still reads the connector state itself and
// sends only when Assess is Ready at that moment (connector and every task
// RUNNING, every requested table in a running task's include list). The proof
// replaces the ReadyStable wait only when it is a streak of at least
// ReadyStable that ended no more than ReadyStable before the dispatcher's own
// Ready reading. Any not-Ready reading drops it.

// RunningProof is a streak in which the connector and every task were read
// RUNNING at every poll, from Since to At.
type RunningProof struct {
	Since time.Time
	At    time.Time
}

// covers reports whether p stands in for a ReadyStable streak ending at now.
func (p RunningProof) covers(now time.Time, stable time.Duration) bool {
	if p.Since.IsZero() || p.At.Before(p.Since) || now.Before(p.At) {
		return false
	}
	return p.At.Sub(p.Since) >= stable && now.Sub(p.At) <= stable
}

// Hurry records proof for the queued request requestID (only that request: a
// later Edit tables restarts the connector and queues its own request) and
// wakes the dispatcher. Safe on a nil or stopped dispatcher.
func (d *Dispatcher) Hurry(requestID string, proof RunningProof) {
	if d == nil || requestID == "" {
		return
	}
	d.mu.Lock()
	if proof.covers(proof.At, d.timing.ReadyStable) {
		d.proofs[requestID] = proof
	}
	d.mu.Unlock()
	d.Kick()
}

// Kick runs a tick now instead of at the next interval. Never blocks.
func (d *Dispatcher) Kick() {
	if d == nil || d.kick == nil {
		return
	}
	select {
	case d.kick <- struct{}{}:
	default: // a tick is already pending
	}
}

// provenReadySince is the start of r's recorded proof when it covers a
// ReadyStable streak ending now. The proof is used once.
func (d *Dispatcher) provenReadySince(id string, now time.Time) (time.Time, bool) {
	p, ok := d.proofs[id]
	if !ok {
		return time.Time{}, false
	}
	delete(d.proofs, id)
	if !p.covers(now, d.timing.ReadyStable) {
		return time.Time{}, false
	}
	return p.Since, true
}
