package kafkaclient

import (
	"fmt"
	"time"
)

// The envelope every rsync-ai domain event carries on pipeline.domain.events.
//
// Two services write that topic about the same run — the orchestrator's stage
// workers (schema_version 1) and the temporal adapter's V2 workflow
// (schema_version 2) — and the api-gateway projector folds both into one
// `pipeline_run_events` timeline. When the two disagree about the shape of the
// envelope, the projector cannot order or dedupe them, and the disagreement is
// invisible: a row with a projector-invented seq looks exactly like a row whose
// producer supplied one.
//
// That is not hypothetical. Before this package existed the orchestrator stamped
// seq = UnixNano (~1.7e18) while the adapter stamped nothing, so the projector
// fell back to a local per-execution counter and gave those events seq 1, 2, 3.
// Both columns were populated, both looked fine, and every ORDER BY that used
// seq as a tiebreaker sorted one producer's whole stream underneath the other's.
//
// So the definitions live here, in the one module all three services already
// require, rather than being spelled out again at each producer.

// DomainEventSeq is the ordering token for a domain event.
//
// Nanoseconds since the epoch, from the producer's own clock. It is a tiebreaker
// after occurred_at, not a gapless sequence — the point is only that every
// producer of this topic derives it the same way, so two producers' streams
// interleave by time instead of by which one happened to supply a larger number.
//
// Callers inside a Temporal workflow must pass workflow.Now(ctx), never
// time.Now(): the value is recorded in workflow history and has to replay
// identically.
func DomainEventSeq(t time.Time) int64 {
	return t.UTC().UnixNano()
}

// DomainEventID builds the human-readable event id the orchestrator has always
// used: evt-<first 8 chars of pipeline id>-<seq>.
//
// The format is load-bearing for people, not for machines — an operator reading
// a log line or a `pipeline_run_events` row can tell at a glance which pipeline
// an event belongs to. The uniqueness that the (pipeline_id, event_id) index
// depends on comes from seq.
//
// Deliberately NOT used by the temporal adapter, and that is worth stating
// plainly because the opposite looks like an obvious improvement. Within a
// single workflow task workflow.Now() does not advance, so two events emitted
// without an intervening yield would be handed the same seq, hence the same id,
// and ON CONFLICT DO NOTHING would silently drop the second — trading a visible
// duplicate for an invisible deletion. The adapter's events are content-hashed
// by the projector instead, which cannot collide by construction.
func DomainEventID(pipelineID string, seq int64) string {
	pfx := pipelineID
	if len(pfx) > 8 {
		pfx = pfx[:8]
	}
	return "evt-" + pfx + "-" + fmt.Sprintf("%d", seq)
}
