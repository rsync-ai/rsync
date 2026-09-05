package workflows

import (
	"time"

	kafkaclient "github.com/rsync-ai/shared/kafkaclient"
	"go.temporal.io/sdk/workflow"
)

// emitDomainEvent is the only way this workflow puts an event on
// pipeline.domain.events.
//
// It exists because the envelope was being left off. The api-gateway projector
// folds this workflow's events and the orchestrator's stage-worker events into
// one pipeline_run_events timeline and orders them by
// (occurred_at, seq, event_id). The orchestrator supplied seq = UnixNano; this
// workflow supplied nothing, so the projector fell back to a local per-execution
// counter (event_projector.go, "assign projector-local increment") and stamped
// 1, 2, 3. Both producers ended up with a populated seq column and the two were
// three orders of magnitude apart, so on any occurred_at tie one producer's
// entire stream sorted underneath the other's — including in the keyset cursor
// that pipeline_events.go pages on. Nothing about that is visible from a row:
// an invented seq and a supplied seq are the same integer column.
//
// What this deliberately does NOT do is stamp event_id, even though the missing
// id is the more eye-catching half (the projector content-hashes those events
// into a "sha256:" id while the orchestrator's read "evt-…"). Two reasons, and
// the second is the one that matters:
//
//  1. A unique id does not dedupe anything. The duplicate rows on this topic are
//     two different producers describing one stage transition ~3-6s apart with
//     different payloads, not one producer emitting twice — so any id that is
//     unique per emission, in any format, leaves both rows exactly as they are.
//
//  2. An id derived from workflow.Now() would be actively unsafe. Within a
//     single workflow task that clock does not advance, so two events emitted
//     without an intervening yield would share a seq, hence an id, and the
//     projector's ON CONFLICT (pipeline_id, event_id) DO NOTHING would drop the
//     second without a trace. That trades a visible duplicate for an invisible
//     deletion.
//
// The projector's content hash is unique by construction and now logs when it
// fires, so the gap is recorded rather than papered over. Collapsing the genuine
// duplicates needs a shared logical identity these two producers do not have —
// see KI-EVENTS-TWO-OBSERVERS-ONE-TRANSITION in CAPABILITIES.md.
func emitDomainEvent(ctx workflow.Context, event map[string]interface{}) error {
	stampDomainEventEnvelope(event, workflow.Now(ctx))
	return workflow.ExecuteActivity(ctx, EmitDomainEventActivity, event).Get(ctx, nil)
}

// stampDomainEventEnvelope fills the envelope fields a producer owns, leaving
// anything the caller already set. Split out from emitDomainEvent so it is
// testable without a workflow environment.
//
// occurred_at is stamped only as a floor. Every call site today sets "timestamp",
// which the projector already reads as its second choice, so in practice this
// changes nothing — but its remaining fallbacks are the Kafka message time and
// the projector's own wall clock, and a call site added later that forgets the
// timestamp would silently be ordered by when the projector happened to poll.
func stampDomainEventEnvelope(event map[string]interface{}, now time.Time) {
	if event == nil {
		return
	}
	if v, ok := event["seq"]; !ok || v == nil {
		event["seq"] = kafkaclient.DomainEventSeq(now)
	}
	if v, ok := event["occurred_at"].(string); !ok || v == "" {
		if ts, ok := event["timestamp"].(string); ok && ts != "" {
			event["occurred_at"] = ts
		} else {
			event["occurred_at"] = now.UTC().Format(time.RFC3339)
		}
	}
}
