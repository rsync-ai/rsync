package workers

import (
	"context"
	"testing"
	"time"

	kafkaclient "github.com/rsync-ai/shared/kafkaclient"
)

// This service and the temporal adapter both write pipeline.domain.events about the
// same run, and the api-gateway projector folds them into one pipeline_run_events
// timeline ordered by (occurred_at, seq). These assertions pin this producer's half
// of that contract to the shared helpers, so the two cannot drift apart again
// without a red test — the previous drift (UnixNano here, nothing there, a
// projector-invented 1/2/3 in the gap) left every column populated and looked fine.

func TestNormalizeProgressEventStampsTheSharedEnvelope(t *testing.T) {
	event := ProgressEvent{
		EventType:  "STAGE_STARTED",
		PipelineID: "12c3579c-8a1e-4f2b-9d70-6b5e2f0a1c34",
		Stage:      "executor",
	}

	before := time.Now().UTC().UnixNano()
	if err := normalizeProgressEvent(context.Background(), &event); err != nil {
		t.Fatalf("normalizeProgressEvent: %v", err)
	}
	after := time.Now().UTC().UnixNano()

	if event.Seq < before || event.Seq > after {
		t.Fatalf("seq %d is not a nanosecond wall-clock reading taken during the call "+
			"(%d..%d) — the adapter derives seq the same way, and a producer using a "+
			"different scale sorts its whole stream to one end of every tie",
			event.Seq, before, after)
	}
	if want := kafkaclient.DomainEventID(event.PipelineID, event.Seq); event.EventID != want {
		t.Fatalf("event_id = %q, want %q (the shared format operators grep for)",
			event.EventID, want)
	}
	if event.OccurredAt == "" || event.OccurredAt != event.Timestamp {
		t.Fatalf("occurred_at = %q, timestamp = %q — occurred_at must be set and must be "+
			"this producer's own clock, or the projector falls back to its poll time",
			event.OccurredAt, event.Timestamp)
	}
	if event.SchemaVersion != 1 {
		t.Fatalf("schema_version = %d, want 1 — the projector's only way to tell the two "+
			"producers apart", event.SchemaVersion)
	}
}

func TestNormalizeProgressEventKeepsCallerSuppliedEnvelope(t *testing.T) {
	// Replay and re-emit paths hand in an already-stamped envelope; overwriting it
	// would give the same logical event two identities.
	event := ProgressEvent{
		EventType:  "STAGE_COMPLETED",
		PipelineID: "12c3579c-8a1e-4f2b-9d70-6b5e2f0a1c34",
		Seq:        1723929600000000001,
		EventID:    "evt-12c3579c-1723929600000000001",
		OccurredAt: "2026-08-17T11:59:00Z",
	}
	if err := normalizeProgressEvent(context.Background(), &event); err != nil {
		t.Fatalf("normalizeProgressEvent: %v", err)
	}

	if event.Seq != 1723929600000000001 {
		t.Fatalf("seq was overwritten: %d", event.Seq)
	}
	if event.EventID != "evt-12c3579c-1723929600000000001" {
		t.Fatalf("event_id was overwritten: %q", event.EventID)
	}
	if event.OccurredAt != "2026-08-17T11:59:00Z" {
		t.Fatalf("occurred_at was overwritten: %q", event.OccurredAt)
	}
}

func TestNormalizeProgressEventRejectsEventsTheProjectorCannotPlace(t *testing.T) {
	// A missing pipeline_id fails the projector's WHERE EXISTS guard and the event
	// is dropped without a row; a missing event_type lands as an untyped row no
	// reader filters on. Both are better caught here than counted as emitted.
	cases := []struct {
		name  string
		event ProgressEvent
	}{
		{"no pipeline_id", ProgressEvent{EventType: "STAGE_STARTED"}},
		{"no event_type", ProgressEvent{PipelineID: "12c3579c-8a1e-4f2b-9d70-6b5e2f0a1c34"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := normalizeProgressEvent(context.Background(), &tc.event); err == nil {
				t.Fatal("expected an error, got nil")
			}
		})
	}
}
