package kafkaclient

import (
	"testing"
	"time"
)

// The point of these helpers is that two services agree, so the tests pin the
// exact wire values rather than asserting the functions are self-consistent.

func TestDomainEventIDFormatIsPinned(t *testing.T) {
	// The orchestrator has emitted this format since the topic existed, and
	// operators grep for it. Changing it is a decision, not a refactor.
	got := DomainEventID("12c3579c-8a1e-4f2b-9d70-6b5e2f0a1c34", 1723929600000000001)
	want := "evt-12c3579c-1723929600000000001"
	if got != want {
		t.Fatalf("DomainEventID changed the wire format:\n got %q\nwant %q", got, want)
	}
}

func TestDomainEventIDHandlesShortAndEmptyPipelineIDs(t *testing.T) {
	// Not hypothetical: EmitProgress rejects an empty pipeline_id, but it does so
	// after the caller has already built the event, and the orchestrator's own
	// tests construct short ids. A slice panic here would take down a worker on
	// the telemetry path.
	cases := []struct {
		name       string
		pipelineID string
		want       string
	}{
		{"exactly eight", "12345678", "evt-12345678-42"},
		{"shorter than eight", "abc", "evt-abc-42"},
		{"empty", "", "evt--42"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := DomainEventID(tc.pipelineID, 42); got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestDomainEventIDIsUniquePerSeq(t *testing.T) {
	// The (pipeline_id, event_id) unique index is what the projector's
	// ON CONFLICT DO NOTHING keys on, so two events of one pipeline sharing an id
	// means the second row is dropped without a trace. Distinct seq must give
	// distinct ids.
	a := DomainEventID("pipeline-a", 1)
	b := DomainEventID("pipeline-a", 2)
	if a == b {
		t.Fatalf("two seqs produced the same event id (%q) — the projector would "+
			"silently drop the second event", a)
	}
}

func TestDomainEventSeqIsUTCNanosRegardlessOfLocation(t *testing.T) {
	// A producer running with TZ set (and one caller does pass a wall clock) must
	// land in the same numeric namespace as one running in UTC. If seq were derived
	// per-location the two producers' streams would interleave wrongly, which is the
	// exact class of bug this package exists to prevent.
	utc := time.Date(2026, 8, 17, 12, 0, 0, 123456789, time.UTC)
	kolkata := utc.In(time.FixedZone("IST", 5*3600+1800))

	if DomainEventSeq(utc) != DomainEventSeq(kolkata) {
		t.Fatalf("same instant produced different seqs across locations: %d vs %d",
			DomainEventSeq(utc), DomainEventSeq(kolkata))
	}
	if want := utc.UnixNano(); DomainEventSeq(utc) != want {
		t.Fatalf("DomainEventSeq is not nanoseconds since the epoch: got %d, want %d",
			DomainEventSeq(utc), want)
	}
}

func TestDomainEventSeqOrdersByTime(t *testing.T) {
	earlier := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	later := earlier.Add(time.Nanosecond)
	if DomainEventSeq(earlier) >= DomainEventSeq(later) {
		t.Fatal("seq does not increase with time — it is useless as an ordering tiebreaker")
	}
}
