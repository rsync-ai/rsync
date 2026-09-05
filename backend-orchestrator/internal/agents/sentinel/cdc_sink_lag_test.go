package sentinel

// Coverage for the P1-B sink-drain lag alarm invariants. The end-to-end check needs a
// live Kafka + DB (concrete kafka.Manager), so this locks the two bug-prone, pure
// pieces: the derived consumer-group name must stay in step with its single
// definition in handlers, and the sink-lag issue id must be distinct from the
// source-lag issue id (anti-stomp).

import (
	"fmt"
	"testing"

	"github.com/rsync-ai/backend-orchestrator/internal/handlers"
)

func TestDerivedSinkConsumerGroupIsTheLegacyName(t *testing.T) {
	// This used to be TestSinkConsumerGroupMatchesExecutor, and its "cross-check
	// against the executor's exact expression" was a COPY of that expression, not a
	// call into it — so it kept passing while the executor moved. It has moved: the
	// executor now qualifies all three sink groups through kafkaclient.Group
	// (executor/sink_consumer_group.go), so the live name is "rsync.sink-<id8>".
	//
	// The derived name below is deliberately NOT updated to match. It is the
	// fallback for a pipeline with no pipeline_dependencies row — a pre-manifest
	// pipeline, whose sink really was started under the unqualified name — and for
	// those the historical spelling is the correct guess. Anything started since
	// registers its real group, and ResolveSinkConsumerGroup reads that instead;
	// that is the only path that is authoritative, which is why this one is allowed
	// to be a guess.
	cases := []struct {
		pid  string
		want string
	}{
		{"a1b2c3d4-5678-90ab-cdef-1234567890ab", "sink-a1b2c3d4"}, // UUID → first 8
		{"short", "sink-short"},                                   // <=8 chars → whole id
		{"", "sink-unknown"},                                      // SafeID8("") → "unknown"
	}
	for _, c := range cases {
		if got := sinkConsumerGroup(c.pid); got != c.want {
			t.Errorf("sinkConsumerGroup(%q) = %q, want %q", c.pid, got, c.want)
		}
		// The prefix is defined once, in handlers. Assert against that rather than
		// against a second copy of the literal.
		if got, derived := sinkConsumerGroup(c.pid), handlers.DerivedSinkConsumerGroup(c.pid); got != derived {
			t.Errorf("sinkConsumerGroup(%q)=%q drifted from handlers.DerivedSinkConsumerGroup %q", c.pid, got, derived)
		}
	}
}

func TestSinkLagIssueDistinctFromSourceLag(t *testing.T) {
	// The sink-drain lag issue MUST NOT share an id with the source-lag issue
	// ("cdc-lag-%s"): sentinel_active_issues keys on id, so a shared id would let a
	// healthy-source resolveLagIssue clear a live dead-sink alarm (and vice versa).
	for _, pid := range []string{"a1b2c3d4-5678-90ab-cdef-1234567890ab", "short", ""} {
		sink := sinkLagIssueID(pid)
		source := fmt.Sprintf("cdc-lag-%s", pid)
		if sink == source {
			t.Fatalf("pid=%q: sink and source lag issue ids must differ, both = %q", pid, sink)
		}
		if want := "cdc-sink-lag-" + pid; sink != want {
			t.Errorf("sinkLagIssueID(%q) = %q, want %q", pid, sink, want)
		}
	}
}
