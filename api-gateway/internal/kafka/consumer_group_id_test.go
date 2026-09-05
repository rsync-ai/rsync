package kafka

import (
	"strings"
	"testing"
)

// newConsumerGroupIDs builds a Consumer the way main() does and returns the
// group id every one of its per-topic readers ended up with.
//
// The readers are closed on cleanup: kafka-go starts a coordinator goroutine
// per reader, and 127.0.0.1:1 refuses at once, so nothing here waits on a
// broker. What is being read is the config the constructor wrote, which is the
// exact value a JoinGroup would carry.
func newConsumerGroupIDs(t *testing.T, prefix, logical string) []string {
	t.Helper()
	clearKafkaEnv(t)
	t.Setenv("KAFKA_TOPIC_PREFIX", prefix)

	c := NewConsumer(
		[]string{"127.0.0.1:1"},
		[]string{"rsync.agent.planner.responses", "rsync.pii.scan.response"},
		logical,
	)
	t.Cleanup(func() { _ = c.Close() })

	ids := make([]string, 0, len(c.readers))
	for _, r := range c.readers {
		ids = append(ids, r.Config().GroupID)
	}
	if len(ids) != 2 {
		t.Fatalf("built %d readers, want one per topic", len(ids))
	}
	return ids
}

// The agent-response consumer is the one whose group id main() spells as a bare
// logical name. Namespacing it is the constructor's job, so this asserts the
// value that actually reaches kafka-go rather than the literal in main().
func TestNewConsumerNamespacesTheGroupID(t *testing.T) {
	for _, got := range newConsumerGroupIDs(t, "rsync.", "api-gateway-consumer-group") {
		if got != "rsync.api-gateway-consumer-group" {
			t.Errorf("GroupID = %q, want %q: a PREFIXED \"rsync.\" ACL would not cover this group, and the "+
				"consumer would join, be refused, and never surface an error", got, "rsync.api-gateway-consumer-group")
		}
	}
}

// Every reader in one Consumer must share one group — that is what makes the
// per-topic fan-out a single logical consumer rather than N independent ones,
// each needing its own grant.
func TestNewConsumerGivesEveryReaderTheSameGroup(t *testing.T) {
	ids := newConsumerGroupIDs(t, "rsync.", "api-gateway-consumer-group")
	if ids[0] != ids[1] {
		t.Fatalf("readers joined different groups: %q vs %q", ids[0], ids[1])
	}
}

// The migration lever. A deployment with a live api-gateway-consumer-group and
// committed offsets sets KAFKA_TOPIC_PREFIX="" to take this code without every
// consumer restarting from auto.offset.reset.
func TestNewConsumerEmptyPrefixLeavesTheGroupIDUntouched(t *testing.T) {
	for _, got := range newConsumerGroupIDs(t, "", "api-gateway-consumer-group") {
		if got != "api-gateway-consumer-group" {
			t.Errorf("GroupID = %q with qualification disabled, want the bare id: this deployment would "+
				"silently reset its committed offsets", got)
		}
	}
}

// An operator who spells the prefix into their own group id must not be
// double-prefixed. This is the case that makes qualifying an operator-supplied
// value safe rather than presumptuous.
func TestNewConsumerDoesNotDoublePrefixAnAlreadyQualifiedGroupID(t *testing.T) {
	for _, got := range newConsumerGroupIDs(t, "rsync.", "rsync.acme-ingest") {
		if got != "rsync.acme-ingest" {
			t.Errorf("GroupID = %q, want %q", got, "rsync.acme-ingest")
		}
		if strings.Count(got, "rsync.") != 1 {
			t.Errorf("GroupID = %q carries the prefix %d times", got, strings.Count(got, "rsync."))
		}
	}
}
