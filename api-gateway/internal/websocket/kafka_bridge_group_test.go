package websocket

import (
	"strings"
	"testing"
)

// The bridge runs one consumer group per bridged topic — four of them, after the
// eleven producer-less subscriptions were pruned — which is still why a customer's
// operator wants a PREFIXED grant rather than an enumerated one. Every id it mints has to carry
// the namespace or the grant is half-covered, and a half-covered grant is worse
// than none: the process stays healthy and only the live UI goes quiet.
func TestBridgeGroupIDIsNamespaced(t *testing.T) {
	t.Setenv("KAFKA_TOPIC_PREFIX", "rsync.")

	got := bridgeGroupID("rsync.pipeline.domain.events")
	if !strings.HasPrefix(got, "rsync.") {
		t.Fatalf("bridgeGroupID = %q, want it under the rsync. namespace", got)
	}
	if got != "rsync.websocket-bridge-pipeline.domain.events" {
		t.Fatalf("bridgeGroupID = %q, want %q", got, "rsync.websocket-bridge-pipeline.domain.events")
	}
}

// The topic arriving here is already qualified, so a naive concatenation gives
// "rsync.websocket-bridge-rsync.pipeline.domain.events". That is still covered
// by the ACL, so it is not a security bug — it is a readability one, and
// readability of `kafka-consumer-groups --list` on a shared cluster is half of
// what the namespace is for.
func TestBridgeGroupIDDoesNotRepeatThePrefixInside(t *testing.T) {
	t.Setenv("KAFKA_TOPIC_PREFIX", "rsync.")

	got := bridgeGroupID("rsync.pipeline.domain.events")
	if n := strings.Count(got, "rsync."); n != 1 {
		t.Fatalf("bridgeGroupID = %q carries the prefix %d times, want 1", got, n)
	}
}

// The migration lever, at the value the broker sees. With qualification
// disabled the id must be byte-identical to what this bridge used before the
// namespace existed, or every one of its groups restarts from
// auto.offset.reset on the deploy that adopts this code.
func TestBridgeGroupIDEmptyPrefixMatchesTheLegacyID(t *testing.T) {
	t.Setenv("KAFKA_TOPIC_PREFIX", "")

	got := bridgeGroupID("pipeline.domain.events")
	if got != "websocket-bridge-pipeline.domain.events" {
		t.Fatalf("bridgeGroupID = %q, want the pre-namespacing id %q", got, "websocket-bridge-pipeline.domain.events")
	}
}

// One group per topic is the invariant the bridge depends on: two topics
// sharing a group would have their partitions split between them and each
// would see only part of its own stream.
func TestBridgeGroupIDIsDistinctPerTopic(t *testing.T) {
	t.Setenv("KAFKA_TOPIC_PREFIX", "rsync.")

	seen := map[string]string{}
	for _, topic := range []string{
		"rsync.pipeline.domain.events",
		"rsync.pipeline.agent.telemetry",
		"rsync.task.results",
		"rsync.agent.planner.progress",
		"rsync.cdc.status.updates",
	} {
		id := bridgeGroupID(topic)
		if prev, dup := seen[id]; dup {
			t.Errorf("topics %q and %q both map to group %q", prev, topic, id)
		}
		seen[id] = topic
	}
}

// An operator whose prefix has no separator gets one, the same way topics do —
// otherwise "rsync" + the id reads "rsyncwebsocket-bridge-…", which is a legal
// group name and so fails by not matching the ACL rather than by erroring.
func TestBridgeGroupIDInheritsThePrefixNormalization(t *testing.T) {
	t.Setenv("KAFKA_TOPIC_PREFIX", "acme")

	got := bridgeGroupID("acme.pipeline.domain.events")
	if got != "acme.websocket-bridge-pipeline.domain.events" {
		t.Fatalf("bridgeGroupID = %q, want %q", got, "acme.websocket-bridge-pipeline.domain.events")
	}
}
