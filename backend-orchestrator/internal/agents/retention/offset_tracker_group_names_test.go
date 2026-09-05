package retention

import (
	"strings"
	"testing"

	kafkaclient "github.com/rsync-ai/shared/kafkaclient"
)

// simulateConsumerGroups feeds CheckAllConsumersCaughtUp, which safety.go:103
// consults before letting retention delete data. If its names are not the names
// consumers actually join, that gate is evaluated against groups that do not
// exist -- and nothing anywhere raises, because "no offsets for this group" is
// indistinguishable from "this group has not committed yet".
//
// The authority for the real spelling is groupIDForTopic in
// internal/agents/consumer/kafka_identity.go. The expected values here are
// spelled as literals rather than built with kafkaclient.Group, because deriving
// them from the same function the code under test uses would make this agree by
// construction and it could never fail.
func TestFallbackConsumerGroupsCarryTheNamespace(t *testing.T) {
	t.Setenv("KAFKA_TOPIC_PREFIX", "rsync.")

	got := (&OffsetTracker{}).simulateConsumerGroups("rsync.cdc.abc12345")
	want := []string{
		"rsync.rsync-pipeline-rsync.cdc.abc12345",
		"rsync.rsync-transform-rsync.cdc.abc12345",
	}
	if len(got) != len(want) {
		t.Fatalf("got %d group(s) %v, want %d", len(got), got, len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("fallback group %d = %q, want %q -- an unqualified id is refused at "+
				"JoinGroup under a PREFIXED ACL and names a group nobody joins", i, got[i], want[i])
		}
	}
}

// The empty prefix is the documented migration lever; it has to turn this off too.
func TestFallbackConsumerGroupsHonourTheEmptyPrefixLever(t *testing.T) {
	t.Setenv("KAFKA_TOPIC_PREFIX", "")

	got := (&OffsetTracker{}).simulateConsumerGroups("cdc.abc12345")
	want := []string{"rsync-pipeline-cdc.abc12345", "rsync-transform-cdc.abc12345"}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("fallback group %d = %q, want %q with KAFKA_TOPIC_PREFIX=\"\"", i, got[i], want[i])
		}
	}
}

// Anti-vacuity: if Group() ever became the identity function, both tests above
// would still pass under the default prefix. Prove the helper is live.
func TestFallbackNamesActuallyRouteThroughGroup(t *testing.T) {
	t.Setenv("KAFKA_TOPIC_PREFIX", "acme.")
	for _, g := range (&OffsetTracker{}).simulateConsumerGroups("t") {
		if !strings.HasPrefix(g, "acme.") {
			t.Fatalf("%q does not follow KAFKA_TOPIC_PREFIX, so it is a bare literal", g)
		}
	}
	if kafkaclient.Group("x") == "x" {
		t.Fatal("kafkaclient.Group is a no-op under acme., so this file proves nothing")
	}
}
