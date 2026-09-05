package sentinel

import (
	"strings"
	"testing"

	kafkaclient "github.com/rsync-ai/shared/kafkaclient"
)

// The defect these guard: the two DLQ drains built their consumer-group id with
// time.Now().UnixNano(). A fresh group has no committed offset, so OffsetOldest
// rewound every invocation to the head of the DLQ — re-replaying messages the
// previous run had already replayed, and never reaching anything past maxReplay.

func TestStableGroupIDIsIdenticalAcrossCalls(t *testing.T) {
	// The core regression. A UnixNano-suffixed id fails this by construction.
	first := stableGroupID("sentinel-dlq-replay", "orders.dlq")
	for i := 0; i < 100; i++ {
		if got := stableGroupID("sentinel-dlq-replay", "orders.dlq"); got != first {
			t.Fatalf("group id is not stable: call %d gave %q, want %q", i, got, first)
		}
	}
}

func TestStableGroupIDIncludesPrefixAndTopic(t *testing.T) {
	got := stableGroupID("sentinel-dlq-replay", "orders.dlq")
	if want := kafkaclient.Group("sentinel-dlq-replay-orders.dlq"); got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestStableGroupIDSeparatesTopics(t *testing.T) {
	// Two DLQs must not share offsets — draining one would strand the other.
	a := stableGroupID("sentinel-dlq-replay", "orders.dlq")
	b := stableGroupID("sentinel-dlq-replay", "payments.dlq")
	if a == b {
		t.Fatalf("distinct topics collided on %q", a)
	}
}

func TestStableGroupIDSeparatesPrefixes(t *testing.T) {
	// protocol-fix and dlq-replay apply different transforms to the same DLQ,
	// so their progress must stay independent.
	a := stableGroupID("sentinel-dlq-replay", "orders.dlq")
	b := stableGroupID("sentinel-protocol-fix", "orders.dlq")
	if a == b {
		t.Fatalf("distinct prefixes collided on %q", a)
	}
}

func TestStableGroupIDSanitizesTopicCharset(t *testing.T) {
	// dlq_topic can come from issue metadata, so it is not trusted verbatim.
	// Qualification off so this measures the sanitizer and nothing else.
	t.Setenv(kafkaclient.EnvTopicPrefix, "")

	got := stableGroupID("p", "bad topic/with:junk")
	if strings.ContainsAny(got[2:], " /:") {
		t.Fatalf("unsanitized characters survived: %q", got)
	}
	if want := "p-bad_topic_with_junk"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestStableGroupIDTruncatesLongTopics(t *testing.T) {
	t.Setenv(kafkaclient.EnvTopicPrefix, "")

	got := stableGroupID("p", strings.Repeat("x", 500))
	if len(got) > 2+200 {
		t.Fatalf("length %d exceeds prefix+200", len(got))
	}
	// Still deterministic after truncation.
	if got != stableGroupID("p", strings.Repeat("x", 500)) {
		t.Fatal("truncated id is not stable")
	}
}

func TestStableGroupIDEmptyTopicYieldsBarePrefix(t *testing.T) {
	// A dangling separator would be an odd group name; the prefix alone is
	// still stable, which is the property that matters.
	if got, want := stableGroupID("sentinel-dlq-replay", "   "), kafkaclient.Group("sentinel-dlq-replay"); got != want {
		t.Fatalf("got %q, want bare prefix %q", got, want)
	}
}

func TestStableGroupIDTrimsSurroundingWhitespace(t *testing.T) {
	if a, b := stableGroupID("p", "  orders.dlq  "), stableGroupID("p", "orders.dlq"); a != b {
		t.Fatalf("whitespace changed the id: %q vs %q", a, b)
	}
}

// A6. On a shared (BYO) cluster the customer grants topics and consumer groups in
// one ACL set keyed on KAFKA_TOPIC_PREFIX. A group id minted outside that
// namespace is denied at join, and the sentinel's DLQ drain reports that as a
// drain that replayed nothing rather than as an authorization error.

func TestStableGroupIDIsNamespaced(t *testing.T) {
	t.Setenv(kafkaclient.EnvTopicPrefix, "acme.")

	for _, prefix := range []string{"sentinel-dlq-replay", "sentinel-protocol-fix"} {
		if got := stableGroupID(prefix, "orders.dlq"); !strings.HasPrefix(got, "acme.") {
			t.Fatalf("%s group %q is outside the configured namespace", prefix, got)
		}
	}
	// The bare-prefix fallback is a group id too, so it needs the same grant.
	if got := stableGroupID("sentinel-dlq-replay", ""); !strings.HasPrefix(got, "acme.") {
		t.Fatalf("bare-prefix group %q is outside the configured namespace", got)
	}
}

func TestStableGroupIDDefaultsToTheRsyncNamespace(t *testing.T) {
	// Unset means the shipped default, not "unqualified" — that distinction is
	// what makes the platform's groups greppable on a customer's cluster.
	if got := stableGroupID("sentinel-dlq-replay", "orders.dlq"); !strings.HasPrefix(got, kafkaclient.DefaultTopicPrefix) {
		t.Fatalf("got %q, want it under %q", got, kafkaclient.DefaultTopicPrefix)
	}
}

func TestStableGroupIDQualificationIsIdempotent(t *testing.T) {
	// The id is handed to sarama.NewConsumerGroup and logged; re-qualifying one
	// must not produce rsync.rsync.sentinel-dlq-replay-....
	got := stableGroupID("sentinel-dlq-replay", "orders.dlq")
	if requalified := kafkaclient.Group(got); requalified != got {
		t.Fatalf("re-qualifying %q gave %q", got, requalified)
	}
}
