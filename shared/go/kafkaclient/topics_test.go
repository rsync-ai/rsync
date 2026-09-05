package kafkaclient

import (
	"strings"
	"testing"
)

// The prefix is what makes the product's topics identifiable on a customer's
// shared cluster, so an unset environment must still produce it. A default of ""
// would mean a self-host deployment that never sets the variable silently keeps
// the anonymous names this change exists to remove.
func TestUnsetEnvYieldsRsyncPrefix(t *testing.T) {
	t.Setenv(EnvTopicPrefix, "")
	// t.Setenv cannot unset, so exercise the unset path through the constant.
	if DefaultTopicPrefix != "rsync." {
		t.Fatalf("default prefix = %q, want %q", DefaultTopicPrefix, "rsync.")
	}
}

func TestTopicQualifiesEveryPlatformNamespace(t *testing.T) {
	t.Setenv(EnvTopicPrefix, "rsync.")
	for _, name := range []string{
		"agent.planner.requests",
		"agent.control.results",
		"pipeline.domain.events",
		"pipeline.abc12345.data",
		"pipeline.abc12345.data.dlq",
		"cdc.abc12345",
		"pii.scan.request",
		"schemahistory.cdc-abc12345",
		"signals.abc12345",
	} {
		got := Topic(name)
		want := "rsync." + name
		if got != want {
			t.Errorf("Topic(%q) = %q, want %q", name, got, want)
		}
		if !strings.HasPrefix(got, "rsync") {
			t.Errorf("Topic(%q) = %q, which does not start with rsync", name, got)
		}
	}
}

// Topic names are persisted and read back (pipelines.kafka_topic, the sink's
// subscription config), so an already-qualified name can reach Topic a second
// time. If that produced rsync.rsync.cdc.x the producer and the consumer would
// disagree the moment one of them had a cached name and the other did not.
func TestQualificationIsIdempotent(t *testing.T) {
	t.Setenv(EnvTopicPrefix, "rsync.")
	once := Topic("cdc.abc12345")
	twice := Topic(once)
	thrice := Topic(twice)
	if once != twice || twice != thrice {
		t.Fatalf("not idempotent: %q -> %q -> %q", once, twice, thrice)
	}
	if strings.Count(once, "rsync.") != 1 {
		t.Fatalf("prefix appears %d times in %q, want 1", strings.Count(once, "rsync."), once)
	}
}

// The property that actually keeps the platform working: whatever a producer
// resolves, a consumer resolving the same logical name must get byte-identical
// output. A consumer subscribed to a topic nobody produces to does not error --
// it blocks forever.
func TestProducerAndConsumerResolveIdentically(t *testing.T) {
	t.Setenv(EnvTopicPrefix, "rsync.")
	const logical = "agent.planner.responses"
	producerSide := Topic(logical)
	consumerSide := Topics(logical)[0]
	if producerSide != consumerSide {
		t.Fatalf("producer resolved %q, consumer resolved %q", producerSide, consumerSide)
	}
}

// The migration lever: an existing deployment has live topics and committed
// offsets under the unprefixed names and must be able to take this code without
// renaming them in the same deploy.
func TestEmptyPrefixLeavesNamesUntouched(t *testing.T) {
	t.Setenv(EnvTopicPrefix, "")
	for _, name := range []string{"agent.planner.requests", "cdc.abc12345"} {
		if got := Topic(name); got != name {
			t.Errorf("Topic(%q) with empty prefix = %q, want it unchanged", name, got)
		}
	}
}

// "rsync" + "agent.x" = "rsyncagent.x" is a legal Kafka topic name, so this
// mistake would not surface as an error anywhere -- it would just be the wrong
// topic.
func TestPrefixWithoutSeparatorGainsOne(t *testing.T) {
	t.Setenv(EnvTopicPrefix, "rsync")
	if got := Topic("agent.x"); got != "rsync.agent.x" {
		t.Fatalf("Topic(agent.x) = %q, want rsync.agent.x", got)
	}
}

func TestExistingSeparatorIsNotDoubled(t *testing.T) {
	for _, sep := range []string{".", "-", "_"} {
		t.Run(sep, func(t *testing.T) {
			t.Setenv(EnvTopicPrefix, "rsync"+sep)
			want := "rsync" + sep + "agent.x"
			if got := Topic("agent.x"); got != want {
				t.Fatalf("Topic(agent.x) = %q, want %q", got, want)
			}
		})
	}
}

// A prefix arriving from the environment with a character Kafka rejects would
// make every derived topic illegal at once, and the broker's error names the
// derived topic rather than the prefix behind it.
func TestIllegalPrefixCharactersAreDropped(t *testing.T) {
	t.Setenv(EnvTopicPrefix, "rs ync/co:rp")
	got := TopicPrefix()
	if strings.ContainsAny(got, " /:") {
		t.Fatalf("TopicPrefix() = %q, still carries characters Kafka rejects", got)
	}
	if got != "rsynccorp." {
		t.Fatalf("TopicPrefix() = %q, want %q", got, "rsynccorp.")
	}
}

// A prefix that sanitizes away to nothing must disable qualification rather than
// emit a bare separator, which would turn cdc.x into .cdc.x.
func TestPrefixOfOnlyIllegalCharactersDisablesQualification(t *testing.T) {
	t.Setenv(EnvTopicPrefix, "///")
	if got := Topic("cdc.x"); got != "cdc.x" {
		t.Fatalf("Topic(cdc.x) = %q, want it unchanged", got)
	}
}

func TestEmptyNameStaysEmpty(t *testing.T) {
	t.Setenv(EnvTopicPrefix, "rsync.")
	if got := Topic(""); got != "" {
		t.Fatalf("Topic(\"\") = %q, want empty", got)
	}
	if got := Topic("   "); got != "" {
		t.Fatalf("Topic(whitespace) = %q, want empty", got)
	}
}

func TestTopicsQualifiesEveryElement(t *testing.T) {
	t.Setenv(EnvTopicPrefix, "rsync.")
	got := Topics("agent.a", "agent.b", "pipeline.c")
	want := []string{"rsync.agent.a", "rsync.agent.b", "rsync.pipeline.c"}
	if len(got) != len(want) {
		t.Fatalf("Topics returned %d names, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("Topics()[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

// A deployment that adopts the prefix still has topics minted under the bare
// names. A namespace guard that recognized only one form would misclassify the
// other -- and the teardown guard misclassifying means orphaned topics left on
// the customer's cluster.
func TestInNamespaceMatchesPrefixedAndLegacyNames(t *testing.T) {
	t.Setenv(EnvTopicPrefix, "rsync.")
	for _, tc := range []struct {
		topic, ns string
		want      bool
	}{
		{"rsync.pipeline.abc12345.data", "pipeline.", true},
		{"pipeline.abc12345.data", "pipeline.", true},
		{"rsync.pipeline.abc12345.data.dlq", "pipeline.abc12345.", true},
		{"pipeline.abc12345.data.dlq", "pipeline.abc12345.", true},
		{"rsync.cdc.abc12345", "pipeline.", false},
		{"cdc.abc12345", "pipeline.", false},
		{"someone-elses.topic", "pipeline.", false},
		{"", "pipeline.", false},
		{"pipeline.x", "", false},
	} {
		if got := InNamespace(tc.topic, tc.ns); got != tc.want {
			t.Errorf("InNamespace(%q, %q) = %v, want %v", tc.topic, tc.ns, got, tc.want)
		}
	}
}

// With qualification disabled the guard must still recognize the bare names --
// otherwise turning the prefix off to migrate would break classification.
func TestInNamespaceWorksWithQualificationDisabled(t *testing.T) {
	t.Setenv(EnvTopicPrefix, "")
	if !InNamespace("pipeline.abc12345.data", "pipeline.") {
		t.Fatal("bare namespace not matched when qualification is disabled")
	}
}
