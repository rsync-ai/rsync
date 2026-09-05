package main

import (
	"testing"

	"github.com/rsync-ai/backend-temporal-adapter/internal/adapter"
	"github.com/rsync-ai/shared/kafkaclient"
)

// The adapter is a single process holding both a producer (here) and a consumer
// group (internal/adapter). They resolve their Kafka config independently, so
// the failure this guards is drift: one names itself, the other stays anonymous,
// and a customer's broker logs attribute half our traffic to a shared default.

func TestProducerNamesItselfToTheBroker(t *testing.T) {
	t.Setenv("KAFKA_CLIENT_ID", "")
	cfg, security, err := newProducerConfig("kafka:29092")
	if err != nil {
		t.Fatalf("newProducerConfig: %v", err)
	}
	const want = "rsync-temporal-adapter"
	if security.ClientID != want {
		t.Errorf("resolved ClientID = %q, want %q", security.ClientID, want)
	}
	// The resolved config means nothing if the applier drops it on the way to
	// sarama -- that is the half that actually reaches the broker.
	if cfg.ClientID != want {
		t.Errorf("sarama ClientID = %q, want %q", cfg.ClientID, want)
	}
}

func TestProducerAndConsumerPresentOneIdentity(t *testing.T) {
	t.Setenv("KAFKA_CLIENT_ID", "")
	_, producer, err := newProducerConfig("kafka:29092")
	if err != nil {
		t.Fatalf("newProducerConfig: %v", err)
	}
	// Calls the consumer's REAL config path, not a re-derivation of it. An
	// earlier version of this test rebuilt the config from adapter.ServiceName
	// itself and so passed even with the consumer reverted to an anonymous
	// FromEnv -- it asserted the constant, not the code.
	_, consumer, err := adapter.NewConsumerConfig("kafka:29092")
	if err != nil {
		t.Fatalf("consumer config: %v", err)
	}
	if producer.ClientID != consumer.ClientID {
		t.Errorf("producer identifies as %q but consumer as %q -- one process, two "+
			"identities; a throttled cluster cannot attribute either half", producer.ClientID, consumer.ClientID)
	}
}

func TestProducerClientIDIsDistinctFromTheAnonymousDefault(t *testing.T) {
	// Guards a silent regression: FromEnv still returns a valid config, so
	// nothing errors -- the identity just collapses back into the shared default.
	if kafkaclient.DefaultClientID(adapter.ServiceName) == kafkaclient.DefaultClientID("") {
		t.Fatal("the adapter's client.id is indistinguishable from the anonymous default")
	}
}

func TestProducerClientIDEnvOverrideStillWins(t *testing.T) {
	t.Setenv("KAFKA_CLIENT_ID", "one-identity-for-the-platform")
	cfg, _, err := newProducerConfig("kafka:29092")
	if err != nil {
		t.Fatalf("newProducerConfig: %v", err)
	}
	if cfg.ClientID != "one-identity-for-the-platform" {
		t.Errorf("ClientID = %q, want the operator's KAFKA_CLIENT_ID to win", cfg.ClientID)
	}
}

func TestProducerKeepsMultiBrokerBootstrap(t *testing.T) {
	// Regression the original comment names: []string{brokers} collapsed a CSV
	// into one unresolvable hostname. Asserted here so the split above cannot
	// quietly reintroduce it.
	_, security, err := newProducerConfig("b1:9093,b2:9093,b3:9093")
	if err != nil {
		t.Fatalf("newProducerConfig: %v", err)
	}
	if len(security.Brokers) != 3 {
		t.Errorf("Brokers = %v, want 3 separate brokers", security.Brokers)
	}
}

// The consumer GROUP id is a separate identity from the client id above, and it was the
// one that went unasserted. Group ids share the topic namespace on purpose
// (kafkaclient.Group is Topic) so one PREFIXED ACL covers both; the adapter's group was
// a bare literal, so it sat outside the grant that covers everything else this process
// touches. Nothing about that is loud: JoinGroup is denied, the Consume loop logs and
// retries forever, and agent.control.results is simply never drained -- so agent results
// stop signalling their workflows and every pipeline hangs with no error surfaced.
func TestConsumerGroupIsNamespaced(t *testing.T) {
	const want = "rsync.temporal-adapter-consumer"
	if got := adapter.ConsumerGroupID(); got != want {
		t.Errorf("consumer group id = %q, want %q", got, want)
	}
}

func TestConsumerGroupFollowsACustomPrefix(t *testing.T) {
	// The case above pins the default. This one pins the property, and it is the one
	// that would have caught the bug on a cluster that actually has a custom prefix:
	// an unqualified id cannot move when the prefix changes, and a qualified one must.
	t.Setenv("KAFKA_TOPIC_PREFIX", "acme.")
	const want = "acme.temporal-adapter-consumer"
	if got := adapter.ConsumerGroupID(); got != want {
		t.Errorf("consumer group id = %q under KAFKA_TOPIC_PREFIX=acme., want %q — the id "+
			"is not being qualified, so it falls outside the customer's PREFIXED grant", got, want)
	}
}
