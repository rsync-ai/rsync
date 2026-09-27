package main

import (
	"testing"

	"github.com/rsync-ai/shared/kafkaclient"
)

// The adapter's only Kafka client is the producer built by newProducerConfig. These
// pin the identity it presents: a customer's broker keys logs, throttling and quota
// metrics off client.id, so an anonymous default makes our traffic unattributable.

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

func TestProducerClientIDIsDistinctFromTheAnonymousDefault(t *testing.T) {
	// Guards a silent regression: FromEnv still returns a valid config, so
	// nothing errors -- the identity just collapses back into the shared default.
	if kafkaclient.DefaultClientID(kafkaServiceName) == kafkaclient.DefaultClientID("") {
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
