package main

import (
	"testing"

	"github.com/rsync-ai/shared/kafkaclient"
	"github.com/rsync-ai/shared/kafkaclient/kgoauth"
)

// The sink reaches the broker through kgoauth (kafka-go), not saramaauth. The
// Go services' identity tests all exercise the sarama applier, so nothing they
// assert covers this path -- these tests are the only thing standing between a
// refactor and the sink silently going back to an anonymous client.id on a
// customer's cluster.

func TestSinkNamesItselfToTheBroker(t *testing.T) {
	t.Setenv("KAFKA_CLIENT_ID", "")
	if err := initKafkaSecurity("kafka:29092"); err != nil {
		t.Fatalf("initKafkaSecurity: %v", err)
	}
	if got, want := kafkaSecurity.ClientID, "rsync-kafka-sink"; got != want {
		t.Errorf("ClientID = %q, want %q -- the sink is the process most likely to be "+
			"throttled on a shared cluster, and an anonymous default makes that unattributable", got, want)
	}
}

func TestSinkClientIDIsDistinctFromTheAnonymousDefault(t *testing.T) {
	// The regression this guards is a silent one: FromEnv still returns a valid
	// config, so nothing fails -- the identity just collapses back to the shared
	// default. Compare against that default rather than hardcoding it twice.
	if kafkaclient.DefaultClientID(kafkaServiceName) == kafkaclient.DefaultClientID("") {
		t.Fatal("the sink's client.id is indistinguishable from the anonymous default")
	}
}

func TestSinkClientIDReachesTheKafkaGoWire(t *testing.T) {
	t.Setenv("KAFKA_CLIENT_ID", "")
	if err := initKafkaSecurity("kafka:29092"); err != nil {
		t.Fatalf("initKafkaSecurity: %v", err)
	}
	// Both appliers, because the sink uses both: Transport for the writer,
	// Dialer for the reader. One carrying the id is not proof the other does.
	//
	// Goes through kgoauth.Transport directly rather than kafkaTransport(): that
	// wrapper memoises behind a sync.Once, so whichever test touched it first
	// would pin the result and every later assertion would be reading a cached
	// value instead of checking anything. This asserts the exact call the wrapper
	// makes -- kgoauth.Transport(kafkaSecurity) -- with no shared state.
	tr, err := kgoauth.Transport(kafkaSecurity)
	if err != nil {
		t.Fatalf("kgoauth.Transport: %v", err)
	}
	if tr.ClientID != "rsync-kafka-sink" {
		t.Errorf("transport ClientID = %q, want rsync-kafka-sink", tr.ClientID)
	}
	d, err := kafkaDialer("kafka:29092")
	if err != nil {
		t.Fatalf("kafkaDialer: %v", err)
	}
	if d.ClientID != "rsync-kafka-sink" {
		t.Errorf("dialer ClientID = %q, want rsync-kafka-sink", d.ClientID)
	}
}

func TestSinkClientIDEnvOverrideStillWins(t *testing.T) {
	t.Setenv("KAFKA_CLIENT_ID", "one-identity-for-the-platform")
	if err := initKafkaSecurity("kafka:29092"); err != nil {
		t.Fatalf("initKafkaSecurity: %v", err)
	}
	if kafkaSecurity.ClientID != "one-identity-for-the-platform" {
		t.Errorf("ClientID = %q, want the operator's KAFKA_CLIENT_ID to win", kafkaSecurity.ClientID)
	}
}
