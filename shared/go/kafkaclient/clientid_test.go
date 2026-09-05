package kafkaclient

import (
	"strings"
	"testing"
)

// On a managed cluster (MSK, Confluent Cloud) throttle metrics and quotas key
// off client.id. With it unset, every connection from every rsync service was
// the library default, so a throttled customer could not tell which service --
// or even which tenant -- caused it.
func TestDefaultClientIDNamesTheProductAndTheService(t *testing.T) {
	for _, tc := range []struct{ service, want string }{
		{"orchestrator", "rsync-orchestrator"},
		{"api-gateway", "rsync-api-gateway"},
		{"kafka-sink", "rsync-kafka-sink"},
		{"temporal-adapter", "rsync-temporal-adapter"},
		{"", "rsync"}, // no name to give, but still not anonymous
		{"   ", "rsync"},
	} {
		if got := DefaultClientID(tc.service); got != tc.want {
			t.Errorf("DefaultClientID(%q) = %q, want %q", tc.service, got, tc.want)
		}
	}
}

// Brokers below 1.0.0 reject a client.id outside [a-zA-Z0-9._-] outright, and
// sarama refuses to even build a config for one. On a newer broker it is worse:
// the stray character survives into metric names and quota rules where nobody
// looks.
func TestDefaultClientIDDropsCharactersKafkaRejects(t *testing.T) {
	got := DefaultClientID("orch estrator/prod:1")
	if strings.ContainsAny(got, " /:") {
		t.Fatalf("DefaultClientID = %q, still carries characters Kafka rejects", got)
	}
	if got != "rsync-orchestratorprod1" {
		t.Fatalf("DefaultClientID = %q, want rsync-orchestratorprod1", got)
	}
	// A name that sanitizes away to nothing must not leave a trailing separator.
	if got := DefaultClientID("///"); got != ClientIDNamespace {
		t.Fatalf("DefaultClientID(///) = %q, want %q", got, ClientIDNamespace)
	}
}

func TestFromEnvForServiceDerivesClientID(t *testing.T) {
	clearKafkaEnv(t)
	t.Setenv(EnvBootstrapServers, "b:9092")

	c, err := FromEnvForService("orchestrator", "kafka:29092")
	if err != nil {
		t.Fatal(err)
	}
	if c.ClientID != "rsync-orchestrator" {
		t.Errorf("ClientID = %q, want rsync-orchestrator", c.ClientID)
	}

	// The one-arg form the eight existing call sites use must keep compiling and
	// must still stop being anonymous.
	c, err = FromEnv("kafka:29092")
	if err != nil {
		t.Fatal(err)
	}
	if c.ClientID != ClientIDNamespace {
		t.Errorf("FromEnv ClientID = %q, want %q", c.ClientID, ClientIDNamespace)
	}
}

// A deployment that wants one identity for the whole platform sets the variable
// once; no service may then override it from code.
func TestExplicitClientIDWinsOverTheServiceName(t *testing.T) {
	clearKafkaEnv(t)
	t.Setenv(EnvBootstrapServers, "b:9092")
	t.Setenv(EnvClientID, "acme-data-platform")

	c, err := FromEnvForService("orchestrator", "kafka:29092")
	if err != nil {
		t.Fatal(err)
	}
	if c.ClientID != "acme-data-platform" {
		t.Errorf("ClientID = %q, want the env var to win", c.ClientID)
	}
	if got := c.WithServiceName("kafka-sink").ClientID; got != "acme-data-platform" {
		t.Errorf("WithServiceName overrode an explicit %s: got %q", EnvClientID, got)
	}
}

// KAFKA_CLIENT_ID is operator input, so it gets the same sanitization a
// code-supplied name does.
func TestExplicitClientIDIsSanitized(t *testing.T) {
	clearKafkaEnv(t)
	t.Setenv(EnvBootstrapServers, "b:9092")
	t.Setenv(EnvClientID, "acme data/platform")

	c, err := FromEnv("kafka:29092")
	if err != nil {
		t.Fatal(err)
	}
	if strings.ContainsAny(c.ClientID, " /") {
		t.Fatalf("ClientID = %q, still carries characters Kafka rejects", c.ClientID)
	}
}

// The hand-off for the other agents: a service holding a Config it did not
// build from the environment still gets to name itself.
func TestWithServiceName(t *testing.T) {
	clearKafkaEnv(t)
	t.Setenv(EnvBootstrapServers, "b:9092")
	c, err := FromEnv("kafka:29092")
	if err != nil {
		t.Fatal(err)
	}

	if got := c.WithServiceName("api-gateway").ClientID; got != "rsync-api-gateway" {
		t.Errorf("WithServiceName(api-gateway) = %q, want rsync-api-gateway", got)
	}
	// An empty name is ignored rather than clearing the id: losing the identity
	// is the failure this exists to prevent.
	if got := c.WithServiceName("").ClientID; got != c.ClientID {
		t.Errorf("WithServiceName(\"\") changed the id to %q", got)
	}
	// Value receiver: the caller's copy must be untouched.
	before := c.ClientID
	_ = c.WithServiceName("kafka-sink")
	if c.ClientID != before {
		t.Errorf("WithServiceName mutated the receiver: %q -> %q", before, c.ClientID)
	}
}

// A struct-literal Config (the shape in the existing call sites' tests) has no
// client.id to give, and blanking sarama's own default would be a regression.
func TestZeroConfigCarriesNoClientID(t *testing.T) {
	c := Config{Brokers: []string{"b:9092"}, SecurityProtocol: ProtocolPlaintext}
	if c.ClientID != "" {
		t.Errorf("a zero Config must not invent a client id, got %q", c.ClientID)
	}
}
