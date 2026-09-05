package consumer

import (
	"strings"
	"testing"

	kafkaclient "github.com/rsync-ai/shared/kafkaclient"
)

// A4. Every shipped compose file hands the orchestrator KAFKA_BROKERS
// (docker-compose.yml:713) and nothing hands it KAFKA_BOOTSTRAP_SERVERS, while
// this agent is on by default (ENABLE_CONSUMER_AGENT, config/config.go:239). It
// read only the second name, so in every deployment — prod included — it held a
// Kafka client pointed at localhost:9092 and made lag, autoscale and
// auto-restart decisions against a cluster that was never there.

func TestDefaultConfigResolvesBrokersFromKafkaBrokers(t *testing.T) {
	t.Setenv(kafkaclient.EnvBrokers, "kafka:29092")
	// The variable the old code read exclusively is deliberately absent: this is
	// the shipped orchestrator's environment.
	t.Setenv(kafkaclient.EnvBootstrapServers, "")

	if got := DefaultConfig().Kafka.BootstrapServers; got != "kafka:29092" {
		t.Fatalf("BootstrapServers = %q, want %q", got, "kafka:29092")
	}
}

func TestDefaultConfigStillReadsBootstrapServers(t *testing.T) {
	t.Setenv(kafkaclient.EnvBrokers, "")
	t.Setenv(kafkaclient.EnvBootstrapServers, "standalone:9092")

	if got := DefaultConfig().Kafka.BootstrapServers; got != "standalone:9092" {
		t.Fatalf("BootstrapServers = %q, want %q", got, "standalone:9092")
	}
}

func TestKafkaBrokersWinsOverBootstrapServers(t *testing.T) {
	// Same precedence kafkaclient.FromEnv applies. Two rules that could disagree
	// is what produced the defect above.
	t.Setenv(kafkaclient.EnvBrokers, "real:29092")
	t.Setenv(kafkaclient.EnvBootstrapServers, "stale:9092")

	if got := DefaultConfig().Kafka.BootstrapServers; got != "real:29092" {
		t.Fatalf("BootstrapServers = %q, want KAFKA_BROKERS to win", got)
	}
}

func TestDefaultConfigFallsBackToLocalhostWhenNeitherIsSet(t *testing.T) {
	// The standalone shape stays as it was; the bug was never the default, it
	// was reaching the default in a container.
	t.Setenv(kafkaclient.EnvBrokers, "")
	t.Setenv(kafkaclient.EnvBootstrapServers, "")

	if got := DefaultConfig().Kafka.BootstrapServers; got != DefaultBootstrapServers {
		t.Fatalf("BootstrapServers = %q, want %q", got, DefaultBootstrapServers)
	}
}

// The second half of A4: even once the config field is right, the registry used
// to overwrite the resolved brokers with it via WithBrokers. Assert the
// environment reaches the client the agent actually dials with.
func TestRegistryKafkaSecurityUsesTheEnvironmentBrokers(t *testing.T) {
	t.Setenv(kafkaclient.EnvBrokers, "kafka:29092")
	t.Setenv(kafkaclient.EnvBootstrapServers, "")

	// A config carrying the stale value the old code re-imposed on every call.
	r := &Registry{config: &Config{Kafka: KafkaConfig{BootstrapServers: "localhost:9092"}}}

	security, err := r.kafkaSecurity()
	if err != nil {
		t.Fatalf("kafkaSecurity: %v", err)
	}
	if len(security.Brokers) != 1 || security.Brokers[0] != "kafka:29092" {
		t.Fatalf("Brokers = %v, want [kafka:29092] — the config field must not override the environment", security.Brokers)
	}
}

func TestRegistryKafkaSecurityFallsBackToTheConfigField(t *testing.T) {
	// With the environment silent the struct field is still authoritative, so a
	// caller that builds a Config by hand is not broken by the fix.
	t.Setenv(kafkaclient.EnvBrokers, "")
	t.Setenv(kafkaclient.EnvBootstrapServers, "")

	r := &Registry{config: &Config{Kafka: KafkaConfig{BootstrapServers: "programmatic:9092"}}}

	security, err := r.kafkaSecurity()
	if err != nil {
		t.Fatalf("kafkaSecurity: %v", err)
	}
	if len(security.Brokers) != 1 || security.Brokers[0] != "programmatic:9092" {
		t.Fatalf("Brokers = %v, want [programmatic:9092]", security.Brokers)
	}
}

func TestRegistryKafkaSecurityCarriesTheServiceClientID(t *testing.T) {
	// A6/client.id: an anonymous default client.id is unattributable in the
	// customer's broker logs and quota metrics.
	t.Setenv(kafkaclient.EnvBrokers, "kafka:29092")
	t.Setenv(kafkaclient.EnvClientID, "")

	r := &Registry{config: &Config{Kafka: KafkaConfig{BootstrapServers: "kafka:29092"}}}

	security, err := r.kafkaSecurity()
	if err != nil {
		t.Fatalf("kafkaSecurity: %v", err)
	}
	if want := kafkaclient.DefaultClientID(kafkaServiceName); security.ClientID != want {
		t.Fatalf("ClientID = %q, want %q", security.ClientID, want)
	}
	if !strings.HasPrefix(security.ClientID, kafkaclient.ClientIDNamespace) {
		t.Fatalf("ClientID %q is not under the %q namespace", security.ClientID, kafkaclient.ClientIDNamespace)
	}
}

func TestRegistryKafkaSecurityLetsKafkaClientIDWin(t *testing.T) {
	t.Setenv(kafkaclient.EnvBrokers, "kafka:29092")
	t.Setenv(kafkaclient.EnvClientID, "acme-platform")

	r := &Registry{config: &Config{Kafka: KafkaConfig{BootstrapServers: "kafka:29092"}}}

	security, err := r.kafkaSecurity()
	if err != nil {
		t.Fatalf("kafkaSecurity: %v", err)
	}
	if security.ClientID != "acme-platform" {
		t.Fatalf("ClientID = %q, want the operator's KAFKA_CLIENT_ID to win", security.ClientID)
	}
}
