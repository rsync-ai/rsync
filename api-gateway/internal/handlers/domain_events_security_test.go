package handlers

import (
	"context"
	"strings"
	"testing"
)

// clearKafkaEnvForTest neutralizes the Kafka variables the shared security
// config reads, so a developer machine that exports one cannot change what
// these tests assert.
func clearKafkaEnvForTest(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"KAFKA_BROKERS", "KAFKA_BOOTSTRAP_SERVERS", "KAFKA_CLIENT_ID",
		"KAFKA_SECURITY_PROTOCOL", "KAFKA_SASL_MECHANISM",
		"KAFKA_SASL_USERNAME", "KAFKA_SASL_PASSWORD",
		"KAFKA_SSL_CA_LOCATION", "KAFKA_SSL_CERT_LOCATION", "KAFKA_SSL_KEY_LOCATION",
	} {
		t.Setenv(k, "")
	}
}

// The domain event manager must read the deployment's Kafka security settings
// like every other client in this service.
//
// Pre-fix it built a bare sarama.NewConfig() and ignored them entirely, so on a
// customer's SASL cluster it connected anonymously: the process started clean,
// every other subsystem authenticated, and only the NL-pipeline HITL
// checkpoints went dark — which presents as "the pipeline hangs forever".
//
// The probe is a SASL configuration the consumer cannot possibly use (a
// mechanism with no credentials). Wired, it is rejected by name before any
// connection is attempted; unwired, the environment is invisible and the only
// error that can come back is a transport failure from dialing the broker.
func TestInitDomainEventManagerAppliesKafkaSecurity(t *testing.T) {
	clearKafkaEnvForTest(t)
	t.Setenv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
	t.Setenv("KAFKA_SASL_MECHANISM", "PLAIN")

	// 127.0.0.1:1 refuses immediately, so an unwired consumer fails fast with a
	// dial error rather than hanging on a metadata retry loop.
	err := InitDomainEventManager(context.Background(), []string{"127.0.0.1:1"})
	if err == nil {
		t.Fatal("InitDomainEventManager started with a SASL mechanism and no credentials")
	}
	if !strings.Contains(err.Error(), "KAFKA_SASL_USERNAME") {
		t.Fatalf("error %q does not name the missing SASL credential: the consumer is not reading the "+
			"deployment's Kafka security config and would connect unauthenticated", err)
	}
	if eventManager != nil {
		t.Error("eventManager was installed despite the error")
	}
}
