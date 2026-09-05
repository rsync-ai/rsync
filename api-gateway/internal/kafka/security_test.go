package kafka

import (
	"strings"
	"testing"

	"github.com/IBM/sarama"
)

// clearKafkaEnv neutralizes every Kafka variable the shared config reads, so a
// developer machine that happens to export one cannot change what these tests
// assert. t.Setenv restores the previous value when the test ends.
func clearKafkaEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"KAFKA_BROKERS", "KAFKA_BOOTSTRAP_SERVERS", "KAFKA_CLIENT_ID",
		"KAFKA_SECURITY_PROTOCOL", "KAFKA_SASL_MECHANISM",
		"KAFKA_SASL_USERNAME", "KAFKA_SASL_PASSWORD",
		"KAFKA_SSL_CA_LOCATION", "KAFKA_TLS_CA",
		"KAFKA_SSL_CERT_LOCATION", "KAFKA_SSL_KEY_LOCATION",
		"KAFKA_SSL_INSECURE_SKIP_VERIFY", "KAFKA_SSL_SKIP_VERIFY",
	} {
		t.Setenv(k, "")
	}
}

// The client.id is what a customer's broker attributes load, throttling and
// quotas to. Unset, every connection from every rsync service is
// indistinguishable from every other tenant's default client.
func TestSecurityStampsServiceClientID(t *testing.T) {
	clearKafkaEnv(t)

	got := Security([]string{"kafka:29092"}).ClientID
	if got != "rsync-api-gateway" {
		t.Fatalf("client.id = %q, want %q", got, "rsync-api-gateway")
	}
}

// An operator who wants one identity for the whole platform sets the variable
// once, and no service may override it from code.
func TestSecurityClientIDEnvOverridesService(t *testing.T) {
	clearKafkaEnv(t)
	t.Setenv("KAFKA_CLIENT_ID", "acme-data-platform")

	got := Security([]string{"kafka:29092"}).ClientID
	if got != "acme-data-platform" {
		t.Fatalf("client.id = %q, want the KAFKA_CLIENT_ID value", got)
	}
}

// A bootstrap list written with spaces after the commas must not reach a
// broker as a space-padded address.
func TestSecurityTrimsBrokerList(t *testing.T) {
	clearKafkaEnv(t)

	brokers := Security([]string{" b1:9092", "b2:9092 ", " "}).Brokers
	want := []string{"b1:9092", "b2:9092"}
	if len(brokers) != len(want) {
		t.Fatalf("brokers = %q, want %q", brokers, want)
	}
	for i := range want {
		if brokers[i] != want[i] {
			t.Fatalf("brokers = %q, want %q", brokers, want)
		}
	}
}

// The sarama consumer groups get the same authentication as every other Kafka
// client here. Pre-fix they built a bare sarama.NewConfig() and were the only
// subsystems that stayed PLAINTEXT on an authenticated cluster.
func TestApplySaramaWiresSASLAndClientID(t *testing.T) {
	clearKafkaEnv(t)
	t.Setenv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
	t.Setenv("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512")
	t.Setenv("KAFKA_SASL_USERNAME", "rsync")
	t.Setenv("KAFKA_SASL_PASSWORD", "s3cret")

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V3_4_0_0
	if err := ApplySarama(cfg, []string{"b1:9092"}); err != nil {
		t.Fatalf("ApplySarama: %v", err)
	}

	if !cfg.Net.SASL.Enable {
		t.Error("Net.SASL.Enable = false, want true")
	}
	if cfg.Net.SASL.Mechanism != sarama.SASLTypeSCRAMSHA512 {
		t.Errorf("Net.SASL.Mechanism = %q, want %q", cfg.Net.SASL.Mechanism, sarama.SASLTypeSCRAMSHA512)
	}
	if cfg.Net.SASL.User != "rsync" || cfg.Net.SASL.Password != "s3cret" {
		t.Errorf("SASL credentials not applied: user=%q", cfg.Net.SASL.User)
	}
	if cfg.Net.SASL.SCRAMClientGeneratorFunc == nil {
		t.Error("SCRAMClientGeneratorFunc is nil, so SCRAM cannot complete a handshake")
	}
	if cfg.ClientID != "rsync-api-gateway" {
		t.Errorf("ClientID = %q, want %q", cfg.ClientID, "rsync-api-gateway")
	}
	// The caller's own settings survive.
	if cfg.Version != sarama.V3_4_0_0 {
		t.Errorf("Version = %v, want the caller's V3_4_0_0", cfg.Version)
	}
}

func TestApplySaramaEnablesTLS(t *testing.T) {
	clearKafkaEnv(t)
	t.Setenv("KAFKA_SECURITY_PROTOCOL", "SSL")

	cfg := sarama.NewConfig()
	if err := ApplySarama(cfg, []string{"b1:9093"}); err != nil {
		t.Fatalf("ApplySarama: %v", err)
	}
	if !cfg.Net.TLS.Enable {
		t.Error("Net.TLS.Enable = false, want true for KAFKA_SECURITY_PROTOCOL=SSL")
	}
}

// PLAINTEXT deployments must be untouched: adopting the shared path cannot
// change how an existing deployment connects.
func TestApplySaramaPlaintextLeavesSecurityOff(t *testing.T) {
	clearKafkaEnv(t)

	cfg := sarama.NewConfig()
	if err := ApplySarama(cfg, []string{"kafka:29092"}); err != nil {
		t.Fatalf("ApplySarama: %v", err)
	}
	if cfg.Net.SASL.Enable || cfg.Net.TLS.Enable {
		t.Errorf("SASL=%t TLS=%t, want both off with no security configured", cfg.Net.SASL.Enable, cfg.Net.TLS.Enable)
	}
}

// A half-configured credential is reported, not silently downgraded to an
// anonymous connection.
func TestApplySaramaRejectsIncompleteSASL(t *testing.T) {
	clearKafkaEnv(t)
	t.Setenv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
	t.Setenv("KAFKA_SASL_MECHANISM", "PLAIN")

	err := ApplySarama(sarama.NewConfig(), []string{"b1:9092"})
	if err == nil {
		t.Fatal("ApplySarama accepted SASL_PLAINTEXT with no username or password")
	}
	if !strings.Contains(err.Error(), "KAFKA_SASL_USERNAME") {
		t.Errorf("error %q does not name the missing variable", err)
	}
}

// An unset KAFKA_BROKERS must not be fatal when the caller passed an explicit
// address: brokers is what the process already resolved.
func TestSecurityUsesCallerBrokersWhenEnvUnset(t *testing.T) {
	clearKafkaEnv(t)

	c := Security([]string{"kafka:29092"})
	if len(c.Brokers) != 1 || c.Brokers[0] != "kafka:29092" {
		t.Fatalf("brokers = %q, want the caller's list", c.Brokers)
	}
}

// Every kafka-go Reader and Writer in this service is built from these two, so
// this is where the client.id reaches them.
func TestDialerAndTransportCarryClientID(t *testing.T) {
	clearKafkaEnv(t)

	if got := Dialer([]string{"kafka:29092"}).ClientID; got != "rsync-api-gateway" {
		t.Errorf("Dialer ClientID = %q, want %q", got, "rsync-api-gateway")
	}
	if got := Transport([]string{"kafka:29092"}).ClientID; got != "rsync-api-gateway" {
		t.Errorf("Transport ClientID = %q, want %q", got, "rsync-api-gateway")
	}
}

// The producer configs resolve their own broker list from the environment, so
// they need the same trimming as the list main() hands down.
func TestDefaultProducerConfigsTrimBrokerList(t *testing.T) {
	clearKafkaEnv(t)
	t.Setenv("KAFKA_BROKERS", "b1:9092, b2:9092,")

	want := []string{"b1:9092", "b2:9092"}
	for _, tc := range []struct {
		name string
		got  []string
	}{
		{"unified", DefaultUnifiedConfig().Brokers},
		{"avro", DefaultAvroConfig().Brokers},
	} {
		if len(tc.got) != len(want) {
			t.Fatalf("%s brokers = %q, want %q", tc.name, tc.got, want)
		}
		for i := range want {
			if tc.got[i] != want[i] {
				t.Fatalf("%s brokers = %q, want %q", tc.name, tc.got, want)
			}
		}
	}
}
