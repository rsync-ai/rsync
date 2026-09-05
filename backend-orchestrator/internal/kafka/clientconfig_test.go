package kafka

import (
	"testing"

	kafkaclient "github.com/rsync-ai/shared/kafkaclient"
)

// client.id was set nowhere in this platform. On a managed cluster (MSK, Confluent
// Cloud) broker-side quotas and throttle metrics key off it, so every connection from
// every rsync service was indistinguishable both from each other and from any other
// tenant's default client: when the cluster throttled us, neither side could tell which
// service caused it, and the customer could not scope a quota to this product.
//
// A unit test on kafkaclient.DefaultClientID proves the naming rule; it cannot prove
// this package applies it. These drive the helper both clients are built from.

func TestServiceSecurityConfigNamesTheOrchestrator(t *testing.T) {
	c, err := serviceSecurityConfig("kafka:29092")
	if err != nil {
		t.Fatalf("serviceSecurityConfig: %v", err)
	}
	want := kafkaclient.DefaultClientID(ServiceName)
	if c.ClientID != want {
		t.Errorf("ClientID = %q, want %q", c.ClientID, want)
	}
	if c.ClientID == kafkaclient.DefaultClientID("") {
		t.Errorf("ClientID = %q — the bare namespace means the service name was never "+
			"passed, which is indistinguishable from every other rsync service on the broker", c.ClientID)
	}
}

// An explicit KAFKA_CLIENT_ID is a deployment-wide decision and must beat the
// service-derived default, so an operator can give the whole platform one identity.
func TestServiceSecurityConfigYieldsToAnExplicitClientID(t *testing.T) {
	t.Setenv("KAFKA_CLIENT_ID", "acme-data-platform")
	c, err := serviceSecurityConfig("kafka:29092")
	if err != nil {
		t.Fatalf("serviceSecurityConfig: %v", err)
	}
	if c.ClientID != "acme-data-platform" {
		t.Errorf("ClientID = %q, want the operator's %q", c.ClientID, "acme-data-platform")
	}
}

// Adopting the helper must change only HOW the connection is secured and what it calls
// itself — never WHERE it connects. The caller's broker string stays authoritative
// over KAFKA_BROKERS, which is the precedence both constructors already had.
func TestServiceSecurityConfigKeepsTheCallersBrokers(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "should-not-win:9092")
	c, err := serviceSecurityConfig("b1:9092, b2:9092")
	if err != nil {
		t.Fatalf("serviceSecurityConfig: %v", err)
	}
	if len(c.Brokers) != 2 || c.Brokers[0] != "b1:9092" || c.Brokers[1] != "b2:9092" {
		t.Errorf("Brokers = %v, want [b1:9092 b2:9092] — a multi-broker bootstrap list "+
			"that collapses to one entry takes the whole data path with it", c.Brokers)
	}
}

func TestServiceSecurityConfigRejectsABadEnvironment(t *testing.T) {
	// The foundation validates at construction rather than at the first handshake, and
	// adopting it only buys anything if this constructor surfaces that instead of
	// swallowing it into a zero-value Config that then dials in the clear.
	//
	// SASL_PLAINTEXT + AWS_MSK_IAM is the case worth pinning: the IAM token is a
	// presigned bearer credential, so carrying it unencrypted hands a working identity
	// to anyone on the path — and the connection would otherwise come up.
	//
	// Note it is SASL_PLAINTEXT and not PLAINTEXT here: Config.Validate only enters the
	// mechanism switch when the protocol is a SASL one (config.go:350 UsesSASL), so a
	// mechanism set under bare PLAINTEXT is ignored rather than rejected.
	t.Setenv("KAFKA_SASL_MECHANISM", "AWS_MSK_IAM")
	t.Setenv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
	if _, err := serviceSecurityConfig("kafka:29092"); err == nil {
		t.Fatal("an invalid security configuration must fail the constructor")
	}
}
