package sentinel

import (
	"testing"

	"github.com/rsync-ai/backend-orchestrator/internal/kafka"
	kafkaclient "github.com/rsync-ai/shared/kafkaclient"
)

// A6. Both healer Kafka clients connected with the client library's anonymous
// default. On a customer-managed cluster that is the identity in the broker's
// request logs, its quota buckets and its authorization denials, so an rsync
// connection was indistinguishable from any other tenant's.

func TestHealerKafkaSecurityCarriesTheServiceClientID(t *testing.T) {
	// A zero Manager is enough: the point under test is what kafkaSecurity adds
	// to whatever the manager resolved, not what the manager resolves.
	h := &Healer{kafkaManager: &kafka.Manager{}}

	got := h.kafkaSecurity().ClientID
	if want := kafkaclient.DefaultClientID(kafkaServiceName); got != want {
		t.Fatalf("ClientID = %q, want %q", got, want)
	}
	if got == "" || got == kafkaclient.ClientIDNamespace {
		t.Fatalf("ClientID = %q, which does not name the process", got)
	}
}

func TestHealerKafkaSecurityKeepsTheManagersSettings(t *testing.T) {
	// The client.id stamp must not be a rebuild — the manager's brokers and
	// SASL/TLS settings are the ones a customer-managed cluster needs, and
	// re-deriving them here is the drift this method exists to prevent.
	m := &kafka.Manager{}
	base := m.SecurityConfig()

	got := (&Healer{kafkaManager: m}).kafkaSecurity()
	if len(got.Brokers) != len(base.Brokers) {
		t.Fatalf("Brokers = %v, want the manager's %v", got.Brokers, base.Brokers)
	}
	if got.SecurityProtocol != base.SecurityProtocol {
		t.Fatalf("SecurityProtocol = %q, want the manager's %q", got.SecurityProtocol, base.SecurityProtocol)
	}
	if got.SASLMechanism != base.SASLMechanism {
		t.Fatalf("SASLMechanism = %q, want the manager's %q", got.SASLMechanism, base.SASLMechanism)
	}
}

func TestExplicitKafkaClientIDStillWins(t *testing.T) {
	// The service name is a default, not a policy: an operator who tags their
	// connections for a cluster-side quota or ACL keeps that tag. The manager
	// builds its Config through FromEnv, which is what records the override.
	t.Setenv(kafkaclient.EnvClientID, "acme-audit-tag")

	base, err := kafkaclient.FromEnv("localhost:9092")
	if err != nil {
		t.Fatalf("FromEnv: %v", err)
	}
	if got := base.WithServiceName(kafkaServiceName).ClientID; got != "acme-audit-tag" {
		t.Fatalf("ClientID = %q, want the operator's %q", got, "acme-audit-tag")
	}
}
