package kafka

import (
	"fmt"

	kafkaclient "github.com/rsync-ai/shared/kafkaclient"
)

// ServiceName is what this process calls itself on the wire.
//
// It becomes the Kafka client.id (kafkaclient.DefaultClientID -> "rsync-orchestrator")
// for every connection this package opens. client.id was set nowhere in this platform,
// which on a managed cluster has a specific cost: broker-side quotas and throttle
// metrics key off it, so every connection from every rsync service was
// indistinguishable both from each other and from any other tenant's default client.
// When the cluster throttled us, neither side could tell which service caused it.
const ServiceName = "orchestrator"

// serviceSecurityConfig resolves the Kafka connection settings for this service:
// SASL/TLS from the environment, brokers from the caller, client.id naming the
// orchestrator.
//
// Both the Manager and the TopologyManager build their sarama clients from this, so
// the two cannot drift into presenting different identities to the same cluster.
// brokers stays authoritative for WHERE we connect — the environment supplies only
// how the connection is secured and what it calls itself.
func serviceSecurityConfig(brokers string) (kafkaclient.Config, error) {
	c, err := kafkaclient.FromEnvForService(ServiceName, brokers)
	if err != nil {
		return kafkaclient.Config{}, fmt.Errorf("invalid Kafka security configuration: %w", err)
	}
	return c.WithBrokers(brokers), nil
}
