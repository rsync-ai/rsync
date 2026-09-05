package kafka

import (
	"fmt"
	"log"
	"strings"

	"github.com/IBM/sarama"
	kafkago "github.com/segmentio/kafka-go"

	kafkaclient "github.com/rsync-ai/shared/kafkaclient"
	"github.com/rsync-ai/shared/kafkaclient/kgoauth"
	"github.com/rsync-ai/shared/kafkaclient/saramaauth"
)

// This file is the one place api-gateway decides how it reaches Kafka.
//
// Every kafka-go Reader and Writer in this service used to be constructed with
// nothing but a broker list, so there was no way to express the SASL or TLS a
// customer-managed cluster requires. The constructors involved cannot return an
// error (they build Readers and Writers inline), which leaves two options for a
// bad security config: carry on unauthenticated, or refuse to start.
//
// These helpers refuse to start. A misconfigured broker credential is an
// operator error caught at boot in one line; the alternative is a service that
// looks healthy while silently connecting with less protection than the
// operator asked for — and against a cluster that requires SASL it would fail
// later anyway, as an unexplained connection error deep in a consumer loop.

// ServiceName is what this process calls itself on a broker. It becomes the
// client.id of every Kafka client here, so a customer reading a request log,
// a throttle metric or a quota rule on a shared cluster sees which rsync
// service the load came from instead of the client library's anonymous default
// — which is indistinguishable both between our services and from any other
// tenant's default client.
const ServiceName = "api-gateway"

// securityConfig is Security without the process-level exit, for a caller that
// can report the error itself.
func securityConfig(brokers []string) (kafkaclient.Config, error) {
	// The caller's list doubles as the default so that an unset KAFKA_BROKERS
	// is not an error in itself: brokers is the address this service already
	// resolved, and WithBrokerList below is what actually decides it.
	c, err := kafkaclient.FromEnvForService(ServiceName, strings.Join(brokers, ","))
	if err != nil {
		return kafkaclient.Config{}, err
	}
	c = c.WithBrokerList(brokers)
	if err := c.Validate(); err != nil {
		return kafkaclient.Config{}, err
	}
	for _, w := range c.Warnings() {
		log.Printf("⚠️  Kafka config: %s", w)
	}
	return c, nil
}

// Security resolves the SASL/TLS settings for this process, keeping brokers as
// the authoritative address the caller already resolved.
func Security(brokers []string) kafkaclient.Config {
	c, err := securityConfig(brokers)
	if err != nil {
		log.Fatalf("❌ invalid Kafka security configuration: %v", err)
	}
	return c
}

// ApplySarama stamps the same settings onto a *sarama.Config the caller has
// already filled in with its own consumer options.
//
// The two sarama consumer groups in this service — the domain-event manager and
// the notifier — built a bare sarama.NewConfig() and so were the only Kafka
// clients here that could not speak SASL or TLS. On a customer's authenticated
// cluster that does not fail loudly: the producer, the agent-response consumer,
// the WebSocket bridge and the event projector all authenticate and the service
// reports healthy, while HITL checkpoints never reach the browser and no alert
// is ever delivered — the failure disables the alerting that would report it.
//
// Unlike Dialer/Transport this returns the error instead of exiting, because
// both callers can return one and the decision of what an unreachable Kafka
// means for the process belongs to them.
func ApplySarama(cfg *sarama.Config, brokers []string) error {
	c, err := securityConfig(brokers)
	if err != nil {
		return fmt.Errorf("kafka: %w", err)
	}
	return saramaauth.Apply(cfg, c)
}

// Dialer is the *kafka.Dialer for Readers. Nil TLS and nil mechanism (the
// PLAINTEXT case) reproduce kafka-go's own defaults, so adopting this changes
// nothing for a deployment that has not configured security.
func Dialer(brokers []string) *kafkago.Dialer {
	d, err := kgoauth.Dialer(Security(brokers))
	if err != nil {
		log.Fatalf("❌ failed to build Kafka dialer: %v", err)
	}
	return d
}

// Transport is the *kafka.Transport for Writers and Clients.
func Transport(brokers []string) *kafkago.Transport {
	t, err := kgoauth.Transport(Security(brokers))
	if err != nil {
		log.Fatalf("❌ failed to build Kafka transport: %v", err)
	}
	return t
}
