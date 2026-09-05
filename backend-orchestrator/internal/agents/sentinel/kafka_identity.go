package sentinel

import (
	kafkaclient "github.com/rsync-ai/shared/kafkaclient"
)

// kafkaServiceName names the PROCESS the sentinel's Kafka clients run inside, not
// the sentinel, so the customer's broker logs and quota metrics attribute the
// connection to "rsync-orchestrator" instead of an anonymous library default.
//
// It is threaded through Config.WithServiceName rather than FromEnvForService
// because these call sites do not build their Config from the environment: they
// take the one the kafka.Manager already resolved, which is what keeps a
// customer-managed cluster's brokers and SASL/TLS settings in one place. An
// explicit KAFKA_CLIENT_ID still wins over it.
const kafkaServiceName = "orchestrator"

// kafkaSecurity is how every Kafka client the healer opens gets its settings:
// the Config the kafka.Manager already resolved, with this process's client.id
// stamped on. Going through one method is what keeps the two call sites from
// drifting apart the way the group ids did.
func (h *Healer) kafkaSecurity() kafkaclient.Config {
	return h.kafkaManager.SecurityConfig().WithServiceName(kafkaServiceName)
}
