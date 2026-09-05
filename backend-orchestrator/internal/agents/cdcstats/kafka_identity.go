package cdcstats

import (
	"github.com/IBM/sarama"

	kafkaclient "github.com/rsync-ai/shared/kafkaclient"
)

// tableStatsGroupPrefix names the per-pipeline TABLE_STATS consumer group. It
// lives here rather than inline in ensureWorker for the same reason
// schemaChangeGroupPrefix lives in schema_changes.go: the id is not private to
// the line that builds it. cdc_kafka_teardown.go's ownsGroup reaps
// "cdc-table-stats-<uuid>" (and its qualified spelling) when a pipeline is
// deleted, so this string is half of a contract with another package.
const tableStatsGroupPrefix = "cdc-table-stats-"

// tableStatsGroupID and schemaChangeGroupID are the only two consumer group ids
// this package names. Both go through kafkaclient.Group so they land in the same
// KAFKA_TOPIC_PREFIX namespace as the Debezium topics they read.
//
// The point of routing them through Group() rather than concatenating the prefix
// here is what happens on a customer-managed cluster. Topics and consumer groups
// are granted together, in one ACL set; an operator who grants PREFIXED "rsync."
// on both expects that grant to cover everything this product joins. A group id
// that stayed bare would be denied at JoinGroup — and a denied join does not
// crash or log an error the product surfaces. The consumer sits there having
// never been assigned a partition, which is indistinguishable from a quiet
// pipeline. That failure mode is exactly the one this agent exists to detect,
// so it is the last place that should be vulnerable to it.
//
// Both take the full pipeline UUID, not SafeID8: the teardown matcher keys on
// the full uuid, and shortening it here would silently strand the group.
func tableStatsGroupID(pipelineID string) string {
	return kafkaclient.Group(tableStatsGroupPrefix + pipelineID)
}

func schemaChangeGroupID(pipelineID string) string {
	return kafkaclient.Group(schemaChangeGroupPrefix + pipelineID)
}

// consumerGroup is the single place this package opens a Kafka consumer group.
//
// Funnelling both call sites through one method is the same trick Topic() plays
// for topic names, applied at the smaller scale of one agent: a third consumer
// added to this file later is namespaced by construction rather than by its
// author remembering to be. kafka_identity_test.go enforces that by scanning
// this package's own sources for sarama.NewConsumerGroup and failing if it
// appears anywhere but here.
//
// The re-qualification of groupID is deliberate belt-and-braces, not a mistake:
// Group() is idempotent, so an id that already came from a minter above passes
// through untouched, while a bare literal handed straight to this method still
// lands inside the namespace.
func (a *Agent) consumerGroup(groupID string, cfg *sarama.Config) (sarama.ConsumerGroup, error) {
	return sarama.NewConsumerGroup(a.security.Brokers, kafkaclient.Group(groupID), cfg)
}
