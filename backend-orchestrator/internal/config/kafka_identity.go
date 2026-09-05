package config

import (
	"strings"

	kafkaclient "github.com/rsync-ai/shared/kafkaclient"
)

// kafkaGroupID qualifies KAFKA_GROUP_ID with the deployment's Kafka namespace,
// once, at the point the value is read — so every consumer derived from it is
// namespaced without its own author having to remember.
//
// KAFKA_GROUP_ID is not a group id, it is a group id PREFIX: kafka/manager.go
// joins as "<KAFKA_GROUP_ID>-<topic>" for per-topic consumers and as
// "<KAFKA_GROUP_ID>" for the single-group one, and sentinel's lag monitor
// recomputes the same string to look the group up on the broker. All three read
// this one field, so qualifying it here keeps them in step by construction;
// qualifying at any one of them would have desynced the other two, and a lag
// query against a group name nobody joined returns "no lag" rather than an
// error.
//
// DECISION — yes, an operator-supplied group id gets the prefix.
//
// This is the platform's one variable that lets an operator name a consumer
// group directly, so the question that api-gateway/internal/kafka/consumer.go
// NewConsumer settled in the abstract is concrete here. It is settled the same
// way, and it is worth writing down why, because the reasonable-looking
// alternative is actively dangerous.
//
// FOR: the entire purpose of a shared KAFKA_TOPIC_PREFIX is that a customer
// running a managed cluster can write ONE PREFIXED ACL pair — topics and
// groups — and have it cover everything this product touches. Consumer groups
// that opt out of the namespace because their name came from configuration are
// precisely the ones that fall outside that grant. And Kafka does not fail
// loudly for them: the broker rejects the JoinGroup with an authorization
// error, the client retries, and the consumer simply never receives a record.
// The operator sees a running process, a healthy container, and no data. Half a
// grant is worse than none, because none fails during the smoke test.
//
// AGAINST: an operator who deliberately set KAFKA_GROUP_ID=acme-ingest may be
// surprised to see rsync.acme-ingest on the broker, and may have set it exactly
// because their cluster convention demands that literal string.
//
// The against is real, and it is answered rather than dismissed: the same lever
// that turns qualification off for topics turns it off for groups.
// KAFKA_TOPIC_PREFIX="" disables both together, which is the outcome that
// operator actually wants — their literal group id AND their literal topic
// names, consistently, instead of a group id that disagrees with the topics it
// reads. That lever is also the migration path for an existing deployment,
// which matters more here than it does for topics: renaming a consumer group
// abandons its committed offsets, and the new name starts from
// auto.offset.reset.
//
// Qualification is idempotent, so an operator who writes the prefix into the
// variable themselves ("rsync.acme-ingest") is not double-prefixed.
func kafkaGroupID(raw string) string {
	return kafkaclient.Group(strings.TrimSpace(raw))
}
