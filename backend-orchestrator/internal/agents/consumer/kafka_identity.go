package consumer

import (
	"strings"

	kafkaclient "github.com/rsync-ai/shared/kafkaclient"
)

// consumerGroupID is the one gate every consumer group id in this package passes
// through before it reaches a broker.
//
// This agent does not open Kafka consumer groups itself — it spawns containers
// and hands them CONSUMER_GROUP_ID (spawner.go). That indirection is precisely
// why the gate has to be here and not at the call sites: the group id is minted
// in this process, joined in another, and the failure it causes is silent in
// both. A group id outside the deployment's namespace is denied at JoinGroup on
// a cluster with PREFIXED group ACLs; the spawned consumer then sits with no
// partition assignment, reporting itself alive to the health monitor, while the
// registry's lag readings describe a group that is not consuming. Nothing
// crashes and nothing logs an error the product surfaces.
//
// DECISION — an operator-supplied group id is qualified too.
//
// Two of the ids this package builds are derived from CONSUMER_GROUP_PREFIX
// (config.go), and a third arrives verbatim from the HTTP SpawnRequest body, so
// the operator-supplied case is not hypothetical here the way it still is in
// api-gateway. The rule is the same one settled in
// api-gateway/internal/kafka/consumer.go NewConsumer, and it is settled the same
// way: qualify it.
//
// The argument for is the whole reason Group() exists — a customer on a shared
// cluster wants ONE PREFIXED grant to cover every group this product joins, and
// the group that skips qualification because it came from configuration is
// exactly the one that falls outside that grant.
//
// The argument against — an operator who typed CONSUMER_GROUP_PREFIX=acme-ingest
// did not ask for rsync.acme-ingest — is real, and is answered by the lever
// topics already use: KAFKA_TOPIC_PREFIX="" disables qualification for topics
// and groups together, so an operator who wants their exact string has a
// supported way to get it and gets it consistently across both, rather than a
// group id that silently disagrees with the topics it reads. Qualification is
// idempotent, so an operator who spells the prefix into the variable themselves
// is not double-prefixed.
//
// Idempotence also covers the reuse path: Registry.RestartConsumer respawns from
// oldInfo.GroupID (registry.go), which was already qualified when the consumer
// was first spawned. Passing it through here a second time must — and does —
// leave it alone, so a restart rejoins the group it left rather than minting
// rsync.rsync.… and starting from auto.offset.reset.
func consumerGroupID(name string) string {
	return kafkaclient.Group(strings.TrimSpace(name))
}

// groupIDForTopic mints the group id this agent uses for the consumers it
// manages for a topic.
//
// Two call sites built this by hand — handlers.ManualScale and
// Registry.ApplyScaling — with two different spellings of the same
// concatenation. They are one function now so that a third cannot drift, and so
// the qualification above cannot be forgotten at one of them; a package test
// enforces that ConsumerGroupPrefix is referenced nowhere else.
func (c *Config) groupIDForTopic(topic string) string {
	return consumerGroupID(c.ConsumerGroupPrefix + "-" + topic)
}
