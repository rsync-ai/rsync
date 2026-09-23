package kafka

import (
	"os"
	"strconv"
	"sync"
)

// EnvCDCTopicPartitions is the partition count a CDC data topic is born with.
//
// It is the partition twin of EnvReplicationFactor, and it exists for the same
// reason: the derived value is right for the bundled single-broker cluster and
// wrong for somebody else's. An operator with 6 brokers who wants their CDC topics
// spread over 6 partitions had no way to ask, and the derivation below deliberately
// stops at 3.
//
// Raising this on a pipeline that is already streaming does NOT repartition its
// topics — ensureAuthoritativeTopic sets KeepExistingPartitions, and Kafka cannot
// reduce a partition count at all. It applies to topics created after the change.
const EnvCDCTopicPartitions = "KAFKA_CDC_TOPIC_PARTITIONS"

// maxDerivedPartitions caps the derived partition count.
//
// 3 rather than "one per broker" because partitions bought past the point where
// every broker leads one stop buying spread and start costing per-partition
// overhead on the broker, in the sink's fetch loop, and in the object-storage
// layout (one open file per partition per table). An operator who wants more says
// so through EnvCDCTopicPartitions.
const maxDerivedPartitions int32 = 3

var (
	partitionPolicyOnce sync.Once
	partitionPolicyRead int32
)

// partitionDefaults reads the operator's requested partition count once per process,
// for the same reasons replicationDefaults does: it cannot change under a running
// orchestrator, and a malformed value should warn once rather than on every CDC start.
func partitionDefaults() int32 {
	partitionPolicyOnce.Do(func() {
		partitionPolicyRead = readPartitionPolicyFromEnv()
	})
	return partitionPolicyRead
}

// readPartitionPolicyFromEnv is the env-reading half, kept out of the sync.Once so a
// test can prove the variable NAME is wired without racing a process-wide latch.
func readPartitionPolicyFromEnv() int32 {
	return int32(parsePositiveEnv(EnvCDCTopicPartitions, os.Getenv(EnvCDCTopicPartitions)))
}

// partitionsForCluster is the partition count for a CDC data topic on this cluster.
//
// One partition means one broker leads the topic and takes 100% of its produce and
// fetch traffic, however many brokers the customer added. That is the load-balancing
// defect this fixes: a 3-node cluster was doing single-node work.
//
// The shape mirrors replicationPolicy.forCluster on purpose — 1 on a single broker,
// min(3, brokers) beyond that — because the two numbers answer the same question
// ("how much of this cluster can this topic actually use?") and a reader who has
// learned one should not have to learn the other.
func partitionsForCluster(brokerCount int) int32 {
	return partitionsFor(partitionDefaults(), brokerCount)
}

// partitionsFor is partitionsForCluster against a requested count a caller chose,
// rather than the process-wide one. Split out for the same reason applyTo is: the
// derivation can then be tested without racing a sync.Once that whichever test ran
// first has already tripped.
func partitionsFor(requested int32, brokerCount int) int32 {
	if requested > 0 {
		return requested
	}
	if brokerCount > 1 {
		return int32(min(int(maxDerivedPartitions), brokerCount))
	}
	return 1
}

// CDCTopicShape is everything a CDC data topic's creator needs to agree on.
//
// The three fields travel together because they are only meaningful together: a
// replication factor without its min.insync.replicas floor produces a topic that is
// created, listed, subscribable, and rejects every acks=all produce (see
// pinMinInsyncReplicas). rsync is not the only creator of these topics — Kafka
// Connect creates them too, from topic.creation.* on the Debezium connector — so the
// numbers have to be stated somewhere both creators can read, rather than each
// deriving its own.
type CDCTopicShape struct {
	Partitions        int32
	ReplicationFactor int16
	MinInsyncReplicas int
}

// CDCTopicShapeForCluster reports the shape CDC data topics should have on the live
// cluster. ok is false when the broker count cannot be read.
//
// ok exists so that callers do not have to guess. Guessing is worse than not acting:
// the fallback for an unknown cluster is RF=1, and on a managed broker whose default
// min.insync.replicas is 2 (MSK's, and most others') an RF=1 topic is born
// permanently unwritable. A caller that cannot see the cluster should leave topic
// creation exactly as it was rather than publish a confident wrong number.
func (m *Manager) CDCTopicShapeForCluster() (CDCTopicShape, bool) {
	if !m.connected || m.client == nil {
		return CDCTopicShape{}, false
	}
	tm, err := m.topologyFor()
	if err != nil {
		return CDCTopicShape{}, false
	}
	brokers := tm.brokerCount()
	if brokers <= 0 {
		return CDCTopicShape{}, false
	}
	return cdcTopicShapeFor(brokers), true
}

// cdcTopicShapeFor derives the shape by running the REAL policy over a throwaway
// TopicConfig rather than restating the rules.
//
// Restating them is how the two numbers drift: applyReplicationPolicy defaults the
// RF, clamps it to the cluster and only then pins the floor, and that order is
// load-bearing. A second implementation here that pinned min.insync.replicas against
// an unclamped RF would be wrong in exactly the case the clamp exists for.
func cdcTopicShapeFor(brokerCount int) CDCTopicShape {
	return cdcTopicShapeUsing(partitionDefaults(), replicationDefaults(), brokerCount)
}

// cdcTopicShapeUsing is cdcTopicShapeFor against policies a caller chose, so the
// whole shape can be tested without the two process-wide sync.Once latches.
func cdcTopicShapeUsing(requestedPartitions int32, p replicationPolicy, brokerCount int) CDCTopicShape {
	cfg := TopicConfig{
		// Named only so a policy warning has something to print; this config is never
		// sent to a broker.
		Name:       "cdc-data-topic",
		Partitions: partitionsFor(requestedPartitions, brokerCount),
	}
	p.applyTo(&cfg, brokerCount, p.forCluster(brokerCount))
	misr, err := strconv.Atoi(cfg.Config[minInsyncReplicasKey])
	if err != nil {
		misr = 0
	}
	return CDCTopicShape{
		Partitions:        cfg.Partitions,
		ReplicationFactor: cfg.ReplicationFactor,
		MinInsyncReplicas: misr,
	}
}
