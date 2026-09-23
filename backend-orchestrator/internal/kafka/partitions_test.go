package kafka

import (
	"os"
	"testing"
)

// Before this, cdcDataTopicPartitions was the literal 1 and nothing read a partition
// count from anywhere: a customer who attached a three-broker cluster still got one
// partition per CDC topic, one leader, and one broker carrying all of its traffic.

func TestPartitionPolicyReadsTheEnvironment(t *testing.T) {
	t.Setenv(EnvCDCTopicPartitions, "6")

	// Deliberately not partitionDefaults(): that latches once per process, so a test
	// calling it would prove nothing about which variable was read.
	if got := readPartitionPolicyFromEnv(); got != 6 {
		t.Errorf("%s=6 → %d, want 6", EnvCDCTopicPartitions, got)
	}
}

func TestPartitionPolicyEnvNameIsTheDocumentedOne(t *testing.T) {
	// The name is the whole contract with the compose files and the Helm chart. A
	// rename here is a silent no-op there.
	if EnvCDCTopicPartitions != "KAFKA_CDC_TOPIC_PARTITIONS" {
		t.Errorf("EnvCDCTopicPartitions = %q", EnvCDCTopicPartitions)
	}
	if _, set := os.LookupEnv(EnvCDCTopicPartitions); set {
		t.Skip("environment already sets " + EnvCDCTopicPartitions)
	}
}

// A typo in a tuning knob must degrade to the derivation, not to zero partitions —
// Kafka rejects partitions=0 outright, so a leaked 0 fails every topic creation.
func TestPartitionPolicyIgnoresMalformedValues(t *testing.T) {
	for _, raw := range []string{"", "   ", "three", "0", "-1", "2.5"} {
		t.Setenv(EnvCDCTopicPartitions, raw)
		if got := readPartitionPolicyFromEnv(); got != 0 {
			t.Errorf("%s=%q → %d, want 0 (fall back to the derivation)", EnvCDCTopicPartitions, raw, got)
		}
		if got := partitionsFor(readPartitionPolicyFromEnv(), 3); got != 3 {
			t.Errorf("%s=%q → partitionsFor(...,3) = %d, want the derived 3", EnvCDCTopicPartitions, raw, got)
		}
	}
}

// The load-balancing fix itself. min(brokers, 3), mirroring forCluster, so a reader
// who has learned the replication rule already knows this one.
func TestPartitionsForDerivesFromTheBrokerCount(t *testing.T) {
	for _, tc := range []struct {
		brokers int
		want    int32
		why     string
	}{
		{0, 1, "an unknown cluster is treated as the bundled single broker"},
		{1, 1, "one broker cannot spread anything; extra partitions are pure overhead"},
		{2, 2, "both brokers lead a partition"},
		{3, 3, "the case this fix exists for: a 3-node cluster stops doing 1-node work"},
		{4, 3, "capped — partitions past one-per-broker buy overhead, not spread"},
		{12, 3, "still capped; an operator who wants more says so through the env var"},
	} {
		if got := partitionsFor(0, tc.brokers); got != tc.want {
			t.Errorf("partitionsFor(0, %d) = %d, want %d (%s)", tc.brokers, got, tc.want, tc.why)
		}
	}
}

// The cap is a default, not a ceiling: an operator with 6 brokers who asked for 6
// partitions gets 6, on any cluster size.
func TestAnExplicitPartitionCountWinsOverTheDerivation(t *testing.T) {
	for _, brokers := range []int{1, 3, 6} {
		if got := partitionsFor(6, brokers); got != 6 {
			t.Errorf("partitionsFor(6, %d) = %d, want the requested 6", brokers, got)
		}
	}
}

// The three fields are only meaningful together. This pins that the shape is derived
// by running the real replication policy rather than by restating its rules here —
// if applyTo's clamp/floor ordering changes, this test moves with it instead of
// quietly disagreeing with the topics the rest of the package creates.
func TestCDCTopicShapeAgreesWithTheReplicationPolicy(t *testing.T) {
	unset := replicationPolicy{}
	for _, brokers := range []int{1, 2, 3, 6} {
		got := cdcTopicShapeUsing(0, unset, brokers)

		want := TopicConfig{Name: "x", Partitions: 1}
		unset.applyTo(&want, brokers, unset.forCluster(brokers))
		if got.ReplicationFactor != want.ReplicationFactor {
			t.Errorf("%d brokers → RF %d, but the replication policy says %d",
				brokers, got.ReplicationFactor, want.ReplicationFactor)
		}
	}
}

// A topic whose min.insync.replicas exceeds its replication factor is created,
// listed, subscribable — and rejects every acks=all produce. It is born unwritable
// and reports no error until the first record.
func TestCDCTopicShapeIsNeverBornUnwritable(t *testing.T) {
	for _, p := range []replicationPolicy{{}, {replicationFactor: 3, minInsyncReplicas: 2}, {minInsyncReplicas: 3}} {
		for _, brokers := range []int{1, 2, 3, 6} {
			got := cdcTopicShapeUsing(0, p, brokers)
			if got.MinInsyncReplicas > int(got.ReplicationFactor) {
				t.Errorf("policy %+v on %d brokers → min.insync.replicas %d > RF %d: every acks=all produce fails",
					p, brokers, got.MinInsyncReplicas, got.ReplicationFactor)
			}
			if got.Partitions < 1 {
				t.Errorf("policy %+v on %d brokers → %d partitions; Kafka rejects that outright",
					p, brokers, got.Partitions)
			}
		}
	}
}

// The default three-broker shape, spelled out, because it is the number a customer
// will read off their cluster and the one this change is judged by.
func TestTheDefaultThreeBrokerShape(t *testing.T) {
	got := cdcTopicShapeUsing(0, replicationPolicy{}, 3)
	if got.Partitions != 3 || got.ReplicationFactor != 3 || got.MinInsyncReplicas != 2 {
		t.Errorf("3 brokers → %+v, want {Partitions:3 ReplicationFactor:3 MinInsyncReplicas:2}", got)
	}
}

// ok=false is the whole safety margin: the fallback for an unknown cluster is RF=1,
// and on a managed broker whose default min.insync.replicas is 2 an RF=1 topic is
// permanently unwritable. A caller that cannot see the cluster must leave topic
// creation exactly as it was rather than publish a confident wrong number.
func TestCDCTopicShapeForClusterRefusesToGuess(t *testing.T) {
	for name, m := range map[string]*Manager{
		"never connected":      {},
		"connected, no client": {connected: true},
	} {
		if shape, ok := m.CDCTopicShapeForCluster(); ok {
			t.Errorf("%s → ok=true with %+v, want ok=false so the caller leaves creation alone", name, shape)
		}
	}
}
