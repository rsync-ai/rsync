package kafka

import (
	"context"
	"sync"
	"testing"

	"github.com/IBM/sarama"
)

// These exercise the real creation entry points end-to-end against fake broker
// deps, rather than calling normalizeTopicConfig directly. That distinction is
// the whole point: the clamp originally lived only in EnsureTopic, so the
// planner-facing CreateTopic path stayed broken while unit tests on the clamp
// itself were green. A test that never enters CreateTopic cannot catch that.

type fakeAdmin struct {
	sarama.ClusterAdmin // unimplemented methods panic loudly if the code path touches them
	existing            map[string]sarama.TopicDetail
	created             map[string]*sarama.TopicDetail
	// repartitioned records CreatePartitions calls. Growing an existing topic is a
	// silent data-ordering change on a keyed topic, so the tests have to be able to
	// assert it did NOT happen, not merely that creation looked right.
	repartitioned map[string]int32
	createErr     error
	listErr       error
}

func (f *fakeAdmin) ListTopics() (map[string]sarama.TopicDetail, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	return f.existing, nil
}

func (f *fakeAdmin) CreateTopic(name string, d *sarama.TopicDetail, _ bool) error {
	if f.createErr != nil {
		return f.createErr
	}
	f.created[name] = d
	return nil
}

func (f *fakeAdmin) CreatePartitions(name string, count int32, _ [][]int32, _ bool) error {
	f.repartitioned[name] = count
	return nil
}

type fakeClient struct {
	sarama.Client
	brokers []*sarama.Broker
}

func (f *fakeClient) Brokers() []*sarama.Broker { return f.brokers }

func newFakeManager(brokerCount int) (*TopologyManager, *fakeAdmin) {
	brokers := make([]*sarama.Broker, brokerCount)
	for i := range brokers {
		brokers[i] = &sarama.Broker{}
	}
	admin := &fakeAdmin{
		existing:      map[string]sarama.TopicDetail{},
		created:       map[string]*sarama.TopicDetail{},
		repartitioned: map[string]int32{},
	}
	return &TopologyManager{
		client:     &fakeClient{brokers: brokers},
		admin:      admin,
		topicCache: map[string]*TopicInfo{},
		mu:         sync.RWMutex{},
	}, admin
}

// plannerPayload is what strategies.py POSTs to /api/v1/topology/topics.
func plannerPayload() TopicConfig {
	return TopicConfig{
		Name:              "rsync.pipeline.abc",
		Partitions:        3,
		ReplicationFactor: 3,
		Config: map[string]string{
			"cleanup.policy":      "delete",
			"min.insync.replicas": "2",
		},
	}
}

func assertClamped(t *testing.T, d *sarama.TopicDetail) {
	t.Helper()
	if d == nil {
		t.Fatal("topic was never created")
	}
	if d.ReplicationFactor != 1 {
		t.Errorf("replication factor sent to Kafka = %d, want 1 (would be InvalidReplicationFactor)", d.ReplicationFactor)
	}
	got := d.ConfigEntries["min.insync.replicas"]
	if got == nil {
		t.Fatal("min.insync.replicas missing from ConfigEntries")
	}
	if *got != "1" {
		t.Errorf("min.insync.replicas sent to Kafka = %q, want \"1\" (topic would be unwritable)", *got)
	}
}

func TestCreateTopicClampsPlannerPayloadOnSingleBroker(t *testing.T) {
	// The regression: this is the path the planner actually reaches over HTTP.
	tm, admin := newFakeManager(1)
	if err := tm.CreateTopic(context.Background(), plannerPayload()); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	assertClamped(t, admin.created["rsync.pipeline.abc"])
}

func TestEnsureTopicClampsPlannerPayloadOnSingleBroker(t *testing.T) {
	tm, admin := newFakeManager(1)
	if err := tm.EnsureTopic(context.Background(), plannerPayload()); err != nil {
		t.Fatalf("EnsureTopic: %v", err)
	}
	assertClamped(t, admin.created["rsync.pipeline.abc"])
}

func TestCreateTopicHonoursIntentOnHealthyCluster(t *testing.T) {
	// Clamping must not cost durability on a cluster that can satisfy the ask.
	tm, admin := newFakeManager(3)
	if err := tm.CreateTopic(context.Background(), plannerPayload()); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	d := admin.created["rsync.pipeline.abc"]
	if d.ReplicationFactor != 3 {
		t.Errorf("replication factor = %d, want 3 (unchanged)", d.ReplicationFactor)
	}
	if *d.ConfigEntries["min.insync.replicas"] != "2" {
		t.Errorf("min.insync.replicas = %q, want \"2\" (unchanged)", *d.ConfigEntries["min.insync.replicas"])
	}
}
