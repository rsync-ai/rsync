package kafka

import (
	"errors"
	"reflect"
	"testing"

	"github.com/IBM/sarama"
)

// alterRecordingAdmin records IncrementalAlterConfig calls on top of fakeAdmin.
type alterRecordingAdmin struct {
	*fakeAdmin
	altered  map[string]map[string]string
	alterErr error
}

func (a *alterRecordingAdmin) IncrementalAlterConfig(rt sarama.ConfigResourceType, name string,
	entries map[string]sarama.IncrementalAlterConfigsEntry, _ bool) error {
	if a.alterErr != nil {
		return a.alterErr
	}
	if rt != sarama.TopicResource {
		return errors.New("not a topic resource")
	}
	got := map[string]string{}
	for k, e := range entries {
		if e.Operation != sarama.IncrementalAlterConfigsOperationSet || e.Value == nil {
			return errors.New("only SET with a value is expected")
		}
		got[k] = *e.Value
	}
	a.altered[name] = got
	return nil
}

type topicsClient struct {
	*fakeClient
	topics []string
}

func (c *topicsClient) Topics() ([]string, error) { return c.topics, nil }

func signalTopicManager(existing ...string) (*Manager, *alterRecordingAdmin) {
	tm, base := newFakeManager(1)
	admin := &alterRecordingAdmin{fakeAdmin: base, altered: map[string]map[string]string{}}
	tm.admin = admin
	client := &topicsClient{fakeClient: tm.client.(*fakeClient), topics: existing}
	return &Manager{connected: true, client: client, topology: tm}, admin
}

var wantSignalConfig = map[string]string{"cleanup.policy": "delete", "retention.ms": "86400000", "segment.ms": "3600000"}

// A signal topic created before #23 carries the broker's 7-day retention; the
// first EnsureSignalTopic in a process brings it to one day, and later calls
// leave the broker alone.
func TestEnsureSignalTopicTunesAnExistingTopicOnce(t *testing.T) {
	m, admin := signalTopicManager("rsync.signals.600b012e")
	if err := m.EnsureSignalTopic("rsync.signals.600b012e"); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(admin.altered["rsync.signals.600b012e"], wantSignalConfig) {
		t.Fatalf("altered = %v, want %v", admin.altered, wantSignalConfig)
	}
	if len(admin.created) != 0 {
		t.Fatalf("an existing topic was created again: %v", admin.created)
	}
	admin.altered = map[string]map[string]string{}
	if err := m.EnsureSignalTopic("rsync.signals.600b012e"); err != nil {
		t.Fatal(err)
	}
	if len(admin.altered) != 0 {
		t.Fatalf("the second call altered the topic again: %v", admin.altered)
	}
}

func TestEnsureSignalTopicCreatesWithShortRetention(t *testing.T) {
	m, admin := signalTopicManager()
	if err := m.EnsureSignalTopic("rsync.signals.ae6e0deb"); err != nil {
		t.Fatal(err)
	}
	d, ok := admin.created["rsync.signals.ae6e0deb"]
	if !ok {
		t.Fatalf("the topic was not created: %v", admin.created)
	}
	for k, want := range wantSignalConfig {
		if got := d.ConfigEntries[k]; got == nil || *got != want {
			t.Errorf("created with %s = %v, want %s", k, got, want)
		}
	}
	if d.NumPartitions != 1 {
		t.Errorf("partitions = %d, want 1 (signals are read in order)", d.NumPartitions)
	}
	if !reflect.DeepEqual(admin.altered["rsync.signals.ae6e0deb"], wantSignalConfig) {
		t.Errorf("altered = %v, want %v", admin.altered, wantSignalConfig)
	}
}

// A broker that refuses the alter still leaves a usable topic: the error is
// returned (the callers log it) and the next call tries again.
func TestEnsureSignalTopicReportsARefusedAlterAndRetries(t *testing.T) {
	m, admin := signalTopicManager("rsync.signals.600b012e")
	admin.alterErr = errors.New("CLUSTER_AUTHORIZATION_FAILED")
	if err := m.EnsureSignalTopic("rsync.signals.600b012e"); err == nil {
		t.Fatal("a refused alter was reported as success")
	}
	admin.alterErr = nil
	if err := m.EnsureSignalTopic("rsync.signals.600b012e"); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(admin.altered["rsync.signals.600b012e"], wantSignalConfig) {
		t.Fatalf("the retry did not set the config: %v", admin.altered)
	}
}
