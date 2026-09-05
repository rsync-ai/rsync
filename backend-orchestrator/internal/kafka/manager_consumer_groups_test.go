package kafka

import (
	"errors"
	"sort"
	"strings"
	"testing"
)

// Kafka partitions group metadata across brokers, so a ListGroups request answers only
// for the broker that received it. ListConsumerGroups used to ask Brokers()[0] and
// return that as the cluster's answer. On the bundled single-broker stack the two are
// the same thing, which is why it stayed invisible; on a customer's multi-broker
// cluster it is a partial list, and a partial list is worse than an error here — the
// sentinel's wedge detector and any lag-based autoscaler read a group they cannot see
// as "that group has no lag" rather than as "I could not see it".
//
// The aggregation itself is sarama's (admin.go ListConsumerGroups queries every broker
// in parallel and returns an error if ANY of them failed). What these tests pin is that
// this package does not undo either half of that contract.

type stubGroupLister struct {
	groups map[string]string
	err    error
}

func (s stubGroupLister) ListConsumerGroups() (map[string]string, error) {
	return s.groups, s.err
}

func TestListConsumerGroupsReturnsEveryGroup(t *testing.T) {
	got, err := listConsumerGroups(stubGroupLister{groups: map[string]string{
		// In a real multi-broker cluster these come back from different brokers.
		"rsync.cdc-sink-abc12345": "consumer",
		"rsync.cdc-sink-def67890": "consumer",
		"rsync.orchestrator":      "consumer",
	}})
	if err != nil {
		t.Fatalf("listConsumerGroups: %v", err)
	}
	sort.Strings(got)
	want := []string{"rsync.cdc-sink-abc12345", "rsync.cdc-sink-def67890", "rsync.orchestrator"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("groups = %v, want %v", got, want)
	}
}

// sarama populates the group map AND returns an error when only some brokers answered.
// Returning both would hand the caller a plausible-looking list that is silently
// missing whatever lived on the broker that failed — the exact shape of the bug.
func TestListConsumerGroupsRefusesToAnswerPartially(t *testing.T) {
	brokerDown := errors.New("dial tcp 10.0.1.7:9092: connect: connection refused")
	got, err := listConsumerGroups(stubGroupLister{
		groups: map[string]string{"rsync.cdc-sink-abc12345": "consumer"}, // the one broker that DID answer
		err:    brokerDown,
	})
	if err == nil {
		t.Fatal("a broker that did not answer must surface as an error, not as a shorter list")
	}
	if got != nil {
		t.Errorf("groups = %v, want nil: a caller handed both a list and an error reads the list", got)
	}
	if !errors.Is(err, brokerDown) {
		t.Errorf("error %v does not wrap the broker's, so the cause is unrecoverable from the log", err)
	}
}

func TestListConsumerGroupsHandlesAnEmptyCluster(t *testing.T) {
	got, err := listConsumerGroups(stubGroupLister{groups: map[string]string{}})
	if err != nil {
		t.Fatalf("listConsumerGroups: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("groups = %v, want empty", got)
	}
}

// A disconnected manager must not reach for a nil client.
func TestListConsumerGroupsRequiresAConnection(t *testing.T) {
	if _, err := (&Manager{}).ListConsumerGroups(); err == nil {
		t.Fatal("a manager with no client must return an error rather than panicking")
	}
}
