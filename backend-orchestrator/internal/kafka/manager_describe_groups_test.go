package kafka

// DescribeConsumerGroups supplies the one fact lag cannot: whether anybody is
// actually in the group.
//
// A consumer group with lag 0 and state "Empty" has no members — nothing is
// reading it, and the queue is drained only in the sense that the producer stopped
// too. That is the same blind spot that let a dead Debezium read as "caught up",
// one level down, so the rules below are worth pinning: an unknown group must be
// ABSENT rather than reported as Dead, because "this consumer has not started" and
// "this consumer has stopped" are different answers and the UI shows them
// differently.

import (
	"errors"
	"testing"

	"github.com/IBM/sarama"
)

type fakeDescriber struct {
	groups []*sarama.GroupDescription
	err    error
	asked  []string
}

func (f *fakeDescriber) DescribeConsumerGroups(groups []string) ([]*sarama.GroupDescription, error) {
	f.asked = groups
	return f.groups, f.err
}

func member() *sarama.GroupMemberDescription { return &sarama.GroupMemberDescription{} }

func TestDescribeConsumerGroups_ReportsStateAndMemberCount(t *testing.T) {
	f := &fakeDescriber{groups: []*sarama.GroupDescription{
		{GroupId: "rsync.sink-aa4c1a3c", State: "Stable", ProtocolType: "consumer",
			Members: map[string]*sarama.GroupMemberDescription{"m1": member(), "m2": member()}},
		{GroupId: "rsync.sink-aa4c1a3c-batch", State: "Empty", ProtocolType: "consumer",
			Members: map[string]*sarama.GroupMemberDescription{}},
	}}

	got, err := describeConsumerGroups(f, []string{"rsync.sink-aa4c1a3c", "rsync.sink-aa4c1a3c-batch"})
	if err != nil {
		t.Fatalf("describeConsumerGroups: %v", err)
	}

	if d := got["rsync.sink-aa4c1a3c"]; d.State != "Stable" || d.Members != 2 {
		t.Errorf("sink group = %+v, want Stable with 2 members", d)
	}
	// Empty WITH a description is a real finding — the group exists and has lost
	// its consumers — and must survive, unlike the unknown-group case below.
	if d, ok := got["rsync.sink-aa4c1a3c-batch"]; !ok || d.State != "Empty" || d.Members != 0 {
		t.Errorf("batch group = %+v (present=%v), want a reported Empty with 0 members", d, ok)
	}
}

// Sarama answers about a group id the broker has never seen with state "Dead" and
// no members. Reporting that would tell the user a consumer had died when it
// simply has not started — the census asks about groups it read from a manifest,
// so this case is routine, not exceptional.
func TestDescribeConsumerGroups_UnknownGroupIsAbsentNotDead(t *testing.T) {
	f := &fakeDescriber{groups: []*sarama.GroupDescription{
		{GroupId: "rsync.sink-never-started", State: "Dead", ProtocolType: "consumer",
			Members: map[string]*sarama.GroupMemberDescription{}},
	}}

	got, err := describeConsumerGroups(f, []string{"rsync.sink-never-started"})
	if err != nil {
		t.Fatalf("describeConsumerGroups: %v", err)
	}
	if d, ok := got["rsync.sink-never-started"]; ok {
		t.Errorf("unknown group reported as %+v; want absent so the UI shows 'unknown', not 'Dead'", d)
	}
}

// A Dead group that still HAS members is not the unknown-group shape and must be
// reported — otherwise the filter above would swallow a real failure.
func TestDescribeConsumerGroups_DeadWithMembersIsStillReported(t *testing.T) {
	f := &fakeDescriber{groups: []*sarama.GroupDescription{
		{GroupId: "g", State: "Dead", ProtocolType: "consumer",
			Members: map[string]*sarama.GroupMemberDescription{"m1": member()}},
	}}

	got, err := describeConsumerGroups(f, []string{"g"})
	if err != nil {
		t.Fatalf("describeConsumerGroups: %v", err)
	}
	if d, ok := got["g"]; !ok || d.State != "Dead" || d.Members != 1 {
		t.Errorf("got %+v (present=%v), want a reported Dead with 1 member", d, ok)
	}
}

// Kafka Connect workers use the "connect" protocol and carry no consumer-group
// membership in this wire format, so their member count would be meaningless.
// TopicConsumerGroups applies the same filter.
func TestDescribeConsumerGroups_SkipsNonConsumerProtocols(t *testing.T) {
	f := &fakeDescriber{groups: []*sarama.GroupDescription{
		{GroupId: "connect-cluster", State: "Stable", ProtocolType: "connect",
			Members: map[string]*sarama.GroupMemberDescription{"w1": member()}},
		{GroupId: "plain", State: "Stable", ProtocolType: "",
			Members: map[string]*sarama.GroupMemberDescription{"m1": member()}},
	}}

	got, err := describeConsumerGroups(f, []string{"connect-cluster", "plain"})
	if err != nil {
		t.Fatalf("describeConsumerGroups: %v", err)
	}
	if _, ok := got["connect-cluster"]; ok {
		t.Error("a Kafka Connect worker group was reported as a consumer group")
	}
	// An empty protocol type is the ordinary consumer case on older brokers.
	if _, ok := got["plain"]; !ok {
		t.Error("a group with an empty protocol type was dropped; that is the ordinary consumer case")
	}
}

// A failed describe must not read as "every group is fine": the caller stores
// state NULL and the UI renders unknown.
func TestDescribeConsumerGroups_ErrorIsNotAnEmptyAnswer(t *testing.T) {
	f := &fakeDescriber{err: errors.New("broker unreachable")}

	got, err := describeConsumerGroups(f, []string{"g"})
	if err == nil {
		t.Fatal("expected an error, got none — a failed describe would read as 'no groups'")
	}
	if got != nil {
		t.Errorf("got %+v alongside the error; want nil so a partial answer cannot be used", got)
	}
}

// An unconnected manager refuses rather than answering, exactly as
// ListConsumerGroups does. The census treats the error as "could not ask" and
// stores state NULL, so the UI shows unknown — whereas an empty map would have
// been indistinguishable from "this pipeline has no consumers".
func TestDescribeConsumerGroups_UnconnectedManagerRefuses(t *testing.T) {
	m := &Manager{}
	if _, err := m.DescribeConsumerGroups([]string{"g"}); err == nil {
		t.Fatal("an unconnected manager answered; want an error so the caller stores 'unknown'")
	}
	// Same answer for an empty ask: the guard is on the connection, not the input,
	// so a caller cannot accidentally read "no groups" off a dead client.
	if _, err := m.DescribeConsumerGroups(nil); err == nil {
		t.Fatal("an unconnected manager answered an empty ask; want the connection error")
	}
}
