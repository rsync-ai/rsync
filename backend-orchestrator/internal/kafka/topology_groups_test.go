package kafka

import (
	"encoding/binary"
	"reflect"
	"testing"

	"github.com/IBM/sarama"
)

// memberMetadataV0 encodes the subscription a consumer sends when it joins
// (ConsumerGroupMemberMetadata v0: version, topics, null user data).
func memberMetadataV0(topics ...string) []byte {
	b := binary.BigEndian.AppendUint16(nil, 0)
	b = binary.BigEndian.AppendUint32(b, uint32(len(topics)))
	for _, t := range topics {
		b = binary.BigEndian.AppendUint16(b, uint16(len(t)))
		b = append(b, t...)
	}
	return binary.BigEndian.AppendUint32(b, 0xFFFFFFFF)
}

// memberAssignmentV0 encodes the partitions the leader assigned a member.
func memberAssignmentV0(topic string, partitions ...int32) []byte {
	b := binary.BigEndian.AppendUint16(nil, 0)
	b = binary.BigEndian.AppendUint32(b, 1)
	b = binary.BigEndian.AppendUint16(b, uint16(len(topic)))
	b = append(b, topic...)
	b = binary.BigEndian.AppendUint32(b, uint32(len(partitions)))
	for _, p := range partitions {
		b = binary.BigEndian.AppendUint32(b, uint32(p))
	}
	return binary.BigEndian.AppendUint32(b, 0xFFFFFFFF)
}

func TestGroupsReadingTopic(t *testing.T) {
	const topic = "rsync.cdc-600b.public.orders"
	described := []*sarama.GroupDescription{
		// Subscribed to the topic.
		{GroupId: "sink-600b-stream", ProtocolType: "consumer", Members: map[string]*sarama.GroupMemberDescription{
			"m1": {MemberMetadata: memberMetadataV0("rsync.cdc-600b.public.users", topic)},
		}},
		// Assigned a partition of it (subscription names only another topic).
		{GroupId: "cdc-stats", ProtocolType: "consumer", Members: map[string]*sarama.GroupMemberDescription{
			"m1": {MemberMetadata: memberMetadataV0("other"), MemberAssignment: memberAssignmentV0(topic, 0)},
		}},
		// Reads other topics only.
		{GroupId: "sink-other", ProtocolType: "consumer", Members: map[string]*sarama.GroupMemberDescription{
			"m1": {MemberMetadata: memberMetadataV0("rsync.cdc-600b.public.users")},
		}},
		// No members: an empty group reads nothing.
		{GroupId: "sink-600b-batch", ProtocolType: "consumer"},
		// Kafka Connect workers use the "connect" protocol; their metadata is not a
		// topic subscription.
		{GroupId: "connect-cluster", ProtocolType: "connect", Members: map[string]*sarama.GroupMemberDescription{
			"m1": {MemberMetadata: []byte{0xde, 0xad}, MemberAssignment: []byte{0xbe}},
		}},
		// Undecodable metadata and assignment: counted as a reader, because a
		// member we cannot read might still fetch the topic.
		{GroupId: "unreadable", ProtocolType: "consumer", Members: map[string]*sarama.GroupMemberDescription{
			"m1": {MemberMetadata: []byte{0x00}, MemberAssignment: []byte{0x00}},
		}},
		nil,
	}
	got := groupsReadingTopic(described, topic)
	want := []string{"cdc-stats", "sink-600b-stream", "unreadable"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("groupsReadingTopic = %v, want %v", got, want)
	}
	if got := groupsReadingTopic(described[2:4], topic); len(got) != 0 {
		t.Fatalf("groups that do not read the topic were reported: %v", got)
	}
}
