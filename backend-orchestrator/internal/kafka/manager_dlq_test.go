package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/IBM/sarama"
	kafkaclient "github.com/rsync-ai/shared/kafkaclient"
	"go.opentelemetry.io/otel"
)

// The Manager's consumers retry a failing message, then park it on <topic>.dlq and
// commit past it. When that DLQ produce ALSO fails the message must not be committed,
// and "not marking it" is not enough on its own: the next message that succeeds is
// marked, and a MarkMessage commits every offset below it, so the failed message
// would be skipped for good. These tests pin the fail-closed path, the DLQ companion
// that ConsumeWithContext pre-creates, and the two exact-name helpers the CDC code
// uses (ProduceToExactTopic, EnsureDDLTopic).

// fakeSession is a sarama.ConsumerGroupSession that records the messages marked.
type fakeSession struct {
	ctx    context.Context
	marked []int64
}

func (s *fakeSession) Claims() map[string][]int32               { return nil }
func (s *fakeSession) MemberID() string                         { return "m" }
func (s *fakeSession) GenerationID() int32                      { return 1 }
func (s *fakeSession) MarkOffset(string, int32, int64, string)  {}
func (s *fakeSession) Commit()                                  {}
func (s *fakeSession) ResetOffset(string, int32, int64, string) {}
func (s *fakeSession) MarkMessage(m *sarama.ConsumerMessage, _ string) {
	s.marked = append(s.marked, m.Offset)
}
func (s *fakeSession) Context() context.Context { return s.ctx }

// fakeClaim delivers a fixed set of messages and then closes, which ConsumeClaim
// reads as the end of the claim (a nil message), so no test here can hang.
type fakeClaim struct{ msgs chan *sarama.ConsumerMessage }

func newFakeClaim(topic string, offsets ...int64) *fakeClaim {
	c := &fakeClaim{msgs: make(chan *sarama.ConsumerMessage, len(offsets))}
	for _, o := range offsets {
		c.msgs <- &sarama.ConsumerMessage{Topic: topic, Partition: 0, Offset: o, Value: []byte("v")}
	}
	close(c.msgs)
	return c
}

func (c *fakeClaim) Topic() string                            { return "" }
func (c *fakeClaim) Partition() int32                         { return 0 }
func (c *fakeClaim) InitialOffset() int64                     { return 0 }
func (c *fakeClaim) HighWaterMarkOffset() int64               { return 0 }
func (c *fakeClaim) Messages() <-chan *sarama.ConsumerMessage { return c.msgs }

// failingProducer refuses every send, as a DLQ topic the broker will not accept does.
type failingProducer struct{ captureProducer }

func (p *failingProducer) SendMessage(msg *sarama.ProducerMessage) (int32, int64, error) {
	p.sent = append(p.sent, msg)
	return 0, 0, errors.New("UNKNOWN_TOPIC_OR_PARTITION")
}

// runClaim runs one ConsumeClaim over offsets 10 and 11 where the handler fails
// offset 10 on every attempt and handles 11 fine.
func runClaim(t *testing.T, producer sarama.SyncProducer) (err error, marked []int64, handled11 bool) {
	t.Helper()
	old := dlqFailureBackoff
	dlqFailureBackoff = 0
	t.Cleanup(func() { dlqFailureBackoff = old })

	const topic = "rsync.healer.schema-changes"
	h := &ConsumerGroupHandler{
		topic: topic,
		handlerWithCtx: func(_ context.Context, m *sarama.ConsumerMessage) error {
			if m.Offset == 10 {
				return errors.New("handler failed")
			}
			handled11 = true
			return nil
		},
		producer:   producer,
		maxRetries: 1,
		tracer:     otel.Tracer("test"),
	}
	sess := &fakeSession{ctx: context.Background()}
	done := make(chan error, 1)
	go func() { done <- h.ConsumeClaim(sess, newFakeClaim(topic, 10, 11)) }()
	select {
	case err = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("ConsumeClaim did not return")
	}
	return err, sess.marked, handled11
}

func TestConsumeClaimEndsTheClaimWhenTheDLQSendFails(t *testing.T) {
	p := &failingProducer{}
	err, marked, handled11 := runClaim(t, p)

	if len(p.sent) != 1 || p.sent[0].Topic != "rsync.healer.schema-changes.dlq" {
		t.Fatalf("expected one DLQ attempt on rsync.healer.schema-changes.dlq, got %d", len(p.sent))
	}
	if err == nil {
		t.Fatal("ConsumeClaim returned nil after the handler and the DLQ send both failed; " +
			"the claim must end so the group rejoins at the uncommitted message")
	}
	if len(marked) != 0 {
		t.Errorf("marked offsets %v after a failed DLQ send; marking the NEXT message commits "+
			"past offset 10 and loses it", marked)
	}
	if handled11 {
		t.Error("offset 11 was processed after offset 10 could not be parked; the claim must " +
			"stop at the failed message")
	}
}

// The control: with a working DLQ the failed message is parked and committed past,
// and the claim reads on. This is the behaviour the fail-closed path must not disturb.
func TestConsumeClaimCommitsPastAMessageTheDLQAccepted(t *testing.T) {
	p := &captureProducer{}
	err, marked, handled11 := runClaim(t, p)

	if err != nil {
		t.Fatalf("ConsumeClaim returned %v with a working DLQ", err)
	}
	if len(p.sent) != 1 || p.sent[0].Topic != "rsync.healer.schema-changes.dlq" {
		t.Fatalf("expected one DLQ send to rsync.healer.schema-changes.dlq, got %v", p.sent)
	}
	if len(marked) != 2 || marked[0] != 10 || marked[1] != 11 || !handled11 {
		t.Errorf("marked %v (handled 11: %v), want [10 11] and 11 handled", marked, handled11)
	}
}

// ConsumeWithContext pre-creates the consumed topic's DLQ companion at 1 partition
// with seven days of delete retention, before the group joins, so the first DLQ send
// neither auto-creates it at the broker's defaults nor fails on a broker that has
// auto-creation off.
func TestConsumeWithContextPreCreatesTheDLQCompanion(t *testing.T) {
	m, admin := signalTopicManager()
	f := &groupFactory{}
	m.Config = Config{GroupID: "orchestrator"}
	m.producer = &captureProducer{}
	m.consumers = make(map[string]sarama.ConsumerGroup)
	m.tracer = otel.Tracer("test")
	m.newConsumerGroup = f.build

	topic := kafkaclient.Topic("healer.schema-changes")
	if err := m.ConsumeWithContext("healer.schema-changes", noopHandler); err != nil {
		t.Fatalf("ConsumeWithContext: %v", err)
	}
	defer m.StopConsuming(topic)
	joinedHandler(t, f.groups[0])

	d, ok := admin.created[topic+".dlq"]
	if !ok {
		t.Fatalf("ConsumeWithContext did not create %s.dlq; created %v", topic, keysOf(admin.created))
	}
	if d.NumPartitions != 1 {
		t.Errorf("%s.dlq created with %d partitions, want 1", topic, d.NumPartitions)
	}
	for k, want := range map[string]string{"cleanup.policy": "delete", "retention.ms": "604800000"} {
		if got := d.ConfigEntries[k]; got == nil || *got != want {
			t.Errorf("%s.dlq %s = %v, want %s", topic, k, got, want)
		}
	}
	if len(admin.created) != 1 {
		t.Errorf("created %v; ConsumeWithContext should create only the DLQ companion", keysOf(admin.created))
	}
}

func TestProduceToExactTopicDoesNotQualify(t *testing.T) {
	const exact = "_rsync-connect-offsets"
	if kafkaclient.Topic(exact) == exact {
		t.Fatalf("test is vacuous: kafkaclient.Topic(%q) is unchanged -- is KAFKA_TOPIC_PREFIX empty?", exact)
	}
	p := &captureProducer{}
	if err := newTestManager(p).ProduceToExactTopic(context.Background(), exact, []byte("k"), []byte("v")); err != nil {
		t.Fatalf("ProduceToExactTopic: %v", err)
	}
	if len(p.sent) != 1 || p.sent[0].Topic != exact {
		t.Fatalf("sent %v, want exactly one message on %q", p.sent, exact)
	}
	if err := newTestManager(p).ProduceToExactTopic(context.Background(), " ", nil, nil); err == nil {
		t.Error("ProduceToExactTopic accepted a blank topic name")
	}
}

func TestEnsureDDLTopicCreatesOnePartitionWithSevenDayRetention(t *testing.T) {
	m, admin := signalTopicManager()
	const ddl = "rsync.cdc-600b012e"
	if err := m.EnsureDDLTopic(ddl); err != nil {
		t.Fatalf("EnsureDDLTopic: %v", err)
	}
	d, ok := admin.created[ddl]
	if !ok {
		t.Fatalf("EnsureDDLTopic did not create %s under its exact name; created %v", ddl, keysOf(admin.created))
	}
	if d.NumPartitions != 1 {
		t.Errorf("%s created with %d partitions, want 1", ddl, d.NumPartitions)
	}
	for k, want := range map[string]string{"cleanup.policy": "delete", "retention.ms": "604800000"} {
		if got := d.ConfigEntries[k]; got == nil || *got != want {
			t.Errorf("%s %s = %v, want %s", ddl, k, got, want)
		}
	}
}
