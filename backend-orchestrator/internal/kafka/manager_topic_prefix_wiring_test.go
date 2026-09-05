package kafka

import (
	"context"
	"strings"
	"testing"

	"github.com/IBM/sarama"
	"go.opentelemetry.io/otel"

	kafkaclient "github.com/rsync-ai/shared/kafkaclient"
)

// A unit test on kafkaclient.Topic proves the naming RULE. It cannot prove the
// Manager applies it -- and "applied in the helper, missed at the call site" is
// exactly the shape of bug that hides here, because a producer writing an
// unqualified topic and a consumer reading a qualified one raise nothing: the
// consumer simply blocks on a topic nobody writes.
//
// These tests therefore drive the real Produce/Consume entry points and read
// back the topic the Manager actually handed to sarama.

// captureProducer records what SendMessage was asked to send. Only SendMessage
// carries behavior; the rest satisfies sarama.SyncProducer.
type captureProducer struct {
	sent []*sarama.ProducerMessage
}

func (p *captureProducer) SendMessage(msg *sarama.ProducerMessage) (int32, int64, error) {
	p.sent = append(p.sent, msg)
	return 0, 0, nil
}

func (p *captureProducer) SendMessages(msgs []*sarama.ProducerMessage) error {
	p.sent = append(p.sent, msgs...)
	return nil
}

func (p *captureProducer) Close() error                            { return nil }
func (p *captureProducer) TxnStatus() sarama.ProducerTxnStatusFlag { return 0 }
func (p *captureProducer) IsTransactional() bool                   { return false }
func (p *captureProducer) BeginTxn() error                         { return nil }
func (p *captureProducer) CommitTxn() error                        { return nil }
func (p *captureProducer) AbortTxn() error                         { return nil }
func (p *captureProducer) AddOffsetsToTxn(map[string][]*sarama.PartitionOffsetMetadata, string) error {
	return nil
}
func (p *captureProducer) AddOffsetsToTxnWithGroupMetadata(map[string][]*sarama.PartitionOffsetMetadata, *sarama.ConsumerGroupMetadata) error {
	return nil
}
func (p *captureProducer) AddMessageToTxn(*sarama.ConsumerMessage, string, *string) error {
	return nil
}
func (p *captureProducer) AddMessageToTxnWithGroupMetadata(*sarama.ConsumerMessage, *sarama.ConsumerGroupMetadata, *string) error {
	return nil
}

func newTestManager(p sarama.SyncProducer) *Manager {
	return &Manager{
		Config:    Config{GroupID: "orchestrator"},
		producer:  p,
		consumers: make(map[string]sarama.ConsumerGroup),
		tracer:    otel.Tracer("test"),
	}
}

// TestProduceEntryPointsQualifyTheTopic covers every producing entry point,
// including the two thin wrappers, because a future refactor that stops
// delegating would otherwise slip past unnoticed.
func TestProduceEntryPointsQualifyTheTopic(t *testing.T) {
	const logical = "agent.chat.requests"
	want := kafkaclient.Topic(logical)
	if want == logical {
		t.Fatalf("test is vacuous: kafkaclient.Topic(%q) returned it unchanged -- "+
			"is KAFKA_TOPIC_PREFIX set in this environment?", logical)
	}

	cases := []struct {
		name string
		call func(m *Manager) error
	}{
		{"Produce", func(m *Manager) error {
			return m.Produce(logical, []byte("k"), []byte("v"))
		}},
		{"ProduceWithContext", func(m *Manager) error {
			return m.ProduceWithContext(context.Background(), logical, []byte("k"), []byte("v"))
		}},
		{"ProduceWithHeaders", func(m *Manager) error {
			return m.ProduceWithHeaders(logical, []byte("k"), []byte("v"), map[string]string{"h": "1"})
		}},
		{"ProduceWithHeadersAndContext", func(m *Manager) error {
			return m.ProduceWithHeadersAndContext(context.Background(), logical, []byte("k"), []byte("v"), nil)
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := &captureProducer{}
			if err := tc.call(newTestManager(p)); err != nil {
				t.Fatalf("%s returned %v", tc.name, err)
			}
			if len(p.sent) != 1 {
				t.Fatalf("%s sent %d messages, want 1", tc.name, len(p.sent))
			}
			if got := p.sent[0].Topic; got != want {
				t.Errorf("%s wrote to topic %q, want %q -- an unqualified topic is a "+
					"live topic on the customer's cluster that no consumer reads", tc.name, got, want)
			}
		})
	}
}

// TestProduceDoesNotDoubleQualify guards the persisted-name path: topic names
// are stored (pipelines.kafka_topic, sink subscription config) and replayed on
// the next run, so an already-qualified name arrives here a second time.
func TestProduceDoesNotDoubleQualify(t *testing.T) {
	already := kafkaclient.Topic("pipeline.abc12345.data")

	p := &captureProducer{}
	if err := newTestManager(p).ProduceWithContext(context.Background(), already, nil, []byte("v")); err != nil {
		t.Fatalf("ProduceWithContext returned %v", err)
	}
	if got := p.sent[0].Topic; got != already {
		t.Errorf("re-qualified a stored name: %q -> %q", already, got)
	}
}

// TestConsumeQualifiesTheTopic reads the qualification back out of the
// duplicate-subscription guard, which is the first thing ConsumeWithContext
// does after qualifying and the last step reachable without a live broker.
func TestConsumeQualifiesTheTopic(t *testing.T) {
	const logical = "agent.planner.requests"
	want := kafkaclient.Topic(logical)
	if want == logical {
		t.Fatalf("test is vacuous: kafkaclient.Topic(%q) returned it unchanged", logical)
	}

	m := newTestManager(&captureProducer{})
	// Seed the registry under the QUALIFIED name only. If ConsumeWithContext
	// qualifies, it finds this entry and reports the duplicate; if it does not,
	// it misses and walks on toward the broker.
	m.consumers[want] = nil

	err := consumeCatchingPanic(m, logical)
	if err == nil {
		t.Fatal("ConsumeWithContext did not detect the already-registered topic, " +
			"so it looked it up under an unqualified name")
	}
	if !strings.Contains(err.Error(), want) {
		t.Errorf("ConsumeWithContext registered topic %v, want a name containing %q", err, want)
	}
}

// consumeCatchingPanic runs ConsumeWithContext with no broker. On the intended
// path it returns the duplicate-subscription error and never touches the nil
// client; converting a panic into an error keeps an unqualified lookup a plain
// test failure instead of aborting the package's test binary.
func consumeCatchingPanic(m *Manager, topic string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = nil
			_ = r
		}
	}()
	return m.ConsumeWithContext(topic, func(context.Context, *sarama.ConsumerMessage) error { return nil })
}

// TestGeneratedTopicNamesAreQualified pins the two names the orchestrator mints
// itself. These are written to the database and handed to Debezium and the
// sink, so they must already carry the namespace before anyone stores them.
func TestGeneratedTopicNamesAreQualified(t *testing.T) {
	prefix := kafkaclient.TopicPrefix()
	if prefix == "" {
		t.Fatal("test is vacuous: no topic prefix configured in this environment")
	}

	tm := &TopologyManager{}
	for _, tt := range []struct{ kind, ns string }{
		{"cdc", "cdc."},
		{"batch", "pipeline."},
	} {
		got := tm.generateTopicName("abc12345-0000-0000-0000-000000000000", tt.kind)
		if !strings.HasPrefix(got, prefix) {
			t.Errorf("generateTopicName(%q) = %q, want the %q namespace", tt.kind, got, prefix)
		}
		if !kafkaclient.InNamespace(got, tt.ns) {
			t.Errorf("generateTopicName(%q) = %q, want it inside %q", tt.kind, got, tt.ns)
		}
	}
}
