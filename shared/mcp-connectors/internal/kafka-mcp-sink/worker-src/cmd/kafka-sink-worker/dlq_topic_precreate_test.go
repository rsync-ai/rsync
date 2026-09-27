package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
	createTopicsAPI "github.com/segmentio/kafka-go/protocol/createtopics"
	metadataAPI "github.com/segmentio/kafka-go/protocol/metadata"
	produceAPI "github.com/segmentio/kafka-go/protocol/produce"
)

// A "<source_topic>.dlq" used to come into being only by broker auto-create on its
// first write, so it got the broker's num.partitions and retention.ms: sized and kept
// like a data topic, for a trail of parked records nobody bounded. ensureDLQTopic
// creates it first, at one partition and seven days, and retries a failed create. These tests drive the real
// sendToDLQ through the writer's own transport, so they see the CreateTopics request
// the broker would see, and in what order relative to the produce.

// dlqTopicBroker is a fake broker that records CreateTopics and Produce requests in
// arrival order and can be told to fail either.
type dlqTopicBroker struct {
	mu      sync.Mutex
	events  []string // "create:<topic>" and "produce:<topic>", in arrival order
	creates []createTopicsAPI.RequestTopic

	createErrorCode int16 // per-topic error code in the CreateTopics response
	createErr       error // fails the CreateTopics round trip itself
	produceErr      error // fails every Produce round trip
}

func (b *dlqTopicBroker) RoundTrip(_ context.Context, _ net.Addr, m protocol.Message) (protocol.Message, error) {
	switch req := m.(type) {
	case *createTopicsAPI.Request:
		b.mu.Lock()
		defer b.mu.Unlock()
		res := &createTopicsAPI.Response{}
		for _, t := range req.Topics {
			b.events = append(b.events, "create:"+t.Name)
			b.creates = append(b.creates, t)
			res.Topics = append(res.Topics, createTopicsAPI.ResponseTopic{Name: t.Name, ErrorCode: b.createErrorCode})
		}
		if b.createErr != nil {
			return nil, b.createErr
		}
		return res, nil
	case *metadataAPI.Request:
		res := &metadataAPI.Response{}
		for _, name := range req.TopicNames {
			res.Topics = append(res.Topics, metadataAPI.ResponseTopic{Name: name,
				Partitions: []metadataAPI.ResponsePartition{{PartitionIndex: 0}}})
		}
		return res, nil
	case *produceAPI.Request:
		b.mu.Lock()
		defer b.mu.Unlock()
		if b.produceErr != nil {
			return nil, b.produceErr
		}
		res := &produceAPI.Response{}
		for _, tp := range req.Topics {
			b.events = append(b.events, "produce:"+tp.Topic)
			rt := produceAPI.ResponseTopic{Topic: tp.Topic}
			for _, p := range tp.Partitions {
				rt.Partitions = append(rt.Partitions, produceAPI.ResponsePartition{Partition: p.Partition})
			}
			res.Topics = append(res.Topics, rt)
		}
		return res, nil
	}
	return nil, fmt.Errorf("unexpected kafka request %T", m)
}

func (b *dlqTopicBroker) snapshot() ([]string, []createTopicsAPI.RequestTopic) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.events...), append([]createTopicsAPI.RequestTopic(nil), b.creates...)
}

func (b *dlqTopicBroker) createsFor(topic string) int {
	_, creates := b.snapshot()
	n := 0
	for _, c := range creates {
		if c.Name == topic {
			n++
		}
	}
	return n
}

// dlqTopicWriter mirrors the production dlqWriter's contract (synchronous, one attempt
// per write) against the fake broker.
func dlqTopicWriter(b *dlqTopicBroker) *kafka.Writer {
	return &kafka.Writer{
		Addr:                   kafka.TCP("127.0.0.1:1"),
		Transport:              b,
		BatchSize:              1,
		RequiredAcks:           kafka.RequireOne,
		AllowAutoTopicCreation: true,
		MaxAttempts:            1,
	}
}

// resetDLQTopicsEnsured forgets every topic, so a test's expectations do not depend on
// which other test in the package parked a record on the same name first.
func resetDLQTopicsEnsured(t *testing.T) {
	t.Helper()
	forget := func() {
		dlqTopicsEnsured.Range(func(k, _ any) bool {
			dlqTopicsEnsured.Delete(k)
			return true
		})
	}
	forget()
	t.Cleanup(forget)
}

func parkOne(t *testing.T, w *kafka.Writer, srcTopic string, metrics *Metrics) error {
	t.Helper()
	msg := kafka.Message{Topic: srcTopic, Partition: 0, Offset: 7, Key: []byte("k"), Value: []byte(`{"op":"c"}`)}
	return sendToDLQ(context.Background(), w, msg, errors.New("destination rejected the row"), metrics, "public.orders")
}

func TestSendToDLQCreatesTheDLQTopicBeforeItsFirstWrite(t *testing.T) {
	resetDLQTopicsEnsured(t)
	b := &dlqTopicBroker{}
	w := dlqTopicWriter(b)
	defer w.Close()

	if err := parkOne(t, w, "rsync.cdc.a1b2c3d4.public.orders", &Metrics{}); err != nil {
		t.Fatalf("sendToDLQ: %v", err)
	}

	const dlq = "rsync.cdc.a1b2c3d4.public.orders.dlq"
	events, creates := b.snapshot()
	if len(events) < 2 || events[0] != "create:"+dlq || events[len(events)-1] != "produce:"+dlq {
		t.Fatalf("broker saw %v, want create:%s before produce:%s — without the create first, the "+
			"topic is born by auto-create with the broker's partition count and retention", events, dlq, dlq)
	}
	if len(creates) != 1 {
		t.Fatalf("CreateTopics carried %d topics, want 1", len(creates))
	}
	c := creates[0]
	if c.NumPartitions != 1 {
		t.Errorf("NumPartitions = %d, want 1", c.NumPartitions)
	}
	if c.ReplicationFactor != -1 {
		t.Errorf("ReplicationFactor = %d, want -1 (the broker default, what auto-create would use)", c.ReplicationFactor)
	}
	retention := ""
	for _, cfg := range c.Configs {
		if cfg.Name == "retention.ms" {
			retention = cfg.Value
		}
	}
	if retention != "604800000" {
		t.Errorf("retention.ms = %q, want %q (seven days); configs = %+v", retention, "604800000", c.Configs)
	}
}

func TestSendToDLQCreatesEachDLQTopicOnce(t *testing.T) {
	resetDLQTopicsEnsured(t)
	b := &dlqTopicBroker{}
	w := dlqTopicWriter(b)
	defer w.Close()

	const src, other = "rsync.cdc.once.public.a", "rsync.cdc.once.public.b"
	var wg sync.WaitGroup
	var failed atomic.Int32
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := sendToDLQ(context.Background(), w, kafka.Message{Topic: src, Value: []byte(`{}`)},
				errors.New("bad row"), &Metrics{}, "public.a"); err != nil {
				failed.Add(1)
			}
		}()
	}
	wg.Wait()
	for i := 0; i < 3; i++ {
		if err := parkOne(t, w, src, &Metrics{}); err != nil {
			t.Fatalf("sendToDLQ: %v", err)
		}
	}
	if err := parkOne(t, w, other, &Metrics{}); err != nil {
		t.Fatalf("sendToDLQ: %v", err)
	}
	if n := failed.Load(); n != 0 {
		t.Fatalf("%d concurrent sendToDLQ calls failed", n)
	}

	if n := b.createsFor(src + ".dlq"); n != 1 {
		t.Errorf("%s.dlq was created %d times across 11 parked records, want exactly once", src, n)
	}
	if n := b.createsFor(other + ".dlq"); n != 1 {
		t.Errorf("%s.dlq was created %d times, want once — each DLQ topic gets its own create", other, n)
	}
}

func TestSendToDLQToleratesADLQTopicThatAlreadyExists(t *testing.T) {
	resetDLQTopicsEnsured(t)
	b := &dlqTopicBroker{createErrorCode: int16(kafka.TopicAlreadyExists)}
	w := dlqTopicWriter(b)
	defer w.Close()

	var err error
	lines := captureLogLines(t, func() { err = parkOne(t, w, "rsync.cdc.exists.public.t", &Metrics{}) })
	if err != nil {
		t.Fatalf("sendToDLQ: %v", err)
	}
	for _, l := range lines {
		if strings.Contains(fmt.Sprint(l["message"]), "pre-create failed") {
			t.Errorf("TopicAlreadyExists is the steady state, not a failure, but it logged: %v", l)
		}
	}
	if events, _ := b.snapshot(); len(events) == 0 || events[len(events)-1] != "produce:rsync.cdc.exists.public.t.dlq" {
		t.Errorf("broker saw %v, want the record written after the create", events)
	}
}

func TestSendToDLQStillWritesWhenTheDLQTopicCannotBeCreated(t *testing.T) {
	for name, b := range map[string]*dlqTopicBroker{
		"the broker refuses the create": {createErrorCode: int16(kafka.TopicAuthorizationFailed)},
		"the create round trip fails":   {createErr: errors.New("connection reset")},
	} {
		t.Run(name, func(t *testing.T) {
			resetDLQTopicsEnsured(t)
			w := dlqTopicWriter(b)
			defer w.Close()
			const dlq = "rsync.cdc.nocreate.public.t.dlq"

			metrics := &Metrics{}
			var err error
			lines := captureLogLines(t, func() { err = parkOne(t, w, "rsync.cdc.nocreate.public.t", metrics) })
			if err != nil {
				t.Fatalf("sendToDLQ = %v, want the write to go ahead on broker auto-create", err)
			}
			if got := atomic.LoadUint64(&metrics.dlqRouted); got != 1 {
				t.Errorf("dlqRouted = %d, want 1", got)
			}
			warned := false
			for _, l := range lines {
				if strings.Contains(fmt.Sprint(l["message"]), "pre-create failed") && l["dlq_topic"] == dlq {
					warned = true
				}
			}
			if !warned {
				t.Errorf("a failed create must be logged with its dlq_topic; got %v", lines)
			}
		})
	}
}

// The create is advisory; the fail-closed contract belongs to the write. A DLQ that
// cannot be created AND cannot be written must still come back as an error, so the
// caller halts instead of committing an offset for a record that went nowhere.
func TestSendToDLQStillFailsClosedWhenTheWriteFails(t *testing.T) {
	resetDLQTopicsEnsured(t)
	b := &dlqTopicBroker{createErr: errors.New("broker down"), produceErr: errors.New("broker down")}
	w := dlqTopicWriter(b)
	defer w.Close()

	metrics := &Metrics{}
	var err error
	captureLogLines(t, func() { err = parkOne(t, w, "rsync.cdc.down.public.t", metrics) })
	if err == nil {
		t.Fatal("sendToDLQ returned nil with the DLQ broker down — the caller would commit and lose the record")
	}
	if got := atomic.LoadUint64(&metrics.dlqPublishFailures); got != 1 {
		t.Errorf("dlqPublishFailures = %d, want 1", got)
	}
	if got := atomic.LoadUint64(&metrics.dlqRouted); got != 0 {
		t.Errorf("dlqRouted = %d, want 0", got)
	}
}

// A record with no source topic used to park on the bare "unknown.dlq": outside the
// deployment's namespace, so outside its PREFIXED ACL grant and unattributable on a
// shared cluster.
func TestSendToDLQQualifiesTheUnknownTopic(t *testing.T) {
	for prefix, want := range map[string]string{
		"":      "rsync.unknown.dlq", // KAFKA_TOPIC_PREFIX unset: the default namespace
		"acme.": "acme.unknown.dlq",
	} {
		t.Run(want, func(t *testing.T) {
			if prefix == "" {
				// t.Setenv first so the original value is restored after the unset.
				t.Setenv("KAFKA_TOPIC_PREFIX", "")
				if err := os.Unsetenv("KAFKA_TOPIC_PREFIX"); err != nil {
					t.Fatal(err)
				}
			} else {
				t.Setenv("KAFKA_TOPIC_PREFIX", prefix)
			}
			resetDLQTopicsEnsured(t)
			b := &dlqTopicBroker{}
			w := dlqTopicWriter(b)
			defer w.Close()

			if err := parkOne(t, w, "  ", &Metrics{}); err != nil {
				t.Fatalf("sendToDLQ: %v", err)
			}
			events, _ := b.snapshot()
			if strings.Join(events, ",") != "create:"+want+",produce:"+want {
				t.Errorf("broker saw %v, want the record created and parked on %q", events, want)
			}
		})
	}
}

// setDLQCreateRetryInterval overrides the retry wait for one test.
func setDLQCreateRetryInterval(t *testing.T, d time.Duration) {
	t.Helper()
	prev := dlqTopicCreateRetryInterval
	dlqTopicCreateRetryInterval = d
	t.Cleanup(func() { dlqTopicCreateRetryInterval = prev })
}

// A create that fails because the broker is down also fails the write, so the topic
// is not auto-created. The next park after the retry wait must try the create again.
// Otherwise the topic is later born by auto-create, at the broker's partition count
// and retention, for the rest of the process.
func TestSendToDLQRetriesTheCreateAfterATransientFailure(t *testing.T) {
	resetDLQTopicsEnsured(t)
	setDLQCreateRetryInterval(t, 0)
	b := &dlqTopicBroker{createErr: errors.New("broker down"), produceErr: errors.New("broker down")}
	w := dlqTopicWriter(b)
	defer w.Close()
	const src = "rsync.cdc.retry.public.t"
	const dlq = src + ".dlq"

	captureLogLines(t, func() {
		if err := parkOne(t, w, src, &Metrics{}); err == nil {
			t.Fatal("sendToDLQ succeeded with the broker down")
		}
	})
	if n := b.createsFor(dlq); n != 1 {
		t.Fatalf("first park sent %d creates, want 1", n)
	}

	b.mu.Lock()
	b.createErr, b.produceErr = nil, nil
	b.mu.Unlock()

	if err := parkOne(t, w, src, &Metrics{}); err != nil {
		t.Fatalf("sendToDLQ after recovery: %v", err)
	}
	if n := b.createsFor(dlq); n != 2 {
		t.Fatalf("after recovery the create was sent %d times in total, want 2: a failed "+
			"create must not use up the topic's only attempt", n)
	}
	events, _ := b.snapshot()
	if events[len(events)-2] != "create:"+dlq || events[len(events)-1] != "produce:"+dlq {
		t.Errorf("broker saw %v, want the retried create before the write", events)
	}

	// Once it has succeeded, the create is never sent again.
	if err := parkOne(t, w, src, &Metrics{}); err != nil {
		t.Fatalf("sendToDLQ: %v", err)
	}
	if n := b.createsFor(dlq); n != 2 {
		t.Errorf("create sent %d times after a success, want it to stay at 2", n)
	}
}

// A refusal that does not go away (no CreateTopics ACL) must not cost a CreateTopics
// round trip and a warn line on every parked record. It is retried at most once per
// dlqTopicCreateRetryInterval.
func TestSendToDLQThrottlesTheCreateRetryAfterARefusal(t *testing.T) {
	resetDLQTopicsEnsured(t)
	setDLQCreateRetryInterval(t, time.Hour)
	b := &dlqTopicBroker{createErrorCode: int16(kafka.TopicAuthorizationFailed)}
	w := dlqTopicWriter(b)
	defer w.Close()
	const src = "rsync.cdc.refused.public.t"

	captureLogLines(t, func() {
		for i := 0; i < 5; i++ {
			if err := parkOne(t, w, src, &Metrics{}); err != nil {
				t.Fatalf("sendToDLQ: %v", err)
			}
		}
	})
	if n := b.createsFor(src + ".dlq"); n != 1 {
		t.Errorf("create sent %d times across 5 parks inside one retry interval, want 1", n)
	}
}

// A caller whose ctx was cancelled learned nothing about the broker, so its failed
// create must not start the retry wait for the next caller.
func TestSendToDLQCancelledCallerDoesNotDelayTheNextCreate(t *testing.T) {
	resetDLQTopicsEnsured(t)
	setDLQCreateRetryInterval(t, time.Hour)
	// The create fails while the caller's ctx is cancelled (the fake broker ignores
	// ctx, so the failure is injected), then the broker is healthy.
	b := &dlqTopicBroker{createErr: errors.New("context canceled")}
	w := dlqTopicWriter(b)
	defer w.Close()
	const src = "rsync.cdc.cancelled.public.t"
	const dlq = src + ".dlq"

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	captureLogLines(t, func() {
		ensureDLQTopic(ctx, w, dlq)
	})
	b.mu.Lock()
	b.createErr = nil
	b.mu.Unlock()
	before := b.createsFor(dlq)

	if err := parkOne(t, w, src, &Metrics{}); err != nil {
		t.Fatalf("sendToDLQ: %v", err)
	}
	if n := b.createsFor(dlq); n != before+1 {
		t.Errorf("the next park sent %d creates, want 1: a cancelled caller must not start the "+
			"retry wait", n-before)
	}
}
