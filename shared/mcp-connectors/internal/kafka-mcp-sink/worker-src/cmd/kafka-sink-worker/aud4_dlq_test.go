package main

// AUD-4 integration test: a persistent destination failure on the batched CDC path
// (cdcDBBatcher.flushBatch) must route every message in the batch to the DLQ, commit
// offsets, and RETURN — not os.Exit(1). Mirrors the single-event processCDCEvent path.
//
// Requires a live kafka broker; env-guarded so normal `go test`/CI skips it.
//   AUD4_LIVE_KAFKA=1 AUD4_BROKER=kafka:29092 go test ./cmd/kafka-sink-worker -run TestAUD4 -v

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

// failRT makes every destination MCP call fail persistently, regardless of URL.
type failRT struct{}

func (failRT) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("aud4: simulated persistent destination failure")
}

func TestAUD4DLQRouting(t *testing.T) {
	if os.Getenv("AUD4_LIVE_KAFKA") == "" {
		t.Skip("set AUD4_LIVE_KAFKA=1 (and AUD4_BROKER) to run against a live kafka")
	}
	broker := os.Getenv("AUD4_BROKER")
	if broker == "" {
		broker = "kafka:29092"
	}
	ctx := context.Background()

	topic := fmt.Sprintf("aud4test_%d", os.Getpid())
	dlqTopic := topic + ".dlq"

	dlqWriter := &kafka.Writer{
		Addr:                   kafka.TCP(broker),
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: true,
	}
	defer dlqWriter.Close()

	// Placeholder reader: flushBatch calls CommitMessages on it. Without a GroupID this
	// returns an ignored error (the code does `_ = b.reader.CommitMessages(...)`); we
	// never Read from it, so no broker round-trip is needed here.
	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: []string{broker}, Topic: topic})
	defer reader.Close()

	metrics := &Metrics{}
	b := &cdcDBBatcher{
		cfg: &WorkerConfig{
			PipelineID:           "p-aud4",
			DestinationConnector: "postgresql",
			DestinationConfig:    map[string]interface{}{},
		},
		destType:   "postgresql",
		destCfg:    map[string]interface{}{},
		params:     cdcBatchingParams{enabled: true, maxRetries: 1, backoff: time.Millisecond},
		reader:     reader,
		hw:         newHighWaterTracker(),
		httpClient: &http.Client{Transport: failRT{}, Timeout: 2 * time.Second},
		dlqWriter:  dlqWriter,
		metrics:    metrics,
		batches:    map[string]*cdcDBBatch{},
	}

	msgs := []kafka.Message{
		{Topic: topic, Partition: 0, Offset: 10, Key: []byte("k1"), Value: []byte(`{"op":"c","id":1}`)},
		{Topic: topic, Partition: 0, Offset: 11, Key: []byte("k2"), Value: []byte(`{"op":"u","id":2}`)},
		{Topic: topic, Partition: 0, Offset: 12, Key: []byte("k3"), Value: []byte(`{"op":"u","id":3}`)},
	}
	sms := []*SinkMessage{{CDCOp: "c", Table: "t"}, {CDCOp: "u", Table: "t"}, {CDCOp: "u", Table: "t"}}
	rows := []map[string]interface{}{{"id": 1}, {"id": 2}, {"id": 3}}

	batch := &cdcDBBatch{
		table: "t", targetTable: "t", topic: topic, partition: 0,
		rows: rows, messages: msgs, sms: sms,
		keyFields:   []string{"id"},
		columnTypes: map[string]string{"id": "integer"},
		firstOffset: 10, lastOffset: 12,
	}
	key := "topic|0|t"
	b.batches[key] = batch

	// If flushBatch wrongly took the no-DLQ fail-closed path, os.Exit(1) would kill the
	// test binary here (reported as a failure). Reaching the asserts == it returned.
	b.flushBatch(ctx, key, batch, "aud4-test")

	if got := atomic.LoadUint64(&metrics.dlqRouted); got != uint64(len(msgs)) {
		t.Fatalf("metrics.dlqRouted = %d, want %d", got, len(msgs))
	}
	if _, ok := b.batches[key]; ok {
		t.Fatalf("batch was not deleted from b.batches after DLQ routing")
	}
	if hw := atomic.LoadInt64(&metrics.lastCommittedOffset); hw != 12 {
		t.Fatalf("lastCommittedOffset = %d, want 12 (offset advanced past failed batch)", hw)
	}

	// Confirm the messages actually landed in <topic>.dlq. GroupID reader so we consume
	// across whatever partition count the auto-created topic has.
	dlqReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{broker},
		Topic:       dlqTopic,
		GroupID:     "aud4-verify-" + fmt.Sprint(os.Getpid()),
		StartOffset: kafka.FirstOffset,
	})
	defer dlqReader.Close()

	rctx, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()
	seen := 0
	for seen < len(msgs) {
		m, err := dlqReader.ReadMessage(rctx)
		if err != nil {
			t.Fatalf("reading DLQ topic %s: %v (saw %d/%d)", dlqTopic, err, seen, len(msgs))
		}
		seen++
		t.Logf("  DLQ msg %d: key=%s bytes=%d", seen, string(m.Key), len(m.Value))
	}
	t.Logf("AUD-4 PASS: %d messages routed to %s, dlqRouted=%d, offset advanced to 12, no os.Exit",
		seen, dlqTopic, atomic.LoadUint64(&metrics.dlqRouted))
}
