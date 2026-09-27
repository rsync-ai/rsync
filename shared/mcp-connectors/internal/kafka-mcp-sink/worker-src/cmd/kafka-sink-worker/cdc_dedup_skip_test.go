package main

// The dedup hot-path in cdcDBBatcher.add is what makes a sink restart safe: the
// destination's durable _rsync_cdc_offsets high-water mark is seeded into the
// tracker at startup, and any redelivered offset at or below it must be dropped
// and committed instead of written a second time. Nothing asserted that skip —
// highWaterTracker.seen had coverage, but not the batcher path that consumes it,
// so a regression there would have shipped silently as duplicate rows.
//
// Every assertion here is paired with a control that makes it able to fail:
// offset 101 (above the mark) must still be buffered, so a batcher that dropped
// everything, or one whose comparison was inverted, fails just as loudly as one
// that dropped nothing.

import (
	"context"
	"database/sql"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

// dedupBatcher builds a cdcDBBatcher whose tracker already holds `mark` as the
// durable high-water offset for (topic, partition 0) — the state a worker is in
// immediately after seeding from the destination on startup.
func dedupBatcher(t *testing.T, topic string, mark int64) *cdcDBBatcher {
	t.Helper()
	captureFailClosed(t)
	// Unreachable broker: add() ignores the CommitMessages error, and no test
	// here depends on the commit landing — only on the message not being buffered.
	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: []string{"127.0.0.1:1"}, Topic: topic})
	t.Cleanup(func() { reader.Close() })

	hw := newHighWaterTracker()
	hw.seed(map[string]int64{hwKey(topic, 0): mark})

	return &cdcDBBatcher{
		cfg:          &WorkerConfig{PipelineID: "p-dedup", SinkMode: "cdc"},
		destType:     "mongodb",
		destCfg:      map[string]interface{}{},
		params:       cdcBatchingParams{enabled: true, maxEvents: 100, maxRetries: 0, backoff: time.Millisecond},
		reader:       reader,
		hw:           hw,
		pgDB:         (*sql.DB)(nil),
		httpClient:   &http.Client{},
		ddl:          &DDLSupport{},
		eventsWriter: &kafka.Writer{},
		metrics:      &Metrics{},
		cdcInserts:   &sync.Map{},
		cdcUpdates:   &sync.Map{},
		cdcDeletes:   &sync.Map{},
		cdcBytes:     &sync.Map{},
		batches:      map[string]*cdcDBBatch{},
	}
}

func dedupEvent(topic string, offset int64) (kafka.Message, *SinkMessage) {
	msg := kafka.Message{Topic: topic, Partition: 0, Offset: offset}
	sm := &SinkMessage{
		PipelineID: "p-dedup",
		IsCDC:      true,
		CDCOp:      "c",
		Table:      "public.orders",
		Data:       []map[string]interface{}{{"id": offset, "name": "row"}},
	}
	return msg, sm
}

func bufferedRows(b *cdcDBBatcher) int {
	n := 0
	for _, batch := range b.batches {
		n += len(batch.rows)
	}
	return n
}

func TestCDCDBBatcherSkipsRedeliveredOffsetsAtOrBelowTheHighWaterMark(t *testing.T) {
	const topic = "cdc.shop.orders"
	ctx := context.Background()

	for _, tc := range []struct {
		name    string
		offset  int64
		skipped bool // want: dropped as a duplicate rather than buffered
	}{
		// Strictly below the durable mark — written before the crash.
		{"below the mark", 99, true},
		// Equal to it. `seen` is `offset <= h`: the mark names an offset that IS
		// durable, so re-writing it would duplicate that row. Guards the off-by-one.
		{"exactly at the mark", 100, true},
		// The control. Above the mark ⇒ never written ⇒ must still reach the batch.
		// Without this case a batcher that skipped every message would pass.
		{"above the mark", 101, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := dedupBatcher(t, topic, 100)
			msg, sm := dedupEvent(topic, tc.offset)
			b.add(ctx, msg, sm)

			gotSkipped := atomic.LoadUint64(&b.metrics.skipped)
			gotRows := bufferedRows(b)

			if tc.skipped {
				if gotSkipped != 1 {
					t.Errorf("offset %d is at/below the high-water mark 100: skipped counter = %d, want 1", tc.offset, gotSkipped)
				}
				if gotRows != 0 {
					t.Errorf("offset %d was buffered for a second write to the destination: %d rows held, want 0", tc.offset, gotRows)
				}
				return
			}
			if gotSkipped != 0 {
				t.Errorf("offset %d is above the high-water mark 100 and was never written: skipped counter = %d, want 0", tc.offset, gotSkipped)
			}
			if gotRows != 1 {
				t.Errorf("offset %d is above the high-water mark 100 and must be written: %d rows buffered, want 1", tc.offset, gotRows)
			}
		})
	}
}

// A seeded mark covers only its own (topic, partition). A redelivery on a
// partition the destination never reported must NOT be dropped — dropping it
// would lose data outright, which is the worse failure of the two.
func TestCDCDBBatcherDedupIsScopedToOneTopicPartition(t *testing.T) {
	const topic = "cdc.shop.orders"
	ctx := context.Background()
	b := dedupBatcher(t, topic, 100)

	// Same offset as the seeded mark, but partition 1 — unseeded.
	msg := kafka.Message{Topic: topic, Partition: 1, Offset: 100}
	_, sm := dedupEvent(topic, 100)
	b.add(ctx, msg, sm)

	if got := atomic.LoadUint64(&b.metrics.skipped); got != 0 {
		t.Errorf("partition 1 has no seeded high-water mark, so nothing about it is durable: skipped = %d, want 0", got)
	}
	if got := bufferedRows(b); got != 1 {
		t.Errorf("partition 1's event was dropped using partition 0's mark: %d rows buffered, want 1", got)
	}

	// Control: the same offset on partition 0 IS covered, proving the seed is live
	// and this test is not passing merely because dedup is off everywhere.
	b2 := dedupBatcher(t, topic, 100)
	msg0, sm0 := dedupEvent(topic, 100)
	b2.add(ctx, msg0, sm0)
	if got := atomic.LoadUint64(&b2.metrics.skipped); got != 1 {
		t.Fatalf("control: partition 0 offset 100 must be deduped, skipped = %d, want 1", got)
	}
}
