package main

// The batched CDC apply path's two guarantees to a document destination
// (document_batch_apply.go): one write per key per batch, by its last event (#22),
// and every row the destination skips parked in the DLQ and counted there (#24).

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
	metadataAPI "github.com/segmentio/kafka-go/protocol/metadata"
	produceAPI "github.com/segmentio/kafka-go/protocol/produce"
)

// The reported sequence, c(k1,v1) u(k1,v2) d(k2) c(k2,v3): each key is written once,
// by its last event, and the survivors keep offset order.
func TestLastEventPerKey_LastEventWinsInOffsetOrder(t *testing.T) {
	rows := []map[string]interface{}{
		{"id": "k1", "v": "v1"}, // c
		{"id": "k1", "v": "v2"}, // u
		{"id": "k2"},            // d (its key image)
		{"id": "k2", "v": "v3"}, // c
	}
	got := lastEventPerKey(rows, []string{"id"})
	if want := []int{1, 3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("lastEventPerKey = %v, want %v (u(k1,v2), c(k2,v3))", got, want)
	}
}

func TestLastEventPerKey_LeavesUnkeyableRowsAlone(t *testing.T) {
	distinct := []map[string]interface{}{{"id": 1}, {"id": 2}, {"id": 3}}
	if got := lastEventPerKey(distinct, []string{"id"}); got != nil {
		t.Errorf("no repeated key: got %v, want nil (write every row)", got)
	}
	if got := lastEventPerKey([]map[string]interface{}{{"id": 1}, {"id": 1}}, nil); got != nil {
		t.Errorf("no key fields: got %v, want nil", got)
	}
	// A row whose key is missing or null is never merged with anything — not with
	// another keyless row, not with a keyed one — so the destination still sees it
	// and reports it skipped.
	rows := []map[string]interface{}{
		{"v": "a"},            // 0: no key
		{"id": 1, "v": "b"},   // 1: superseded by 4
		{"id": nil, "v": "c"}, // 2: null key
		{"v": "d"},            // 3: no key
		{"id": 1, "v": "e"},   // 4
	}
	if got, want := lastEventPerKey(rows, []string{"id"}), []int{0, 2, 3, 4}; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	// A composite key merges only on the whole tuple, and "1" is not 1.
	comp := []map[string]interface{}{
		{"a": 1, "b": 1}, {"a": 1, "b": 2}, {"a": 1, "b": 1}, {"a": "1", "b": 1},
	}
	if got, want := lastEventPerKey(comp, []string{"a", "b"}), []int{1, 2, 3}; !reflect.DeepEqual(got, want) {
		t.Errorf("composite: got %v, want %v", got, want)
	}
}

func TestDestSkippedRows(t *testing.T) {
	idx := func(v ...float64) []interface{} {
		out := make([]interface{}, len(v))
		for i, f := range v {
			out[i] = f
		}
		return out
	}
	cases := []struct {
		name    string
		res     map[string]interface{}
		sent    []int
		want    []int
		wantOK  bool
		batchLn int
	}{
		{"nothing skipped", map[string]interface{}{"rows_upserted": 3.0}, nil, nil, true, 3},
		{"skipped rows named", map[string]interface{}{"skipped": 2.0, "skipped_indexes": idx(2, 0)}, nil, []int{0, 2}, true, 3},
		{"mapped back through sent", map[string]interface{}{"skipped": 1.0, "skipped_indexes": idx(0)}, []int{1, 2, 3}, []int{1}, true, 4},
		{"count without indexes", map[string]interface{}{"skipped": 1.0}, nil, nil, false, 3},
		{"count disagrees", map[string]interface{}{"skipped": 2.0, "skipped_indexes": idx(1)}, nil, nil, false, 3},
		{"index past what was sent", map[string]interface{}{"skipped": 1.0, "skipped_indexes": idx(3)}, []int{1, 2, 3}, nil, false, 4},
		{"negative index", map[string]interface{}{"skipped": 1.0, "skipped_indexes": idx(-1)}, nil, nil, false, 3},
		{"fractional index", map[string]interface{}{"skipped": 1.0, "skipped_indexes": idx(0.5)}, nil, nil, false, 3},
		{"repeated index", map[string]interface{}{"skipped": 2.0, "skipped_indexes": idx(1, 1)}, nil, nil, false, 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := destSkippedRows(tc.res, tc.sent, tc.batchLn)
			if ok != tc.wantOK || !reflect.DeepEqual(got, tc.want) {
				t.Errorf("destSkippedRows = %v, %v; want %v, %v", got, ok, tc.want, tc.wantOK)
			}
		})
	}
}

// mongoLikeDestination answers upsert_data the way the MongoDB connector does: a
// document missing a key field is skipped and named by position in skipped_indexes,
// the rest land, and a call where nothing carried a key fails. It records every
// data array it was sent.
type mongoLikeDestination struct {
	mu   sync.Mutex
	sent [][]map[string]interface{}
}

func (d *mongoLikeDestination) RoundTrip(r *http.Request) (*http.Response, error) {
	var req struct {
		Params struct {
			Arguments struct {
				Data      []map[string]interface{} `json:"data"`
				KeyFields []string                 `json:"key_fields"`
			} `json:"arguments"`
		} `json:"params"`
	}
	raw, _ := io.ReadAll(r.Body)
	_ = json.Unmarshal(raw, &req)
	data, keys := req.Params.Arguments.Data, req.Params.Arguments.KeyFields
	d.mu.Lock()
	d.sent = append(d.sent, data)
	d.mu.Unlock()

	var skipped []int
	for i, row := range data {
		for _, k := range keys {
			if _, ok := row[k]; !ok {
				skipped = append(skipped, i)
				break
			}
		}
	}
	result := map[string]interface{}{"success": true, "rows_upserted": len(data) - len(skipped)}
	if len(skipped) > 0 {
		result["skipped"] = len(skipped)
		result["skipped_indexes"] = skipped
	}
	if len(data) > 0 && len(skipped) == len(data) {
		result = map[string]interface{}{"success": false, "error": fmt.Sprintf("No records carried key field(s) %v for upsert", keys)}
	}
	body, _ := json.Marshal(map[string]interface{}{"jsonrpc": "2.0", "id": 1, "result": result})
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(string(body))),
		Request:    r,
	}, nil
}

// dlqCapture is a Kafka broker that accepts every produce and keeps the payloads, so
// a test can see what sendToDLQ published.
type dlqCapture struct {
	mu       sync.Mutex
	topics   []string
	payloads []map[string]interface{}
}

func (c *dlqCapture) RoundTrip(_ context.Context, _ net.Addr, m protocol.Message) (protocol.Message, error) {
	switch req := m.(type) {
	case *metadataAPI.Request:
		res := &metadataAPI.Response{}
		for _, name := range req.TopicNames {
			res.Topics = append(res.Topics, metadataAPI.ResponseTopic{Name: name,
				Partitions: []metadataAPI.ResponsePartition{{PartitionIndex: 0}}})
		}
		return res, nil
	case *produceAPI.Request:
		res := &produceAPI.Response{}
		for _, tp := range req.Topics {
			rt := produceAPI.ResponseTopic{Topic: tp.Topic}
			for _, p := range tp.Partitions {
				for {
					rec, err := p.RecordSet.Records.ReadRecord()
					if err != nil {
						break
					}
					v, _ := protocol.ReadAll(rec.Value)
					var payload map[string]interface{}
					_ = json.Unmarshal(v, &payload)
					c.mu.Lock()
					c.topics = append(c.topics, tp.Topic)
					c.payloads = append(c.payloads, payload)
					c.mu.Unlock()
				}
				rt.Partitions = append(rt.Partitions, produceAPI.ResponsePartition{Partition: p.Partition})
			}
			res.Topics = append(res.Topics, rt)
		}
		return res, nil
	}
	return nil, fmt.Errorf("unexpected kafka request %T", m)
}

// docBatchFixture is one PostgreSQL → MongoDB batch on public.orders, partition 0,
// offsets 10.., one row per op. A row value carries "secret-" so a test can prove
// no log line or DLQ reason repeats it.
func docBatchFixture(t *testing.T, destType string, dest http.RoundTripper, dlq *dlqCapture, ops []string, rows []map[string]interface{}) (*cdcDBBatcher, *cdcDBBatch, cdcTestCounters) {
	t.Helper()
	c := newCDCTestCounters()
	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: []string{"127.0.0.1:1"}, Topic: "cdc.public.orders"})
	t.Cleanup(func() { reader.Close() })
	b := &cdcDBBatcher{
		cfg:          &WorkerConfig{PipelineID: ackFKPipelineID, SinkMode: "cdc"},
		destType:     destType,
		destCfg:      map[string]interface{}{},
		params:       cdcBatchingParams{maxRetries: 0, backoff: time.Millisecond},
		reader:       reader,
		hw:           newHighWaterTracker(),
		httpClient:   &http.Client{Transport: dest},
		ddl:          &DDLSupport{},
		eventsWriter: &kafka.Writer{},
		metrics:      c.metrics,
		cdcInserts:   c.inserts,
		cdcUpdates:   c.updates,
		cdcDeletes:   c.deletes,
		cdcBytes:     &sync.Map{},
		batches:      map[string]*cdcDBBatch{},
	}
	if dlq != nil {
		w := &kafka.Writer{Addr: kafka.TCP("127.0.0.1:1"), Transport: dlq, BatchSize: 1, RequiredAcks: kafka.RequireOne}
		t.Cleanup(func() { w.Close() })
		b.dlqWriter = w
	}
	batch := &cdcDBBatch{table: "public.orders", targetTable: "orders", topic: "cdc.public.orders", keyFields: []string{"id"}, rows: rows}
	for i, op := range ops {
		batch.sms = append(batch.sms, &SinkMessage{PipelineID: ackFKPipelineID, ExecutionID: ackFKExecID,
			Table: "public.orders", StorageType: "cdc", RowCount: 1, CDCOp: op})
		batch.messages = append(batch.messages, kafka.Message{Topic: "cdc.public.orders", Offset: int64(10 + i),
			Value: []byte(fmt.Sprintf(`{"op":%q}`, op))})
	}
	batch.firstOffset, batch.lastOffset = 10, int64(10+len(ops)-1)
	return b, batch, c
}

func flushDocBatch(t *testing.T, b *cdcDBBatcher, batch *cdcDBBatch) {
	t.Helper()
	b.batches["k"] = batch
	b.submitFlush(context.Background(), "k", batch, "test_flush")
}

// #22 end to end: MongoDB is sent each key once, by its last event, while every
// message is still counted and committed — a superseded event was applied by its
// successor.
func TestCDCDBBatch_DocumentDestinationGetsLastEventPerKey(t *testing.T) {
	captureFailClosed(t)
	dest := &mongoLikeDestination{}
	b, batch, c := docBatchFixture(t, "mongodb", dest, nil, []string{"c", "u", "c", "u"}, []map[string]interface{}{
		{"id": 1, "v": "v1"}, {"id": 1, "v": "v2"}, {"id": 2, "v": "v3"}, {"id": 2, "v": "v4"},
	})
	flushDocBatch(t, b, batch)

	if len(dest.sent) != 1 {
		t.Fatalf("destination calls = %d, want 1", len(dest.sent))
	}
	var got []string
	for _, row := range dest.sent[0] {
		got = append(got, fmt.Sprint(row["id"], "=", row["v"]))
	}
	if want := []string{"1=v2", "2=v4"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("sent %v, want %v", got, want)
	}
	if ins, upd := loadCounter(c.inserts, "public.orders"), loadCounter(c.updates, "public.orders"); ins != 2 || upd != 2 {
		t.Errorf("inserts=%d updates=%d, want 2 and 2: every event was applied", ins, upd)
	}
	if !b.hw.seen("cdc.public.orders", 0, 13) {
		t.Error("the batch's last offset was not committed")
	}
}

// #24: a row MongoDB skips for want of a key goes to the DLQ with a reason that
// names columns, not values, is counted in dlq_rows and NOT as applied, and the
// batch still commits.
func TestCDCDBBatch_SkippedDocumentRowIsDeadLettered(t *testing.T) {
	captureFailClosed(t)
	dest, dlq := &mongoLikeDestination{}, &dlqCapture{}
	// Row 0 is superseded by row 2, so the destination is sent rows 1-3 and names its
	// own position 0 (batch row 1, offset 11) — the mapping back must hold.
	b, batch, c := docBatchFixture(t, "mongodb", dest, dlq, []string{"c", "c", "u", "c"}, []map[string]interface{}{
		{"id": 1, "v": "secret-a"}, {"v": "secret-keyless"}, {"id": 1, "v": "secret-b"}, {"id": 2, "v": "secret-c"},
	})
	flushDocBatch(t, b, batch)

	if len(dest.sent) != 1 || len(dest.sent[0]) != 3 {
		t.Fatalf("destination was sent %v, want one call of 3 rows", dest.sent)
	}
	if len(dlq.payloads) != 1 {
		t.Fatalf("DLQ messages = %d, want 1", len(dlq.payloads))
	}
	p := dlq.payloads[0]
	if dlq.topics[0] != "cdc.public.orders.dlq" || p["offset"] != float64(11) {
		t.Errorf("dead-lettered %s offset %v, want cdc.public.orders.dlq offset 11", dlq.topics[0], p["offset"])
	}
	reason, _ := p["error"].(string)
	if !strings.Contains(reason, "missing primary key") || !strings.Contains(reason, "key_fields=id") {
		t.Errorf("DLQ reason %q does not say which key was missing", reason)
	}
	if strings.Contains(reason, "secret") {
		t.Errorf("DLQ reason carries a row value: %q", reason)
	}
	if got := loadCounter(&c.metrics.dlqByTable, "public.orders"); got != 1 {
		t.Errorf("dlq_rows = %d, want 1", got)
	}
	if ins, upd := loadCounter(c.inserts, "public.orders"), loadCounter(c.updates, "public.orders"); ins != 2 || upd != 1 {
		t.Errorf("inserts=%d updates=%d, want 2 and 1: the skipped insert did not land", ins, upd)
	}
	if got := c.metrics.processed; got != 3 {
		t.Errorf("processed = %d, want 3", got)
	}
	if !b.hw.seen("cdc.public.orders", 0, 13) {
		t.Error("the batch's last offset was not committed")
	}
}

// Without a DLQ a skipped row has nowhere to go: fail closed, nothing committed.
func TestCDCDBBatch_SkippedDocumentRowWithoutDLQFailsClosed(t *testing.T) {
	msg := captureFailClosed(t)
	b, batch, c := docBatchFixture(t, "mongodb", &mongoLikeDestination{}, nil, []string{"c", "c"}, []map[string]interface{}{
		{"id": 1, "v": "secret-a"}, {"v": "secret-keyless"},
	})
	func() {
		defer func() {
			if _, ok := recover().(failClosedPanic); !ok {
				t.Fatal("a skipped row with no DLQ did not fail closed")
			}
		}()
		flushDocBatch(t, b, batch)
	}()
	if !strings.Contains(*msg, "no DLQ") || strings.Contains(*msg, "secret") {
		t.Errorf("fail-closed message %q: want it to name the missing DLQ and carry no row value", *msg)
	}
	if b.hw.seen("cdc.public.orders", 0, 10) || loadCounter(c.inserts, "public.orders") != 0 {
		t.Error("a batch that failed closed committed or counted rows")
	}
}

// A relational destination is untouched by both changes: it gets every row, and a
// "skipped" in its result is not read.
func TestCDCDBBatch_RelationalDestinationIgnoresDocumentRules(t *testing.T) {
	captureFailClosed(t)
	b, batch, c := docBatchFixture(t, "postgresql", nil, nil, []string{"c", "u"}, []map[string]interface{}{
		{"id": 1, "v": "a"}, {"id": 1, "v": "b"},
	})
	b.landFlushedBatch(context.Background(), "k", batch,
		map[string]interface{}{"rows_upserted": 2.0, "skipped": 1.0, "skipped_indexes": []interface{}{0.0}}, nil, "test_flush")
	if ins, upd := loadCounter(c.inserts, "public.orders"), loadCounter(c.updates, "public.orders"); ins != 1 || upd != 1 {
		t.Errorf("inserts=%d updates=%d, want 1 and 1", ins, upd)
	}
	if !b.hw.seen("cdc.public.orders", 0, 11) {
		t.Error("the batch was not committed")
	}
}

// skippingDeleteDestination answers every delete_data the way the MongoDB connector
// does when the record carries no key it can match on.
type skippingDeleteDestination struct{}

func (skippingDeleteDestination) RoundTrip(r *http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}},
		Body:    io.NopCloser(strings.NewReader(`{"jsonrpc":"2.0","id":1,"result":{"success":true,"rows_deleted":0,"skipped":1,"skipped_indexes":[0]}}`)),
		Request: r}, nil
}

// The single-event lane (every CDC delete, and upserts when batching is off): a
// document destination that skips the row makes it poison, so the consume loop
// dead-letters and commits it instead of counting it applied.
func TestProcessCDCEvent_SkippedDocumentRowIsPoison(t *testing.T) {
	skipAll := skippingDeleteDestination{}
	c := newCDCTestCounters()
	sms, msgs := ledgerTestMessages(5, 6, "d")
	sms[0].PK = map[string]interface{}{"id": "secret-7"}
	sms[0].KeyFields = []string{"id"}
	cfg := &WorkerConfig{PipelineID: ackFKPipelineID, SinkMode: "cdc", DestinationConnector: "mongodb", DestinationConfig: map[string]interface{}{}}
	commit, err := processCDCEvent(context.Background(), newHighWaterTracker(), nil, &http.Client{Transport: skipAll}, cfg,
		&DDLSupport{Enabled: false, resolved: true}, &kafka.Writer{}, nil, msgs[0], sms[0], c.metrics,
		c.inserts, c.updates, c.deletes, &sync.Map{}, time.Minute)
	if commit || !isPoison(err) {
		t.Fatalf("commit=%v err=%v, want a poison error so the row is dead-lettered", commit, err)
	}
	if !strings.Contains(err.Error(), "missing primary key") || strings.Contains(err.Error(), "secret") {
		t.Errorf("reason %q: want it to name the missing key and carry no row value", err)
	}
	if got := loadCounter(c.deletes, "public.orders"); got != 0 {
		t.Errorf("deletes = %d, want 0: the skipped delete did not land", got)
	}
}
