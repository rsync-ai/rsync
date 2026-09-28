package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

// docStore is a MongoDB-shaped destination with the write semantics that matter
// for replay: import_data is insert_many — a document without _id gets a fresh
// one (so a replay inserts it again) and a duplicate _id is tolerated and not
// inserted — and upsert_data is ReplaceOne(upsert=True) on key_fields, defaulting
// to _id, skipping (by position) a document that lacks a key field. It records
// the tool and key_fields of every call.
type docStore struct {
	mu     sync.Mutex
	docs   []map[string]interface{}
	autoID int
	tools  []string
	keys   [][]string
}

func (s *docStore) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.docs)
}

func (s *docStore) RoundTrip(r *http.Request) (*http.Response, error) {
	var req struct {
		Params struct {
			Name      string `json:"name"`
			Arguments struct {
				Data      []map[string]interface{} `json:"data"`
				KeyFields []string                 `json:"key_fields"`
			} `json:"arguments"`
		} `json:"params"`
	}
	raw, _ := io.ReadAll(r.Body)
	_ = json.Unmarshal(raw, &req)
	name, data, keys := req.Params.Name, req.Params.Arguments.Data, req.Params.Arguments.KeyFields

	s.mu.Lock()
	s.tools = append(s.tools, name)
	s.keys = append(s.keys, keys)
	var result map[string]interface{}
	switch {
	case strings.HasSuffix(name, "_import_data"):
		inserted := 0
		for _, row := range data {
			if id, ok := row["_id"]; ok && s.findLocked([]string{"_id"}, map[string]interface{}{"_id": id}) >= 0 {
				continue // duplicate _id: Mongo rejects it, the connector tolerates it
			}
			doc := copyRow(row)
			if _, ok := doc["_id"]; !ok {
				s.autoID++
				doc["_id"] = fmt.Sprintf("auto-%d", s.autoID)
			}
			s.docs = append(s.docs, doc)
			inserted++
		}
		result = map[string]interface{}{"success": true, "rows_inserted": inserted}
	case strings.HasSuffix(name, "_upsert_data"):
		if len(keys) == 0 {
			keys = []string{"_id"}
		}
		var skipped []int
		for i, row := range data {
			missing := false
			for _, k := range keys {
				if _, ok := row[k]; !ok {
					missing = true
				}
			}
			if missing {
				skipped = append(skipped, i)
				continue
			}
			if at := s.findLocked(keys, row); at >= 0 {
				s.docs[at] = copyRow(row)
			} else {
				s.docs = append(s.docs, copyRow(row))
			}
		}
		result = map[string]interface{}{"success": true, "rows_upserted": len(data) - len(skipped)}
		if len(skipped) > 0 {
			result["skipped"] = len(skipped)
			result["skipped_indexes"] = skipped
		}
		if len(data) > 0 && len(skipped) == len(data) {
			result = map[string]interface{}{"success": false, "error": fmt.Sprintf("No records carried key field(s) %v for upsert", keys)}
		}
	default:
		result = map[string]interface{}{"success": false, "error": "tool not found: " + name}
	}
	s.mu.Unlock()

	body, _ := json.Marshal(map[string]interface{}{"jsonrpc": "2.0", "id": 1, "result": result})
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}},
		Body: io.NopCloser(strings.NewReader(string(body))), Request: r}, nil
}

func (s *docStore) findLocked(keys []string, row map[string]interface{}) int {
	for i, d := range s.docs {
		match := true
		for _, k := range keys {
			if fmt.Sprint(d[k]) != fmt.Sprint(row[k]) {
				match = false
				break
			}
		}
		if match {
			return i
		}
	}
	return -1
}

func copyRow(row map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(row))
	for k, v := range row {
		out[k] = v
	}
	return out
}

const keylessTopic = "cdc.public.events"

// keylessMessages builds one change per row on public.events, offsets 100.., with
// no Kafka message key (sm.KeyFields empty) — what Debezium emits for a table with
// no primary key.
func keylessMessages(rows []map[string]interface{}) ([]*SinkMessage, []kafka.Message) {
	var sms []*SinkMessage
	var msgs []kafka.Message
	for i, row := range rows {
		sms = append(sms, &SinkMessage{PipelineID: ackFKPipelineID, ExecutionID: ackFKExecID, Table: "public.events",
			StorageType: "cdc", RowCount: 1, CDCOp: "c", Data: []map[string]interface{}{copyRow(row)}})
		msgs = append(msgs, kafka.Message{Topic: keylessTopic, Partition: 0, Offset: int64(100 + i),
			Time: time.UnixMilli(1727400000000 + int64(i)), Value: []byte(`{"op":"c"}`)})
	}
	return sms, msgs
}

func keylessBatcher(t *testing.T, destType string, destCfg map[string]interface{}, dest http.RoundTripper) *cdcDBBatcher {
	t.Helper()
	c := newCDCTestCounters()
	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: []string{"127.0.0.1:1"}, Topic: keylessTopic})
	t.Cleanup(func() { reader.Close() })
	if destCfg == nil {
		destCfg = map[string]interface{}{}
	}
	return &cdcDBBatcher{
		cfg:          &WorkerConfig{PipelineID: ackFKPipelineID, SinkMode: "cdc", DestinationConnector: destType, DestinationConfig: destCfg},
		destType:     destType,
		destCfg:      destCfg,
		params:       cdcBatchingParams{maxEvents: 1000, maxRetries: 0, backoff: time.Millisecond},
		reader:       reader,
		hw:           newHighWaterTracker(),
		httpClient:   &http.Client{Transport: dest},
		ddl:          &DDLSupport{Enabled: false, resolved: true},
		eventsWriter: &kafka.Writer{},
		metrics:      c.metrics,
		cdcInserts:   c.inserts,
		cdcUpdates:   c.updates,
		cdcDeletes:   c.deletes,
		cdcBytes:     &sync.Map{},
		batches:      map[string]*cdcDBBatch{},
	}
}

// keylessWritePaths are the three ways a CDC upsert reaches a database
// destination: the batched flush, the per-row isolation that retries a failed
// batch, and the single-event lane (batching off). Each delivers the messages
// once, against a fresh process (fresh high-water tracker, as after a restart).
var keylessWritePaths = []struct {
	name    string
	deliver func(t *testing.T, destType string, destCfg map[string]interface{}, dest http.RoundTripper, sms []*SinkMessage, msgs []kafka.Message)
}{
	{"batched flush", func(t *testing.T, destType string, destCfg map[string]interface{}, dest http.RoundTripper, sms []*SinkMessage, msgs []kafka.Message) {
		b := keylessBatcher(t, destType, destCfg, dest)
		for i := range msgs {
			b.add(context.Background(), msgs[i], sms[i])
		}
		b.flushAll(context.Background())
	}},
	{"per-row isolation", func(t *testing.T, destType string, destCfg map[string]interface{}, dest http.RoundTripper, sms []*SinkMessage, msgs []kafka.Message) {
		b := keylessBatcher(t, destType, destCfg, dest)
		for i := range msgs {
			b.add(context.Background(), msgs[i], sms[i])
		}
		for key, batch := range b.batches {
			delete(b.batches, key)
			b.flushBatchPerRow(context.Background(), key, batch, "test", errors.New("batch write failed"))
		}
	}},
	{"single event (batching off)", func(t *testing.T, destType string, destCfg map[string]interface{}, dest http.RoundTripper, sms []*SinkMessage, msgs []kafka.Message) {
		singleEvents(t, destType, destCfg, dest, sms, msgs)
	}},
	{"append-only history (single event)", func(t *testing.T, destType string, destCfg map[string]interface{}, dest http.RoundTripper, sms []*SinkMessage, msgs []kafka.Message) {
		cfg := map[string]interface{}{"cdc_write_mode": "append"}
		for k, v := range destCfg {
			cfg[k] = v
		}
		singleEvents(t, destType, cfg, dest, sms, msgs)
	}},
}

func singleEvents(t *testing.T, destType string, destCfg map[string]interface{}, dest http.RoundTripper, sms []*SinkMessage, msgs []kafka.Message) {
	t.Helper()
	if destCfg == nil {
		destCfg = map[string]interface{}{}
	}
	cfg := &WorkerConfig{PipelineID: ackFKPipelineID, SinkMode: "cdc", DestinationConnector: destType, DestinationConfig: destCfg}
	c := newCDCTestCounters()
	for i := range msgs {
		if _, err := processCDCEvent(context.Background(), newHighWaterTracker(), nil, &http.Client{Transport: dest}, cfg,
			&DDLSupport{Enabled: false, resolved: true}, &kafka.Writer{}, nil, msgs[i], sms[i], c.metrics,
			c.inserts, c.updates, c.deletes, &sync.Map{}, time.Minute); err != nil {
			t.Fatalf("processCDCEvent offset %d: %v", msgs[i].Offset, err)
		}
	}
}

// KI-MONGODB-DEST-KEYLESS-REPLAY-DUPLICATES. Bug class: "a keyless CDC write to a
// document destination is not idempotent", so any redelivery — the crash between
// the write and its best-effort offset record, or any restart before the offset
// is saved — inserts the rows again. Every write path delivers the same three
// changes twice, the second time as a restarted worker that has no high-water
// mark; the collection must hold three documents. The fixture deliberately has
// no id / *_id column, so no key can be adopted from it.
func TestKeylessCDCRow_ReplayToDocumentDestinationIsIdempotent_EveryWritePath(t *testing.T) {
	for _, p := range keylessWritePaths {
		t.Run(p.name, func(t *testing.T) {
			captureFailClosed(t)
			store := &docStore{}
			rows := []map[string]interface{}{{"seq": 1, "label": "a"}, {"seq": 2, "label": "b"}, {"seq": 3, "label": "c"}}
			sms, msgs := keylessMessages(rows)
			p.deliver(t, "mongodb", nil, store, sms, msgs)
			if got := store.count(); got != 3 {
				t.Fatalf("first delivery: %d documents, want 3 (tools %v)", got, store.tools)
			}
			sms, msgs = keylessMessages(rows) // the same Kafka records, redelivered
			p.deliver(t, "mongodb", nil, store, sms, msgs)
			if got := store.count(); got != 3 {
				t.Errorf("after a replay of the same 3 Kafka records: %d documents, want 3 — the keyless write is not idempotent (tools %v)", got, store.tools)
			}
		})
	}
}

// The identity is the Kafka record's own coordinates, so two distinct records
// never share one and the same record always gets the same one.
func TestKeylessDocumentID_IsTheKafkaRecordsIdentity(t *testing.T) {
	m := kafka.Message{Topic: "cdc.public.events", Partition: 2, Offset: 41, Time: time.UnixMilli(1727400000123)}
	a := keylessDocumentID("pipe", m, 0)
	if b := keylessDocumentID("pipe", m, 0); a != b {
		t.Fatalf("same record, different ids: %q vs %q", a, b)
	}
	for name, other := range map[string]string{
		"offset":    keylessDocumentID("pipe", kafka.Message{Topic: m.Topic, Partition: 2, Offset: 42, Time: m.Time}, 0),
		"partition": keylessDocumentID("pipe", kafka.Message{Topic: m.Topic, Partition: 3, Offset: 41, Time: m.Time}, 0),
		"topic":     keylessDocumentID("pipe", kafka.Message{Topic: "cdc.public.other", Partition: 2, Offset: 41, Time: m.Time}, 0),
		// A topic recreated with offsets from 0 must not collide with the old records.
		"timestamp": keylessDocumentID("pipe", kafka.Message{Topic: m.Topic, Partition: 2, Offset: 41, Time: time.UnixMilli(1727400999999)}, 0),
		"pipeline":  keylessDocumentID("other", m, 0),
		"row index": keylessDocumentID("pipe", m, 1),
	} {
		if other == a {
			t.Errorf("records differing only by %s share the id %q", name, a)
		}
	}
	// A source column already named _id is the document's identity; it is kept.
	row := map[string]interface{}{"_id": "src", "v": 1}
	if got := withKeylessDocumentID(row, "pipe", m, 0); got["_id"] != "src" {
		t.Errorf("source _id overwritten: %v", got["_id"])
	}
	fresh := map[string]interface{}{"v": 1}
	if got := withKeylessDocumentID(fresh, "pipe", m, 0); got["_id"] != a {
		t.Errorf("_id = %v, want %q", got["_id"], a)
	}
	if _, ok := fresh["_id"]; ok {
		t.Error("the caller's row map was mutated")
	}
}
