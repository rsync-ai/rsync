package main

import (
	"encoding/json"
	"testing"

	"github.com/segmentio/kafka-go"
)

// cdcEnvelopeWithFields builds a schema-enabled Debezium (Kafka Connect
// JsonConverter) envelope whose before/after structs carry the supplied field
// schemas. Unlike wrapCDCEnvelope it lets a test declare logical-type names
// (e.g. io.debezium.time.Timestamp) and non-string base types so the relational
// value-decode path (coerceCDCRowValues) is exercised for real.
func cdcEnvelopeWithFields(op string, after map[string]interface{}, fields []interface{}) []byte {
	schema := map[string]interface{}{
		"type": "struct",
		"fields": []interface{}{
			map[string]interface{}{"field": "before", "type": "struct", "optional": true, "fields": fields},
			map[string]interface{}{"field": "after", "type": "struct", "optional": true, "fields": fields},
			map[string]interface{}{"field": "source", "type": "struct", "optional": true, "fields": []interface{}{}},
			map[string]interface{}{"field": "op", "type": "string", "optional": false},
			map[string]interface{}{"field": "ts_ms", "type": "int64", "optional": true},
		},
	}
	payload := map[string]interface{}{
		"op": op,
		"source": map[string]interface{}{
			"db":    "mydb",
			"table": "edge",
		},
		"before": nil,
		"after":  after,
		"ts_ms":  int64(1704067200000),
	}
	b, _ := json.Marshal(map[string]interface{}{"schema": schema, "payload": payload})
	return b
}

// TestCoerceCDCRowValues_EpochZeroTimestamp proves that a Debezium
// io.debezium.time.Timestamp of exactly epoch 0 (1970-01-01 00:00:00.000) is
// converted to a datetime literal instead of being left as a raw integer. The
// old `if n > 0` guard skipped n==0, leaking a bare 0 into a `timestamp`
// destination column -> "is of type timestamp ... but expression is of type
// integer" -> whole batch DLQ'd -> zero rows land.
func TestCoerceCDCRowValues_EpochZeroTimestamp(t *testing.T) {
	cfg := &WorkerConfig{
		PipelineID:           "test-pipeline",
		Topic:                "dbserver.mydb.edge",
		SinkMode:             "cdc",
		DestinationConnector: "postgresql",
	}
	fields := []interface{}{
		map[string]interface{}{"field": "id", "type": "int32", "optional": false},
		map[string]interface{}{"field": "ts", "type": "int64", "optional": true, "name": "io.debezium.time.Timestamp"},
	}
	raw := cdcEnvelopeWithFields("c", map[string]interface{}{
		"id": 1,
		"ts": int64(0), // epoch 0 — a valid instant, not "absent"
	}, fields)

	sm, err := parseSinkMessage(cfg, kafka.Message{Topic: "dbserver.mydb.edge", Value: raw, Offset: 1})
	if err != nil {
		t.Fatalf("parseSinkMessage: %v", err)
	}
	got, ok := sm.After["ts"].(string)
	if !ok {
		t.Fatalf("epoch-0 timestamp not converted: want string datetime literal, got %T (%#v)", sm.After["ts"], sm.After["ts"])
	}
	if got != "1970-01-01T00:00:00Z" {
		t.Fatalf("epoch-0 timestamp: got %q, want %q", got, "1970-01-01T00:00:00Z")
	}
}

// TestCoerceCDCRowValues_PreEpochTimestamp proves a pre-1970 timestamp (negative
// ms since epoch) is also converted rather than left raw.
func TestCoerceCDCRowValues_PreEpochTimestamp(t *testing.T) {
	cfg := &WorkerConfig{
		PipelineID:           "test-pipeline",
		Topic:                "dbserver.mydb.edge",
		SinkMode:             "cdc",
		DestinationConnector: "postgresql",
	}
	fields := []interface{}{
		map[string]interface{}{"field": "id", "type": "int32", "optional": false},
		map[string]interface{}{"field": "ts", "type": "int64", "optional": true, "name": "io.debezium.time.Timestamp"},
	}
	raw := cdcEnvelopeWithFields("c", map[string]interface{}{
		"id": 1,
		"ts": int64(-86400000), // 1969-12-31 00:00:00.000 UTC
	}, fields)

	sm, err := parseSinkMessage(cfg, kafka.Message{Topic: "dbserver.mydb.edge", Value: raw, Offset: 1})
	if err != nil {
		t.Fatalf("parseSinkMessage: %v", err)
	}
	got, ok := sm.After["ts"].(string)
	if !ok {
		t.Fatalf("pre-1970 timestamp not converted: got %T (%#v)", sm.After["ts"], sm.After["ts"])
	}
	if got != "1969-12-31T00:00:00Z" {
		t.Fatalf("pre-1970 timestamp: got %q, want %q", got, "1969-12-31T00:00:00Z")
	}
}

// TestCoerceCDCRowValues_EpochZeroDateAndMidnightTime confirms the adjacent Date
// and Time branches already treat 0 as a valid instant (a DATE of 1970-01-01 is
// 0 days; midnight is 0). Guards the fix against a regression that would re-break
// them.
func TestCoerceCDCRowValues_EpochZeroDateAndMidnightTime(t *testing.T) {
	cfg := &WorkerConfig{
		PipelineID:           "test-pipeline",
		Topic:                "dbserver.mydb.edge",
		SinkMode:             "cdc",
		DestinationConnector: "postgresql",
	}
	fields := []interface{}{
		map[string]interface{}{"field": "id", "type": "int32", "optional": false},
		map[string]interface{}{"field": "d", "type": "int32", "optional": true, "name": "io.debezium.time.Date"},
		map[string]interface{}{"field": "tm", "type": "int64", "optional": true, "name": "io.debezium.time.MicroTime"},
	}
	raw := cdcEnvelopeWithFields("c", map[string]interface{}{
		"id": 1,
		"d":  int64(0), // 1970-01-01
		"tm": int64(0), // 00:00:00
	}, fields)

	sm, err := parseSinkMessage(cfg, kafka.Message{Topic: "dbserver.mydb.edge", Value: raw, Offset: 1})
	if err != nil {
		t.Fatalf("parseSinkMessage: %v", err)
	}
	if d, _ := sm.After["d"].(string); d != "1970-01-01" {
		t.Fatalf("epoch-0 date: got %#v, want \"1970-01-01\"", sm.After["d"])
	}
	if tm, _ := sm.After["tm"].(string); tm != "00:00:00" {
		t.Fatalf("midnight time: got %#v, want \"00:00:00\"", sm.After["tm"])
	}
}

// TestCoerceCDCRowValues_NegativeTime confirms the Time-branch audit: a negative
// MySQL TIME (legal, range -838:59:59..838:59:59; Debezium encodes it as negative
// µs) is converted to a signed literal instead of being left as a raw int64 (which
// the old `if v < 0 { continue }` did → type error on INSERT → batch DLQ'd). A
// non-negative value's output is unchanged.
func TestCoerceCDCRowValues_NegativeTime(t *testing.T) {
	cfg := &WorkerConfig{
		PipelineID:           "test-pipeline",
		Topic:                "dbserver.mydb.edge",
		SinkMode:             "cdc",
		DestinationConnector: "mysql",
	}
	fields := []interface{}{
		map[string]interface{}{"field": "id", "type": "int32", "optional": false},
		map[string]interface{}{"field": "tm", "type": "int64", "optional": true, "name": "io.debezium.time.MicroTime"},
	}
	raw := cdcEnvelopeWithFields("c", map[string]interface{}{
		"id": 1,
		"tm": int64(-45296000000), // -12:34:56
	}, fields)

	sm, err := parseSinkMessage(cfg, kafka.Message{Topic: "dbserver.mydb.edge", Value: raw, Offset: 1})
	if err != nil {
		t.Fatalf("parseSinkMessage: %v", err)
	}
	if got, _ := sm.After["tm"].(string); got != "-12:34:56" {
		t.Fatalf("negative time: got %#v, want \"-12:34:56\"", sm.After["tm"])
	}
}

// TestCoerceCDCRowValues_MaxInt64BigintPrecision proves a BIGINT at the int64
// limit survives decode. Go's encoding/json unmarshals a JSON number into
// float64 by default, which cannot represent 9223372036854775807 exactly (it
// rounds to 9223372036854775808) -> "bigint out of range" on INSERT -> row
// DLQ'd. The value must round-trip exactly.
func TestCoerceCDCRowValues_MaxInt64BigintPrecision(t *testing.T) {
	cfg := &WorkerConfig{
		PipelineID:           "test-pipeline",
		Topic:                "dbserver.mydb.edge",
		SinkMode:             "cdc",
		DestinationConnector: "postgresql",
	}
	const maxInt64 = int64(9223372036854775807)
	const minInt64 = int64(-9223372036854775808)
	fields := []interface{}{
		map[string]interface{}{"field": "id", "type": "int32", "optional": false},
		map[string]interface{}{"field": "big", "type": "int64", "optional": true},
		map[string]interface{}{"field": "big_neg", "type": "int64", "optional": true},
	}
	raw := cdcEnvelopeWithFields("c", map[string]interface{}{
		"id":      1,
		"big":     maxInt64,
		"big_neg": minInt64,
	}, fields)

	sm, err := parseSinkMessage(cfg, kafka.Message{Topic: "dbserver.mydb.edge", Value: raw, Offset: 1})
	if err != nil {
		t.Fatalf("parseSinkMessage: %v", err)
	}
	// The int64 Connect field must be re-materialized to a concrete int64 (not
	// left as float64/json.Number), and the value must be exact.
	big, ok := sm.After["big"].(int64)
	if !ok {
		t.Fatalf("max int64 not coerced to int64: got %T (%#v)", sm.After["big"], sm.After["big"])
	}
	if big != maxInt64 {
		t.Fatalf("max int64 lost precision: got %d, want %d", big, maxInt64)
	}
	bigNeg, ok := sm.After["big_neg"].(int64)
	if !ok {
		t.Fatalf("min int64 not coerced to int64: got %T (%#v)", sm.After["big_neg"], sm.After["big_neg"])
	}
	if bigNeg != minInt64 {
		t.Fatalf("min int64 lost precision: got %d, want %d", bigNeg, minInt64)
	}
	// The value must also re-marshal to the exact integer literal (this is what
	// is shipped to the destination connector) — never scientific notation.
	b, _ := json.Marshal(sm.After["big"])
	if string(b) != "9223372036854775807" {
		t.Fatalf("max int64 re-marshalled as %q, want \"9223372036854775807\"", string(b))
	}
}

// TestParseSinkMessage_BatchMaxInt64Precision proves the fix is source-agnostic:
// a batch (non-CDC) message — which never runs coerceCDCRowValues — also
// preserves a max int64 exactly, because normalizeJSONNumbers runs at the shared
// decode boundary for every message shape.
func TestParseSinkMessage_BatchMaxInt64Precision(t *testing.T) {
	cfg := &WorkerConfig{
		PipelineID:  "test-pipeline",
		ExecutionID: "test-execution",
		Topic:       "test-topic",
		SinkMode:    "batch",
	}
	const maxInt64 = int64(9223372036854775807)
	payload := map[string]interface{}{
		"table":        "test_table",
		"storage_type": "inline",
		"row_count":    1,
		"data": []interface{}{
			map[string]interface{}{"id": 1, "big": maxInt64},
		},
	}
	payloadBytes, _ := json.Marshal(payload)

	sm, err := parseSinkMessage(cfg, kafka.Message{Value: payloadBytes})
	if err != nil {
		t.Fatalf("parseSinkMessage: %v", err)
	}
	if len(sm.Data) != 1 {
		t.Fatalf("expected 1 data row, got %d", len(sm.Data))
	}
	big, ok := sm.Data[0]["big"].(int64)
	if !ok {
		t.Fatalf("batch max int64 not int64: got %T (%#v)", sm.Data[0]["big"], sm.Data[0]["big"])
	}
	if big != maxInt64 {
		t.Fatalf("batch max int64 lost precision: got %d, want %d", big, maxInt64)
	}
}
