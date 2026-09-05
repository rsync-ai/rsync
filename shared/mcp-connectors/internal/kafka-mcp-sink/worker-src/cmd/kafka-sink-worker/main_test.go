package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

// TestParseSinkMessageBatch tests batch message parsing
func TestParseSinkMessageBatch(t *testing.T) {
	cfg := &WorkerConfig{
		PipelineID:  "test-pipeline",
		ExecutionID: "test-execution",
		Topic:       "test-topic",
		SinkMode:    "batch",
	}

	// Test inline batch message
	payload := map[string]interface{}{
		"pipeline_id":  "test-pipeline",
		"execution_id": "test-execution",
		"table":        "test_table",
		"storage_type": "inline",
		"batch_offset": 100,
		"row_count":    10,
		"data": []interface{}{
			map[string]interface{}{"id": 1, "name": "test"},
		},
	}
	payloadBytes, _ := json.Marshal(payload)
	msg := kafka.Message{
		Value: payloadBytes,
	}

	sm, err := parseSinkMessage(cfg, msg)
	if err != nil {
		t.Fatalf("Failed to parse batch message: %v", err)
	}

	if sm.Table != "test_table" {
		t.Errorf("Expected table 'test_table', got '%s'", sm.Table)
	}
	if sm.StorageType != "inline" {
		t.Errorf("Expected storage_type 'inline', got '%s'", sm.StorageType)
	}
	if sm.BatchOffset != 100 {
		t.Errorf("Expected batch_offset 100, got %d", sm.BatchOffset)
	}
	if sm.IsCDC {
		t.Error("Expected IsCDC to be false for batch message")
	}
}

// TestParseSinkMessageMinIO tests MinIO claim-check message parsing
func TestParseSinkMessageMinIO(t *testing.T) {
	cfg := &WorkerConfig{
		PipelineID:  "test-pipeline",
		ExecutionID: "test-execution",
		Topic:       "test-topic",
		SinkMode:    "batch",
	}

	// Pin the staging-bucket allow-list so validation is deterministic regardless of
	// ambient env, and use a realistic s3:// claim_check_url (the minio connector emits
	// s3://<bucket>/<key>; "minio://bucket/key" was a stale, never-produced placeholder).
	t.Setenv("RSYNC_SINK_STAGING_BUCKET", "")
	t.Setenv("MINIO_BUCKET", "")
	const claimURL = "s3://pipeline-data/staging/test-pipeline/large_table/batch-0.json"

	payload := map[string]interface{}{
		"pipeline_id":     "test-pipeline",
		"execution_id":    "test-execution",
		"table":           "large_table",
		"storage_type":    "minio",
		"claim_check_url": claimURL,
		"batch_offset":    0,
		"row_count":       10000,
	}
	payloadBytes, _ := json.Marshal(payload)
	msg := kafka.Message{
		Value: payloadBytes,
	}

	sm, err := parseSinkMessage(cfg, msg)
	if err != nil {
		t.Fatalf("Failed to parse MinIO message: %v", err)
	}

	if sm.StorageType != "minio" {
		t.Errorf("Expected storage_type 'minio', got '%s'", sm.StorageType)
	}
	if sm.ClaimCheckURL != claimURL {
		t.Errorf("Expected claim_check_url %q, got '%s'", claimURL, sm.ClaimCheckURL)
	}
}

// TestParseSinkMessageEOF tests EOF marker parsing
func TestParseSinkMessageEOF(t *testing.T) {
	cfg := &WorkerConfig{
		PipelineID: "test-pipeline",
		Topic:      "test-topic",
		SinkMode:   "batch",
	}

	payload := map[string]interface{}{
		"pipeline_id":     "test-pipeline",
		"table":           "test_table",
		"storage_type":    "eof",
		"eof":             true,
		"total_read_rows": 1000,
	}
	payloadBytes, _ := json.Marshal(payload)
	msg := kafka.Message{
		Value: payloadBytes,
	}

	sm, err := parseSinkMessage(cfg, msg)
	if err != nil {
		t.Fatalf("Failed to parse EOF message: %v", err)
	}

	if !sm.EOF {
		t.Error("Expected EOF to be true")
	}
	if sm.TotalReadRows != 1000 {
		t.Errorf("Expected total_read_rows 1000, got %d", sm.TotalReadRows)
	}
}

func TestConnectorClassificationHelpers(t *testing.T) {
	cases := []struct {
		in        string
		wantObj   bool
		wantWh    bool
		wantCanon string
	}{
		{in: "aws-s3", wantObj: true, wantWh: false, wantCanon: "aws-s3"},
		{in: "s3", wantObj: true, wantWh: false, wantCanon: "s3"},
		{in: "minio", wantObj: true, wantWh: false, wantCanon: "minio"},
		{in: "gcs", wantObj: true, wantWh: false, wantCanon: "gcs"},
		{in: "azure_blob", wantObj: true, wantWh: false, wantCanon: "azure-blob"},
		{in: "azure-blob", wantObj: true, wantWh: false, wantCanon: "azure-blob"},
		{in: "bigquery", wantObj: false, wantWh: true, wantCanon: "bigquery"},
		{in: "redshift", wantObj: false, wantWh: true, wantCanon: "redshift"},
		{in: "snowflake", wantObj: false, wantWh: true, wantCanon: "snowflake"},
		{in: "postgresql", wantObj: false, wantWh: false, wantCanon: "postgresql"},
	}

	for _, tc := range cases {
		if got := canonicalConnectorType(tc.in); got != tc.wantCanon {
			t.Errorf("canonicalConnectorType(%q)=%q, want %q", tc.in, got, tc.wantCanon)
		}
		if got := isObjectStorageConnector(tc.in); got != tc.wantObj {
			t.Errorf("isObjectStorageConnector(%q)=%v, want %v", tc.in, got, tc.wantObj)
		}
		if got := isDataWarehouseConnector(tc.in); got != tc.wantWh {
			t.Errorf("isDataWarehouseConnector(%q)=%v, want %v", tc.in, got, tc.wantWh)
		}
	}
}

// TestDocumentDBDestinationClassification locks the contract that MongoDB is a
// document-DB destination: it is NEITHER object storage NOR a warehouse (so it
// would otherwise fall into the relational "else" that forces ensure_table +
// synthetic-PK), isDocumentDBConnector routes it away from that path, and it is a
// single-namespace dest (collection == bare table, db from config).
func TestDocumentDBDestinationClassification(t *testing.T) {
	// Only MongoDB is a document DB today; every other dest type must be false so
	// the relational/object/warehouse paths are unaffected.
	for _, in := range []string{"mongodb", "mongo", "mongodb_atlas"} {
		if !isDocumentDBConnector(in) {
			t.Errorf("isDocumentDBConnector(%q)=false, want true", in)
		}
	}
	for _, in := range []string{"postgresql", "mysql", "sqlserver", "aws-s3", "minio", "bigquery", "snowflake", ""} {
		if isDocumentDBConnector(in) {
			t.Errorf("isDocumentDBConnector(%q)=true, want false", in)
		}
	}
	// Mongo must not be misclassified as object storage or warehouse (else it never
	// reaches the document-DB branch).
	if isObjectStorageConnector("mongodb") || isDataWarehouseConnector("mongodb") {
		t.Errorf("mongodb must be neither object storage nor warehouse")
	}
	// Single-namespace: a Postgres-shaped source table (public.orders) must resolve
	// to the bare collection name so the db comes from the connection config.
	if !isSingleNamespaceDest("mongodb") {
		t.Errorf("isSingleNamespaceDest(mongodb)=false, want true")
	}
	if got := normalizeTargetTable("mongodb", nil, "public.orders"); got != "orders" {
		t.Errorf("normalizeTargetTable(mongodb, public.orders)=%q, want %q", got, "orders")
	}
	if got := resolveDestTableForWrite("mongodb", nil, "inventory.items", "mongo_ns"); got != "items" {
		t.Errorf("resolveDestTableForWrite(mongodb, inventory.items)=%q, want %q", got, "items")
	}
}

// TestParseSinkMessageBootstrap tests bootstrap message parsing
func TestParseSinkMessageBootstrap(t *testing.T) {
	cfg := &WorkerConfig{
		PipelineID: "test-pipeline",
		Topic:      "test-topic",
		SinkMode:   "batch",
	}

	payload := map[string]interface{}{
		"pipeline_id":  "test-pipeline",
		"storage_type": "bootstrap",
	}
	payloadBytes, _ := json.Marshal(payload)
	msg := kafka.Message{
		Value: payloadBytes,
	}

	sm, err := parseSinkMessage(cfg, msg)
	if err != nil {
		t.Fatalf("Failed to parse bootstrap message: %v", err)
	}

	if !sm.Ignore {
		t.Error("Expected bootstrap message to be ignored")
	}
}

// TestParseCDCMessage tests Debezium CDC message parsing
func TestParseCDCMessage(t *testing.T) {
	cfg := &WorkerConfig{
		PipelineID: "test-pipeline",
		Topic:      "dbserver.mydb.users",
		SinkMode:   "cdc",
	}

	// Debezium create event
	payload := map[string]interface{}{
		"op": "c",
		"source": map[string]interface{}{
			"db":    "mydb",
			"table": "users",
			"txId":  "12345",
			"lsn":   int64(98765),
		},
		"before": nil,
		"after": map[string]interface{}{
			"id":    1,
			"name":  "John",
			"email": "john@example.com",
		},
		"ts_ms": int64(1704067200000),
	}
	// Kafka Connect JsonConverter with schemas enabled wraps Debezium payload.
	envelope := wrapCDCEnvelope(payload)
	payloadBytes, _ := json.Marshal(envelope)
	msg := kafka.Message{
		Topic:  "dbserver.mydb.users",
		Value:  payloadBytes,
		Offset: 42,
	}

	sm, err := parseSinkMessage(cfg, msg)
	if err != nil {
		t.Fatalf("Failed to parse CDC message: %v", err)
	}

	if !sm.IsCDC {
		t.Error("Expected IsCDC to be true")
	}
	if sm.CDCOp != "c" {
		t.Errorf("Expected CDC op 'c', got '%s'", sm.CDCOp)
	}
	if sm.TxID != "12345" {
		t.Errorf("Expected TxID '12345', got '%s'", sm.TxID)
	}
	if sm.LSN != 98765 {
		t.Errorf("Expected LSN 98765, got %d", sm.LSN)
	}
	if len(sm.Data) != 1 {
		t.Errorf("Expected 1 data row, got %d", len(sm.Data))
	}
}

func TestApplyConsumerTransforms_SkipsNonUUIDPipeline(t *testing.T) {
	sm := &SinkMessage{
		PipelineID:  "test-pipeline",
		ExecutionID: "test-execution",
		Table:       "mydb.users",
		IsCDC:       true,
		CDCOp:       "c",
		Data: []map[string]interface{}{
			{"id": 1, "email": "a@b.com"},
		},
	}
	orig := sm.Data[0]["email"]
	if err := applyConsumerTransforms(context.Background(), nil, &WorkerConfig{}, sm); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sm.Data[0]["email"] != orig {
		t.Fatalf("expected no mutation for non-uuid pipeline, got %#v", sm.Data[0]["email"])
	}
}

// A batch message (IsCDC=false, empty CDCOp) must be accepted by applyConsumerTransforms
// — the function is shared by the CDC and batch write paths. With a non-UUID pipeline it
// short-circuits before any DB access, so a nil *sql.DB is safe and data is left unchanged.
func TestApplyConsumerTransforms_BatchMessageNoMutationOnNonUUID(t *testing.T) {
	sm := &SinkMessage{
		PipelineID:  "adhoc-batch",
		ExecutionID: "adhoc-exec",
		Table:       "mydb.orders",
		IsCDC:       false,
		CDCOp:       "", // batch messages carry no CDC op
		Data: []map[string]interface{}{
			{"id": 1, "email": "a@b.com"},
		},
	}
	orig := sm.Data[0]["email"]
	if err := applyConsumerTransforms(context.Background(), nil, &WorkerConfig{}, sm); err != nil {
		t.Fatalf("unexpected error for batch message: %v", err)
	}
	if sm.Data[0]["email"] != orig {
		t.Fatalf("expected no mutation for non-uuid batch pipeline, got %#v", sm.Data[0]["email"])
	}
}

// TestParseCDCMessageUpdate tests CDC update event parsing
func TestParseCDCMessageUpdate(t *testing.T) {
	cfg := &WorkerConfig{
		PipelineID: "test-pipeline",
		Topic:      "dbserver.mydb.users",
		SinkMode:   "auto", // Test auto-detection
	}

	payload := map[string]interface{}{
		"op": "u",
		"source": map[string]interface{}{
			"db":    "mydb",
			"table": "users",
		},
		"before": map[string]interface{}{
			"id":   1,
			"name": "John",
		},
		"after": map[string]interface{}{
			"id":   1,
			"name": "Jane",
		},
	}
	envelope := wrapCDCEnvelope(payload)
	payloadBytes, _ := json.Marshal(envelope)
	msg := kafka.Message{
		Topic:  "dbserver.mydb.users",
		Value:  payloadBytes,
		Offset: 43,
	}

	sm, err := parseSinkMessage(cfg, msg)
	if err != nil {
		t.Fatalf("Failed to parse CDC update message: %v", err)
	}

	if !sm.IsCDC {
		t.Error("Expected auto-detect to recognize CDC message")
	}
	if sm.CDCOp != "u" {
		t.Errorf("Expected CDC op 'u', got '%s'", sm.CDCOp)
	}
	if sm.Before == nil {
		t.Error("Expected Before to be populated for update")
	}
	if sm.After == nil {
		t.Error("Expected After to be populated for update")
	}
}

// TestParseCDCMessageDelete tests CDC delete event parsing
func TestParseCDCMessageDelete(t *testing.T) {
	cfg := &WorkerConfig{
		PipelineID: "test-pipeline",
		Topic:      "dbserver.mydb.users",
		SinkMode:   "cdc",
	}

	payload := map[string]interface{}{
		"op": "d",
		"source": map[string]interface{}{
			"db":    "mydb",
			"table": "users",
		},
		"before": map[string]interface{}{
			"id":   1,
			"name": "John",
		},
		"after": nil,
	}
	envelope := wrapCDCEnvelope(payload)
	payloadBytes, _ := json.Marshal(envelope)
	msg := kafka.Message{
		Topic:  "dbserver.mydb.users",
		Value:  payloadBytes,
		Offset: 44,
	}

	sm, err := parseSinkMessage(cfg, msg)
	if err != nil {
		t.Fatalf("Failed to parse CDC delete message: %v", err)
	}

	if sm.CDCOp != "d" {
		t.Errorf("Expected CDC op 'd', got '%s'", sm.CDCOp)
	}
	if sm.Before == nil {
		t.Error("Expected Before to be populated for delete")
	}
	if len(sm.Data) != 1 {
		t.Errorf("Expected 1 data row (from Before), got %d", len(sm.Data))
	}
}

func wrapCDCEnvelope(payload map[string]interface{}) map[string]interface{} {
	// Minimal Kafka Connect schema with before/after struct fields. This matches the
	// shape produced by JsonConverter when schemas are enabled.
	rowFields := []interface{}{
		map[string]interface{}{"field": "id", "type": "int32", "optional": false},
		map[string]interface{}{"field": "name", "type": "string", "optional": true},
		map[string]interface{}{"field": "email", "type": "string", "optional": true},
	}
	schema := map[string]interface{}{
		"type": "struct",
		"fields": []interface{}{
			map[string]interface{}{"field": "before", "type": "struct", "optional": true, "fields": rowFields},
			map[string]interface{}{"field": "after", "type": "struct", "optional": true, "fields": rowFields},
			map[string]interface{}{"field": "source", "type": "struct", "optional": true, "fields": []interface{}{}},
			map[string]interface{}{"field": "op", "type": "string", "optional": false},
			map[string]interface{}{"field": "ts_ms", "type": "int64", "optional": true},
		},
	}
	return map[string]interface{}{
		"schema":  schema,
		"payload": payload,
	}
}

// KI-SINK-CDC-AUTODETECT: a schemas-disabled ("flat") Debezium envelope — the
// message itself IS the Debezium payload, with no {"schema":…,"payload":…} wrapper
// — must be auto-detected as CDC. Before the detection fix, sink_mode "auto" (the
// worker's default for an unset sink_mode) fell through to batch parsing, which
// requires payload["table"], so every change event was DLQ'd "missing table" and
// the pipeline landed 0 rows while reporting healthy.
func TestParseSinkMessageAutoDetectsFlatDebeziumEnvelope(t *testing.T) {
	// Both spellings of the default: explicit "auto" and unset (parseSinkMessage
	// defaults an empty SinkMode to "auto").
	for _, mode := range []string{"auto", ""} {
		cfg := &WorkerConfig{
			PipelineID: "test-pipeline",
			Topic:      "dbserver.mydb.users",
			SinkMode:   mode,
		}

		flat := map[string]interface{}{
			"op": "c",
			"source": map[string]interface{}{
				"db":    "mydb",
				"table": "users",
			},
			"before": nil,
			"after": map[string]interface{}{
				"id":   1,
				"name": "John",
			},
			"ts_ms": int64(1704067200000),
		}
		raw, _ := json.Marshal(flat)
		msg := kafka.Message{Topic: "dbserver.mydb.users", Value: raw, Offset: 7}

		sm, err := parseSinkMessage(cfg, msg)
		if err != nil {
			t.Fatalf("sink_mode=%q: flat Debezium envelope must parse as CDC, got error: %v", mode, err)
		}
		if !sm.IsCDC {
			t.Errorf("sink_mode=%q: expected IsCDC=true for flat Debezium envelope", mode)
		}
		if sm.CDCOp != "c" {
			t.Errorf("sink_mode=%q: expected CDCOp 'c', got %q", mode, sm.CDCOp)
		}
		if sm.Table != "mydb.users" {
			t.Errorf("sink_mode=%q: expected table 'mydb.users', got %q", mode, sm.Table)
		}
		if len(sm.Data) != 1 {
			t.Errorf("sink_mode=%q: expected 1 data row from 'after', got %d", mode, len(sm.Data))
		}
	}
}

// False-positive guard on the same detection gate: a BATCH envelope must stay
// batch in "auto" mode even when it carries top-level keys literally named "op"
// and "source". The flat-CDC discriminator requires `source` to be a Debezium
// source STRUCT and `op` to be a Debezium operation code, so a naive
// key-presence check would mis-route this batch message to parseCDCMessage.
// ("auto" is not hypothetical for batch: the hybrid-CDC batch-backfill sink is
// started with sink_mode="auto" on the batch topic.)
func TestParseSinkMessageAutoKeepsBatchEnvelopeAsBatch(t *testing.T) {
	cfg := &WorkerConfig{
		PipelineID:  "test-pipeline",
		ExecutionID: "test-execution",
		Topic:       "pipeline.test.data",
		SinkMode:    "auto",
	}

	payload := map[string]interface{}{
		"pipeline_id":  "test-pipeline",
		"execution_id": "test-execution",
		"table":        "test_table",
		"storage_type": "inline",
		"batch_offset": 100,
		"row_count":    1,
		// Decoys: scalar op/source at the envelope level are NOT a Debezium event.
		"op":     "export",
		"source": "executor_batch",
		"data": []interface{}{
			map[string]interface{}{"id": 1, "name": "test"},
		},
	}
	raw, _ := json.Marshal(payload)

	sm, err := parseSinkMessage(cfg, kafka.Message{Value: raw})
	if err != nil {
		t.Fatalf("batch envelope must parse as batch in auto mode, got error: %v", err)
	}
	if sm.IsCDC {
		t.Error("expected IsCDC=false: a batch envelope must never be auto-detected as CDC")
	}
	if sm.Table != "test_table" {
		t.Errorf("expected table 'test_table', got %q", sm.Table)
	}
	if len(sm.Data) != 1 {
		t.Errorf("expected 1 data row, got %d", len(sm.Data))
	}
}

// TestAckKeyFor tests idempotency key generation for batch
func TestAckKeyFor(t *testing.T) {
	key := ackKeyFor("pipeline-123", "exec-456", "schema.table", 100)
	expected := "sink_ack:pipeline-123:exec-456:schema.table:100"
	if key != expected {
		t.Errorf("Expected '%s', got '%s'", expected, key)
	}

	// Test with special characters
	key2 := ackKeyFor("pipeline", "exec", "table/with/slashes", 0)
	if key2 != "sink_ack:pipeline:exec:table_with_slashes:0" {
		t.Errorf("Expected slashes to be replaced, got '%s'", key2)
	}
}

// TestCdcAckKeyFor tests idempotency key generation for CDC
func TestCdcAckKeyFor(t *testing.T) {
	// New strategy: topic + partition + offset
	key := cdcAckKeyFor("pipeline", "db.schema.table", 2, 42)
	if key != "cdc_ack:pipeline:db.schema.table:p2:o42" {
		t.Errorf("Expected topic+partition+offset key, got '%s'", key)
	}

	// Topic sanitization
	key2 := cdcAckKeyFor("pipeline", "topic/with:weird\\chars", 0, 1)
	if key2 != "cdc_ack:pipeline:topic_with_weird_chars:p0:o1" {
		t.Errorf("Expected sanitized topic, got '%s'", key2)
	}
}

// TestExtractTableFromTopic tests table extraction from Debezium topics
func TestExtractTableFromTopic(t *testing.T) {
	tests := []struct {
		topic    string
		payload  map[string]interface{}
		expected string
	}{
		{
			topic:    "dbserver.mydb.users",
			payload:  map[string]interface{}{},
			expected: "mydb.users",
		},
		{
			topic:    "prefix.schema.table",
			payload:  map[string]interface{}{},
			expected: "schema.table",
		},
		{
			topic: "any.topic",
			payload: map[string]interface{}{
				"source": map[string]interface{}{
					"db":    "testdb",
					"table": "orders",
				},
			},
			expected: "testdb.orders",
		},
		{
			topic: "topic.with.source.schema",
			payload: map[string]interface{}{
				"source": map[string]interface{}{
					"schema": "public",
					"table":  "items",
				},
			},
			expected: "public.items",
		},
	}

	for _, tc := range tests {
		result := extractTableFromTopicOrSource(tc.topic, tc.payload)
		if result != tc.expected {
			t.Errorf("For topic '%s', expected '%s', got '%s'", tc.topic, tc.expected, result)
		}
	}
}

// TestToString tests type conversion helper
func TestToString(t *testing.T) {
	tests := []struct {
		input    interface{}
		expected string
	}{
		{"hello", "hello"},
		{[]byte("bytes"), "bytes"},
		{123, "123"},
		{nil, ""},
		{12.5, "12.5"},
	}

	for _, tc := range tests {
		result := toString(tc.input)
		if result != tc.expected {
			t.Errorf("For input %v, expected '%s', got '%s'", tc.input, tc.expected, result)
		}
	}
}

// TestToInt64 tests integer conversion helper
func TestToInt64(t *testing.T) {
	tests := []struct {
		input    interface{}
		expected int64
	}{
		{int64(100), 100},
		{int(50), 50},
		{float64(75.5), 75},
		{json.Number("200"), 200},
		{"300", 300},
		{nil, 0},
	}

	for _, tc := range tests {
		result := toInt64(tc.input)
		if result != tc.expected {
			t.Errorf("For input %v, expected %d, got %d", tc.input, tc.expected, result)
		}
	}
}

// BenchmarkParseSinkMessage benchmarks message parsing
func BenchmarkParseSinkMessage(b *testing.B) {
	cfg := &WorkerConfig{
		PipelineID: "bench-pipeline",
		Topic:      "bench-topic",
		SinkMode:   "batch",
	}

	payload := map[string]interface{}{
		"pipeline_id":  "bench-pipeline",
		"table":        "bench_table",
		"storage_type": "inline",
		"batch_offset": 0,
		"row_count":    100,
		"data":         []interface{}{},
	}
	payloadBytes, _ := json.Marshal(payload)
	msg := kafka.Message{Value: payloadBytes}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = parseSinkMessage(cfg, msg)
	}
}

// TestMetrics tests metrics structure
func TestMetrics(t *testing.T) {
	m := &Metrics{startedAt: time.Now()}

	// Test error recording
	testErr := context.DeadlineExceeded
	m.setErr(testErr)

	m.mu.Lock()
	if m.lastError != "context deadline exceeded" {
		t.Errorf("Expected error message, got '%s'", m.lastError)
	}
	m.mu.Unlock()

	// Test nil error
	m.setErr(nil)
	m.mu.Lock()
	if m.lastError != "context deadline exceeded" {
		t.Error("nil error should not overwrite lastError")
	}
	m.mu.Unlock()
}

func TestBronzeOpFromDebezium(t *testing.T) {
	tests := []struct {
		in  string
		out string
	}{
		{"c", "I"},
		{"r", "I"},
		{"u", "U"},
		{"d", "D"},
		{"", ""},
		{"X", "X"},
	}
	for _, tc := range tests {
		if got := bronzeOpFromDebezium(tc.in); got != tc.out {
			t.Errorf("bronzeOpFromDebezium(%q) expected %q, got %q", tc.in, tc.out, got)
		}
	}
}

func TestNormalizeTableForPath(t *testing.T) {
	if got := normalizeTableForPath("public.users"); got != "public_users" {
		t.Errorf("expected public_users, got %q", got)
	}
	if got := normalizeTableForPath("db/schema\\table name"); got != "db_schema_table_name" {
		t.Errorf("expected db_schema_table_name, got %q", got)
	}
}

func TestCDCObjectKeyLayout(t *testing.T) {
	// DMS-style: <prefix>/<db_or_schema>/<table>/<YYYY-MM-DD>/<YYYYMMDD-HHMMSSmmm>[-p<n>]-<offset>.<ext>.
	// No dataset segment; plain date folder; a non-zero Kafka partition folds into the leaf.
	// fixedTS = 2026-01-25 14:30:00.000Z → timestamp stem 20260125-143000000.
	key := cdcObjectKey("prefix", "public", "users", "2026-01-25", "", fixedTS, 3, 100, 110, "jsonl", "none")
	want := "prefix/public/users/2026-01-25/20260125-143000000-p3-100.jsonl"
	if key != want {
		t.Errorf("expected %q, got %q", want, key)
	}
	// Partition 0 → no -p suffix; leading prefix slash trimmed; gzip adds .gz.
	key2 := cdcObjectKey("p/", "sch", "t", "2026-01-25", "", fixedTS, 0, 1, 1, "parquet", "gzip")
	want2 := "p/sch/t/2026-01-25/20260125-143000000-1.parquet.gz"
	if key2 != want2 {
		t.Errorf("expected %q, got %q", want2, key2)
	}
}

func TestCDCObjectPath(t *testing.T) {
	// schema/table split from "<schema>.<table>"; no dataset segment (path_prefix is the namespace).
	dbs, tbl := cdcObjectPath(&SinkMessage{Dataset: "My Pipeline", Table: "blended_cost.costs"})
	if dbs != "blended_cost" || tbl != "costs" {
		t.Errorf("got db=%q table=%q", dbs, tbl)
	}
	// DBOrSchema wins over the schema embedded in Table.
	dbs, tbl = cdcObjectPath(&SinkMessage{PipelineID: "abc123", DBOrSchema: "sales", Table: "raw.orders"})
	if dbs != "sales" || tbl != "orders" {
		t.Errorf("got db=%q table=%q", dbs, tbl)
	}
	// No schema in Table and no DBOrSchema → "default".
	if dbs, tbl := cdcObjectPath(&SinkMessage{Table: "events"}); dbs != "default" || tbl != "events" {
		t.Errorf("got db=%q table=%q", dbs, tbl)
	}
}

func TestResolveCDCBatchingParamsDefaultsAndOverrides(t *testing.T) {
	p := resolveCDCBatchingParams(nil)
	if !p.enabled || p.maxEvents != 2000 || p.maxBytes != 24*1024*1024 || p.flushInterval != 30*time.Second {
		t.Fatalf("unexpected defaults: %+v", p)
	}

	f := false
	cfg := &WorkerConfig{
		KafkaSinkWorker: &KafkaSinkWorkerConfig{
			CDCBatching: &CDCBatchingConfig{
				Enabled:               &f,
				MaxEvents:             10,
				MaxBytes:              1234,
				FlushIntervalSeconds:  5,
				MaxRetries:            2,
				BackoffMS:             50,
				AbsoluteMaxEventBytes: 999,
			},
		},
	}
	p2 := resolveCDCBatchingParams(cfg)
	if p2.enabled {
		t.Errorf("expected batching disabled")
	}
	if p2.maxEvents != 10 || p2.maxBytes != 1234 || p2.flushInterval != 5*time.Second || p2.maxRetries != 2 || p2.backoff != 50*time.Millisecond || p2.absoluteMaxEventBytes != 999 {
		t.Fatalf("unexpected overrides: %+v", p2)
	}
}

func TestBuildBronzeCDCEventJSONL(t *testing.T) {
	cfg := &WorkerConfig{}
	msg := kafka.Message{Topic: "dbserver.mydb.users", Partition: 3, Offset: 42}
	sm := &SinkMessage{
		Table:       "public.users",
		CDCOp:       "c",
		LSN:         123,
		SourceTS:    1000,
		IngestionTS: 2000,
		PK:          map[string]interface{}{"id": 1},
		After:       map[string]interface{}{"id": 1, "name": "Alice"},
	}
	evt, _, err := buildBronzeCDCEvent(cfg, msg, sm, "upsert", "jsonl")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if evt["op"] != "I" {
		t.Errorf("expected op=I, got %v", evt["op"])
	}
	if evt["table"] != "public.users" {
		t.Errorf("expected table=public.users, got %v", evt["table"])
	}
	if evt["lsn"] != int64(123) {
		t.Errorf("expected lsn=123, got %v", evt["lsn"])
	}
	if evt["source_ts_ms"] != int64(1000) || evt["ingestion_ts_ms"] != int64(2000) {
		t.Errorf("unexpected timestamps: source=%v ingestion=%v", evt["source_ts_ms"], evt["ingestion_ts_ms"])
	}
	if evt["kafka_partition"] != 3 || evt["kafka_offset"] != int64(42) {
		t.Errorf("expected kafka metadata, got partition=%v offset=%v", evt["kafka_partition"], evt["kafka_offset"])
	}
	if !reflect.DeepEqual(evt["pk"], map[string]interface{}{"id": 1}) {
		t.Errorf("expected pk object, got %v", evt["pk"])
	}
	if !reflect.DeepEqual(evt["after"], map[string]interface{}{"id": 1, "name": "Alice"}) {
		t.Errorf("expected after object, got %v", evt["after"])
	}
	if _, ok := evt["pk_json"]; ok {
		t.Errorf("did not expect pk_json for jsonl")
	}
	if _, ok := evt["after_json"]; ok {
		t.Errorf("did not expect after_json for jsonl")
	}
}

func TestBuildBronzeCDCEventColumnar(t *testing.T) {
	cfg := &WorkerConfig{}
	msg := kafka.Message{Topic: "dbserver.mydb.users", Partition: 1, Offset: 7}
	sm := &SinkMessage{
		Table:       "public.users",
		CDCOp:       "u",
		LSN:         555,
		SourceTS:    1000,
		IngestionTS: 2000,
		PK:          map[string]interface{}{"id": 1},
		After:       map[string]interface{}{"id": 1, "email": "a@x.com"},
	}
	evt, _, err := buildBronzeCDCEvent(cfg, msg, sm, "upsert", "parquet")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if evt["op"] != "U" {
		t.Errorf("expected op=U, got %v", evt["op"])
	}
	if _, ok := evt["pk"]; ok {
		t.Errorf("did not expect pk object for columnar")
	}
	if _, ok := evt["after"]; ok {
		t.Errorf("did not expect after object for columnar")
	}
	if s, ok := evt["pk_json"].(string); !ok || s == "" {
		t.Errorf("expected non-empty pk_json, got %v", evt["pk_json"])
	}
	if s, ok := evt["after_json"].(string); !ok || s == "" {
		t.Errorf("expected non-empty after_json, got %v", evt["after_json"])
	}
}

func TestBuildBronzeCDCEventDelete(t *testing.T) {
	cfg := &WorkerConfig{}
	msg := kafka.Message{Topic: "dbserver.mydb.users", Partition: 0, Offset: 9}
	sm := &SinkMessage{
		Table:       "public.users",
		CDCOp:       "d",
		LSN:         9,
		SourceTS:    1000,
		IngestionTS: 2000,
		PK:          map[string]interface{}{"id": 1},
	}
	evt, _, err := buildBronzeCDCEvent(cfg, msg, sm, "delete", "jsonl")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if evt["op"] != "D" {
		t.Errorf("expected op=D, got %v", evt["op"])
	}
	if !reflect.DeepEqual(evt["pk"], map[string]interface{}{"id": 1}) {
		t.Errorf("expected pk for delete, got %v", evt["pk"])
	}
	if _, ok := evt["after"]; !ok || evt["after"] != nil {
		t.Errorf("expected after=null for delete, got %v", evt["after"])
	}
}

func TestBuildBronzeCDCEventDeleteMissingPKErrors(t *testing.T) {
	cfg := &WorkerConfig{}
	msg := kafka.Message{Topic: "t", Partition: 0, Offset: 1}
	sm := &SinkMessage{Table: "public.users", CDCOp: "d", SourceTS: 1, IngestionTS: 2}
	_, _, err := buildBronzeCDCEvent(cfg, msg, sm, "delete", "jsonl")
	if err == nil {
		t.Fatalf("expected error for delete without pk/before, got nil")
	}
}

func TestIncludeKafkaMetadataCanBeDisabled(t *testing.T) {
	f := false
	cfg := &WorkerConfig{
		KafkaSinkWorker: &KafkaSinkWorkerConfig{
			Metadata: &WorkerMetadata{IncludeKafkaMetadata: &f},
		},
	}
	msg := kafka.Message{Topic: "t", Partition: 3, Offset: 42}
	sm := &SinkMessage{
		Table:       "public.users",
		CDCOp:       "c",
		LSN:         1,
		SourceTS:    1,
		IngestionTS: 2,
		PK:          map[string]interface{}{"id": 1},
		After:       map[string]interface{}{"id": 1},
	}
	evt, _, err := buildBronzeCDCEvent(cfg, msg, sm, "upsert", "jsonl")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := evt["kafka_partition"]; ok {
		t.Errorf("did not expect kafka_partition when disabled")
	}
	if _, ok := evt["kafka_offset"]; ok {
		t.Errorf("did not expect kafka_offset when disabled")
	}
}

func TestPKObjectFallbackFromRowWhenKeyMissing(t *testing.T) {
	sm := &SinkMessage{
		Table:     "public.users",
		KeyFields: []string{"id"},
		After:     map[string]interface{}{"id": 7, "name": "n"},
	}
	pk := pkObjectForCDC(sm, "upsert")
	if !reflect.DeepEqual(pk, map[string]interface{}{"id": 7}) {
		t.Errorf("expected pk derived from row, got %v", pk)
	}
}

// ---------------------------------------------------------------------------
// Ledger-fail-closed helpers
//
// These tests pin down the exactly-once contract introduced after the Redis
// dedup removal: the worker MUST NOT silently advance the Kafka offset on a
// ledger-side failure. Concretely the three properties below are what the
// audit calls out as blockers — they have no other test coverage today.
// ---------------------------------------------------------------------------

func TestLedgerRetryBackoffIsCapped(t *testing.T) {
	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{0, 2 * time.Second}, // attempt<1 floored to 1
		{1, 2 * time.Second},
		{5, 10 * time.Second},
		{30, 60 * time.Second}, // saturates at 60s
		{1000, 60 * time.Second},
	}
	for _, tc := range cases {
		got := ledgerRetryBackoff(tc.attempt)
		if got != tc.want {
			t.Errorf("ledgerRetryBackoff(%d) = %s, want %s", tc.attempt, got, tc.want)
		}
	}
}

func TestCtxAwareSleepRespectsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	completed := ctxAwareSleep(ctx, 5*time.Second)
	elapsed := time.Since(start)
	if completed {
		t.Fatalf("expected ctxAwareSleep to be cut short by cancel, but it ran to completion (%s)", elapsed)
	}
	if elapsed > 1*time.Second {
		t.Errorf("ctxAwareSleep took %s — should have exited near-immediately after cancel", elapsed)
	}
}

func TestRetryUntilSuccessOrCancel_StopsOnFirstSuccess(t *testing.T) {
	calls := 0
	err := retryUntilSuccessOrCancel(context.Background(), "test", nil, func(context.Context) error {
		calls++
		if calls < 3 {
			return errors.New("transient")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil after eventual success, got %v", err)
	}
	if calls != 3 {
		t.Fatalf("expected 3 attempts to reach success, got %d", calls)
	}
}

func TestRetryUntilSuccessOrCancel_ReturnsCtxErrOnCancel(t *testing.T) {
	// The retry loop sleeps with ledgerRetryBackoff between attempts — the
	// first sleep is 2s. We cancel during that sleep to assert that a
	// sustained ledger outage doesn't keep the retry loop alive past
	// shutdown.
	ctx, cancel := context.WithCancel(context.Background())
	attempts := 0
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	err := retryUntilSuccessOrCancel(ctx, "test", nil, func(context.Context) error {
		attempts++
		return errors.New("ledger down")
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled on shutdown, got %v", err)
	}
	if attempts < 1 {
		t.Fatalf("expected at least one attempt before cancel, got %d", attempts)
	}
}

// TestExtractDestRowCount locks in the canonical write-count contract:
// every destination MCP must report writes via one of destWriteCountFields,
// and the sink accepts them all interchangeably. Add a new field to that
// list before adding a connector that emits a new name — both batch and
// streaming paths share this helper.
func TestExtractDestRowCount(t *testing.T) {
	cases := []struct {
		name   string
		input  map[string]interface{}
		want   int64
		wantOk bool
	}{
		{"rows_inserted (postgres/mysql import_data)", map[string]interface{}{"rows_inserted": float64(42)}, 42, true},
		{"rows_upserted (postgres/mysql upsert_data — DB→DB hot path)", map[string]interface{}{"rows_upserted": float64(13)}, 13, true},
		{"rows_merged (warehouse CDC apply)", map[string]interface{}{"rows_merged": float64(7)}, 7, true},
		{"rows_loaded (BigQuery/Snowflake large load)", map[string]interface{}{"rows_loaded": float64(1000)}, 1000, true},
		{"rows_written (legacy alias)", map[string]interface{}{"rows_written": float64(5)}, 5, true},
		{"rows_affected (generic SQL execute)", map[string]interface{}{"rows_affected": float64(3)}, 3, true},
		{"rows_deleted (delete-only op)", map[string]interface{}{"rows_deleted": float64(2)}, 2, true},
		{"int wire encoding", map[string]interface{}{"rows_inserted": 9}, 9, true},
		{"json.Number wire encoding", map[string]interface{}{"rows_inserted": json.Number("11")}, 11, true},
		{"string wire encoding", map[string]interface{}{"rows_inserted": "8"}, 8, true},
		{"zero is a valid count", map[string]interface{}{"rows_inserted": float64(0)}, 0, true},
		{"first match wins", map[string]interface{}{"rows_inserted": float64(5), "rows_upserted": float64(99)}, 5, true},
		{"missing field returns false", map[string]interface{}{"success": true}, 0, false},
		{"empty map returns false", map[string]interface{}{}, 0, false},
		{"unknown synonym returns false (caller must add to allowlist)", map[string]interface{}{"rows_imported": float64(50)}, 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := extractDestRowCount(tc.input)
			if ok != tc.wantOk {
				t.Fatalf("ok = %v, want %v", ok, tc.wantOk)
			}
			if got != tc.want {
				t.Fatalf("count = %d, want %d", got, tc.want)
			}
		})
	}
}

// TestNormalizeTargetTable pins the cross-DB schema-name translation rules.
// Hot demo path: Postgres → MySQL pipelines fail with "Access denied for
// user … to database 'public'" when the sink hands the MySQL connector a
// "public.<table>" name — MySQL has no schema concept and treats "public"
// as a database. The fix strips any leading schema part for single-
// namespace destinations.
func TestNormalizeTargetTable(t *testing.T) {
	cases := []struct {
		name     string
		destType string
		destCfg  map[string]interface{}
		fallback string
		want     string
	}{
		// Postgres dest: strip MySQL db prefix when it matches the dest
		// database name (legacy MySQL-source → PG-dest path).
		{"PG dest strips MySQL db prefix", "postgresql", map[string]interface{}{"database": "shopify"}, "shopify.products", "public.products"},
		{"PG dest leaves non-matching prefix alone", "postgresql", map[string]interface{}{"database": "shopify"}, "other.products", "other.products"},

		// MySQL / single-namespace dest: strip ANY leading schema part. This
		// is the bug class that blocked PG → MySQL E2E — without it, every
		// row failed write with "Access denied … to database 'public'".
		{"MySQL strips public schema (PG → MySQL hot path)", "mysql", map[string]interface{}{"database": "e2e_db"}, "public.products", "products"},
		{"MySQL strips arbitrary schema", "mysql", map[string]interface{}{"database": "e2e_db"}, "analytics.events", "events"},
		{"MySQL leaves unqualified name alone", "mysql", map[string]interface{}{"database": "e2e_db"}, "products", "products"},
		{"MariaDB strips schema", "mariadb", map[string]interface{}{"database": "e2e_db"}, "public.products", "products"},
		{"SQLite strips schema", "sqlite", map[string]interface{}{}, "public.products", "products"},
		{"ClickHouse strips schema", "clickhouse", map[string]interface{}{"database": "default"}, "public.products", "products"},

		// destCfg override beats fallback.
		{"destCfg table override wins over fallback", "mysql", map[string]interface{}{"database": "e2e_db", "table": "renamed_table"}, "public.products", "renamed_table"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := normalizeTargetTable(tc.destType, tc.destCfg, tc.fallback)
			if got != tc.want {
				t.Fatalf("normalizeTargetTable(%q, %v, %q) = %q, want %q", tc.destType, tc.destCfg, tc.fallback, got, tc.want)
			}
		})
	}
}

// TestBareTableForNamespace pins the CDC bare-table rule: the relational
// connectors only apply the separately-forwarded destination namespace when the
// table has NO dot, so the CDC write paths must strip any source-schema/db
// qualifier before forwarding the namespace. A trailing dot (no name part) is
// left untouched.
func TestBareTableForNamespace(t *testing.T) {
	cases := []struct{ in, want string }{
		{"rsync_public.customers", "customers"},
		{"customers", "customers"},
		{`"rsync_public"."customers"`, "customers"},
		{"db.schema.customers", "customers"},
		{"  public.orders  ", "orders"},
		{"", ""},
		{"public.", "public."},
	}
	for _, tc := range cases {
		if got := bareTableForNamespace(tc.in); got != tc.want {
			t.Fatalf("bareTableForNamespace(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestResolveDestTableForWrite pins the unified batch table-resolution rule used
// by the non-CDC write path (writeToDestination). It must agree with the CDC
// paths (main.go 963-970, 3383-3392): normalize the source-derived table, then
// strip any schema/db qualifier when a REAL per-pipeline namespace is set (the
// namespace travels separately via addNamespaceParam). Without the strip, a PG
// dest receives a source-qualified table like "rsync_public.orders" PLUS a
// namespace param; per the connector contract a qualified table makes the
// connector IGNORE the namespace and write to the leaked source schema, so the
// batch silent-drops (read>0, landed=0) — the exact shopify→postgres symptom.
func TestResolveDestTableForWrite(t *testing.T) {
	cases := []struct {
		name      string
		destType  string
		destCfg   map[string]interface{}
		table     string
		namespace string
		want      string
	}{
		// THE bug: PG dest + real namespace + leaked source schema -> must go bare
		// so the separately-forwarded namespace is honored.
		{"PG real namespace strips leaked source schema", "postgresql", map[string]interface{}{"database": "pipeline_db"}, "rsync_public.orders", "rsync_public", "orders"},
		{"PG real namespace strips arbitrary source schema", "postgresql", map[string]interface{}{"database": "pipeline_db"}, "analytics.events", "warehouse", "events"},
		{"PG real namespace, already bare, unchanged", "postgresql", map[string]interface{}{"database": "pipeline_db"}, "orders", "rsync_public", "orders"},

		// Safety guard: NO real namespace -> PG keeps its conservative behavior so
		// a legitimately schema-qualified dest table is preserved (there is no
		// namespace to route by). This must NOT regress.
		{"PG no namespace keeps legit schema", "postgresql", map[string]interface{}{"database": "pipeline_db"}, "analytics.orders", "", "analytics.orders"},
		{"PG 'default' namespace is not real, keeps schema", "postgresql", map[string]interface{}{"database": "pipeline_db"}, "analytics.orders", "default", "analytics.orders"},

		// MySQL already strips in normalizeTargetTable; the rule is idempotent
		// whether or not a namespace is set.
		{"MySQL strips schema regardless of namespace", "mysql", map[string]interface{}{"database": "e2e_db"}, "public.products", "", "products"},
		{"MySQL real namespace still bare", "mysql", map[string]interface{}{"database": "e2e_db"}, "public.products", "tenant7", "products"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := resolveDestTableForWrite(tc.destType, tc.destCfg, tc.table, tc.namespace)
			if got != tc.want {
				t.Fatalf("resolveDestTableForWrite(%q, %v, %q, %q) = %q, want %q", tc.destType, tc.destCfg, tc.table, tc.namespace, got, tc.want)
			}
		})
	}
}

// TestParseCDCMessageDestinationNamespace locks C2: the CDC parser must carry the
// orchestrator-injected destination namespace (WorkerConfig.DestinationNamespace)
// onto every CDC SinkMessage so the relational write paths route to the user's
// namespace. Empty => empty (back-compat), and whitespace is trimmed. The source
// table identity (sm.Table) must be preserved (the namespace travels separately).
func TestParseCDCMessageDestinationNamespace(t *testing.T) {
	mkPayload := func() map[string]interface{} {
		return map[string]interface{}{
			"op":    "c",
			"after": map[string]interface{}{"id": 1, "name": "a"},
			"source": map[string]interface{}{
				"table":  "customers",
				"schema": "rsync_public",
			},
		}
	}
	msg := kafka.Message{Topic: "srv.rsync_public.customers", Offset: 1}

	cases := []struct {
		name   string
		ns     string
		wantNS string
	}{
		{"real namespace carried onto CDC message", "analytics", "analytics"},
		{"whitespace trimmed", "  analytics  ", "analytics"},
		{"empty namespace stays empty (back-compat)", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &WorkerConfig{PipelineID: "p1", SinkMode: "cdc", DestinationNamespace: tc.ns}
			sm, err := parseCDCMessage(cfg, msg, mkPayload(), map[string]interface{}{})
			if err != nil {
				t.Fatalf("parseCDCMessage error: %v", err)
			}
			if sm.DBOrSchema != tc.wantNS {
				t.Fatalf("sm.DBOrSchema = %q, want %q", sm.DBOrSchema, tc.wantNS)
			}
			if sm.Table != "rsync_public.customers" {
				t.Fatalf("sm.Table = %q, want %q", sm.Table, "rsync_public.customers")
			}
		})
	}
}

// TestDDLSupportRetriesTransientStartupMiss locks the root-cause fix for the
// "0 rows landed / relation does not exist" class: a transient startup probe
// failure (destination connector not yet reachable during a cold stack start)
// must NOT latch Enabled=false forever. It must leave the support undetermined
// (resolved=false) so supported() re-probes on the first real write.
func TestDDLSupportRetriesTransientStartupMiss(t *testing.T) {
	// A short-timeout, no-keepalive client so the unreachable probe fails fast.
	fastClient := &http.Client{Timeout: 150 * time.Millisecond}
	cfg := &WorkerConfig{DestinationConnector: "postgresql", DestinationVersion: "1.0.0"}
	ctx := context.Background()

	// Startup probe against an unreachable connector -> transient miss.
	ddl := resolveDDLSupport(ctx, fastClient, cfg, "postgresql")
	if ddl.resolved {
		t.Fatalf("transient startup probe must leave resolved=false so it retries; got resolved=true")
	}
	if ddl.Enabled {
		t.Fatalf("Enabled must be false after a failed probe; got true")
	}

	// supported() must keep returning false WITHOUT latching resolved=true, so a
	// later call (once the connector is up) can still flip Enabled.
	if got := ddl.supported(ctx, fastClient, cfg, "postgresql"); got {
		t.Fatalf("supported() returned true for an unreachable destination")
	}
	if ddl.resolved {
		t.Fatalf("a still-failing re-probe must not mark resolved=true (would re-introduce the latch bug)")
	}
}

// TestDDLSupportDefinitiveAnswerIsCached ensures we do NOT re-probe forever once
// we have a definitive answer: a resolved DDLSupport returns its cached Enabled
// value without any network call (proven by using a nil client + bogus dest that
// would panic/err if a probe were attempted).
func TestDDLSupportDefinitiveAnswerIsCached(t *testing.T) {
	ctx := context.Background()
	cfg := &WorkerConfig{DestinationConnector: "postgresql", DestinationVersion: "1.0.0"}

	enabled := &DDLSupport{Enabled: true, resolved: true}
	if !enabled.supported(ctx, nil, cfg, "postgresql") {
		t.Fatalf("resolved=true,Enabled=true must short-circuit to true without probing")
	}

	// A legit non-DDL destination (definitive false) must also be cached, never retried.
	disabled := &DDLSupport{Enabled: false, resolved: true}
	if disabled.supported(ctx, nil, cfg, "postgresql") {
		t.Fatalf("resolved=true,Enabled=false must short-circuit to false without probing")
	}

	// Empty destType is a definitive "no DDL needed".
	if e, got := probeDDLSupport(ctx, nil, cfg, ""); e || !got {
		t.Fatalf("probeDDLSupport(empty dest) = (%v,%v), want (false,true)", e, got)
	}

	// nil receiver is safe.
	var nilDDL *DDLSupport
	if nilDDL.supported(ctx, nil, cfg, "postgresql") {
		t.Fatalf("nil *DDLSupport must report unsupported, not panic")
	}
}
