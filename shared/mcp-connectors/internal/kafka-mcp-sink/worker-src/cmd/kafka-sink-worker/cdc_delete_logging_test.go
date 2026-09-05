package main

// KI-CDC-DELETE-PATH-UNLOGGED coverage.
//
// A CDC delete used to apply correctly and log absolutely NOTHING: the counter
// moved (applied_deletes=1) but no line named the table, the key, the row count
// or the Kafka coordinate, so a wrong-row delete left no forensic trail at all.
//
// The mechanism was not "the success path forgot a log". writeCDCToDestination
// duplicates the MCP JSON-RPC HTTP loop instead of going through
// callDestinationTool, so it inherited none of that function's "destination tool
// call ok" logging — which is exactly why upsert_data lines were present in the
// live logs while delete_data lines were absent.
//
// These tests pin the fix AND its privacy constraint: the sink log scrubber is an
// ALLOWLIST (main.go logSafeFields), and the PK VALUE must never be logged — an
// email primary key is customer PII. What ships is a non-reversible fingerprint.

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/segmentio/kafka-go"
)

// captureLogStderr runs fn with os.Stderr redirected to a pipe and returns the
// raw text it wrote. Raw (not decoded) because the privacy assertion below has to
// search the bytes that would reach SigNoz, not a field the decoder picked out.
func captureLogStderr(t *testing.T, fn func()) string {
	t.Helper()

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	orig := os.Stderr
	os.Stderr = w

	fn()

	os.Stderr = orig
	w.Close()
	out, err := io.ReadAll(r)
	r.Close()
	if err != nil {
		t.Fatalf("read captured stderr: %v", err)
	}
	return string(out)
}

// captureLogLines decodes every JSON log line fn emitted.
//
// Deliberately NOT captureLogEvent (log_scrub_test.go): that helper decodes
// exactly ONE line and t.Fatalf's on anything else, and the CDC apply path may
// emit several lines in one call.
func captureLogLines(t *testing.T, fn func()) []map[string]any {
	t.Helper()

	raw := captureLogStderr(t, fn)
	recs := []map[string]any{}
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("log line is not JSON: %v\nline: %s", err, line)
		}
		recs = append(recs, rec)
	}
	return recs
}

// recordsWithMessage filters decoded log records by their "message" field.
func recordsWithMessage(recs []map[string]any, message string) []map[string]any {
	out := []map[string]any{}
	for _, rec := range recs {
		if m, _ := rec["message"].(string); m == message {
			out = append(out, rec)
		}
	}
	return out
}

// scriptedDeleteRT stands in for the destination MCP connector: every call
// succeeds and reports one deleted row. All responses are HTTP 200 — an MCP
// connector signals tool errors via {"success":false} in the JSON-RPC result,
// not via HTTP status (precedent: scriptedMinioRT in claimcheck_retry_test.go).
type scriptedDeleteRT struct {
	calls int32
	body  string
}

func (s *scriptedDeleteRT) RoundTrip(*http.Request) (*http.Response, error) {
	atomic.AddInt32(&s.calls, 1)
	body := s.body
	if body == "" {
		body = `{"jsonrpc":"2.0","id":1,"result":{"success":true,"rows_deleted":1}}`
	}
	return &http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     make(http.Header),
	}, nil
}

// cdcDeleteFixture builds the minimal (cfg, kafka.Message, SinkMessage) triple a
// relational CDC delete needs. ddl is passed as nil by every caller below:
// ensureDestinationTable short-circuits on a nil *DDLSupport, so no destination
// has to exist.
func cdcDeleteFixture(pk map[string]interface{}, keyFields []string) (*WorkerConfig, kafka.Message, *SinkMessage) {
	cfg := &WorkerConfig{
		PipelineID:           "p-ki-cdc-delete-log",
		DestinationConnector: "postgresql",
		DestinationConfig:    map[string]interface{}{},
	}
	msg := kafka.Message{
		Topic:     "cdc.inventory.orders",
		Partition: 2,
		Offset:    4711,
	}
	sm := &SinkMessage{
		PipelineID: "p-ki-cdc-delete-log",
		IsCDC:      true,
		CDCOp:      "d",
		Table:      "inventory.orders",
		TraceID:    "9f8e7d6c5b4a3210",
		PK:         pk,
		Before:     pk,
		KeyFields:  keyFields,
		LSN:        987654321,
		TxID:       "12345678",
	}
	return cfg, msg, sm
}

// (1) The defect itself, reproduced as a test: an applied CDC delete must emit
// exactly one line naming the tool, the destination table, the key columns and
// the reported row count. Against unmodified HEAD this fails with zero captured
// records — that is the bug.
func TestCDCDeleteEmitsLogLine(t *testing.T) {
	cfg, msg, sm := cdcDeleteFixture(map[string]interface{}{"id": 42}, []string{"id"})
	client := &http.Client{Transport: &scriptedDeleteRT{}}

	var n int64
	var err error
	recs := captureLogLines(t, func() {
		n, _, err = writeCDCToDestination(context.Background(), client, cfg, nil, msg, sm, "delete")
	})
	if err != nil {
		t.Fatalf("writeCDCToDestination(delete) returned error: %v", err)
	}
	if n != 1 {
		t.Fatalf("rows deleted = %d, want 1", n)
	}

	hits := recordsWithMessage(recs, "cdc delete applied")
	if len(hits) != 1 {
		t.Fatalf("want exactly 1 \"cdc delete applied\" log line, got %d (all records: %#v)", len(hits), recs)
	}
	rec := hits[0]

	if got, _ := rec["tool"].(string); got != "postgresql_delete_data" {
		t.Errorf("tool = %q, want %q", got, "postgresql_delete_data")
	}
	if got, _ := rec["dest_table"].(string); got != "inventory.orders" {
		t.Errorf("dest_table = %q, want %q", got, "inventory.orders")
	}
	if got, _ := rec["key_fields"].(string); got != "id" {
		t.Errorf("key_fields = %q, want %q", got, "id")
	}
	if got, ok := rec["rows_deleted"].(float64); !ok || int64(got) != 1 {
		t.Errorf("rows_deleted = %#v, want the number 1", rec["rows_deleted"])
	}
	if fp, _ := rec["pk_fingerprint"].(string); fp == "" {
		t.Errorf("pk_fingerprint missing or empty: %#v", rec["pk_fingerprint"])
	}
	// logMsgEvent auto-injects the Kafka coordinate; that is what makes the line
	// correlatable back to the Debezium record.
	if got, _ := rec["table"].(string); got != sm.Table {
		t.Errorf("table = %q, want %q", got, sm.Table)
	}
	if got, _ := rec["topic"].(string); got != msg.Topic {
		t.Errorf("topic = %q, want %q", got, msg.Topic)
	}
}

// (2) The privacy rule, guarded against a later "just log the PK" edit. An email
// primary key is customer data; only the fingerprint may ship.
func TestCDCDeleteLogNeverLeaksPKValue(t *testing.T) {
	cfg, msg, sm := cdcDeleteFixture(map[string]interface{}{"email": "alice@example.com"}, []string{"email"})
	client := &http.Client{Transport: &scriptedDeleteRT{}}

	raw := captureLogStderr(t, func() {
		if _, _, err := writeCDCToDestination(context.Background(), client, cfg, nil, msg, sm, "delete"); err != nil {
			t.Errorf("writeCDCToDestination(delete) returned error: %v", err)
		}
	})

	for _, leaked := range []string{"alice@example.com", "example.com"} {
		if strings.Contains(raw, leaked) {
			t.Errorf("delete log leaked the PK value %q to stderr:\n%s", leaked, raw)
		}
	}
	if !strings.Contains(raw, "cdc delete applied") {
		t.Fatalf("no delete log line was emitted at all:\n%s", raw)
	}
}

// (3) The allowlist half. logSafeFields is an ALLOWLIST and reScrubLongDigits
// masks any run of 7+ digits, so a field left off the list is silently mangled:
// a 12-hex fingerprint containing 7+ consecutive digits becomes "[num-redacted]",
// a numeric count becomes unusable, and an all-digit tx id is destroyed.
func TestCDCDeleteLogFieldsSurviveScrubber(t *testing.T) {
	cfg, msg, sm := cdcDeleteFixture(map[string]interface{}{"id": 42}, []string{"id"})
	client := &http.Client{Transport: &scriptedDeleteRT{}}

	recs := captureLogLines(t, func() {
		if _, _, err := writeCDCToDestination(context.Background(), client, cfg, nil, msg, sm, "delete"); err != nil {
			t.Errorf("writeCDCToDestination(delete) returned error: %v", err)
		}
	})
	hits := recordsWithMessage(recs, "cdc delete applied")
	if len(hits) != 1 {
		t.Fatalf("want exactly 1 \"cdc delete applied\" log line, got %d", len(hits))
	}
	rec := hits[0]

	fp, _ := rec["pk_fingerprint"].(string)
	if !regexp.MustCompile(`^[0-9a-f]{12}$`).MatchString(fp) {
		t.Errorf("pk_fingerprint = %q, want 12 lowercase hex chars (a scrubbed value means the key is missing from logSafeFields)", fp)
	}
	if _, ok := rec["rows_deleted"].(float64); !ok {
		t.Errorf("rows_deleted = %#v, want a number (a string means it was scrubbed)", rec["rows_deleted"])
	}
	if got, _ := rec["tx_id"].(string); got != "12345678" {
		t.Errorf("tx_id = %q, want %q — SinkMessage.TxID is a string of digits and reScrubLongDigits masks 7+ digit runs", got, "12345678")
	}

	// Deterministic coverage for the "pk_fingerprint" allowlist entry.
	//
	// This case exists because the assertions above CANNOT catch a missing entry:
	// {"id":42} fingerprints to 17b4db064e17, which has no run of 7+ digits, so
	// scrubLog would pass it through unchanged and the regex above would still
	// match. rows_deleted cannot catch it either — scrubLogValue only scrubs
	// STRINGS, and that field is an int64.
	//
	// {"id":2} is chosen precisely because it fingerprints to 9e7e65453739, whose
	// "65453739" IS a 7+ digit run. Drop "pk_fingerprint" from logSafeFields and
	// this assertion fails every run, not one in five. The literal is pinned on
	// purpose: it also proves the encoding is canonical and stable (json.Marshal
	// sorts map keys), which is what makes the fingerprint comparable across
	// workers and restarts.
	cfg2, msg2, sm2 := cdcDeleteFixture(map[string]interface{}{"id": 2}, []string{"id"})
	recs2 := captureLogLines(t, func() {
		if _, _, err := writeCDCToDestination(context.Background(), client, cfg2, nil, msg2, sm2, "delete"); err != nil {
			t.Errorf("writeCDCToDestination(delete) returned error: %v", err)
		}
	})
	hits2 := recordsWithMessage(recs2, "cdc delete applied")
	if len(hits2) != 1 {
		t.Fatalf("want exactly 1 \"cdc delete applied\" log line, got %d", len(hits2))
	}
	if got, _ := hits2[0]["pk_fingerprint"].(string); got != "9e7e65453739" {
		t.Errorf("pk_fingerprint = %q, want %q — a %q here means the key is missing from logSafeFields and reScrubLongDigits ate it",
			got, "9e7e65453739", "[num-redacted]")
	}
}

// (4) The control. The delete line is gated on operation=="delete" on purpose: in
// append-only mode every c/r/u/d flows through writeCDCToDestination, so an
// unconditional info line would be one log per event and would make the delete
// line meaningless as a signal.
func TestCDCUpsertEmitsNoDeleteLine(t *testing.T) {
	cfg, msg, sm := cdcDeleteFixture(map[string]interface{}{"id": 42}, []string{"id"})
	sm.CDCOp = "u"
	sm.Data = []map[string]interface{}{{"id": 42, "status": "shipped"}}
	client := &http.Client{Transport: &scriptedDeleteRT{}}

	recs := captureLogLines(t, func() {
		if _, _, err := writeCDCToDestination(context.Background(), client, cfg, nil, msg, sm, "upsert"); err != nil {
			t.Errorf("writeCDCToDestination(upsert) returned error: %v", err)
		}
	})
	if hits := recordsWithMessage(recs, "cdc delete applied"); len(hits) != 0 {
		t.Errorf("upsert emitted %d \"cdc delete applied\" line(s); the delete log must be gated on operation==\"delete\"", len(hits))
	}
}

// (5) flushTable now reports how many rows it flushed so the caller can log the
// ordering barrier. Zero is a meaningful answer, not an error: it means nothing
// was buffered for this table, so the delete did not race any pending upsert.
func TestFlushTableReportsZeroWhenNothingBuffered(t *testing.T) {
	b := &cdcDBBatcher{batches: map[string]*cdcDBBatch{}}
	if got := b.flushTable(context.Background(), "cdc.inventory.orders", 2, "inventory.orders"); got != 0 {
		t.Errorf("flushTable with an empty batch map = %d, want 0", got)
	}

	// The nil-receiver guard must stay a no-op too — dbBatcher is nil for
	// object-storage and warehouse destinations.
	var nilBatcher *cdcDBBatcher
	if got := nilBatcher.flushTable(context.Background(), "cdc.inventory.orders", 2, "inventory.orders"); got != 0 {
		t.Errorf("flushTable on a nil *cdcDBBatcher = %d, want 0", got)
	}
}
