package handlers

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"unicode/utf8"
)

// The payload shape is the one llm-service actually publishes on
// pii.scan.response: llm-service/src/agents/pii_scanner/kafka_consumer.py:185-209
// builds result.tables[].columns[] with exactly these keys, and reports every
// column it looked at, PII or not.
const scannerPayload = `{
  "scan_id": "s-1",
  "connection_id": "c-1",
  "tables_scanned": 2,
  "total_pii_columns_found": 2,
  "scan_method": "pattern",
  "errors": [],
  "tables": [
    {"table_name": "customers", "columns": [
      {"column_name": "email",    "is_pii": true,  "pii_type": "EMAIL_ADDRESS", "confidence": 0.95, "detection_method": "pattern", "suggested_masking": "hash"},
      {"column_name": "id",       "is_pii": false, "pii_type": "", "confidence": 0, "detection_method": "pattern", "suggested_masking": ""},
      {"column_name": "ssn",      "is_pii": true,  "pii_type": "US_SSN", "confidence": 0.88, "detection_method": "ml", "suggested_masking": "redact"}
    ]},
    {"table_name": "orders", "columns": [
      {"column_name": "total", "is_pii": false, "pii_type": "", "confidence": 0, "detection_method": "pattern", "suggested_masking": ""}
    ]}
  ]
}`

func decodeScanPayload(t *testing.T, raw string) map[string]interface{} {
	t.Helper()
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		t.Fatalf("payload does not decode: %v", err)
	}
	return m
}

func TestPIIFindingsFromRealScannerPayload(t *testing.T) {
	findings, scanned := piiFindingsFrom(decodeScanPayload(t, scannerPayload))

	if len(findings) != 2 {
		t.Fatalf("want 2 PII findings, got %d: %+v", len(findings), findings)
	}
	if findings[0].table != "customers" || findings[0].column != "email" ||
		findings[0].piiType != "EMAIL_ADDRESS" || findings[0].confidence != 0.95 ||
		findings[0].method != "pattern" || findings[0].masking != "hash" {
		t.Errorf("first finding not carried through intact: %+v", findings[0])
	}
	if findings[1].column != "ssn" || findings[1].piiType != "US_SSN" {
		t.Errorf("second finding wrong: %+v", findings[1])
	}

	// "orders" was scanned and clean. It must still be reported as scanned, or
	// the prune would never clear a finding that a later scan cleared.
	if len(scanned) != 2 || scanned[0] != "customers" || scanned[1] != "orders" {
		t.Errorf("want both scanned tables including the clean one, got %v", scanned)
	}
}

// A scan that reported on tables but flagged nothing has to be distinguishable
// from a scan that reported nothing at all: the first prunes, the second must
// not (projectScanResults returns early on an empty scanned list).
func TestPIIFindingsFromDistinguishesCleanScanFromEmptyPayload(t *testing.T) {
	clean := `{"tables":[{"table_name":"t","columns":[{"column_name":"c","is_pii":false}]}]}`
	findings, scanned := piiFindingsFrom(decodeScanPayload(t, clean))
	if len(findings) != 0 || len(scanned) != 1 {
		t.Errorf("clean scan: want 0 findings / 1 scanned table, got %d / %v", len(findings), scanned)
	}

	for name, raw := range map[string]string{
		"no tables key": `{"scan_id":"s"}`,
		"empty tables":  `{"tables":[]}`,
		"tables null":   `{"tables":null}`,
	} {
		findings, scanned := piiFindingsFrom(decodeScanPayload(t, raw))
		if len(findings) != 0 || len(scanned) != 0 {
			t.Errorf("%s: want nothing reported, got %d findings / %v scanned", name, len(findings), scanned)
		}
	}
}

func TestPIIFindingsFromSkipsMalformedEntries(t *testing.T) {
	raw := `{"tables":[
      "not-an-object",
      {"table_name": "", "columns": [{"column_name":"x","is_pii":true}]},
      {"table_name": "good", "columns": [
        "not-an-object",
        {"column_name": "", "is_pii": true},
        {"is_pii": true},
        {"column_name": "keep", "is_pii": true}
      ]}
    ]}`
	findings, scanned := piiFindingsFrom(decodeScanPayload(t, raw))

	// A table with no usable name cannot be pruned against, so it is not claimed
	// as scanned either.
	if len(scanned) != 1 || scanned[0] != "good" {
		t.Errorf("want only the named table scanned, got %v", scanned)
	}
	if len(findings) != 1 || findings[0].column != "keep" {
		t.Fatalf("want only the one usable finding, got %+v", findings)
	}
}

// pii_type and detection_method are NOT NULL and are rendered verbatim in the
// UI, so a missing one becomes a word rather than a blank cell.
func TestPIIFindingsFromDefaultsMissingLabels(t *testing.T) {
	raw := `{"tables":[{"table_name":"t","columns":[{"column_name":"c","is_pii":true}]}]}`
	findings, _ := piiFindingsFrom(decodeScanPayload(t, raw))
	if len(findings) != 1 {
		t.Fatalf("want 1 finding, got %d", len(findings))
	}
	if findings[0].piiType != "unknown" || findings[0].method != "unknown" {
		t.Errorf("want both labels defaulted, got type=%q method=%q", findings[0].piiType, findings[0].method)
	}
	if findings[0].masking != "" {
		t.Errorf("suggested_masking is nullable and should stay empty, got %q", findings[0].masking)
	}
}

// confidence carries a CHECK (confidence >= 0 AND confidence <= 1). An
// out-of-range value must not be allowed to abort the whole projection.
func TestPIIClamp01(t *testing.T) {
	cases := []struct{ in, want float64 }{
		{0, 0}, {0.5, 0.5}, {1, 1},
		{1.0000001, 1}, {42, 1},
		{-0.0001, 0}, {-7, 0},
	}
	for _, c := range cases {
		if got := piiClamp01(c.in); got != c.want {
			t.Errorf("piiClamp01(%v) = %v, want %v", c.in, got, c.want)
		}
	}
	// NaN also violates the constraint and must not survive.
	if got := piiClamp01(math.NaN()); got != 0 {
		t.Errorf("piiClamp01(NaN) = %v, want 0", got)
	}
}

// A decoder configured with UseNumber anywhere upstream must not silently turn
// every confidence into zero.
func TestPIIFloatAcceptsJSONNumber(t *testing.T) {
	dec := json.NewDecoder(strings.NewReader(`{"confidence": 0.75}`))
	dec.UseNumber()
	var m map[string]interface{}
	if err := dec.Decode(&m); err != nil {
		t.Fatal(err)
	}
	if _, ok := m["confidence"].(json.Number); !ok {
		t.Fatalf("test setup: want a json.Number, got %T", m["confidence"])
	}
	if got := piiFloat(m, "confidence"); got != 0.75 {
		t.Errorf("piiFloat over json.Number = %v, want 0.75", got)
	}
	if got := piiFloat(map[string]interface{}{"c": "x"}, "c"); got != 0 {
		t.Errorf("piiFloat over a non-number = %v, want 0", got)
	}
}

// Postgres aborts the statement on an over-wide value, which would cost the
// scan its entire result set. VARCHAR(n) counts characters, so the cut is by
// rune — cutting mid-rune would produce invalid UTF-8.
func TestPIITruncateIsRuneSafe(t *testing.T) {
	if got := piiTruncate("short", 50); got != "short" {
		t.Errorf("a value that fits must be untouched, got %q", got)
	}

	wide := strings.Repeat("é", 80) // 2 bytes per rune
	got := piiTruncate(wide, 50)
	if len([]rune(got)) != 50 {
		t.Errorf("want 50 runes, got %d", len([]rune(got)))
	}
	if !utf8.ValidString(got) {
		t.Errorf("truncation produced invalid UTF-8: %q", got)
	}

	// And it is applied where it matters: a pii_type wider than VARCHAR(50).
	raw := `{"tables":[{"table_name":"t","columns":[
      {"column_name":"c","is_pii":true,"pii_type":"` + strings.Repeat("A", 200) + `"}]}]}`
	findings, _ := piiFindingsFrom(decodeScanPayload(t, raw))
	if len(findings) != 1 || len([]rune(findings[0].piiType)) != 50 {
		t.Errorf("want pii_type clipped to 50 runes, got %d", len([]rune(findings[0].piiType)))
	}
}

func utf8Valid(s string) bool {
	for _, r := range s {
		if r == '�' {
			return false
		}
	}
	return true
}

// The delimiters must not be producible by a schema scan, or a crafted table
// name could smuggle an extra key into the prune list.
func TestPIIKeySeparatorsAreControlCharacters(t *testing.T) {
	if piiKeyFieldSep != "\x1f" || piiKeyRecordSep != "\x1e" {
		t.Fatalf("separators changed: field=%q record=%q", piiKeyFieldSep, piiKeyRecordSep)
	}
	if strings.ContainsAny("customers_table.column-name 123", piiKeyFieldSep+piiKeyRecordSep) {
		t.Error("a realistic identifier must not contain the separators")
	}
}
