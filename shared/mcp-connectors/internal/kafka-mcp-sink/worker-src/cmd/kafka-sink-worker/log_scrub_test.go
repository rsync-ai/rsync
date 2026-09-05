package main

import (
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"
)

// captureLogEvent runs fn with stderr redirected and returns the decoded JSON of
// the single log line it emitted.
func captureLogEvent(t *testing.T, fn func()) map[string]any {
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

	line := strings.TrimSpace(string(out))
	var rec map[string]any
	if err := json.Unmarshal([]byte(line), &rec); err != nil {
		t.Fatalf("log line is not JSON: %v\nline: %s", err, line)
	}
	return rec
}

// The leak this closes, observed live on prod 2026-07-31: a destination driver
// error carrying the offending ROW VALUES was passed through a logEvent FIELD.
// logf/logMsgEvent scrubbed their message text, but nothing scrubbed fields, so
// the raw values shipped to SigNoz — a metadata-only privacy rule violation.
func TestLogEventScrubsErrorField(t *testing.T) {
	pgErr := `ERROR: duplicate key value violates unique constraint "customers_email_key" ` +
		`(SQLSTATE 23505) DETAIL: Key (email)=(alice@example.com) already exists. ` +
		`Failing row contains (91827364, alice@example.com, 555-123-4567).`

	rec := captureLogEvent(t, func() {
		logEvent("warn", "destination tool call failed",
			"tool", "postgres_upsert", "host", "dest-db", "error", pgErr)
	})

	got, _ := rec["error"].(string)
	if got == "" {
		t.Fatalf("error field missing from log record: %#v", rec)
	}
	for _, leaked := range []string{"alice@example.com", "555-123-4567", "91827364"} {
		if strings.Contains(got, leaked) {
			t.Errorf("error field leaked %q to the log: %s", leaked, got)
		}
	}
}

// The same value reaching the log through the MESSAGE rather than a field.
func TestLogEventScrubsMessage(t *testing.T) {
	rec := captureLogEvent(t, func() {
		logEvent("error", `insert failed: Failing row contains (7, bob@acme.com)`)
	})

	msg, _ := rec["message"].(string)
	if strings.Contains(msg, "bob@acme.com") {
		t.Errorf("message leaked a row value: %s", msg)
	}
}

// Metadata fields must survive intact — scrubbing them would make the logs
// useless for correlation. Counts are the sharp edge: the digit rule masks any
// run of 7+ digits, so a million-row write must still log its real count.
func TestLogEventPreservesMetadataFields(t *testing.T) {
	rec := captureLogEvent(t, func() {
		logEvent("info", "destination tool call ok",
			"tool", "postgres_upsert",
			"host", "dest-db",
			"table", "public.customers",
			"topic", "cdc-12345678.inventory.orders",
			"trace_id", "9f8e7d6c5b4a3210",
			"partition", 3,
			"offset", 1048576,
			"rows_written", "1250000",
		)
	})

	want := map[string]any{
		"tool":         "postgres_upsert",
		"host":         "dest-db",
		"table":        "public.customers",
		"topic":        "cdc-12345678.inventory.orders",
		"trace_id":     "9f8e7d6c5b4a3210",
		"rows_written": "1250000",
	}
	for k, v := range want {
		if rec[k] != v {
			t.Errorf("field %q = %#v, want %#v (metadata must not be scrubbed)", k, rec[k], v)
		}
	}
}

// Unknown keys are scrubbed: the allowlist has to fail closed, or every new
// field that happens to carry error text becomes a fresh leak.
func TestLogEventScrubsUnknownField(t *testing.T) {
	rec := captureLogEvent(t, func() {
		logEvent("warn", "something failed",
			"some_new_diagnostic", "Key (email)=(carol@example.com) already exists")
	})

	got, _ := rec["some_new_diagnostic"].(string)
	if strings.Contains(got, "carol@example.com") {
		t.Errorf("unknown field was not scrubbed: %s", got)
	}
}
