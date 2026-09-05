package handlers

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

// signoz_logs_test.go — Phase 3b live integration test.
//
// Exercises the real fetchLogsByTraceID + enrichFlowWithSigNozLogs against a live
// SigNoz ClickHouse. It is GATED on SIGNOZ_TEST_HTTP_URL: with no SigNoz reachable
// (e.g. CI), the test SKIPS rather than fails, so it never breaks the build. Run
// locally with:
//
//	SIGNOZ_TEST_HTTP_URL=http://localhost:8123 go test ./internal/handlers/ -run SigNoz -v
//
// (8123 = the ClickHouse HTTP port the SigNoz compose exposes for log enrichment.)

func signozTestEndpoint(t *testing.T) string {
	t.Helper()
	u := strings.TrimSpace(os.Getenv("SIGNOZ_TEST_HTTP_URL"))
	if u == "" {
		t.Skip("SIGNOZ_TEST_HTTP_URL not set — skipping live SigNoz log-enrichment test")
	}
	return strings.TrimRight(u, "/")
}

// discoverTraceWithLogs returns any trace_id that currently has at least one log
// row in SigNoz, or "" if none / unreachable.
func discoverTraceWithLogs(ctx context.Context, base string) string {
	const q = `SELECT trace_id FROM signoz_logs.distributed_logs_v2 WHERE trace_id != '' ORDER BY timestamp DESC LIMIT 1 FORMAT TabSeparated`
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/", bytes.NewReader([]byte(q)))
	if err != nil {
		return ""
	}
	resp, err := (&http.Client{Timeout: 4 * time.Second}).Do(req)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return ""
	}
	sc := bufio.NewScanner(resp.Body)
	if sc.Scan() {
		return strings.TrimSpace(sc.Text())
	}
	return ""
}

func TestSigNozLogEnrichment_Live(t *testing.T) {
	base := signozTestEndpoint(t)
	// Point the production code at the test endpoint.
	t.Setenv("SIGNOZ_CLICKHOUSE_HTTP_URL", base)

	ctx := context.Background()
	tid := discoverTraceWithLogs(ctx, base)
	if tid == "" {
		t.Skip("no trace_id with logs found in SigNoz — nothing to enrich against")
	}
	t.Logf("using trace_id with logs: %s", tid)

	// 1) Direct fetch path returns at least one structured line.
	logs := fetchLogsByTraceID(ctx, tid, 25)
	if len(logs) == 0 {
		t.Fatalf("fetchLogsByTraceID returned 0 lines for a trace known to have logs")
	}
	if logs[0].Timestamp == "" || logs[0].Service == "" {
		// body/severity can legitimately be empty; timestamp+service should not.
		t.Errorf("first log line missing core fields: %+v", logs[0])
	}
	if b, _ := json.Marshal(logs[0]); len(b) == 0 {
		t.Errorf("log line did not marshal back to JSON")
	}

	// 2) Enrichment attaches the lines onto a flow report when a stalled stage
	//    with that trace is present.
	flow := map[string]interface{}{
		"available":              true,
		"stalled_stage":          "executor",
		"stalled_stage_trace_id": tid,
	}
	enrichFlowWithSigNozLogs(ctx, flow)
	got, ok := flow["stalled_stage_logs"].([]signozLogLine)
	if !ok || len(got) == 0 {
		t.Fatalf("enrichFlowWithSigNozLogs did not attach stalled_stage_logs (present=%v)", ok)
	}
	if src, _ := flow["stalled_stage_logs_source"].(string); src != "signoz:"+tid {
		t.Errorf("stalled_stage_logs_source = %q, want signoz:%s", src, tid)
	}

	// 3) Fail-soft: a clean run (no stalled stage) must not be enriched.
	clean := map[string]interface{}{"available": true, "stalled_stage": ""}
	enrichFlowWithSigNozLogs(ctx, clean)
	if _, present := clean["stalled_stage_logs"]; present {
		t.Errorf("enrichment ran on a clean run with no stalled stage")
	}

	// 4) Fail-soft: a stalled stage with no trace_id must not be enriched.
	noTrace := map[string]interface{}{"available": true, "stalled_stage": "executor", "stalled_stage_trace_id": ""}
	enrichFlowWithSigNozLogs(ctx, noTrace)
	if _, present := noTrace["stalled_stage_logs"]; present {
		t.Errorf("enrichment ran on a stalled stage with no trace_id")
	}
}

// TestSigNozLogsDisabled verifies the kill-switch short-circuits before any I/O.
func TestSigNozLogsDisabled(t *testing.T) {
	t.Setenv("SIGNOZ_LOGS_ENRICH", "false")
	if u := signozLogsHTTPURL(); u != "" {
		t.Errorf("SIGNOZ_LOGS_ENRICH=false should disable enrichment, got url %q", u)
	}
	if got := fetchLogsByTraceID(context.Background(), "deadbeef", 5); got != nil {
		t.Errorf("disabled enrichment should return nil, got %d lines", len(got))
	}
}
