package handlers

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/rsync-ai/backend-orchestrator/pkg/llmscrub"
	log "github.com/sirupsen/logrus"
)

// signoz_logs.go — Phase 3b: log-aware diagnose.
//
// computePipelineFlow (pipeline_flow.go) tells us *which* stage stalled or failed
// and carries that stage's OTel trace_id. This file closes the loop by pulling the
// actual log lines for that trace out of self-hosted SigNoz, so the diagnose
// evidence shows *why* — the verbatim error lines around the failure — not just a
// structural "stage X never completed".
//
// Read path: SigNoz stores logs in ClickHouse (signoz_logs.distributed_logs_v2).
// We query ClickHouse's HTTP interface directly (POST SQL, FORMAT JSONEachRow) —
// no ClickHouse Go driver dependency, no SigNoz query-service JWT. The default
// user has no password in the self-hosted compose. Reachability mirrors the
// otel-collector path: host.docker.internal:8123 (8123 exposed by the SigNoz
// compose).
//
// Everything here is BEST-EFFORT and FAIL-SOFT: if SigNoz is not deployed,
// unreachable, slow, or returns junk, we silently attach nothing. Diagnose must
// never break because the log backend is down.

// signozLogLine is one enriched log row surfaced in the flow evidence.
type signozLogLine struct {
	Timestamp string `json:"timestamp"`
	Severity  string `json:"severity"`
	Service   string `json:"service"`
	Body      string `json:"body"`
}

// signozLogsHTTPURL returns the ClickHouse HTTP base URL to query, or "" when log
// enrichment is disabled. Default targets the host-exposed SigNoz ClickHouse; set
// SIGNOZ_CLICKHOUSE_HTTP_URL to override, or SIGNOZ_LOGS_ENRICH=false to disable.
func signozLogsHTTPURL() string {
	if v := strings.ToLower(strings.TrimSpace(os.Getenv("SIGNOZ_LOGS_ENRICH"))); v == "false" || v == "0" || v == "off" {
		return ""
	}
	if u := strings.TrimSpace(os.Getenv("SIGNOZ_CLICKHOUSE_HTTP_URL")); u != "" {
		return strings.TrimRight(u, "/")
	}
	return "http://host.docker.internal:8123"
}

// fetchLogsByTraceID queries SigNoz ClickHouse for log lines on the given trace,
// errors/warnings first, then most recent. Returns nil on any failure (fail-soft).
func fetchLogsByTraceID(ctx context.Context, traceID string, limit int) []signozLogLine {
	base := signozLogsHTTPURL()
	if base == "" || strings.TrimSpace(traceID) == "" {
		return nil
	}
	if limit <= 0 || limit > 100 {
		limit = 25
	}

	// Parameterised query — trace_id is bound via ClickHouse's param_<name> HTTP
	// params, never string-interpolated, so a malformed trace_id can't inject SQL.
	// timestamp is UInt64 nanoseconds; severity_number follows OTel (WARN>=13,
	// ERROR>=17, FATAL>=21) so we float the loud lines to the top.
	const query = `SELECT
  toString(toDateTime64(timestamp/1000000000, 3)) AS timestamp,
  severity_text AS severity,
  resources_string['service.name'] AS service,
  substring(body, 1, 2000) AS body
FROM signoz_logs.distributed_logs_v2
WHERE trace_id = {tid:String}
ORDER BY (severity_number >= 17) DESC, (severity_number >= 13) DESC, timestamp DESC
LIMIT {lim:UInt32}
FORMAT JSONEachRow`

	q := url.Values{}
	q.Set("param_tid", traceID)
	q.Set("param_lim", strconv.Itoa(limit))
	// Tight per-statement budget — diagnose should not block on a slow query.
	q.Set("max_execution_time", "3")
	endpoint := base + "/?" + q.Encode()

	reqCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, endpoint, bytes.NewReader([]byte(query)))
	if err != nil {
		log.Debugf("diagnose flow: build signoz request failed: %v", err)
		return nil
	}
	req.Header.Set("Content-Type", "text/plain")
	if u := strings.TrimSpace(os.Getenv("SIGNOZ_CLICKHOUSE_USER")); u != "" {
		req.SetBasicAuth(u, os.Getenv("SIGNOZ_CLICKHOUSE_PASSWORD"))
	}

	client := &http.Client{Timeout: 4 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		log.Debugf("diagnose flow: signoz unreachable (%s): %v", base, err)
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		log.Debugf("diagnose flow: signoz query returned %d", resp.StatusCode)
		return nil
	}

	out := make([]signozLogLine, 0, limit)
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20) // allow long log bodies
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var rec signozLogLine
		if err := json.Unmarshal(line, &rec); err != nil {
			continue
		}
		// These verbatim log lines feed LLM diagnose evidence; sink write errors
		// can embed failed row values (privacy contract: metadata only).
		rec.Body = llmscrub.Scrub(rec.Body)
		out = append(out, rec)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// fetchSinkErrorsByPipelineID queries SigNoz for sink write error log lines for
// the given pipeline (filtered by pipeline_id attribute). Used to surface the
// verbatim error — e.g. "there is no unique or exclusion constraint matching the
// ON CONFLICT specification" — when inserted_rows == 0 and no stalled_stage
// trace is available. Returns nil on any failure (fail-soft).
func fetchSinkErrorsByPipelineID(ctx context.Context, pipelineID string, limit int) []signozLogLine {
	base := signozLogsHTTPURL()
	if base == "" || strings.TrimSpace(pipelineID) == "" {
		return nil
	}
	if limit <= 0 || limit > 100 {
		limit = 10
	}

	const query = `SELECT
  toString(toDateTime64(timestamp/1000000000, 3)) AS timestamp,
  severity_text AS severity,
  resources_string['service.name'] AS service,
  substring(body, 1, 2000) AS body
FROM signoz_logs.distributed_logs_v2
WHERE attributes_string['pipeline_id'] = {pid:String}
  AND (
    body LIKE '%dest write failed%'
    OR body LIKE '%ON CONFLICT%'
    OR body LIKE '%reconciliation%'
    OR body LIKE '%deadline reached%'
  )
ORDER BY timestamp DESC
LIMIT {lim:UInt32}
FORMAT JSONEachRow`

	q := url.Values{}
	q.Set("param_pid", pipelineID)
	q.Set("param_lim", strconv.Itoa(limit))
	q.Set("max_execution_time", "3")
	endpoint := base + "/?" + q.Encode()

	reqCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, endpoint, bytes.NewReader([]byte(query)))
	if err != nil {
		log.Debugf("diagnose sink-errors: build signoz request failed: %v", err)
		return nil
	}
	req.Header.Set("Content-Type", "text/plain")
	if u := strings.TrimSpace(os.Getenv("SIGNOZ_CLICKHOUSE_USER")); u != "" {
		req.SetBasicAuth(u, os.Getenv("SIGNOZ_CLICKHOUSE_PASSWORD"))
	}

	client := &http.Client{Timeout: 4 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		log.Debugf("diagnose sink-errors: signoz unreachable (%s): %v", base, err)
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		log.Debugf("diagnose sink-errors: signoz query returned %d", resp.StatusCode)
		return nil
	}

	out := make([]signozLogLine, 0, limit)
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var rec signozLogLine
		if err := json.Unmarshal(line, &rec); err != nil {
			continue
		}
		// These verbatim log lines feed LLM diagnose evidence; sink write errors
		// can embed failed row values (privacy contract: metadata only).
		rec.Body = llmscrub.Scrub(rec.Body)
		out = append(out, rec)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// enrichEvidenceWithSinkErrors attaches verbatim sink write error log lines from
// SigNoz onto the evidence map when the execution failed with a silent-drop
// pattern. This gives the LLM the actual error text (e.g. "there is no unique
// or exclusion constraint matching the ON CONFLICT specification") so it can
// output an actionable root cause (missing PK) instead of a generic "silent
// drop" report. Triggers on failed execution with silent_drop in error_message —
// not on aggregate inserted_rows, which can be non-zero when only some tables
// fail. Always fail-soft.
func enrichEvidenceWithSinkErrors(ctx context.Context, pipelineID string, evidence map[string]interface{}) {
	exec, _ := evidence["latest_execution"].(map[string]interface{})
	if exec == nil {
		return
	}
	execStatus, _ := exec["status"].(string)
	if execStatus != "failed" {
		return
	}
	errMsg, _ := exec["error_message"].(string)
	if !strings.Contains(errMsg, "silent_drop") && !strings.Contains(errMsg, "dest write failed") {
		return
	}
	logs := fetchSinkErrorsByPipelineID(ctx, pipelineID, 10)
	if len(logs) == 0 {
		return
	}
	evidence["sink_error_detail"] = logs
}

// enrichFlowWithSigNozLogs attaches verbatim SigNoz log lines for the stalled or
// failed stage onto the flow report (in place). No-op when the run completed
// cleanly, when no trace_id is available, or when SigNoz returns nothing. Always
// fail-soft — never mutates correctness-bearing fields, only adds evidence.
func enrichFlowWithSigNozLogs(ctx context.Context, flow map[string]interface{}) {
	if flow == nil {
		return
	}
	if available, ok := flow["available"].(bool); ok && !available {
		return
	}
	// Only enrich when there is a problem stage worth explaining. A clean
	// completion (no stalled stage) has no error logs to fetch.
	stalled, _ := flow["stalled_stage"].(string)
	if strings.TrimSpace(stalled) == "" {
		return
	}
	traceID, _ := flow["stalled_stage_trace_id"].(string)
	if strings.TrimSpace(traceID) == "" {
		return
	}

	logs := fetchLogsByTraceID(ctx, traceID, 25)
	if len(logs) == 0 {
		return
	}
	flow["stalled_stage_logs"] = logs
	flow["stalled_stage_logs_source"] = "signoz:" + traceID
}
