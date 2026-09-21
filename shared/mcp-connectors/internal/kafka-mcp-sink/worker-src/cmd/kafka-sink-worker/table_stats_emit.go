package main

import (
	"errors"
	"fmt"
	"sync/atomic"
)

// noteTableStatsEmit is the single place a TABLE_STATS publish failure is handled.
//
// TABLE_STATS is how the destination's applied counts reach the product: the
// api-gateway projector upserts them into pipeline_run_table_stats, which feeds the
// per-table counts in the UI, the captured-vs-applied reconciliation, and the
// "run after pipeline" trigger for CDC pipelines. All eight emit sites used to be
// `_ = emitCDCTableStats(...)` / `_ = emitTableStats(...)`, so a broker outage or a
// misconfigured events topic left every one of those surfaces frozen with nothing in
// the logs, nothing on /status and no counter anywhere.
//
// It stays NON-FATAL on purpose: the rows have already landed in the destination and
// their offsets are (or are about to be) committed. Failing the batch over a
// bookkeeping event would redeliver rows that are already applied. So a failure is
// made loud — log line, /status last_error, and table_stats_emit_failures_total —
// and nothing else changes.
//
// The call sites pass the emit call's result straight in, so an emit can never be
// added without a handler (cmd/kafka-sink-worker/table_stats_emit_test.go censuses
// main.go for any `_ = emit…TableStats(` that creeps back).
//
// Privacy: the log carries identifiers, the table NAME and the mode; the error text
// goes through logEvent's scrubber like every other field, and the copy stored for
// /status is scrubbed the same way. No row value reaches either.
func noteTableStatsEmit(metrics *Metrics, sm *SinkMessage, mode string, err error) {
	if err == nil {
		return
	}
	table := ""
	fields := []any{"stats_mode", mode}
	if sm != nil {
		table = sm.Table
		if sm.PipelineID != "" {
			fields = append(fields, "pipeline_id", sm.PipelineID)
		}
		if sm.ExecutionID != "" {
			fields = append(fields, "execution_id", sm.ExecutionID)
		}
		if sm.TraceID != "" {
			fields = append(fields, "trace_id", sm.TraceID)
		}
		if sm.Table != "" {
			fields = append(fields, "table", sm.Table)
		}
	}
	if metrics != nil {
		atomic.AddUint64(&metrics.tableStatsEmitFailures, 1)
		metrics.setErr(fmt.Errorf("table stats emit failed (mode=%s table=%s): %w",
			mode, table, errors.New(scrubLog(err.Error()))))
	}
	fields = append(fields, "error", err.Error())
	logEvent("warning",
		"TABLE_STATS emit failed; rows already applied are unaffected, but per-table counts and the after-pipeline trigger will lag until the next successful emit",
		fields...)
}
