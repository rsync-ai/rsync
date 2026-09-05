package handlers

import (
	"database/sql"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
)

// pipeline_flow.go — structural "where did the run stop?" verification.
//
// The diagnose path historically fed the LLM a flat bag of signals (progress,
// deps, recent events) and asked it to infer where a pipeline broke. That works
// but buries the single most useful triage fact: *which stage did the run reach,
// and where did it stall?* This file folds the pipeline_run_events stream onto
// the canonical stage sequence so that fact is computed deterministically (no LLM
// guess, no SigNoz dependency) and surfaced as evidence.
//
// SigNoz log enrichment (pulling the actual error log lines for the stalled
// stage via its trace_id) is a deliberate follow-up — this is the robust
// structural backbone it will hang off.

// canonicalPipelineStages is the ordered backbone of a batch pipeline run, as
// emitted by the Temporal workflow (nl_pipeline_v2_workflow.go emitStageEvent).
// connector_generation is intentionally omitted: it is a *nested, conditional*
// stage (only when a connector must be built), so its absence is not a stall.
var canonicalPipelineStages = []string{
	"intent",
	"capability_resolver",
	"connector_check",
	"connection_validation",
	"planner",
	"validator",
	"executor",
}

// Per-stage status values.
const (
	flowStageNotReached = "not_reached"
	flowStageStarted    = "started" // entered but neither completed nor failed (in-progress or stalled)
	flowStageCompleted  = "completed"
	flowStageFailed     = "failed"
)

// stageState accumulates what we learned about one stage from the event stream.
type stageState struct {
	status      string
	startedAt   *time.Time
	finishedAt  *time.Time // completion or failure time
	severity    string
	traceID     string // OTel trace_id carried on the stage's events (for SigNoz log enrichment)
}

// computePipelineFlow reads the pipeline_run_events stream for the given
// pipeline (scoped to executionID when non-empty) and returns a deterministic
// flow report: per-stage status, the last completed stage, where it stalled, the
// terminal status, and a one-line human summary. Always fail-soft — on any query
// error it returns a minimal report rather than failing the diagnose call.
func computePipelineFlow(database *sql.DB, pipelineID, executionID, syncMode string) map[string]interface{} {
	report := map[string]interface{}{
		"sync_mode": syncMode,
	}

	// Pull the stage/terminal events oldest-first so we can replay them. Scope to
	// the execution when known to avoid mixing reruns; otherwise fall back to the
	// whole pipeline.
	var (
		rows *sql.Rows
		err  error
	)
	if executionID != "" {
		rows, err = database.Query(`
			SELECT event_type, COALESCE(stage_id, ''), COALESCE(severity, ''),
			       COALESCE(trace_id, ''), COALESCE(occurred_at, received_at)
			FROM pipeline_run_events
			WHERE pipeline_id = $1 AND execution_id = $2
			ORDER BY COALESCE(occurred_at, received_at) ASC`, pipelineID, executionID)
	} else {
		rows, err = database.Query(`
			SELECT event_type, COALESCE(stage_id, ''), COALESCE(severity, ''),
			       COALESCE(trace_id, ''), COALESCE(occurred_at, received_at)
			FROM pipeline_run_events
			WHERE pipeline_id = $1
			ORDER BY COALESCE(occurred_at, received_at) ASC`, pipelineID)
	}
	if err != nil {
		log.Debugf("diagnose flow: events query failed: %v", err)
		report["available"] = false
		return report
	}
	defer rows.Close()

	states := map[string]*stageState{}
	for _, s := range canonicalPipelineStages {
		states[s] = &stageState{status: flowStageNotReached}
	}

	terminal := ""
	considered := 0
	lastTraceID := "" // most recent non-empty trace_id seen — fallback for stages with none
	for rows.Next() {
		var eventType, stageID, severity, traceID string
		var ts time.Time
		if err := rows.Scan(&eventType, &stageID, &severity, &traceID, &ts); err != nil {
			continue
		}
		considered++
		if traceID != "" {
			lastTraceID = traceID
		}

		switch eventType {
		case "PIPELINE_COMPLETED", "PIPELINE_FAILED", "PIPELINE_WAITING":
			terminal = eventType
			continue
		}

		st, ok := states[stageID]
		if !ok {
			continue // non-canonical / nested stage (e.g. connector_generation)
		}
		// Capture the trace_id for this stage — prefer the latest non-empty one so a
		// STAGE_FAILED's trace (the interesting one for log enrichment) wins over the
		// earlier STAGE_STARTED's.
		if traceID != "" {
			st.traceID = traceID
		}
		switch eventType {
		case "STAGE_STARTED":
			// Don't downgrade a stage that already completed/failed (handles reruns
			// within the same execution: keep the strongest terminal signal).
			if st.status == flowStageNotReached {
				st.status = flowStageStarted
				t := ts
				st.startedAt = &t
			}
		case "STAGE_COMPLETED":
			st.status = flowStageCompleted
			t := ts
			st.finishedAt = &t
		case "STAGE_FAILED":
			st.status = flowStageFailed
			t := ts
			st.finishedAt = &t
			if severity != "" {
				st.severity = severity
			}
		}
	}

	// Project per-stage rows in canonical order + derive the stall point.
	stages := make([]map[string]interface{}, 0, len(canonicalPipelineStages))
	lastCompleted := ""
	failedStage := ""
	firstIncomplete := "" // earliest started-but-unfinished or not_reached
	for _, name := range canonicalPipelineStages {
		st := states[name]
		row := map[string]interface{}{"name": name, "status": st.status}
		if st.startedAt != nil {
			row["started_at"] = st.startedAt.Format(time.RFC3339)
		}
		if st.finishedAt != nil {
			row["finished_at"] = st.finishedAt.Format(time.RFC3339)
		}
		if st.severity != "" {
			row["severity"] = st.severity
		}
		if st.traceID != "" {
			row["trace_id"] = st.traceID
		}
		stages = append(stages, row)

		switch st.status {
		case flowStageCompleted:
			lastCompleted = name
		case flowStageFailed:
			if failedStage == "" {
				failedStage = name
			}
		case flowStageStarted, flowStageNotReached:
			if firstIncomplete == "" {
				firstIncomplete = name
			}
		}
	}

	// Determine where the run stalled. A failed stage is the strongest signal; a
	// CDC executor that is merely "started" is normal (streaming), not a stall.
	stalled := ""
	switch {
	case failedStage != "":
		stalled = failedStage
	case terminal == "PIPELINE_COMPLETED":
		stalled = "" // ran to completion
	case syncMode == "cdc" && states["executor"].status == flowStageStarted:
		stalled = "" // CDC executor stays "started" while streaming — not a stall
	default:
		stalled = firstIncomplete
	}

	// trace_id to hang SigNoz log enrichment off: prefer the stalled/failed stage's
	// own trace, fall back to the run's most recent trace when the stall point never
	// emitted one (e.g. a "not_reached" stage). Empty when no event carried a trace.
	stalledTraceID := ""
	if stalled != "" {
		if st, ok := states[stalled]; ok {
			stalledTraceID = st.traceID
		}
	}
	if stalledTraceID == "" {
		stalledTraceID = lastTraceID
	}

	report["available"] = true
	report["events_considered"] = considered
	report["stages"] = stages
	report["last_completed_stage"] = lastCompleted
	report["stalled_stage"] = stalled
	report["stalled_stage_trace_id"] = stalledTraceID
	report["terminal_status"] = terminal
	report["summary"] = flowSummary(syncMode, lastCompleted, stalled, failedStage, terminal, considered)
	return report
}

// flowSummary renders a single human-readable triage line from the derived flow.
func flowSummary(syncMode, lastCompleted, stalled, failedStage, terminal string, considered int) string {
	if considered == 0 {
		return "No pipeline_run_events recorded yet — the run has not emitted any stage events."
	}
	if terminal == "PIPELINE_COMPLETED" && stalled == "" {
		return "Run reached PIPELINE_COMPLETED; all canonical stages completed."
	}
	if failedStage != "" {
		return fmt.Sprintf("Run failed at stage '%s' (STAGE_FAILED). Last stage to complete: %s.",
			failedStage, orNone(lastCompleted))
	}
	if syncMode == "cdc" && stalled == "" {
		return fmt.Sprintf("CDC run is streaming in the 'executor' stage (last completed setup stage: %s).",
			orNone(lastCompleted))
	}
	if stalled != "" {
		if terminal == "PIPELINE_WAITING" {
			return fmt.Sprintf("Run is WAITING at stage '%s' (last completed: %s) — likely HITL/approval or credential reauth.",
				stalled, orNone(lastCompleted))
		}
		return fmt.Sprintf("Run stalled at stage '%s' — it was entered but never completed (last completed: %s).",
			stalled, orNone(lastCompleted))
	}
	return fmt.Sprintf("Run progressed through %s; no failure detected (terminal: %s).",
		orNone(lastCompleted), orNone(terminal))
}

func orNone(s string) string {
	if s == "" {
		return "(none)"
	}
	return s
}
