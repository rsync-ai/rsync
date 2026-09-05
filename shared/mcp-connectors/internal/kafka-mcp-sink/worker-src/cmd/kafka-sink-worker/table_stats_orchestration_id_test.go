package main

// KI-CDC-SYNTHETIC-EXECUTION-NEVER-CLOSED (correlation half): a CDC pipeline's sink logs
// and its stats rows shared no id at all.
//
// parseCDCMessage forces sm.ExecutionID = sm.PipelineID so the captured-side counters
// (orchestrator cdcstats) and the applied-side counters (this sink) upsert into one
// pipeline_run_table_stats row — migration 034's unique index is
// (pipeline_id, execution_id, qualified_name). That convention is load-bearing and stays:
// two hard FKs hang off the matching executions row (migrations 043, 045).
//
// Its cost was that the execution id every sink log line carries appeared in NO stats row,
// so "which run produced these numbers?" was answerable only by knowing the convention
// existed. These tests pin the id being carried alongside — and pin the three cases where
// it must stay ABSENT, because a blank is an answer and would overwrite a stored one
// through the projector's COALESCE (migration 090).

import (
	"encoding/json"
	"testing"

	"github.com/segmentio/kafka-go"
)

// The whole point: on the CDC lane the two ids differ, and the stats must carry both.
func TestCDCTableStatsCarriesTheOrchestrationExecutionID(t *testing.T) {
	sm := &SinkMessage{
		PipelineID:               "20912e3b-0000-4000-8000-000000000001",
		ExecutionID:              "20912e3b-0000-4000-8000-000000000001", // rewritten to pipeline id
		OrchestrationExecutionID: "7f3d9c21-0000-4000-8000-0000000000aa", // what the logs say
		Table:                    "pipeline_test.demo_products",
	}

	meta, ok := buildCDCTableStatsEvent(sm, 10, 5, 2, 1024, 0)["metadata"].(map[string]interface{})
	if !ok {
		t.Fatalf("event has no metadata map")
	}

	// Unchanged, and load-bearing — see the file header.
	if got := meta["orchestration_execution_id"]; got != "7f3d9c21-0000-4000-8000-0000000000aa" {
		t.Errorf("orchestration_execution_id = %#v, want %q — without it nothing joins a CDC "+
			"sink log line back to the stats row it produced",
			got, "7f3d9c21-0000-4000-8000-0000000000aa")
	}
}

// "Nothing to add" and "a sink older than this change" must read identically, because the
// projector COALESCEs this column: cdcstats upserts the same row on every tick, and a "" or
// a duplicate would erase or muddy what the sink recorded seconds earlier.
func TestTableStatsOmitOrchestrationIDWhenItSaysNothingNew(t *testing.T) {
	for name, sm := range map[string]*SinkMessage{
		// Batch lane: ExecutionID already IS the orchestration id.
		"ids agree": {
			PipelineID:               "p",
			ExecutionID:              "7f3d9c21-0000-4000-8000-0000000000aa",
			OrchestrationExecutionID: "7f3d9c21-0000-4000-8000-0000000000aa",
			Table:                    "public.orders",
		},
		// Pre-090 plumbing, or a message the parse path never carried one for.
		"never told one": {
			PipelineID:  "p",
			ExecutionID: "e",
			Table:       "public.orders",
		},
		"whitespace only": {
			PipelineID:               "p",
			ExecutionID:              "e",
			OrchestrationExecutionID: "   ",
			Table:                    "public.orders",
		},
	} {
		meta := buildCDCTableStatsEvent(sm, 1, 0, 0, 8, 0)["metadata"].(map[string]interface{})
		if v, present := meta["orchestration_execution_id"]; present {
			t.Errorf("%s: orchestration_execution_id present as %#v; it must be absent so the "+
				"projector stores NULL and the COALESCE leaves an existing value alone", name, v)
		}
	}
}

// The batch builder is a separate function and was deliberately left alone: on that lane
// execution_id is already the orchestration id, so there is nothing to correlate.
func TestBatchTableStatsNeverCarriesTheOrchestrationID(t *testing.T) {
	sm := &SinkMessage{
		PipelineID:               "p",
		ExecutionID:              "e",
		OrchestrationExecutionID: "7f3d9c21-0000-4000-8000-0000000000aa",
		Table:                    "public.orders",
	}
	meta := buildTableStatsEvent(sm, "batch", "completed", 1, 1, 8)["metadata"].(map[string]interface{})
	if v, present := meta["orchestration_execution_id"]; present {
		t.Errorf("batch stats carry orchestration_execution_id = %#v; execution_id already IS "+
			"that id on this lane", v)
	}
}

// The builder above is only reachable with a field something set. This proves the live
// parse path sets it — so a green suite above cannot coexist with a field that is always
// empty in production — and, critically, that it captures the id BEFORE the CDC rewrite.
func TestParseCDCMessageKeepsTheOrchestrationIDBeforeRewritingExecutionID(t *testing.T) {
	cfg := &WorkerConfig{
		PipelineID:  "20912e3b-0000-4000-8000-000000000001",
		ExecutionID: "7f3d9c21-0000-4000-8000-0000000000aa",
		Topic:       "dbserver.pipeline_test.demo_products",
		SinkMode:    "cdc",
	}
	b, _ := json.Marshal(wrapCDCEnvelope(map[string]interface{}{
		"op":     "c",
		"source": map[string]interface{}{"db": "pipeline_test", "table": "demo_products"},
		"before": nil,
		"after":  map[string]interface{}{"id": 1},
		"ts_ms":  int64(1704067200000),
	}))

	sm, err := parseSinkMessage(cfg, kafka.Message{Topic: cfg.Topic, Value: b, Offset: 7})
	if err != nil {
		t.Fatalf("parseSinkMessage: %v", err)
	}

	// The rewrite really did happen, so the problem this fixes is reproduced here.
	if sm.ExecutionID != cfg.PipelineID {
		t.Fatalf("sm.ExecutionID = %q, want the pipeline id %q — without the CDC rewrite this "+
			"test proves nothing", sm.ExecutionID, cfg.PipelineID)
	}
	if sm.OrchestrationExecutionID != cfg.ExecutionID {
		t.Errorf("sm.OrchestrationExecutionID = %q, want %q — it must be captured before the "+
			"line above overwrites ExecutionID, which is the only place it is still in hand",
			sm.OrchestrationExecutionID, cfg.ExecutionID)
	}
}
