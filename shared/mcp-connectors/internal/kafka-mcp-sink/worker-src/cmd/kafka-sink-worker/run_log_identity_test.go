package main

// Journal O-1 / O-2 (pipeline matrix 2026-09-26) coverage.
//
// O-1: a batch-lane destination write that LANDED logged nothing. writeToDestination
// does its own MCP round-trip instead of going through callDestinationTool, so only
// the ack ledger proved a batch landed.
//
// O-2: every sink line carried the worker's FIRST execution id. logEvent stamps the
// process-global gExecutionID, set once from the worker config, and a batch worker
// is reused across runs — so a Reload's lines named the first run.
//
// The bug class is "a log line names the worker, not the run it is about", so the
// tests pin every way a line gets its execution id: the per-message helper
// (logMsgEvent), the new write line, and the worker-level default for lines logged
// with no message in hand.

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/segmentio/kafka-go"
)

const (
	firstRunID  = "88a72a09-0000-4000-8000-000000000001" // the worker config's run
	secondRunID = "3b705571-0000-4000-8000-000000000002" // the run the message belongs to
)

// withWorkerExecutionID sets the process-global log default for one test and
// restores it afterwards.
func withWorkerExecutionID(t *testing.T, id string) {
	t.Helper()
	prev := loadStr(&gExecutionID)
	gExecutionID.Store(id)
	t.Cleanup(func() { gExecutionID.Store(prev) })
}

func TestBatchWriteLogsASuccessLineForItsOwnRun(t *testing.T) {
	withWorkerExecutionID(t, firstRunID)

	client, _ := nsTestClient()
	cfg, sm, rows := batchMessage("mongodb", "appdb", map[string]interface{}{"database": "appdb"})
	sm.ExecutionID = secondRunID
	sm.BatchOffset = 7
	sm.RunMode = "reload"
	rows[0]["email"] = "alice.secret@example.com" // a row value that must never be logged

	var werr error
	raw := captureLogStderr(t, func() {
		_, _, werr = writeToDestination(context.Background(), client, cfg, nil, sm, rows, "", "", nil)
	})
	if werr != nil {
		t.Fatalf("writeToDestination errored: %v", werr)
	}
	if strings.Contains(raw, "alice.secret") {
		t.Fatalf("a row value reached the log:\n%s", raw)
	}

	var ok []map[string]any
	for _, line := range strings.Split(raw, "\n") {
		if !strings.Contains(line, `"batch destination write ok"`) {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("log line is not JSON: %v\nline: %s", err, line)
		}
		ok = append(ok, rec)
	}
	if len(ok) != 1 {
		t.Fatalf("want exactly 1 \"batch destination write ok\" line, got %d; log:\n%s", len(ok), raw)
	}
	rec := ok[0]
	want := map[string]any{
		"execution_id": secondRunID,
		"pipeline_id":  "p1",
		"table":        "public.orders",
		"rows_written": float64(1),
		"rows_sent":    float64(1),
		"batch_offset": float64(7),
		"run_mode":     "reload",
	}
	for k, v := range want {
		if rec[k] != v {
			t.Errorf("%s = %v, want %v (line: %v)", k, rec[k], v, rec)
		}
	}
}

func TestLogMsgEventNamesTheMessagesRun(t *testing.T) {
	withWorkerExecutionID(t, firstRunID)
	msg := kafka.Message{Topic: "rsync.batch.p1", Partition: 0, Offset: 42}

	cases := []struct {
		name string
		sm   *SinkMessage
		want string
	}{
		{"batch message carries its own run", &SinkMessage{PipelineID: "p1", ExecutionID: secondRunID, Table: "t"}, secondRunID},
		{"cdc message with an orchestration id", &SinkMessage{PipelineID: "p1", ExecutionID: "p1", OrchestrationExecutionID: secondRunID, IsCDC: true, Table: "t"}, secondRunID},
		// CDC forces ExecutionID to the pipeline id (a stats key, not a run): never log that.
		{"cdc message without one keeps the worker run", &SinkMessage{PipelineID: "p1", ExecutionID: "p1", IsCDC: true, Table: "t"}, firstRunID},
		{"nil message keeps the worker run", nil, firstRunID},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := captureLogEvent(t, func() { logMsgEvent("info", tc.sm, msg, "probe") })
			if rec["execution_id"] != tc.want {
				t.Fatalf("execution_id = %v, want %v", rec["execution_id"], tc.want)
			}
		})
	}
}

func TestParsedBatchMessageMovesTheWorkerDefault(t *testing.T) {
	withWorkerExecutionID(t, firstRunID)

	// A CDC message never moves it: its worker is per run and its batchers log
	// from other goroutines.
	noteRunExecutionID(&SinkMessage{PipelineID: "p1", ExecutionID: "p1", IsCDC: true})
	if rec := captureLogEvent(t, func() { logf("info", "cdc line") }); rec["execution_id"] != firstRunID {
		t.Fatalf("CDC message moved the worker default to %v", rec["execution_id"])
	}

	// A line logged with no message in hand (logf, "destination tool call ok")
	// must name the run being processed once its first message is parsed.
	noteRunExecutionID(&SinkMessage{PipelineID: "p1", ExecutionID: secondRunID})
	if rec := captureLogEvent(t, func() { logf("info", "retrying") }); rec["execution_id"] != secondRunID {
		t.Fatalf("execution_id = %v after a %s message was parsed, want %s", rec["execution_id"], secondRunID, secondRunID)
	}
}
