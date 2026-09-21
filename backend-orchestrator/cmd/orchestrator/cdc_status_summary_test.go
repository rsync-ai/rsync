package main

import (
	"encoding/json"
	"testing"
)

func decodeStatus(t *testing.T, raw string) map[string]interface{} {
	t.Helper()
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		t.Fatalf("bad fixture: %v", err)
	}
	return m
}

// Issue #20: the chip read result.connector_state, which the status handler never set.
func TestSummarizeKafkaConnectStatus(t *testing.T) {
	for _, tc := range []struct {
		name        string
		payload     string
		wantState   string
		wantHealthy bool
		wantTasks   int
	}{
		{
			name:        "running connector, all tasks running",
			payload:     `{"name":"cdc-aaa0ded3","connector":{"state":"RUNNING","worker_id":"w:8083"},"tasks":[{"id":0,"state":"RUNNING"}],"type":"source"}`,
			wantState:   "RUNNING",
			wantHealthy: true,
			wantTasks:   1,
		},
		{
			name:        "running connector with a failed task is not healthy",
			payload:     `{"connector":{"state":"RUNNING"},"tasks":[{"id":0,"state":"RUNNING"},{"id":1,"state":"FAILED","trace":"boom"}]}`,
			wantState:   "RUNNING",
			wantHealthy: false,
			wantTasks:   2,
		},
		// A task that carries a stack trace has hit an error whatever it calls its
		// state, so it cannot be reported healthy. Connect normally only fills trace on
		// a FAILED task — this is defence in depth against the shape that motivated
		// KI-CDC-MONGO-RESUME-TOKEN-SILENT-STALL, where a connector retried a permanent
		// error forever without ever leaving RUNNING. (In that live incident Connect
		// exposed no trace at all, which is why the Sentinel's freshness alarm, not this
		// check, is what actually catches it.)
		{
			name:        "running task carrying a trace is not healthy",
			payload:     `{"connector":{"state":"RUNNING"},"tasks":[{"id":0,"state":"RUNNING","trace":"org.apache.kafka.connect.errors.ConnectException: boom"}]}`,
			wantState:   "RUNNING",
			wantHealthy: false,
			wantTasks:   1,
		},
		{
			// An empty or whitespace-only trace is Connect saying nothing, not Connect
			// reporting an error. Treating it as one would mark every healthy pipeline
			// on a worker that serializes the field unconditionally as broken.
			name:        "running task with a blank trace stays healthy",
			payload:     `{"connector":{"state":"RUNNING"},"tasks":[{"id":0,"state":"RUNNING","trace":"   "}]}`,
			wantState:   "RUNNING",
			wantHealthy: true,
			wantTasks:   1,
		},
		{
			name:        "running connector with no tasks is not streaming",
			payload:     `{"connector":{"state":"RUNNING"},"tasks":[]}`,
			wantState:   "RUNNING",
			wantHealthy: false,
			wantTasks:   0,
		},
		{
			name:        "paused",
			payload:     `{"connector":{"state":"paused"},"tasks":[{"id":0,"state":"PAUSED"}]}`,
			wantState:   "PAUSED",
			wantHealthy: false,
			wantTasks:   1,
		},
		{
			name:        "missing connector block",
			payload:     `{"tasks":[{"id":0,"state":"RUNNING"}]}`,
			wantState:   "UNKNOWN",
			wantHealthy: false,
			wantTasks:   1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := summarizeKafkaConnectStatus(decodeStatus(t, tc.payload))
			if got.ConnectorState != tc.wantState {
				t.Errorf("ConnectorState = %q, want %q", got.ConnectorState, tc.wantState)
			}
			if got.Healthy != tc.wantHealthy {
				t.Errorf("Healthy = %v, want %v", got.Healthy, tc.wantHealthy)
			}
			if len(got.Tasks) != tc.wantTasks || len(got.TaskStates) != tc.wantTasks {
				t.Errorf("tasks = %d / task_states = %d, want %d", len(got.Tasks), len(got.TaskStates), tc.wantTasks)
			}
		})
	}
}

func TestSummarizeKafkaConnectStatus_NilPayload(t *testing.T) {
	got := summarizeKafkaConnectStatus(nil)
	if got.ConnectorState != "UNKNOWN" || got.Healthy || got.Tasks == nil || got.TaskStates == nil {
		t.Fatalf("nil payload summary = %+v", got)
	}
	// Tasks must serialize as [] rather than null so the UI can iterate it.
	b, _ := json.Marshal(map[string]interface{}{"tasks": got.Tasks})
	if string(b) != `{"tasks":[]}` {
		t.Fatalf("tasks serialized as %s", b)
	}
}
