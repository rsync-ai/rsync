package executor

import (
	"encoding/json"
	"testing"
	"time"
)

func TestMetricsEmitter_EmitsRecentFilesAndFilesWritten(t *testing.T) {
	emitted := make([]map[string]interface{}, 0, 4)
	emitFn := func(event []byte, headers map[string]string) error {
		var m map[string]interface{}
		if err := json.Unmarshal(event, &m); err != nil {
			t.Fatalf("failed to unmarshal emitted event: %v", err)
		}
		emitted = append(emitted, m)
		return nil
	}

	em := NewMetricsEmitter(MetricsEmitterConfig{
		PipelineID:              "pipeline-1",
		ExecutionID:             "exec-1",
		TraceID:                 "trace-1",
		Source:                  "executor_batch",
		MinFlushInterval:        0,
		MaxFlushInterval:        50 * time.Millisecond,
		DefaultMaxFilesPerFlush: 100,
		MaxPayloadBytes:         1024,
	}, emitFn)

	em.ObserveTotals("t1", 10, 1000)
	em.ObserveFileWrite("t1", "minio://bucket/pipeline-1/exec-1/t1/file-1.json", 10, 1000)

	// force flush (completion semantics)
	if err := em.MaybeFlush(true); err != nil {
		t.Fatalf("unexpected flush error: %v", err)
	}

	if len(emitted) != 1 {
		t.Fatalf("expected 1 emitted event, got %d", len(emitted))
	}

	e := emitted[0]
	if e["event_type"] != "DATA_PLANE_METRICS" {
		t.Fatalf("expected event_type DATA_PLANE_METRICS, got %v", e["event_type"])
	}

	meta, _ := e["metadata"].(map[string]interface{})
	if meta == nil {
		t.Fatalf("missing metadata")
	}
	if meta["metrics_schema"] != "v2" {
		t.Fatalf("expected metrics_schema v2, got %v", meta["metrics_schema"])
	}

	metrics, _ := meta["metrics"].(map[string]interface{})
	if metrics == nil {
		t.Fatalf("missing metadata.metrics")
	}
	if metrics["files_written"] == nil {
		t.Fatalf("expected metrics.files_written")
	}

	rf, ok := metrics["recent_files"].([]interface{})
	if !ok {
		t.Fatalf("expected metrics.recent_files array, got %T", metrics["recent_files"])
	}
	if len(rf) != 1 {
		t.Fatalf("expected 1 recent_files entry, got %d", len(rf))
	}
}

