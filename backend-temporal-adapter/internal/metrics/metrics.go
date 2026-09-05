// Package metrics defines the application-level Prometheus collectors
// for backend-temporal-adapter. All collectors auto-register on package
// import via promauto so the `/metrics` HTTP endpoint started in
// `cmd/adapter/main.go` exposes them with no further wiring.
//
// Cardinality is bounded by design:
//   - `workflow` is the Temporal workflow type name (fixed set of 2:
//     NLPipelineWorkflowV2, ScheduledPipelineRunWorkflow).
//   - `activity` is the Temporal activity type name (fixed set of ~27
//     registered activities — see cmd/adapter/main.go).
//   - `result` is success|failure.
//
// NO labels include free-form user input (pipeline_id, run_id, workflow
// run id, connection_id). Those would explode series memory. Per-run
// drill-down lives in traces/logs in SigNoz, correlated by trace id.
//
// F-Obs-2 (Obs-TemporalAdapter).
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// WorkflowExecutionsTotal counts workflow executions terminating in each
// result, by workflow type. Incremented once per non-replay completion
// from the worker interceptor so Temporal history replays do not
// double-count.
var WorkflowExecutionsTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "rsync_temporal_workflow_executions_total",
		Help: "Temporal workflow executions by workflow type + result",
	},
	[]string{"workflow", "result"}, // result = success|failure
)

// ActivityExecutionsTotal counts activity executions by activity type +
// result. Activities run exactly once (no replay), so every interceptor
// invocation is a real execution.
var ActivityExecutionsTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "rsync_temporal_activity_executions_total",
		Help: "Temporal activity executions by activity type + result",
	},
	[]string{"activity", "result"}, // result = success|failure
)

// ActivityDurationSeconds histograms the wall-clock duration of each
// activity execution. Pair with ActivityExecutionsTotal to compute
// "p95 latency by activity" and spot slow connector/LLM round-trips.
var ActivityDurationSeconds = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "rsync_temporal_activity_duration_seconds",
		Help:    "Wall-clock duration of Temporal activity executions",
		Buckets: []float64{0.05, 0.1, 0.5, 1, 2.5, 5, 10, 30, 60, 300},
	},
	[]string{"activity"},
)
