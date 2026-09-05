// Package metrics defines the application-level Prometheus collectors
// for backend-orchestrator. All collectors auto-register on package
// import via promauto so the existing `/metrics` endpoint in
// `cmd/orchestrator/main.go` exposes them with no further wiring.
//
// Cardinality is bounded by design:
//   - `connector` label is the connector_type string (small fixed set:
//     postgresql, mysql, shopify-admin-graphql, ...).
//   - `op` is the MCP operation name (export, import_data, ensure_table,
//     test_connection, ...).
//   - `status` is success|failure|retry.
//   - `topic` is the Kafka topic name (small set of well-known names).
//   - `action` is the healer action name (restart, scale, notify, ...).
//
// NO labels include free-form user input (pipeline_id, table name,
// connection_id). Those go on dashboards via log-based metrics, not
// Prometheus labels — high-cardinality labels would explode memory.
//
// F-Obs-2.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// PipelineRunsTotal counts pipeline executions terminating in each
// status. Increment from the executor worker (workers/executor.go)
// or temporal-adapter result handler when an execution row reaches
// a terminal state.
var PipelineRunsTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "rsync_pipeline_runs_total",
		Help: "Pipeline executions by terminal status",
	},
	[]string{"status"}, // completed|failed|cancelled
)

// PipelineSyncDurationSeconds histograms the wall-clock duration of
// each pipeline execution from start_time to terminal status. Use
// for the customer-visible "is sync getting slower over time?"
// dashboard.
var PipelineSyncDurationSeconds = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "rsync_pipeline_sync_duration_seconds",
		Help:    "Wall-clock duration of pipeline executions",
		Buckets: []float64{1, 5, 15, 30, 60, 300, 900, 3600, 14400},
	},
	[]string{"sync_mode"}, // batch|cdc
)

// MCPCallsTotal counts every outbound MCP call (executor → connector
// container). Used to alert on connector-specific failure spikes and
// to compute success rate dashboards per connector + operation.
var MCPCallsTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "rsync_mcp_calls_total",
		Help: "MCP operation calls by connector + operation + status",
	},
	[]string{"connector", "operation", "status"},
)

// MCPCallDurationSeconds histograms the latency of MCP calls. Pair
// with MCPCallsTotal to compute "p95 latency by connector" SLI.
var MCPCallDurationSeconds = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "rsync_mcp_call_duration_seconds",
		Help:    "Latency of MCP calls by connector + operation",
		Buckets: []float64{0.1, 0.5, 1, 2.5, 5, 10, 30, 60, 300},
	},
	[]string{"connector", "operation"},
)

// KafkaMessagesPublishedTotal counts producer events into each topic.
// Used for "is the executor making progress?" + DLQ rate alerting.
var KafkaMessagesPublishedTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "rsync_kafka_messages_published_total",
		Help: "Kafka producer events by topic + result",
	},
	[]string{"topic", "result"}, // result = success|failure
)

// HealerActionsTotal counts healer-agent actions. Used to detect
// "healer is thrashing" situations where the same pipeline keeps
// hitting the same recovery loop.
var HealerActionsTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "rsync_healer_actions_total",
		Help: "Healer agent actions by action type + outcome",
	},
	[]string{"action", "outcome"}, // outcome = success|failure|escalated
)

// DLQMessagesTotal counts messages routed to a dead-letter queue
// (one per source topic). Critical for "is data being silently dropped?"
// alerting.
var DLQMessagesTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "rsync_dlq_messages_total",
		Help: "Messages sent to dead-letter queues by source topic",
	},
	[]string{"source_topic"},
)
