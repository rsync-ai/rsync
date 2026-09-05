package sentinel

import (
	"time"
)

// ComponentType represents the type of component being monitored
type ComponentType string

const (
	ComponentTypeAgent          ComponentType = "agent"
	ComponentTypeMCPConnector   ComponentType = "mcp_connector"
	ComponentTypeKafkaConsumer  ComponentType = "kafka_consumer"
	ComponentTypeInfrastructure ComponentType = "infrastructure"
	ComponentTypeCDCPipeline    ComponentType = "cdc_pipeline"
	// ComponentTypeBatchPipeline is the batch counterpart of ComponentTypeCDCPipeline.
	// The CHECK constraint that makes it storable is widened in migration 081; adding
	// the Go constant without that migration produces a runtime constraint violation,
	// not a compile error, so the two must ship together.
	ComponentTypeBatchPipeline ComponentType = "batch_pipeline"
)

// HealthStatus represents the health status of a component
type HealthStatus string

const (
	HealthStatusHealthy   HealthStatus = "healthy"
	HealthStatusDegraded  HealthStatus = "degraded"
	HealthStatusUnhealthy HealthStatus = "unhealthy"
	HealthStatusDead      HealthStatus = "dead"
	HealthStatusUnknown   HealthStatus = "unknown"
)

// IssueType represents the type of issue detected
type IssueType string

const (
	IssueTypeMissingHeartbeat    IssueType = "missing_heartbeat"
	IssueTypeHighLag             IssueType = "high_lag"
	IssueTypeConsumerGroupClosed IssueType = "consumer_group_closed"
	IssueTypeProtocolMismatch    IssueType = "protocol_mismatch"
	IssueTypeDLQGrowth           IssueType = "dlq_growth"
	IssueTypeConnectorDown       IssueType = "connector_down"
	IssueTypeInfrastructureDown  IssueType = "infrastructure_down"
	IssueTypeRepeatedFailure     IssueType = "repeated_failure"
	IssueTypeAnomaly             IssueType = "anomaly"
	// IssueTypeStalledRun — a run that is neither progressing nor failing. Distinct
	// from IssueTypeMissingHeartbeat, which is about a COMPONENT being absent: here
	// every component is present and reporting, the work just stopped moving.
	IssueTypeStalledRun IssueType = "stalled_run"
	// IssueTypeSinkWriteRejected — the sink acknowledged batches it could not write
	// (negative acks). The run may still look healthy end-to-end.
	IssueTypeSinkWriteRejected IssueType = "sink_write_rejected"
	// IssueTypeSinkWorkerAbsent — the sink container holds no worker for this run's
	// consumer group at all (sink_status: not_found). Distinct from
	// IssueTypeSinkWriteRejected, where a worker exists and is failing its writes:
	// here nothing is consuming, so there is nothing to reject. The producer side
	// keeps working, which is why every other signal stays green until the stall
	// threshold fires with no named cause.
	IssueTypeSinkWorkerAbsent IssueType = "sink_worker_absent"
	// IssueTypeNoRowMovement — the run's row counters have not changed while its
	// progress timestamps keep updating. Distinct from IssueTypeStalledRun, which is
	// the opposite reading: there NOTHING is being emitted, here something is emitting
	// steadily and the numbers never move. The two are mutually exclusive by
	// construction (runningBatchRowCountsQuery excludes timestamp-stalled runs).
	IssueTypeNoRowMovement IssueType = "no_row_movement"
)

// IssueSeverity represents the severity of an issue
type IssueSeverity string

const (
	IssueSeverityCritical IssueSeverity = "critical"
	IssueSeverityWarning  IssueSeverity = "warning"
	IssueSeverityInfo     IssueSeverity = "info"
)

// HealingAction represents an action the sentinel can take
type HealingAction string

const (
	HealingActionRestartAgent    HealingAction = "restart_agent"
	HealingActionRestartConsumer HealingAction = "restart_consumer"
	HealingActionProvisionTopic  HealingAction = "provision_topic"
	HealingActionFixProtocol     HealingAction = "fix_protocol"
	HealingActionReplayMessages  HealingAction = "replay_messages"
	HealingActionScaleUp         HealingAction = "scale_up"
	HealingActionAlert           HealingAction = "alert"
	HealingActionCircuitBreak    HealingAction = "circuit_break"
)

// ComponentHealth represents the health state of a monitored component
type ComponentHealth struct {
	ComponentID       string                 `json:"component_id"`
	ComponentType     ComponentType          `json:"component_type"`
	Status            HealthStatus           `json:"status"`
	LastHeartbeat     time.Time              `json:"last_heartbeat"`
	MessagesProcessed int64                  `json:"messages_processed"`
	ErrorCount        int64                  `json:"error_count"`
	ConsumerLag       int64                  `json:"consumer_lag"`
	LastError         string                 `json:"last_error,omitempty"`
	Metadata          map[string]interface{} `json:"metadata,omitempty"`
	UpdatedAt         time.Time              `json:"updated_at"`
}

// Issue represents a detected issue that needs healing
type Issue struct {
	ID              string                 `json:"id"`
	Type            IssueType              `json:"type"`
	Severity        IssueSeverity          `json:"severity"`
	ComponentID     string                 `json:"component_id"`
	ComponentType   ComponentType          `json:"component_type"`
	Description     string                 `json:"description"`
	DetectedAt      time.Time              `json:"detected_at"`
	ResolvedAt      *time.Time             `json:"resolved_at,omitempty"`
	Metadata        map[string]interface{} `json:"metadata,omitempty"`
	OccurrenceCount int                    `json:"occurrence_count"`
	LastOccurrence  time.Time              `json:"last_occurrence"`
}

// HealingResult represents the result of a healing action.
//
// Skipped separates an action the healer DECLINED to attempt (it returned
// ErrHealingSkipped — see healer.go) from one it attempted and failed. Success is
// false in both cases; the distinction is what stops "there was nothing I could do
// here" from reading as "I fixed it". ExecuteHealing sets it, and Agent.triggerHealing
// (sentinel.go) closes an issue only on a real success — never on a skip.
type HealingResult struct {
	IssueID       string                 `json:"issue_id"`
	Action        HealingAction          `json:"action"`
	ComponentID   string                 `json:"component_id"`
	ComponentType ComponentType          `json:"component_type"`
	Success       bool                   `json:"success"`
	Skipped       bool                   `json:"skipped,omitempty"`
	Error         string                 `json:"error,omitempty"`
	DurationMs    int64                  `json:"duration_ms"`
	Timestamp     time.Time              `json:"timestamp"`
	Details       map[string]interface{} `json:"details,omitempty"`
}

// AgentHeartbeat represents a heartbeat message from an agent
type AgentHeartbeat struct {
	Agent             string    `json:"agent"`
	Status            string    `json:"status"`
	LastMessageAt     string    `json:"last_message_at"`
	MessagesProcessed int64     `json:"messages_processed"`
	ErrorCount        int64     `json:"error_count"`
	ConsumerLag       int64     `json:"consumer_lag"`
	Timestamp         time.Time `json:"timestamp"`
}

// SentinelConfig holds configuration for the sentinel agent
type SentinelConfig struct {
	// Heartbeat monitoring
	HeartbeatTimeout       time.Duration
	HeartbeatCheckInterval time.Duration

	// Consumer lag thresholds
	ConsumerLagWarningThreshold  int64
	ConsumerLagCriticalThreshold int64

	// DLQ thresholds
	DLQWarningThreshold  int64
	DLQCriticalThreshold int64

	// Healing behavior
	MaxRestartAttempts      int
	RestartBackoffBase      time.Duration
	RestartBackoffMax       time.Duration
	CircuitBreakerThreshold int
	CircuitBreakerCooldown  time.Duration

	// Issue deduplication
	IssueCooldownPeriod time.Duration

	// Anomaly detection
	AnomalyStdDevThreshold float64
	AnomalyWindowSize      int

	// Stale component eviction — how long to keep a dead component in memory before removing it
	StaleComponentTTL time.Duration

	// SigNoz export
	EnableSigNozExport   bool
	SigNozEndpoint       string
	MetricExportInterval time.Duration
}

// DefaultSentinelConfig returns default configuration
func DefaultSentinelConfig() *SentinelConfig {
	return &SentinelConfig{
		HeartbeatTimeout:             30 * time.Second,
		HeartbeatCheckInterval:       10 * time.Second,
		ConsumerLagWarningThreshold:  5000,
		ConsumerLagCriticalThreshold: 10000,
		DLQWarningThreshold:          50,
		DLQCriticalThreshold:         100,
		MaxRestartAttempts:           3,
		RestartBackoffBase:           10 * time.Second,
		RestartBackoffMax:            5 * time.Minute,
		CircuitBreakerThreshold:      5,
		CircuitBreakerCooldown:       10 * time.Minute,
		IssueCooldownPeriod:          5 * time.Minute,
		StaleComponentTTL:            10 * time.Minute,
		AnomalyStdDevThreshold:       3.0,
		AnomalyWindowSize:            100,
		EnableSigNozExport:           true,
		SigNozEndpoint:               "http://otel-collector:4318",
		MetricExportInterval:         30 * time.Second,
	}
}
