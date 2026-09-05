package consumer

import (
	"sync"
	"time"
)

// ConsumerState represents the state of a managed consumer
type ConsumerState string

const (
	StateStarting  ConsumerState = "starting"
	StateRunning   ConsumerState = "running"
	StateUnhealthy ConsumerState = "unhealthy"
	StateStopping  ConsumerState = "stopping"
	StateStopped   ConsumerState = "stopped"
	StateFailed    ConsumerState = "failed"
)

// HealthStatus represents consumer health status
type HealthStatus string

const (
	HealthHealthy   HealthStatus = "healthy"
	HealthDegraded  HealthStatus = "degraded"
	HealthUnhealthy HealthStatus = "unhealthy"
	HealthDead      HealthStatus = "dead"
	HealthUnknown   HealthStatus = "unknown"
)

// ScalingAction represents type of scaling action
type ScalingAction string

const (
	ActionScaleUp   ScalingAction = "scale_up"
	ActionScaleDown ScalingAction = "scale_down"
	ActionReplace   ScalingAction = "replace"
	ActionNoAction  ScalingAction = "no_action"
)

// ScalingReason represents reason for scaling
type ScalingReason string

const (
	ReasonHighLag           ScalingReason = "high_lag"
	ReasonLowLag            ScalingReason = "low_lag"
	ReasonHighThroughput    ScalingReason = "high_throughput"
	ReasonPartitionMismatch ScalingReason = "partition_mismatch"
	ReasonConsumerUnhealthy ScalingReason = "consumer_unhealthy"
	ReasonManual            ScalingReason = "manual"
)

// ConsumerHealth holds health information for a consumer
type ConsumerHealth struct {
	ConsumerID string       `json:"consumer_id"`
	GroupID    string       `json:"group_id"`
	Topic      string       `json:"topic"`
	Status     HealthStatus `json:"status"`

	// Metrics
	CurrentLag        int64   `json:"current_lag"`
	MessagesProcessed int64   `json:"messages_processed"`
	MessagesPerSecond float64 `json:"messages_per_second"`
	ErrorsCount       int     `json:"errors_count"`

	// Heartbeat
	LastHeartbeat       time.Time `json:"last_heartbeat"`
	ConsecutiveFailures int       `json:"consecutive_failures"`

	// Partitions
	AssignedPartitions []int `json:"assigned_partitions"`

	// Timestamps
	StartedAt   time.Time `json:"started_at"`
	LastChecked time.Time `json:"last_checked"`

	mu sync.RWMutex
}

// IsAlive checks if consumer is alive based on heartbeat
func (h *ConsumerHealth) IsAlive(timeoutSeconds int) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.LastHeartbeat.IsZero() {
		return false
	}

	elapsed := time.Since(h.LastHeartbeat).Seconds()
	return elapsed < float64(timeoutSeconds)
}

// RecordHeartbeat records a heartbeat
func (h *ConsumerHealth) RecordHeartbeat() {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.LastHeartbeat = time.Now()
	h.ConsecutiveFailures = 0
}

// RecordFailure records a failure
func (h *ConsumerHealth) RecordFailure() {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.ConsecutiveFailures++
	h.ErrorsCount++
}

// UpdateMetrics updates consumer metrics
func (h *ConsumerHealth) UpdateMetrics(lag int64, processed int64, throughput float64) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.CurrentLag = lag
	h.MessagesProcessed = processed
	h.MessagesPerSecond = throughput
}

// ConsumerInfo holds information about a managed consumer
type ConsumerInfo struct {
	ConsumerID string        `json:"consumer_id"`
	GroupID    string        `json:"group_id"`
	Topic      string        `json:"topic"`
	PipelineID string        `json:"pipeline_id,omitempty"`
	State      ConsumerState `json:"state"`

	// Container info
	ContainerID   string `json:"container_id,omitempty"`
	ContainerName string `json:"container_name,omitempty"`

	// Health reference
	Health *ConsumerHealth `json:"health,omitempty"`

	// Restart tracking
	RestartCount int       `json:"restart_count"`
	LastRestart  time.Time `json:"last_restart,omitempty"`

	// Timestamps
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`

	mu sync.RWMutex
}

// SetState updates consumer state
func (c *ConsumerInfo) SetState(state ConsumerState) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.State = state
	c.UpdatedAt = time.Now()
}

// IncrementRestart increments restart count
func (c *ConsumerInfo) IncrementRestart() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.RestartCount++
	c.LastRestart = time.Now()
	c.UpdatedAt = time.Now()
}

// SpawnResult represents result of spawning a consumer
type SpawnResult struct {
	Success       bool      `json:"success"`
	ConsumerID    string    `json:"consumer_id"`
	GroupID       string    `json:"group_id"`
	Topic         string    `json:"topic"`
	ContainerID   string    `json:"container_id,omitempty"`
	ContainerName string    `json:"container_name,omitempty"`
	Error         string    `json:"error,omitempty"`
	SpawnedAt     time.Time `json:"spawned_at"`
}

// ScalingDecision represents a scaling decision
type ScalingDecision struct {
	Topic             string        `json:"topic"`
	Action            ScalingAction `json:"action"`
	Reason            ScalingReason `json:"reason"`
	CurrentConsumers  int           `json:"current_consumers"`
	TargetConsumers   int           `json:"target_consumers"`
	ConsumersToAdd    int           `json:"consumers_to_add"`
	ConsumersToRemove int           `json:"consumers_to_remove"`
	CurrentLag        int64         `json:"current_lag"`
	Throughput        float64       `json:"throughput"`
	PartitionCount    int           `json:"partition_count"`
	UnhealthyCount    int           `json:"unhealthy_count"`
	Confidence        float64       `json:"confidence"`
	Explanation       string        `json:"explanation"`
	CreatedAt         time.Time     `json:"created_at"`
}

// HealthSummary provides summary of all consumer health
type HealthSummary struct {
	TotalConsumers int            `json:"total_consumers"`
	ByStatus       map[string]int `json:"by_status"`
	TotalLag       int64          `json:"total_lag"`
	UnhealthyCount int            `json:"unhealthy_count"`
}

// TopicConsumers holds consumers for a topic
type TopicConsumers struct {
	Topic         string          `json:"topic"`
	ConsumerCount int             `json:"consumer_count"`
	TotalLag      int64           `json:"total_lag"`
	Consumers     []*ConsumerInfo `json:"consumers"`
}
