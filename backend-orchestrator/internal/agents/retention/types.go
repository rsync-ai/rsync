package retention

import (
	"sync"
	"time"
)

// BulkLoadStatus represents the status of a bulk load operation
type BulkLoadStatus string

const (
	StatusActive     BulkLoadStatus = "active"
	StatusMonitoring BulkLoadStatus = "monitoring"
	StatusCompleted  BulkLoadStatus = "completed"
	StatusCleaned    BulkLoadStatus = "cleaned"
	StatusFailed     BulkLoadStatus = "failed"
	StatusArchived   BulkLoadStatus = "archived"
)

// CleanupAction represents type of cleanup action
type CleanupAction string

const (
	ActionReduceRetention CleanupAction = "reduce_retention"
	ActionDeleteRecords   CleanupAction = "delete_records"
	ActionArchiveToS3     CleanupAction = "archive_to_s3"
	ActionCompact         CleanupAction = "compact"
)

// BulkLoadInfo holds information about a bulk data load operation
type BulkLoadInfo struct {
	ID           string `json:"id"`
	Topic        string `json:"topic"`
	PipelineID   string `json:"pipeline_id"`
	ConnectionID string `json:"connection_id,omitempty"`

	// Offset range for bulk data
	StartOffset  int64 `json:"start_offset"`
	EndOffset    int64 `json:"end_offset"`
	MessageCount int64 `json:"message_count"`
	SizeBytes    int64 `json:"size_bytes,omitempty"`

	// Consumer tracking
	ExpectedConsumers []string `json:"expected_consumers"`
	CaughtUpConsumers []string `json:"caught_up_consumers"`

	// Status
	Status   BulkLoadStatus `json:"status"`
	Progress float64        `json:"progress"` // 0.0 to 100.0

	// Timestamps
	RegisteredAt  time.Time `json:"registered_at"`
	AllCaughtUpAt time.Time `json:"all_caught_up_at,omitempty"`
	CleanedAt     time.Time `json:"cleaned_at,omitempty"`
	ArchivedAt    time.Time `json:"archived_at,omitempty"`

	// Cleanup info
	CleanupAction   CleanupAction `json:"cleanup_action,omitempty"`
	ArchiveLocation string        `json:"archive_location,omitempty"`

	mu sync.RWMutex
}

// MessageCount returns the number of messages in the bulk load
func (b *BulkLoadInfo) GetMessageCount() int64 {
	return b.EndOffset - b.StartOffset
}

// IsAllCaughtUp checks if all expected consumers have caught up
func (b *BulkLoadInfo) IsAllCaughtUp() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if len(b.ExpectedConsumers) == 0 {
		return len(b.CaughtUpConsumers) > 0
	}

	for _, expected := range b.ExpectedConsumers {
		found := false
		for _, caught := range b.CaughtUpConsumers {
			if expected == caught {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// MarkCaughtUp marks a consumer as caught up
func (b *BulkLoadInfo) MarkCaughtUp(consumerGroup string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, c := range b.CaughtUpConsumers {
		if c == consumerGroup {
			return
		}
	}
	b.CaughtUpConsumers = append(b.CaughtUpConsumers, consumerGroup)

	// Check if all caught up
	if b.isAllCaughtUpUnlocked() && b.AllCaughtUpAt.IsZero() {
		b.AllCaughtUpAt = time.Now()
	}
}

func (b *BulkLoadInfo) isAllCaughtUpUnlocked() bool {
	if len(b.ExpectedConsumers) == 0 {
		return len(b.CaughtUpConsumers) > 0
	}

	for _, expected := range b.ExpectedConsumers {
		found := false
		for _, caught := range b.CaughtUpConsumers {
			if expected == caught {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// ConsumerProgress represents progress of a consumer group
type ConsumerProgress struct {
	ConsumerGroup string    `json:"consumer_group"`
	Topic         string    `json:"topic"`
	CurrentOffset int64     `json:"current_offset"`
	TargetOffset  int64     `json:"target_offset"`
	Lag           int64     `json:"lag"`
	Progress      float64   `json:"progress"` // 0.0 to 100.0
	CaughtUp      bool      `json:"caught_up"`
	LastUpdated   time.Time `json:"last_updated"`
}

// TopicRetention holds retention configuration for a topic
type TopicRetention struct {
	Topic               string    `json:"topic"`
	RetentionMs         int64     `json:"retention_ms"`
	RetentionBytes      int64     `json:"retention_bytes,omitempty"`
	CleanupPolicy       string    `json:"cleanup_policy"` // "delete" or "compact"
	OriginalRetentionMs int64     `json:"original_retention_ms,omitempty"`
	ModifiedAt          time.Time `json:"modified_at,omitempty"`
	RestoredAt          time.Time `json:"restored_at,omitempty"`
}

// CleanupResult represents result of a cleanup operation
type CleanupResult struct {
	BulkLoadID string        `json:"bulk_load_id"`
	Topic      string        `json:"topic"`
	Action     CleanupAction `json:"action"`
	Success    bool          `json:"success"`
	Error      string        `json:"error,omitempty"`

	// Metrics
	MessagesRemoved int64  `json:"messages_removed,omitempty"`
	BytesFreed      int64  `json:"bytes_freed,omitempty"`
	ArchiveLocation string `json:"archive_location,omitempty"`

	// Timing
	StartedAt   time.Time `json:"started_at"`
	CompletedAt time.Time `json:"completed_at"`
	DurationMs  int64     `json:"duration_ms"`
}

// SafetyCheckResult represents result of safety checks
type SafetyCheckResult struct {
	Safe            bool     `json:"safe"`
	Reason          string   `json:"reason"`
	ChecksPerformed []string `json:"checks_performed"`
	FailedChecks    []string `json:"failed_checks,omitempty"`
	Warnings        []string `json:"warnings,omitempty"`
}

// RetentionStats provides statistics about retention management
type RetentionStats struct {
	ActiveBulkLoads    int   `json:"active_bulk_loads"`
	CompletedBulkLoads int   `json:"completed_bulk_loads"`
	CleanedBulkLoads   int   `json:"cleaned_bulk_loads"`
	TotalBytesFreed    int64 `json:"total_bytes_freed"`
	TotalArchived      int   `json:"total_archived"`
	TopicsManaged      int   `json:"topics_managed"`
}

// PartitionOffset represents offset for a partition
type PartitionOffset struct {
	Partition int32 `json:"partition"`
	Offset    int64 `json:"offset"`
}

// ConsumerGroupOffsets represents offsets for a consumer group
type ConsumerGroupOffsets struct {
	ConsumerGroup string            `json:"consumer_group"`
	Topic         string            `json:"topic"`
	Offsets       []PartitionOffset `json:"offsets"`
	TotalOffset   int64             `json:"total_offset"`
	TotalLag      int64             `json:"total_lag"`
	LastCommit    time.Time         `json:"last_commit"`
}
