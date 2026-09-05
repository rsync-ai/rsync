package retention

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"

	"github.com/google/uuid"
)

// BulkLoadDetector detects and manages bulk load operations
type BulkLoadDetector struct {
	config        *Config
	offsetTracker *OffsetTracker

	// Registered bulk loads
	bulkLoads map[string]*BulkLoadInfo // key: bulk load ID
	mu        sync.RWMutex

	// Topic to bulk loads mapping
	topicBulkLoads map[string][]string // key: topic, value: bulk load IDs
}

// NewBulkLoadDetector creates a new bulk load detector
func NewBulkLoadDetector(config *Config, offsetTracker *OffsetTracker) *BulkLoadDetector {
	return &BulkLoadDetector{
		config:         config,
		offsetTracker:  offsetTracker,
		bulkLoads:      make(map[string]*BulkLoadInfo),
		topicBulkLoads: make(map[string][]string),
	}
}

// RegisterBulkLoad registers a new bulk load operation
func (d *BulkLoadDetector) RegisterBulkLoad(
	topic string,
	pipelineID string,
	startOffset int64,
	endOffset int64,
	expectedConsumers []string,
) (*BulkLoadInfo, error) {
	messageCount := endOffset - startOffset

	// Check if this qualifies as a bulk load
	if messageCount < d.config.Retention.BulkLoadThreshold {
		log.Printf("[BulkLoadDetector] Load of %d messages doesn't meet bulk threshold (%d)",
			messageCount, d.config.Retention.BulkLoadThreshold)
		// Still register but mark as small
	}

	bulkLoadID := fmt.Sprintf("bulk-%s", uuid.New().String()[:8])

	info := &BulkLoadInfo{
		ID:                bulkLoadID,
		Topic:             topic,
		PipelineID:        pipelineID,
		StartOffset:       startOffset,
		EndOffset:         endOffset,
		MessageCount:      messageCount,
		ExpectedConsumers: expectedConsumers,
		CaughtUpConsumers: make([]string, 0),
		Status:            StatusActive,
		Progress:          0,
		RegisteredAt:      time.Now(),
	}

	d.mu.Lock()
	d.bulkLoads[bulkLoadID] = info
	d.topicBulkLoads[topic] = append(d.topicBulkLoads[topic], bulkLoadID)
	d.mu.Unlock()

	log.Printf("[BulkLoadDetector] Registered bulk load: %s for topic %s (%d messages)",
		bulkLoadID, topic, messageCount)

	return info, nil
}

// DetectBulkLoad automatically detects a bulk load from offset changes
func (d *BulkLoadDetector) DetectBulkLoad(
	ctx context.Context,
	topic string,
	pipelineID string,
) (*BulkLoadInfo, error) {
	// Get current end offset
	endOffset, err := d.offsetTracker.GetTopicEndOffset(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("failed to get end offset: %w", err)
	}

	// Get consumer groups
	consumers, err := d.offsetTracker.GetConsumerGroupsForTopic(ctx, topic)
	if err != nil {
		consumers = []string{}
	}

	// Assume bulk load starts from offset 0 or last known position
	startOffset := int64(0)

	return d.RegisterBulkLoad(topic, pipelineID, startOffset, endOffset, consumers)
}

// GetBulkLoad gets a bulk load by ID
func (d *BulkLoadDetector) GetBulkLoad(id string) *BulkLoadInfo {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.bulkLoads[id]
}

// GetActiveBulkLoads gets all active bulk loads
func (d *BulkLoadDetector) GetActiveBulkLoads() []*BulkLoadInfo {
	d.mu.RLock()
	defer d.mu.RUnlock()

	result := make([]*BulkLoadInfo, 0)
	for _, bl := range d.bulkLoads {
		if bl.Status == StatusActive || bl.Status == StatusMonitoring {
			result = append(result, bl)
		}
	}
	return result
}

// GetAllBulkLoads gets all bulk loads
func (d *BulkLoadDetector) GetAllBulkLoads() []*BulkLoadInfo {
	d.mu.RLock()
	defer d.mu.RUnlock()

	result := make([]*BulkLoadInfo, 0, len(d.bulkLoads))
	for _, bl := range d.bulkLoads {
		result = append(result, bl)
	}
	return result
}

// UpdateBulkLoadProgress updates the progress of a bulk load
func (d *BulkLoadDetector) UpdateBulkLoadProgress(
	ctx context.Context,
	bulkLoadID string,
) (*BulkLoadInfo, error) {
	bl := d.GetBulkLoad(bulkLoadID)
	if bl == nil {
		return nil, fmt.Errorf("bulk load not found: %s", bulkLoadID)
	}

	// Get progress from offset tracker
	progress, progressMap := d.offsetTracker.GetBulkLoadProgress(ctx, bl)

	bl.mu.Lock()
	bl.Progress = progress

	// Update caught up consumers
	for consumer, p := range progressMap {
		if p.CaughtUp {
			bl.MarkCaughtUp(consumer)
		}
	}

	// Check if status should change
	if bl.IsAllCaughtUp() && bl.Status == StatusActive {
		bl.Status = StatusMonitoring
		bl.AllCaughtUpAt = time.Now()
	}

	bl.mu.Unlock()

	return bl, nil
}

// MarkBulkLoadCompleted marks a bulk load as completed
func (d *BulkLoadDetector) MarkBulkLoadCompleted(id string) error {
	bl := d.GetBulkLoad(id)
	if bl == nil {
		return fmt.Errorf("bulk load not found: %s", id)
	}

	bl.mu.Lock()
	bl.Status = StatusCompleted
	bl.mu.Unlock()

	log.Printf("[BulkLoadDetector] Bulk load completed: %s", id)
	return nil
}

// MarkBulkLoadCleaned marks a bulk load as cleaned
func (d *BulkLoadDetector) MarkBulkLoadCleaned(id string, action CleanupAction) error {
	bl := d.GetBulkLoad(id)
	if bl == nil {
		return fmt.Errorf("bulk load not found: %s", id)
	}

	bl.mu.Lock()
	bl.Status = StatusCleaned
	bl.CleanedAt = time.Now()
	bl.CleanupAction = action
	bl.mu.Unlock()

	log.Printf("[BulkLoadDetector] Bulk load cleaned: %s (action: %s)", id, action)
	return nil
}

// MarkBulkLoadArchived marks a bulk load as archived
func (d *BulkLoadDetector) MarkBulkLoadArchived(id string, location string) error {
	bl := d.GetBulkLoad(id)
	if bl == nil {
		return fmt.Errorf("bulk load not found: %s", id)
	}

	bl.mu.Lock()
	bl.Status = StatusArchived
	bl.ArchivedAt = time.Now()
	bl.ArchiveLocation = location
	bl.mu.Unlock()

	log.Printf("[BulkLoadDetector] Bulk load archived: %s (location: %s)", id, location)
	return nil
}

// GetStats returns statistics about bulk loads
func (d *BulkLoadDetector) GetStats() RetentionStats {
	d.mu.RLock()
	defer d.mu.RUnlock()

	stats := RetentionStats{}

	for _, bl := range d.bulkLoads {
		switch bl.Status {
		case StatusActive, StatusMonitoring:
			stats.ActiveBulkLoads++
		case StatusCompleted:
			stats.CompletedBulkLoads++
		case StatusCleaned:
			stats.CleanedBulkLoads++
		case StatusArchived:
			stats.TotalArchived++
		}
	}

	stats.TopicsManaged = len(d.topicBulkLoads)

	return stats
}

// CleanupOldBulkLoads removes bulk loads older than a certain age
func (d *BulkLoadDetector) CleanupOldBulkLoads(maxAge time.Duration) int {
	d.mu.Lock()
	defer d.mu.Unlock()

	cutoff := time.Now().Add(-maxAge)
	removed := 0

	for id, bl := range d.bulkLoads {
		if bl.Status == StatusCleaned || bl.Status == StatusArchived {
			if bl.CleanedAt.Before(cutoff) || bl.ArchivedAt.Before(cutoff) {
				// Remove from topic mapping
				topic := bl.Topic
				ids := d.topicBulkLoads[topic]
				newIDs := make([]string, 0, len(ids)-1)
				for _, existingID := range ids {
					if existingID != id {
						newIDs = append(newIDs, existingID)
					}
				}
				d.topicBulkLoads[topic] = newIDs

				delete(d.bulkLoads, id)
				removed++
			}
		}
	}

	if removed > 0 {
		log.Printf("[BulkLoadDetector] Cleaned up %d old bulk loads", removed)
	}

	return removed
}
