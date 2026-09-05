package retention

import (
	"context"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

// AgentState represents state of the retention agent
type AgentState string

const (
	AgentStopped  AgentState = "stopped"
	AgentStarting AgentState = "starting"
	AgentRunning  AgentState = "running"
	AgentPaused   AgentState = "paused"
	AgentError    AgentState = "error"
)

// Agent is the main Retention Manager Agent
type Agent struct {
	config *Config

	// Components
	offsetTracker  *OffsetTracker
	bulkDetector   *BulkLoadDetector
	safetyChecker  *SafetyChecker
	cleanupManager *CleanupManager

	// State
	state AgentState
	mu    sync.RWMutex

	// Control
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Stats
	stats struct {
		StartTime          time.Time
		BulkLoadsMonitored int64
		CleanupsTrigggered int64
		BytesFreed         int64
		Errors             int64
	}
}

// NewAgent creates a new Retention Manager Agent
func NewAgent(config *Config) *Agent {
	offsetTracker := NewOffsetTracker(config)
	bulkDetector := NewBulkLoadDetector(config, offsetTracker)
	safetyChecker := NewSafetyChecker(config, offsetTracker)
	cleanupManager := NewCleanupManager(config, safetyChecker)

	return &Agent{
		config:         config,
		offsetTracker:  offsetTracker,
		bulkDetector:   bulkDetector,
		safetyChecker:  safetyChecker,
		cleanupManager: cleanupManager,
		state:          AgentStopped,
	}
}

// Start starts the retention manager agent
func (a *Agent) Start(ctx context.Context) error {
	a.mu.Lock()
	if a.state == AgentRunning {
		a.mu.Unlock()
		log.Println("[RetentionAgent] Already running")
		return nil
	}

	a.state = AgentStarting
	a.mu.Unlock()

	log.Println("[RetentionAgent] Starting Retention Manager Agent...")

	a.ctx, a.cancel = context.WithCancel(ctx)

	// Start monitoring loop
	a.wg.Add(1)
	go a.monitoringLoop()

	// Start cleanup check loop
	a.wg.Add(1)
	go a.cleanupLoop()

	// Start retention restore loop
	if a.config.Retention.RestoreRetentionAfterCleanup {
		a.wg.Add(1)
		go a.restoreLoop()
	}

	a.mu.Lock()
	a.state = AgentRunning
	a.stats.StartTime = time.Now()
	a.mu.Unlock()

	log.Println("[RetentionAgent] Retention Manager Agent started")
	return nil
}

// Stop stops the retention manager agent
func (a *Agent) Stop() error {
	log.Println("[RetentionAgent] Stopping Retention Manager Agent...")

	a.mu.Lock()
	a.state = AgentStopped
	a.mu.Unlock()

	if a.cancel != nil {
		a.cancel()
	}

	a.wg.Wait()

	// Restore all modified retentions before stopping
	if errs := a.cleanupManager.RestoreAllRetentions(context.Background()); len(errs) > 0 {
		for _, err := range errs {
			log.Printf("[RetentionAgent] Error restoring retention: %v", err)
		}
	}

	log.Println("[RetentionAgent] Retention Manager Agent stopped")
	return nil
}

// State returns current agent state
func (a *Agent) State() AgentState {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.state
}

// IsRunning checks if agent is running
func (a *Agent) IsRunning() bool {
	return a.State() == AgentRunning
}

// === Bulk Load Management ===

// RegisterBulkLoad registers a new bulk load for monitoring
func (a *Agent) RegisterBulkLoad(
	topic string,
	pipelineID string,
	startOffset int64,
	endOffset int64,
	expectedConsumers []string,
) (*BulkLoadInfo, error) {
	info, err := a.bulkDetector.RegisterBulkLoad(
		topic, pipelineID, startOffset, endOffset, expectedConsumers,
	)
	if err != nil {
		return nil, err
	}

	a.mu.Lock()
	a.stats.BulkLoadsMonitored++
	a.mu.Unlock()

	return info, nil
}

// DetectBulkLoad automatically detects a bulk load from a topic
func (a *Agent) DetectBulkLoad(
	ctx context.Context,
	topic string,
	pipelineID string,
) (*BulkLoadInfo, error) {
	info, err := a.bulkDetector.DetectBulkLoad(ctx, topic, pipelineID)
	if err != nil {
		return nil, err
	}

	a.mu.Lock()
	a.stats.BulkLoadsMonitored++
	a.mu.Unlock()

	return info, nil
}

// GetBulkLoad gets a bulk load by ID
func (a *Agent) GetBulkLoad(id string) *BulkLoadInfo {
	return a.bulkDetector.GetBulkLoad(id)
}

// GetActiveBulkLoads gets all active bulk loads
func (a *Agent) GetActiveBulkLoads() []*BulkLoadInfo {
	return a.bulkDetector.GetActiveBulkLoads()
}

// GetAllBulkLoads gets all bulk loads
func (a *Agent) GetAllBulkLoads() []*BulkLoadInfo {
	return a.bulkDetector.GetAllBulkLoads()
}

// === Cleanup Operations ===

// TriggerCleanup manually triggers cleanup for a bulk load
func (a *Agent) TriggerCleanup(
	ctx context.Context,
	bulkLoadID string,
	action CleanupAction,
) (*CleanupResult, error) {
	bl := a.bulkDetector.GetBulkLoad(bulkLoadID)
	if bl == nil {
		return nil, &BulkLoadNotFoundError{ID: bulkLoadID}
	}

	result, err := a.cleanupManager.ExecuteCleanup(ctx, bl, action)
	if err != nil {
		a.mu.Lock()
		a.stats.Errors++
		a.mu.Unlock()
		return result, err
	}

	// Update bulk load status
	if result.Success {
		if action == ActionArchiveToS3 {
			a.bulkDetector.MarkBulkLoadArchived(bulkLoadID, result.ArchiveLocation)
		} else {
			a.bulkDetector.MarkBulkLoadCleaned(bulkLoadID, action)
		}

		a.mu.Lock()
		a.stats.CleanupsTrigggered++
		a.stats.BytesFreed += result.BytesFreed
		a.mu.Unlock()
	}

	return result, nil
}

// CheckCleanupReadiness checks if a bulk load is ready for cleanup
func (a *Agent) CheckCleanupReadiness(
	ctx context.Context,
	bulkLoadID string,
) (*SafetyCheckResult, error) {
	bl := a.bulkDetector.GetBulkLoad(bulkLoadID)
	if bl == nil {
		return nil, &BulkLoadNotFoundError{ID: bulkLoadID}
	}

	return a.safetyChecker.CanCleanupSafely(ctx, bl)
}

// === Consumer Progress ===

// GetConsumerProgress gets progress for a consumer group
func (a *Agent) GetConsumerProgress(
	ctx context.Context,
	consumerGroup, topic string,
	targetOffset int64,
) (*ConsumerProgress, error) {
	return a.offsetTracker.GetConsumerProgress(ctx, consumerGroup, topic, targetOffset)
}

// GetBulkLoadProgress gets progress for a bulk load
func (a *Agent) GetBulkLoadProgress(
	ctx context.Context,
	bulkLoadID string,
) (float64, map[string]*ConsumerProgress, error) {
	bl := a.bulkDetector.GetBulkLoad(bulkLoadID)
	if bl == nil {
		return 0, nil, &BulkLoadNotFoundError{ID: bulkLoadID}
	}

	progress, progressMap := a.offsetTracker.GetBulkLoadProgress(ctx, bl)
	return progress, progressMap, nil
}

// === Background Loops ===

// monitoringLoop monitors active bulk loads
func (a *Agent) monitoringLoop() {
	defer a.wg.Done()

	ticker := time.NewTicker(time.Duration(a.config.Monitoring.CheckIntervalSecs) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-a.ctx.Done():
			return
		case <-ticker.C:
			if a.State() != AgentRunning {
				continue
			}

			a.monitorActiveBulkLoads()
		}
	}
}

// monitorActiveBulkLoads monitors all active bulk loads
func (a *Agent) monitorActiveBulkLoads() {
	bulkLoads := a.bulkDetector.GetActiveBulkLoads()

	for _, bl := range bulkLoads {
		// Update progress
		updated, err := a.bulkDetector.UpdateBulkLoadProgress(a.ctx, bl.ID)
		if err != nil {
			log.Printf("[RetentionAgent] Error updating progress for %s: %v", bl.ID, err)
			continue
		}

		// Log progress
		if updated.Progress > 0 && updated.Progress < 100 {
			log.Printf("[RetentionAgent] Bulk load %s: %.1f%% complete", bl.ID, updated.Progress)
		}

		// Check if ready for cleanup
		if updated.IsAllCaughtUp() && updated.Status == StatusMonitoring {
			// Perform quick check
			ready, reason := a.safetyChecker.PerformQuickCheck(a.ctx, updated)
			if ready {
				log.Printf("[RetentionAgent] Bulk load %s ready for cleanup: %s", bl.ID, reason)
				a.bulkDetector.MarkBulkLoadCompleted(bl.ID)
			}
		}
	}
}

// cleanupLoop checks for cleanup opportunities
func (a *Agent) cleanupLoop() {
	defer a.wg.Done()

	ticker := time.NewTicker(time.Duration(a.config.Monitoring.CleanupCheckIntervalSecs) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-a.ctx.Done():
			return
		case <-ticker.C:
			if a.State() != AgentRunning {
				continue
			}

			a.checkForCleanupOpportunities()
		}
	}
}

// checkForCleanupOpportunities checks for bulk loads ready for cleanup
func (a *Agent) checkForCleanupOpportunities() {
	bulkLoads := a.bulkDetector.GetAllBulkLoads()

	for _, bl := range bulkLoads {
		if bl.Status != StatusCompleted {
			continue
		}

		// Perform full safety check
		safetyResult, err := a.safetyChecker.CanCleanupSafely(a.ctx, bl)
		if err != nil {
			log.Printf("[RetentionAgent] Safety check error for %s: %v", bl.ID, err)
			continue
		}

		if safetyResult.Safe {
			log.Printf("[RetentionAgent] Triggering automatic cleanup for %s", bl.ID)

			// Determine cleanup action
			action := ActionReduceRetention
			if a.config.Retention.ArchiveBeforeCleanup && a.config.Archive.Enabled {
				action = ActionArchiveToS3
			}

			result, err := a.cleanupManager.ExecuteCleanup(a.ctx, bl, action)
			if err != nil {
				log.Printf("[RetentionAgent] Cleanup failed for %s: %v", bl.ID, err)
				a.mu.Lock()
				a.stats.Errors++
				a.mu.Unlock()
			} else if result.Success {
				if action == ActionArchiveToS3 {
					a.bulkDetector.MarkBulkLoadArchived(bl.ID, result.ArchiveLocation)
				} else {
					a.bulkDetector.MarkBulkLoadCleaned(bl.ID, action)
				}

				a.mu.Lock()
				a.stats.CleanupsTrigggered++
				a.stats.BytesFreed += result.BytesFreed
				a.mu.Unlock()
			}
		}
	}
}

// restoreLoop restores modified topic retentions
func (a *Agent) restoreLoop() {
	defer a.wg.Done()

	ticker := time.NewTicker(time.Duration(a.config.Monitoring.RestoreCheckIntervalSecs) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-a.ctx.Done():
			return
		case <-ticker.C:
			if a.State() != AgentRunning {
				continue
			}

			// Restore retentions for cleaned bulk loads
			topics := a.cleanupManager.GetModifiedTopics()
			for _, topic := range topics {
				if err := a.cleanupManager.RestoreRetention(a.ctx, topic); err != nil {
					log.Printf("[RetentionAgent] Error restoring retention for %s: %v", topic, err)
				}
			}

			// Cleanup old bulk loads
			a.bulkDetector.CleanupOldBulkLoads(24 * time.Hour)
		}
	}
}

// === Status and Metrics ===

// GetStatus returns agent status
func (a *Agent) GetStatus() map[string]interface{} {
	a.mu.RLock()
	defer a.mu.RUnlock()

	var uptime float64
	if !a.stats.StartTime.IsZero() {
		uptime = time.Since(a.stats.StartTime).Seconds()
	}

	bulkStats := a.bulkDetector.GetStats()

	return map[string]interface{}{
		"state":          a.state,
		"uptime_seconds": uptime,
		"config": map[string]interface{}{
			"bulk_threshold":    a.config.Retention.BulkLoadThreshold,
			"archive_enabled":   a.config.Archive.Enabled,
			"grace_period_mins": a.config.Safety.GracePeriodMins,
			"min_wait_mins":     a.config.Safety.MinWaitAfterLoadMins,
		},
		"statistics": map[string]interface{}{
			"bulk_loads_monitored": a.stats.BulkLoadsMonitored,
			"cleanups_triggered":   a.stats.CleanupsTrigggered,
			"bytes_freed":          a.stats.BytesFreed,
			"errors":               a.stats.Errors,
		},
		"bulk_loads":      bulkStats,
		"modified_topics": a.cleanupManager.GetModifiedTopics(),
	}
}

// GetCleanupHistory returns cleanup history
func (a *Agent) GetCleanupHistory(limit int) []CleanupResult {
	return a.cleanupManager.GetCleanupHistory(limit)
}

// === Errors ===

// BulkLoadNotFoundError is returned when a bulk load is not found
type BulkLoadNotFoundError struct {
	ID string
}

func (e *BulkLoadNotFoundError) Error() string {
	return "bulk load not found: " + e.ID
}
