package workers

import (
	"context"
	"strings"

	log "github.com/sirupsen/logrus"
)

// ModeSelector decides between batch and CDC based on requirements
type ModeSelector struct {
	logger *log.Entry
}

// PipelineMode represents execution mode
type PipelineMode string

const (
	ModeBatch  PipelineMode = "batch"
	ModeCDC    PipelineMode = "cdc"
	ModeStream PipelineMode = "stream"
)

// ModeDecision contains mode selection result
type ModeDecision struct {
	Mode       PipelineMode           `json:"mode"`
	Reasoning  string                 `json:"reasoning"`
	Confidence float64                `json:"confidence"`
	Parameters map[string]interface{} `json:"parameters"`
}

// ModeSelectionRequest contains information needed for mode selection
type ModeSelectionRequest struct {
	Intent              string
	SourceConnectorType string
	DestConnectorType   string
	SourceMetadata      SourceMetadata
	Context             map[string]interface{}
}

// SourceMetadata contains metadata about the source
type SourceMetadata struct {
	EstimatedRowCount int64
	AvgRowSizeBytes   int64
	HasTimestamps     bool
	SupportsCDC       bool
}

// NewModeSelector creates a new mode selector
func NewModeSelector() *ModeSelector {
	return &ModeSelector{
		logger: log.WithField("component", "mode_selector"),
	}
}

// SelectMode analyzes requirements and chooses optimal mode
func (ms *ModeSelector) SelectMode(ctx context.Context, req ModeSelectionRequest) (*ModeDecision, error) {
	ms.logger.Info("🔍 Starting mode selection analysis")

	// Factor 1: Data freshness requirement
	freshnessScore := ms.analyzeFreshnessRequirement(req.Intent)
	ms.logger.WithField("freshness_score", freshnessScore).Debug("Analyzed freshness requirement")

	// Factor 2: Data volume
	volumeScore := ms.analyzeDataVolume(req.SourceMetadata)
	ms.logger.WithField("volume_score", volumeScore).Debug("Analyzed data volume")

	// Factor 3: Source capabilities
	supportsCDC := ms.checkCDCSupport(req.SourceConnectorType)
	ms.logger.WithField("supports_cdc", supportsCDC).Debug("Checked CDC support")

	// Factor 4: Frequency (from intent or explicit)
	isOneTime := ms.isOneTimeSync(req.Intent)
	ms.logger.WithField("is_one_time", isOneTime).Debug("Checked sync frequency")

	// Decision logic
	var decision ModeDecision

	// Rule 1: Real-time requirement → CDC
	if freshnessScore > 0.7 && supportsCDC {
		decision.Mode = ModeCDC
		decision.Reasoning = "Real-time data freshness required, source supports CDC"
		decision.Confidence = 0.9
		decision.Parameters = map[string]interface{}{
			"offset_storage": "kafka",
			"snapshot_mode":  "initial",
			"poll_interval":  "100ms",
		}
		ms.logger.WithFields(log.Fields{
			"mode":       decision.Mode,
			"confidence": decision.Confidence,
		}).Info("✅ Selected CDC mode (real-time requirement)")
		return &decision, nil
	}

	// Rule 2: Large volume + one-time → Batch
	if volumeScore > 0.5 && isOneTime {
		batchSize := ms.calculateOptimalBatchSize(req.SourceMetadata)
		parallelism := ms.calculateParallelism(volumeScore)

		decision.Mode = ModeBatch
		decision.Reasoning = "Large historical dataset, one-time migration"
		decision.Confidence = 0.85
		decision.Parameters = map[string]interface{}{
			"batch_size":       batchSize,
			"parallel_workers": parallelism,
			"checkpoint_every": batchSize * 10,
		}
		ms.logger.WithFields(log.Fields{
			"mode":        decision.Mode,
			"batch_size":  batchSize,
			"parallelism": parallelism,
		}).Info("✅ Selected Batch mode (large one-time)")
		return &decision, nil
	}

	// Rule 3: Continuous sync requirement → CDC
	if ms.detectContinuousSync(req.Intent) {
		if supportsCDC {
			decision.Mode = ModeCDC
			decision.Reasoning = "Continuous synchronization required"
			decision.Confidence = 0.8
			decision.Parameters = map[string]interface{}{
				"offset_storage": "kafka",
				"snapshot_mode":  "initial",
			}
		} else {
			decision.Mode = ModeBatch
			decision.Reasoning = "Continuous sync desired but CDC not supported, using scheduled batch"
			decision.Confidence = 0.6
			decision.Parameters = map[string]interface{}{
				"schedule":   "*/5 * * * *", // Every 5 minutes
				"batch_size": 1000,
			}
		}
		ms.logger.WithFields(log.Fields{
			"mode":       decision.Mode,
			"confidence": decision.Confidence,
		}).Info("✅ Selected mode for continuous sync")
		return &decision, nil
	}

	// Rule 4: Small dataset → Simple batch
	if volumeScore < 0.3 {
		decision.Mode = ModeBatch
		decision.Reasoning = "Small dataset, simple batch transfer is optimal"
		decision.Confidence = 0.8
		decision.Parameters = map[string]interface{}{
			"batch_size":       500,
			"parallel_workers": 1,
		}
		ms.logger.Info("✅ Selected Batch mode (small dataset)")
		return &decision, nil
	}

	// Default: Batch for safety
	decision.Mode = ModeBatch
	decision.Reasoning = "Default batch mode for standard data transfer"
	decision.Confidence = 0.7
	decision.Parameters = map[string]interface{}{
		"batch_size": 1000,
	}

	ms.logger.Info("✅ Selected default Batch mode")
	return &decision, nil
}

// Helper methods

// analyzeFreshnessRequirement determines how critical real-time data is
func (ms *ModeSelector) analyzeFreshnessRequirement(intent string) float64 {
	intentLower := strings.ToLower(intent)

	// Keywords indicating real-time need
	realtimeKeywords := []string{
		"real-time", "realtime", "live", "immediate", "instant",
		"continuous", "stream", "streaming", "real time",
	}

	for _, keyword := range realtimeKeywords {
		if strings.Contains(intentLower, keyword) {
			return 0.9
		}
	}

	// Check for update frequency mentions
	if strings.Contains(intentLower, "every few seconds") ||
		strings.Contains(intentLower, "as soon as") ||
		strings.Contains(intentLower, "instantly") {
		return 0.8
	}

	// Check for near-real-time
	if strings.Contains(intentLower, "near real-time") ||
		strings.Contains(intentLower, "near-realtime") {
		return 0.7
	}

	return 0.3 // Default: not real-time critical
}

// analyzeDataVolume calculates a score based on data volume
func (ms *ModeSelector) analyzeDataVolume(metadata SourceMetadata) float64 {
	estimatedRows := metadata.EstimatedRowCount

	if estimatedRows > 10000000 { // >10M rows
		return 1.0
	} else if estimatedRows > 1000000 { // >1M rows
		return 0.7
	} else if estimatedRows > 100000 { // >100K rows
		return 0.5
	}

	return 0.3 // Small dataset
}

// checkCDCSupport determines if a connector supports CDC
func (ms *ModeSelector) checkCDCSupport(connectorType string) bool {
	connectorLower := strings.ToLower(connectorType)

	cdcSupportedSources := map[string]bool{
		"mysql":      true,
		"postgresql": true,
		"postgres":   true,
		"mongodb":    true,
		"mongo":      true,
		"sqlserver":  true,
		"mssql":      true,
		"oracle":     true,
		"debezium":   true,
	}

	return cdcSupportedSources[connectorLower]
}

// isOneTimeSync determines if this is a one-time migration
func (ms *ModeSelector) isOneTimeSync(intent string) bool {
	intentLower := strings.ToLower(intent)

	oneTimeKeywords := []string{
		"migrate", "migration", "one-time", "one time",
		"initial load", "historical", "archive", "backup",
	}

	for _, keyword := range oneTimeKeywords {
		if strings.Contains(intentLower, keyword) {
			return true
		}
	}

	return false
}

// detectContinuousSync determines if continuous synchronization is needed
func (ms *ModeSelector) detectContinuousSync(intent string) bool {
	intentLower := strings.ToLower(intent)

	continuousKeywords := []string{
		"continuous", "continuously", "ongoing", "keep in sync",
		"replicate", "replication", "sync continuously",
		"always in sync", "stay in sync",
	}

	for _, keyword := range continuousKeywords {
		if strings.Contains(intentLower, keyword) {
			return true
		}
	}

	return false
}

// calculateOptimalBatchSize determines the best batch size based on metadata
func (ms *ModeSelector) calculateOptimalBatchSize(metadata SourceMetadata) int {
	estimatedRows := metadata.EstimatedRowCount
	avgRowSize := metadata.AvgRowSizeBytes

	// For large rows, use smaller batches
	if avgRowSize > 10000 { // >10KB per row
		return 500
	}

	// Based on total row count
	if estimatedRows > 10000000 {
		return 5000 // Large datasets: bigger batches
	} else if estimatedRows > 1000000 {
		return 2000
	} else if estimatedRows > 100000 {
		return 1000
	}

	return 500 // Default for small datasets
}

// calculateParallelism determines optimal number of parallel workers
func (ms *ModeSelector) calculateParallelism(volumeScore float64) int {
	if volumeScore > 0.8 {
		return 4 // High volume: 4 parallel workers
	} else if volumeScore > 0.5 {
		return 2
	}
	return 1 // Single worker for small datasets
}

// ExtractSourceMetadata extracts metadata from task context
func ExtractSourceMetadata(task Task) SourceMetadata {
	metadata := SourceMetadata{
		EstimatedRowCount: 0,
		AvgRowSizeBytes:   1000, // Default 1KB
		HasTimestamps:     false,
		SupportsCDC:       false,
	}

	// Try to extract from discovery result
	if discoveryResult, ok := task.Context["discovery_result"].(map[string]interface{}); ok {
		if rowCount, ok := discoveryResult["estimated_row_count"].(float64); ok {
			metadata.EstimatedRowCount = int64(rowCount)
		}
		if avgSize, ok := discoveryResult["avg_row_size"].(float64); ok {
			metadata.AvgRowSizeBytes = int64(avgSize)
		}
	}

	// Fallback: try direct fields
	if rowCount, ok := task.Payload["estimated_rows"].(float64); ok {
		metadata.EstimatedRowCount = int64(rowCount)
	} else if rowCount, ok := task.Payload["estimated_rows"].(int); ok {
		metadata.EstimatedRowCount = int64(rowCount)
	}

	return metadata
}

// GetStringFromContext safely extracts a string from context
func GetStringFromContext(m map[string]interface{}, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}
