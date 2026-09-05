package workers

// DataPlaneMetrics is the standardized payload for DATA_PLANE_METRICS events.
// It works for both batch and CDC pipelines.
//
// For batch pipelines:
//   - rows_processed: cumulative count of rows exported/imported
//   - bytes_processed: cumulative bytes transferred
//   - cdc_lag_ms: nil
//   - cdc_freshness_ms: nil
//
// For CDC pipelines:
//   - rows_processed: cumulative count from Kafka topic offsets (if available)
//   - bytes_processed: cumulative bytes from Kafka topic (if available)
//   - cdc_lag_ms: consumer group lag in milliseconds
//   - cdc_freshness_ms: time since last CDC event in milliseconds
//
// All fields are optional; the UI will gracefully handle missing fields.
type DataPlaneMetrics struct {
	// Common fields (batch + CDC)
	RowsProcessed  *int64 `json:"rows_processed,omitempty"`
	BytesProcessed *int64 `json:"bytes_processed,omitempty"`

	// CDC-specific fields
	CDCLagMs       *int64 `json:"cdc_lag_ms,omitempty"`
	CDCFreshnessMs *int64 `json:"cdc_freshness_ms,omitempty"`

	// Metadata
	Source    string `json:"source"`     // "executor_batch", "executor_cdc", "cdc_status_poll"
	Timestamp string `json:"timestamp"`  // RFC3339
	TableName string `json:"table_name,omitempty"` // For batch: which table is currently being processed
}

