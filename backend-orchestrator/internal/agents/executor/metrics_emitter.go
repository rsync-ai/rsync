package executor

import (
	"encoding/json"
	"sync"
	"time"
)

// FileDetail is a bounded per-flush record of recent file writes.
// It is intentionally small and used only for "recent_files[]" reporting.
type FileDetail struct {
	StorageKey     string `json:"storage_key"`
	TableName      string `json:"table_name,omitempty"`
	RowsWritten    int64  `json:"rows_written,omitempty"`
	BytesWritten   int64  `json:"bytes_written,omitempty"`
	SequenceNumber int64  `json:"sequence_number,omitempty"`
	WrittenAt      string `json:"written_at,omitempty"` // RFC3339
}

type MetricsEmitterConfig struct {
	PipelineID  string
	ExecutionID string
	TraceID     string
	Source      string // e.g. "executor_batch"

	MinFlushInterval        time.Duration
	MaxFlushInterval        time.Duration
	DefaultMaxFilesPerFlush int
	MaxPayloadBytes         int // approximate serialized bytes for recent_files[]
}

// MetricsEmitter accumulates file write details and emits DATA_PLANE_METRICS events.
// It is best-effort: failures to emit MUST NOT fail the pipeline.
type MetricsEmitter struct {
	cfg    MetricsEmitterConfig
	emitFn func(event []byte, headers map[string]string) error

	mu sync.Mutex

	// dynamic tuning
	maxFilesPerFlush int

	// accumulation
	seq              int64
	filesWritten     int64
	totalRows        int64
	totalBytes       int64
	currentTable     string
	recentFiles      []FileDetail
	recentFilesBytes int

	lastFlush time.Time
}

func NewMetricsEmitter(cfg MetricsEmitterConfig, emitFn func(event []byte, headers map[string]string) error) *MetricsEmitter {
	maxFiles := cfg.DefaultMaxFilesPerFlush
	if maxFiles <= 0 {
		maxFiles = 100
	}
	if cfg.MinFlushInterval <= 0 {
		cfg.MinFlushInterval = 2 * time.Second
	}
	if cfg.MaxFlushInterval <= 0 {
		cfg.MaxFlushInterval = 10 * time.Second
	}
	if cfg.MaxPayloadBytes <= 0 {
		cfg.MaxPayloadBytes = 300 * 1024
	}
	if cfg.Source == "" {
		cfg.Source = "executor_batch"
	}
	return &MetricsEmitter{
		cfg:             cfg,
		emitFn:           emitFn,
		maxFilesPerFlush: maxFiles,
		recentFiles:      make([]FileDetail, 0, maxFiles),
		lastFlush:        time.Now(),
	}
}

// SetExpectedFileCount dynamically adjusts maxFilesPerFlush.
// This reduces event spam for huge runs while keeping UI responsive for small runs.
func (m *MetricsEmitter) SetExpectedFileCount(expectedTotalFiles int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	switch {
	case expectedTotalFiles > 5000:
		m.maxFilesPerFlush = 250
	case expectedTotalFiles > 0 && expectedTotalFiles < 100:
		m.maxFilesPerFlush = 50
	default:
		if m.cfg.DefaultMaxFilesPerFlush > 0 {
			m.maxFilesPerFlush = m.cfg.DefaultMaxFilesPerFlush
		} else {
			m.maxFilesPerFlush = 100
		}
	}
}

func (m *MetricsEmitter) ObserveTotals(currentTable string, totalRows, totalBytes int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.currentTable = currentTable
	m.totalRows = totalRows
	m.totalBytes = totalBytes
}

func (m *MetricsEmitter) ObserveFileWrite(tableName, storageKey string, rowsWritten, bytesWritten int64) {
	if storageKey == "" {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Ensure we don't exceed payload size: if adding this entry would exceed the cap,
	// flush first (best-effort) and then record.
	entry := FileDetail{
		StorageKey:     storageKey,
		TableName:      tableName,
		RowsWritten:    rowsWritten,
		BytesWritten:   bytesWritten,
		SequenceNumber: m.seq + 1,
		WrittenAt:      time.Now().UTC().Format(time.RFC3339),
	}

	// Estimate bytes by JSON marshaling a single entry (cheap enough at flush cadence).
	estBytes := 0
	if b, err := json.Marshal(entry); err == nil {
		estBytes = len(b)
	}

	// If we already have entries and adding this one would exceed the cap, trigger a flush soon.
	// We don't flush synchronously here to avoid holding the lock while emitting.
	if m.recentFilesBytes > 0 && estBytes > 0 && (m.recentFilesBytes+estBytes) > m.cfg.MaxPayloadBytes {
		// Mark as needing flush by setting lastFlush far in the past; caller's MaybeFlush() will flush.
		m.lastFlush = time.Now().Add(-2 * m.cfg.MaxFlushInterval)
	}

	m.seq++
	m.filesWritten++
	m.recentFiles = append(m.recentFiles, entry)
	m.recentFilesBytes += estBytes
}

// MaybeFlush flushes accumulated metrics if conditions are met.
// If force=true, it will flush regardless of intervals (flush-on-completion).
func (m *MetricsEmitter) MaybeFlush(force bool) error {
	// Fast path: check conditions under lock and snapshot payload
	m.mu.Lock()
	now := time.Now()
	since := now.Sub(m.lastFlush)
	should := force ||
		(len(m.recentFiles) >= m.maxFilesPerFlush && since >= m.cfg.MinFlushInterval) ||
		(since >= m.cfg.MaxFlushInterval)

	if !should || m.emitFn == nil {
		m.mu.Unlock()
		return nil
	}

	// Snapshot and reset recent-files buffer (counters remain cumulative).
	recent := make([]FileDetail, len(m.recentFiles))
	copy(recent, m.recentFiles)
	m.recentFiles = m.recentFiles[:0]
	m.recentFilesBytes = 0
	m.lastFlush = now

	pipelineID := m.cfg.PipelineID
	execID := m.cfg.ExecutionID
	traceID := m.cfg.TraceID
	source := m.cfg.Source
	totalRows := m.totalRows
	totalBytes := m.totalBytes
	currentTable := m.currentTable
	filesWritten := m.filesWritten
	m.mu.Unlock()

	// Build event (schema_version=2 domain event)
	event := map[string]interface{}{
		"schema_version": 2,
		"event_type":     "DATA_PLANE_METRICS",
		"pipeline_id":    pipelineID,
		"execution_id":   execID,
		"trace_id":       traceID,
		"timestamp":      time.Now().UTC().Format(time.RFC3339),
		"stage":          "executor",
		"stage_group":    "executing",
		"status":         "processing",
		"message":        "Data plane metrics update",
		"metadata": map[string]interface{}{
			"source":         source,
			"metrics_schema": "v2",
			"metrics": map[string]interface{}{
				"records_read":    totalRows,
				"bytes_read":      totalBytes,
				"files_written":   filesWritten,
				"recent_files":    recent,
			},
			// Legacy fields (backward compatibility)
			"rows_processed":  totalRows,
			"bytes_processed": totalBytes,
			"table_name":      currentTable,
		},
	}

	b, err := json.Marshal(event)
	if err != nil {
		return nil
	}

	headers := map[string]string{}
	if traceID != "" {
		headers["trace_id"] = traceID
	}

	// best-effort emit
	_ = m.emitFn(b, headers)
	return nil
}

