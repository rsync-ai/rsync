package idempotency

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
)

// OperationType represents the type of idempotent operation
type OperationType string

const (
	OpTypeDiscovery     OperationType = "discovery"
	OpTypeValidation    OperationType = "validation"
	OpTypeExecution     OperationType = "execution"
	OpTypeExecutorChunk OperationType = "executor_chunk"
	OpTypeConnection    OperationType = "connection"
	OpTypeCapability    OperationType = "capability"
	OpTypeIntent        OperationType = "intent"
	OpTypePlanner       OperationType = "planner"
	OpTypeTransform     OperationType = "transform"
)

// Status represents the status of an idempotent operation
type Status string

const (
	StatusCompleted  Status = "completed"
	StatusFailed     Status = "failed"
	StatusInProgress Status = "in_progress"
)

// Key represents a deterministic idempotency key
type Key struct {
	PipelineID  string
	ExecutionID string
	StepID      string
	ChunkID     string // Optional, for chunked operations
}

// String generates the idempotency key string
func (k Key) String() string {
	if k.ChunkID == "" {
		return fmt.Sprintf("%s-%s-%s", k.PipelineID, k.ExecutionID, k.StepID)
	}
	return fmt.Sprintf("%s-%s-%s-%s", k.PipelineID, k.ExecutionID, k.StepID, k.ChunkID)
}

// Record represents an idempotency log record
type Record struct {
	IdempotencyKey string
	PipelineID     string
	ExecutionID    string
	StepID         string
	ChunkID        string
	OperationType  OperationType
	Status         Status
	ResultData     map[string]interface{}
	ErrorMessage   string
	StartedAt      time.Time
	CompletedAt    *time.Time
	ExpiresAt      time.Time
}

// Manager handles idempotency checks and recording
type Manager struct {
	db *sql.DB
}

// NewManager creates a new idempotency manager
func NewManager(db *sql.DB) *Manager {
	return &Manager{db: db}
}

// CheckOrStart checks if an operation was already completed or starts a new one
// Returns: (completed bool, cachedResult map, error)
func (m *Manager) CheckOrStart(ctx context.Context, key Key, opType OperationType) (bool, map[string]interface{}, error) {
	idempotencyKey := key.String()

	// Check if operation already exists
	var status string
	var resultData []byte
	var completedAt sql.NullTime

	query := `
		SELECT status, result_data, completed_at
		FROM idempotency_log
		WHERE idempotency_key = $1
	`

	err := m.db.QueryRowContext(ctx, query, idempotencyKey).Scan(&status, &resultData, &completedAt)

	if err == sql.ErrNoRows {
		// Operation not found, start new one
		err := m.start(ctx, key, opType)
		if err != nil {
			return false, nil, fmt.Errorf("failed to start idempotency record: %w", err)
		}
		return false, nil, nil
	}

	if err != nil {
		return false, nil, fmt.Errorf("failed to check idempotency: %w", err)
	}

	// Operation already exists
	switch Status(status) {
	case StatusCompleted:
		// Return cached result
		var result map[string]interface{}
		if len(resultData) > 0 {
			if err := json.Unmarshal(resultData, &result); err != nil {
				log.WithError(err).Warn("Failed to unmarshal cached result, proceeding with retry")
				return false, nil, nil
			}
		}
		log.WithFields(log.Fields{
			"idempotency_key": idempotencyKey,
			"operation_type":  opType,
			"completed_at":    completedAt.Time,
		}).Info("⏭️  Operation already completed, returning cached result")
		return true, result, nil

	case StatusInProgress:
		// Check if it's stale (started more than 10 minutes ago)
		var startedAt time.Time
		query := `SELECT started_at FROM idempotency_log WHERE idempotency_key = $1`
		if err := m.db.QueryRowContext(ctx, query, idempotencyKey).Scan(&startedAt); err == nil {
			if time.Since(startedAt) > 10*time.Minute {
				log.WithFields(log.Fields{
					"idempotency_key": idempotencyKey,
					"started_at":      startedAt,
				}).Warn("⚠️  Stale in-progress operation detected, resetting")
				// Reset to allow retry
				if err := m.Reset(ctx, key); err != nil {
					return false, nil, fmt.Errorf("failed to reset stale operation: %w", err)
				}
				return false, nil, nil
			}
		}
		// Still in progress, consider it a duplicate
		log.WithField("idempotency_key", idempotencyKey).Info("⏳ Operation in progress, skipping duplicate")
		return true, nil, nil

	case StatusFailed:
		// Allow retry for failed operations
		log.WithField("idempotency_key", idempotencyKey).Info("🔄 Previous operation failed, allowing retry")
		if err := m.Reset(ctx, key); err != nil {
			return false, nil, fmt.Errorf("failed to reset failed operation: %w", err)
		}
		return false, nil, nil
	}

	return false, nil, nil
}

// start creates a new in-progress idempotency record
func (m *Manager) start(ctx context.Context, key Key, opType OperationType) error {
	idempotencyKey := key.String()

	query := `
		INSERT INTO idempotency_log (
			idempotency_key, pipeline_id, execution_id, step_id, chunk_id,
			operation_type, status, started_at, expires_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), NOW() + INTERVAL '7 days')
		ON CONFLICT (idempotency_key) DO NOTHING
	`

	_, err := m.db.ExecContext(ctx, query,
		idempotencyKey, key.PipelineID, key.ExecutionID, key.StepID, key.ChunkID,
		opType, StatusInProgress,
	)

	if err != nil {
		return fmt.Errorf("failed to insert idempotency record: %w", err)
	}

	log.WithFields(log.Fields{
		"idempotency_key": idempotencyKey,
		"operation_type":  opType,
	}).Debug("▶️  Started idempotency tracking")

	return nil
}

// Complete marks an operation as completed with its result
func (m *Manager) Complete(ctx context.Context, key Key, result map[string]interface{}) error {
	idempotencyKey := key.String()

	resultJSON, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("failed to marshal result: %w", err)
	}

	query := `
		UPDATE idempotency_log
		SET status = $1, result_data = $2, completed_at = NOW()
		WHERE idempotency_key = $3
	`

	_, err = m.db.ExecContext(ctx, query, StatusCompleted, resultJSON, idempotencyKey)
	if err != nil {
		return fmt.Errorf("failed to complete idempotency record: %w", err)
	}

	log.WithField("idempotency_key", idempotencyKey).Debug("✅ Idempotency record completed")
	return nil
}

// Fail marks an operation as failed with error message
func (m *Manager) Fail(ctx context.Context, key Key, errorMsg string) error {
	idempotencyKey := key.String()

	query := `
		UPDATE idempotency_log
		SET status = $1, error_message = $2, completed_at = NOW()
		WHERE idempotency_key = $3
	`

	_, err := m.db.ExecContext(ctx, query, StatusFailed, errorMsg, idempotencyKey)
	if err != nil {
		return fmt.Errorf("failed to mark idempotency record as failed: %w", err)
	}

	log.WithFields(log.Fields{
		"idempotency_key": idempotencyKey,
		"error":           errorMsg,
	}).Debug("❌ Idempotency record marked as failed")
	return nil
}

// Reset resets an idempotency record to allow retry
func (m *Manager) Reset(ctx context.Context, key Key) error {
	idempotencyKey := key.String()

	query := `
		UPDATE idempotency_log
		SET status = $1, started_at = NOW(), completed_at = NULL
		WHERE idempotency_key = $2
	`

	_, err := m.db.ExecContext(ctx, query, StatusInProgress, idempotencyKey)
	if err != nil {
		return fmt.Errorf("failed to reset idempotency record: %w", err)
	}

	log.WithField("idempotency_key", idempotencyKey).Debug("🔄 Idempotency record reset")
	return nil
}

// CleanupExpired removes expired idempotency records
func (m *Manager) CleanupExpired(ctx context.Context) (int64, error) {
	query := `DELETE FROM idempotency_log WHERE expires_at < NOW()`

	result, err := m.db.ExecContext(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("failed to cleanup expired records: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected > 0 {
		log.WithField("count", rowsAffected).Info("🧹 Cleaned up expired idempotency records")
	}

	return rowsAffected, nil
}

// GetRecord retrieves an idempotency record
func (m *Manager) GetRecord(ctx context.Context, key Key) (*Record, error) {
	idempotencyKey := key.String()

	query := `
		SELECT idempotency_key, pipeline_id, execution_id, step_id, chunk_id,
		       operation_type, status, result_data, error_message,
		       started_at, completed_at, expires_at
		FROM idempotency_log
		WHERE idempotency_key = $1
	`

	var record Record
	var resultData []byte
	var completedAt sql.NullTime

	err := m.db.QueryRowContext(ctx, query, idempotencyKey).Scan(
		&record.IdempotencyKey, &record.PipelineID, &record.ExecutionID,
		&record.StepID, &record.ChunkID, &record.OperationType, &record.Status,
		&resultData, &record.ErrorMessage, &record.StartedAt, &completedAt, &record.ExpiresAt,
	)

	if err == sql.ErrNoRows {
		return nil, nil
	}

	if err != nil {
		return nil, fmt.Errorf("failed to get idempotency record: %w", err)
	}

	if completedAt.Valid {
		record.CompletedAt = &completedAt.Time
	}

	if len(resultData) > 0 {
		if err := json.Unmarshal(resultData, &record.ResultData); err != nil {
			log.WithError(err).Warn("Failed to unmarshal result data")
		}
	}

	return &record, nil
}
