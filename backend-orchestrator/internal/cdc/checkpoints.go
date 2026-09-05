package cdc

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
)

// Checkpoint represents a pipeline checkpoint
type Checkpoint struct {
	ID            string                 `json:"id"`
	PipelineID    string                 `json:"pipeline_id"`
	ConnectionID  string                 `json:"connection_id"`
	SourceTable   string                 `json:"source_table"`
	CDCResourceID *string                `json:"cdc_resource_id,omitempty"`
	Position      map[string]interface{} `json:"position"`
	CreatedAt     time.Time              `json:"created_at"`
	UpdatedAt     time.Time              `json:"updated_at"`
}

// SaveCheckpoint saves or updates a checkpoint for a table
func SaveCheckpoint(ctx context.Context, db *sql.DB, checkpoint Checkpoint) error {
	positionJSON, err := json.Marshal(checkpoint.Position)
	if err != nil {
		return fmt.Errorf("failed to marshal position: %w", err)
	}

	query := `
		INSERT INTO pipeline_checkpoints (
			pipeline_id, connection_id, source_table, cdc_resource_id, position, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, NOW(), NOW())
		ON CONFLICT (pipeline_id, source_table)
		DO UPDATE SET
			position = EXCLUDED.position,
			updated_at = NOW()
		RETURNING id, created_at, updated_at
	`

	err = db.QueryRowContext(
		ctx,
		query,
		checkpoint.PipelineID,
		checkpoint.ConnectionID,
		checkpoint.SourceTable,
		checkpoint.CDCResourceID,
		string(positionJSON),
	).Scan(&checkpoint.ID, &checkpoint.CreatedAt, &checkpoint.UpdatedAt)

	if err != nil {
		return fmt.Errorf("failed to save checkpoint: %w", err)
	}

	log.WithFields(log.Fields{
		"pipeline_id":  checkpoint.PipelineID,
		"source_table": checkpoint.SourceTable,
	}).Debug("Saved checkpoint")

	return nil
}

// GetCheckpoints retrieves all checkpoints for a pipeline
func GetCheckpoints(ctx context.Context, db *sql.DB, pipelineID string) ([]Checkpoint, error) {
	query := `
		SELECT id, pipeline_id, connection_id, source_table, cdc_resource_id, position, created_at, updated_at
		FROM pipeline_checkpoints
		WHERE pipeline_id = $1
		ORDER BY source_table
	`

	rows, err := db.QueryContext(ctx, query, pipelineID)
	if err != nil {
		return nil, fmt.Errorf("failed to query checkpoints: %w", err)
	}
	defer rows.Close()

	var checkpoints []Checkpoint
	for rows.Next() {
		var cp Checkpoint
		var positionJSON []byte
		var cdcResourceID sql.NullString

		err := rows.Scan(
			&cp.ID,
			&cp.PipelineID,
			&cp.ConnectionID,
			&cp.SourceTable,
			&cdcResourceID,
			&positionJSON,
			&cp.CreatedAt,
			&cp.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan checkpoint: %w", err)
		}

		if cdcResourceID.Valid {
			cp.CDCResourceID = &cdcResourceID.String
		}

		if len(positionJSON) > 0 {
			if err := json.Unmarshal(positionJSON, &cp.Position); err != nil {
				log.WithError(err).Warn("Failed to unmarshal checkpoint position")
			}
		}

		checkpoints = append(checkpoints, cp)
	}

	return checkpoints, nil
}

// HasCheckpoint checks if a checkpoint exists for a pipeline
func HasCheckpoint(ctx context.Context, db *sql.DB, pipelineID string) (bool, error) {
	var exists bool
	err := db.QueryRowContext(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM pipeline_checkpoints 
			WHERE pipeline_id = $1 
			LIMIT 1
		)
	`, pipelineID).Scan(&exists)

	return exists, err
}

// GetCheckpointForTable retrieves the checkpoint for a specific table
func GetCheckpointForTable(ctx context.Context, db *sql.DB, pipelineID, table string) (*Checkpoint, error) {
	query := `
		SELECT id, pipeline_id, connection_id, source_table, cdc_resource_id, position, created_at, updated_at
		FROM pipeline_checkpoints
		WHERE pipeline_id = $1 AND source_table = $2
	`

	var cp Checkpoint
	var positionJSON []byte
	var cdcResourceID sql.NullString

	err := db.QueryRowContext(ctx, query, pipelineID, table).Scan(
		&cp.ID,
		&cp.PipelineID,
		&cp.ConnectionID,
		&cp.SourceTable,
		&cdcResourceID,
		&positionJSON,
		&cp.CreatedAt,
		&cp.UpdatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, nil // No checkpoint found
	}

	if err != nil {
		return nil, fmt.Errorf("failed to query checkpoint: %w", err)
	}

	if cdcResourceID.Valid {
		cp.CDCResourceID = &cdcResourceID.String
	}

	if len(positionJSON) > 0 {
		if err := json.Unmarshal(positionJSON, &cp.Position); err != nil {
			log.WithError(err).Warn("Failed to unmarshal checkpoint position")
		}
	}

	return &cp, nil
}

// DeleteCheckpoints deletes all checkpoints for a pipeline
func DeleteCheckpoints(ctx context.Context, db *sql.DB, pipelineID string) error {
	result, err := db.ExecContext(ctx, "DELETE FROM pipeline_checkpoints WHERE pipeline_id = $1", pipelineID)
	if err != nil {
		return fmt.Errorf("failed to delete checkpoints: %w", err)
	}

	rows, _ := result.RowsAffected()
	log.WithFields(log.Fields{
		"pipeline_id":   pipelineID,
		"rows_affected": rows,
	}).Info("Deleted checkpoints")

	return nil
}
