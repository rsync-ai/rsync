package cdc

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
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

// ErrCheckpointPositionUnreadable reports a checkpoint row whose position JSON
// could not be decoded.
//
// This used to be a Warn log and a checkpoint returned with a nil Position, which
// the resume path reads identically to "this table has no checkpoint": it restarts
// the sweep at batch_idx 0, offset 0, key_ordinal 0 and with no since_cursor. That
// re-reads the whole table AND reuses part-000000, overwriting the objects the
// previous sweep wrote - the silent overwrite the key_ordinal comment in
// executeBatchDataTransfer exists to prevent. A caller has to be able to tell
// "no resume state" from "unreadable resume state", so it is a sentinel:
// errors.Is(err, cdc.ErrCheckpointPositionUnreadable).
var ErrCheckpointPositionUnreadable = errors.New("checkpoint position is unreadable")

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
				return nil, fmt.Errorf("%w: pipeline %s table %s: %v",
					ErrCheckpointPositionUnreadable, pipelineID, cp.SourceTable, err)
			}
		}

		checkpoints = append(checkpoints, cp)
	}

	return checkpoints, nil
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
			return nil, fmt.Errorf("%w: pipeline %s table %s: %v",
				ErrCheckpointPositionUnreadable, pipelineID, table, err)
		}
	}

	return &cp, nil
}

// DeleteCheckpointsNotFromExecution deletes a pipeline's checkpoints except the
// ones executionID wrote (position.execution_id). A batch reload calls it on
// every dispatch: the first deletes every earlier run's checkpoint, and a chunk
// continuation of the same reload keeps its own progress. An empty executionID
// deletes them all.
func DeleteCheckpointsNotFromExecution(ctx context.Context, db *sql.DB, pipelineID, executionID string) error {
	if strings.TrimSpace(executionID) == "" {
		return DeleteCheckpoints(ctx, db, pipelineID)
	}
	result, err := db.ExecContext(ctx,
		`DELETE FROM pipeline_checkpoints WHERE pipeline_id = $1 AND COALESCE(position->>'execution_id', '') <> $2`,
		pipelineID, executionID)
	if err != nil {
		return fmt.Errorf("failed to delete checkpoints: %w", err)
	}
	rows, _ := result.RowsAffected()
	log.WithFields(log.Fields{
		"pipeline_id":   pipelineID,
		"execution_id":  executionID,
		"rows_affected": rows,
	}).Info("Deleted checkpoints of earlier runs")
	return nil
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

// RunStartKey is the checkpoint position field holding the position the table had
// before the run named by position.execution_id began. An empty object means the
// table had no checkpoint then. Checkpoints written before this field existed lack
// it, and RewindCheckpointsOfExecution leaves those alone.
const RunStartKey = "run_start"

// RunStartPosition returns the run_start a checkpoint saved by executionID should
// carry, given the checkpoint the table had when this dispatch began.
//
// A batch checkpoint is saved when a batch is PRODUCED, not when the destination
// acks it, so a batch the sink later dead-letters is already behind the checkpoint:
// a Resume starts after it and moves 0 rows while the gap stays (B-RESUME-DLQ).
// Recording where the run started is what lets a run that lost batches put the
// table back there (RewindCheckpointsOfExecution).
//
//   - no checkpoint                     -> {} (rewinding removes the checkpoint)
//   - one from an earlier run           -> that position, minus its own run_start
//   - one from this run (a chunk resume) -> the run_start it already carries, or nil
//     when it has none (written before this field existed; unknown, so omitted)
func RunStartPosition(existing *Checkpoint, executionID string) map[string]interface{} {
	if existing == nil || existing.Position == nil {
		return map[string]interface{}{}
	}
	if id, _ := existing.Position["execution_id"].(string); id != "" && id == executionID {
		rs, _ := existing.Position[RunStartKey].(map[string]interface{})
		return rs
	}
	out := make(map[string]interface{}, len(existing.Position))
	for k, v := range existing.Position {
		if k != RunStartKey {
			out[k] = v
		}
	}
	return out
}

// RewindCheckpointsOfExecution puts every checkpoint executionID wrote back to the
// position its table had before that run: restored from run_start, or deleted when
// the table had none. Call it once the run is known to have lost batches, so the
// next Resume re-reads them. The re-read uses the same key_ordinal sequence, so an
// object-storage destination overwrites the objects the run already wrote instead
// of adding copies; a keyed destination upserts. Returns the checkpoints rewound.
func RewindCheckpointsOfExecution(ctx context.Context, db *sql.DB, pipelineID, executionID string) (int64, error) {
	if db == nil || strings.TrimSpace(pipelineID) == "" || strings.TrimSpace(executionID) == "" {
		return 0, nil
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to begin checkpoint rewind: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op after Commit
	restored, err := tx.ExecContext(ctx, `
		UPDATE pipeline_checkpoints SET position = position->'run_start', updated_at = NOW()
		WHERE pipeline_id = $1 AND position->>'execution_id' = $2
		  AND jsonb_typeof(position->'run_start') = 'object' AND position->'run_start' <> '{}'::jsonb`,
		pipelineID, executionID)
	if err != nil {
		return 0, fmt.Errorf("failed to restore checkpoints: %w", err)
	}
	deleted, err := tx.ExecContext(ctx, `
		DELETE FROM pipeline_checkpoints
		WHERE pipeline_id = $1 AND position->>'execution_id' = $2 AND position->'run_start' = '{}'::jsonb`,
		pipelineID, executionID)
	if err != nil {
		return 0, fmt.Errorf("failed to delete checkpoints: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("failed to commit checkpoint rewind: %w", err)
	}
	r, _ := restored.RowsAffected()
	d, _ := deleted.RowsAffected()
	log.WithFields(log.Fields{
		"pipeline_id":  pipelineID,
		"execution_id": executionID,
		"restored":     r,
		"deleted":      d,
	}).Info("Rewound checkpoints of a run that lost batches")
	return r + d, nil
}
