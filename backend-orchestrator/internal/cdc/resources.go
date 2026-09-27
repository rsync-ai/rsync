package cdc

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
)

// CDCResourceConfig contains configuration for CDC resource provisioning
type CDCResourceConfig struct {
	PipelineID   string
	ConnectionID string
	DatabaseType string
	Database     string
	Table        string // Optional, for table-level resources
}

// CDCResource represents a tracked CDC resource
type CDCResource struct {
	ID             string                 `json:"id"`
	PipelineID     *string                `json:"pipeline_id,omitempty"`
	ConnectionID   string                 `json:"connection_id"`
	SourceTable    *string                `json:"source_table,omitempty"`
	ResourceType   string                 `json:"resource_type"`
	ResourceName   string                 `json:"resource_name"`
	Status         string                 `json:"status"`
	DatabaseType   string                 `json:"database_type"`
	Metadata       map[string]interface{} `json:"metadata,omitempty"`
	CreatedAt      time.Time              `json:"created_at"`
	DeletedAt      *time.Time             `json:"deleted_at,omitempty"`
	LastVerifiedAt *time.Time             `json:"last_verified_at,omitempty"`
}

// pipelineShortID is the pipeline-identifying part of every CDC resource name.
func pipelineShortID(pipelineID string) string {
	if len(pipelineID) >= 8 {
		return pipelineID[:8]
	}
	return pipelineID
}

// PipelineResourcePrefix returns the leading part of a replication slot or
// publication name that GenerateResourceName builds for this pipeline — the
// part that does not depend on connection/database/table. Code that must
// recognise a pipeline's own slot or publication on the source (the
// pre-migration assessor) matches on this instead of re-deriving the format.
// Returns "" for other resource types.
func PipelineResourcePrefix(pipelineID, resourceType string) string {
	switch resourceType {
	case "replication_slot":
		return fmt.Sprintf("debezium_slot_pipe_%s_", pipelineShortID(pipelineID))
	case "publication":
		return fmt.Sprintf("debezium_pub_pipe_%s_", pipelineShortID(pipelineID))
	}
	return ""
}

// GenerateResourceName creates deterministic, unique resource names
func GenerateResourceName(config CDCResourceConfig, resourceType string) string {
	pipelineShort := pipelineShortID(config.PipelineID)

	// Create deterministic hash for additional uniqueness
	hashInput := fmt.Sprintf(
		"%s:%s:%s:%s",
		config.PipelineID,
		config.ConnectionID,
		config.Database,
		config.Table,
	)
	hash := sha256.Sum256([]byte(hashInput))
	hashShort := fmt.Sprintf("%x", hash)[:8]

	switch resourceType {
	case "replication_slot":
		// PostgreSQL slot names: max 63 chars, lowercase, underscores
		return PipelineResourcePrefix(config.PipelineID, resourceType) + hashShort

	case "publication":
		// PostgreSQL publication names: per-pipeline to avoid shared-resource refcount complexity.
		// Max identifier length is 63 chars.
		return PipelineResourcePrefix(config.PipelineID, resourceType) + hashShort

	case "server_id":
		// MySQL server ID: numeric, use hash as number
		// Range: 1 to 2^32-1, reserve 1000000-2000000 for CDC
		hashInt := int(hash[0])<<24 | int(hash[1])<<16 | int(hash[2])<<8 | int(hash[3])
		serverID := 1000000 + (hashInt % 1000000)
		return fmt.Sprintf("%d", serverID)

	case "capture_instance":
		// SQL Server capture instance names
		return fmt.Sprintf("cdc_%s_%s", pipelineShort, hashShort)

	case "supplemental_log_group":
		// Oracle per-table supplemental logging marker (ledger-only; the physical
		// attribute is a shared table property, so the name just identifies the
		// pipeline+table pairing in cdc_resources).
		return fmt.Sprintf("suplog_%s_%s", pipelineShort, hashShort)

	default:
		return fmt.Sprintf("debezium_%s_%s", pipelineShort, hashShort)
	}
}

// RecordResource stores a CDC resource in the database
func RecordResource(ctx context.Context, db *sql.DB, resource CDCResource) error {
	metadataJSON, err := json.Marshal(resource.Metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	query := `
		INSERT INTO cdc_resources (
			pipeline_id, connection_id, source_table,
			resource_type, resource_name, status,
			database_type, metadata, last_verified_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
		ON CONFLICT (resource_type, resource_name, connection_id) 
		DO UPDATE SET
			status = EXCLUDED.status,
			metadata = EXCLUDED.metadata,
			last_verified_at = NOW()
	`

	_, err = db.ExecContext(
		ctx,
		query,
		resource.PipelineID,
		resource.ConnectionID,
		resource.SourceTable,
		resource.ResourceType,
		resource.ResourceName,
		resource.Status,
		resource.DatabaseType,
		string(metadataJSON),
	)

	if err != nil {
		return fmt.Errorf("failed to record resource: %w", err)
	}

	log.WithFields(log.Fields{
		"resource_type": resource.ResourceType,
		"resource_name": resource.ResourceName,
		"pipeline_id":   resource.PipelineID,
		"status":        resource.Status,
	}).Info("Recorded CDC resource")

	return nil
}

// MarkResourceDeleted marks a resource as deleted
func MarkResourceDeleted(ctx context.Context, db *sql.DB, resourceName string, resourceType string) error {
	query := `
		UPDATE cdc_resources
		SET status = 'deleted', deleted_at = NOW()
		WHERE resource_name = $1 AND resource_type = $2
	`

	result, err := db.ExecContext(ctx, query, resourceName, resourceType)
	if err != nil {
		return fmt.Errorf("failed to mark resource as deleted: %w", err)
	}

	rows, _ := result.RowsAffected()
	log.WithFields(log.Fields{
		"resource_name": resourceName,
		"resource_type": resourceType,
		"rows_affected": rows,
	}).Info("Marked CDC resource as deleted")

	return nil
}

// MarkResourceFailed marks a resource as failed (physical drop did not succeed)
// so it is NOT lost: GetCDCResources still returns 'failed' rows, and the CDC
// reconciler retries the drop on its next sweep. This replaces the old behavior
// of marking 'deleted' unconditionally, which permanently hid a slot whose drop
// had failed → a guaranteed leak.
func MarkResourceFailed(ctx context.Context, db *sql.DB, resourceName string, resourceType string) error {
	query := `
		UPDATE cdc_resources
		SET status = 'failed'
		WHERE resource_name = $1 AND resource_type = $2 AND status <> 'deleted'
	`
	if _, err := db.ExecContext(ctx, query, resourceName, resourceType); err != nil {
		return fmt.Errorf("failed to mark resource as failed: %w", err)
	}
	return nil
}

// reapableResourceQuery selects PostgreSQL cdc_resources rows of one type whose
// owning pipeline is gone (pipeline_id NULL via ON DELETE SET NULL, or the row
// deleted) and, with includeStopped, also those of a 'stopped' pipeline.
// 'inactive'/'failed' statuses are included so prior failed drops retry.
func reapableResourceQuery(resourceType string, includeStopped bool) string {
	owner := `cr.pipeline_id IS NULL OR p.id IS NULL`
	if includeStopped {
		owner += ` OR p.status = 'stopped'`
	}
	return `
		SELECT cr.id, cr.pipeline_id, cr.connection_id, cr.source_table,
		       cr.resource_type, cr.resource_name, cr.status,
		       cr.database_type, cr.metadata, cr.created_at, cr.deleted_at, cr.last_verified_at
		FROM cdc_resources cr
		LEFT JOIN pipelines p ON p.id = cr.pipeline_id
		WHERE cr.resource_type = '` + resourceType + `'
		  AND cr.database_type = 'postgresql'
		  AND cr.status IN ('active', 'inactive', 'failed', 'orphaned')
		  AND (` + owner + `)
	`
}

func queryReapableResources(ctx context.Context, db *sql.DB, resourceType string, includeStopped bool) ([]CDCResource, error) {
	rows, err := db.QueryContext(ctx, reapableResourceQuery(resourceType, includeStopped))
	if err != nil {
		return nil, fmt.Errorf("failed to query reapable %s resources: %w", resourceType, err)
	}
	defer rows.Close()
	return scanCDCResources(rows)
}

// GetReapableSlots returns replication_slot resources whose owning pipeline was
// deleted. A 'stopped' pipeline's slot is NOT returned: Stop keeps the slot as
// the resume position, so Start continues where the stream stopped instead of
// losing every change made while it was stopped. The one exception is WAL
// pressure — GetWALPressureReapableSlots — because a stopped slot still retains
// WAL on the source. Rows for 'running' and 'paused' pipelines are never returned.
func GetReapableSlots(ctx context.Context, db *sql.DB) ([]CDCResource, error) {
	return queryReapableResources(ctx, db, "replication_slot", false)
}

// GetWALPressureReapableSlots is GetReapableSlots plus the slots of 'stopped'
// pipelines. Only the WAL watchdog uses it, and only for slots it has measured
// past the CRITICAL retained-WAL threshold: losing a stopped pipeline's resume
// position is recoverable (Reload), a source whose disk fills is not.
func GetWALPressureReapableSlots(ctx context.Context, db *sql.DB) ([]CDCResource, error) {
	return queryReapableResources(ctx, db, "replication_slot", true)
}

// GetReapablePublications is the publication analogue of GetReapableSlots (BUG-3):
// every PostgreSQL publication row whose owning pipeline is gone. Publications
// are per-pipeline (debezium_pub_pipe_*), so each such row is safe to DROP.
// A 'stopped' pipeline keeps its publication, like its slot, so Start can resume.
// Without this, a publication whose synchronous delete-time cleanup did not run
// leaked forever — slots had a reaper, publications did not.
func GetReapablePublications(ctx context.Context, db *sql.DB) ([]CDCResource, error) {
	return queryReapableResources(ctx, db, "publication", false)
}

// GetReapableCaptureInstances is the SQL Server analogue of GetReapableSlots:
// it reaps only when the owning pipeline is GONE (pipeline_id NULL via ON
// DELETE SET NULL, or the pipeline row deleted) — never merely 'stopped'. A
// capture instance is where the change data LIVES: disabling one on a
// stopped-but-resumable pipeline discards every change row accumulated since
// the stop and forces a full re-snapshot on resume. Unlike a PostgreSQL slot
// it has no WAL-pressure exception, since it does not grow the source's log.
//
// SQL Server is the only non-PostgreSQL family that needs a reaper at all:
// Oracle (supplemental log groups) and MySQL (server_id) cleanup is ledger-only
// by design — see oracle.go / mysql.go CleanupResources — so there is nothing
// physical that can be left behind.
func GetReapableCaptureInstances(ctx context.Context, db *sql.DB) ([]CDCResource, error) {
	query := `
		SELECT cr.id, cr.pipeline_id, cr.connection_id, cr.source_table,
		       cr.resource_type, cr.resource_name, cr.status,
		       cr.database_type, cr.metadata, cr.created_at, cr.deleted_at, cr.last_verified_at
		FROM cdc_resources cr
		LEFT JOIN pipelines p ON p.id = cr.pipeline_id
		WHERE cr.resource_type = 'capture_instance'
		  AND cr.database_type = 'sqlserver'
		  AND cr.status IN ('active', 'inactive', 'failed', 'orphaned')
		  AND (cr.pipeline_id IS NULL OR p.id IS NULL)
	`
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query reapable capture instances: %w", err)
	}
	defer rows.Close()
	return scanCDCResources(rows)
}

// GetCDCResources retrieves all active CDC resources for a pipeline
func GetCDCResources(ctx context.Context, db *sql.DB, pipelineID string) ([]CDCResource, error) {
	query := `
		SELECT id, pipeline_id, connection_id, source_table,
		       resource_type, resource_name, status,
		       database_type, metadata, created_at, deleted_at, last_verified_at
		FROM cdc_resources
		WHERE pipeline_id = $1 AND status IN ('active', 'inactive')
		ORDER BY created_at DESC
	`

	rows, err := db.QueryContext(ctx, query, pipelineID)
	if err != nil {
		return nil, fmt.Errorf("failed to query CDC resources: %w", err)
	}
	defer rows.Close()

	return scanCDCResources(rows)
}

// scanCDCResources scans a *sql.Rows whose columns are, in order:
// id, pipeline_id, connection_id, source_table, resource_type, resource_name,
// status, database_type, metadata, created_at, deleted_at, last_verified_at.
func scanCDCResources(rows *sql.Rows) ([]CDCResource, error) {
	var resources []CDCResource
	for rows.Next() {
		var r CDCResource
		var metadataJSON []byte
		var pipelineID, sourceTable sql.NullString
		var deletedAt, lastVerifiedAt sql.NullTime

		err := rows.Scan(
			&r.ID, &pipelineID, &r.ConnectionID, &sourceTable,
			&r.ResourceType, &r.ResourceName, &r.Status,
			&r.DatabaseType, &metadataJSON, &r.CreatedAt, &deletedAt, &lastVerifiedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan resource: %w", err)
		}

		if pipelineID.Valid {
			r.PipelineID = &pipelineID.String
		}
		if sourceTable.Valid {
			r.SourceTable = &sourceTable.String
		}
		if deletedAt.Valid {
			r.DeletedAt = &deletedAt.Time
		}
		if lastVerifiedAt.Valid {
			r.LastVerifiedAt = &lastVerifiedAt.Time
		}

		if len(metadataJSON) > 0 {
			if err := json.Unmarshal(metadataJSON, &r.Metadata); err != nil {
				log.WithError(err).Warn("Failed to unmarshal resource metadata")
			}
		}

		resources = append(resources, r)
	}

	return resources, nil
}
