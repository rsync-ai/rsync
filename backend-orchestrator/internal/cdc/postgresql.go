package cdc

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	_ "github.com/rsync-ai/shared/pgdriver"
	log "github.com/sirupsen/logrus"

	"github.com/rsync-ai/backend-orchestrator/internal/connections"
)

// PostgreSQLManager handles CDC resource provisioning for PostgreSQL
type PostgreSQLManager struct {
	db *sql.DB
}

// NewPostgreSQLManager creates a new PostgreSQL CDC manager
func NewPostgreSQLManager(db *sql.DB) *PostgreSQLManager {
	return &PostgreSQLManager{db: db}
}

// init registers PostgreSQL (and its "postgres" alias, via NormalizeDBType) as a
// CDC source provider so the handler dispatches through the shared registry.
func init() {
	RegisterProvider(func(db *sql.DB) CDCSourceProvider {
		return NewPostgreSQLManager(db)
	}, "postgresql")
}

// Family returns the canonical source-family key for PostgreSQL.
func (m *PostgreSQLManager) Family() string { return "postgresql" }

// PrimaryKeyNamespace uses the schema to qualify unqualified table names for PK
// validation (PostgreSQL groups tables by schema, not database).
func (m *PostgreSQLManager) PrimaryKeyNamespace(defaultDB, defaultSchema string) string {
	return defaultSchema
}

// ProvisionResources provisions publication and replication slot for a CDC pipeline.
//
// ORDERING INVARIANT — publication MUST be created before the replication slot:
//  1. CREATE PUBLICATION  (step 1 below)
//  2. SET REPLICA IDENTITY FULL on selected tables (step 1.5)
//  3. CREATE REPLICATION SLOT with pgoutput  (step 2 below)
//
// Rationale: pgoutput activates the publication membership filter when a
// replication slot is first used. If the slot is created before the
// publication, the first WAL batch is decoded without table filtering,
// producing silent data loss or schema errors. The caller (executor.go)
// passes publication.autocreate.mode=disabled to Debezium so it never
// tries to create resources itself (which would reverse the order).
func (m *PostgreSQLManager) ProvisionResources(ctx context.Context, config CDCResourceConfig, tables []string) ([]CDCResource, error) {
	log.WithFields(log.Fields{
		"pipeline_id":   config.PipelineID,
		"connection_id": config.ConnectionID,
		"database":      config.Database,
		"table_count":   len(tables),
	}).Info("Provisioning PostgreSQL CDC resources")

	resources := []CDCResource{}

	// Get connection details for direct DB access
	decryptedConfig, err := m.getDecryptedConnectionConfig(ctx, config.ConnectionID)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection config: %w", err)
	}

	targetDB, err := connectToPostgreSQL(decryptedConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to target PostgreSQL: %w", err)
	}
	defer targetDB.Close()

	// 1. Create publication (per-pipeline for MVP to avoid refcount complexity)
	publicationName := GenerateResourceName(config, "publication")

	// Check if publication already exists
	var exists bool
	err = targetDB.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname = $1)", publicationName).Scan(&exists)
	if err != nil {
		return nil, fmt.Errorf("failed to check publication existence: %w", err)
	}

	if !exists {
		// Create publication FOR ALL TABLES (simplifies table selection)
		// Note: Debezium will filter tables via connector config (table.include.list).
		createPubQuery := fmt.Sprintf("CREATE PUBLICATION %s FOR ALL TABLES", publicationName)
		if _, err := targetDB.ExecContext(ctx, createPubQuery); err != nil {
			return nil, fmt.Errorf("failed to create publication: %w", err)
		}

		log.WithField("publication", publicationName).Info("Created PostgreSQL publication")
	}

	publicationResource := CDCResource{
		PipelineID:   &config.PipelineID,
		ConnectionID: config.ConnectionID,
		ResourceType: "publication",
		ResourceName: publicationName,
		Status:       "active",
		DatabaseType: "postgresql",
		Metadata: map[string]interface{}{
			"database": config.Database,
			"tables":   tables,
		},
		CreatedAt: time.Now(),
	}

	// Record publication in cdc_resources table
	if err := RecordResource(ctx, m.db, publicationResource); err != nil {
		return nil, fmt.Errorf("failed to record publication: %w", err)
	}

	resources = append(resources, publicationResource)

	// 1.5. ENFORCE REPLICA IDENTITY FULL on every selected table
	// (T2-1 — closes the TOAST data-corruption gap).
	//
	// Default REPLICA IDENTITY (= PK only) tells pgoutput to omit
	// unchanged TOAST columns from UPDATE row images. Debezium then
	// substitutes the literal string `__debezium_unavailable_value`
	// into the `after` payload for those columns. The kafka-mcp-sink
	// has no filter for that sentinel and writes it verbatim,
	// OVERWRITING the destination's good TOAST value with the 25-
	// byte sentinel string.
	//
	// REPLICA IDENTITY FULL forces pgoutput to include the full row
	// in every UPDATE — no sentinels possible. The tradeoff is more
	// WAL volume (proportional to row size on updates); for the
	// pilot workload this is the safer default. Future PR can let
	// the user opt out per-table when they're certain no TOAST
	// columns are present, paired with the sink-side sentinel filter
	// below as a backstop.
	for _, t := range tables {
		// Tables are typically `schema.name` or bare `name` from the
		// user's table picker. The ALTER must run against the same
		// identifier path the user selected.
		// Quote each part to prevent SQL injection from attacker-controlled table names.
		quotedTable := quotePgIdentifier(t)
		alterQuery := fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", quotedTable)
		if _, err := targetDB.ExecContext(ctx, alterQuery); err != nil {
			// Don't fail provisioning on a single ALTER error — log
			// loudly and continue. The sink-side filter is the
			// belt-and-suspenders backstop for any table the user
			// can't ALTER (e.g. extension-owned tables, RLS-locked
			// shares).
			log.WithError(err).WithFields(log.Fields{
				"table":          t,
				"pipeline_id":    config.PipelineID,
				"recommendation": "Run `ALTER TABLE " + t + " REPLICA IDENTITY FULL` manually, or trust the sink-side sentinel filter",
			}).Warn("⚠️  Could not set REPLICA IDENTITY FULL — TOAST columns on this table may rely on the sink-side sentinel filter for correctness")
			continue
		}
		log.WithFields(log.Fields{
			"table":       t,
			"pipeline_id": config.PipelineID,
		}).Info("✅ Set REPLICA IDENTITY FULL for CDC TOAST safety")
	}

	// 2. Create replication slot
	slotName := GenerateResourceName(config, "replication_slot")

	// Check if slot already exists
	err = targetDB.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)", slotName).Scan(&exists)
	if err != nil {
		return nil, fmt.Errorf("failed to check replication slot existence: %w", err)
	}

	// consistentPoint is the WAL LSN at which the slot starts retaining changes —
	// the position "P" for the hybrid batch+CDC handoff. For a freshly created slot
	// it is the consistent_point returned by pg_create_logical_replication_slot();
	// for a pre-existing slot we read confirmed_flush_lsn (falling back to restart_lsn).
	// Debezium with snapshot.mode=no_data against this slot resumes streaming from here,
	// so a batch historical load taken at/after P plus CDC from P converge with no gap.
	consistentPoint := ""
	if !exists {
		// Create logical replication slot with pgoutput plugin.
		// pg_create_logical_replication_slot returns (slot_name name, lsn pg_lsn);
		// the lsn is the consistent_point — capture it for the hybrid handoff.
		createSlotQuery := fmt.Sprintf("SELECT lsn::text FROM pg_create_logical_replication_slot('%s', 'pgoutput')", slotName)
		if err := targetDB.QueryRowContext(ctx, createSlotQuery).Scan(&consistentPoint); err != nil {
			return nil, fmt.Errorf("failed to create replication slot: %w", err)
		}

		log.WithFields(log.Fields{
			"slot_name":        slotName,
			"consistent_point": consistentPoint,
		}).Info("Created PostgreSQL replication slot")
	} else {
		// Slot already exists (resume / rerun) — read its current confirmed position
		// so the hybrid handoff anchors Debezium to where the slot actually stands.
		var confirmed, restart sql.NullString
		err = targetDB.QueryRowContext(ctx,
			"SELECT confirmed_flush_lsn::text, restart_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
			slotName,
		).Scan(&confirmed, &restart)
		if err != nil {
			log.WithError(err).WithField("slot_name", slotName).Warn("⚠️  Could not read existing slot LSN; hybrid handoff will rely on the slot's stored offset")
		} else if confirmed.Valid && strings.TrimSpace(confirmed.String) != "" {
			consistentPoint = confirmed.String
		} else if restart.Valid {
			consistentPoint = restart.String
		}
		log.WithFields(log.Fields{
			"slot_name":        slotName,
			"consistent_point": consistentPoint,
		}).Info("Reusing existing PostgreSQL replication slot")
	}

	slotMetadata := map[string]interface{}{
		"database":    config.Database,
		"plugin":      "pgoutput",
		"publication": publicationName,
	}
	if strings.TrimSpace(consistentPoint) != "" {
		slotMetadata["consistent_point"] = consistentPoint
	}

	slotResource := CDCResource{
		PipelineID:   &config.PipelineID,
		ConnectionID: config.ConnectionID,
		ResourceType: "replication_slot",
		ResourceName: slotName,
		Status:       "active",
		DatabaseType: "postgresql",
		Metadata:     slotMetadata,
		CreatedAt:    time.Now(),
	}

	// Record slot in cdc_resources table
	if err := RecordResource(ctx, m.db, slotResource); err != nil {
		// Attempt to clean up the slot if we can't record it
		targetDB.ExecContext(ctx, fmt.Sprintf("SELECT pg_drop_replication_slot('%s')", slotName))
		return nil, fmt.Errorf("failed to record replication slot: %w", err)
	}

	resources = append(resources, slotResource)

	log.WithFields(log.Fields{
		"pipeline_id": config.PipelineID,
		"publication": publicationName,
		"slot":        slotName,
	}).Info("Successfully provisioned PostgreSQL CDC resources")

	return resources, nil
}

// CleanupResources removes CDC resources for a pipeline with retry logic
func (m *PostgreSQLManager) CleanupResources(ctx context.Context, pipelineID string) error {
	log.WithField("pipeline_id", pipelineID).Info("Cleaning up PostgreSQL CDC resources")

	// Get all resources for this pipeline
	resources, err := GetCDCResources(ctx, m.db, pipelineID)
	if err != nil {
		return fmt.Errorf("failed to get CDC resources: %w", err)
	}

	var cleanupErrors []error

	for _, resource := range resources {
		if resource.DatabaseType != "postgresql" {
			continue
		}

		// Get connection config
		decryptedConfig, err := m.getDecryptedConnectionConfig(ctx, resource.ConnectionID)
		if err != nil {
			log.WithError(err).WithField("resource", resource.ResourceName).Warn("Failed to get connection config for cleanup")
			cleanupErrors = append(cleanupErrors, err)
			continue
		}

		targetDB, err := connectToPostgreSQL(decryptedConfig)
		if err != nil {
			log.WithError(err).WithField("resource", resource.ResourceName).Warn("Failed to connect for cleanup")
			cleanupErrors = append(cleanupErrors, err)
			continue
		}

		dropped := false
		switch resource.ResourceType {
		case "replication_slot":
			// Drop replication slot with retry and active-slot handling
			err := m.dropReplicationSlotWithRetry(ctx, targetDB, resource.ResourceName, 3)
			if err != nil {
				log.WithError(err).WithField("slot", resource.ResourceName).Warn("Failed to drop replication slot")
				cleanupErrors = append(cleanupErrors, err)
			} else {
				dropped = true
				log.WithField("slot", resource.ResourceName).Info("Dropped replication slot")
			}

		case "publication":
			// Drop publication with retry
			err := m.dropPublicationWithRetry(ctx, targetDB, resource.ResourceName, 3)
			if err != nil {
				log.WithError(err).WithField("publication", resource.ResourceName).Warn("Failed to drop publication")
				cleanupErrors = append(cleanupErrors, err)
			} else {
				dropped = true
				log.WithField("publication", resource.ResourceName).Info("Dropped publication")
			}
		default:
			dropped = true // non-DB resource types have nothing to drop here
		}

		targetDB.Close()

		// Only mark 'deleted' when the physical resource is actually gone. On
		// failure mark 'failed' so GetCDCResources/GetReapableSlots still return
		// it and the reconciler retries — marking 'deleted' unconditionally
		// permanently hid a slot whose drop failed → guaranteed WAL/slot leak.
		if dropped {
			if err := MarkResourceDeleted(ctx, m.db, resource.ResourceName, resource.ResourceType); err != nil {
				log.WithError(err).WithField("resource", resource.ResourceName).Warn("Failed to mark resource as deleted")
			}
		} else {
			if err := MarkResourceFailed(ctx, m.db, resource.ResourceName, resource.ResourceType); err != nil {
				log.WithError(err).WithField("resource", resource.ResourceName).Warn("Failed to mark resource as failed")
			}
		}
	}

	// Return combined error if any cleanup failed
	if len(cleanupErrors) > 0 {
		return fmt.Errorf("some resources failed to clean up: %d errors", len(cleanupErrors))
	}

	return nil
}

// ReapOrphanedSlots is the safety-net for the slot lifecycle: it drops the
// physical replication slot on the source server for every cdc_resources slot
// row that is no longer live — pipeline deleted (pipeline_id NULL via ON DELETE
// SET NULL) or pipeline 'stopped'. It is idempotent and is meant to be called
// periodically by the CDC reconciler. Returns the number of slots dropped.
//
// This is what guarantees a slot is never permanently leaked even if the
// synchronous pre-delete cleanup did not run (orchestrator down, network flake,
// a delete path that bypassed the handler) — and it auto-reaps pre-existing
// debris from before the lifecycle fix.
func (m *PostgreSQLManager) ReapOrphanedSlots(ctx context.Context) (int, error) {
	slots, err := GetReapableSlots(ctx, m.db)
	if err != nil {
		return 0, err
	}
	dropped := 0
	for _, resource := range slots {
		cfg, err := m.getDecryptedConnectionConfig(ctx, resource.ConnectionID)
		if err != nil {
			log.WithError(err).WithField("slot", resource.ResourceName).Warn("reaper: failed to get connection config")
			continue
		}
		targetDB, err := connectToPostgreSQL(cfg)
		if err != nil {
			log.WithError(err).WithField("slot", resource.ResourceName).Warn("reaper: failed to connect to source for slot drop")
			continue
		}
		err = m.dropReplicationSlotWithRetry(ctx, targetDB, resource.ResourceName, 3)
		targetDB.Close()
		if err != nil {
			log.WithError(err).WithField("slot", resource.ResourceName).Warn("reaper: failed to drop orphaned replication slot")
			_ = MarkResourceFailed(ctx, m.db, resource.ResourceName, resource.ResourceType)
			continue
		}
		dropped++
		log.WithFields(log.Fields{"slot": resource.ResourceName, "pipeline_id": derefStr(resource.PipelineID)}).
			Info("reaper: dropped orphaned/stopped replication slot")
		if err := MarkResourceDeleted(ctx, m.db, resource.ResourceName, resource.ResourceType); err != nil {
			log.WithError(err).WithField("slot", resource.ResourceName).Warn("reaper: failed to mark slot deleted")
		}
	}
	return dropped, nil
}

// ReapOrphanedPublications is the publication safety-net (BUG-3), mirroring
// ReapOrphanedSlots: it DROPs the physical publication on the source server for
// every publication cdc_resources row whose pipeline is gone or 'stopped'.
// Idempotent (DROP PUBLICATION IF EXISTS) and meant to be called periodically by
// the CDC reconciler. Publications are per-pipeline (debezium_pub_pipe_*), so
// dropping one never affects another pipeline. Returns the number dropped.
//
// This closes the leak where a publication whose synchronous pre-delete cleanup
// did not run (orchestrator down, a delete path that bypassed the handler, or a
// swallowed drop error) previously survived forever — slots were reaped, but
// publications had no equivalent net.
func (m *PostgreSQLManager) ReapOrphanedPublications(ctx context.Context) (int, error) {
	pubs, err := GetReapablePublications(ctx, m.db)
	if err != nil {
		return 0, err
	}
	dropped := 0
	for _, resource := range pubs {
		cfg, err := m.getDecryptedConnectionConfig(ctx, resource.ConnectionID)
		if err != nil {
			log.WithError(err).WithField("publication", resource.ResourceName).Warn("reaper: failed to get connection config")
			continue
		}
		targetDB, err := connectToPostgreSQL(cfg)
		if err != nil {
			log.WithError(err).WithField("publication", resource.ResourceName).Warn("reaper: failed to connect to source for publication drop")
			continue
		}
		err = m.dropPublicationWithRetry(ctx, targetDB, resource.ResourceName, 3)
		targetDB.Close()
		if err != nil {
			log.WithError(err).WithField("publication", resource.ResourceName).Warn("reaper: failed to drop orphaned publication")
			_ = MarkResourceFailed(ctx, m.db, resource.ResourceName, resource.ResourceType)
			continue
		}
		dropped++
		log.WithFields(log.Fields{"publication": resource.ResourceName, "pipeline_id": derefStr(resource.PipelineID)}).
			Info("reaper: dropped orphaned/stopped publication")
		if err := MarkResourceDeleted(ctx, m.db, resource.ResourceName, resource.ResourceType); err != nil {
			log.WithError(err).WithField("publication", resource.ResourceName).Warn("reaper: failed to mark publication deleted")
		}
	}
	return dropped, nil
}

func derefStr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// dropReplicationSlotWithRetry drops a replication slot with retry and active-slot handling
func (m *PostgreSQLManager) dropReplicationSlotWithRetry(ctx context.Context, db *sql.DB, slotName string, maxRetries int) error {
	var lastErr error

	for attempt := 0; attempt <= maxRetries; attempt++ {
		// First check if slot exists
		var exists bool
		if err := db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)", slotName).Scan(&exists); err != nil {
			lastErr = err
			continue
		}

		if !exists {
			// Slot doesn't exist, nothing to do
			return nil
		}

		// Check if slot is active (has an active connection)
		var isActive bool
		if err := db.QueryRowContext(ctx, "SELECT active FROM pg_replication_slots WHERE slot_name = $1", slotName).Scan(&isActive); err != nil {
			lastErr = err
			continue
		}

		if isActive {
			log.WithField("slot", slotName).Warn("Replication slot is active, attempting to terminate blockers")

			// Try to terminate blocking connections (requires superuser)
			// This terminates the Debezium connector that's using the slot
			_, termErr := db.ExecContext(ctx, `
				SELECT pg_terminate_backend(active_pid) 
				FROM pg_replication_slots 
				WHERE slot_name = $1 AND active_pid IS NOT NULL
			`, slotName)
			if termErr != nil {
				log.WithError(termErr).Warn("Failed to terminate blocking connection")
			}

			// Wait a bit for the connection to close
			time.Sleep(2 * time.Second)
		}

		// Attempt to drop the slot
		_, err := db.ExecContext(ctx, fmt.Sprintf("SELECT pg_drop_replication_slot('%s')", slotName))
		if err != nil {
			lastErr = err
			log.WithError(err).WithFields(log.Fields{
				"slot":    slotName,
				"attempt": attempt + 1,
			}).Warn("Failed to drop replication slot, will retry")

			// Wait before retry with exponential backoff
			time.Sleep(time.Duration(1<<attempt) * time.Second)
			continue
		}

		// Success
		return nil
	}

	return fmt.Errorf("failed to drop replication slot after %d attempts: %w", maxRetries+1, lastErr)
}

// dropPublicationWithRetry drops a publication with retry
func (m *PostgreSQLManager) dropPublicationWithRetry(ctx context.Context, db *sql.DB, pubName string, maxRetries int) error {
	var lastErr error

	for attempt := 0; attempt <= maxRetries; attempt++ {
		// Use IF EXISTS to make it idempotent
		_, err := db.ExecContext(ctx, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", pubName))
		if err != nil {
			lastErr = err
			log.WithError(err).WithFields(log.Fields{
				"publication": pubName,
				"attempt":     attempt + 1,
			}).Warn("Failed to drop publication, will retry")

			// Wait before retry
			time.Sleep(time.Duration(1<<attempt) * time.Second)
			continue
		}

		// Success
		return nil
	}

	return fmt.Errorf("failed to drop publication after %d attempts: %w", maxRetries+1, lastErr)
}

// ValidatePrerequisites checks if PostgreSQL is properly configured for CDC
func (m *PostgreSQLManager) ValidatePrerequisites(ctx context.Context, connectionID string) ([]ValidationError, error) {
	errors := []ValidationError{}

	decryptedConfig, err := m.getDecryptedConnectionConfig(ctx, connectionID)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection config: %w", err)
	}

	targetDB, err := connectToPostgreSQL(decryptedConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}
	defer targetDB.Close()

	// Check WAL level
	var walLevel string
	err = targetDB.QueryRowContext(ctx, "SHOW wal_level").Scan(&walLevel)
	if err != nil {
		return nil, fmt.Errorf("failed to check wal_level: %w", err)
	}

	if walLevel != "logical" {
		errors = append(errors, ValidationError{
			Code:     "PG_WAL_LEVEL_INCORRECT",
			Severity: "error",
			Message:  fmt.Sprintf("PostgreSQL wal_level is '%s' but must be 'logical' for CDC", walLevel),
			Action:   "Set wal_level='logical' in postgresql.conf and restart PostgreSQL (on managed PostgreSQL such as Azure/RDS, set the rds.logical_replication / azure.replication_support parameter and restart)",
		})
	}

	// Check max_replication_slots — Debezium needs at least one free logical
	// replication slot. If this is 0, CREATE_REPLICATION_SLOT fails hard.
	var maxReplicationSlots int
	err = targetDB.QueryRowContext(ctx, "SHOW max_replication_slots").Scan(&maxReplicationSlots)
	if err == nil && maxReplicationSlots < 1 {
		errors = append(errors, ValidationError{
			Code:     "PG_MAX_REPLICATION_SLOTS",
			Severity: "error",
			Message:  fmt.Sprintf("PostgreSQL max_replication_slots is %d but must be >= 1 for CDC", maxReplicationSlots),
			Action:   "Set max_replication_slots >= 1 (recommended 10) in postgresql.conf and restart PostgreSQL",
		})
	}

	// Check max_wal_senders — each Debezium connector consumes a WAL sender.
	// If this is 0, the replication connection is refused.
	var maxWalSenders int
	err = targetDB.QueryRowContext(ctx, "SHOW max_wal_senders").Scan(&maxWalSenders)
	if err == nil && maxWalSenders < 1 {
		errors = append(errors, ValidationError{
			Code:     "PG_MAX_WAL_SENDERS",
			Severity: "error",
			Message:  fmt.Sprintf("PostgreSQL max_wal_senders is %d but must be >= 1 for CDC", maxWalSenders),
			Action:   "Set max_wal_senders >= 1 (recommended 10) in postgresql.conf and restart PostgreSQL",
		})
	}

	// Check replication permissions
	var hasReplication bool
	err = targetDB.QueryRowContext(ctx, "SELECT usesuper OR userepl FROM pg_user WHERE usename = CURRENT_USER").Scan(&hasReplication)
	if err != nil {
		return nil, fmt.Errorf("failed to check replication permissions: %w", err)
	}

	if !hasReplication {
		errors = append(errors, ValidationError{
			Code:     "PG_NO_REPLICATION_PERMISSION",
			Severity: "error",
			Message:  "Database user lacks REPLICATION privilege",
			Action:   "GRANT REPLICATION privilege to the user",
		})
	}

	return errors, nil
}

// ValidateTablesHavePrimaryKeys hard-blocks CDC tables without PRIMARY KEY.
//
// Tables can be passed as "schema.table" or "table". If unqualified, defaultSchema is used.
func (m *PostgreSQLManager) ValidateTablesHavePrimaryKeys(ctx context.Context, connectionID string, defaultSchema string, tables []string) ([]string, error) {
	decryptedConfig, err := m.getDecryptedConnectionConfig(ctx, connectionID)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection config: %w", err)
	}

	targetDB, err := connectToPostgreSQL(decryptedConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to postgresql for pk validation: %w", err)
	}
	defer targetDB.Close()

	if strings.TrimSpace(defaultSchema) == "" {
		defaultSchema = "public"
	}

	missing := make([]string, 0)
	for _, raw := range tables {
		t := strings.TrimSpace(raw)
		if t == "" {
			continue
		}
		schemaName, tableName := splitSchemaTable(t)
		if strings.TrimSpace(schemaName) == "" {
			schemaName = defaultSchema
		}
		if schemaName == "" || tableName == "" {
			return nil, fmt.Errorf("invalid table identifier %q (expected schema.table or table)", t)
		}

		// Verify table exists (clearer error than "missing PK").
		var exists bool
		if err := targetDB.QueryRowContext(
			ctx,
			`SELECT EXISTS(
				SELECT 1
				FROM information_schema.tables
				WHERE table_schema = $1 AND table_name = $2
			)`,
			schemaName, tableName,
		).Scan(&exists); err != nil {
			return nil, fmt.Errorf("failed to check table existence for %s.%s: %w", schemaName, tableName, err)
		}
		if !exists {
			return nil, fmt.Errorf("table not found in source postgresql: %s.%s", schemaName, tableName)
		}

		var hasPK bool
		if err := targetDB.QueryRowContext(
			ctx,
			`SELECT EXISTS(
				SELECT 1
				FROM pg_index i
				JOIN pg_class c ON c.oid = i.indrelid
				JOIN pg_namespace n ON n.oid = c.relnamespace
				WHERE i.indisprimary = true
				  AND n.nspname = $1
				  AND c.relname = $2
			)`,
			schemaName, tableName,
		).Scan(&hasPK); err != nil {
			return nil, fmt.Errorf("failed to check primary key for %s.%s: %w", schemaName, tableName, err)
		}
		if !hasPK {
			missing = append(missing, fmt.Sprintf("%s.%s", schemaName, tableName))
		}
	}

	return missing, nil
}

func splitSchemaTable(qualified string) (string, string) {
	s := strings.TrimSpace(qualified)
	if s == "" {
		return "", ""
	}
	parts := strings.Split(s, ".")
	if len(parts) == 1 {
		return "", strings.TrimSpace(parts[0])
	}
	// Postgres Debezium commonly uses "schema.table". If more dots exist, keep last segment as table.
	schema := strings.TrimSpace(parts[len(parts)-2])
	table := strings.TrimSpace(parts[len(parts)-1])
	return schema, table
}

// Helper: get decrypted connection config
func (m *PostgreSQLManager) getDecryptedConnectionConfig(ctx context.Context, connectionID string) (map[string]interface{}, error) {
	// IMPORTANT: Reuse the shared connection decryption logic.
	// This ensures config parsing is consistent across orchestrator features.
	mgr := connections.NewManager(m.db)
	cfg, err := mgr.Get(ctx, strings.TrimSpace(connectionID))
	if err != nil {
		return nil, fmt.Errorf("failed to get connection config: %w", err)
	}
	out := make(map[string]interface{}, len(cfg))
	for k, v := range cfg {
		out[k] = v
	}
	return out, nil
}

// Helper: connect to PostgreSQL database
func connectToPostgreSQL(config map[string]interface{}) (*sql.DB, error) {
	host, _ := config["host"].(string)
	port := asIntFromAny(config["port"], 5432)
	database, _ := config["database"].(string)
	user, _ := config["user"].(string)
	password, _ := config["password"].(string)

	// Accept "db_name" as an alias.
	if strings.TrimSpace(database) == "" {
		if v, ok := config["db_name"].(string); ok {
			database = v
		}
	}
	// Accept "dbname" as an alias (common postgres DSN naming).
	if strings.TrimSpace(database) == "" {
		if v, ok := config["dbname"].(string); ok {
			database = v
		}
	}

	// Host-aware SSL, mirroring api-gateway resolvePostgresSSLMode. Stored PG
	// connections carry NO sslmode key (config is host/port/user/password/db),
	// so a hardcoded "disable" default made every CDC connection to a managed
	// PG (Azure, RDS — which reject non-SSL with "no pg_hba.conf entry … no
	// encryption") fail at source-prereq / PK validation / publication+slot
	// provisioning. An explicit sslmode/ssl_mode config key wins; otherwise the
	// default is host-based: local/docker hosts get "disable", remote hosts get
	// "require".
	sslmode := ResolvePostgresSSLMode(config, host)

	connStr := fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s sslmode=%s connect_timeout=5",
		host, port, database, user, password, sslmode,
	)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}

// ResolvePostgresSSLMode picks the DSN "sslmode" for an outbound PostgreSQL
// connection. Mirrors api-gateway resolvePostgresSSLMode (the prod-proven
// connection-test path): an explicit ssl_mode/sslmode config key wins,
// normalised by normalizePostgresSSLMode; otherwise the default is host-based.
// Local/docker-internal hosts get "disable" (dev/e2e PG rarely runs TLS); any
// remote host — e.g. Azure Database for PostgreSQL or RDS, which reject non-SSL
// connections with "no pg_hba.conf entry … no encryption" — gets "verify-full":
// encrypt AND authenticate the server, closing the MITM window.
//
// Stored PG connections carry no sslmode key, so without this the bare
// "disable" default broke every CDC operation against managed PostgreSQL.
func ResolvePostgresSSLMode(config map[string]interface{}, host string) string {
	for _, key := range []string{"ssl_mode", "sslmode"} {
		if v, ok := config[key]; ok && v != nil {
			if s := strings.TrimSpace(fmt.Sprint(v)); s != "" {
				return normalizePostgresSSLMode(s)
			}
		}
	}
	if IsLocalDBHost(host) {
		return "disable"
	}
	return "verify-full"
}

// IsLocalDBHost reports whether host is a local/docker-internal target that
// typically runs without TLS (dev/e2e). Explicit loopback names are local. IP
// LITERALS are classified by address (loopback/RFC1918/ULA/link-local/CGNAT →
// local), so an IPv6 literal or public IPv4 literal is correctly remote instead
// of by a textual "." heuristic that missed them. Non-literal hostnames keep the
// dotless=local heuristic (docker service names). Residual: a dotless hostname
// resolving to a PUBLIC address is still treated local (follow-up).
func IsLocalDBHost(host string) bool {
	h := strings.ToLower(strings.TrimSpace(host))
	h = strings.Trim(h, "[]") // tolerate bracketed IPv6 literals
	if h == "" {
		return true
	}
	switch h {
	case "localhost", "127.0.0.1", "::1", "host.docker.internal":
		return true
	}
	if ip := net.ParseIP(h); ip != nil {
		return isPrivateOrLoopbackIP(ip)
	}
	return !strings.Contains(h, ".")
}

// isPrivateOrLoopbackIP reports whether ip is loopback, RFC1918/ULA private,
// link-local, or CGNAT (100.64.0.0/10) — i.e. NOT a public destination.
func isPrivateOrLoopbackIP(ip net.IP) bool {
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
		return true
	}
	if v4 := ip.To4(); v4 != nil && v4[0] == 100 && v4[1] >= 64 && v4[1] <= 127 {
		return true // 100.64.0.0/10 carrier-grade NAT
	}
	return false
}

// normalizePostgresSSLMode maps assorted SSL spellings onto the four values
// libpq defines: "disable", "require", "verify-ca", "verify-full".
//
// The "prefer"/"allow" → "require" fold is load-bearing for SECURITY, not for
// driver compatibility, and the reason inverted when this repo moved off
// lib/pq. lib/pq REJECTED both with "unsupported sslmode", so emitting one
// failed the connection loudly; the fold merely avoided that error. pgx follows
// libpq and ACCEPTS them, where they mean "try TLS, then silently fall back to
// cleartext". The same fold is now the only thing stopping a user-configured
// "prefer" from downgrading a remote connection — password included — to
// plaintext. Do not drop it on the grounds that pgx tolerates the value:
// tolerating it is exactly the hazard. Unknown values fall back to "require"
// rather than risk a non-SSL connection to a remote host.
func normalizePostgresSSLMode(s string) string {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "disable", "disabled", "false", "off", "0", "none":
		return "disable"
	case "require", "required", "prefer", "preferred", "allow", "true", "on", "1":
		return "require"
	case "verify-ca", "verify_ca":
		return "verify-ca"
	case "verify-full", "verify_full", "verify-identity", "verify_identity":
		return "verify-full"
	default:
		return "require"
	}
}

func asIntFromAny(v interface{}, def int) int {
	switch tv := v.(type) {
	case int:
		return tv
	case int32:
		return int(tv)
	case int64:
		return int(tv)
	case float32:
		return int(tv)
	case float64:
		return int(tv)
	case string:
		n, err := strconv.Atoi(strings.TrimSpace(tv))
		if err == nil {
			return n
		}
	}
	return def
}

// ValidationError represents a validation error
type ValidationError struct {
	Code     string `json:"code"`
	Severity string `json:"severity"` // error, warning
	Message  string `json:"message"`
	Action   string `json:"action,omitempty"`
}

// quotePgIdentifier safely double-quotes a PostgreSQL identifier (table or schema name)
// to prevent SQL injection. Handles "schema.table" notation by quoting each part.
func quotePgIdentifier(ident string) string {
	parts := strings.Split(ident, ".")
	quoted := make([]string, len(parts))
	for i, p := range parts {
		// Standard SQL double-quote escaping: embedded double-quotes become two double-quotes.
		quoted[i] = `"` + strings.ReplaceAll(p, `"`, `""`) + `"`
	}
	return strings.Join(quoted, ".")
}
