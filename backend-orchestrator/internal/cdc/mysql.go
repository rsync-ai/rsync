package cdc

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/rsync-ai/shared/crypto"
	log "github.com/sirupsen/logrus"
)

// BinlogPosition is a MySQL binlog coordinate — the position "P" for the hybrid
// batch+CDC handoff. Debezium resumes streaming from this point when its offset is
// seeded into the Kafka connect-offsets topic before the connector is created with
// a schema-recovery snapshot mode. A batch historical load taken at/after P plus
// CDC from P converge (upsert by PK) with no gap.
type BinlogPosition struct {
	File string `json:"file"`
	Pos  uint64 `json:"pos"`
	GTID string `json:"gtids,omitempty"`
}

// IsZero reports whether no usable position was captured.
func (b BinlogPosition) IsZero() bool { return strings.TrimSpace(b.File) == "" }

// binlogColToString coerces a driver scan value (often []byte) to a string.
func binlogColToString(v interface{}) string {
	switch t := v.(type) {
	case nil:
		return ""
	case []byte:
		return string(t)
	case string:
		return t
	default:
		return fmt.Sprintf("%v", t)
	}
}

// captureBinlogPositionDB reads the current binlog coordinates from an open MySQL
// connection. It tries SHOW MASTER STATUS (MySQL <8.4 / MariaDB) and falls back to
// SHOW BINARY LOG STATUS (MySQL 8.4+, where SHOW MASTER STATUS was removed). The
// column set varies by version, so it scans positionally by column name.
func captureBinlogPositionDB(ctx context.Context, db *sql.DB) (BinlogPosition, error) {
	read := func(query string) (BinlogPosition, error) {
		rows, err := db.QueryContext(ctx, query)
		if err != nil {
			return BinlogPosition{}, err
		}
		defer rows.Close()
		cols, err := rows.Columns()
		if err != nil {
			return BinlogPosition{}, err
		}
		if !rows.Next() {
			if err := rows.Err(); err != nil {
				return BinlogPosition{}, err
			}
			return BinlogPosition{}, fmt.Errorf("binlog status returned no rows (is binary logging enabled?)")
		}
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return BinlogPosition{}, err
		}
		var out BinlogPosition
		for i, c := range cols {
			s := binlogColToString(vals[i])
			switch strings.ToLower(c) {
			case "file":
				out.File = strings.TrimSpace(s)
			case "position":
				if n, perr := strconv.ParseUint(strings.TrimSpace(s), 10, 64); perr == nil {
					out.Pos = n
				}
			case "executed_gtid_set":
				out.GTID = strings.ReplaceAll(strings.TrimSpace(s), "\n", "")
			}
		}
		if out.File == "" {
			return BinlogPosition{}, fmt.Errorf("binlog status missing File column (binary logging may be disabled)")
		}
		return out, nil
	}
	if p, err := read("SHOW MASTER STATUS"); err == nil {
		return p, nil
	}
	// MySQL 8.4+ removed SHOW MASTER STATUS in favor of SHOW BINARY LOG STATUS.
	return read("SHOW BINARY LOG STATUS")
}

// CaptureBinlogPosition connects to the source MySQL for the given connection and
// returns the current binlog coordinates (position "P" for the hybrid handoff). The
// executor calls this at the precise moment before the batch historical load begins.
func (m *MySQLManager) CaptureBinlogPosition(ctx context.Context, connectionID string) (BinlogPosition, error) {
	decryptedConfig, err := m.getDecryptedConnectionConfig(ctx, connectionID)
	if err != nil {
		return BinlogPosition{}, fmt.Errorf("failed to get connection config: %w", err)
	}
	targetDB, err := connectToMySQL(decryptedConfig)
	if err != nil {
		return BinlogPosition{}, fmt.Errorf("failed to connect to target MySQL: %w", err)
	}
	defer targetDB.Close()
	return captureBinlogPositionDB(ctx, targetDB)
}

// CheckBinlogRetention returns the configured binlog retention in seconds for the
// source. For the hybrid handoff the captured position P must remain valid (binlog not
// purged) for the entire batch load — if retention is shorter than the batch takes, the
// purged binlog means CDC can't resume from P and changes are lost. We cannot know batch
// duration up front, so this surfaces the retention so the caller can warn the user.
// Returns (seconds, "" ) when retention is effectively unlimited (0) or could not be read.
func (m *MySQLManager) CheckBinlogRetention(ctx context.Context, connectionID string) (int, error) {
	decryptedConfig, err := m.getDecryptedConnectionConfig(ctx, connectionID)
	if err != nil {
		return 0, fmt.Errorf("failed to get connection config: %w", err)
	}
	targetDB, err := connectToMySQL(decryptedConfig)
	if err != nil {
		return 0, fmt.Errorf("failed to connect to target MySQL: %w", err)
	}
	defer targetDB.Close()

	// MySQL 8.0+: binlog_expire_logs_seconds (0 = no automatic purge).
	var name, val string
	if err := targetDB.QueryRowContext(ctx, "SHOW VARIABLES LIKE 'binlog_expire_logs_seconds'").Scan(&name, &val); err == nil {
		if secs, perr := strconv.Atoi(strings.TrimSpace(val)); perr == nil {
			if secs > 0 {
				return secs, nil
			}
			// 0 means no automatic expiry by seconds; fall through to legacy days.
		}
	}
	// Legacy MySQL 5.7 / fallback: expire_logs_days (0 = no automatic purge).
	if err := targetDB.QueryRowContext(ctx, "SHOW VARIABLES LIKE 'expire_logs_days'").Scan(&name, &val); err == nil {
		if days, perr := strconv.Atoi(strings.TrimSpace(val)); perr == nil && days > 0 {
			return days * 86400, nil
		}
	}
	return 0, nil // unlimited / unknown
}

// readBinlogIndex returns the source's binlog files (in order) and their sizes via
// SHOW BINARY LOGS. The column set is (Log_name, File_size[, Encrypted]); we scan by
// name to stay version-tolerant. Requires the REPLICATION CLIENT privilege (the same
// privilege Debezium already needs), so it is available for any CDC-capable user.
func readBinlogIndex(ctx context.Context, db *sql.DB) (sizes map[string]int64, order []string, err error) {
	rows, err := db.QueryContext(ctx, "SHOW BINARY LOGS")
	if err != nil {
		return nil, nil, fmt.Errorf("SHOW BINARY LOGS failed (needs REPLICATION CLIENT): %w", err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}
	sizes = make(map[string]int64)
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, nil, err
		}
		var name string
		var size int64
		for i, c := range cols {
			s := binlogColToString(vals[i])
			switch strings.ToLower(c) {
			case "log_name":
				name = strings.TrimSpace(s)
			case "file_size":
				if n, perr := strconv.ParseInt(strings.TrimSpace(s), 10, 64); perr == nil {
					size = n
				}
			}
		}
		if name != "" {
			sizes[name] = size
			order = append(order, name)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	if len(order) == 0 {
		return nil, nil, fmt.Errorf("SHOW BINARY LOGS returned no rows (is binary logging enabled?)")
	}
	return sizes, order, nil
}

// BinlogLagBytes measures how many bytes of binary log the source MySQL has produced
// beyond the connector's committed position. This is the MySQL analog of PostgreSQL
// replication-slot lag (pg_current_wal_lsn − confirmed_flush_lsn): it is computed
// entirely from the source's own binlog index, NOT from the Kafka sink, so it
// reflects true source-side capture lag.
//
// committed is the Debezium connector's last-committed binlog coordinate (read from
// the Kafka Connect offsets API). The returned lag is:
//
//	abspos(current) − abspos(committed)
//
// where abspos(file,pos) sums the sizes (SHOW BINARY LOGS) of every binlog preceding
// the file plus the in-file position. It also returns the source's current position
// (useful for logging). A negative result is clamped to 0 (a committed offset can
// momentarily read ahead of a slightly stale current snapshot).
func (m *MySQLManager) BinlogLagBytes(ctx context.Context, connectionID string, committed BinlogPosition) (int64, BinlogPosition, error) {
	if committed.IsZero() {
		return 0, BinlogPosition{}, fmt.Errorf("no committed binlog position provided")
	}
	decryptedConfig, err := m.getDecryptedConnectionConfig(ctx, connectionID)
	if err != nil {
		return 0, BinlogPosition{}, fmt.Errorf("failed to get connection config: %w", err)
	}
	srcDB, err := connectToMySQL(decryptedConfig)
	if err != nil {
		return 0, BinlogPosition{}, fmt.Errorf("failed to connect to source MySQL: %w", err)
	}
	defer srcDB.Close()

	current, err := captureBinlogPositionDB(ctx, srcDB)
	if err != nil {
		return 0, BinlogPosition{}, fmt.Errorf("failed to read current binlog position: %w", err)
	}

	sizes, order, err := readBinlogIndex(ctx, srcDB)
	if err != nil {
		return 0, current, err
	}

	lag, err := binlogLagFromIndex(sizes, order, current, committed)
	if err != nil {
		return 0, current, err
	}
	return lag, current, nil
}

// binlogAbsPos returns the absolute byte offset of a binlog coordinate across the
// ordered set of binlog files: the summed sizes of every file preceding p.File plus
// p.Pos. The bool is false when p.File is not present in the index (purged/unknown).
// Kept pure (no I/O) so the cross-file arithmetic is unit-testable.
func binlogAbsPos(sizes map[string]int64, order []string, p BinlogPosition) (int64, bool) {
	base := int64(0)
	for _, name := range order {
		if name == p.File {
			return base + int64(p.Pos), true
		}
		base += sizes[name]
	}
	return 0, false
}

// binlogLagFromIndex computes current − committed in absolute binlog bytes given the
// source's binlog index (file sizes + order). A negative result is clamped to 0 (a
// committed offset can momentarily read ahead of a slightly stale current snapshot).
func binlogLagFromIndex(sizes map[string]int64, order []string, current, committed BinlogPosition) (int64, error) {
	curAbs, okCur := binlogAbsPos(sizes, order, current)
	comAbs, okCom := binlogAbsPos(sizes, order, committed)
	if !okCur || !okCom {
		return 0, fmt.Errorf(
			"binlog file not in source index (committed=%s current=%s; binlog may have been purged)",
			committed.File, current.File)
	}
	lag := curAbs - comAbs
	if lag < 0 {
		lag = 0
	}
	return lag, nil
}

// MySQLManager handles CDC resource provisioning for MySQL
type MySQLManager struct {
	db *sql.DB
}

// NewMySQLManager creates a new MySQL CDC manager
func NewMySQLManager(db *sql.DB) *MySQLManager {
	return &MySQLManager{db: db}
}

// init registers MySQL as a CDC source provider so the handler dispatches
// through the shared registry.
func init() {
	RegisterProvider(func(db *sql.DB) CDCSourceProvider {
		return NewMySQLManager(db)
	}, "mysql")
}

// Family returns the canonical source-family key for MySQL.
func (m *MySQLManager) Family() string { return "mysql" }

// PrimaryKeyNamespace uses the database to qualify unqualified table names for
// PK validation (MySQL groups tables by database, not schema).
func (m *MySQLManager) PrimaryKeyNamespace(defaultDB, defaultSchema string) string {
	return defaultDB
}

// ProvisionResources provisions server ID for a MySQL CDC pipeline (idempotent)
func (m *MySQLManager) ProvisionResources(ctx context.Context, config CDCResourceConfig, tables []string) ([]CDCResource, error) {
	log.WithFields(log.Fields{
		"pipeline_id":   config.PipelineID,
		"connection_id": config.ConnectionID,
		"database":      config.Database,
		"table_count":   len(tables),
	}).Info("Provisioning MySQL CDC resources")

	resources := []CDCResource{}

	// Get connection details
	decryptedConfig, err := m.getDecryptedConnectionConfig(ctx, config.ConnectionID)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection config: %w", err)
	}

	targetDB, err := connectToMySQL(decryptedConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to target MySQL: %w", err)
	}
	defer targetDB.Close()

	// 1. Check if we already have a server_id for this pipeline (idempotent)
	existingResources, err := GetCDCResources(ctx, m.db, config.PipelineID)
	if err == nil {
		for _, r := range existingResources {
			if r.ResourceType == "server_id" && r.DatabaseType == "mysql" && r.Status == "active" {
				log.WithFields(log.Fields{
					"pipeline_id": config.PipelineID,
					"server_id":   r.ResourceName,
				}).Info("MySQL CDC server_id already provisioned (idempotent)")
				return append(resources, r), nil
			}
		}
	}

	// 2. Generate deterministic server ID with collision check
	serverID := GenerateResourceName(config, "server_id")

	// Check for server_id collision with other pipelines
	var collisionCount int
	err = m.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM cdc_resources 
		WHERE resource_type = 'server_id' 
		AND resource_name = $1 
		AND status = 'active'
		AND (pipeline_id IS NULL OR pipeline_id != $2)
	`, serverID, config.PipelineID).Scan(&collisionCount)
	if err == nil && collisionCount > 0 {
		// Collision detected - append a unique suffix
		log.WithField("server_id", serverID).Warn("Server ID collision detected, generating alternative")
		// Add random suffix to avoid collision
		serverID = fmt.Sprintf("%s_%d", serverID[:len(serverID)-3], time.Now().UnixNano()%10000)
	}

	// Verify the server ID doesn't conflict with the MySQL server's own ID
	var mysqlServerID int
	var variable string
	err = targetDB.QueryRowContext(ctx, "SHOW VARIABLES LIKE 'server_id'").Scan(&variable, &mysqlServerID)
	if err == nil {
		log.WithFields(log.Fields{
			"mysql_server_id":    mysqlServerID,
			"debezium_server_id": serverID,
		}).Debug("Checked MySQL server_id for conflicts")
	}

	serverMetadata := map[string]interface{}{
		"database":        config.Database,
		"tables":          tables,
		"note":            "Debezium connector server ID",
		"mysql_server_id": mysqlServerID,
		"provisioned_at":  time.Now().UTC().Format(time.RFC3339),
	}
	// Best-effort: record the binlog position at provisioning time for observability.
	// The hybrid executor path captures its own P immediately before the batch load
	// (CaptureBinlogPosition) for precise handoff timing; this is a record-keeping copy.
	if bp, err := captureBinlogPositionDB(ctx, targetDB); err == nil && !bp.IsZero() {
		serverMetadata["binlog_file"] = bp.File
		serverMetadata["binlog_pos"] = bp.Pos
		if bp.GTID != "" {
			serverMetadata["binlog_gtid"] = bp.GTID
		}
	}

	serverIDResource := CDCResource{
		PipelineID:   &config.PipelineID,
		ConnectionID: config.ConnectionID,
		ResourceType: "server_id",
		ResourceName: serverID,
		Status:       "active",
		DatabaseType: "mysql",
		Metadata:     serverMetadata,
		CreatedAt:    time.Now(),
	}

	// Record server ID in cdc_resources table (uses ON CONFLICT for idempotency)
	if err := RecordResource(ctx, m.db, serverIDResource); err != nil {
		return nil, fmt.Errorf("failed to record server ID: %w", err)
	}

	resources = append(resources, serverIDResource)

	log.WithFields(log.Fields{
		"pipeline_id": config.PipelineID,
		"server_id":   serverID,
	}).Info("Successfully provisioned MySQL CDC resources")

	return resources, nil
}

// CleanupResources removes CDC resources for a pipeline
func (m *MySQLManager) CleanupResources(ctx context.Context, pipelineID string) error {
	log.WithField("pipeline_id", pipelineID).Info("Cleaning up MySQL CDC resources")

	// Get all resources for this pipeline
	resources, err := GetCDCResources(ctx, m.db, pipelineID)
	if err != nil {
		return fmt.Errorf("failed to get CDC resources: %w", err)
	}

	for _, resource := range resources {
		if resource.DatabaseType != "mysql" {
			continue
		}

		// For MySQL, we primarily track server_id for bookkeeping
		// No actual database-level cleanup needed (Debezium manages its state in Kafka)

		// Mark resource as deleted in DB
		if err := MarkResourceDeleted(ctx, m.db, resource.ResourceName, resource.ResourceType); err != nil {
			log.WithError(err).WithField("resource", resource.ResourceName).Warn("Failed to mark resource as deleted")
		} else {
			log.WithField("server_id", resource.ResourceName).Info("Marked MySQL CDC resource as deleted")
		}
	}

	return nil
}

// ValidatePrerequisites checks if MySQL is properly configured for CDC
func (m *MySQLManager) ValidatePrerequisites(ctx context.Context, connectionID string) ([]ValidationError, error) {
	errors := []ValidationError{}

	decryptedConfig, err := m.getDecryptedConnectionConfig(ctx, connectionID)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection config: %w", err)
	}

	targetDB, err := connectToMySQL(decryptedConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MySQL: %w", err)
	}
	defer targetDB.Close()

	// Check binlog format
	var variable, binlogFormat string
	err = targetDB.QueryRowContext(ctx, "SHOW VARIABLES LIKE 'binlog_format'").Scan(&variable, &binlogFormat)
	if err != nil {
		return nil, fmt.Errorf("failed to check binlog_format: %w", err)
	}

	if binlogFormat != "ROW" {
		errors = append(errors, ValidationError{
			Code:     "MYSQL_BINLOG_FORMAT",
			Severity: "error",
			Message:  fmt.Sprintf("MySQL binlog_format is '%s' but must be 'ROW' for CDC", binlogFormat),
			Action:   "SET GLOBAL binlog_format='ROW'; and restart MySQL",
		})
	}

	// Check binlog row image
	var binlogRowImage string
	err = targetDB.QueryRowContext(ctx, "SHOW VARIABLES LIKE 'binlog_row_image'").Scan(&variable, &binlogRowImage)
	if err == nil && binlogRowImage != "FULL" {
		// BLOCKING for CDC: Debezium hard-requires binlog_row_image=FULL and aborts
		// the connector task with "The database server is not configured to use a FULL
		// binlog_row_image" when it is MINIMAL/NOBLOB. A warning would let the pipeline
		// proceed and fail deep inside Debezium, so this must be a hard error.
		errors = append(errors, ValidationError{
			Code:     "MYSQL_BINLOG_ROW_IMAGE",
			Severity: "error",
			Message:  fmt.Sprintf("MySQL binlog_row_image is '%s' but must be 'FULL' for CDC", binlogRowImage),
			Action:   "SET GLOBAL binlog_row_image='FULL'; (on managed MySQL such as Azure/RDS, change the binlog_row_image server parameter and restart)",
		})
	}

	// Check replication permissions
	var grants string
	rows, err := targetDB.QueryContext(ctx, "SHOW GRANTS FOR CURRENT_USER()")
	if err != nil {
		return nil, fmt.Errorf("failed to check grants: %w", err)
	}
	defer rows.Close()

	hasReplicationClient := false
	hasReplicationSlave := false

	for rows.Next() {
		if err := rows.Scan(&grants); err != nil {
			continue
		}
		if contains(grants, "REPLICATION CLIENT") {
			hasReplicationClient = true
		}
		if contains(grants, "REPLICATION SLAVE") {
			hasReplicationSlave = true
		}
		if contains(grants, "ALL PRIVILEGES") {
			hasReplicationClient = true
			hasReplicationSlave = true
		}
	}

	if !hasReplicationClient {
		errors = append(errors, ValidationError{
			Code:     "MYSQL_NO_REPLICATION_CLIENT",
			Severity: "error",
			Message:  "Database user lacks REPLICATION CLIENT privilege",
			Action:   "GRANT REPLICATION CLIENT ON *.* TO 'user'@'host';",
		})
	}

	if !hasReplicationSlave {
		errors = append(errors, ValidationError{
			Code:     "MYSQL_NO_REPLICATION_SLAVE",
			Severity: "error",
			Message:  "Database user lacks REPLICATION SLAVE privilege",
			Action:   "GRANT REPLICATION SLAVE ON *.* TO 'user'@'host';",
		})
	}

	return errors, nil
}

// ValidateTablesHavePrimaryKeys hard-blocks CDC tables without PRIMARY KEY.
//
// This is required for correct upsert/delete semantics when the destination is a relational DB.
// Tables can be passed as "db.table" or "table". If unqualified, defaultDB is used.
func (m *MySQLManager) ValidateTablesHavePrimaryKeys(ctx context.Context, connectionID string, defaultDB string, tables []string) ([]string, error) {
	decryptedConfig, err := m.getDecryptedConnectionConfig(ctx, connectionID)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection config: %w", err)
	}
	if strings.TrimSpace(defaultDB) != "" {
		decryptedConfig["database"] = strings.TrimSpace(defaultDB)
	}

	targetDB, err := connectToMySQL(decryptedConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to mysql for pk validation: %w", err)
	}
	defer targetDB.Close()

	missing := make([]string, 0)
	for _, raw := range tables {
		t := strings.TrimSpace(raw)
		if t == "" {
			continue
		}
		dbName, tableName := split2(t)
		if strings.TrimSpace(dbName) == "" {
			dbName = strings.TrimSpace(defaultDB)
		}
		if dbName == "" || tableName == "" {
			return nil, fmt.Errorf("invalid table identifier %q (expected db.table or table)", t)
		}

		// Verify table exists (clearer error than "missing PK").
		var exists int
		if err := targetDB.QueryRowContext(
			ctx,
			`SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`,
			dbName, tableName,
		).Scan(&exists); err != nil {
			return nil, fmt.Errorf("failed to check table existence for %s.%s: %w", dbName, tableName, err)
		}
		if exists == 0 {
			return nil, fmt.Errorf("table not found in source mysql: %s.%s", dbName, tableName)
		}

		var pkCount int
		if err := targetDB.QueryRowContext(
			ctx,
			`SELECT COUNT(*) FROM information_schema.KEY_COLUMN_USAGE
			  WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY'`,
			dbName, tableName,
		).Scan(&pkCount); err != nil {
			return nil, fmt.Errorf("failed to check primary key for %s.%s: %w", dbName, tableName, err)
		}
		if pkCount == 0 {
			missing = append(missing, fmt.Sprintf("%s.%s", dbName, tableName))
		}
	}

	return missing, nil
}

func split2(qualified string) (string, string) {
	// MySQL Debezium: usually "db.table". If multiple dots exist, keep last segment as table.
	s := strings.TrimSpace(qualified)
	if s == "" {
		return "", ""
	}
	parts := strings.Split(s, ".")
	if len(parts) == 1 {
		return "", strings.TrimSpace(parts[0])
	}
	db := strings.TrimSpace(parts[0])
	table := strings.TrimSpace(parts[len(parts)-1])
	return db, table
}

// EnsureDebeziumSignalTable ensures the Debezium signaling table exists in the source MySQL database.
// This enables ad-hoc snapshots (incremental or blocking) via "execute-snapshot" signals.
//
// NOTE: This is a best-effort helper. It requires the source user to have CREATE TABLE privileges.
func (m *MySQLManager) EnsureDebeziumSignalTable(ctx context.Context, connectionID, database string) error {
	decryptedConfig, err := m.getDecryptedConnectionConfig(ctx, connectionID)
	if err != nil {
		return fmt.Errorf("failed to get connection config: %w", err)
	}
	if database != "" {
		decryptedConfig["database"] = database
	}

	targetDB, err := connectToMySQL(decryptedConfig)
	if err != nil {
		return fmt.Errorf("failed to connect to MySQL: %w", err)
	}
	defer targetDB.Close()

	// Keep schema minimal and compatible with Debezium docs + our e2e fixtures.
	_, err = targetDB.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS debezium_signal (
			id VARCHAR(64) PRIMARY KEY,
			type VARCHAR(32) NOT NULL,
			data TEXT
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to ensure debezium_signal table: %w", err)
	}
	return nil
}

// UpsertDebeziumSignal inserts a Debezium signal row into the source database.
// For MySQL we use ON DUPLICATE KEY UPDATE to make retries idempotent.
func (m *MySQLManager) UpsertDebeziumSignal(ctx context.Context, connectionID, database, id, signalType, data string) error {
	decryptedConfig, err := m.getDecryptedConnectionConfig(ctx, connectionID)
	if err != nil {
		return fmt.Errorf("failed to get connection config: %w", err)
	}
	if database != "" {
		decryptedConfig["database"] = database
	}

	targetDB, err := connectToMySQL(decryptedConfig)
	if err != nil {
		return fmt.Errorf("failed to connect to MySQL: %w", err)
	}
	defer targetDB.Close()

	// Ensure table exists first (best-effort, safe to call repeatedly).
	if err := m.EnsureDebeziumSignalTable(ctx, connectionID, database); err != nil {
		return err
	}

	_, err = targetDB.ExecContext(
		ctx,
		`INSERT INTO debezium_signal (id, type, data)
		 VALUES (?, ?, ?)
		 ON DUPLICATE KEY UPDATE type=VALUES(type), data=VALUES(data)`,
		id,
		signalType,
		data,
	)
	if err != nil {
		return fmt.Errorf("failed to upsert debezium signal: %w", err)
	}
	return nil
}

// Helper: get decrypted connection config
func (m *MySQLManager) getDecryptedConnectionConfig(ctx context.Context, connectionID string) (map[string]interface{}, error) {
	var configJSON string
	err := m.db.QueryRowContext(ctx, "SELECT config FROM connections WHERE id = $1", connectionID).Scan(&configJSON)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection config: %w", err)
	}

	raw := strings.TrimSpace(configJSON)
	decryptedJSON := ""

	// Backward-compat: some environments may store plaintext JSON (dev only).
	if strings.HasPrefix(raw, "{") || strings.HasPrefix(raw, "[") {
		decryptedJSON = raw
	} else {
		dec, err := crypto.DecryptString(raw)
		if err != nil {
			return nil, fmt.Errorf("failed to decrypt config: %w", err)
		}
		decryptedJSON = dec
	}

	var config map[string]interface{}
	if err := json.Unmarshal([]byte(decryptedJSON), &config); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	return config, nil
}

// Helper: connect to MySQL database
func connectToMySQL(config map[string]interface{}) (*sql.DB, error) {
	host, _ := config["host"].(string)
	port, _ := config["port"].(float64)
	database, _ := config["database"].(string)
	user, _ := config["user"].(string)
	password, _ := config["password"].(string)

	if port == 0 {
		port = 3306
	}

	// Host-aware TLS, mirroring api-gateway resolveMySQLTLSMode. Remote hosts
	// (e.g. Azure Database for MySQL, RDS) enforce TLS server-side via
	// require_secure_transport=ON; a non-TLS DSN is rejected with Error 3159.
	// An explicit ssl_mode/sslmode/tls config key wins; otherwise default by
	// host: local→false, remote→true (verify CA+hostname; opt out ssl_mode=require).
	tlsMode := resolveMySQLTLSMode(config, host)

	// MySQL DSN format: user:password@tcp(host:port)/database
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&tls=%s",
		user, password, host, int(port), database, tlsMode)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}

// resolveMySQLTLSMode picks the go-sql-driver/mysql DSN "tls" parameter for an
// outbound MySQL connection. Mirrors api-gateway resolveMySQLTLSMode: an
// explicit config value wins (ssl_mode, sslmode, tls, or tls_mode key, nil-safe
// and trimmed) and is normalised to a driver-accepted value; otherwise the
// default is host-based. Local/docker-internal hosts get "false"; any remote
// host defaults to "true" (verify CA + hostname against the image trust store —
// the orchestrator Dockerfile ships ca-certificates), closing the MITM window
// that "skip-verify" left open on customer-DB (CDC) traffic. A connection whose
// CA is not in the trust store opts out with ssl_mode=require (encrypt, no
// verify). Kept in lockstep with api-gateway + assessor resolveMySQLTLSMode.
func resolveMySQLTLSMode(config map[string]interface{}, host string) string {
	for _, key := range []string{"ssl_mode", "sslmode", "tls", "tls_mode"} {
		if v, ok := config[key]; ok && v != nil {
			if s := strings.TrimSpace(fmt.Sprint(v)); s != "" {
				return normalizeMySQLTLS(s)
			}
		}
	}
	if isLocalMySQLHost(host) {
		return "false"
	}
	return "true"
}

// normalizeMySQLTLS maps assorted SSL/TLS spellings — libpq-style, booleans, and
// native go-sql-driver values — onto the four values the mysql driver
// understands: "false", "true", "skip-verify", "preferred". Unrecognised values
// pass through unchanged so a custom RegisterTLSConfig name still works.
func normalizeMySQLTLS(s string) string {
	switch strings.ToLower(s) {
	case "disable", "disabled", "false", "off", "0", "none":
		return "false"
	case "require", "required", "skip-verify", "skip_verify":
		return "skip-verify"
	case "prefer", "preferred":
		return "preferred"
	case "verify-ca", "verify_ca", "verify-full", "verify_full", "verify-identity", "verify_identity", "true", "on", "1":
		return "true"
	default:
		return s
	}
}

// isLocalMySQLHost reports whether host is a local/docker-internal target that
// typically runs MySQL without TLS. Delegates to IsLocalDBHost so every cdc DB
// engine shares one address-aware classifier (IP literals classified by address).
func isLocalMySQLHost(host string) bool {
	return IsLocalDBHost(host)
}

// Helper: check if string contains substring (case-insensitive)
func contains(s, substr string) bool {
	return len(s) >= len(substr) &&
		(s == substr ||
			len(s) > len(substr) &&
				(s[:len(substr)] == substr ||
					s[len(s)-len(substr):] == substr ||
					findSubstring(s, substr)))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
