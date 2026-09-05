package cdc

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	go_ora "github.com/sijms/go-ora/v2"
	"github.com/rsync-ai/shared/crypto"
	log "github.com/sirupsen/logrus"
)

// OracleManager handles CDC resource provisioning for Oracle Database (LogMiner).
//
// Oracle CDC via Debezium (io.debezium.connector.oracle.OracleConnector,
// database.connection.adapter=logminer) has prerequisites that split cleanly
// into two groups:
//
//   - DBA-only, NOT auto-provisionable → surfaced by ValidatePrerequisites as
//     escalation-worthy ValidationErrors (the healer classifies them
//     ActionEscalate; see diagnose.go): ARCHIVELOG mode (needs a SHUTDOWN/STARTUP
//     MOUNT instance restart), database-level minimal supplemental logging, and
//     the LogMiner capture user + grants. The connector never silently retries
//     these — an unnecessary escalation is recoverable; a retry loop is not.
//   - Per-table, auto-provisionable IF the CDC user has ALTER → ProvisionResources
//     runs `ALTER TABLE ... ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS` so UPDATE and
//     DELETE change events carry complete before-images. This is the analog of
//     SQL Server's sp_cdc_enable_table.
//
// Unlike PostgreSQL there is NO publication/slot ordering to own, and there is no
// Debezium autocreate.mode analog — Debezium never creates Oracle-side infra.
//
// Teardown is deliberately conservative: table-level supplemental logging is a
// SINGLE shared table attribute (not a per-consumer object like a SQL Server
// capture instance), so CleanupResources marks this pipeline's ledger rows
// deleted but does NOT physically DROP SUPPLEMENTAL LOG DATA — doing so would
// silently break any sibling pipeline capturing the same table. (An ownership-
// aware reaper could drop it once no pipeline needs it — out of scope here,
// mirroring how SQL Server never calls sp_cdc_disable_db.)
type OracleManager struct {
	db *sql.DB
}

// NewOracleManager creates a new Oracle CDC manager.
func NewOracleManager(db *sql.DB) *OracleManager {
	return &OracleManager{db: db}
}

// init registers Oracle (and its "oracledb" alias, via NormalizeDBType) as a CDC
// source provider so the handler dispatches through the shared registry.
func init() {
	RegisterProvider(func(db *sql.DB) CDCSourceProvider {
		return NewOracleManager(db)
	}, "oracle")
}

// Family returns the canonical source-family key for Oracle.
func (m *OracleManager) Family() string { return "oracle" }

// PrimaryKeyNamespace uses the schema/owner to qualify unqualified table names.
// Oracle groups tables by owner (uppercase). An empty schema means "the
// connecting user's own schema", which ValidateTablesHavePrimaryKeys resolves via
// the USER_* data-dictionary views.
func (m *OracleManager) PrimaryKeyNamespace(defaultDB, defaultSchema string) string {
	s := strings.TrimSpace(defaultSchema)
	if s == "" {
		return ""
	}
	return strings.ToUpper(s)
}

var oracleIdentRe = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_$#]*$`)

// splitOwnerTableOra parses "owner.table" or "table" and upper-cases both parts
// (Oracle folds unquoted identifiers to uppercase; conventional source tables are
// stored uppercase). owner is "" when the table is unqualified.
func splitOwnerTableOra(qualified string) (string, string) {
	q := strings.TrimSpace(strings.ReplaceAll(qualified, "\"", ""))
	if q == "" {
		return "", ""
	}
	if i := strings.Index(q, "."); i > 0 {
		return strings.ToUpper(strings.TrimSpace(q[:i])), strings.ToUpper(strings.TrimSpace(q[i+1:]))
	}
	return "", strings.ToUpper(q)
}

// oraQName builds a quoted, optionally owner-qualified identifier after validating
// each part (identifiers cannot be bound parameters in DDL, so they are validated
// against a strict allowlist and double-quoted).
func oraQName(owner, table string) (string, error) {
	if !oracleIdentRe.MatchString(table) {
		return "", fmt.Errorf("unsafe Oracle table identifier %q", table)
	}
	if owner != "" {
		if !oracleIdentRe.MatchString(owner) {
			return "", fmt.Errorf("unsafe Oracle owner identifier %q", owner)
		}
		return fmt.Sprintf("%q.%q", owner, table), nil
	}
	return fmt.Sprintf("%q", table), nil
}

// ProvisionResources enables per-table supplemental logging (ALL COLUMNS) so CDC
// UPDATE/DELETE events carry full before-images, and records each as a
// supplemental_log_group resource. Idempotent: an already-logged table (ORA-32588)
// is treated as success. DBA-only prerequisites (ARCHIVELOG, DB-level supplemental
// logging, grants) are validated separately by ValidatePrerequisites and are NOT
// provisioned here.
func (m *OracleManager) ProvisionResources(ctx context.Context, config CDCResourceConfig, tables []string) ([]CDCResource, error) {
	log.WithFields(log.Fields{
		"pipeline_id":   config.PipelineID,
		"connection_id": config.ConnectionID,
		"table_count":   len(tables),
	}).Info("Provisioning Oracle CDC resources (per-table supplemental logging)")

	resources := []CDCResource{}

	decryptedConfig, err := m.getDecryptedConnectionConfig(ctx, config.ConnectionID)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection config: %w", err)
	}

	targetDB, err := connectToOracle(decryptedConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to target Oracle: %w", err)
	}
	defer targetDB.Close()

	for _, raw := range tables {
		owner, table := splitOwnerTableOra(raw)
		if table == "" {
			return nil, fmt.Errorf("invalid table identifier %q (expected owner.table or table)", raw)
		}
		qname, err := oraQName(owner, table)
		if err != nil {
			return nil, err
		}

		if err := enableTableSupplementalLogging(ctx, targetDB, qname); err != nil {
			// ORA-01031 (insufficient privileges) / ORA-00942 (table missing) are
			// terminal here — the error string carries the ORA- code so the
			// healer's RuleBasedDiagnoser classifies it ActionEscalate.
			return nil, fmt.Errorf("failed to add supplemental logging for %s: %w", raw, err)
		}

		sourceTable := raw
		tblCfg := config
		if owner != "" {
			tblCfg.Table = fmt.Sprintf("%s.%s", owner, table)
		} else {
			tblCfg.Table = table
		}
		res := CDCResource{
			PipelineID:   &config.PipelineID,
			ConnectionID: config.ConnectionID,
			SourceTable:  &sourceTable,
			ResourceType: "supplemental_log_group",
			ResourceName: GenerateResourceName(tblCfg, "supplemental_log_group"),
			Status:       "active",
			DatabaseType: "oracle",
			Metadata: map[string]interface{}{
				"owner":          owner,
				"table":          table,
				"provisioned_at": time.Now().UTC().Format(time.RFC3339),
			},
			CreatedAt: time.Now(),
		}
		if err := RecordResource(ctx, m.db, res); err != nil {
			return nil, fmt.Errorf("failed to record supplemental log group for %s: %w", raw, err)
		}
		resources = append(resources, res)
	}

	log.WithFields(log.Fields{
		"pipeline_id":    config.PipelineID,
		"resource_count": len(resources),
	}).Info("Successfully provisioned Oracle CDC resources")

	return resources, nil
}

// CleanupResources marks this pipeline's supplemental_log_group ledger rows
// deleted. It is idempotent/best-effort and NEVER physically DROPs table-level
// supplemental logging (a shared table attribute — dropping it would break any
// sibling pipeline capturing the same table).
func (m *OracleManager) CleanupResources(ctx context.Context, pipelineID string) error {
	log.WithField("pipeline_id", pipelineID).Info("Cleaning up Oracle CDC resources")

	resources, err := GetCDCResources(ctx, m.db, pipelineID)
	if err != nil {
		return fmt.Errorf("failed to get CDC resources: %w", err)
	}

	found := 0
	for _, r := range resources {
		if r.DatabaseType != "oracle" || r.ResourceType != "supplemental_log_group" {
			continue
		}
		found++
		// Ledger-only teardown: supplemental logging is shared, so we never run
		// `DROP SUPPLEMENTAL LOG DATA`. Mark the row deleted so accounting stays
		// accurate; the physical attribute persists for any sibling pipeline.
		if err := MarkResourceDeleted(ctx, m.db, r.ResourceName, r.ResourceType); err != nil {
			log.WithError(err).WithField("resource", r.ResourceName).
				Warn("Failed to mark Oracle CDC resource as deleted")
		}
	}
	if found == 0 {
		return nil // nothing this family owns for the pipeline — no-op
	}
	return nil
}

// ValidatePrerequisites checks the DBA-provisioned prerequisites for Oracle
// LogMiner CDC. Every failure is escalation-worthy (ActionEscalate): these need a
// DBA/SYSDBA and, for ARCHIVELOG, an instance restart — the connector must never
// silently retry them. Error strings embed the exact remediation SQL.
func (m *OracleManager) ValidatePrerequisites(ctx context.Context, connectionID string) ([]ValidationError, error) {
	errors := []ValidationError{}

	decryptedConfig, err := m.getDecryptedConnectionConfig(ctx, connectionID)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection config: %w", err)
	}
	targetDB, err := connectToOracle(decryptedConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Oracle: %w", err)
	}
	defer targetDB.Close()

	// ARCHIVELOG mode — LogMiner cannot mine redo without it. Enabling it requires
	// SHUTDOWN IMMEDIATE; STARTUP MOUNT; ALTER DATABASE ARCHIVELOG; ALTER DATABASE
	// OPEN (a full instance restart) — DBA/SYSDBA only.
	var logMode sql.NullString
	if err := targetDB.QueryRowContext(ctx, "SELECT LOG_MODE FROM V$DATABASE").Scan(&logMode); err != nil {
		errors = append(errors, ValidationError{
			Code:     "ORACLE_VVIEW_ACCESS_DENIED",
			Severity: "error",
			Message:  fmt.Sprintf("Cannot read V$DATABASE (LogMiner requires SELECT on the V_$ views): %v", err),
			Action:   "Grant the capture user SELECT_CATALOG_ROLE and the LogMiner privileges (SELECT on V_$DATABASE/V_$LOG/V_$LOGMNR_CONTENTS, EXECUTE on DBMS_LOGMNR).",
		})
	} else if !strings.EqualFold(strings.TrimSpace(logMode.String), "ARCHIVELOG") {
		errors = append(errors, ValidationError{
			Code:     "ORACLE_ARCHIVELOG_DISABLED",
			Severity: "error",
			Message:  fmt.Sprintf("Database LOG_MODE is %q but Oracle LogMiner CDC requires ARCHIVELOG mode", logMode.String),
			Action:   "As a DBA: SHUTDOWN IMMEDIATE; STARTUP MOUNT; ALTER DATABASE ARCHIVELOG; ALTER DATABASE OPEN; (requires an instance restart).",
		})
	}

	// Database-level minimal supplemental logging — required so redo carries the
	// row identifiers Debezium needs. ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
	var suppMin sql.NullString
	if err := targetDB.QueryRowContext(ctx, "SELECT SUPPLEMENTAL_LOG_DATA_MIN FROM V$DATABASE").Scan(&suppMin); err == nil {
		v := strings.ToUpper(strings.TrimSpace(suppMin.String))
		if v != "YES" && v != "IMPLICIT" {
			errors = append(errors, ValidationError{
				Code:     "ORACLE_SUPPLEMENTAL_LOGGING_DISABLED",
				Severity: "error",
				Message:  "Database-level minimal supplemental logging is disabled; Oracle LogMiner CDC requires it",
				Action:   "As a DBA: ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;",
			})
		}
	}

	// LogMiner V$ access proxy — if the capture user cannot read V$LOG it lacks the
	// LogMiner grants and CDC will fail at connector start.
	var logCount int
	if err := targetDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM V$LOG").Scan(&logCount); err != nil {
		errors = append(errors, ValidationError{
			Code:     "ORACLE_LOGMINER_PRIVS_MISSING",
			Severity: "error",
			Message:  fmt.Sprintf("Capture user cannot read V$LOG — LogMiner privileges are missing: %v", err),
			Action:   "Grant the capture user: CREATE SESSION, SET CONTAINER, LOGMINING, SELECT ANY TRANSACTION, SELECT_CATALOG_ROLE, EXECUTE_CATALOG_ROLE, and SELECT on the V_$ LogMiner views.",
		})
	}

	return errors, nil
}

// ValidateTablesHavePrimaryKeys hard-blocks CDC tables without a PRIMARY KEY
// (required for upsert/delete semantics on relational destinations). Tables may be
// "owner.table" or "table"; unqualified names use defaultNamespace (or the
// connecting user's own schema via USER_*). Names are upper-cased (Oracle fold).
func (m *OracleManager) ValidateTablesHavePrimaryKeys(ctx context.Context, connectionID string, defaultNamespace string, tables []string) ([]string, error) {
	decryptedConfig, err := m.getDecryptedConnectionConfig(ctx, connectionID)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection config: %w", err)
	}
	targetDB, err := connectToOracle(decryptedConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to oracle for pk validation: %w", err)
	}
	defer targetDB.Close()

	defaultOwner := strings.ToUpper(strings.TrimSpace(defaultNamespace))

	missing := make([]string, 0)
	for _, raw := range tables {
		t := strings.TrimSpace(raw)
		if t == "" {
			continue
		}
		owner, table := splitOwnerTableOra(t)
		if table == "" {
			return nil, fmt.Errorf("invalid table identifier %q (expected owner.table or table)", t)
		}
		if owner == "" {
			owner = defaultOwner
		}

		// Existence + PK count. When the owner is known, use ALL_* (owner-qualified);
		// otherwise fall back to USER_* (the connecting user's own schema).
		var exists, pkCount int
		if owner != "" {
			if err := targetDB.QueryRowContext(ctx,
				"SELECT COUNT(*) FROM all_tables WHERE owner = :1 AND table_name = :2",
				owner, table,
			).Scan(&exists); err != nil {
				return nil, fmt.Errorf("failed to check table existence for %s.%s: %w", owner, table, err)
			}
			if exists == 0 {
				return nil, fmt.Errorf("table not found in source oracle: %s.%s", owner, table)
			}
			if err := targetDB.QueryRowContext(ctx,
				"SELECT COUNT(*) FROM all_constraints WHERE owner = :1 AND table_name = :2 AND constraint_type = 'P'",
				owner, table,
			).Scan(&pkCount); err != nil {
				return nil, fmt.Errorf("failed to check primary key for %s.%s: %w", owner, table, err)
			}
			if pkCount == 0 {
				missing = append(missing, fmt.Sprintf("%s.%s", owner, table))
			}
		} else {
			if err := targetDB.QueryRowContext(ctx,
				"SELECT COUNT(*) FROM user_tables WHERE table_name = :1", table,
			).Scan(&exists); err != nil {
				return nil, fmt.Errorf("failed to check table existence for %s: %w", table, err)
			}
			if exists == 0 {
				return nil, fmt.Errorf("table not found in source oracle: %s", table)
			}
			if err := targetDB.QueryRowContext(ctx,
				"SELECT COUNT(*) FROM user_constraints WHERE table_name = :1 AND constraint_type = 'P'", table,
			).Scan(&pkCount); err != nil {
				return nil, fmt.Errorf("failed to check primary key for %s: %w", table, err)
			}
			if pkCount == 0 {
				missing = append(missing, table)
			}
		}
	}

	return missing, nil
}

// --- Oracle CDC helpers -------------------------------------------------------

// enableTableSupplementalLogging runs `ALTER TABLE <qname> ADD SUPPLEMENTAL LOG
// DATA (ALL) COLUMNS`. Idempotent: ORA-32588 ("supplemental logging attribute …
// exists") is treated as already-provisioned success.
func enableTableSupplementalLogging(ctx context.Context, db *sql.DB, qname string) error {
	stmt := fmt.Sprintf("ALTER TABLE %s ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS", qname)
	if _, err := db.ExecContext(ctx, stmt); err != nil {
		if strings.Contains(err.Error(), "ORA-32588") {
			return nil // already has ALL-column supplemental logging
		}
		return err
	}
	log.WithField("table", qname).Info("Enabled table-level supplemental logging (ALL COLUMNS)")
	return nil
}

// getDecryptedConnectionConfig loads and decrypts a connection's config JSON.
func (m *OracleManager) getDecryptedConnectionConfig(ctx context.Context, connectionID string) (map[string]interface{}, error) {
	var configJSON string
	if err := m.db.QueryRowContext(ctx, "SELECT config FROM connections WHERE id = $1", connectionID).Scan(&configJSON); err != nil {
		return nil, fmt.Errorf("failed to get connection config: %w", err)
	}

	raw := strings.TrimSpace(configJSON)
	decryptedJSON := ""
	if strings.HasPrefix(raw, "{") || strings.HasPrefix(raw, "[") {
		decryptedJSON = raw // dev-only plaintext
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

// connectToOracle opens a verified connection using the pure-Go sijms/go-ora
// driver (registered as "oracle"; no Oracle Instant Client). It resolves the
// service name from service_name/service/database (SID is used as a fallback
// service identifier when only a SID is supplied). A wallet directory (Autonomous
// DB mTLS) is passed through when present.
func connectToOracle(config map[string]interface{}) (*sql.DB, error) {
	host, _ := config["host"].(string)
	port, _ := config["port"].(float64)
	user, _ := config["user"].(string)
	if user == "" {
		user, _ = config["username"].(string)
	}
	password, _ := config["password"].(string)
	if port == 0 {
		port = 1521
	}

	service, _ := config["service_name"].(string)
	if strings.TrimSpace(service) == "" {
		service, _ = config["service"].(string)
	}
	if strings.TrimSpace(service) == "" {
		service, _ = config["database"].(string)
	}
	if strings.TrimSpace(service) == "" {
		if sid, ok := config["sid"].(string); ok {
			service = sid // listener commonly resolves the SID as a service
		}
	}

	options := map[string]string{}
	if wallet, _ := config["wallet_location"].(string); strings.TrimSpace(wallet) != "" {
		options["WALLET"] = strings.TrimSpace(wallet)
	} else if wallet, _ := config["config_dir"].(string); strings.TrimSpace(wallet) != "" {
		options["WALLET"] = strings.TrimSpace(wallet)
	}

	connStr := go_ora.BuildUrl(host, int(port), service, user, password, options)
	db, err := sql.Open("oracle", connStr)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}
