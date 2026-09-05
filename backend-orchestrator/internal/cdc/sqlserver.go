package cdc

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	_ "github.com/microsoft/go-mssqldb"
	"github.com/rsync-ai/shared/crypto"
	log "github.com/sirupsen/logrus"
)

// SQLServerManager handles CDC resource provisioning for SQL Server.
//
// Unlike PostgreSQL (replication slot + publication) or MySQL (server_id only),
// SQL Server CDC is provisioned with two stored procedures, and — like the PG
// publication-before-slot rule — the ORDER is mandatory:
//
//  1. sys.sp_cdc_enable_db      — enables CDC at the database level (idempotent;
//     creates the cdc schema + change-tracking infrastructure). MUST run first;
//     cdc.change_tables and sys.sp_cdc_enable_table do not exist until it has.
//  2. sys.sp_cdc_enable_table   — per source table, creating a named capture
//     instance (the analog of a PG replication slot). Requires (1) to have run.
//  3. Poll sys.fn_cdc_get_max_lsn() — readiness gate; it returns NULL until the
//     capture job has scanned the log at least once. Streaming is not ready
//     until this is non-NULL (the #1 silent "no data" symptom).
//
// Teardown disables only the capture instances THIS pipeline created
// (sp_cdc_disable_table). It deliberately never calls sp_cdc_disable_db: CDC at
// the database level is shared across every pipeline on that database, so
// disabling it would silently break siblings. (A separate, ownership-aware
// reaper can disable_db once no capture instances remain — out of scope here.)
type SQLServerManager struct {
	db *sql.DB
}

// NewSQLServerManager creates a new SQL Server CDC manager.
func NewSQLServerManager(db *sql.DB) *SQLServerManager {
	return &SQLServerManager{db: db}
}

// init registers SQL Server (and its "mssql" alias, via NormalizeDBType) as a
// CDC source provider so the handler dispatches through the shared registry.
func init() {
	RegisterProvider(func(db *sql.DB) CDCSourceProvider {
		return NewSQLServerManager(db)
	}, "sqlserver")
}

// Family returns the canonical source-family key for SQL Server.
func (m *SQLServerManager) Family() string { return "sqlserver" }

// PrimaryKeyNamespace uses the schema to qualify unqualified table names for PK
// validation (SQL Server groups tables by schema, defaulting to dbo).
func (m *SQLServerManager) PrimaryKeyNamespace(defaultDB, defaultSchema string) string {
	if strings.TrimSpace(defaultSchema) == "" {
		return "dbo"
	}
	return defaultSchema
}

// ProvisionResources enables CDC at the database level and per table, then waits
// for the capture job to become ready. Idempotent: re-running for the same
// pipeline/tables re-uses existing capture instances and records via ON CONFLICT.
func (m *SQLServerManager) ProvisionResources(ctx context.Context, config CDCResourceConfig, tables []string) ([]CDCResource, error) {
	log.WithFields(log.Fields{
		"pipeline_id":   config.PipelineID,
		"connection_id": config.ConnectionID,
		"database":      config.Database,
		"table_count":   len(tables),
	}).Info("Provisioning SQL Server CDC resources")

	resources := []CDCResource{}

	decryptedConfig, err := m.getDecryptedConnectionConfig(ctx, config.ConnectionID)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection config: %w", err)
	}
	if strings.TrimSpace(config.Database) != "" {
		decryptedConfig["database"] = strings.TrimSpace(config.Database)
	}

	targetDB, err := connectToSQLServer(decryptedConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to target SQL Server: %w", err)
	}
	defer targetDB.Close()

	// STEP 1 — enable CDC at the database level (idempotent). MUST precede any
	// sp_cdc_enable_table call; cdc.change_tables does not exist before this.
	if err := ensureDatabaseCDCEnabled(ctx, targetDB); err != nil {
		return nil, err
	}

	// STEP 2 — enable a capture instance per table (idempotent per capture name).
	for _, raw := range tables {
		schema, table := splitSchemaTableSS(raw)
		if table == "" {
			return nil, fmt.Errorf("invalid table identifier %q (expected schema.table or table)", raw)
		}

		// Deterministic, per-pipeline+table capture instance name (the SQL Server
		// analog of a replication slot). Two pipelines capturing the same source
		// table get distinct capture instances (SQL Server allows up to 2 per
		// table — a third enable will fail, which the healer surfaces).
		tblCfg := config
		tblCfg.Table = fmt.Sprintf("%s.%s", schema, table)
		captureInstance := GenerateResourceName(tblCfg, "capture_instance")

		enabled, err := captureInstanceExists(ctx, targetDB, captureInstance)
		if err != nil {
			return nil, fmt.Errorf("failed to check capture instance %s: %w", captureInstance, err)
		}
		if !enabled {
			if err := enableTableCDC(ctx, targetDB, schema, table, captureInstance); err != nil {
				return nil, fmt.Errorf("failed to enable CDC for %s.%s: %w", schema, table, err)
			}
		}

		sourceTable := fmt.Sprintf("%s.%s", schema, table)
		res := CDCResource{
			PipelineID:   &config.PipelineID,
			ConnectionID: config.ConnectionID,
			SourceTable:  &sourceTable,
			ResourceType: "capture_instance",
			ResourceName: captureInstance,
			Status:       "active",
			DatabaseType: "sqlserver",
			Metadata: map[string]interface{}{
				"schema":           schema,
				"table":            table,
				"capture_instance": captureInstance,
				"provisioned_at":   time.Now().UTC().Format(time.RFC3339),
			},
			CreatedAt: time.Now(),
		}
		if err := RecordResource(ctx, m.db, res); err != nil {
			return nil, fmt.Errorf("failed to record capture instance %s: %w", captureInstance, err)
		}
		resources = append(resources, res)
	}

	// STEP 3 — readiness gate. Non-fatal: if the capture job has not produced a
	// max LSN within the budget we still succeed (Debezium's snapshot phase can
	// proceed; streaming waits), but we record readiness so callers can observe.
	// Azure SQL Database has no SQL Server Agent — capture runs on an internal
	// scheduler that commonly needs >20s to publish a first max LSN on a freshly
	// enabled DB, so the budget is 90s to avoid a false "not ready" on run one.
	ready := waitForCaptureReady(ctx, targetDB, 90*time.Second)
	if !ready {
		log.WithField("pipeline_id", config.PipelineID).
			Warn("SQL Server CDC capture not yet producing a max LSN (job may still be starting); proceeding")
	}

	log.WithFields(log.Fields{
		"pipeline_id":    config.PipelineID,
		"capture_ready":  ready,
		"resource_count": len(resources),
	}).Info("Successfully provisioned SQL Server CDC resources")

	return resources, nil
}

// CleanupResources disables the capture instances this pipeline created. It is
// idempotent/best-effort and NEVER disables database-level CDC (shared).
func (m *SQLServerManager) CleanupResources(ctx context.Context, pipelineID string) error {
	log.WithField("pipeline_id", pipelineID).Info("Cleaning up SQL Server CDC resources")

	resources, err := GetCDCResources(ctx, m.db, pipelineID)
	if err != nil {
		return fmt.Errorf("failed to get CDC resources: %w", err)
	}

	// Group SQL Server capture instances by connection so we connect once per
	// source (a pipeline has a single source, but this stays correct if not).
	byConn := map[string][]CDCResource{}
	for _, r := range resources {
		if r.DatabaseType != "sqlserver" || r.ResourceType != "capture_instance" {
			continue
		}
		byConn[r.ConnectionID] = append(byConn[r.ConnectionID], r)
	}
	if len(byConn) == 0 {
		return nil // nothing this family owns for the pipeline — no-op
	}

	for connID, group := range byConn {
		decryptedConfig, err := m.getDecryptedConnectionConfig(ctx, connID)
		if err != nil {
			log.WithError(err).WithField("connection_id", connID).
				Warn("SQL Server cleanup: failed to load connection config (marking resources deleted anyway)")
			for _, r := range group {
				_ = MarkResourceDeleted(ctx, m.db, r.ResourceName, r.ResourceType)
			}
			continue
		}
		targetDB, err := connectToSQLServer(decryptedConfig)
		if err != nil {
			log.WithError(err).WithField("connection_id", connID).
				Warn("SQL Server cleanup: failed to connect (marking resources deleted anyway)")
			for _, r := range group {
				_ = MarkResourceDeleted(ctx, m.db, r.ResourceName, r.ResourceType)
			}
			continue
		}

		for _, r := range group {
			schema, table := sqlServerResourceSchemaTable(r)
			if table != "" {
				if err := disableTableCDC(ctx, targetDB, schema, table, r.ResourceName); err != nil {
					// Best-effort: already-disabled / dropped table is not fatal.
					log.WithError(err).WithFields(log.Fields{
						"schema": schema, "table": table, "capture_instance": r.ResourceName,
					}).Warn("SQL Server cleanup: sp_cdc_disable_table failed (continuing)")
				}
			}
			if err := MarkResourceDeleted(ctx, m.db, r.ResourceName, r.ResourceType); err != nil {
				log.WithError(err).WithField("resource", r.ResourceName).Warn("Failed to mark resource as deleted")
			}
		}
		targetDB.Close()
	}

	return nil
}

// ValidatePrerequisites checks that the connection can drive SQL Server CDC:
// the login can enable CDC (sysadmin or db_owner) and, on box/MI SQL Server,
// that the SQL Server Agent is running (Azure SQL Database has no Agent and
// auto-schedules capture, so the check is skipped there).
func (m *SQLServerManager) ValidatePrerequisites(ctx context.Context, connectionID string) ([]ValidationError, error) {
	errors := []ValidationError{}

	decryptedConfig, err := m.getDecryptedConnectionConfig(ctx, connectionID)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection config: %w", err)
	}
	targetDB, err := connectToSQLServer(decryptedConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to SQL Server: %w", err)
	}
	defer targetDB.Close()

	// Privilege check: enabling CDC requires sysadmin (server) or db_owner (db).
	var isSysadmin, isDBOwner int
	_ = targetDB.QueryRowContext(ctx, "SELECT IS_SRVROLEMEMBER('sysadmin')").Scan(&isSysadmin)
	_ = targetDB.QueryRowContext(ctx, "SELECT IS_MEMBER('db_owner')").Scan(&isDBOwner)
	if isSysadmin != 1 && isDBOwner != 1 {
		errors = append(errors, ValidationError{
			Code:     "SQLSERVER_INSUFFICIENT_PRIVS",
			Severity: "error",
			Message:  "Login lacks the privilege to enable CDC (needs sysadmin or db_owner)",
			Action:   "Grant db_owner on the target database (or run the one-time sp_cdc_enable_db bootstrap as sysadmin).",
		})
	}

	// Agent check — only meaningful on box/Managed Instance SQL Server.
	// EngineEdition 5 = Azure SQL Database (no Agent; capture auto-scheduled).
	var engineEdition int
	_ = targetDB.QueryRowContext(ctx, "SELECT CAST(SERVERPROPERTY('EngineEdition') AS INT)").Scan(&engineEdition)
	if engineEdition != 5 {
		var agentStatus sql.NullString
		// sys.dm_server_services is unavailable on Azure SQL DB but present on
		// box/MI; ignore query errors and only fail on an explicit "stopped".
		if err := targetDB.QueryRowContext(ctx,
			"SELECT status_desc FROM sys.dm_server_services WHERE servicename LIKE 'SQL Server Agent%'",
		).Scan(&agentStatus); err == nil && agentStatus.Valid {
			if !strings.EqualFold(strings.TrimSpace(agentStatus.String), "Running") {
				errors = append(errors, ValidationError{
					Code:     "SQLSERVER_AGENT_NOT_RUNNING",
					Severity: "error",
					Message:  fmt.Sprintf("SQL Server Agent is '%s' but must be Running for CDC capture (fn_cdc_get_max_lsn stays NULL otherwise)", agentStatus.String),
					Action:   "Start the SQL Server Agent service on the source instance.",
				})
			}
		}
	}

	return errors, nil
}

// ValidateTablesHavePrimaryKeys hard-blocks CDC tables without a PRIMARY KEY
// (required for upsert/delete semantics on relational destinations). Tables may
// be "schema.table" or "table"; unqualified names use defaultNamespace (or dbo).
func (m *SQLServerManager) ValidateTablesHavePrimaryKeys(ctx context.Context, connectionID string, defaultNamespace string, tables []string) ([]string, error) {
	decryptedConfig, err := m.getDecryptedConnectionConfig(ctx, connectionID)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection config: %w", err)
	}
	targetDB, err := connectToSQLServer(decryptedConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to sql server for pk validation: %w", err)
	}
	defer targetDB.Close()

	defaultSchema := strings.TrimSpace(defaultNamespace)
	if defaultSchema == "" {
		defaultSchema = "dbo"
	}

	missing := make([]string, 0)
	for _, raw := range tables {
		t := strings.TrimSpace(raw)
		if t == "" {
			continue
		}
		schema, table := splitSchemaTableSS(t)
		if schema == "" {
			schema = defaultSchema
		}
		if table == "" {
			return nil, fmt.Errorf("invalid table identifier %q (expected schema.table or table)", t)
		}

		var exists int
		if err := targetDB.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM sys.tables t
			   JOIN sys.schemas s ON t.schema_id = s.schema_id
			  WHERE s.name = @schema AND t.name = @table`,
			sql.Named("schema", schema), sql.Named("table", table),
		).Scan(&exists); err != nil {
			return nil, fmt.Errorf("failed to check table existence for %s.%s: %w", schema, table, err)
		}
		if exists == 0 {
			return nil, fmt.Errorf("table not found in source sqlserver: %s.%s", schema, table)
		}

		var pkCount int
		if err := targetDB.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM sys.key_constraints kc
			   JOIN sys.tables t  ON kc.parent_object_id = t.object_id
			   JOIN sys.schemas s ON t.schema_id = s.schema_id
			  WHERE kc.type = 'PK' AND s.name = @schema AND t.name = @table`,
			sql.Named("schema", schema), sql.Named("table", table),
		).Scan(&pkCount); err != nil {
			return nil, fmt.Errorf("failed to check primary key for %s.%s: %w", schema, table, err)
		}
		if pkCount == 0 {
			missing = append(missing, fmt.Sprintf("%s.%s", schema, table))
		}
	}

	return missing, nil
}

// --- SQL Server CDC helpers ---------------------------------------------------

// ensureDatabaseCDCEnabled runs sp_cdc_enable_db if the current database does
// not yet have CDC enabled. Idempotent and safe to call from every provision.
func ensureDatabaseCDCEnabled(ctx context.Context, db *sql.DB) error {
	// is_cdc_enabled is a BIT column; go-mssqldb returns BIT as a Go bool, which
	// cannot Scan into an int. CAST to INT in T-SQL so the 0/1 int check holds
	// (mirrors the SERVERPROPERTY('EngineEdition') CAST above).
	var isEnabled int
	if err := db.QueryRowContext(ctx,
		"SELECT CAST(is_cdc_enabled AS INT) FROM sys.databases WHERE name = DB_NAME()",
	).Scan(&isEnabled); err != nil {
		return fmt.Errorf("failed to read is_cdc_enabled: %w", err)
	}
	if isEnabled == 1 {
		return nil
	}
	if _, err := db.ExecContext(ctx, "EXEC sys.sp_cdc_enable_db"); err != nil {
		return fmt.Errorf("sp_cdc_enable_db failed: %w", err)
	}
	log.Info("Enabled database-level CDC (sp_cdc_enable_db)")
	return nil
}

// captureInstanceExists reports whether a capture instance already exists.
func captureInstanceExists(ctx context.Context, db *sql.DB, captureInstance string) (bool, error) {
	var cnt int
	if err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM cdc.change_tables WHERE capture_instance = @ci",
		sql.Named("ci", captureInstance),
	).Scan(&cnt); err != nil {
		return false, err
	}
	return cnt > 0, nil
}

// enableTableCDC creates a named capture instance for a source table.
// @role_name = NULL means no gating role (any user with SELECT can read the
// change table); @supports_net_changes = 0 keeps per-row change events (Debezium
// consumes the individual change function, not net changes).
func enableTableCDC(ctx context.Context, db *sql.DB, schema, table, captureInstance string) error {
	_, err := db.ExecContext(ctx,
		`EXEC sys.sp_cdc_enable_table
		    @source_schema = @schema,
		    @source_name = @table,
		    @role_name = NULL,
		    @capture_instance = @ci,
		    @supports_net_changes = 0`,
		sql.Named("schema", schema),
		sql.Named("table", table),
		sql.Named("ci", captureInstance),
	)
	if err != nil {
		return err
	}
	log.WithFields(log.Fields{"schema": schema, "table": table, "capture_instance": captureInstance}).
		Info("Enabled table-level CDC (sp_cdc_enable_table)")
	return nil
}

// disableTableCDC removes a specific capture instance for a source table.
func disableTableCDC(ctx context.Context, db *sql.DB, schema, table, captureInstance string) error {
	_, err := db.ExecContext(ctx,
		`EXEC sys.sp_cdc_disable_table
		    @source_schema = @schema,
		    @source_name = @table,
		    @capture_instance = @ci`,
		sql.Named("schema", schema),
		sql.Named("table", table),
		sql.Named("ci", captureInstance),
	)
	return err
}

// waitForCaptureReady polls sys.fn_cdc_get_max_lsn() until it returns a non-NULL
// LSN (capture job has scanned the log) or the budget/ctx expires.
func waitForCaptureReady(ctx context.Context, db *sql.DB, budget time.Duration) bool {
	deadline := time.Now().Add(budget)
	for {
		var maxLSN []byte
		if err := db.QueryRowContext(ctx, "SELECT sys.fn_cdc_get_max_lsn()").Scan(&maxLSN); err == nil && len(maxLSN) > 0 {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(2 * time.Second):
		}
	}
}

// sqlServerResourceSchemaTable extracts schema+table for a capture_instance
// resource, preferring metadata and falling back to SourceTable ("schema.table").
func sqlServerResourceSchemaTable(r CDCResource) (string, string) {
	if r.Metadata != nil {
		schema, _ := r.Metadata["schema"].(string)
		table, _ := r.Metadata["table"].(string)
		if strings.TrimSpace(table) != "" {
			if strings.TrimSpace(schema) == "" {
				schema = "dbo"
			}
			return schema, table
		}
	}
	if r.SourceTable != nil {
		return splitSchemaTableSS(*r.SourceTable)
	}
	return "", ""
}

// splitSchemaTableSS parses "schema.table" or "table" (defaulting schema to dbo).
func splitSchemaTableSS(qualified string) (string, string) {
	q := strings.TrimSpace(qualified)
	if q == "" {
		return "", ""
	}
	if i := strings.Index(q, "."); i > 0 {
		return strings.TrimSpace(q[:i]), strings.TrimSpace(q[i+1:])
	}
	return "dbo", q
}

// getDecryptedConnectionConfig loads and decrypts a connection's config JSON.
func (m *SQLServerManager) getDecryptedConnectionConfig(ctx context.Context, connectionID string) (map[string]interface{}, error) {
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

// connectToSQLServer opens a verified connection using the microsoft/go-mssqldb
// driver. Encryption is host-aware (mirrors connectToMySQL): an explicit config
// key wins; otherwise local hosts connect without TLS and remote hosts (Azure
// SQL DB/MI, RDS) encrypt with TrustServerCertificate to avoid provider-CA
// verification failures inside the container trust store.
func connectToSQLServer(config map[string]interface{}) (*sql.DB, error) {
	host, _ := config["host"].(string)
	port, _ := config["port"].(float64)
	database, _ := config["database"].(string)
	user, _ := config["user"].(string)
	password, _ := config["password"].(string)
	if port == 0 {
		port = 1433
	}

	encrypt, trustCert := resolveSQLServerEncrypt(config, host)

	query := url.Values{}
	if strings.TrimSpace(database) != "" {
		query.Add("database", database)
	}
	query.Add("encrypt", encrypt)
	if trustCert {
		query.Add("TrustServerCertificate", "true")
	}
	query.Add("connection timeout", "30")

	u := &url.URL{
		Scheme:   "sqlserver",
		User:     url.UserPassword(user, password),
		Host:     fmt.Sprintf("%s:%d", host, int(port)),
		RawQuery: query.Encode(),
	}

	db, err := sql.Open("sqlserver", u.String())
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

// resolveSQLServerEncrypt picks the go-mssqldb "encrypt" DSN value and whether
// to trust the server certificate. An explicit ssl_mode/sslmode/encrypt config
// key wins; otherwise local→"disable", remote→"true"+trust.
func resolveSQLServerEncrypt(config map[string]interface{}, host string) (string, bool) {
	for _, key := range []string{"encrypt", "ssl_mode", "sslmode", "tls", "tls_mode"} {
		if v, ok := config[key]; ok && v != nil {
			if s := strings.TrimSpace(fmt.Sprint(v)); s != "" {
				return normalizeSQLServerEncrypt(s)
			}
		}
	}
	// Azure SQL Database / Managed Instance present a publicly-trusted (DigiCert)
	// certificate — encrypt AND verify (trust=false).
	if strings.HasSuffix(strings.ToLower(strings.TrimSpace(host)), ".database.windows.net") {
		return "true", false
	}
	if isLocalSQLServerHost(host) {
		return "disable", false
	}
	// Remote non-Azure: encrypt AND verify by default (trust=false), closing the
	// prior encrypt-without-verify MITM window. On-prem / boxed SQL Server with a
	// self-signed cert opts out explicitly with ssl_mode=require (encrypt, trust
	// the cert); ssl_mode=disable turns TLS off.
	return "true", false
}

// normalizeSQLServerEncrypt maps assorted SSL/TLS spellings onto the go-mssqldb
// "encrypt" values ("disable", "false", "true") and a trust-cert flag.
func normalizeSQLServerEncrypt(s string) (string, bool) {
	switch strings.ToLower(s) {
	case "disable", "disabled", "off", "0", "none":
		return "disable", false
	case "false":
		return "false", false
	case "prefer", "preferred", "require", "required", "skip-verify", "skip_verify", "true", "on", "1":
		return "true", true
	case "verify-ca", "verify_ca", "verify-full", "verify_full", "verify-identity", "verify_identity", "strict":
		return "true", false
	default:
		return s, false
	}
}

// isLocalSQLServerHost reports whether host is a local/docker-internal target
// that typically runs SQL Server without TLS. Delegates to IsLocalDBHost so
// every cdc DB engine shares one address-aware classifier.
func isLocalSQLServerHost(host string) bool {
	return IsLocalDBHost(host)
}
