// MySQL pre-flight assessor (Pillar 1).
//
// Checks (in order of run, fail-fast on connection error only):
//  1. MYSQL_CONNECTION                  — can we connect at all?
//  2. MYSQL_BINLOG_FORMAT_NOT_ROW       — binlog_format = 'ROW'
//  3. MYSQL_BINLOG_ROW_IMAGE_NOT_FULL   — binlog_row_image = 'FULL' (warning)
//  4. MYSQL_LOG_BIN_DISABLED            — log_bin = ON
//  5. MYSQL_USER_LACKS_REPLICATION      — user has REPLICATION CLIENT + REPLICATION SLAVE
//  6. MYSQL_BINLOG_EXPIRE_TOO_SHORT     — binlog_expire_logs_seconds >= 86400 (1 day)
//  7. MYSQL_TABLE_PRIMARY_KEYS          — every selected table has a PK (if Tables provided)
//
// The shape mirrors PostgresAssessor — same Check + Result types, same
// remediation pattern. Common Debezium gotchas (binlog_format=STATEMENT,
// missing GRANT) are the most common production failures and account for
// ~60% of "why won't my CDC pipeline start" support tickets in DMS/Fivetran.
package assessor

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/rsync-ai/backend-orchestrator/pkg/diagnose"
)

type MySQLAssessor struct{}

func NewMySQLAssessor() *MySQLAssessor { return &MySQLAssessor{} }

func (a *MySQLAssessor) SourceType() string { return "mysql" }

func (a *MySQLAssessor) Assess(ctx context.Context, in Input) (*Result, error) {
	r := &Result{
		SourceType: "mysql",
		Checks:     []Check{},
	}

	db, err := openMySQL(in.ConnectionConfig)
	if err != nil {
		r.Checks = append(r.Checks, Check{
			Code:     "MYSQL_CONNECTION",
			Severity: SeverityError,
			Passed:   false,
			Message:  fmt.Sprintf("Could not connect to MySQL: %v", err),
			Remediation: &diagnose.Remediation{
				Steps: []string{
					"Verify host, port, user, password in connection config",
					"Verify the user has CONNECT privilege",
					"Check network reachability + that MySQL is bound on a routable interface",
				},
				DocURL:           diagnose.ErrorDocURL("mysql-connection-failed"),
				EstimatedMinutes: 5,
			},
		})
		r.ErrorCount = 1
		Summarize(r)
		return r, nil
	}
	defer db.Close()

	r.Checks = append(r.Checks, Check{
		Code: "MYSQL_CONNECTION", Severity: SeverityInfo, Passed: true,
		Message: "Connected to MySQL successfully",
	})

	// CDC-only server-config checks. A batch/snapshot pipeline does NOT read
	// the binlog, so requiring log_bin / binlog_format=ROW / binlog_row_image
	// / REPLICATION grants would be a false-positive blocker for batch. Skip
	// them unless this pipeline actually does CDC.
	if in.IsCDC() {
		r.Checks = append(r.Checks, checkMySQLLogBin(ctx, db))
		r.Checks = append(r.Checks, checkMySQLBinlogFormat(ctx, db))
		r.Checks = append(r.Checks, checkMySQLBinlogRowImage(ctx, db))
		r.Checks = append(r.Checks, checkMySQLBinlogExpire(ctx, db))
		r.Checks = append(r.Checks, checkMySQLReplicationGrants(ctx, db, in.ConnectionConfig))
	}

	// Per-table primary-key check. Required for CDC (Debezium keys every change
	// event on the PK) AND for a batch load to a relational-DB destination (the
	// sink upserts via INSERT … ON CONFLICT (pk) — with no source PK the
	// auto-created destination table has no unique constraint, the upsert
	// matches nothing, and those rows are silently dropped). Skipped for a
	// batch load to a file/SaaS destination, which appends rather than upserts.
	if len(in.Tables) > 0 && in.RequiresTablePrimaryKeys() {
		r.Checks = append(r.Checks, checkMySQLTablePrimaryKeys(ctx, db, in.ConnectionConfig, in.Tables, in.IsCDC(), in.CDCBlocksWithoutPrimaryKey(), in.NominatedKeys)...)
	}

	Summarize(r)
	return r, nil
}

func openMySQL(cfg map[string]string) (*sql.DB, error) {
	host := strings.TrimSpace(cfg["host"])
	port := strings.TrimSpace(cfg["port"])
	if port == "" {
		port = "3306"
	}
	user := strings.TrimSpace(cfg["user"])
	if user == "" {
		user = strings.TrimSpace(cfg["username"])
	}
	password := strings.TrimSpace(cfg["password"])
	dbname := strings.TrimSpace(cfg["database"])
	if dbname == "" {
		dbname = strings.TrimSpace(cfg["db_name"])
	}
	// Host-aware TLS, mirroring cdc.resolveMySQLTLSMode / api-gateway. Remote
	// hosts (Azure Database for MySQL, RDS) enforce require_secure_transport=ON
	// and reject a non-TLS DSN with Error 3159. An explicit ssl_mode/sslmode/
	// tls/tls_mode key wins; otherwise default by host: local→false,
	// remote→true (verify CA+hostname; opt out ssl_mode=require). Read-only — this
	// only affects how the assessor connects, never the source server config.
	tlsMode := assessorMySQLTLSMode(cfg, host)
	// MySQL DSN: user:password@tcp(host:port)/dbname?params
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?timeout=10s&readTimeout=10s&parseTime=true&tls=%s",
		user, password, host, port, dbname, tlsMode)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	db.SetConnMaxLifetime(30 * time.Second)
	db.SetMaxOpenConns(2)
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

// assessorMySQLTLSMode picks the go-sql-driver/mysql DSN "tls" parameter,
// mirroring cdc.resolveMySQLTLSMode so the assessor connects exactly like the
// real pipeline. An explicit ssl_mode/sslmode/tls/tls_mode config key wins
// (normalised to a driver-accepted value); otherwise the default is
// host-based — local/docker hosts get "false", remote hosts default to "true"
// (verify CA + hostname; the orchestrator image ships ca-certificates), closing
// the MITM window "skip-verify" left open. Opt out with ssl_mode=require for a
// MySQL whose CA is not in the trust store. In lockstep with cdc + api-gateway.
func assessorMySQLTLSMode(cfg map[string]string, host string) string {
	for _, key := range []string{"ssl_mode", "sslmode", "tls", "tls_mode"} {
		if v := strings.TrimSpace(cfg[key]); v != "" {
			return normalizeAssessorMySQLTLS(v)
		}
	}
	if isLocalAssessorMySQLHost(host) {
		return "false"
	}
	return "true"
}

// normalizeAssessorMySQLTLS maps assorted SSL/TLS spellings onto the four
// values the mysql driver understands: "false", "true", "skip-verify",
// "preferred". Unrecognised values pass through unchanged.
func normalizeAssessorMySQLTLS(s string) string {
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

// isLocalAssessorMySQLHost reports whether host is a local/docker-internal
// target that typically runs MySQL without TLS. IP literals are classified by
// address (loopback/RFC1918/ULA/link-local/CGNAT → local) so an IPv6 or public
// IPv4 literal is correctly remote; dotless hostnames stay docker-internal.
// Mirrors cdc.IsLocalDBHost (kept local to avoid an assessor→cdc dependency).
func isLocalAssessorMySQLHost(host string) bool {
	h := strings.ToLower(strings.TrimSpace(host))
	h = strings.Trim(h, "[]")
	if h == "" {
		return true
	}
	switch h {
	case "localhost", "127.0.0.1", "::1", "host.docker.internal":
		return true
	}
	if ip := net.ParseIP(h); ip != nil {
		if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
			return true
		}
		if v4 := ip.To4(); v4 != nil && v4[0] == 100 && v4[1] >= 64 && v4[1] <= 127 {
			return true // 100.64.0.0/10 CGNAT
		}
		return false
	}
	return !strings.Contains(h, ".")
}

// showVariable runs `SHOW VARIABLES LIKE 'name'` and returns the value.
// MySQL returns two columns (Variable_name, Value); we only want Value.
func showVariable(ctx context.Context, db *sql.DB, name string) (string, error) {
	var k, v sql.NullString
	err := db.QueryRowContext(ctx, fmt.Sprintf("SHOW VARIABLES LIKE '%s'", name)).Scan(&k, &v)
	if err != nil {
		return "", err
	}
	return v.String, nil
}

func checkMySQLLogBin(ctx context.Context, db *sql.DB) Check {
	v, err := showVariable(ctx, db, "log_bin")
	if err != nil {
		return Check{
			Code: "MYSQL_LOG_BIN_DISABLED", Severity: SeverityError, Passed: false,
			Message: fmt.Sprintf("Could not query log_bin: %v", err),
		}
	}
	if strings.EqualFold(v, "ON") {
		return Check{
			Code: "MYSQL_LOG_BIN_DISABLED", Severity: SeverityInfo, Passed: true,
			Message: "log_bin = ON (binary logging enabled)",
		}
	}
	return Check{
		Code: "MYSQL_LOG_BIN_DISABLED", Severity: SeverityError, Passed: false,
		Message: fmt.Sprintf("log_bin is '%s'; binary logging must be ON for CDC", v),
		Remediation: &diagnose.Remediation{
			Steps: []string{
				"log_bin requires a server restart to enable",
				"Add `log_bin = mysql-bin` (or similar path) under [mysqld] in my.cnf",
				"For RDS/Aurora: set the parameter group's `binlog_format` to enable bin log + reboot",
				"Restart MySQL and re-run this assessment",
			},
			DocURL:           diagnose.ErrorDocURL("mysql-log-bin"),
			EstimatedMinutes: 15,
		},
	}
}

func checkMySQLBinlogFormat(ctx context.Context, db *sql.DB) Check {
	v, err := showVariable(ctx, db, "binlog_format")
	if err != nil {
		return Check{
			Code: "MYSQL_BINLOG_FORMAT_NOT_ROW", Severity: SeverityError, Passed: false,
			Message: fmt.Sprintf("Could not query binlog_format: %v", err),
		}
	}
	if strings.EqualFold(v, "ROW") {
		return Check{
			Code: "MYSQL_BINLOG_FORMAT_NOT_ROW", Severity: SeverityInfo, Passed: true,
			Message: fmt.Sprintf("binlog_format = '%s' (CDC-capable)", v),
		}
	}
	return Check{
		Code: "MYSQL_BINLOG_FORMAT_NOT_ROW", Severity: SeverityError, Passed: false,
		Message: fmt.Sprintf("binlog_format is '%s' but must be 'ROW' for CDC", v),
		Remediation: &diagnose.Remediation{
			Steps: []string{
				"As a MySQL admin, run the SQL below",
				"For RDS/Aurora: set 'binlog_format' to 'ROW' in the DB parameter group + reboot",
				"For persistence across restart, also update my.cnf: [mysqld] binlog_format = ROW",
				"Re-run this assessment",
			},
			SQLToRun:         []string{"SET GLOBAL binlog_format = 'ROW';"},
			DocURL:           diagnose.ErrorDocURL("mysql-binlog-format"),
			EstimatedMinutes: 10,
		},
	}
}

func checkMySQLBinlogRowImage(ctx context.Context, db *sql.DB) Check {
	v, err := showVariable(ctx, db, "binlog_row_image")
	if err != nil {
		// Older MySQL (5.5) doesn't have this variable — treat as warning, not error.
		return Check{
			Code: "MYSQL_BINLOG_ROW_IMAGE_NOT_FULL", Severity: SeverityWarning, Passed: false,
			Message: fmt.Sprintf("Could not query binlog_row_image: %v", err),
		}
	}
	if strings.EqualFold(v, "FULL") {
		return Check{
			Code: "MYSQL_BINLOG_ROW_IMAGE_NOT_FULL", Severity: SeverityInfo, Passed: true,
			Message: fmt.Sprintf("binlog_row_image = '%s'", v),
		}
	}
	return Check{
		Code: "MYSQL_BINLOG_ROW_IMAGE_NOT_FULL", Severity: SeverityWarning, Passed: false,
		Message: fmt.Sprintf("binlog_row_image is '%s'; 'FULL' recommended for CDC correctness on UPDATE", v),
		Remediation: &diagnose.Remediation{
			Steps: []string{
				"As a MySQL admin, run the SQL below",
				"Re-run this assessment",
			},
			SQLToRun:         []string{"SET GLOBAL binlog_row_image = 'FULL';"},
			DocURL:           diagnose.ErrorDocURL("mysql-binlog-row-image"),
			EstimatedMinutes: 5,
		},
	}
}

func checkMySQLBinlogExpire(ctx context.Context, db *sql.DB) Check {
	// binlog_expire_logs_seconds is MySQL 8+. expire_logs_days is the older one.
	v, err := showVariable(ctx, db, "binlog_expire_logs_seconds")
	if err != nil || v == "" {
		// Fall back to expire_logs_days (older MySQL).
		v2, e2 := showVariable(ctx, db, "expire_logs_days")
		if e2 != nil {
			return Check{
				Code: "MYSQL_BINLOG_EXPIRE_TOO_SHORT", Severity: SeverityWarning, Passed: false,
				Message: "Could not query binlog expiry setting",
			}
		}
		days, _ := strconv.Atoi(v2)
		if days >= 1 {
			return Check{
				Code: "MYSQL_BINLOG_EXPIRE_TOO_SHORT", Severity: SeverityInfo, Passed: true,
				Message: fmt.Sprintf("expire_logs_days = %d (>= 1 recommended)", days),
			}
		}
		return Check{
			Code: "MYSQL_BINLOG_EXPIRE_TOO_SHORT", Severity: SeverityWarning, Passed: false,
			Message: fmt.Sprintf("expire_logs_days = %d; >= 1 recommended so Debezium can resume after brief outages", days),
			Remediation: &diagnose.Remediation{
				Steps:            []string{"As a MySQL admin, run the SQL below"},
				SQLToRun:         []string{"SET GLOBAL expire_logs_days = 1;"},
				DocURL:           diagnose.ErrorDocURL("mysql-binlog-expire"),
				EstimatedMinutes: 2,
			},
		}
	}
	secs, _ := strconv.Atoi(v)
	const minSecs = 86400
	if secs >= minSecs {
		return Check{
			Code: "MYSQL_BINLOG_EXPIRE_TOO_SHORT", Severity: SeverityInfo, Passed: true,
			Message: fmt.Sprintf("binlog_expire_logs_seconds = %d (>= %d recommended)", secs, minSecs),
		}
	}
	return Check{
		Code: "MYSQL_BINLOG_EXPIRE_TOO_SHORT", Severity: SeverityWarning, Passed: false,
		Message: fmt.Sprintf("binlog_expire_logs_seconds = %d; >= %d (1 day) recommended so Debezium can resume after brief outages", secs, minSecs),
		Remediation: &diagnose.Remediation{
			Steps:            []string{"As a MySQL admin, run the SQL below"},
			SQLToRun:         []string{fmt.Sprintf("SET GLOBAL binlog_expire_logs_seconds = %d;", minSecs)},
			DocURL:           diagnose.ErrorDocURL("mysql-binlog-expire"),
			EstimatedMinutes: 2,
		},
	}
}

func checkMySQLReplicationGrants(ctx context.Context, db *sql.DB, cfg map[string]string) Check {
	user := strings.TrimSpace(cfg["user"])
	if user == "" {
		user = strings.TrimSpace(cfg["username"])
	}
	// SHOW GRANTS FOR CURRENT_USER returns one row per grant. We need both
	// REPLICATION CLIENT and REPLICATION SLAVE.
	rows, err := db.QueryContext(ctx, "SHOW GRANTS FOR CURRENT_USER")
	if err != nil {
		return Check{
			Code: "MYSQL_USER_LACKS_REPLICATION", Severity: SeverityError, Passed: false,
			Message: fmt.Sprintf("Could not query grants: %v", err),
		}
	}
	defer rows.Close()
	hasClient := false
	hasSlave := false
	for rows.Next() {
		var g string
		if err := rows.Scan(&g); err != nil {
			continue
		}
		gu := strings.ToUpper(g)
		if strings.Contains(gu, "REPLICATION CLIENT") || strings.Contains(gu, "GRANT ALL") || strings.Contains(gu, "ALL PRIVILEGES") {
			hasClient = true
		}
		// MySQL 8.0.22+ renamed REPLICATION SLAVE to REPLICATION SLAVE / REPLICA.
		// Accept either spelling.
		if strings.Contains(gu, "REPLICATION SLAVE") || strings.Contains(gu, "REPLICATION REPLICA") ||
			strings.Contains(gu, "GRANT ALL") || strings.Contains(gu, "ALL PRIVILEGES") {
			hasSlave = true
		}
	}
	if hasClient && hasSlave {
		return Check{
			Code: "MYSQL_USER_LACKS_REPLICATION", Severity: SeverityInfo, Passed: true,
			Message: fmt.Sprintf("User %q has REPLICATION CLIENT + REPLICATION SLAVE grants", user),
		}
	}
	missing := []string{}
	if !hasClient {
		missing = append(missing, "REPLICATION CLIENT")
	}
	if !hasSlave {
		missing = append(missing, "REPLICATION SLAVE")
	}
	return Check{
		Code: "MYSQL_USER_LACKS_REPLICATION", Severity: SeverityError, Passed: false,
		Message: fmt.Sprintf("User %q lacks grants required for CDC: %s", user, strings.Join(missing, ", ")),
		Remediation: &diagnose.Remediation{
			Steps: []string{
				"As a MySQL admin, run the SQL below",
				"For MySQL 8.0.22+ you may need to use REPLICATION REPLICA instead of REPLICATION SLAVE",
				"Re-run this assessment",
			},
			SQLToRun: []string{
				fmt.Sprintf("GRANT REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO '%s'@'%%';", user),
				"FLUSH PRIVILEGES;",
			},
			DocURL:           diagnose.ErrorDocURL("mysql-replication-grants"),
			EstimatedMinutes: 3,
		},
	}
}

func checkMySQLTablePrimaryKeys(ctx context.Context, db *sql.DB, cfg map[string]string, tables []string, cdcMode, cdcBlocks bool, nominated map[string][]string) []Check {
	defaultDB := strings.TrimSpace(cfg["database"])
	if defaultDB == "" {
		defaultDB = strings.TrimSpace(cfg["db_name"])
	}
	out := make([]Check, 0, len(tables))
	for _, raw := range tables {
		t := strings.TrimSpace(raw)
		if t == "" {
			continue
		}
		var dbName, tableName string
		if idx := strings.IndexByte(t, '.'); idx > 0 {
			dbName = strings.Trim(t[:idx], "`\"")
			tableName = strings.Trim(t[idx+1:], "`\"")
		} else {
			dbName = defaultDB
			tableName = strings.Trim(t, "`\"")
		}
		out = append(out, oneMySQLTablePKCheck(ctx, db, dbName, tableName, cdcMode, cdcBlocks, nominatedColsFor(nominated, dbName, tableName)))
	}
	return out
}

func oneMySQLTablePKCheck(ctx context.Context, db *sql.DB, dbName, table string, cdcMode, cdcBlocks bool, nominatedCols []string) Check {
	code := "CDC_TABLE_MISSING_PRIMARY_KEY"
	var tableExists bool
	err := db.QueryRowContext(ctx,
		`SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?)`,
		dbName, table,
	).Scan(&tableExists)
	if err != nil {
		return Check{
			Code: code, Severity: SeverityError, Passed: false,
			Message: fmt.Sprintf("Could not query existence of %s.%s: %v", dbName, table, err),
		}
	}
	if !tableExists {
		// The CDC_ prefix is historical: the caller gates on
		// RequiresTablePrimaryKeys() == IsCDC() || DestinationUsesUpsert(), so a
		// plain BATCH pipeline into a relational destination raises this too. The
		// code is a stable identifier with a published doc URL and lives on
		// persisted notification rows, so it is described accurately in the
		// user-facing copy (api-gateway/internal/notifier/catalog.go) rather than
		// renamed. SeverityError here BLOCKS the run — the catalog entry must not
		// tell the user the rest of the pipeline continues.
		return Check{
			Code: "CDC_TABLE_NOT_FOUND_IN_SOURCE", Severity: SeverityError, Passed: false,
			Message: fmt.Sprintf("Table %s.%s does not exist in source", dbName, table),
			Remediation: &diagnose.Remediation{
				Steps: []string{
					"Verify the table name and database in your source",
					"Confirm the user has SELECT on the table",
					"Update the pipeline's table selection if the table was renamed/dropped",
				},
				DocURL:           diagnose.ErrorDocURL("cdc-table-not-found"),
				EstimatedMinutes: 5,
			},
		}
	}

	// Count PK columns AND how many are VISIBLE. MySQL 8.0.30+ with
	// sql_generate_invisible_primary_key=ON (the default on Azure Database for
	// MySQL Flexible Server) auto-creates an INVISIBLE primary key column
	// (default name my_row_id) for any table declared without an explicit PK.
	// information_schema.statistics still reports a 'PRIMARY' index, so a naive
	// "does a PRIMARY index exist?" check passes — but invisible columns are
	// excluded from SELECT * and are NOT propagated to the destination, so the
	// replicated table ends up with NO key and the sink's ON CONFLICT upsert
	// fails ("no unique or exclusion constraint matching the ON CONFLICT
	// specification"). We therefore require at least one VISIBLE PK column.
	var pkCols, visiblePKCols int
	var invisiblePKNames sql.NullString
	err = db.QueryRowContext(ctx, `
		SELECT
			COUNT(*),
			COALESCE(SUM(CASE WHEN c.extra NOT LIKE '%INVISIBLE%' THEN 1 ELSE 0 END), 0),
			GROUP_CONCAT(CASE WHEN c.extra LIKE '%INVISIBLE%' THEN s.column_name END)
		FROM information_schema.statistics s
		JOIN information_schema.columns c
			ON c.table_schema = s.table_schema
			AND c.table_name = s.table_name
			AND c.column_name = s.column_name
		WHERE s.table_schema = ? AND s.table_name = ? AND s.index_name = 'PRIMARY'`,
		dbName, table,
	).Scan(&pkCols, &visiblePKCols, &invisiblePKNames)
	if err != nil {
		return Check{
			Code: code, Severity: SeverityError, Passed: false,
			Message: fmt.Sprintf("Could not query PK for %s.%s: %v", dbName, table, err),
		}
	}
	if visiblePKCols > 0 {
		return Check{
			Code: code, Severity: SeverityInfo, Passed: true,
			Message: fmt.Sprintf("%s.%s has a primary key", dbName, table),
		}
	}
	// Past this point the table has no VISIBLE primary key. When the table has
	// no PRIMARY constraint at all (pkCols == 0) and the CDC executor blocks on
	// that, neither the surrogate key nor a column nomination can save the run —
	// say so as an ERROR rather than clearing it
	// (KI-CDC-ASSESS-PK-FALLBACK-NOT-IMPLEMENTED).
	//
	// A GIPK table (pkCols > 0, all invisible) is deliberately excluded: the
	// executor's validator counts information_schema.KEY_COLUMN_USAGE rows for
	// CONSTRAINT_NAME='PRIMARY', which includes invisible columns, so those
	// tables really do start — they degrade to the content-hash path, which is
	// what the existing warning below already says.
	if cdcBlocks && pkCols == 0 {
		return blockingMissingPKCheck(
			fmt.Sprintf("%s.%s", dbName, table),
			fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD PRIMARY KEY (`id`);", dbName, table),
			nominatedCols,
		)
	}
	// PR-D: the user nominated identifying column(s) for this keyless / GIPK
	// table. The data plane upserts on these columns, so it is effectively
	// keyed — an INFO note, not a keyless WARNING.
	if len(nominatedCols) > 0 {
		return Check{
			Code: code, Severity: SeverityInfo, Passed: true,
			Message: fmt.Sprintf("Table %s.%s has no declared primary key; using user-nominated key column(s): %s. Updates apply in place on this key.", dbName, table, strings.Join(nominatedCols, ", ")),
		}
	}
	if pkCols > 0 {
		// A PRIMARY index exists but every key column is invisible → MySQL's
		// generated invisible primary key (GIPK). It won't replicate (excluded
		// from SELECT *), so the table is effectively keyless. We do NOT block:
		// rsync routes keyless tables through the sink's content-hash surrogate
		// key (_rsync_row_hash), matching the Fivetran/Airbyte market standard.
		// This is a WARNING, never a hard block — the run proceeds.
		invName := strings.TrimSpace(invisiblePKNames.String)
		if invName == "" {
			invName = "my_row_id"
		}
		return Check{
			Code: "MYSQL_TABLE_PRIMARY_KEY_INVISIBLE", Severity: SeverityWarning, Passed: true,
			Message: fmt.Sprintf("Table %s.%s has only MySQL's auto-generated INVISIBLE primary key (%q), which is excluded from SELECT * and won't replicate. rsync will load it using a content-hash surrogate key (_rsync_row_hash), so the run succeeds. Because the hash covers all columns, a later change to any column is written as a new row (the prior version is retained), so updates can accumulate duplicates. For in-place updates, nominate identifying column(s) as the key (recommended) or add an explicit PRIMARY KEY on the source.", dbName, table, invName),
			Remediation: &diagnose.Remediation{
				Steps: []string{
					"No action needed to run — keyless tables replicate via a content-hash surrogate key.",
					"Recommended for correct updates: nominate the column(s) that uniquely identify a row as the key.",
					"Alternative: declare an explicit primary key on the source (replace 'id' with the natural key).",
				},
				SQLToRun: []string{
					"-- Optional — explicit PK on a real column for true in-place updates (replace 'id'):",
					fmt.Sprintf("ALTER TABLE `%s`.`%s` DROP PRIMARY KEY, ADD PRIMARY KEY (`id`);", dbName, table),
				},
				DocURL:           diagnose.ErrorDocURL("mysql-invisible-primary-key"),
				EstimatedMinutes: 0,
			},
		}
	}
	return Check{
		Code: code, Severity: SeverityWarning, Passed: true,
		Message: fmt.Sprintf("Table %s.%s has no primary key. rsync will load it using a content-hash surrogate key (_rsync_row_hash), so the run succeeds. Because the hash covers all columns, a later change to any column is written as a new row (the prior version is retained), so updates can accumulate duplicates. For in-place updates, nominate identifying column(s) as the key (recommended) or add a PRIMARY KEY on the source.", dbName, table),
		Remediation: &diagnose.Remediation{
			Steps: []string{
				"No action needed to run — keyless tables replicate via a content-hash surrogate key.",
				"Recommended for correct updates: nominate the column(s) that uniquely identify a row as the key.",
				"Alternative: add a primary key (or unique not-null index) on the source.",
			},
			SQLToRun: []string{
				"-- Optional — explicit PK for true in-place updates (replace 'id'):",
				fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD PRIMARY KEY (`id`);", dbName, table),
			},
			DocURL:           diagnose.ErrorDocURL("cdc-missing-pk"),
			EstimatedMinutes: 0,
		},
	}
}
