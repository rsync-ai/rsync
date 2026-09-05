// PostgreSQL pre-flight assessor (Pillar 1).
//
// Checks (in order of run, fail-fast on connection error only):
//  1. POSTGRES_CONNECTION                — can we connect at all?
//  2. POSTGRES_WAL_LEVEL                 — wal_level = 'logical'
//  3. POSTGRES_MAX_REPLICATION_SLOTS     — max_replication_slots >= 10
//  4. POSTGRES_MAX_WAL_SENDERS           — max_wal_senders >= 10
//  5. POSTGRES_REPLICATION_PRIVILEGE     — user has REPLICATION role attribute
//  6. POSTGRES_SCHEMA_VISIBLE            — user can see the configured schema
//  7. POSTGRES_TABLE_PRIMARY_KEYS        — every selected table has a PK (if Tables provided)
//
// Each check returns a Check struct. Failed checks include a Remediation
// with copy-pasteable SQL the user can run with admin privileges to fix.
//
// Connection is opened once per Assess() and reused for all checks.
package assessor

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	_ "github.com/rsync-ai/shared/pgdriver"

	"github.com/rsync-ai/backend-orchestrator/internal/cdc"
	"github.com/rsync-ai/backend-orchestrator/pkg/diagnose"
)

// PostgresAssessor implements SourceAssessor for PostgreSQL (and family —
// CockroachDB, Aurora PG, AlloyDB, Neon, Supabase all share the same
// pgoutput-based CDC primitives).
type PostgresAssessor struct{}

func NewPostgresAssessor() *PostgresAssessor { return &PostgresAssessor{} }

func (a *PostgresAssessor) SourceType() string { return "postgresql" }

func (a *PostgresAssessor) Assess(ctx context.Context, in Input) (*Result, error) {
	r := &Result{
		SourceType: "postgresql",
		Checks:     []Check{},
	}

	db, err := openPostgres(in.ConnectionConfig)
	if err != nil {
		r.Checks = append(r.Checks, Check{
			Code:     "POSTGRES_CONNECTION",
			Severity: SeverityError,
			Passed:   false,
			Message:  fmt.Sprintf("Could not connect to PostgreSQL: %v", err),
			Remediation: &diagnose.Remediation{
				Steps: []string{
					"Verify host, port, user, password in connection config",
					"Verify the database user has CONNECT privilege",
					"Check network reachability from rsync to PostgreSQL",
				},
				DocURL:           diagnose.ErrorDocURL("postgres-connection-failed"),
				EstimatedMinutes: 5,
			},
		})
		r.ErrorCount = 1
		Summarize(r)
		// Return result, NOT err — err is reserved for "assessment crashed"
		// like a panic. Connection failure is itself a check finding.
		return r, nil
	}
	defer db.Close()

	// Successful connection is itself a passing check — surface it so the
	// user sees a green checkmark for "connectivity OK".
	r.Checks = append(r.Checks, Check{
		Code:     "POSTGRES_CONNECTION",
		Severity: SeverityInfo,
		Passed:   true,
		Message:  "Connected to PostgreSQL successfully",
	})

	// Schema visibility is needed for both CDC and batch — always check.
	r.Checks = append(r.Checks, checkPostgresSchemaVisible(ctx, db, in.ConnectionConfig))

	// CDC-only server-config checks. A batch/snapshot pipeline does NOT replay
	// the WAL, so requiring wal_level=logical / replication slots / REPLICATION
	// privilege would be a false-positive blocker for batch.
	if in.IsCDC() {
		r.Checks = append(r.Checks, checkPostgresWALLevel(ctx, db))
		r.Checks = append(r.Checks, checkPostgresMaxReplicationSlots(ctx, db))
		r.Checks = append(r.Checks, checkPostgresMaxWALSenders(ctx, db))
		r.Checks = append(r.Checks, checkPostgresReplicationPrivilege(ctx, db, in.ConnectionConfig))
	}

	// Per-table primary-key check. Required for CDC (Debezium keys every change
	// event on the PK) AND for a batch load to a relational-DB destination (the
	// sink upserts via INSERT … ON CONFLICT (pk) — with no source PK the
	// auto-created destination table has no unique constraint, the upsert
	// matches nothing, and those rows are silently dropped). Skipped for a
	// batch load to a file/SaaS destination, which appends rather than upserts.
	if len(in.Tables) > 0 && in.RequiresTablePrimaryKeys() {
		r.Checks = append(r.Checks, checkPostgresTablePrimaryKeys(ctx, db, in.ConnectionConfig, in.Tables, in.IsCDC(), in.CDCBlocksWithoutPrimaryKey(), in.NominatedKeys)...)
	}

	Summarize(r)
	return r, nil
}

// resolveAssessorPostgresSSLMode picks the DSN "sslmode" for the read-only
// readiness probe. Stored PG connections carry no sslmode key, so this must
// supply a host-aware default. Delegates to the prod-proven
// cdc.ResolvePostgresSSLMode so the probe matches exactly how the executor/CDC
// path connects: an explicit ssl_mode/sslmode config value wins (normalised,
// folding "prefer"/"allow" → "require"); otherwise the default is host-aware —
// "disable" for local/docker hosts, "verify-full" for any remote (managed)
// server. The probe dials a user-supplied address, so the fold is a security
// guard under pgx, which — unlike the lib/pq this replaced — accepts "prefer"
// and would silently fall back to cleartext. See normalizePostgresSSLMode.
func resolveAssessorPostgresSSLMode(cfg map[string]string, host string) string {
	return cdc.ResolvePostgresSSLMode(
		map[string]interface{}{"sslmode": cfg["sslmode"], "ssl_mode": cfg["ssl_mode"]},
		host,
	)
}

// openPostgres builds a connection from a connection-config map.
// Tolerates both "database"/"db_name"/"dbname" key spellings since
// different config sources use different conventions.
func openPostgres(cfg map[string]string) (*sql.DB, error) {
	host := strings.TrimSpace(cfg["host"])
	port := strings.TrimSpace(cfg["port"])
	user := strings.TrimSpace(cfg["user"])
	if user == "" {
		user = strings.TrimSpace(cfg["username"])
	}
	password := strings.TrimSpace(cfg["password"])
	dbname := strings.TrimSpace(cfg["database"])
	if dbname == "" {
		dbname = strings.TrimSpace(cfg["db_name"])
	}
	if dbname == "" {
		dbname = strings.TrimSpace(cfg["dbname"])
	}
	sslmode := resolveAssessorPostgresSSLMode(cfg, host)
	if port == "" {
		port = "5432"
	}
	u := url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(user, password),
		Host:   fmt.Sprintf("%s:%s", host, port),
		Path:   "/" + dbname,
	}
	q := url.Values{}
	q.Set("sslmode", sslmode)
	q.Set("connect_timeout", "10")
	u.RawQuery = q.Encode()
	db, err := sql.Open("postgres", u.String())
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

func checkPostgresWALLevel(ctx context.Context, db *sql.DB) Check {
	var walLevel string
	err := db.QueryRowContext(ctx, "SHOW wal_level").Scan(&walLevel)
	if err != nil {
		return Check{
			Code: "POSTGRES_WAL_LEVEL_NOT_LOGICAL", Severity: SeverityError, Passed: false,
			Message: fmt.Sprintf("Could not query wal_level: %v", err),
		}
	}
	if strings.EqualFold(walLevel, "logical") {
		return Check{
			Code: "POSTGRES_WAL_LEVEL_NOT_LOGICAL", Severity: SeverityInfo, Passed: true,
			Message: fmt.Sprintf("wal_level = '%s' (CDC-capable)", walLevel),
		}
	}
	return Check{
		Code: "POSTGRES_WAL_LEVEL_NOT_LOGICAL", Severity: SeverityError, Passed: false,
		Message: fmt.Sprintf("wal_level is '%s' but must be 'logical' for CDC", walLevel),
		Remediation: &diagnose.Remediation{
			Steps: []string{
				"As a PostgreSQL superuser, run the SQL below",
				"Restart PostgreSQL for the setting to take effect",
				"Re-run this assessment",
			},
			SQLToRun: []string{
				"ALTER SYSTEM SET wal_level = 'logical';",
				"-- Then restart PostgreSQL (sudo systemctl restart postgresql, or your platform's equivalent)",
			},
			DocURL:           diagnose.ErrorDocURL("postgres-wal-level"),
			EstimatedMinutes: 10,
		},
	}
}

func checkPostgresMaxReplicationSlots(ctx context.Context, db *sql.DB) Check {
	const minSlots = 10
	var v string
	err := db.QueryRowContext(ctx, "SHOW max_replication_slots").Scan(&v)
	if err != nil {
		return Check{
			Code: "POSTGRES_MAX_REPLICATION_SLOTS_LOW", Severity: SeverityWarning, Passed: false,
			Message: fmt.Sprintf("Could not query max_replication_slots: %v", err),
		}
	}
	n, _ := strconv.Atoi(v)
	if n >= minSlots {
		return Check{
			Code: "POSTGRES_MAX_REPLICATION_SLOTS_LOW", Severity: SeverityInfo, Passed: true,
			Message: fmt.Sprintf("max_replication_slots = %d (>= %d recommended)", n, minSlots),
		}
	}
	return Check{
		Code: "POSTGRES_MAX_REPLICATION_SLOTS_LOW", Severity: SeverityWarning, Passed: false,
		Message: fmt.Sprintf("max_replication_slots is %d; >= %d recommended to support multiple CDC pipelines", n, minSlots),
		Remediation: &diagnose.Remediation{
			Steps: []string{
				"As a PostgreSQL superuser, run the SQL below",
				"Restart PostgreSQL for the setting to take effect",
			},
			SQLToRun:         []string{fmt.Sprintf("ALTER SYSTEM SET max_replication_slots = %d;", minSlots)},
			DocURL:           diagnose.ErrorDocURL("postgres-max-replication-slots"),
			EstimatedMinutes: 10,
		},
	}
}

func checkPostgresMaxWALSenders(ctx context.Context, db *sql.DB) Check {
	const minSenders = 10
	var v string
	err := db.QueryRowContext(ctx, "SHOW max_wal_senders").Scan(&v)
	if err != nil {
		return Check{
			Code: "POSTGRES_MAX_WAL_SENDERS_LOW", Severity: SeverityWarning, Passed: false,
			Message: fmt.Sprintf("Could not query max_wal_senders: %v", err),
		}
	}
	n, _ := strconv.Atoi(v)
	if n >= minSenders {
		return Check{
			Code: "POSTGRES_MAX_WAL_SENDERS_LOW", Severity: SeverityInfo, Passed: true,
			Message: fmt.Sprintf("max_wal_senders = %d (>= %d recommended)", n, minSenders),
		}
	}
	return Check{
		Code: "POSTGRES_MAX_WAL_SENDERS_LOW", Severity: SeverityWarning, Passed: false,
		Message: fmt.Sprintf("max_wal_senders is %d; >= %d recommended", n, minSenders),
		Remediation: &diagnose.Remediation{
			Steps: []string{
				"As a PostgreSQL superuser, run the SQL below",
				"Restart PostgreSQL for the setting to take effect",
			},
			SQLToRun:         []string{fmt.Sprintf("ALTER SYSTEM SET max_wal_senders = %d;", minSenders)},
			DocURL:           diagnose.ErrorDocURL("postgres-max-wal-senders"),
			EstimatedMinutes: 10,
		},
	}
}

func checkPostgresReplicationPrivilege(ctx context.Context, db *sql.DB, cfg map[string]string) Check {
	user := strings.TrimSpace(cfg["user"])
	if user == "" {
		user = strings.TrimSpace(cfg["username"])
	}
	var hasRepl sql.NullBool
	err := db.QueryRowContext(ctx,
		"SELECT rolreplication FROM pg_roles WHERE rolname = $1",
		user,
	).Scan(&hasRepl)
	if err != nil {
		return Check{
			Code: "POSTGRES_USER_LACKS_REPLICATION", Severity: SeverityError, Passed: false,
			Message: fmt.Sprintf("Could not query rolreplication for user %q: %v", user, err),
		}
	}
	if hasRepl.Valid && hasRepl.Bool {
		return Check{
			Code: "POSTGRES_USER_LACKS_REPLICATION", Severity: SeverityInfo, Passed: true,
			Message: fmt.Sprintf("User %q has REPLICATION privilege", user),
		}
	}
	return Check{
		Code: "POSTGRES_USER_LACKS_REPLICATION", Severity: SeverityError, Passed: false,
		Message: fmt.Sprintf("User %q lacks the REPLICATION role attribute; CDC requires it", user),
		Remediation: &diagnose.Remediation{
			Steps: []string{
				"As a PostgreSQL superuser, run the SQL below",
				"Re-run this assessment",
			},
			SQLToRun:         []string{fmt.Sprintf("ALTER USER %q WITH REPLICATION;", user)},
			DocURL:           diagnose.ErrorDocURL("postgres-user-replication"),
			EstimatedMinutes: 2,
		},
	}
}

func checkPostgresSchemaVisible(ctx context.Context, db *sql.DB, cfg map[string]string) Check {
	schema := strings.TrimSpace(cfg["schema"])
	if schema == "" {
		schema = "public"
	}
	var exists bool
	err := db.QueryRowContext(ctx,
		"SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)",
		schema,
	).Scan(&exists)
	if err != nil {
		return Check{
			Code: "POSTGRES_SCHEMA_NOT_VISIBLE", Severity: SeverityError, Passed: false,
			Message: fmt.Sprintf("Could not query schema visibility: %v", err),
		}
	}
	if exists {
		return Check{
			Code: "POSTGRES_SCHEMA_NOT_VISIBLE", Severity: SeverityInfo, Passed: true,
			Message: fmt.Sprintf("Schema %q is visible to the connection user", schema),
		}
	}
	return Check{
		Code: "POSTGRES_SCHEMA_NOT_VISIBLE", Severity: SeverityError, Passed: false,
		Message: fmt.Sprintf("Schema %q does not exist or is not visible to the connection user", schema),
		Remediation: &diagnose.Remediation{
			Steps: []string{
				"Verify the schema name in your connection config",
				"Grant USAGE on the schema to the connection user",
				"Re-run this assessment",
			},
			SQLToRun:         []string{fmt.Sprintf("GRANT USAGE ON SCHEMA %q TO <connection_user>;", schema)},
			DocURL:           diagnose.ErrorDocURL("postgres-schema-not-visible"),
			EstimatedMinutes: 5,
		},
	}
}

// checkPostgresTablePrimaryKeys returns one check per selected table.
// A table without a PK fails its own check; tables that don't exist also
// fail (with a more specific code).
func checkPostgresTablePrimaryKeys(ctx context.Context, db *sql.DB, cfg map[string]string, tables []string, cdcMode, cdcBlocks bool, nominated map[string][]string) []Check {
	defaultSchema := strings.TrimSpace(cfg["schema"])
	if defaultSchema == "" {
		defaultSchema = "public"
	}
	out := make([]Check, 0, len(tables))
	for _, raw := range tables {
		t := strings.TrimSpace(raw)
		if t == "" {
			continue
		}
		var schemaName, tableName string
		if idx := strings.IndexByte(t, '.'); idx > 0 {
			schemaName = strings.Trim(t[:idx], `"`)
			tableName = strings.Trim(t[idx+1:], `"`)
		} else {
			schemaName = defaultSchema
			tableName = strings.Trim(t, `"`)
		}
		out = append(out, oneTablePKCheck(ctx, db, schemaName, tableName, cdcMode, cdcBlocks, nominatedColsFor(nominated, schemaName, tableName)))
	}
	return out
}

func oneTablePKCheck(ctx context.Context, db *sql.DB, schema, table string, cdcMode, cdcBlocks bool, nominatedCols []string) Check {
	code := "CDC_TABLE_MISSING_PRIMARY_KEY"
	// First verify the table exists in the source — distinguish "no table"
	// from "no PK" so the user fixes the right problem.
	var tableExists bool
	err := db.QueryRowContext(ctx,
		`SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2)`,
		schema, table,
	).Scan(&tableExists)
	if err != nil {
		return Check{
			Code: code, Severity: SeverityError, Passed: false,
			Message: fmt.Sprintf("Could not query existence of %s.%s: %v", schema, table, err),
		}
	}
	if !tableExists {
		// The CDC_ prefix is historical — see the identical note in
		// assessor/mysql.go oneMySQLTablePKCheck. A plain BATCH pipeline into a
		// relational destination raises this too, and SeverityError BLOCKS the run.
		return Check{
			Code: "CDC_TABLE_NOT_FOUND_IN_SOURCE", Severity: SeverityError, Passed: false,
			Message: fmt.Sprintf("Table %s.%s does not exist in source", schema, table),
			Remediation: &diagnose.Remediation{
				Steps: []string{
					"Verify the table name and schema in your source database",
					"Confirm the connection user has USAGE on the schema and SELECT on the table",
					"Update the pipeline's table selection if the table was renamed/dropped",
				},
				DocURL:           diagnose.ErrorDocURL("cdc-table-not-found"),
				EstimatedMinutes: 5,
			},
		}
	}

	var hasPK bool
	err = db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM information_schema.table_constraints
			WHERE table_schema = $1 AND table_name = $2 AND constraint_type = 'PRIMARY KEY'
		)`,
		schema, table,
	).Scan(&hasPK)
	if err != nil {
		return Check{
			Code: code, Severity: SeverityError, Passed: false,
			Message: fmt.Sprintf("Could not query PK existence for %s.%s: %v", schema, table, err),
		}
	}
	if hasPK {
		return Check{
			Code: code, Severity: SeverityInfo, Passed: true,
			Message: fmt.Sprintf("%s.%s has a primary key", schema, table),
		}
	}
	// Past this point the table is keyless. If the CDC executor will refuse to
	// start over it, say so as an ERROR — neither the surrogate key nor a column
	// nomination reaches its validator, so both of the passes below would be
	// promises the run cannot keep (KI-CDC-ASSESS-PK-FALLBACK-NOT-IMPLEMENTED).
	if cdcBlocks {
		return blockingMissingPKCheck(
			fmt.Sprintf("%s.%s", schema, table),
			fmt.Sprintf("ALTER TABLE %q.%q ADD PRIMARY KEY (id);", schema, table),
			nominatedCols,
		)
	}
	// PR-D: the user nominated identifying column(s) for this keyless table.
	// The data plane upserts on these columns, so it is effectively keyed — an
	// INFO note, not a keyless WARNING.
	if len(nominatedCols) > 0 {
		return Check{
			Code: code, Severity: SeverityInfo, Passed: true,
			Message: fmt.Sprintf("Table %s.%s has no declared primary key; using user-nominated key column(s): %s. Updates apply in place on this key.", schema, table, strings.Join(nominatedCols, ", ")),
		}
	}
	// No primary key → do NOT block. rsync routes keyless tables through the
	// sink's content-hash surrogate key (_rsync_row_hash), matching the
	// Fivetran/Airbyte market standard. WARNING, never a hard block.
	return Check{
		Code: code, Severity: SeverityWarning, Passed: true,
		Message: fmt.Sprintf("Table %s.%s has no primary key. rsync will load it using a content-hash surrogate key (_rsync_row_hash), so the run succeeds. Because the hash covers all columns, a later change to any column is written as a new row (the prior version is retained), so updates can accumulate duplicates. For in-place updates, nominate identifying column(s) as the key (recommended) or add a PRIMARY KEY on the source.", schema, table),
		Remediation: &diagnose.Remediation{
			Steps: []string{
				"No action needed to run — keyless tables replicate via a content-hash surrogate key.",
				"Recommended for correct updates: nominate the column(s) that uniquely identify a row as the key.",
				"Alternative: add a primary key (or unique not-null index) on the source.",
			},
			SQLToRun: []string{
				"-- Optional — explicit PK for true in-place updates (replace 'id'):",
				fmt.Sprintf("ALTER TABLE %q.%q ADD PRIMARY KEY (id);", schema, table),
			},
			DocURL:           diagnose.ErrorDocURL("cdc-missing-pk"),
			EstimatedMinutes: 0,
		},
	}
}
