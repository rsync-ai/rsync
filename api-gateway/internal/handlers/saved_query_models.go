package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"regexp"
	"strings"
	"time"

	"api-gateway/internal/security"
	"api-gateway/internal/validators"

	"github.com/rsync-ai/shared/crypto"
	log "github.com/sirupsen/logrus"
)

// ============================================================================
// Saved-query models — the shared execution core
// ============================================================================
// A MODEL is a saved query plus a destination table (migration 085). Running one
// materializes the query into that table on its own connection — the in-warehouse
// "T" of ELT. The rows never transit rsync; the engine does the work server-side.
//
// This file is deliberately gin-free. Both callers reach the same function:
//
//   - a human clicking Run  → POST /api/v1/explorer/saved/:id/run   (session auth)
//   - a Temporal schedule   → POST /api/v1/internal/explorer/models/:id/run
//
// One execution implementation and one authorization implementation, because two
// would drift and the scheduled one is the copy nobody is watching.
//
// ── Authorization is re-derived at every run, never read from storage ──
//
// runSavedQueryModel takes an actor user ID and re-computes, from scratch:
//
//	1. the statement class of the CURRENT sql_text  (not the stored advisory class)
//	2. the actor's CURRENT workspace role           (not a snapshot taken at create)
//
// Both halves matter and they close different holes. (1) stops a member editing a
// scheduled model's SQL into something they could never have scheduled. (2) stops a
// schedule created by an admin from continuing to run after that admin is demoted or
// removed from the workspace. A stored permission is a permission that outlives the
// person, which is the whole failure mode.

// modelIdentPart bounds one identifier component. Identifiers cannot be bound as
// query parameters, so target_table is interpolated into DDL and this pattern is the
// only thing standing between a stored string and arbitrary SQL. Deliberately
// narrower than what the engines accept: no quotes, no dots, no spaces, no unicode.
//
// The 52-character ceiling (not 63) leaves room for the staging suffixes below to fit
// inside Postgres' 63-byte identifier limit. Without that headroom two long target
// names could truncate to the SAME staging name, and one model would clobber another
// model's in-flight rebuild.
var modelIdentPart = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,51}$`)

const (
	// Scratch names used by the build-then-swap rebuild. Namespaced so they cannot be
	// mistaken for user tables, and stable so a crashed run's leftovers are cleaned up
	// by the next run rather than accumulating.
	modelStagingSuffix = "__rsync_new"
	modelRetiredSuffix = "__rsync_old"

	// modelRunTimeout bounds a single materialization. A rebuild is a bulk server-side
	// operation and legitimately outlasts the 30s Explorer write deadline; beyond this
	// it is a runaway, and the schedule's next tick would pile up behind it.
	modelRunTimeout = 30 * time.Minute
)

// The values saved_queries.materialization may hold — the Go side of migration 088's
// CHECK constraint. Named rather than spelled inline because the run path branches on
// them in five places and a typo in any one of them is a silent behaviour change.
//
// The two runnable modes partition by what the SQL is, and nothing else distinguishes
// them:
//
//   - matTable wraps a SELECT in CREATE TABLE … AS and swaps the result into
//     target_table. The query says what to compute; rsync says where it lands.
//   - matStatement runs the SQL exactly as written. A MERGE / UPDATE / INSERT … SELECT
//     already names its own destination, so there is nothing for rsync to decide and no
//     target_table to ask for — which is what made the old modal unanswerable for the
//     statement people schedule most.
const (
	matNone      = "none"
	matTable     = "table"
	matStatement = "statement"
)

// isStatementModel reports whether this model runs its SQL as written.
//
// Written as an equality test against matStatement rather than an inequality against
// matTable so that an unrecognized materialization — a value written by a future
// migration, a hand-edited row — takes the table path, which is the one that demands a
// read. Failing closed here matters because this predicate is what decides whether
// authorizeModelRun lets a write through at all.
func isStatementModel(materialization string) bool {
	return materialization == matStatement
}

// savedQueryModel is everything a run needs, read in one query.
type savedQueryModel struct {
	ID              string
	WorkspaceID     string
	ConnectionID    string
	Name            string
	SQLText         string
	Materialization string
	TargetTable     string
	TargetOwned     bool
	ConnectorType   string
	ConfigEncrypted string
}

// modelRunResult is the outcome of one run, shaped for both the HTTP response and the
// schedule bookkeeping.
type modelRunResult struct {
	Status         string `json:"status"` // "succeeded" | "failed" | "skipped"
	RowsAffected   *int64 `json:"rows_affected,omitempty"`
	StatementClass string `json:"statement_class"`
	TargetTable    string `json:"target_table,omitempty"`
	Error          string `json:"error,omitempty"`

	// AutoPauseReason is non-empty when the run was refused for an identity or
	// classification reason that will still be true on the next tick. The scheduled
	// caller pauses on this rather than failing in a loop forever. A plain SQL failure
	// leaves it empty — that is worth retrying.
	AutoPauseReason string `json:"auto_pause_reason,omitempty"`
}

// validateModelTarget splits and validates a "table" or "schema.table" identifier.
// Returns the parts; the caller quotes them per dialect.
func validateModelTarget(raw string) (schemaName, tableName string, err error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", "", fmt.Errorf("target table is required")
	}
	parts := strings.Split(raw, ".")
	if len(parts) > 2 {
		return "", "", fmt.Errorf(`target table must be "table" or "schema.table"`)
	}
	for _, p := range parts {
		if !modelIdentPart.MatchString(p) {
			return "", "", fmt.Errorf("invalid identifier %q: use up to 52 letters, digits and underscores, starting with a letter or underscore", p)
		}
	}
	if len(parts) == 2 {
		return parts[0], parts[1], nil
	}
	return "", parts[0], nil
}

// quoteModelIdent renders validated parts for one engine. Every part has already
// passed modelIdentPart, so no escaping is required — the quotes are here so a target
// that collides with a reserved word still works.
func quoteModelIdent(dialect, schemaName, tableName string) string {
	var open, close string
	switch dialect {
	case "mysql":
		open, close = "`", "`"
	case "sqlserver":
		open, close = "[", "]"
	default: // postgres, redshift
		open, close = `"`, `"`
	}
	q := func(s string) string { return open + s + close }
	if schemaName != "" {
		return q(schemaName) + "." + q(tableName)
	}
	return q(tableName)
}

// modelDialect maps a connector type to the direct-driver dialect used for quoting,
// DDL syntax and driver selection. Returns "" for connectors models do not support.
func modelDialect(connectorType string) string {
	ct := strings.ToLower(connectorType)
	switch {
	case strings.Contains(ct, "postgres"), strings.Contains(ct, "redshift"):
		return "postgres"
	case strings.Contains(ct, "mysql"), strings.Contains(ct, "mariadb"):
		return "mysql"
	case strings.Contains(ct, "sqlserver"), strings.Contains(ct, "mssql"):
		return "sqlserver"
	default:
		return ""
	}
}

// createTableAsSQL renders "materialize this SELECT into this table" for one dialect.
//
// T-SQL is the odd one out: SQL Server has no CREATE TABLE AS SELECT (Synapse does,
// the mainline product does not), so it gets SELECT ... INTO over a derived table.
// One consequence worth knowing: T-SQL rejects ORDER BY inside a derived table, so a
// model whose query ends in a bare ORDER BY fails on SQL Server and works elsewhere.
// The engine's own error names that precisely, so it surfaces rather than being masked.
func createTableAsSQL(dialect, quotedTarget, innerSQL string) string {
	inner := strings.TrimRight(strings.TrimSpace(innerSQL), ";")
	if dialect == "sqlserver" {
		return fmt.Sprintf("SELECT * INTO %s FROM (%s) AS rsync_model_src", quotedTarget, inner)
	}
	return fmt.Sprintf("CREATE TABLE %s AS %s", quotedTarget, inner)
}

// renameModelTable renders a table rename for one dialect. Postgres and SQL Server
// both want the destination BARE — neither rename can move a table between schemas —
// while MySQL wants both sides qualified.
func renameModelTable(dialect, schemaName, from, to string) string {
	switch dialect {
	case "mysql":
		return fmt.Sprintf("RENAME TABLE %s TO %s",
			quoteModelIdent(dialect, schemaName, from), quoteModelIdent(dialect, schemaName, to))
	case "sqlserver":
		// sp_rename's arguments are string literals, not identifiers. Interpolating
		// into them is safe only because both names have passed modelIdentPart, which
		// admits no quote character — there is nothing here left to escape.
		return fmt.Sprintf("EXEC sp_rename '%s', '%s'", quoteModelIdent(dialect, schemaName, from), to)
	default: // postgres, redshift
		return fmt.Sprintf("ALTER TABLE %s RENAME TO %s",
			quoteModelIdent(dialect, schemaName, from), quoteModelIdent(dialect, "", to))
	}
}

// modelSwapSQL renders the rename(s) that retire the live table and move the freshly
// built staging table into its place.
//
// These two renames are one operation, and the window between them is the most
// dangerous moment in the whole feature: with the live table already renamed aside and
// the staging table not yet renamed in, the target name does not exist at all. A run
// that dies in that window leaves no table for the user to query — and the next run's
// opening `DROP TABLE IF EXISTS <target>__rsync_old` would then delete the only
// surviving copy of their data before its own rename failed against the missing target,
// wedging the model permanently. Losing the table this feature exists to maintain is
// the worst outcome available here, so the window is closed rather than narrowed.
//
// MySQL closes it with the multi-table form, which is atomic in a single statement and
// is the documented idiom for exactly this swap. Postgres, Redshift and SQL Server have
// transactional DDL, so their two statements are atomic by virtue of the transaction
// executeModelPlan wraps the plan in.
func modelSwapSQL(dialect, schemaName, tableName string, retireExisting bool) []string {
	staging := tableName + modelStagingSuffix
	retired := tableName + modelRetiredSuffix

	if dialect == "mysql" {
		if !retireExisting {
			return []string{renameModelTable(dialect, schemaName, staging, tableName)}
		}
		return []string{fmt.Sprintf("RENAME TABLE %s TO %s, %s TO %s",
			quoteModelIdent(dialect, schemaName, tableName), quoteModelIdent(dialect, schemaName, retired),
			quoteModelIdent(dialect, schemaName, staging), quoteModelIdent(dialect, schemaName, tableName))}
	}

	var stmts []string
	if retireExisting {
		// Move the live table aside rather than dropping it, so the destructive step is
		// the last thing that happens and only after the new table is already in place.
		stmts = append(stmts, renameModelTable(dialect, schemaName, tableName, retired))
	}
	return append(stmts, renameModelTable(dialect, schemaName, staging, tableName))
}

// buildMaterializationPlan returns the statements one rebuild executes, in order.
//
// The shape is build-then-swap, and the reason is the whole point of the feature. The
// obvious implementation — DROP the target, then CREATE it from the query — destroys
// the existing table before it knows whether the new one can be built. That is exactly
// backwards for rsync: the failure this product exists to catch is an upstream schema
// change breaking a model's SQL, and under drop-then-create that failure would convert
// a broken query into permanent data loss. Here the long, failure-prone CREATE happens
// under a scratch name while the live table is untouched; if it fails, the run reports
// failed and yesterday's table is still queryable.
//
// The swap also carries the collision guarantee. A rename onto the target name is NOT
// an "if exists" operation on any of these engines — it fails outright when the name is
// taken. So on a model's first run, when nothing has proven ownership yet, the engine
// itself refuses to let rsync take over a table it did not create; rsync never has to
// ask a catalog who owns what. Only once a run has completed this swap does
// target_owned flip true, and only then may the plan include the step that retires the
// previous table.
//
// retireExisting is that step's switch, and it is deliberately narrower than
// target_owned. Ownership says rsync is ALLOWED to replace what stands at the target
// name; this says there is in fact something standing there to move aside. They agree
// on every ordinary run and come apart in one case: the table gets dropped outside
// rsync. Keying the step off ownership alone wedged those models permanently — every
// subsequent run tried to rename a table that was no longer there and failed
// identically, with no way out but a manual UPDATE to the flag. Asking the catalog
// instead (see modelTargetExists) degrades that case to the first-run plan, which
// rebuilds the table and keeps the collision guarantee intact: if the name turns out to
// be occupied after all, the rename still refuses.
func buildMaterializationPlan(dialect, schemaName, tableName, innerSQL string, retireExisting bool) []string {
	staging := tableName + modelStagingSuffix
	retired := tableName + modelRetiredSuffix

	qStaging := quoteModelIdent(dialect, schemaName, staging)
	qRetired := quoteModelIdent(dialect, schemaName, retired)

	stmts := []string{
		// A leftover staging table from a run that died before its swap. Its name is
		// rsync-owned scratch and its contents are a half-built copy, so dropping it is
		// unconditionally safe.
		"DROP TABLE IF EXISTS " + qStaging,
		createTableAsSQL(dialect, qStaging, innerSQL),
		// The retired name is cleared only AFTER the replacement has been built, never
		// before. The swap is atomic (see modelSwapSQL), so no run can end with the live
		// table renamed aside and not replaced — meaning this should never be the last
		// copy of anything. Ordering it after the build is defence in depth: if that
		// atomicity is ever lost, the worst case becomes a stale backup left in the
		// schema rather than a DROP of the only surviving copy of the user's data.
		// It has to precede the swap regardless, or the rename onto it would collide.
		"DROP TABLE IF EXISTS " + qRetired,
	}

	stmts = append(stmts, modelSwapSQL(dialect, schemaName, tableName, retireExisting)...)
	if retireExisting {
		stmts = append(stmts, "DROP TABLE IF EXISTS "+qRetired)
	}
	return stmts
}

// modelPlanBuilder defers the last decision about the plan's shape until executeModelPlan
// has a connection open on the warehouse.
//
// Two separate questions decide whether a run moves the table at the target name aside,
// and both have to be yes: may we, which is rsync's own record of ownership and is
// already settled by the time we get here, and is there anything there, which only the
// warehouse can answer. The second is the one that can have changed behind our back.
func modelPlanBuilder(dialect, schemaName, tableName, innerSQL string, owned bool) func(context.Context, modelQueryer) ([]string, error) {
	return func(ctx context.Context, q modelQueryer) ([]string, error) {
		retireExisting := owned
		if retireExisting {
			exists, err := modelTargetExists(ctx, q, dialect, schemaName, tableName)
			if err != nil {
				return nil, err
			}
			retireExisting = exists
		}
		return buildMaterializationPlan(dialect, schemaName, tableName, innerSQL, retireExisting), nil
	}
}

// loadSavedQueryModel reads the model and its connection in one round trip. The
// connection is joined on workspace_id as well as id, so a model whose connection has
// since moved to another workspace cannot execute against it.
func loadSavedQueryModel(ctx context.Context, database *sql.DB, modelID string) (*savedQueryModel, error) {
	var m savedQueryModel
	var target sql.NullString
	err := database.QueryRowContext(ctx, `
		SELECT sq.id, sq.workspace_id, sq.connection_id, sq.name, sq.sql_text,
		       sq.materialization, sq.target_table, sq.target_owned,
		       c.connector_type, c.config
		FROM saved_queries sq
		JOIN connections c
		  ON c.id = sq.connection_id
		 AND c.workspace_id = sq.workspace_id
		WHERE sq.id = $1
	`, modelID).Scan(&m.ID, &m.WorkspaceID, &m.ConnectionID, &m.Name, &m.SQLText,
		&m.Materialization, &target, &m.TargetOwned, &m.ConnectorType, &m.ConfigEncrypted)
	if err != nil {
		return nil, err
	}
	m.TargetTable = strings.TrimSpace(target.String)
	return &m, nil
}

// errRoleUndetermined means the lookup itself failed — NOT that the user lacks a role.
//
// Both outcomes deny the run, and that part is not negotiable. What they must not share
// is what happens next: "this user was demoted" is permanent and should auto-pause the
// schedule, while "the role query errored" is a database blip that heals by itself. When
// the two were the same empty string, a one-second connection wobble paused a production
// schedule permanently and told the operator the run-as user was no longer a member —
// a false accusation that needed a human to notice and undo.
var errRoleUndetermined = errors.New("could not determine the workspace role")

// resolveWorkspaceRoleForUser reads a user's CURRENT role in a workspace.
//
// Fails closed in every direction: a non-member, a deleted user and a query error all
// yield the empty role, which Meets() rejects for every minimum. There is no path here
// that returns a usable role without a matching workspace_members row. The error return
// distinguishes "definitively not a member" (nil error, empty role) from "the lookup
// could not be completed" (errRoleUndetermined) so the caller can pause on the first and
// retry the second.
func resolveWorkspaceRoleForUser(ctx context.Context, database *sql.DB, workspaceID, userID string) (security.WorkspaceRole, error) {
	if database == nil || workspaceID == "" || userID == "" {
		// Missing inputs, not a missing membership: nothing was actually looked up.
		return "", errRoleUndetermined
	}
	var role string
	err := database.QueryRowContext(ctx, `
		SELECT role FROM workspace_members
		WHERE workspace_id = $1 AND user_id = $2
	`, workspaceID, userID).Scan(&role)
	if err == sql.ErrNoRows {
		// An authoritative answer: there is no membership row. Permanent.
		return "", nil
	}
	if err != nil {
		log.WithError(err).Warn("model run: failed to resolve workspace role — failing closed for this run")
		return "", errRoleUndetermined
	}
	return security.WorkspaceRole(strings.ToLower(strings.TrimSpace(role))), nil
}

// modelRunMinRole is the FLOOR every model run must clear, in either mode. It is not
// the whole answer: ValidateExplorerStatement then holds the statement to its own
// class minimum, which for a destructive statement is owner. This only sets the bottom.
//
// It is WSAdmin, and that is not a new number: a table-mode run issues CREATE TABLE and
// DROP TABLE, and ClassDDL maps to WSAdmin in validators.classToMinRole — this is the
// existing policy table applied to what the run actually executes.
//
// Statement mode issues no DDL of rsync's own, so its statement's class minimum would
// be defensible on its own. The floor stays anyway, because the thing being authorized
// is not really the statement — it is an unattended, repeating execution of it against
// production with no human at the keyboard, and that is an administrative act whatever
// the verb happens to be.
//
// It deliberately does not escalate to WSOwner even though the table-mode plan contains
// a DROP and ClassDestructive maps to owner. The destructive rule exists to protect
// tables someone else depends on; the only table this plan drops is one a previous run
// of this same model created (target_owned), whose entire content is a pure function of
// the query that just rebuilt it. Requiring owner would make the feature unusable in
// exchange for protecting data that is by construction reproducible.
const modelRunMinRole = security.WSAdmin

// authorizeModelRun re-derives class and role and reports whether the run may proceed.
// A non-empty refusal is intended for auto_paused_reason: every refusal here is a
// condition that will still hold on the next tick, so a scheduled caller should pause
// rather than retry.
//
// A returned error means authorization could not be DECIDED. That is not a refusal and
// must never reach auto_paused_reason — the caller retries instead.
func authorizeModelRun(ctx context.Context, database *sql.DB, m *savedQueryModel, actorUserID string) (validators.StatementClass, string, error) {
	class := validators.ClassifyStatementSQL(m.SQLText)

	role, roleErr := resolveWorkspaceRoleForUser(ctx, database, m.WorkspaceID, actorUserID)
	if roleErr != nil {
		return class, "", roleErr
	}
	if role == "" {
		return class, "the run-as user is no longer a member of this workspace", nil
	}
	if !role.Meets(modelRunMinRole) {
		return class, fmt.Sprintf("the run-as user's role (%s) does not meet the %s role required to run a model", role, modelRunMinRole), nil
	}

	// The statement gate runs for EVERY class, ahead of any question about what this
	// model's mode expects. That ordering is the security property, not a style choice.
	//
	// It used to sit last, behind an early `class != ClassRead` refusal, and that was
	// safe only for as long as no class but read could get past it. Statement mode ends
	// that: a MERGE now legitimately runs. Had the gate stayed where it was, adding the
	// mode would have handed write SQL a path to the engine with nothing but the class
	// having checked it — and a class cannot describe a second statement, which is the
	// whole attack. `MERGE INTO invoices …; DROP TABLE customers` classifies as
	// dml_write on its leading verb, and the plan hands the entire string to a
	// parameter-free Exec that the driver runs under the simple query protocol, executing
	// every statement in it under the schedule creator's authority.
	//
	// ValidateExplorerStatement is where the single-statement rule lives, and running
	// the whole thing (not just that one check) also puts a model's SQL through the same
	// pipeline an interactive query gets: blocked verbs refused, unrecognized statements
	// failed closed to owner, and each write class held to its own minimum role — which
	// is what makes a destructive statement owner-only here rather than admin-only.
	// Nothing validates sql_text on the way in — SaveSavedQuery stores whatever it is
	// given and only classifies it for a display badge — so this is the gate, and like
	// the rest of this function it re-derives from the CURRENT text on every run.
	if v := validators.ValidateExplorerStatement(m.SQLText, role); v != nil && !v.Valid {
		return class, fmt.Sprintf("the model's SQL is not a single safe statement for this user: %s", v.ErrorMessage), nil
	}

	// Only now, with the SQL known safe and the role known sufficient, does the mode
	// have anything to say. The two runnable modes want opposite things, and each is
	// incoherent with the other's SQL rather than merely unusual:
	//
	//   - table mode wraps the SQL in CREATE TABLE … AS. Wrapping a DELETE in that is
	//     not a stricter rebuild, it is meaningless.
	//   - statement mode runs the SQL for its effect on the warehouse. A SELECT has no
	//     effect and delivers its rows nowhere, so a scheduled one burns warehouse time
	//     to produce nothing — exactly what the materialization='none' skip refuses.
	if isStatementModel(m.Materialization) {
		if !validators.IsWriteClass(class) {
			return class, fmt.Sprintf("a statement model must be a write statement; this one classifies as %s — "+
				"write it to a table instead", class), nil
		}
		return class, "", nil
	}
	if class != validators.ClassRead {
		return class, fmt.Sprintf("a model that writes to a table must be a read query; this one classifies as %s — "+
			"run it as a statement instead", class), nil
	}
	return class, "", nil
}

// modelDSN builds the direct-driver DSN, reusing the Explorer's own DSN builders so a
// model connects exactly the way a manual query does — same SSL/TLS resolution, same
// private-host rules.
func modelDSN(dialect string, config map[string]interface{}) (string, error) {
	switch dialect {
	case "postgres":
		return postgresExplorerDSN(config), nil
	case "mysql":
		return mysqlExplorerDSN(config), nil
	case "sqlserver":
		return sqlServerExplorerDSN(config), nil
	default:
		return "", fmt.Errorf("unsupported dialect %q", dialect)
	}
}

// modelExecer is the little of database/sql that a plan needs, satisfied by both
// *sql.DB and *sql.Tx so the transactional and non-transactional paths share a loop.
type modelExecer interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

// modelQueryer is the read half of whatever the plan is about to run on — the
// transaction on engines that have one, the bare connection on MySQL. The plan asks the
// catalog exactly one question before it commits to a shape, and it has to ask it there
// rather than on a second connection: on the transactional engines that puts the
// question and the swap it decides inside the same transaction.
type modelQueryer interface {
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

// modelTargetExists reports whether a plain table stands at the target name right now.
//
// This is NOT an ownership check — saved_queries.target_owned is the record of whether
// this model may replace what it finds, and that question is settled before we get
// here. This answers the narrower one the plan actually needs: is there something to
// retire? The two come apart when a table is dropped outside rsync, which is the case
// that used to wedge a model permanently (see buildMaterializationPlan).
//
// Deliberately limited to BASE TABLE. A view standing at the target name is not
// something a previous run of this model left behind — rsync only ever creates tables —
// and Postgres would happily let ALTER TABLE ... RENAME move a user's view aside.
// Reporting "nothing to retire" sends that case into the rename that collides and
// refuses, which is the right answer for an object rsync did not create.
func modelTargetExists(ctx context.Context, q modelQueryer, dialect, schemaName, tableName string) (bool, error) {
	// An empty schema means "wherever this connection resolves unqualified names",
	// which is exactly where the plan's own unqualified DDL will land — so the check has
	// to resolve it the same way rather than guessing at a default like "public".
	var query string
	switch dialect {
	case "mysql":
		query = `SELECT count(*) FROM information_schema.tables
		         WHERE table_type = 'BASE TABLE' AND table_name = ?
		           AND table_schema = COALESCE(NULLIF(?, ''), DATABASE())`
	case "sqlserver":
		query = `SELECT count(*) FROM information_schema.tables
		         WHERE table_type = 'BASE TABLE' AND table_name = @p1
		           AND table_schema = COALESCE(NULLIF(@p2, ''), SCHEMA_NAME())`
	default: // postgres, redshift
		query = `SELECT count(*) FROM information_schema.tables
		         WHERE table_type = 'BASE TABLE' AND table_name = $1
		           AND table_schema = COALESCE(NULLIF($2, ''), current_schema())`
	}

	var n int
	if err := q.QueryRowContext(ctx, query, tableName, schemaName).Scan(&n); err != nil {
		// Not knowing is not the same as "it is not there": guessing wrong in that
		// direction would send a live table into the rename that refuses, so the run
		// fails instead. That is retryable and leaves everything as it was.
		return false, fmt.Errorf("failed to check whether the target table exists: %w", err)
	}
	return n > 0, nil
}

// modelDialectSupportsTransactionalDDL reports whether a dialect can roll a
// half-finished rebuild back.
//
// Postgres, Redshift and SQL Server all commit DDL transactionally. MySQL commits
// implicitly on every DDL statement, so wrapping its plan in a transaction would buy
// nothing and imply a rollback that will not happen; its swap is made atomic by the
// single-statement RENAME form instead (see modelSwapSQL).
func modelDialectSupportsTransactionalDDL(dialect string) bool {
	return dialect != "mysql"
}

// execModelStatements runs the plan in order, stopping at the first failure.
func execModelStatements(ctx context.Context, ex modelExecer, stmts []string) (*int64, error) {
	var rowsAffected *int64
	for _, stmt := range stmts {
		res, err := ex.ExecContext(ctx, stmt)
		if err != nil {
			return nil, fmt.Errorf("statement failed: %w", err)
		}
		if res == nil || !reportsRowsAffected(validators.ClassifyStatementSQL(stmt)) {
			continue
		}
		if n, rErr := res.RowsAffected(); rErr == nil {
			v := n
			rowsAffected = &v
		}
	}
	return rowsAffected, nil
}

// executeModelPlan runs one materialization plan to completion on a single connection.
//
// Two things force this to exist rather than looping over executeDirectWrite:
//
//  1. executeDirectWrite caps every statement at 30 seconds and recycles the connection
//     after 35. Those are the right bounds for an interactive Explorer write and wrong
//     by three orders of magnitude for a rebuild: modelRunTimeout budgets 30 minutes and
//     the Temporal activity waits 35, but the innermost deadline is the one that fires,
//     so a target big enough to take half a minute to build could never be built at all.
//
//  2. A fresh connection per statement cannot hold a transaction, and on every engine
//     with transactional DDL the swap has to be inside one.
//
// sweepOnFailure is a DROP of the staging table, run only for dialects that have no
// transaction to roll back.
//
// The plan arrives as a builder rather than a finished slice because one detail of its
// shape — whether there is a live table to retire — can only be read from the warehouse,
// and reading it anywhere else would mean asking one connection about a state a
// different connection is about to change. Called once the connection is up and, where
// the engine has one, inside the transaction, so on those engines the question and the
// swap it decides are the same atomic unit.
func executeModelPlan(ctx context.Context, dialect, dsn string, buildPlan func(context.Context, modelQueryer) ([]string, error), sweepOnFailure string) (*int64, error) {
	conn, err := sql.Open(dialect, dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}
	defer conn.Close()

	// One connection for the whole plan: its statements share scratch tables and, where
	// the engine allows it, a transaction. The lifetime has to outlast the longest
	// rebuild the run is allowed to attempt.
	conn.SetMaxOpenConns(1)
	conn.SetMaxIdleConns(1)
	conn.SetConnMaxLifetime(modelRunTimeout + time.Minute)

	if err := conn.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("connection ping failed: %w", err)
	}

	if !modelDialectSupportsTransactionalDDL(dialect) {
		stmts, planErr := buildPlan(ctx, conn)
		if planErr != nil {
			return nil, planErr
		}
		rows, execErr := execModelStatements(ctx, conn, stmts)
		if execErr != nil && sweepOnFailure != "" {
			// No transaction to unwind, so the staging table has to be swept by hand or
			// it sits in the user's schema until the next run reclaims it. Best effort on
			// a fresh deadline — ctx may already be cancelled, and the failure worth
			// reporting is the original one, not this.
			sweepCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if _, dErr := conn.ExecContext(sweepCtx, sweepOnFailure); dErr != nil {
				log.WithError(dErr).Warn("model run: could not sweep the staging table after a failed rebuild")
			}
		}
		return rows, execErr
	}

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open transaction: %w", err)
	}
	// The rollback IS the cleanup. A failed rebuild leaves no scratch tables behind and,
	// far more importantly, leaves the live table exactly as it was.
	defer func() { _ = tx.Rollback() }()

	stmts, err := buildPlan(ctx, tx)
	if err != nil {
		return nil, err
	}

	rows, err := execModelStatements(ctx, tx, stmts)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit the rebuild: %w", err)
	}
	return rows, nil
}

// modelRunLockNamespace separates this feature's advisory locks from every other
// pg_advisory_lock user. Postgres keeps advisory locks in one global 64-bit space; the
// two-int form carves that into a 32-bit namespace and a 32-bit key, so an unrelated
// subsystem hashing some other id cannot collide with a model run.
const modelRunLockNamespace = 0x72534D44 // "rSMD" — rsync saved-query model

// acquireModelRunLock serializes runs of ONE model against each other.
//
// Temporal's SKIP overlap policy only stops a scheduled tick from overlapping another
// scheduled tick. It does nothing about a human clicking Run while a tick is mid-flight,
// and nothing about the activity's own retries. Two runs of the same model share every
// scratch name: both execute `DROP TABLE IF EXISTS <t>__rsync_new` and both build into
// it, so the loser drops the winner's staging table out from under it — and on the
// owned path both also target `<t>__rsync_old`, which is where the live data sits for
// the instant between the renames.
//
// The lock is taken on the rsync application database, not the customer's warehouse:
// it guards rsync's own plan, it must work on engines with no advisory-lock primitive,
// and it must not need any extra privilege on the target.
//
// A held lock is refused rather than queued. The caller wanted a rebuild of this model
// and one is already running — waiting would just mean rebuilding twice in a row from
// the same source, and a queued run holding an HTTP request open is how a retry storm
// becomes a connection exhaustion.
//
// It must be a dedicated *sql.Conn: an advisory lock belongs to the session that took
// it, and a pooled *sql.DB would hand the unlock to some other connection.
func acquireModelRunLock(ctx context.Context, database *sql.DB, modelID string) (release func(), acquired bool, err error) {
	conn, err := database.Conn(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("failed to reserve a connection for the run lock: %w", err)
	}
	// A hash collision between two different models would refuse one legitimate
	// concurrent run and report it as already-running. At 1 in 2^32 that is rarer than
	// the outcome it prevents, and it fails toward not-running rather than corrupting.
	key := int32(crc32.ChecksumIEEE([]byte(modelID)))

	var got bool
	if err := conn.QueryRowContext(ctx,
		`SELECT pg_try_advisory_lock($1, $2)`, int32(modelRunLockNamespace), key).Scan(&got); err != nil {
		_ = conn.Close()
		return nil, false, fmt.Errorf("failed to take the run lock: %w", err)
	}
	if !got {
		_ = conn.Close()
		return nil, false, nil
	}

	return func() {
		// Session-level locks are only released by an explicit unlock or by the session
		// actually ending — and conn.Close() returns the connection to the pool with its
		// session intact, so skipping this would leak the lock and wedge the model until
		// the pool happened to retire that connection.
		//
		// A fresh context because the run's own ctx is frequently already cancelled by
		// the time this runs; that is precisely when releasing matters most.
		relCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := conn.ExecContext(relCtx,
			`SELECT pg_advisory_unlock($1, $2)`, int32(modelRunLockNamespace), key); err != nil {
			log.WithError(err).WithField("model_id", modelID).
				Error("model run: failed to release the run lock; further runs of this model may be refused until the connection is recycled")
		}
		_ = conn.Close()
	}, true, nil
}

// recordTargetOwnership writes the fact that this model built the table it just
// swapped in. It runs AFTER the swap has committed, so it is recording something that
// already happened in the warehouse — the row is catching up to the world, not
// deciding it.
//
// Losing this write is not cosmetic. Ownership is what lets the next run retire the
// previous table; without it the next run re-attempts the first-run plan, whose rename
// collides with the table this run just created, and the model then fails on every run
// until a human deletes the table by hand. One transient blip here permanently wedges
// a working model.
//
// Two things make that blip likelier than it looks, and both are addressed here:
//
//   - The request context. A rebuild is budgeted 30 minutes; by the time the swap
//     lands, the caller may well have hung up, and ctx would already be cancelled —
//     the longer and more valuable the run, the likelier it is to lose its ownership
//     write. A fresh context detaches this from whether anyone is still listening.
//   - A momentary application-database hiccup. A few short retries cover the gap
//     between "the connection blipped" and "the model is wedged forever".
//
// Still best effort at the end of it: the run genuinely succeeded and its result must
// be reported as such. Failing in this direction leaves a visible, diagnosable rename
// collision rather than a silent overwrite of somebody's table, which is the right way
// round — but the warning below is the only notice anyone gets, so it names the fix.
func recordTargetOwnership(database *sql.DB, modelID string) {
	// Deliberately not the run's ctx — see above.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			select {
			case <-time.After(time.Duration(attempt) * time.Second):
			case <-ctx.Done():
			}
			// Checked here rather than folded into lastErr, which by this point already
			// holds the previous attempt's failure — the condition for giving up is the
			// budget running out, not the retry having been necessary at all.
			if err := ctx.Err(); err != nil {
				lastErr = err
				break
			}
		}
		// Idempotent: re-running it after a partial failure costs nothing, and a
		// concurrent run that already set the flag makes this a no-op.
		_, err := database.ExecContext(ctx,
			`UPDATE saved_queries SET target_owned = TRUE WHERE id = $1`, modelID)
		if err == nil {
			return
		}
		lastErr = err
	}

	log.WithError(lastErr).WithField("model_id", modelID).
		Error("model run: the rebuild succeeded but recording target ownership did not; " +
			"further runs of this model will fail on a rename collision until the flag is set " +
			"(UPDATE saved_queries SET target_owned = TRUE) or the target table is removed")
}

// runSavedQueryModel executes one model run end to end. It is the single entry point
// for both the manual and the scheduled path.
//
// actorUserID is whose permissions the run is checked against — the session user for a
// manual run, the schedule's run_as_user_id for a scheduled one.
//
// It does not return a Go error for a refusal or a SQL failure: those are outcomes,
// carried in the result so the caller can persist them and decide about pausing. A
// returned error means the run could not be attempted at all.
func runSavedQueryModel(ctx context.Context, database *sql.DB, modelID, actorUserID string) (*modelRunResult, error) {
	// Before anything is read, so the model's state — target_owned above all — is loaded
	// and acted on without a concurrent run changing it underneath.
	release, acquired, err := acquireModelRunLock(ctx, database, modelID)
	if err != nil {
		// Could not establish whether a run is in flight. Refusing to guess: this is the
		// one branch that returns an error, because it is transient and retryable.
		return nil, err
	}
	if !acquired {
		// Deliberately no AutoPauseReason. A run in progress is the most transient
		// condition this function has — pausing the schedule over it would turn a
		// momentary overlap into an outage that needs a human to clear.
		return &modelRunResult{
			Status: "skipped",
			Error:  "a run of this model is already in progress",
		}, nil
	}
	defer release()

	m, err := loadSavedQueryModel(ctx, database, modelID)
	if err == sql.ErrNoRows {
		// The saved query is gone, or its connection no longer joins inside this
		// workspace. Both are permanent, so this has to be an outcome rather than a Go
		// error: an error return skips the caller's recordModelRunOutcome AND its
		// auto-pause, which left a broken schedule retrying every tick forever while the
		// saved query still displayed its last SUCCESSFUL run.
		return &modelRunResult{
			Status:          "skipped",
			Error:           "this saved query or its connection is no longer available in this workspace",
			AutoPauseReason: "the saved query or its connection is no longer available in this workspace",
		}, nil
	}
	if err != nil {
		// A genuine database fault. Those recover on their own, so this one stays an
		// error and stays retryable.
		return nil, fmt.Errorf("failed to load model: %w", err)
	}

	if m.Materialization == matNone {
		// A saved query with no destination has nowhere to put its rows, and there is
		// no notification consumer to deliver a result set to. Running it anyway would
		// burn warehouse time to produce nothing.
		return &modelRunResult{
			Status:          "skipped",
			StatementClass:  string(validators.ClassifyStatementSQL(m.SQLText)),
			Error:           "this saved query has no materialization target; set one before running it as a model",
			AutoPauseReason: "the saved query is no longer materialized",
		}, nil
	}

	class, refusal, authErr := authorizeModelRun(ctx, database, m, actorUserID)
	if authErr != nil {
		// Undecided, not denied. Returning an error keeps this retryable and, crucially,
		// keeps it out of auto_paused_reason: a database blip must not be recorded as
		// "the run-as user is no longer a member".
		return nil, fmt.Errorf("failed to authorize model run: %w", authErr)
	}
	if refusal != "" {
		return &modelRunResult{
			Status:          "skipped",
			StatementClass:  string(class),
			TargetTable:     m.TargetTable,
			Error:           refusal,
			AutoPauseReason: refusal,
		}, nil
	}

	dialect := modelDialect(m.ConnectorType)
	if dialect == "" {
		// BigQuery and the other delegated engines route through the orchestrator's MCP
		// tools, which have no execute-DDL path today. Say so rather than silently doing
		// nothing.
		return &modelRunResult{
			Status:          "skipped",
			StatementClass:  string(class),
			TargetTable:     m.TargetTable,
			Error:           fmt.Sprintf("materialized models are not supported for %s connections yet", m.ConnectorType),
			AutoPauseReason: fmt.Sprintf("connector %s does not support materialized models", m.ConnectorType),
		}, nil
	}

	// A statement model has no target of rsync's own, so there is nothing here to
	// validate — the destination is inside the SQL, where it is the engine's business
	// and not an identifier rsync interpolates. Skipping the check is not a relaxation:
	// validateModelTarget guards the one injection surface a rebuild has (a table name
	// cannot be bound as a parameter), and a statement model never builds that string.
	var schemaName, tableName string
	if !isStatementModel(m.Materialization) {
		schemaName, tableName, err = validateModelTarget(m.TargetTable)
		if err != nil {
			return &modelRunResult{
				Status:          "skipped",
				StatementClass:  string(class),
				TargetTable:     m.TargetTable,
				Error:           err.Error(),
				AutoPauseReason: "the model's target table is not a valid identifier",
			}, nil
		}
	}

	// The next three are all "this connection's stored config is unusable". None of them
	// heal between ticks, so like the missing-model case above they are outcomes the
	// scheduled caller can pause on rather than errors it would retry forever. The
	// underlying error is logged rather than returned: it is a crypto/parse failure over
	// connection credentials, and its text is not something to hand back to a caller.
	configJSON, err := crypto.DecryptString(m.ConfigEncrypted)
	if err != nil {
		log.WithError(err).WithField("connection_id", m.ConnectionID).
			Error("model run: could not decrypt connection config")
		return unusableConnectionResult(class, m.TargetTable, "could not be decrypted"), nil
	}
	var config map[string]interface{}
	if err := json.Unmarshal([]byte(configJSON), &config); err != nil {
		log.WithError(err).WithField("connection_id", m.ConnectionID).
			Error("model run: could not parse connection config")
		return unusableConnectionResult(class, m.TargetTable, "could not be parsed"), nil
	}

	dsn, err := modelDSN(dialect, config)
	if err != nil {
		log.WithError(err).WithField("connection_id", m.ConnectionID).
			Error("model run: could not build a DSN for this connection")
		return unusableConnectionResult(class, m.TargetTable, "could not be turned into a connection string"), nil
	}

	runCtx, cancel := context.WithTimeout(ctx, modelRunTimeout)
	defer cancel()

	// Both modes run through executeModelPlan, and that is deliberate rather than
	// incidental: everything it does — the dedicated single connection, the 30-minute
	// lifetime, the transaction where the engine has one — is as necessary for a bulk
	// MERGE as it is for a rebuild. What differs between the modes is only which
	// statements the plan contains.
	plan := modelPlanBuilder(dialect, schemaName, tableName, m.SQLText, m.TargetOwned)
	sweepOnFailure := ""
	switch {
	case isStatementModel(m.Materialization):
		// The plan IS the statement. authorizeModelRun has already put this exact text
		// through ValidateExplorerStatement, so what runs here is one statement whose
		// class the run-as user's current role clears.
		//
		// No sweep: the scratch tables a sweep exists to reclaim are a rebuild's, and
		// this plan creates none. On the transactional engines the statement still gets
		// the transaction, so a failed multi-row MERGE leaves nothing half-applied.
		plan = func(context.Context, modelQueryer) ([]string, error) {
			return []string{m.SQLText}, nil
		}
	case !modelDialectSupportsTransactionalDDL(dialect):
		// Only the dialects without transactional DDL need an explicit sweep; the others
		// unwind a failed rebuild by rolling back.
		sweepOnFailure = "DROP TABLE IF EXISTS " + quoteModelIdent(dialect, schemaName, tableName+modelStagingSuffix)
	}

	rowsAffected, execErr := executeModelPlan(runCtx, dialect, dsn, plan, sweepOnFailure)
	if execErr != nil {
		return &modelRunResult{
			Status:         "failed",
			StatementClass: string(class),
			TargetTable:    m.TargetTable,
			Error:          execErr.Error(),
		}, nil
	}

	// The swap succeeded, so this model provably owns the table and later runs may
	// retire it. A statement model owns nothing — it never created the tables its SQL
	// writes to, and claiming otherwise would hand a later table-mode run of the same
	// saved query a licence to DROP a table rsync did not create.
	if !m.TargetOwned && !isStatementModel(m.Materialization) {
		recordTargetOwnership(database, modelID)
	}

	return &modelRunResult{
		Status:         "succeeded",
		RowsAffected:   rowsAffected,
		StatementClass: string(class),
		TargetTable:    m.TargetTable,
	}, nil
}

// unusableConnectionResult is the shared shape for "the stored connection config cannot
// be used", which is a stable condition and therefore an auto-pause.
func unusableConnectionResult(class validators.StatementClass, targetTable, what string) *modelRunResult {
	reason := "this model's connection config " + what
	return &modelRunResult{
		Status:          "skipped",
		StatementClass:  string(class),
		TargetTable:     targetTable,
		Error:           reason,
		AutoPauseReason: reason,
	}
}

// modelRunTrigger names the door a run came through. A typed string rather than a
// bare one so a call site cannot pass a value the saved_query_runs CHECK constraint
// will reject at write time, three layers below where the mistake was made.
type modelRunTrigger string

const (
	triggerManual    modelRunTrigger = "manual"
	triggerScheduled modelRunTrigger = "scheduled"

	// triggerTriggered is an upstream pipeline finishing (migration 095), and it is a
	// third door rather than a flavour of scheduled. The question an operator asks of
	// an event-driven model is "what caused this rebuild"; answering "a clock did"
	// would be false, and it is the answer that makes a stale table impossible to
	// diagnose — the clock would be the first thing they went looking at.
	triggerTriggered modelRunTrigger = "triggered"
)

// modelRunAudit carries the provenance a modelRunResult cannot: which door the run
// came through, whose identity it executed as, which schedule fired it, and when it
// started. Deliberately not fields on modelRunResult — that struct is the execution
// core's return value, and the core has no business asserting facts about how it was
// invoked.
type modelRunAudit struct {
	Trigger    modelRunTrigger
	ScheduleID string    // "" for a manual run
	ActorID    string    // the identity the statement executed as
	StartedAt  time.Time // when the attempt began, for duration
}

// stampLastRunOutcome writes the denormalized last_run_* columns the saved-query list
// renders. Takes a modelExecer so it can run either standalone or inside the history
// transaction below.
func stampLastRunOutcome(ctx context.Context, ex modelExecer, modelID string, res *modelRunResult) error {
	_, err := ex.ExecContext(ctx, `
		UPDATE saved_queries
		SET last_run_at = NOW(), last_run_status = $2, last_run_error = NULLIF($3, '')
		WHERE id = $1
	`, modelID, res.Status, res.Error)
	return err
}

// recordModelRunOutcome stamps the result onto the saved query for the list UI and
// appends the attempt to saved_query_runs (migration 086).
//
// Best-effort by design — losing display bookkeeping must never turn a successful
// materialization into a reported failure. The two writes share one transaction
// because they describe the same event: a badge saying "succeeded" with no matching
// history row is worse than no record at all, since the history panel is what an
// operator consults precisely when the badge looks wrong.
func recordModelRunOutcome(ctx context.Context, database *sql.DB, modelID string, res *modelRunResult, audit modelRunAudit) {
	if database == nil || res == nil {
		return
	}
	if audit.StartedAt.IsZero() {
		audit.StartedAt = time.Now()
	}

	// An unset trigger is a wiring bug, not a runtime condition. Stamp the query
	// anyway — the badge is pre-existing behaviour and must not regress because a new
	// column has a CHECK constraint — and drop only the history row, loudly. Guessing
	// a value here would file an unattended 3am run as a human action, which is the
	// one thing this table exists to keep straight.
	if audit.Trigger != triggerManual && audit.Trigger != triggerScheduled && audit.Trigger != triggerTriggered {
		log.WithField("model_id", modelID).WithField("trigger", string(audit.Trigger)).
			Error("model run: unknown trigger source; recording outcome without run history")
		if err := stampLastRunOutcome(ctx, database, modelID, res); err != nil {
			log.WithError(err).WithField("model_id", modelID).Warn("model run: failed to record outcome")
		}
		return
	}

	tx, err := database.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		log.WithError(err).WithField("model_id", modelID).Warn("model run: failed to begin outcome transaction")
		return
	}
	defer func() { _ = tx.Rollback() }() // no-op once Commit has succeeded

	if err := stampLastRunOutcome(ctx, tx, modelID, res); err != nil {
		log.WithError(err).WithField("model_id", modelID).Warn("model run: failed to record outcome")
		return
	}

	// schedule_id and ran_as_user_id are UUID columns, and "" is not a UUID — NULLIF
	// turns the absent case into NULL instead of a cast error.
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO saved_query_runs (
			saved_query_id, schedule_id, trigger_source, status,
			statement_class, target_table, rows_affected, error,
			auto_pause_reason, ran_as_user_id, started_at, finished_at
		) VALUES (
			$1, NULLIF($2, '')::uuid, $3, $4,
			$5, $6, $7, NULLIF($8, ''),
			NULLIF($9, ''), NULLIF($10, '')::uuid, $11, NOW()
		)
	`, modelID, audit.ScheduleID, string(audit.Trigger), res.Status,
		res.StatementClass, res.TargetTable, res.RowsAffected, res.Error,
		res.AutoPauseReason, audit.ActorID, audit.StartedAt); err != nil {
		log.WithError(err).WithField("model_id", modelID).Warn("model run: failed to append run history")
		return
	}

	if err := tx.Commit(); err != nil {
		log.WithError(err).WithField("model_id", modelID).Warn("model run: failed to commit run outcome")
	}
}
