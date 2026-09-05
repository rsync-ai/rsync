package validators

import (
	"fmt"
	"regexp"
	"strings"

	"api-gateway/internal/security"
)

// Additional error codes for the role-aware Explorer statement validator.
const (
	// ErrCodeInsufficientRole is returned when the statement class is runnable but
	// the caller's workspace role is below the class minimum (classToMinRole).
	ErrCodeInsufficientRole = "INSUFFICIENT_ROLE_FOR_STATEMENT"
	// ErrCodeStatementBlocked is returned for statement classes no role may run via
	// the Data Explorer (GRANT/REVOKE/CALL/EXEC/SET/…).
	ErrCodeStatementBlocked = "STATEMENT_NOT_ALLOWED"
)

// StatementClass buckets a SQL statement by the kind of authority it needs. The
// Data Explorer gates each class on a minimum workspace role (classToMinRole).
type StatementClass string

const (
	ClassRead        StatementClass = "read"        // SELECT, WITH — unchanged read surface
	ClassDMLWrite    StatementClass = "dml_write"   // INSERT, UPDATE, DELETE, MERGE
	ClassDDL         StatementClass = "ddl"         // CREATE, ALTER
	ClassDestructive StatementClass = "destructive" // DROP, TRUNCATE
	ClassBlocked     StatementClass = "blocked"     // GRANT/REVOKE/CALL/EXEC/SET/… never allowed via Explorer
	ClassUnknown     StatementClass = "unknown"     // unrecognized → fail closed (owner-only)
)

// classToMinRole is the single source of truth mapping a statement class to the
// minimum workspace role permitted to run it in the Data Explorer:
//   - viewer/member: read only (Explorer is unchanged for them)
//   - admin: DML writes + non-destructive DDL (INSERT/UPDATE/DELETE/MERGE/CREATE/ALTER)
//   - owner: + destructive (DROP/TRUNCATE)
//
// ClassBlocked is absent on purpose — no role may run those via Explorer.
// ClassUnknown maps to owner so an unrecognized statement fails closed (owner-only)
// rather than silently running at a lower tier.
var classToMinRole = map[StatementClass]security.WorkspaceRole{
	ClassRead:        security.WSViewer,
	ClassDMLWrite:    security.WSAdmin,
	ClassDDL:         security.WSAdmin,
	ClassDestructive: security.WSOwner,
	ClassUnknown:     security.WSOwner,
}

// alterDropRe matches a DROP sub-clause inside an ALTER statement (DROP COLUMN,
// DROP CONSTRAINT, DROP PARTITION, …). Word-boundaried so ordinary identifiers such
// as `drop_reason` or `backdrop` do not trip it. Over-matching is the safe direction
// here: the cost is one extra confirmation prompt, the cost of under-matching is
// irreversible data loss on a single click.
var alterDropRe = regexp.MustCompile(`(?i)\bDROP\b`)

// alterDropsObject reports whether an ALTER statement carries a DROP sub-clause.
// `ALTER TABLE t DROP COLUMN c` destroys data exactly as irreversibly as
// `DROP TABLE t`, but its leading verb is ALTER, so a verb-only classifier files it
// as ordinary DDL — admin role, no confirmation. This is what closes that gap.
func alterDropsObject(sqlText string) bool {
	return alterDropRe.MatchString(sqlText)
}

// cteWriteVerbs maps a write verb to the class a CTE-led statement containing it
// really needs. Ordered by descending severity so the first match wins: a statement
// carrying both DROP and UPDATE must land on the stricter of the two.
//
// Word-boundaried for the same reason alterDropRe is — `delete_count`, `update_time`
// and `merge_id` are ordinary column names and must not escalate anything.
var cteWriteVerbs = []struct {
	re    *regexp.Regexp
	class StatementClass
}{
	{regexp.MustCompile(`(?i)\bDROP\b`), ClassDestructive},
	{regexp.MustCompile(`(?i)\bTRUNCATE\b`), ClassDestructive},
	{regexp.MustCompile(`(?i)\bCREATE\b`), ClassDDL},
	{regexp.MustCompile(`(?i)\bALTER\b`), ClassDDL},
	{regexp.MustCompile(`(?i)\bINSERT\b`), ClassDMLWrite},
	{regexp.MustCompile(`(?i)\bUPDATE\b`), ClassDMLWrite},
	{regexp.MustCompile(`(?i)\bDELETE\b`), ClassDMLWrite},
	{regexp.MustCompile(`(?i)\bMERGE\b`), ClassDMLWrite},
}

// cteWriteClass reports the class a WITH-led statement needs, or ClassRead if it
// writes nothing. Comments and string/identifier literals are stripped first so
// `SELECT 'MERGE INTO x' AS note` and `/* DROP TABLE t */` stay reads.
//
// This scans the whole statement rather than parsing the CTE list, and that is a
// deliberate choice. Both shapes are dangerous — the write can sit in a CTE body
// (`WITH gone AS (DELETE … RETURNING *) SELECT * FROM gone`) or after the CTE list
// (`WITH s AS (SELECT 1) MERGE INTO …`) — and a hand-rolled CTE parser that misses a
// dialect's shape fails open, which is the failure that got us here. Over-matching
// is the safe direction, exactly as it is for alterDropRe: the cost is a viewer
// being told a query needs admin, and the cost of under-matching is a viewer
// silently writing production data on a schedule.
//
// Known over-match: `WITH s AS (…) SELECT … FOR UPDATE` is a locking read and now
// classifies as dml_write. It takes row locks, so admin is defensible, and the
// bare-SELECT form is untouched.
func cteWriteClass(sqlText string) StatementClass {
	scanned := stripStringLiterals(removeComments(sqlText))
	for _, v := range cteWriteVerbs {
		if v.re.MatchString(scanned) {
			return v.class
		}
	}
	return ClassRead
}

// ClassifyStatementSQL classifies a full statement. Prefer this over
// ClassifyStatement: the leading verb alone cannot distinguish
// `ALTER TABLE t ADD COLUMN c INT` (routine DDL) from `ALTER TABLE t DROP COLUMN c`
// (irreversible), and only the second should require owner + confirmation.
func ClassifyStatementSQL(sqlText string) StatementClass {
	// Classify the statement the engine will actually run, which is this text with its
	// comments removed. identifyStatementType prefix-matches the leading verb, so an
	// ordinary `-- daily revenue\nSELECT …` matched no verb at all and fell to
	// ClassUnknown — and ClassUnknown is a write class. That had two visible faces:
	// ExecuteExplorerQuery routed a commented SELECT down the no-rows write path, and
	// authorizeModelRun refused the same SQL for "not being a read query", which
	// auto-paused its schedule. ValidateExplorerStatement had always classified on the
	// stripped text, so the two disagreed about identical SQL.
	//
	// alterDropsObject and cteWriteClass read the stripped text for the same reason in
	// the opposite direction: a DROP named only inside a comment escalated a harmless
	// ALTER to owner-only. Neither over- nor under-matching is wanted here — the
	// classifier should see exactly what the engine sees, which is what removeComments
	// now yields (it deliberately keeps MySQL `/*!` executable-comment bodies).
	stripped := strings.TrimSpace(removeComments(sqlText))
	stmtType := identifyStatementType(stripped)
	class := ClassifyStatement(stmtType)
	if class == ClassDDL && alterDropsObject(stripped) {
		return ClassDestructive
	}
	// WITH is the other verb that lies about what its statement does, and it lies in
	// the more dangerous direction: it claims to be a read. `WITH s AS (SELECT 1)
	// MERGE INTO invoices …` classified as ClassRead, which routed it to the SELECT
	// pipeline, whose dangerous-keyword list names INSERT INTO / UPDATE / DELETE FROM
	// but never named MERGE — so it ran at viewer role. Class must follow the write.
	if stmtType == "WITH" {
		if escalated := cteWriteClass(stripped); escalated != ClassRead {
			return escalated
		}
	}
	return class
}

// ClassifyStatement maps a statement type (as returned by identifyStatementType)
// to its StatementClass. EXPLAIN/SHOW/DESCRIBE and the privilege/procedural verbs
// are ClassBlocked — the read surface stays exactly SELECT/WITH (unchanged), and
// writes are opened per the role matrix.
//
// This classifies by verb only. Callers that hold the full statement text should use
// ClassifyStatementSQL so that ALTER … DROP is caught as destructive.
func ClassifyStatement(stmtType string) StatementClass {
	switch strings.ToUpper(strings.TrimSpace(stmtType)) {
	case "SELECT", "WITH":
		return ClassRead
	case "INSERT", "UPDATE", "DELETE", "MERGE":
		return ClassDMLWrite
	case "CREATE", "ALTER":
		return ClassDDL
	case "DROP", "TRUNCATE":
		return ClassDestructive
	case "GRANT", "REVOKE", "CALL", "EXEC", "EXECUTE", "SET", "COPY",
		"VACUUM", "ANALYZE", "EXPLAIN", "SHOW", "DESCRIBE", "DESC":
		return ClassBlocked
	default:
		return ClassUnknown
	}
}

// MinRoleForClass returns the minimum workspace role for a class and whether the
// class is runnable at all. blocked=true means no role may run it via Explorer.
func MinRoleForClass(class StatementClass) (min security.WorkspaceRole, blocked bool) {
	min, ok := classToMinRole[class]
	if !ok {
		return "", true
	}
	return min, false
}

// IsWriteClass reports whether a class mutates data/schema (i.e. the execution path
// must use a no-rows "execute" rather than a rows-returning "query"). Reads return
// false; every gated write class returns true.
func IsWriteClass(class StatementClass) bool {
	switch class {
	case ClassDMLWrite, ClassDDL, ClassDestructive, ClassUnknown:
		return true
	default:
		return false
	}
}

// ValidateExplorerStatement validates a Data Explorer statement for a caller with
// the given workspace role. Reads (SELECT/WITH) keep the full historical SELECT-only
// safety pipeline (ValidateExplorerSQL) unchanged. Writes are permitted only when the
// caller's role meets the class minimum: DML/DDL require admin, destructive
// (DROP/TRUNCATE) requires owner. Blocked classes (GRANT/REVOKE/CALL/…) and
// unrecognized statements fail closed. A single statement is required for every class.
func ValidateExplorerStatement(sqlText string, role security.WorkspaceRole) *SQLValidationResult {
	trimmed := strings.TrimSpace(sqlText)
	if trimmed == "" {
		return invalidStatement(ErrCodeEmptyQuery, "Query cannot be empty", "")
	}

	stripped := strings.TrimSpace(removeComments(trimmed))
	if stripped == "" {
		return invalidStatement(ErrCodeEmptyQuery, "Query cannot be empty (only comments)", "")
	}

	// Single statement only — for every class. Blocks stacked-statement attacks
	// (e.g. `UPDATE …; DROP TABLE …`) before the class/role gate.
	if hasMultipleStatements(stripped) {
		return invalidStatement(ErrCodeMultipleStatements, "Multiple SQL statements are not allowed", "")
	}

	stmtType := identifyStatementType(stripped)
	// Classify on the comment-stripped statement, not just its verb, so
	// `ALTER … DROP COLUMN` is gated as destructive (owner) rather than as DDL (admin).
	class := ClassifyStatementSQL(stripped)

	// Reads keep the exact historical behavior (SELECT-only pipeline: dangerous
	// keyword + injection heuristics, LIMIT/table extraction, warnings). Reads are
	// gated by workspace membership at the middleware, not by a role tier here.
	if class == ClassRead {
		return ValidateExplorerSQL(sqlText)
	}

	minRole, blocked := MinRoleForClass(class)
	if blocked {
		return invalidStatement(ErrCodeStatementBlocked,
			fmt.Sprintf("%s statements are not allowed in the Data Explorer", stmtType), stmtType)
	}
	if !role.Meets(minRole) {
		return invalidStatement(ErrCodeInsufficientRole,
			fmt.Sprintf("This statement requires the %s role. Found: %s", minRole, stmtType), stmtType)
	}

	// Authorized write / DDL / destructive statement. The dangerous-keyword and
	// injection heuristics are intentionally skipped here: they exist to block exactly
	// these statements on the read path, and the role gate above is the authorization.
	// The single-statement guard already ran.
	return &SQLValidationResult{
		Valid:            true,
		StatementType:    stmtType,
		Warnings:         []string{},
		ReferencedTables: extractTables(stripped),
	}
}

func invalidStatement(code, msg, stmtType string) *SQLValidationResult {
	return &SQLValidationResult{
		Valid:         false,
		ErrorCode:     code,
		ErrorMessage:  msg,
		StatementType: stmtType,
	}
}
