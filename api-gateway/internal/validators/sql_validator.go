package validators

import (
	"fmt"
	"regexp"
	"strings"
)

// SQLValidationResult contains the result of SQL validation
type SQLValidationResult struct {
	Valid            bool     `json:"valid"`
	StatementType    string   `json:"statement_type"`
	ErrorCode        string   `json:"error_code,omitempty"`
	ErrorMessage     string   `json:"error_message,omitempty"`
	Warnings         []string `json:"warnings,omitempty"`
	HasLimit         bool     `json:"has_limit"`
	LimitValue       int      `json:"limit_value,omitempty"`
	ReferencedTables []string `json:"referenced_tables,omitempty"`
}

// SQLValidationError codes
const (
	ErrCodeNotSelectOnly     = "NOT_SELECT_ONLY"
	ErrCodeMultipleStatements = "MULTIPLE_STATEMENTS"
	ErrCodeDangerousKeyword  = "DANGEROUS_KEYWORD"
	ErrCodeEmptyQuery        = "EMPTY_QUERY"
	ErrCodeSQLInjection      = "SQL_INJECTION"
)

// ValidateExplorerSQL validates SQL for Explorer query execution
// Returns detailed validation result with error classification
func ValidateExplorerSQL(sql string) *SQLValidationResult {
	result := &SQLValidationResult{
		Valid:    true,
		Warnings: []string{},
	}

	// Step 1: Basic cleanup
	sql = strings.TrimSpace(sql)
	if sql == "" {
		result.Valid = false
		result.ErrorCode = ErrCodeEmptyQuery
		result.ErrorMessage = "Query cannot be empty"
		return result
	}

	// Step 2: Remove comments (single-line and multi-line)
	sql = removeComments(sql)
	sql = strings.TrimSpace(sql)

	if sql == "" {
		result.Valid = false
		result.ErrorCode = ErrCodeEmptyQuery
		result.ErrorMessage = "Query cannot be empty (only comments)"
		return result
	}

	// Step 3: Check for multiple statements
	if hasMultipleStatements(sql) {
		result.Valid = false
		result.ErrorCode = ErrCodeMultipleStatements
		result.ErrorMessage = "Multiple SQL statements are not allowed"
		return result
	}

	// Step 4: Identify statement type
	stmtType := identifyStatementType(sql)
	result.StatementType = stmtType

	// Step 5: Only allow SELECT and WITH (for CTEs)
	if stmtType != "SELECT" && stmtType != "WITH" {
		result.Valid = false
		result.ErrorCode = ErrCodeNotSelectOnly
		result.ErrorMessage = fmt.Sprintf("Only SELECT queries are allowed. Found: %s", stmtType)
		return result
	}

	// Step 6: Check for dangerous keywords even within SELECT
	if dangerous, keyword := hasDangerousKeyword(sql); dangerous {
		result.Valid = false
		result.ErrorCode = ErrCodeDangerousKeyword
		result.ErrorMessage = fmt.Sprintf("Query contains dangerous keyword: %s", keyword)
		return result
	}

	// Step 7: Check for SQL injection patterns
	if hasInjectionPattern(sql) {
		result.Valid = false
		result.ErrorCode = ErrCodeSQLInjection
		result.ErrorMessage = "Query contains suspicious patterns"
		return result
	}

	// Step 8: Extract LIMIT information
	result.HasLimit, result.LimitValue = extractLimit(sql)

	// Step 9: Extract referenced tables
	result.ReferencedTables = extractTables(sql)

	// Step 10: Add warnings
	if !result.HasLimit {
		result.Warnings = append(result.Warnings, "Query has no LIMIT clause - results will be capped")
	}

	if len(result.ReferencedTables) > 5 {
		result.Warnings = append(result.Warnings, "Query references many tables - may be slow")
	}

	return result
}

// removeComments removes SQL comments
func removeComments(sql string) string {
	// SQL-aware comment stripping: remove `-- ...` and `/* ... */` only when not inside
	// single-quoted strings or double-quoted identifiers.
	var out strings.Builder
	out.Grow(len(sql))

	inSingle := false
	inDouble := false
	inLineComment := false
	inBlockComment := false
	inExecComment := false

	for i := 0; i < len(sql); i++ {
		ch := sql[i]

		// End line comment at newline (preserve newline to keep statement boundaries reasonable)
		if inLineComment {
			if ch == '\n' {
				inLineComment = false
				out.WriteByte(ch)
			}
			continue
		}

		// End block comment at */
		if inBlockComment {
			if ch == '*' && i+1 < len(sql) && sql[i+1] == '/' {
				inBlockComment = false
				i++ // consume '/'
			}
			continue
		}

		// Inside single-quoted literal
		if inSingle {
			out.WriteByte(ch)
			if ch == '\'' {
				// SQL escaping: '' inside a string is an escaped quote
				if i+1 < len(sql) && sql[i+1] == '\'' {
					out.WriteByte(sql[i+1])
					i++
				} else {
					inSingle = false
				}
			}
			continue
		}

		// Inside double-quoted identifier
		if inDouble {
			out.WriteByte(ch)
			if ch == '"' {
				// Escaped double-quote inside identifier: ""
				if i+1 < len(sql) && sql[i+1] == '"' {
					out.WriteByte(sql[i+1])
					i++
				} else {
					inDouble = false
				}
			}
			continue
		}

		// Not inside any string/comment: detect comment starts
		if ch == '-' && i+1 < len(sql) && sql[i+1] == '-' {
			inLineComment = true
			i++ // consume second '-'
			continue
		}
		if ch == '/' && i+1 < len(sql) && sql[i+1] == '*' {
			// `/*!` is a MySQL/MariaDB executable comment, optionally version-gated
			// (`/*!40101 …`). Those engines RUN its contents — it is code wearing a
			// comment's syntax. Stripping it hid real statements from every check
			// built on this function: `SELECT 1; /*!40101 DROP TABLE customers */`
			// stripped down to `SELECT 1;`, so the single-statement rule saw one
			// statement and the classifier saw a read, and it validated for a viewer.
			// Keep the body and drop only the delimiters, so what we classify is what
			// the engine executes. PostgreSQL treats `/*!` as an ordinary comment, so
			// retaining the body there can only over-classify, which is the safe way
			// to be wrong.
			if i+2 < len(sql) && sql[i+2] == '!' {
				i += 2 // consume '*' and '!'
				for i+1 < len(sql) && sql[i+1] >= '0' && sql[i+1] <= '9' {
					i++ // consume the optional version gate
				}
				inExecComment = true
				continue
			}
			inBlockComment = true
			i++ // consume '*'
			continue
		}
		// Closing delimiter of an executable comment: the body was already kept.
		if inExecComment && ch == '*' && i+1 < len(sql) && sql[i+1] == '/' {
			inExecComment = false
			i++ // consume '/'
			continue
		}

		// Detect string starts
		if ch == '\'' {
			inSingle = true
			out.WriteByte(ch)
			continue
		}
		if ch == '"' {
			inDouble = true
			out.WriteByte(ch)
			continue
		}

		out.WriteByte(ch)
	}

	return out.String()
}

// hasMultipleStatements reports whether sql contains more than one statement
// (a stacked-statement / SQL-injection guard). A ';' only separates statements
// when it is OUTSIDE a string literal or quoted identifier.
//
// SECURITY: string-boundary tracking MUST match the destination engines'
// standard SQL semantics, NOT C-style backslash escaping. Under PostgreSQL
// (standard_conforming_strings=on, the default since 9.1), Redshift, and
// SQL Server, a backslash is an ORDINARY character and a literal quote is
// written by DOUBLING it (''). A guard that treats \' as an escaped quote
// diverges from the DB: it reads `… WHERE x='a\'; DROP TABLE t; SELECT '1'`
// as one string-laden statement while the DB closes the string at the quote
// and runs the stacked DROP — the exact bypass that let a workspace admin
// (gated to DML/DDL) execute owner-only DROP/TRUNCATE via PostgreSQL's
// multi-statement simple-query protocol. So reuse stripStringLiterals, which masks
// literal/identifier contents with the correct ''-doubling (no backslash
// special) walker; any ';' that survives is a real separator.
func hasMultipleStatements(sql string) bool {
	masked := stripStringLiterals(sql)
	// A single trailing ';' is a terminator, not a second statement.
	masked = strings.TrimSuffix(strings.TrimSpace(masked), ";")
	return strings.Contains(masked, ";")
}

// identifyStatementType identifies the SQL statement type
func identifyStatementType(sql string) string {
	upper := strings.ToUpper(strings.TrimSpace(sql))

	// Common statement type prefixes
	types := []struct {
		prefix string
		name   string
	}{
		{"SELECT", "SELECT"},
		{"WITH", "WITH"},
		{"INSERT", "INSERT"},
		{"UPDATE", "UPDATE"},
		{"DELETE", "DELETE"},
		{"MERGE", "MERGE"},
		{"DROP", "DROP"},
		{"CREATE", "CREATE"},
		{"ALTER", "ALTER"},
		{"TRUNCATE", "TRUNCATE"},
		{"GRANT", "GRANT"},
		{"REVOKE", "REVOKE"},
		{"EXEC", "EXEC"},
		{"EXECUTE", "EXECUTE"},
		{"CALL", "CALL"},
		{"SET", "SET"},
		{"COPY", "COPY"},
		{"VACUUM", "VACUUM"},
		{"ANALYZE", "ANALYZE"},
		{"EXPLAIN", "EXPLAIN"},
	}

	for _, t := range types {
		if strings.HasPrefix(upper, t.prefix) {
			// Verify it's a word boundary
			if len(upper) == len(t.prefix) || !isAlphanumeric(rune(upper[len(t.prefix)])) {
				return t.name
			}
		}
	}

	return "UNKNOWN"
}

// stripStringLiterals replaces the contents of single-quoted strings,
// double-quoted identifiers, and backtick-quoted identifiers with
// equal-length spaces. Used by hasDangerousKeyword + hasInjectionPattern
// before the regex scan so SQL keywords that appear inside literals
// don't trigger false positives.
//
// Without this, queries like:
//   SELECT 'How do I drop table foo?' AS faq
//   SELECT title FROM articles WHERE body LIKE '%insert into%'
//   SELECT id FROM commits WHERE message ILIKE 'truncate %'
// were being rejected as injection attempts even though the dangerous
// keyword lives inside a string literal — perfectly safe SQL.
//
// Mirrors the SQL-aware walking pattern in removeComments above.
// Preserves source length so any positional / line-column-based
// downstream logic stays accurate.
func stripStringLiterals(sql string) string {
	var out strings.Builder
	out.Grow(len(sql))

	inSingle := false
	inDouble := false
	inBacktick := false

	for i := 0; i < len(sql); i++ {
		ch := sql[i]

		// Inside single-quoted literal
		if inSingle {
			out.WriteByte(' ')
			if ch == '\'' {
				// SQL escape: '' is a literal quote, not end-of-string.
				if i+1 < len(sql) && sql[i+1] == '\'' {
					out.WriteByte(' ')
					i++
				} else {
					inSingle = false
				}
			}
			continue
		}

		// Inside double-quoted identifier
		if inDouble {
			out.WriteByte(' ')
			if ch == '"' {
				if i+1 < len(sql) && sql[i+1] == '"' {
					out.WriteByte(' ')
					i++
				} else {
					inDouble = false
				}
			}
			continue
		}

		// Inside backtick-quoted identifier (MySQL)
		if inBacktick {
			out.WriteByte(' ')
			if ch == '`' {
				inBacktick = false
			}
			continue
		}

		// Detect quote starts. Keep the opening quote so subsequent
		// dangerous-keyword detection can still match queries that
		// genuinely START with a keyword (the literal content is
		// what we're masking, not the quote markers themselves).
		switch ch {
		case '\'':
			inSingle = true
			out.WriteByte(ch)
		case '"':
			inDouble = true
			out.WriteByte(ch)
		case '`':
			inBacktick = true
			out.WriteByte(ch)
		default:
			out.WriteByte(ch)
		}
	}

	return out.String()
}

// hasDangerousKeyword checks for dangerous keywords in the query.
//
// D3 fix: scans the string-literal-stripped form so queries like
// `SELECT 'how do I drop table foo?' AS faq` aren't rejected for
// the word "drop" appearing inside a literal.
func hasDangerousKeyword(sql string) (bool, string) {
	upper := strings.ToUpper(stripStringLiterals(sql))

	// Dangerous keywords that should never appear (even in SELECT)
	dangerous := []string{
		"INSERT INTO",
		"UPDATE ",
		"DELETE FROM",
		// MERGE INTO belongs beside its three siblings above and its absence was
		// load-bearing: a CTE-prefixed MERGE classified as a read, reached this list,
		// and found nothing to stop it. ClassifyStatementSQL now classifies that
		// statement as dml_write so it never gets here — this is the backstop for the
		// next time something reclassifies a write as a read.
		"MERGE INTO",
		"DROP TABLE",
		"DROP DATABASE",
		"DROP SCHEMA",
		"DROP INDEX",
		"DROP VIEW",
		"CREATE TABLE",
		"CREATE DATABASE",
		"ALTER TABLE",
		"TRUNCATE ",
		"GRANT ",
		"REVOKE ",
		"EXEC ",
		"EXECUTE ",
		"CALL ",
		"COPY ",
		"\\COPY",
		"SET ROLE",
		"SET SESSION",
		"IMPORT ",
		"EXPORT ",
		"BACKUP ",
		"RESTORE ",
		"LOAD DATA",
		"INTO OUTFILE",
		"INTO DUMPFILE",
		"pg_read_file",
		"pg_write_file",
		"lo_import",
		"lo_export",
	}

	for _, kw := range dangerous {
		pattern := `\b` + regexp.QuoteMeta(kw)
		if matched, _ := regexp.MatchString(pattern, upper); matched {
			return true, strings.TrimSpace(kw)
		}
	}

	// Check for INTO clause in SELECT (SELECT INTO creates tables)
	if strings.Contains(upper, "SELECT") && hasSelectInto(upper) {
		return true, "SELECT INTO"
	}

	return false, ""
}

// hasSelectInto checks for SELECT INTO pattern
func hasSelectInto(sql string) bool {
	// Match SELECT ... INTO but not INSERT INTO or CAST(... INTO)
	// This is a simplified check
	intoPattern := regexp.MustCompile(`\bINTO\s+(TEMP|TEMPORARY)?\s*TABLE\b`)
	return intoPattern.MatchString(sql)
}

// hasInjectionPattern checks for common SQL injection patterns.
//
// D3 fix:
//   - keyword/structural patterns scan the string-literal-stripped
//     form so analyst queries like
//     `SELECT title FROM articles WHERE body LIKE '%insert into%'`
//     or `WHERE message ILIKE 'truncate %'` aren't rejected for
//     keywords appearing inside literals.
//   - dropped `UNION\s+(ALL\s+)?SELECT` — legitimate analytic SQL
//     (`SELECT ... UNION ALL SELECT ...`) is common in Explorer.
//   - dropped `concat\s*\(` — `CONCAT(first_name, ' ', last_name)` is
//     standard string building. Real concat-based injection still
//     requires a tautology or stacked statement, caught below.
//
// Tautology patterns intentionally scan the ORIGINAL SQL: in a
// classic `' OR '1'='1` injection the payload IS a string literal,
// so stripping literals would mask the very thing we want to find.
// The tradeoff: a literal that happens to contain `OR 1=1` text
// (a contrived FAQ example) is still blocked. Acceptable.
func hasInjectionPattern(sql string) bool {
	stripped := stripStringLiterals(sql)

	// Patterns scanned against the string-literal-stripped form.
	// These catch attempts to insert structural SQL (DDL/DML, hex,
	// char-encoding) anywhere in the query. Stripping makes sure a
	// keyword inside a quoted literal doesn't trigger them.
	strippedPatterns := []string{
		`;\s*(SELECT|INSERT|UPDATE|DELETE|DROP|CREATE|ALTER)`, // Chained statements
		`--\s*$`,             // Trailing comment
		`;\s*--`,             // Statement + comment
		`\b0x[0-9a-fA-F]+\b`, // Hex-encoding bypass
		`\bchar\s*\(\s*\d+`,  // CHAR(NN) encoding bypass
	}
	for _, pattern := range strippedPatterns {
		if matched, _ := regexp.MatchString("(?i)"+pattern, stripped); matched {
			return true
		}
	}

	// Tautology patterns scanned against the ORIGINAL SQL — the
	// injection payload lives INSIDE the literal, so stripping
	// would mask the signal.
	tautologyPatterns := []string{
		`'\s*OR\s+'?1'?\s*=\s*'?1'?\s*'?`, // ' OR '1'='1'
		`\bOR\s+'?1'?\s*=\s*'?1'?\b`,      // OR '1'='1' / OR 1=1
		`\bOR\s+1\s*=\s*1\b`,              // OR 1=1
		`'\s*OR\s+'?true'?\b`,             // ' OR true
		`\bOR\s+true\b`,                   // OR true
	}
	for _, pattern := range tautologyPatterns {
		if matched, _ := regexp.MatchString("(?i)"+pattern, sql); matched {
			return true
		}
	}

	return false
}

// extractLimit extracts the LIMIT value from the query
func extractLimit(sql string) (bool, int) {
	// Match LIMIT N or LIMIT N OFFSET M
	pattern := regexp.MustCompile(`(?i)\bLIMIT\s+(\d+)`)
	matches := pattern.FindStringSubmatch(sql)

	if len(matches) > 1 {
		var limit int
		fmt.Sscanf(matches[1], "%d", &limit)
		return true, limit
	}

	return false, 0
}

// extractTables extracts table names from the query
func extractTables(sql string) []string {
	tables := make(map[string]bool)

	// Match FROM table and JOIN table patterns
	patterns := []string{
		`(?i)\bFROM\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)`,
		`(?i)\bJOIN\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)`,
	}

	for _, pattern := range patterns {
		re := regexp.MustCompile(pattern)
		matches := re.FindAllStringSubmatch(sql, -1)
		for _, match := range matches {
			if len(match) > 1 {
				tableName := strings.ToLower(match[1])
				// Skip common SQL keywords that might be matched
				if tableName != "select" && tableName != "where" && tableName != "and" {
					tables[tableName] = true
				}
			}
		}
	}

	result := make([]string, 0, len(tables))
	for t := range tables {
		result = append(result, t)
	}

	return result
}

// isAlphanumeric checks if a character is alphanumeric or underscore
func isAlphanumeric(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_'
}

// ClampLimit returns SQL with LIMIT clamped to maxLimit
func ClampLimit(sql string, maxLimit int) string {
	hasLimit, existingLimit := extractLimit(sql)

	if !hasLimit {
		// Add LIMIT
		sql = strings.TrimSuffix(strings.TrimSpace(sql), ";")
		return sql + fmt.Sprintf(" LIMIT %d", maxLimit)
	}

	if existingLimit > maxLimit {
		// Replace with clamped value
		pattern := regexp.MustCompile(`(?i)\bLIMIT\s+\d+`)
		return pattern.ReplaceAllString(sql, fmt.Sprintf("LIMIT %d", maxLimit))
	}

	return sql
}

// ClassifyExecutionError classifies database execution errors for better user feedback
func ClassifyExecutionError(err error) (errorType string, suggestion string) {
	errStr := strings.ToLower(err.Error())

	switch {
	case strings.Contains(errStr, "does not exist") ||
		strings.Contains(errStr, "relation") && strings.Contains(errStr, "does not exist"):
		return "missing_table_or_column", "The referenced table or column may have been renamed or deleted. Try refreshing the schema."

	case strings.Contains(errStr, "column") && strings.Contains(errStr, "does not exist"):
		return "missing_table_or_column", "The referenced column does not exist. Try refreshing the schema."

	case strings.Contains(errStr, "timeout") ||
		strings.Contains(errStr, "cancel") ||
		strings.Contains(errStr, "deadline"):
		return "timeout", "Query took too long. Try adding filters or reducing the time range."

	case strings.Contains(errStr, "permission denied") ||
		strings.Contains(errStr, "access denied"):
		return "permission_denied", "You don't have permission to access this data. Contact your administrator."

	case strings.Contains(errStr, "connection") ||
		strings.Contains(errStr, "network") ||
		strings.Contains(errStr, "refused"):
		return "network_or_unavailable", "Database is unavailable. Please try again in a moment."

	// Match both Postgres ("syntax error at or near") and MySQL 1064
	// ("you have an error in your SQL syntax").
	case strings.Contains(errStr, "syntax"):
		return "syntax_error", "The SQL query has a syntax error. Please check the query."

	default:
		return "unknown", "An unexpected error occurred. Please try again."
	}
}
