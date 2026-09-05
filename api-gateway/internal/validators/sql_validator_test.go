package validators

import (
	"strings"
	"testing"
)

func TestValidateExplorerSQL(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		wantValid  bool
		wantCode   string
	}{
		// Valid SELECT queries
		{
			name:      "simple select",
			sql:       "SELECT * FROM users",
			wantValid: true,
		},
		{
			name:      "select with columns",
			sql:       "SELECT id, name, email FROM users WHERE active = true",
			wantValid: true,
		},
		{
			name:      "select with join",
			sql:       "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id",
			wantValid: true,
		},
		{
			name:      "select with CTE",
			sql:       "WITH active_users AS (SELECT * FROM users WHERE active = true) SELECT * FROM active_users",
			wantValid: true,
		},
		{
			name:      "select with aggregation",
			sql:       "SELECT COUNT(*), AVG(total) FROM orders GROUP BY user_id",
			wantValid: true,
		},
		{
			name:      "select with subquery",
			sql:       "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)",
			wantValid: true,
		},
		{
			name:      "select with limit",
			sql:       "SELECT * FROM users LIMIT 100",
			wantValid: true,
		},
		{
			name:      "select with trailing semicolon",
			sql:       "SELECT * FROM users;",
			wantValid: true,
		},

		// Invalid - non-SELECT statements
		{
			name:      "insert statement",
			sql:       "INSERT INTO users (name) VALUES ('test')",
			wantValid: false,
			wantCode:  ErrCodeNotSelectOnly,
		},
		{
			name:      "update statement",
			sql:       "UPDATE users SET name = 'test' WHERE id = 1",
			wantValid: false,
			wantCode:  ErrCodeNotSelectOnly,
		},
		{
			name:      "delete statement",
			sql:       "DELETE FROM users WHERE id = 1",
			wantValid: false,
			wantCode:  ErrCodeNotSelectOnly,
		},
		{
			name:      "drop table",
			sql:       "DROP TABLE users",
			wantValid: false,
			wantCode:  ErrCodeNotSelectOnly,
		},
		{
			name:      "create table",
			sql:       "CREATE TABLE test (id INT)",
			wantValid: false,
			wantCode:  ErrCodeNotSelectOnly,
		},
		{
			name:      "truncate",
			sql:       "TRUNCATE TABLE users",
			wantValid: false,
			wantCode:  ErrCodeNotSelectOnly,
		},

		// Invalid - dangerous keywords in SELECT
		{
			name:      "select into table",
			sql:       "SELECT * INTO TEMP TABLE temp_users FROM users",
			wantValid: false,
			wantCode:  ErrCodeDangerousKeyword,
		},
		{
			name:      "grant statement",
			sql:       "GRANT SELECT ON users TO public",
			wantValid: false,
			wantCode:  ErrCodeNotSelectOnly,
		},

		// Invalid - multiple statements
		{
			name:      "multiple statements",
			sql:       "SELECT * FROM users; DROP TABLE users",
			wantValid: false,
			wantCode:  ErrCodeMultipleStatements,
		},
		{
			name:      "statement injection attempt",
			sql:       "SELECT * FROM users; DELETE FROM users",
			wantValid: false,
			wantCode:  ErrCodeMultipleStatements,
		},
		{
			// Regression: backslash-quote must not hide the stacked statements
			// from the read-path guard either (standard_conforming_strings=on).
			name:      "backslash-quote stacked injection",
			sql:       `SELECT * FROM users WHERE name='a\'; DROP TABLE users; SELECT '1'`,
			wantValid: false,
			wantCode:  ErrCodeMultipleStatements,
		},

		// Invalid - SQL injection patterns
		{
			name:      "or 1=1 injection",
			sql:       "SELECT * FROM users WHERE id = 1 OR '1'='1'",
			wantValid: false,
			wantCode:  ErrCodeSQLInjection,
		},
		// UNION queries are legitimate analytics SQL — the validator
		// relies on `SELECT`/`WITH` gating + multi-statement detection
		// to keep the surface safe, not on banning UNION outright.
		{
			name:      "union is legitimate analytics",
			sql:       "SELECT * FROM users UNION SELECT * FROM admins",
			wantValid: true,
		},
		{
			name:      "union all is legitimate analytics",
			sql:       "SELECT name FROM users UNION ALL SELECT name FROM contacts",
			wantValid: true,
		},

		// D3 false-positive regressions: dangerous keywords appearing
		// inside string literals must NOT trip the validator. These
		// queries are common in analytics (FAQ tables, audit logs,
		// search-as-you-type).
		{
			name:      "drop keyword inside literal is safe",
			sql:       "SELECT 'How do I drop table foo?' AS faq FROM helpdesk",
			wantValid: true,
		},
		{
			name:      "insert keyword inside literal is safe",
			sql:       "SELECT id FROM articles WHERE body LIKE '%insert into%'",
			wantValid: true,
		},
		{
			name:      "truncate keyword inside literal is safe",
			sql:       "SELECT id FROM commits WHERE message ILIKE 'truncate %'",
			wantValid: true,
		},
		{
			name:      "union keyword inside literal is safe",
			sql:       "SELECT 'union select' AS example FROM cheatsheet",
			wantValid: true,
		},
		{
			name:      "concat function is legitimate string building",
			sql:       "SELECT CONCAT(first_name, ' ', last_name) AS full_name FROM users",
			wantValid: true,
		},

		// D3 confirms the dangerous patterns we KEEP still fire when
		// they actually represent attacks.
		{
			name:      "char-encoding bypass is still blocked",
			sql:       "SELECT name FROM users WHERE name = CHAR(97) || CHAR(100) || CHAR(109)",
			wantValid: false,
			wantCode:  ErrCodeSQLInjection,
		},
		{
			name:      "hex-encoding bypass is still blocked",
			sql:       "SELECT name FROM users WHERE name = 0x61646d696e",
			wantValid: false,
			wantCode:  ErrCodeSQLInjection,
		},
		{
			name:      "chained drop after SELECT is still blocked",
			sql:       "SELECT * FROM users; DROP TABLE secrets",
			wantValid: false,
			// Caught earlier by multi-statement check, which is fine.
			wantCode: ErrCodeMultipleStatements,
		},

		// Edge cases
		{
			name:      "empty query",
			sql:       "",
			wantValid: false,
			wantCode:  ErrCodeEmptyQuery,
		},
		{
			name:      "only whitespace",
			sql:       "   \n\t  ",
			wantValid: false,
			wantCode:  ErrCodeEmptyQuery,
		},
		{
			name:      "only comments",
			sql:       "-- this is a comment",
			wantValid: false,
			wantCode:  ErrCodeEmptyQuery,
		},
		{
			name:      "select with single line comment",
			sql:       "SELECT * FROM users -- get all users",
			wantValid: true,
		},
		{
			name:      "select with multi-line comment",
			sql:       "SELECT /* columns */ * FROM users",
			wantValid: true,
		},
		{
			name:      "comment markers inside string literal are preserved",
			sql:       "SELECT '--not a comment' AS s, '/* also not */' AS t FROM users",
			wantValid: true,
		},
		{
			name:      "semicolon inside string does not count as multiple statements",
			sql:       "SELECT ';' AS semi FROM users",
			wantValid: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ValidateExplorerSQL(tt.sql)
			
			if result.Valid != tt.wantValid {
				t.Errorf("ValidateExplorerSQL(%q) valid = %v, want %v", tt.sql, result.Valid, tt.wantValid)
			}
			
			if !tt.wantValid && tt.wantCode != "" && result.ErrorCode != tt.wantCode {
				t.Errorf("ValidateExplorerSQL(%q) errorCode = %v, want %v", tt.sql, result.ErrorCode, tt.wantCode)
			}
		})
	}
}

func TestClampLimit(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		maxLimit int
		want     string
	}{
		{
			name:     "add limit to query without limit",
			sql:      "SELECT * FROM users",
			maxLimit: 100,
			want:     "SELECT * FROM users LIMIT 100",
		},
		{
			name:     "clamp existing limit",
			sql:      "SELECT * FROM users LIMIT 1000",
			maxLimit: 100,
			want:     "SELECT * FROM users LIMIT 100",
		},
		{
			name:     "keep existing limit if lower",
			sql:      "SELECT * FROM users LIMIT 50",
			maxLimit: 100,
			want:     "SELECT * FROM users LIMIT 50",
		},
		{
			name:     "remove trailing semicolon and add limit",
			sql:      "SELECT * FROM users;",
			maxLimit: 100,
			want:     "SELECT * FROM users LIMIT 100",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ClampLimit(tt.sql, tt.maxLimit)
			if got != tt.want {
				t.Errorf("ClampLimit(%q, %d) = %q, want %q", tt.sql, tt.maxLimit, got, tt.want)
			}
		})
	}
}

func TestClassifyExecutionError(t *testing.T) {
	tests := []struct {
		name         string
		errMsg       string
		wantType     string
		wantContains string
	}{
		{
			name:         "missing table",
			errMsg:       "pq: relation \"nonexistent\" does not exist",
			wantType:     "missing_table_or_column",
			wantContains: "refresh",
		},
		{
			name:         "missing column",
			errMsg:       "pq: column \"bad_column\" does not exist",
			wantType:     "missing_table_or_column",
			wantContains: "refresh",
		},
		{
			name:         "timeout",
			errMsg:       "context deadline exceeded",
			wantType:     "timeout",
			wantContains: "filter",
		},
		{
			name:         "permission denied",
			errMsg:       "permission denied for table users",
			wantType:     "permission_denied",
			wantContains: "administrator",
		},
		{
			name:         "connection refused",
			errMsg:       "connection refused",
			wantType:     "network_or_unavailable",
			wantContains: "try again",
		},
		{
			name:         "syntax error",
			errMsg:       "syntax error at or near \"SELEC\"",
			wantType:     "syntax_error",
			wantContains: "syntax",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errType, suggestion := ClassifyExecutionError(errMock{msg: tt.errMsg})
			
			if errType != tt.wantType {
				t.Errorf("ClassifyExecutionError() type = %v, want %v", errType, tt.wantType)
			}
			
			if tt.wantContains != "" && !containsIgnoreCase(suggestion, tt.wantContains) {
				t.Errorf("ClassifyExecutionError() suggestion = %v, want to contain %v", suggestion, tt.wantContains)
			}
		})
	}
}

// errMock is a simple error mock for testing
type errMock struct {
	msg string
}

func (e errMock) Error() string {
	return e.msg
}

func containsIgnoreCase(s, substr string) bool {
	return strings.Contains(strings.ToLower(s), strings.ToLower(substr))
}

func TestExtractTables(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected []string
	}{
		{
			name:     "single table",
			sql:      "SELECT * FROM users",
			expected: []string{"users"},
		},
		{
			name:     "multiple tables",
			sql:      "SELECT * FROM users JOIN orders ON users.id = orders.user_id",
			expected: []string{"users", "orders"},
		},
		{
			name:     "schema qualified",
			sql:      "SELECT * FROM public.users",
			expected: []string{"public.users"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tables := extractTables(tt.sql)
			
			if len(tables) != len(tt.expected) {
				t.Errorf("extractTables(%q) returned %d tables, want %d", tt.sql, len(tables), len(tt.expected))
				return
			}
			
			for _, exp := range tt.expected {
				found := false
				for _, got := range tables {
					if got == exp {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("extractTables(%q) missing expected table %q", tt.sql, exp)
				}
			}
		})
	}
}
