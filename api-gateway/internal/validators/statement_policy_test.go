package validators

import (
	"testing"

	"api-gateway/internal/security"
)

func TestClassifyStatement(t *testing.T) {
	cases := map[string]StatementClass{
		"SELECT":   ClassRead,
		"WITH":     ClassRead,
		"INSERT":   ClassDMLWrite,
		"UPDATE":   ClassDMLWrite,
		"DELETE":   ClassDMLWrite,
		"MERGE":    ClassDMLWrite,
		"CREATE":   ClassDDL,
		"ALTER":    ClassDDL,
		"DROP":     ClassDestructive,
		"TRUNCATE": ClassDestructive,
		"GRANT":    ClassBlocked,
		"REVOKE":   ClassBlocked,
		"CALL":     ClassBlocked,
		"EXEC":     ClassBlocked,
		"EXECUTE":  ClassBlocked,
		"SET":      ClassBlocked,
		"COPY":     ClassBlocked,
		"VACUUM":   ClassBlocked,
		"EXPLAIN":  ClassBlocked,
		"SHOW":     ClassBlocked,
		"DESCRIBE": ClassBlocked,
		"UNKNOWN":  ClassUnknown,
		"PRAGMA":   ClassUnknown,
		"":         ClassUnknown,
	}
	for stmtType, want := range cases {
		if got := ClassifyStatement(stmtType); got != want {
			t.Errorf("ClassifyStatement(%q) = %q, want %q", stmtType, got, want)
		}
	}
}

// TestClassifyStatementSQL_AlterDrop pins the invariant that a statement is classified
// by what it does, not by the word it starts with. `ALTER TABLE t DROP COLUMN c` is as
// irreversible as `DROP TABLE t`; before this it classified as ClassDDL, so it ran at
// admin with no destructive-confirm prompt.
func TestClassifyStatementSQL_AlterDrop(t *testing.T) {
	cases := []struct {
		sql  string
		want StatementClass
	}{
		// The gap this closes.
		{"ALTER TABLE pipeline_test.cdc_drift DROP COLUMN note", ClassDestructive},
		{"alter table t drop column c", ClassDestructive},
		{"ALTER TABLE t DROP CONSTRAINT t_pkey", ClassDestructive},
		{"ALTER TABLE t DROP PARTITION p2024", ClassDestructive},

		// Non-destructive ALTERs stay ordinary DDL (admin, no confirm prompt).
		{"ALTER TABLE t ADD COLUMN c INT", ClassDDL},
		{"ALTER TABLE t RENAME TO t2", ClassDDL},
		{"ALTER TABLE t ALTER COLUMN c TYPE TEXT", ClassDDL},
		// `drop` inside an identifier must not trip the word-boundary match.
		{"ALTER TABLE t ADD COLUMN drop_reason TEXT", ClassDDL},
		{"ALTER TABLE t ADD COLUMN backdrop TEXT", ClassDDL},

		// Every other class is unchanged by the SQL-aware path.
		{"SELECT 1", ClassRead},
		{"CREATE TABLE t (id INT)", ClassDDL},
		{"DROP TABLE t", ClassDestructive},
		{"TRUNCATE TABLE t", ClassDestructive},
		{"INSERT INTO t VALUES (1)", ClassDMLWrite},
		{"GRANT ALL ON t TO u", ClassBlocked},
	}
	for _, tc := range cases {
		if got := ClassifyStatementSQL(tc.sql); got != tc.want {
			t.Errorf("ClassifyStatementSQL(%q) = %q, want %q", tc.sql, got, tc.want)
		}
	}
}

// TestValidateExplorerStatement_AlterDropRequiresOwner proves the class change is
// actually load-bearing at the authorization gate: admin could run ALTER … DROP COLUMN
// before, owner is required now.
func TestValidateExplorerStatement_AlterDropRequiresOwner(t *testing.T) {
	const alterDrop = "ALTER TABLE t DROP COLUMN c"

	if res := ValidateExplorerStatement(alterDrop, security.WSAdmin); res.Valid {
		t.Errorf("admin should no longer be able to run %q", alterDrop)
	} else if res.ErrorCode != ErrCodeInsufficientRole {
		t.Errorf("ErrorCode = %q, want %q", res.ErrorCode, ErrCodeInsufficientRole)
	}

	if res := ValidateExplorerStatement(alterDrop, security.WSOwner); !res.Valid {
		t.Errorf("owner should be able to run %q, got %q", alterDrop, res.ErrorMessage)
	}

	// A non-destructive ALTER must still work at admin — this fix must not tighten
	// the ordinary DDL path.
	if res := ValidateExplorerStatement("ALTER TABLE t ADD COLUMN c INT", security.WSAdmin); !res.Valid {
		t.Errorf("admin must still run additive ALTER, got %q", res.ErrorMessage)
	}
}

func TestMinRoleForClass(t *testing.T) {
	cases := []struct {
		class       StatementClass
		wantMin     security.WorkspaceRole
		wantBlocked bool
	}{
		{ClassRead, security.WSViewer, false},
		{ClassDMLWrite, security.WSAdmin, false},
		{ClassDDL, security.WSAdmin, false},
		{ClassDestructive, security.WSOwner, false},
		{ClassUnknown, security.WSOwner, false},
		{ClassBlocked, "", true},
	}
	for _, tc := range cases {
		gotMin, gotBlocked := MinRoleForClass(tc.class)
		if gotBlocked != tc.wantBlocked || gotMin != tc.wantMin {
			t.Errorf("MinRoleForClass(%q) = (%q, %v), want (%q, %v)",
				tc.class, gotMin, gotBlocked, tc.wantMin, tc.wantBlocked)
		}
	}
}

func TestIsWriteClass(t *testing.T) {
	writes := []StatementClass{ClassDMLWrite, ClassDDL, ClassDestructive, ClassUnknown}
	for _, c := range writes {
		if !IsWriteClass(c) {
			t.Errorf("IsWriteClass(%q) = false, want true", c)
		}
	}
	for _, c := range []StatementClass{ClassRead, ClassBlocked} {
		if IsWriteClass(c) {
			t.Errorf("IsWriteClass(%q) = true, want false", c)
		}
	}
}

// TestValidateExplorerStatementMatrix exercises the full class × role permission
// matrix: every statement type against every workspace role, asserting validity and
// the error classification. This is the crux of the feature and must stay exhaustive.
func TestValidateExplorerStatementMatrix(t *testing.T) {
	roles := []security.WorkspaceRole{security.WSViewer, security.WSMember, security.WSAdmin, security.WSOwner}

	type expect struct {
		valid bool
		code  string
	}

	// For each SQL sample, the expected outcome per role (viewer, member, admin, owner).
	tests := []struct {
		name string
		sql  string
		want map[security.WorkspaceRole]expect
	}{
		{
			name: "SELECT allowed for all roles",
			sql:  "SELECT * FROM users",
			want: map[security.WorkspaceRole]expect{
				security.WSViewer: {true, ""},
				security.WSMember: {true, ""},
				security.WSAdmin:  {true, ""},
				security.WSOwner:  {true, ""},
			},
		},
		{
			name: "CTE (WITH) allowed for all roles",
			sql:  "WITH t AS (SELECT 1) SELECT * FROM t",
			want: map[security.WorkspaceRole]expect{
				security.WSViewer: {true, ""},
				security.WSMember: {true, ""},
				security.WSAdmin:  {true, ""},
				security.WSOwner:  {true, ""},
			},
		},
		{
			name: "INSERT requires admin",
			sql:  "INSERT INTO users (name) VALUES ('a')",
			want: map[security.WorkspaceRole]expect{
				security.WSViewer: {false, ErrCodeInsufficientRole},
				security.WSMember: {false, ErrCodeInsufficientRole},
				security.WSAdmin:  {true, ""},
				security.WSOwner:  {true, ""},
			},
		},
		{
			name: "UPDATE requires admin",
			sql:  "UPDATE users SET name = 'b' WHERE id = 1",
			want: map[security.WorkspaceRole]expect{
				security.WSViewer: {false, ErrCodeInsufficientRole},
				security.WSMember: {false, ErrCodeInsufficientRole},
				security.WSAdmin:  {true, ""},
				security.WSOwner:  {true, ""},
			},
		},
		{
			name: "DELETE requires admin",
			sql:  "DELETE FROM users WHERE id = 1",
			want: map[security.WorkspaceRole]expect{
				security.WSViewer: {false, ErrCodeInsufficientRole},
				security.WSMember: {false, ErrCodeInsufficientRole},
				security.WSAdmin:  {true, ""},
				security.WSOwner:  {true, ""},
			},
		},
		{
			name: "MERGE requires admin",
			sql:  "MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.v = s.v",
			want: map[security.WorkspaceRole]expect{
				security.WSViewer: {false, ErrCodeInsufficientRole},
				security.WSMember: {false, ErrCodeInsufficientRole},
				security.WSAdmin:  {true, ""},
				security.WSOwner:  {true, ""},
			},
		},
		{
			name: "CREATE (DDL) requires admin",
			sql:  "CREATE TABLE t (id INT)",
			want: map[security.WorkspaceRole]expect{
				security.WSViewer: {false, ErrCodeInsufficientRole},
				security.WSMember: {false, ErrCodeInsufficientRole},
				security.WSAdmin:  {true, ""},
				security.WSOwner:  {true, ""},
			},
		},
		{
			name: "ALTER (DDL) requires admin",
			sql:  "ALTER TABLE t ADD COLUMN c INT",
			want: map[security.WorkspaceRole]expect{
				security.WSViewer: {false, ErrCodeInsufficientRole},
				security.WSMember: {false, ErrCodeInsufficientRole},
				security.WSAdmin:  {true, ""},
				security.WSOwner:  {true, ""},
			},
		},
		{
			name: "DROP (destructive) requires owner",
			sql:  "DROP TABLE t",
			want: map[security.WorkspaceRole]expect{
				security.WSViewer: {false, ErrCodeInsufficientRole},
				security.WSMember: {false, ErrCodeInsufficientRole},
				security.WSAdmin:  {false, ErrCodeInsufficientRole},
				security.WSOwner:  {true, ""},
			},
		},
		{
			name: "TRUNCATE (destructive) requires owner",
			sql:  "TRUNCATE TABLE t",
			want: map[security.WorkspaceRole]expect{
				security.WSViewer: {false, ErrCodeInsufficientRole},
				security.WSMember: {false, ErrCodeInsufficientRole},
				security.WSAdmin:  {false, ErrCodeInsufficientRole},
				security.WSOwner:  {true, ""},
			},
		},
		{
			name: "GRANT blocked for everyone including owner",
			sql:  "GRANT SELECT ON t TO analyst",
			want: map[security.WorkspaceRole]expect{
				security.WSViewer: {false, ErrCodeStatementBlocked},
				security.WSMember: {false, ErrCodeStatementBlocked},
				security.WSAdmin:  {false, ErrCodeStatementBlocked},
				security.WSOwner:  {false, ErrCodeStatementBlocked},
			},
		},
		{
			name: "EXPLAIN blocked (read surface stays SELECT/WITH only)",
			sql:  "EXPLAIN SELECT * FROM users",
			want: map[security.WorkspaceRole]expect{
				security.WSViewer: {false, ErrCodeStatementBlocked},
				security.WSOwner:  {false, ErrCodeStatementBlocked},
			},
		},
		{
			name: "unrecognized statement fails closed to owner-only",
			sql:  "PRAGMA foreign_keys = ON",
			want: map[security.WorkspaceRole]expect{
				security.WSViewer: {false, ErrCodeInsufficientRole},
				security.WSMember: {false, ErrCodeInsufficientRole},
				security.WSAdmin:  {false, ErrCodeInsufficientRole},
				security.WSOwner:  {true, ""},
			},
		},
		{
			name: "stacked statements blocked even for owner",
			sql:  "UPDATE users SET name='x' WHERE id=1; DROP TABLE users",
			want: map[security.WorkspaceRole]expect{
				security.WSAdmin: {false, ErrCodeMultipleStatements},
				security.WSOwner: {false, ErrCodeMultipleStatements},
			},
		},
		{
			// Regression: backslash-quote must NOT hide a stacked statement.
			// Under standard_conforming_strings=on the DB closes the string at
			// the quote and runs the DROP; the guard must see the ';' too, so a
			// DML-gated admin cannot smuggle an owner-only DROP.
			name: "backslash-quote stacked injection blocked (admin cannot escalate to DROP)",
			sql:  `UPDATE users SET name='x\'; DROP TABLE users; SELECT '1'`,
			want: map[security.WorkspaceRole]expect{
				security.WSAdmin: {false, ErrCodeMultipleStatements},
				security.WSOwner: {false, ErrCodeMultipleStatements},
			},
		},
		{
			// Companion: a legitimate single statement using SQL ''-doubling to
			// embed a quote must still pass (no false positive from the fix).
			name: "doubled-quote literal is a single valid statement",
			sql:  "UPDATE users SET name='O''Brien' WHERE id=1",
			want: map[security.WorkspaceRole]expect{
				security.WSAdmin: {true, ""},
				security.WSOwner: {true, ""},
			},
		},
		{
			name: "empty query rejected",
			sql:  "   ",
			want: map[security.WorkspaceRole]expect{
				security.WSOwner: {false, ErrCodeEmptyQuery},
			},
		},
	}

	_ = roles
	for _, tt := range tests {
		for role, exp := range tt.want {
			t.Run(tt.name+"/"+string(role), func(t *testing.T) {
				res := ValidateExplorerStatement(tt.sql, role)
				if res.Valid != exp.valid {
					t.Fatalf("ValidateExplorerStatement(%q, %q) valid = %v, want %v (code=%q msg=%q)",
						tt.sql, role, res.Valid, exp.valid, res.ErrorCode, res.ErrorMessage)
				}
				if !exp.valid && res.ErrorCode != exp.code {
					t.Fatalf("ValidateExplorerStatement(%q, %q) code = %q, want %q",
						tt.sql, role, res.ErrorCode, exp.code)
				}
			})
		}
	}
}

// TestValidateExplorerStatementUnknownRoleFailsClosed: an empty/garbage role never
// satisfies a write gate.
func TestValidateExplorerStatementUnknownRoleFailsClosed(t *testing.T) {
	for _, bad := range []security.WorkspaceRole{"", "superuser", "root"} {
		res := ValidateExplorerStatement("INSERT INTO t (a) VALUES (1)", bad)
		if res.Valid {
			t.Errorf("INSERT with role %q unexpectedly allowed", bad)
		}
		if res.ErrorCode != ErrCodeInsufficientRole {
			t.Errorf("INSERT with role %q code = %q, want %q", bad, res.ErrorCode, ErrCodeInsufficientRole)
		}
	}
}

// A CTE is the one construct where the leading verb lies about what the statement
// does. `WITH s AS (SELECT 1) MERGE INTO invoices …` leads with WITH, so a verb-only
// classifier files it as a read — and the read path's dangerous-keyword list, which
// was written for the SELECT pipeline and happens to name INSERT INTO / UPDATE /
// DELETE FROM but not MERGE, let it through at viewer role. These cases pin both
// halves: the class must follow the write, and benign CTEs must stay reads.
func TestClassifyStatementSQL_CTEWrites(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want StatementClass
	}{
		// The reported hole: a CTE prefix in front of each write verb.
		{"CTE-prefixed MERGE", "WITH s AS (SELECT 1 AS id) MERGE INTO invoices t USING s ON t.id = s.id WHEN MATCHED THEN DELETE", ClassDMLWrite},
		{"CTE-prefixed INSERT", "WITH s AS (SELECT 1) INSERT INTO audit SELECT * FROM s", ClassDMLWrite},
		{"CTE-prefixed UPDATE", "WITH s AS (SELECT 1) UPDATE invoices SET paid = true", ClassDMLWrite},
		{"CTE-prefixed DELETE", "WITH s AS (SELECT 1) DELETE FROM invoices", ClassDMLWrite},
		{"CTE-prefixed DROP is destructive", "WITH s AS (SELECT 1) DROP TABLE invoices", ClassDestructive},
		{"CTE-prefixed TRUNCATE is destructive", "WITH s AS (SELECT 1) TRUNCATE TABLE invoices", ClassDestructive},
		{"multiple CTEs then MERGE", "WITH a AS (SELECT 1), b AS (SELECT 2) MERGE INTO t USING a ON 1 = 1 WHEN MATCHED THEN UPDATE SET x = 1", ClassDMLWrite},
		{"lowercase and newlines", "with s as (select 1)\n  merge into t using s on 1 = 1 when matched then delete", ClassDMLWrite},

		// Data-modifying CTEs: the write is inside the parens, the main statement is
		// a SELECT. Previously refused only as a "dangerous keyword" on the read path.
		{"data-modifying CTE DELETE", "WITH gone AS (DELETE FROM invoices RETURNING *) SELECT * FROM gone", ClassDMLWrite},
		{"data-modifying CTE INSERT", "WITH n AS (INSERT INTO audit (x) VALUES (1) RETURNING *) SELECT * FROM n", ClassDMLWrite},
		{"data-modifying CTE UPDATE", "WITH u AS (UPDATE invoices SET paid = true RETURNING *) SELECT * FROM u", ClassDMLWrite},

		// Benign CTEs must stay reads — this is the whole read surface for viewers.
		{"plain CTE", "WITH t AS (SELECT 1) SELECT * FROM t", ClassRead},
		{"CTE with column list", "WITH t (a, b) AS (SELECT 1, 2) SELECT * FROM t", ClassRead},
		{"RECURSIVE CTE", "WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r WHERE n < 5) SELECT * FROM r", ClassRead},
		{"MATERIALIZED CTE", "WITH s AS MATERIALIZED (SELECT 1) SELECT * FROM s", ClassRead},
		{"NOT MATERIALIZED CTE", "WITH s AS NOT MATERIALIZED (SELECT 1) SELECT * FROM s", ClassRead},
		{"nested subquery in CTE body", "WITH s AS (SELECT (SELECT 1) AS x) SELECT * FROM s", ClassRead},
		{"identifier prefixed with a verb name", "WITH selection AS (SELECT 1) SELECT * FROM selection", ClassRead},
		{"column names containing verb names", "WITH s AS (SELECT delete_count, update_time, merge_id FROM audit) SELECT * FROM s", ClassRead},

		// A write verb that is only ever text must not escalate the class.
		{"write verb inside a string literal", "WITH s AS (SELECT 'MERGE INTO x' AS note) SELECT * FROM s", ClassRead},
		{"write verb inside a comment", "WITH s AS (SELECT 1) /* MERGE INTO t */ SELECT * FROM s", ClassRead},

		// Unchanged behavior for statements that do not lead with WITH.
		{"bare SELECT", "SELECT * FROM users", ClassRead},
		{"bare MERGE", "MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE SET x = 1", ClassDMLWrite},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ClassifyStatementSQL(tt.sql); got != tt.want {
				t.Errorf("ClassifyStatementSQL() = %q, want %q\n  sql: %s", got, tt.want, tt.sql)
			}
		})
	}
}

// The class is only half the fix — this pins the role outcome a viewer actually
// hits, which is what the vulnerability was.
func TestValidateExplorerStatement_CTEWriteNeedsAdmin(t *testing.T) {
	const exploit = "WITH s AS (SELECT 1 AS id) MERGE INTO invoices t USING s ON t.id = s.id WHEN MATCHED THEN DELETE"

	for _, role := range []security.WorkspaceRole{security.WSViewer, security.WSMember} {
		res := ValidateExplorerStatement(exploit, role)
		if res.Valid {
			t.Errorf("CTE-prefixed MERGE was accepted for role %q — a viewer must not be able to write", role)
		}
		if res.ErrorCode != ErrCodeInsufficientRole {
			t.Errorf("role %q: ErrorCode = %q, want %q (a role refusal, not a keyword refusal)", role, res.ErrorCode, ErrCodeInsufficientRole)
		}
	}

	// An admin may run it, exactly as they may run a bare MERGE.
	if res := ValidateExplorerStatement(exploit, security.WSAdmin); !res.Valid {
		t.Errorf("admin was refused a CTE-prefixed MERGE: %q", res.ErrorMessage)
	}
}
