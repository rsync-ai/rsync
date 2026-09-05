package validators

import (
	"testing"

	"api-gateway/internal/security"
)

// The classifier and the statement validator have to agree about the same SQL.
// ValidateExplorerStatement always classified the comment-stripped text;
// ClassifyStatementSQL classified the raw text. Every test in this file fails
// against the code as it was before that was reconciled.

// A leading comment is ordinary SQL hygiene, not an unrecognized statement.
// Before the fix `identifyStatementType` prefix-matched the raw text, found no
// verb, and returned UNKNOWN -- which IsWriteClass reports as a write. Two things
// broke on that: ExecuteExplorerQuery routed the read down the no-rows write path
// (the user got "rows affected" and no data), and authorizeModelRun refused the
// model for "not being a read query", which auto-pauses its schedule.
func TestClassifyStatementSQL_LeadingCommentsStayReads(t *testing.T) {
	reads := []struct {
		name string
		sql  string
	}{
		{"line comment", "-- daily revenue\nSELECT * FROM orders"},
		{"block comment", "/* header */ SELECT * FROM orders"},
		{"commented CTE", "-- note\nWITH r AS (SELECT 1) SELECT * FROM r"},
		{"several comments", "-- one\n-- two\n/* three */\nSELECT 1"},
		{"indented comment", "   -- pad\n   SELECT 1"},
	}
	for _, tc := range reads {
		t.Run(tc.name, func(t *testing.T) {
			class := ClassifyStatementSQL(tc.sql)
			if class != ClassRead {
				t.Fatalf("ClassifyStatementSQL(%q) = %q, want %q", tc.sql, class, ClassRead)
			}
			if IsWriteClass(class) {
				t.Fatalf("%q classified as a write; it would run on the no-rows path", tc.sql)
			}
			// The two layers must not disagree, which is the actual defect.
			if v := ValidateExplorerStatement(tc.sql, security.WSViewer); v == nil || !v.Valid {
				msg := "<nil>"
				if v != nil {
					msg = v.ErrorMessage
				}
				t.Fatalf("statement gate rejected a commented read %q: %s", tc.sql, msg)
			}
		})
	}
}

// A DROP that appears only inside a comment destroys nothing, so it must not
// escalate an otherwise routine ALTER to owner-only.
func TestClassifyStatementSQL_CommentedDropDoesNotEscalate(t *testing.T) {
	sql := "ALTER TABLE orders ADD COLUMN note TEXT -- we will drop this later"
	if class := ClassifyStatementSQL(sql); class != ClassDDL {
		t.Fatalf("ClassifyStatementSQL(%q) = %q, want %q", sql, class, ClassDDL)
	}
}

// The real ALTER … DROP must still be destructive -- the fix above must not have
// bought comment accuracy by loosening the escalation.
func TestClassifyStatementSQL_RealAlterDropStillDestructive(t *testing.T) {
	for _, sql := range []string{
		"ALTER TABLE orders DROP COLUMN note",
		"-- tidy up\nALTER TABLE orders DROP COLUMN note",
	} {
		if class := ClassifyStatementSQL(sql); class != ClassDestructive {
			t.Fatalf("ClassifyStatementSQL(%q) = %q, want %q", sql, class, ClassDestructive)
		}
	}
}

// A comment cannot launder a write. Stripping comments must not let a commented
// data-modifying CTE fall back to the read tier.
func TestClassifyStatementSQL_CommentedWritesKeepTheirClass(t *testing.T) {
	cases := []struct {
		sql  string
		want StatementClass
	}{
		{"-- nightly\nWITH gone AS (DELETE FROM invoices RETURNING *) SELECT * FROM gone", ClassDMLWrite},
		{"/* sync */ WITH s AS (SELECT 1 AS id) MERGE INTO invoices t USING s ON t.id = s.id WHEN MATCHED THEN DELETE", ClassDMLWrite},
		{"-- cleanup\nDROP TABLE customers", ClassDestructive},
		{"-- load\nINSERT INTO audit VALUES (1)", ClassDMLWrite},
	}
	for _, tc := range cases {
		if class := ClassifyStatementSQL(tc.sql); class != tc.want {
			t.Fatalf("ClassifyStatementSQL(%q) = %q, want %q", tc.sql, class, tc.want)
		}
	}
}

// `/*!` is a MySQL/MariaDB executable comment: the server runs its contents.
// removeComments used to delete it, so the checks built on removeComments saw SQL
// the engine would not see. `SELECT 1; /*!40101 DROP TABLE customers */` stripped
// to `SELECT 1;`, which is one statement and a read -- so it validated for a
// viewer. The Explorer's MySQL DSN leaves multiStatements at its false default,
// which is the only reason that particular payload did not execute; a validator
// must not be relying on a driver default it never asserts.
func TestRemoveComments_KeepsMySQLExecutableCommentBodies(t *testing.T) {
	t.Run("stacked write is seen as a second statement", func(t *testing.T) {
		sql := "SELECT 1; /*!40101 DROP TABLE customers */"
		v := ValidateExplorerStatement(sql, security.WSViewer)
		if v == nil || v.Valid {
			t.Fatalf("viewer was allowed to run %q", sql)
		}
		if v.ErrorCode != ErrCodeMultipleStatements {
			t.Fatalf("error code = %q, want %q (got message %q)",
				v.ErrorCode, ErrCodeMultipleStatements, v.ErrorMessage)
		}
	})

	t.Run("bare executable comment classifies on the verb it hides", func(t *testing.T) {
		// Previously stripped to "", which reported "only comments". The body is a
		// DROP, so it must land on the destructive tier.
		if class := ClassifyStatementSQL("/*!40101 DROP TABLE customers */"); class != ClassDestructive {
			t.Fatalf("class = %q, want %q", class, ClassDestructive)
		}
	})

	t.Run("version gate is optional", func(t *testing.T) {
		if class := ClassifyStatementSQL("/*! DROP TABLE customers */"); class != ClassDestructive {
			t.Fatalf("class = %q, want %q", class, ClassDestructive)
		}
	})

	t.Run("an ordinary block comment is still a comment", func(t *testing.T) {
		if got := removeComments("SELECT 1 /* DROP TABLE t */"); got != "SELECT 1 " {
			t.Fatalf("removeComments = %q, want %q", got, "SELECT 1 ")
		}
	})
}
