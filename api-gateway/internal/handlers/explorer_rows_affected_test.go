package handlers

import (
	"testing"

	"api-gateway/internal/validators"
)

// TestReportsRowsAffected guards the fix for a prod defect found on 2026-08-01: running
// `DROP TABLE public.zz724_inc` in the Data Explorer toasted "DROP succeeded — 0 rows
// affected". Every driver returns 0 from RowsAffected() for DDL because DDL affects no
// rows by definition, but rendering that reads like nothing happened at the exact moment
// a table was destroyed. Suppressing the count makes the UI fall back to its existing
// "The statement completed successfully." copy.
func TestReportsRowsAffected(t *testing.T) {
	tests := []struct {
		name  string
		class validators.StatementClass
		want  bool
		why   string
	}{
		{
			name:  "dml write",
			class: validators.ClassDMLWrite,
			want:  true,
			why:   "INSERT/UPDATE/DELETE are the only statements where the count means something",
		},
		{
			name:  "ddl",
			class: validators.ClassDDL,
			want:  false,
			why:   "CREATE/ALTER always report 0 — showing it implies the statement did nothing",
		},
		{
			name:  "destructive",
			class: validators.ClassDestructive,
			want:  false,
			why:   "the original repro: DROP TABLE reported '0 rows affected' after destroying a table",
		},
		{
			name:  "read",
			class: validators.ClassRead,
			want:  false,
			why:   "SELECT results carry their own row count",
		},
		{
			name:  "blocked",
			class: validators.ClassBlocked,
			want:  false,
			why:   "never executed, so there is nothing to report",
		},
		{
			name:  "unknown",
			class: validators.ClassUnknown,
			want:  false,
			why:   "if we cannot classify the statement we cannot vouch for the number either",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := reportsRowsAffected(tt.class); got != tt.want {
				t.Errorf("reportsRowsAffected(%q) = %v, want %v — %s", tt.class, got, tt.want, tt.why)
			}
		})
	}
}

// TestReportsRowsAffectedFromSQL runs the classifier end-to-end so a future change to
// ClassifyStatementSQL that reclassifies DROP cannot quietly reintroduce the bug.
func TestReportsRowsAffectedFromSQL(t *testing.T) {
	tests := []struct {
		sql  string
		want bool
	}{
		{"UPDATE pipeline_test.zz724_inc SET name = 'bravo' WHERE id = 2", true},
		{"INSERT INTO demo_products (id, name) VALUES (1, 'x')", true},
		{"DELETE FROM demo_orders WHERE id = 7", true},
		{"DROP TABLE public.zz724_inc", false},
		{"TRUNCATE TABLE demo_customers", false},
		{"ALTER TABLE demo_orders DROP COLUMN memo", false},
		{"CREATE TABLE t (id int)", false},
		{"SELECT count(*) FROM demo_orders", false},
	}

	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			class := validators.ClassifyStatementSQL(tt.sql)
			if got := reportsRowsAffected(class); got != tt.want {
				t.Errorf("%q classified %q -> reportsRowsAffected = %v, want %v", tt.sql, class, got, tt.want)
			}
		})
	}
}
