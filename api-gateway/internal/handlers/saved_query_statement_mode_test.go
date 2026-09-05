package handlers

import (
	"context"
	"strings"
	"testing"

	"api-gateway/internal/security"
	"api-gateway/internal/validators"

	"github.com/DATA-DOG/go-sqlmock"
)

// Statement mode lets a model run its SQL as written, which means authorizeModelRun now
// has a path that deliberately permits writes. Everything below exists because that path
// changes what the older gates were protecting.

// authorizeWithRole drives authorizeModelRun against a workspace_members row holding the
// given role, and reports what it decided.
func authorizeWithRole(t *testing.T, role security.WorkspaceRole, m *savedQueryModel) (validators.StatementClass, string, error) {
	t.Helper()
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer database.Close()
	mock.ExpectQuery("SELECT role FROM workspace_members").
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow(string(role)))

	m.ID = "33333333-3333-3333-3333-333333333333"
	m.WorkspaceID = "11111111-1111-1111-1111-111111111111"
	return authorizeModelRun(context.Background(), database, m, "22222222-2222-2222-2222-222222222222")
}

// The reason the statement gate had to move ahead of the class decision.
//
// It used to sit behind an early `class != ClassRead` refusal, which was safe only
// while no class but read could get past it. Statement mode ends that: a MERGE now
// legitimately runs, and a class cannot describe a second statement. The plan hands the
// whole string to a parameter-free Exec, which the driver runs under the simple query
// protocol — every semicolon-separated statement executes, under the authority of the
// admin who created the schedule.
func TestAuthorizeModelRun_StatementModeStillRefusesStackedSQL(t *testing.T) {
	stacked := []struct {
		name string
		sql  string
	}{
		{"MERGE then DROP", "MERGE INTO invoices t USING staged s ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.paid = s.paid; DROP TABLE customers"},
		{"UPDATE then DROP", "UPDATE invoices SET paid = true; DROP TABLE customers"},
		{"DELETE then GRANT", "DELETE FROM invoices WHERE paid; GRANT ALL ON customers TO PUBLIC"},
		{"INSERT then TRUNCATE, trailing comment", "INSERT INTO audit VALUES (1); TRUNCATE customers; --"},
	}
	for _, tc := range stacked {
		t.Run(tc.name, func(t *testing.T) {
			// Owner is the highest role there is: a refusal here means no role can run it.
			_, refusal, err := authorizeWithRole(t, security.WSOwner, &savedQueryModel{
				Materialization: matStatement,
				SQLText:         tc.sql,
			})
			if err != nil {
				t.Fatalf("authorization is decidable, not an error: %v", err)
			}
			if refusal == "" {
				t.Fatalf("statement mode accepted stacked SQL %q", tc.sql)
			}
			if !strings.Contains(refusal, "Multiple SQL statements") {
				t.Fatalf("the refusal should come from the single-statement rule, got %q", refusal)
			}
		})
	}
}

// Statement mode does not flatten the role tiers it runs on top of. A DROP is
// destructive whoever schedules it, and destructive means owner — the floor
// (modelRunMinRole, admin) is a minimum, not the whole answer.
func TestAuthorizeModelRun_StatementModeKeepsPerClassRoles(t *testing.T) {
	cases := []struct {
		name      string
		sql       string
		role      security.WorkspaceRole
		wantAllow bool
	}{
		{"admin may MERGE", "MERGE INTO invoices t USING staged s ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.paid = s.paid", security.WSAdmin, true},
		{"admin may UPDATE", "UPDATE invoices SET paid = true WHERE id = 1", security.WSAdmin, true},
		{"admin may INSERT … SELECT", "INSERT INTO daily_totals SELECT date, sum(total) FROM orders GROUP BY 1", security.WSAdmin, true},
		{"admin may not DROP", "DROP TABLE customers", security.WSAdmin, false},
		{"admin may not TRUNCATE", "TRUNCATE customers", security.WSAdmin, false},
		{"owner may DROP", "DROP TABLE customers", security.WSOwner, true},
		{"member may not write at all", "UPDATE invoices SET paid = true", security.WSMember, false},
		{"viewer may not write at all", "UPDATE invoices SET paid = true", security.WSViewer, false},
		{"nobody may GRANT", "GRANT ALL ON customers TO PUBLIC", security.WSOwner, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, refusal, err := authorizeWithRole(t, tc.role, &savedQueryModel{
				Materialization: matStatement,
				SQLText:         tc.sql,
			})
			if err != nil {
				t.Fatalf("authorization is decidable, not an error: %v", err)
			}
			if tc.wantAllow && refusal != "" {
				t.Fatalf("%s should be allowed to run %q, refused: %s", tc.role, tc.sql, refusal)
			}
			if !tc.wantAllow && refusal == "" {
				t.Fatalf("%s must not be allowed to run %q", tc.role, tc.sql)
			}
		})
	}
}

// The two modes are not interchangeable, and each is incoherent with the other's SQL
// rather than merely unusual. Wrapping a DELETE in CREATE TABLE … AS is meaningless;
// scheduling a bare SELECT for its effect produces nothing and delivers nowhere.
func TestAuthorizeModelRun_ModesRefuseEachOthersSQL(t *testing.T) {
	t.Run("table mode refuses a write", func(t *testing.T) {
		_, refusal, err := authorizeWithRole(t, security.WSOwner, &savedQueryModel{
			Materialization: matTable,
			TargetTable:     "analytics.orders",
			SQLText:         "UPDATE invoices SET paid = true",
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if refusal == "" {
			t.Fatal("a table-mode model must be a read query")
		}
		if !strings.Contains(refusal, "statement") {
			t.Fatalf("the refusal should point at the mode that does fit, got %q", refusal)
		}
	})

	t.Run("statement mode refuses a read", func(t *testing.T) {
		_, refusal, err := authorizeWithRole(t, security.WSOwner, &savedQueryModel{
			Materialization: matStatement,
			SQLText:         "SELECT * FROM orders",
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if refusal == "" {
			t.Fatal("a scheduled bare SELECT delivers its rows nowhere; it must be refused")
		}
		if !strings.Contains(refusal, "table") {
			t.Fatalf("the refusal should point at the mode that does fit, got %q", refusal)
		}
	})
}

// isStatementModel decides whether writes are permitted, so it must answer "no" for
// every value it does not recognize. Written as an equality test against matStatement
// for exactly this reason: an inequality against matTable would hand a hand-edited row,
// or a mode some later migration adds, the branch that lets writes through.
func TestStatementModeFailsClosedForUnknownModes(t *testing.T) {
	for _, mode := range []string{"", matNone, matTable, "incremental", "STATEMENT", " statement", "view"} {
		if isStatementModel(mode) {
			t.Fatalf("materialization %q must not be treated as statement mode", mode)
		}
	}
	if !isStatementModel(matStatement) {
		t.Fatalf("%q is statement mode", matStatement)
	}
}

// An unrecognized mode must not merely fail the isStatementModel predicate — it has to
// land somewhere safe. The table path is that somewhere, because it demands a read.
func TestAuthorizeModelRun_UnknownModeIsGatedAsTable(t *testing.T) {
	_, refusal, err := authorizeWithRole(t, security.WSOwner, &savedQueryModel{
		Materialization: "incremental", // reserved by migration 085, never implemented
		SQLText:         "DELETE FROM invoices WHERE paid",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if refusal == "" {
		t.Fatal("a model whose mode rsync does not recognize must not run a write")
	}
}
