package assessor

import (
	"context"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// Keyless CDC policy (2026-09-27): a CDC source table with no primary key is
// NOT blocked for a database destination (PostgreSQL/MySQL family, MongoDB).
// Data moves; the pre-flight shows a WARNING that states the exact effect —
// inserts once, each UPDATE adds a new row/document, DELETEs are not applied —
// and carries the ALTER TABLE that gives an exact copy.
//
// The previous policy (2026-09-18) made this an ERROR, in lockstep with a
// hard-block in the executor and the orchestrator's CDC handlers. These tests
// fail if any of that comes back into the pre-flight.

// keylessDriftPhrase is the part of the warning that states the effect. A
// vaguer rewording ("may be duplicated") is what this pins against.
const keylessDriftPhrase = "CDC will copy inserts exactly once, but each UPDATE adds a new"

func TestInput_CDCDatabaseDestination(t *testing.T) {
	cases := []struct {
		name string
		in   Input
		want string
	}{
		{"cdc to postgresql", Input{SyncMode: "cdc", DestinationType: "postgresql"}, "postgresql"},
		{"cdc to postgres alias", Input{SyncMode: "cdc", DestinationType: "postgres"}, "postgresql"},
		{"cdc to mysql", Input{SyncMode: "cdc", DestinationType: "mysql"}, "mysql"},
		{"cdc to mariadb alias", Input{SyncMode: "cdc", DestinationType: "MariaDB"}, "mysql"},
		{"case and space insensitive", Input{SyncMode: "cdc", DestinationType: "  PostgreSQL "}, "postgresql"},
		{"cdc to mongodb", Input{SyncMode: "cdc", DestinationType: "mongodb"}, "mongodb"},
		// Unknown sync mode is treated as CDC everywhere else (IsCDC).
		{"unknown sync mode counts as cdc", Input{SyncMode: "", DestinationType: "postgresql"}, "postgresql"},

		{"batch to postgresql", Input{SyncMode: "batch", DestinationType: "postgresql"}, ""},
		{"batch to mongodb", Input{SyncMode: "batch", DestinationType: "mongodb"}, ""},
		{"full refresh to mysql", Input{SyncMode: "full_refresh", DestinationType: "mysql"}, ""},
		{"cdc to oracle", Input{SyncMode: "cdc", DestinationType: "oracle"}, ""},
		{"cdc to sqlserver", Input{SyncMode: "cdc", DestinationType: "sqlserver"}, ""},
		{"cdc to s3", Input{SyncMode: "cdc", DestinationType: "aws-s3"}, ""},
		{"cdc to unknown destination", Input{SyncMode: "cdc", DestinationType: ""}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.in.CDCDatabaseDestination(); got != tc.want {
				t.Fatalf("CDCDatabaseDestination() = %q; want %q", got, tc.want)
			}
		})
	}
}

// expectPG queues the two lookups oneTablePKCheck performs: does the table
// exist, and does it have a PRIMARY KEY constraint.
func expectPG(mock sqlmock.Sqlmock, schema, table string, hasPK bool) {
	mock.ExpectQuery("information_schema.tables").
		WithArgs(schema, table).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectQuery("table_constraints").
		WithArgs(schema, table).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(hasPK))
}

// expectMySQL queues the existence probe and the PK-column count. pkCols is
// every PRIMARY column, visible is the subset that actually replicates.
func expectMySQL(mock sqlmock.Sqlmock, dbName, table string, pkCols, visible int, invisibleNames interface{}) {
	mock.ExpectQuery("information_schema.tables").
		WithArgs(dbName, table).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectQuery("information_schema.statistics").
		WithArgs(dbName, table).
		WillReturnRows(sqlmock.NewRows([]string{"pk_cols", "visible_pk_cols", "invisible_names"}).
			AddRow(pkCols, visible, invisibleNames))
}

// assertKeylessDriftWarning checks the finding a keyless CDC table into a
// database destination must produce: allowed, exact effect, ALTER TABLE.
func assertKeylessDriftWarning(t *testing.T, got Check, wantUnit, wantAlter string) {
	t.Helper()
	if got.Severity != SeverityWarning || !got.Passed {
		t.Fatalf("severity=%q passed=%v; want warning/true — a keyless table no longer blocks CDC into a database\nmessage: %s",
			got.Severity, got.Passed, got.Message)
	}
	if got.Code != "CDC_TABLE_MISSING_PRIMARY_KEY" {
		t.Fatalf("code = %q; want CDC_TABLE_MISSING_PRIMARY_KEY", got.Code)
	}
	for _, want := range []string{
		keylessDriftPhrase + " " + wantUnit + " (the old version stays)",
		"DELETEs are not applied, so the destination drifts from the source",
		"Add a primary key for an exact copy: " + wantAlter,
	} {
		if !strings.Contains(got.Message, want) {
			t.Fatalf("message lacks %q:\n%s", want, got.Message)
		}
	}
	// The old blocking text must not survive as a claim about this run.
	for _, stale := range []string{"will fail at start", "CDC requires PRIMARY KEY", "the run succeeds"} {
		if strings.Contains(got.Message, stale) {
			t.Fatalf("message still says %q:\n%s", stale, got.Message)
		}
	}
	if got.Remediation == nil || !containsString(got.Remediation.SQLToRun, wantAlter) {
		t.Fatalf("remediation does not carry the ALTER TABLE %q: %+v", wantAlter, got.Remediation)
	}
}

func containsString(list []string, want string) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}
	return false
}

// TestKeylessCDCIntoDatabaseIsNeverAnError is the guard against re-adding the
// block: for every database destination, the pre-flight Input a real request
// builds must turn a keyless table into a passed warning, never an error, and
// the summarised result must not stop the launch.
func TestKeylessCDCIntoDatabaseIsNeverAnError(t *testing.T) {
	for _, dest := range []string{"postgresql", "postgres", "mysql", "mariadb", "mongodb"} {
		in := Input{SyncMode: "cdc", DestinationType: dest}
		unit := "row"
		if dest == "mongodb" {
			unit = "document"
		}

		t.Run("postgres source to "+dest, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock: %v", err)
			}
			defer db.Close()
			expectPG(mock, "public", "events", false)

			checks := checkPostgresTablePrimaryKeys(context.Background(), db, nil, []string{"public.events"}, in.CDCDatabaseDestination(), nil)
			if len(checks) != 1 {
				t.Fatalf("got %d checks; want 1", len(checks))
			}
			assertKeylessDriftWarning(t, checks[0], unit, `ALTER TABLE "public"."events" ADD PRIMARY KEY (id);`)

			r := &Result{Checks: checks}
			Summarize(r)
			if r.BlocksStart() || r.ErrorCount != 0 {
				t.Fatalf("a keyless table blocks the launch again (errors=%d, status=%q)", r.ErrorCount, r.OverallStatus)
			}
		})

		t.Run("mysql source to "+dest, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock: %v", err)
			}
			defer db.Close()
			expectMySQL(mock, "appdb", "orders", 0, 0, nil)

			checks := checkMySQLTablePrimaryKeys(context.Background(), db, nil, []string{"appdb.orders"}, in.CDCDatabaseDestination(), nil)
			if len(checks) != 1 {
				t.Fatalf("got %d checks; want 1", len(checks))
			}
			assertKeylessDriftWarning(t, checks[0], unit, "ALTER TABLE `appdb`.`orders` ADD PRIMARY KEY (`id`);")

			r := &Result{Checks: checks}
			Summarize(r)
			if r.BlocksStart() || r.ErrorCount != 0 {
				t.Fatalf("a keyless table blocks the launch again (errors=%d, status=%q)", r.ErrorCount, r.OverallStatus)
			}
		})
	}
}

func TestOneTablePKCheck_PostgresKeyless(t *testing.T) {
	const schema, table = "public", "events"
	const alter = `ALTER TABLE "public"."events" ADD PRIMARY KEY (id);`

	t.Run("nominated key columns are named as batch-only", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer db.Close()
		expectPG(mock, schema, table, false)

		got := oneTablePKCheck(context.Background(), db, schema, table, "postgresql", []string{"tenant_id", "external_id"})

		// The nomination feeds batch pkByTable only; the CDC sink never sees it,
		// so it must not turn the drift warning into a clean INFO pass.
		assertKeylessDriftWarning(t, got, "row", alter)
		if !strings.Contains(got.Message, "tenant_id, external_id") || !strings.Contains(got.Message, "batch loads only") {
			t.Fatalf("message does not say the nomination is batch-only: %s", got.Message)
		}
	})

	t.Run("batch to a relational destination keeps the surrogate-key warning", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer db.Close()
		expectPG(mock, schema, table, false)

		got := oneTablePKCheck(context.Background(), db, schema, table, "", nil)

		if got.Severity != SeverityWarning || !got.Passed {
			t.Fatalf("severity=%q passed=%v; want warning/true\nmessage: %s", got.Severity, got.Passed, got.Message)
		}
		if !strings.Contains(got.Message, "_rsync_row_hash") {
			t.Fatalf("batch warning changed: %s", got.Message)
		}
	})

	t.Run("table with a primary key still passes", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer db.Close()
		expectPG(mock, schema, table, true)

		got := oneTablePKCheck(context.Background(), db, schema, table, "postgresql", nil)

		if got.Severity != SeverityInfo || !got.Passed {
			t.Fatalf("severity=%q passed=%v; want info/true\nmessage: %s", got.Severity, got.Passed, got.Message)
		}
	})
}

func TestOneMySQLTablePKCheck_Keyless(t *testing.T) {
	const dbName, table = "appdb", "orders"

	t.Run("generated invisible primary key keeps its own warning", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer db.Close()
		// MySQL 8.0.30+ GIPK: one PRIMARY column, invisible.
		expectMySQL(mock, dbName, table, 1, 0, "my_row_id")

		got := oneMySQLTablePKCheck(context.Background(), db, dbName, table, "mysql", nil)

		if got.Severity != SeverityWarning || !got.Passed || got.Code != "MYSQL_TABLE_PRIMARY_KEY_INVISIBLE" {
			t.Fatalf("code=%q severity=%q passed=%v; want MYSQL_TABLE_PRIMARY_KEY_INVISIBLE warning/true\nmessage: %s",
				got.Code, got.Severity, got.Passed, got.Message)
		}
	})

	t.Run("batch keeps the surrogate-key warning", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer db.Close()
		expectMySQL(mock, dbName, table, 0, 0, nil)

		got := oneMySQLTablePKCheck(context.Background(), db, dbName, table, "", nil)

		if got.Severity != SeverityWarning || !got.Passed || !strings.Contains(got.Message, "_rsync_row_hash") {
			t.Fatalf("severity=%q passed=%v; want the batch surrogate-key warning\nmessage: %s", got.Severity, got.Passed, got.Message)
		}
	})

	t.Run("visible primary key still passes", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer db.Close()
		expectMySQL(mock, dbName, table, 1, 1, nil)

		got := oneMySQLTablePKCheck(context.Background(), db, dbName, table, "mysql", nil)

		if got.Severity != SeverityInfo || !got.Passed {
			t.Fatalf("severity=%q passed=%v; want info/true\nmessage: %s", got.Severity, got.Passed, got.Message)
		}
	})
}
