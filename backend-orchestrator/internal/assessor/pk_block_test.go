package assessor

import (
	"context"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// These tests pin KI-CDC-ASSESS-PK-FALLBACK-NOT-IMPLEMENTED.
//
// The pre-flight assessor used to clear a keyless table for a CDC pipeline as a
// WARNING whose message promised "rsync will load it using a content-hash
// surrogate key (_rsync_row_hash), so the run succeeds" — and a nominated key
// column made it a clean INFO pass. Neither is true for a CDC pipeline writing
// to a PostgreSQL/MySQL destination: executeStreamingDataTransfer calls
// ValidateTablesHavePrimaryKeys before any data moves and fails the run with
// "CDC requires PRIMARY KEY for DB destinations; missing PK on: …". That
// validator takes no override — not the surrogate key, not the nomination. So
// the assessment cleared the user to launch a run that could only fail, on the
// exact check the assessment had just performed.

func TestInput_CDCBlocksWithoutPrimaryKey(t *testing.T) {
	cases := []struct {
		name string
		in   Input
		want bool
	}{
		// The executor's hard block: CDC + a destination normalising to
		// postgresql or mysql.
		{"cdc to postgresql", Input{SyncMode: "cdc", DestinationType: "postgresql"}, true},
		{"cdc to postgres alias", Input{SyncMode: "cdc", DestinationType: "postgres"}, true},
		{"cdc to mysql", Input{SyncMode: "cdc", DestinationType: "mysql"}, true},
		{"cdc to mariadb alias", Input{SyncMode: "cdc", DestinationType: "MariaDB"}, true},
		{"case and space insensitive", Input{SyncMode: "cdc", DestinationType: "  PostgreSQL "}, true},

		// Unknown sync mode is treated as CDC everywhere else in this file
		// (IsCDC), and must be here too — otherwise the strict path is the one
		// that gets skipped on a guess.
		{"unknown sync mode counts as cdc", Input{SyncMode: "", DestinationType: "postgresql"}, true},

		// Batch never reaches that gate: it loads keyless tables through the
		// content-hash surrogate key, exactly as the warning claims.
		{"batch to postgresql", Input{SyncMode: "batch", DestinationType: "postgresql"}, false},
		{"full refresh to mysql", Input{SyncMode: "full_refresh", DestinationType: "mysql"}, false},

		// Deliberately outside the block — executor.go gates on
		// normalizedDest == "postgresql" || "mysql" only, so these CDC runs do
		// start and the existing warning remains the honest answer.
		{"cdc to oracle is not blocked", Input{SyncMode: "cdc", DestinationType: "oracle"}, false},
		{"cdc to sqlserver is not blocked", Input{SyncMode: "cdc", DestinationType: "sqlserver"}, false},
		{"cdc to s3 is not blocked", Input{SyncMode: "cdc", DestinationType: "aws-s3"}, false},
		{"cdc to unknown destination is not blocked", Input{SyncMode: "cdc", DestinationType: ""}, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.in.CDCBlocksWithoutPrimaryKey(); got != tc.want {
				t.Fatalf("CDCBlocksWithoutPrimaryKey() = %v; want %v", got, tc.want)
			}
		})
	}
}

func TestOneTablePKCheck_PostgresCDCBlockIsAnError(t *testing.T) {
	const schema, table = "public", "events"

	// expectPG queues the two lookups oneTablePKCheck performs: does the table
	// exist, and does it have a PRIMARY KEY constraint.
	expectPG := func(mock sqlmock.Sqlmock, hasPK bool) {
		mock.ExpectQuery("information_schema.tables").
			WithArgs(schema, table).
			WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
		mock.ExpectQuery("table_constraints").
			WithArgs(schema, table).
			WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(hasPK))
	}

	t.Run("keyless table blocks the CDC run", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer db.Close()
		expectPG(mock, false)

		got := oneTablePKCheck(context.Background(), db, schema, table, true, true, nil)

		if got.Severity != SeverityError || got.Passed {
			t.Fatalf("severity=%q passed=%v; want error/false — the executor refuses to start this run\nmessage: %s",
				got.Severity, got.Passed, got.Message)
		}
		// The message must not keep promising the fallback that isn't wired up.
		if strings.Contains(got.Message, "_rsync_row_hash") || strings.Contains(got.Message, "the run succeeds") {
			t.Fatalf("message still promises the surrogate-key fallback: %s", got.Message)
		}
		if !strings.Contains(got.Message, "CDC requires PRIMARY KEY for DB destinations") {
			t.Fatalf("message does not quote the failure the user will hit: %s", got.Message)
		}
		if got.Remediation == nil || len(got.Remediation.SQLToRun) == 0 {
			t.Fatal("blocking finding carries no copy-pasteable fix")
		}
	})

	t.Run("nominated key columns do not clear the block", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer db.Close()
		expectPG(mock, false)

		got := oneTablePKCheck(context.Background(), db, schema, table, true, true, []string{"tenant_id", "external_id"})

		if got.Severity != SeverityError || got.Passed {
			t.Fatalf("severity=%q passed=%v; want error/false — the nomination never reaches ValidateTablesHavePrimaryKeys\nmessage: %s",
				got.Severity, got.Passed, got.Message)
		}
		// Say plainly why the nomination didn't help, or the user re-nominates
		// and runs into the same wall.
		if !strings.Contains(got.Message, "tenant_id") {
			t.Fatalf("message does not address the nominated columns: %s", got.Message)
		}
	})

	t.Run("batch to a relational destination keeps the surrogate-key warning", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer db.Close()
		expectPG(mock, false)

		// cdcMode=false, cdcBlocks=false: a batch load really does route keyless
		// tables through the content-hash key, so nothing here should change.
		got := oneTablePKCheck(context.Background(), db, schema, table, false, false, nil)

		if got.Severity != SeverityWarning || !got.Passed {
			t.Fatalf("severity=%q passed=%v; want warning/true — batch loads keyless tables fine\nmessage: %s",
				got.Severity, got.Passed, got.Message)
		}
	})

	t.Run("cdc to a non-blocking destination keeps the warning", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer db.Close()
		expectPG(mock, false)

		// CDC to e.g. aws-s3 or oracle: executor.go never runs the PK gate.
		got := oneTablePKCheck(context.Background(), db, schema, table, true, false, nil)

		if got.Severity != SeverityWarning || !got.Passed {
			t.Fatalf("severity=%q passed=%v; want warning/true — this destination is outside the executor's hard block\nmessage: %s",
				got.Severity, got.Passed, got.Message)
		}
	})

	t.Run("table with a primary key still passes", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer db.Close()
		expectPG(mock, true)

		got := oneTablePKCheck(context.Background(), db, schema, table, true, true, nil)

		if got.Severity != SeverityInfo || !got.Passed {
			t.Fatalf("severity=%q passed=%v; want info/true\nmessage: %s", got.Severity, got.Passed, got.Message)
		}
	})
}

func TestOneMySQLTablePKCheck_CDCBlockRespectsGIPK(t *testing.T) {
	const dbName, table = "appdb", "orders"

	// expectMySQL queues the existence probe and the PK-column count. pkCols is
	// every PRIMARY column, visible is the subset that actually replicates.
	expectMySQL := func(mock sqlmock.Sqlmock, pkCols, visible int, invisibleNames interface{}) {
		mock.ExpectQuery("information_schema.tables").
			WithArgs(dbName, table).
			WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
		mock.ExpectQuery("information_schema.statistics").
			WithArgs(dbName, table).
			WillReturnRows(sqlmock.NewRows([]string{"pk_cols", "visible_pk_cols", "invisible_names"}).
				AddRow(pkCols, visible, invisibleNames))
	}

	t.Run("no primary key at all blocks the CDC run", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer db.Close()
		expectMySQL(mock, 0, 0, nil)

		got := oneMySQLTablePKCheck(context.Background(), db, dbName, table, true, true, nil)

		if got.Severity != SeverityError || got.Passed {
			t.Fatalf("severity=%q passed=%v; want error/false\nmessage: %s", got.Severity, got.Passed, got.Message)
		}
		if !strings.Contains(got.Message, "CDC requires PRIMARY KEY for DB destinations") {
			t.Fatalf("message does not quote the failure the user will hit: %s", got.Message)
		}
	})

	t.Run("generated invisible primary key is NOT blocked", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer db.Close()
		// MySQL 8.0.30+ GIPK: one PRIMARY column, invisible.
		expectMySQL(mock, 1, 0, "my_row_id")

		got := oneMySQLTablePKCheck(context.Background(), db, dbName, table, true, true, nil)

		// The executor's validator counts KEY_COLUMN_USAGE rows for
		// CONSTRAINT_NAME='PRIMARY', which include invisible columns — so this
		// run DOES start and degrades to the content-hash path. Escalating it to
		// an error here would block a pipeline that works.
		if got.Severity != SeverityWarning || !got.Passed {
			t.Fatalf("severity=%q passed=%v; want warning/true — GIPK satisfies the executor's PK gate\nmessage: %s",
				got.Severity, got.Passed, got.Message)
		}
		if got.Code != "MYSQL_TABLE_PRIMARY_KEY_INVISIBLE" {
			t.Fatalf("code = %q; want MYSQL_TABLE_PRIMARY_KEY_INVISIBLE", got.Code)
		}
	})

	t.Run("nominated columns do not clear the block", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer db.Close()
		expectMySQL(mock, 0, 0, nil)

		got := oneMySQLTablePKCheck(context.Background(), db, dbName, table, true, true, []string{"order_ref"})

		if got.Severity != SeverityError || got.Passed {
			t.Fatalf("severity=%q passed=%v; want error/false\nmessage: %s", got.Severity, got.Passed, got.Message)
		}
		if !strings.Contains(got.Message, "order_ref") {
			t.Fatalf("message does not address the nominated columns: %s", got.Message)
		}
	})

	t.Run("batch keeps the surrogate-key warning", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer db.Close()
		expectMySQL(mock, 0, 0, nil)

		got := oneMySQLTablePKCheck(context.Background(), db, dbName, table, false, false, nil)

		if got.Severity != SeverityWarning || !got.Passed {
			t.Fatalf("severity=%q passed=%v; want warning/true\nmessage: %s", got.Severity, got.Passed, got.Message)
		}
	})

	t.Run("visible primary key still passes", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer db.Close()
		expectMySQL(mock, 1, 1, nil)

		got := oneMySQLTablePKCheck(context.Background(), db, dbName, table, true, true, nil)

		if got.Severity != SeverityInfo || !got.Passed {
			t.Fatalf("severity=%q passed=%v; want info/true\nmessage: %s", got.Severity, got.Passed, got.Message)
		}
	})
}

// TestBlockingMissingPKCheck_SummarizesAsBlocking closes the loop: an ERROR
// finding is only useful if Summarize turns it into a result that stops the
// launch. The frontend modal gates its submit button on the error count.
func TestBlockingMissingPKCheck_SummarizesAsBlocking(t *testing.T) {
	r := &Result{Checks: []Check{
		{Code: "PG_WAL_LEVEL", Severity: SeverityInfo, Passed: true, Message: "wal_level=logical"},
		blockingMissingPKCheck("public.events", "ALTER TABLE public.events ADD PRIMARY KEY (id);", nil),
	}}
	Summarize(r)

	if r.OverallStatus != "failed" {
		t.Fatalf("overall_status = %q; want \"failed\" (failed=%d error=%d)", r.OverallStatus, r.FailedCount, r.ErrorCount)
	}
	if !r.BlocksStart() {
		t.Fatal("BlocksStart() is false — the user would be allowed to launch a run that cannot start")
	}
}
