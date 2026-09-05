package handlers

// Live Data Explorer verification for the SQL Server / Azure SQL path.
// Skips unless SS_LIVE_HOST is set, so CI (no creds) is unaffected.
// Run: SS_LIVE_HOST=... SS_LIVE_USER=... SS_LIVE_PWD=... SS_LIVE_DB=... \
//      go test ./internal/handlers/ -run TestSQLServerExplorerLive -v

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

func ssLiveConfig(t *testing.T) map[string]interface{} {
	host := os.Getenv("SS_LIVE_HOST")
	if strings.TrimSpace(host) == "" {
		t.Skip("SS_LIVE_HOST not set — skipping live SQL Server explorer test")
	}
	return map[string]interface{}{
		"host":     host,
		"port":     1433,
		"user":     os.Getenv("SS_LIVE_USER"),
		"password": os.Getenv("SS_LIVE_PWD"),
		"database": os.Getenv("SS_LIVE_DB"),
		"encrypt":  "yes",
	}
}

func TestClampLimitSQLServer(t *testing.T) {
	cases := []struct{ in, want string }{
		{"SELECT id, name FROM dbo.t", "SELECT TOP (11) id, name FROM dbo.t"},
		// SQL Server grammar is SELECT [DISTINCT] [TOP (n)] — DISTINCT precedes TOP.
		{"select DISTINCT x from y;", "select DISTINCT TOP (11) x from y"},
		{"SELECT TOP (5) * FROM t", "SELECT TOP (5) * FROM t"},                 // already TOP → untouched
		{"WITH c AS (SELECT 1 AS a) SELECT a FROM c", "WITH c AS (SELECT 1 AS a) SELECT a FROM c"}, // CTE → untouched
	}
	for _, c := range cases {
		got := clampLimitSQLServer(c.in, 11)
		if got != c.want {
			t.Errorf("clampLimitSQLServer(%q)\n  got  %q\n  want %q", c.in, got, c.want)
		}
	}
}

func TestSQLServerExplorerDSN(t *testing.T) {
	dsn := sqlServerExplorerDSN(map[string]interface{}{
		"host": "x.database.windows.net", "port": 1433, "user": "u", "password": "p@:s", "database": "d",
	})
	// Azure host → encrypt=true & verify CA (TrustServerCertificate=false); password url-escaped.
	for _, want := range []string{"sqlserver://u:", "x.database.windows.net:1433", "encrypt=true", "TrustServerCertificate=false", "database=d"} {
		if !strings.Contains(dsn, want) {
			t.Errorf("DSN %q missing %q", dsn, want)
		}
	}
	if strings.Contains(dsn, "p@:s") {
		t.Errorf("password not url-escaped in DSN: %q", dsn)
	}
}

func TestSQLServerExplorerLive(t *testing.T) {
	config := ssLiveConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 1. Schema index — must discover the seeded dbo.cdc_test table with a PK.
	tables, err := fetchSQLServerSchemaIndex(ctx, config)
	if err != nil {
		t.Fatalf("fetchSQLServerSchemaIndex: %v", err)
	}
	t.Logf("schema index: %d tables", len(tables))
	var found *struct {
		cols int
		pk   int
	}
	for _, tb := range tables {
		if strings.EqualFold(tb.Schema, "dbo") && strings.EqualFold(tb.Name, "cdc_test") {
			found = &struct {
				cols int
				pk   int
			}{len(tb.Columns), len(tb.PrimaryKey)}
			t.Logf("  dbo.cdc_test: %d columns, PK=%v", len(tb.Columns), tb.PrimaryKey)
		}
	}
	if found == nil {
		t.Fatalf("dbo.cdc_test not found in schema index (%d tables)", len(tables))
	}
	if found.cols == 0 {
		t.Errorf("dbo.cdc_test has 0 columns in index")
	}
	if found.pk == 0 {
		t.Errorf("dbo.cdc_test PK not detected")
	}

	// 2. Query execution — SELECT rows back (read-only preview path).
	res, err := executeSQLServerQueryWithRedact(ctx, config, "SELECT id, name FROM dbo.cdc_test ORDER BY id", 100, true)
	if err != nil {
		t.Fatalf("executeSQLServerQueryWithRedact: %v", err)
	}
	t.Logf("query returned %d rows, columns=%v, %dms", res.RowCount, res.Columns, res.ExecutionTimeMs)
	if res.RowCount < 1 {
		t.Fatalf("expected >=1 row from dbo.cdc_test, got %d", res.RowCount)
	}
	if len(res.Columns) != 2 {
		t.Errorf("expected 2 columns, got %v", res.Columns)
	}
	// Value sanity: first row has an id.
	if _, ok := res.Rows[0]["id"]; !ok {
		t.Errorf("first row missing 'id' column: %v", res.Rows[0])
	}
}
