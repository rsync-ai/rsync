package cdc

import "testing"

func TestSplitSchemaTableSS(t *testing.T) {
	cases := []struct {
		in         string
		wantSchema string
		wantTable  string
	}{
		{"dbo.orders", "dbo", "orders"},
		{"sales.customers", "sales", "customers"},
		{"orders", "dbo", "orders"}, // unqualified defaults to dbo
		{"  dbo.orders  ", "dbo", "orders"},
		{"", "", ""},
	}
	for _, tc := range cases {
		s, tbl := splitSchemaTableSS(tc.in)
		if s != tc.wantSchema || tbl != tc.wantTable {
			t.Errorf("splitSchemaTableSS(%q) = (%q,%q), want (%q,%q)", tc.in, s, tbl, tc.wantSchema, tc.wantTable)
		}
	}
}

func TestResolveSQLServerEncrypt(t *testing.T) {
	cases := []struct {
		name      string
		config    map[string]interface{}
		host      string
		wantEnc   string
		wantTrust bool
	}{
		{"local no tls", map[string]interface{}{}, "localhost", "disable", false},
		{"docker host", map[string]interface{}{}, "sqlserver", "disable", false},
		{"azure sql encrypt + verify (no blind trust)", map[string]interface{}{}, "myserver.database.windows.net", "true", false},
		{"non-azure remote defaults to encrypt+verify", map[string]interface{}{}, "db.example.com", "true", false},
		{"explicit disable wins over azure", map[string]interface{}{"encrypt": "disable"}, "myserver.database.windows.net", "disable", false},
		{"explicit require -> true+trust", map[string]interface{}{"ssl_mode": "require"}, "localhost", "true", true},
		{"verify-full -> true no trust", map[string]interface{}{"sslmode": "verify-full"}, "localhost", "true", false},
	}
	for _, tc := range cases {
		enc, trust := resolveSQLServerEncrypt(tc.config, tc.host)
		if enc != tc.wantEnc || trust != tc.wantTrust {
			t.Errorf("%s: resolveSQLServerEncrypt = (%q,%v), want (%q,%v)", tc.name, enc, trust, tc.wantEnc, tc.wantTrust)
		}
	}
}

func TestIsLocalSQLServerHost(t *testing.T) {
	local := []string{"", "localhost", "127.0.0.1", "::1", "host.docker.internal", "sqlserver", "mssql-1"}
	remote := []string{"myserver.database.windows.net", "10.0.0.5.nip.io", "db.example.com"}
	for _, h := range local {
		if !isLocalSQLServerHost(h) {
			t.Errorf("isLocalSQLServerHost(%q) = false, want true", h)
		}
	}
	for _, h := range remote {
		if isLocalSQLServerHost(h) {
			t.Errorf("isLocalSQLServerHost(%q) = true, want false", h)
		}
	}
}

// A capture instance name must be deterministic per pipeline+table and stay
// within SQL Server's identifier limits.
func TestCaptureInstanceNaming(t *testing.T) {
	cfg := CDCResourceConfig{
		PipelineID:   "abcdef12-3456-7890-abcd-ef1234567890",
		ConnectionID: "conn-1",
		Database:     "salesdb",
		Table:        "dbo.orders",
	}
	name := GenerateResourceName(cfg, "capture_instance")
	if name == "" {
		t.Fatal("capture instance name is empty")
	}
	if len(name) > 100 {
		t.Errorf("capture instance name %q exceeds SQL Server's 100-char limit", name)
	}
	// Deterministic: same inputs -> same name.
	if again := GenerateResourceName(cfg, "capture_instance"); again != name {
		t.Errorf("capture instance name not deterministic: %q vs %q", name, again)
	}
	// Distinct per table.
	cfg2 := cfg
	cfg2.Table = "dbo.customers"
	if other := GenerateResourceName(cfg2, "capture_instance"); other == name {
		t.Errorf("capture instance name collides across tables: %q", name)
	}
}

// SQL Server PK validation qualifies unqualified names by schema, defaulting to dbo.
func TestSQLServerPrimaryKeyNamespace(t *testing.T) {
	mgr := NewSQLServerManager(nil)
	if got := mgr.PrimaryKeyNamespace("mydb", "sales"); got != "sales" {
		t.Errorf("PrimaryKeyNamespace with schema = %q, want sales", got)
	}
	if got := mgr.PrimaryKeyNamespace("mydb", ""); got != "dbo" {
		t.Errorf("PrimaryKeyNamespace empty schema = %q, want dbo", got)
	}
}
