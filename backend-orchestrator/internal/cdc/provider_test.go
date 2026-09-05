package cdc

import (
	"database/sql"
	"testing"
)

// The two built-in families must self-register via their init() functions.
func TestBuiltinProvidersRegistered(t *testing.T) {
	for _, dbType := range []string{"postgresql", "mysql"} {
		if !IsSupportedDBType(dbType) {
			t.Fatalf("expected %q to have a registered CDC provider", dbType)
		}
	}
}

func TestNewProviderDispatch(t *testing.T) {
	var db *sql.DB // nil is fine: we never touch it, just check dispatch + identity.

	cases := []struct {
		dbType     string
		wantOK     bool
		wantFamily string
	}{
		{"postgresql", true, "postgresql"},
		{"postgres", true, "postgresql"}, // alias folds to postgresql
		{"mysql", true, "mysql"},
		{"MySQL", true, "mysql"},         // case-insensitive
		{"  mysql ", true, "mysql"},      // trimmed
		{"sqlserver", true, "sqlserver"}, // registered in M1
		{"mssql", true, "sqlserver"},     // alias folds to sqlserver
		{"mongodb", true, "mongodb"},     // registered in M2 (no-op provisioner)
		{"oracle", true, "oracle"},       // registered in M3 (LogMiner supplemental logging)
		{"oracledb", true, "oracle"},     // alias folds to oracle
		{"", false, ""},
	}

	for _, tc := range cases {
		mgr, ok := NewProvider(tc.dbType, db)
		if ok != tc.wantOK {
			t.Errorf("NewProvider(%q): ok=%v, want %v", tc.dbType, ok, tc.wantOK)
			continue
		}
		if ok && mgr.Family() != tc.wantFamily {
			t.Errorf("NewProvider(%q): family=%q, want %q", tc.dbType, mgr.Family(), tc.wantFamily)
		}
	}
}

// PK validation must qualify unqualified table names by schema (PostgreSQL) or
// database (MySQL) — the one per-family difference the old switch encoded.
func TestPrimaryKeyNamespaceSelection(t *testing.T) {
	pg, _ := NewProvider("postgresql", nil)
	if got := pg.PrimaryKeyNamespace("mydb", "myschema"); got != "myschema" {
		t.Errorf("postgresql PrimaryKeyNamespace = %q, want %q", got, "myschema")
	}

	my, _ := NewProvider("mysql", nil)
	if got := my.PrimaryKeyNamespace("mydb", "myschema"); got != "mydb" {
		t.Errorf("mysql PrimaryKeyNamespace = %q, want %q", got, "mydb")
	}
}

func TestNormalizeDBType(t *testing.T) {
	cases := map[string]string{
		"postgres":   "postgresql",
		"postgresql": "postgresql",
		"POSTGRES":   "postgresql",
		"mssql":      "sqlserver",
		"mysql":      "mysql",
		" Oracle ":   "oracle",
	}
	for in, want := range cases {
		if got := NormalizeDBType(in); got != want {
			t.Errorf("NormalizeDBType(%q) = %q, want %q", in, got, want)
		}
	}
}

// Cleanup relies on RegisteredDBTypes to run teardown for every known family.
func TestRegisteredDBTypesContainsBuiltins(t *testing.T) {
	got := RegisteredDBTypes()
	found := map[string]bool{}
	for _, t := range got {
		found[t] = true
	}
	if !found["postgresql"] || !found["mysql"] {
		t.Fatalf("RegisteredDBTypes()=%v, want it to include postgresql and mysql", got)
	}
}
