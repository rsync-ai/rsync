package handlers

import "testing"

// TestDialectFromConnectorType pins the connector_type -> SQL dialect mapping
// used to harden POST /api/v1/sql/generate so it can derive the dialect from a
// connection instead of blindly defaulting to postgresql. Mirrors the
// isMySQL/isPostgres checks in ExecuteExplorerQuery.
func TestDialectFromConnectorType(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"mysql", "mysql"},
		{"MySQL", "mysql"},
		{"mysql-cdc", "mysql"},
		{"mariadb", "mysql"},
		{"postgresql", "postgresql"},
		{"postgres", "postgresql"},
		{"PostgreSQL", "postgresql"},
		{"redshift", "postgresql"},
		{"databricks", "databricks"},
		{"google-sheets", ""},
		{"shopify-admin-graphql", ""},
		{"", ""},
	}
	for _, tc := range cases {
		if got := dialectFromConnectorType(tc.in); got != tc.want {
			t.Errorf("dialectFromConnectorType(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
