package executor

import "testing"

// TestResolveDestTableName locks in the C1 fix for the source-schema namespace
// leak: a PostgreSQL source schema (e.g. "rsync_public.customers") must NOT reach
// a relational destination as a schema-qualified name, or the connector writes to
// "rsync_public.customers" and the expected public.customers stays empty
// (read>0/landed=0 silent drop). Object-storage dests and namespaced pipelines
// keep their prior behavior.
func TestResolveDestTableName(t *testing.T) {
	cases := []struct {
		name        string
		tableName   string
		namespace   string
		objectStore bool
		want        string
	}{
		// --- the bug: relational dest, no namespace, source-qualified PG schema ---
		{"pg_source_schema_no_namespace_bareified", "rsync_public.customers", "", false, "customers"},
		{"public_schema_no_namespace_bareified", "public.golden_types_1", "", false, "golden_types_1"},
		{"bare_source_no_namespace_unchanged", "customers", "", false, "customers"},
		{"mysql_sourcedb_no_namespace_bareified", "sourcedb.orders", "", false, "orders"},
		{"three_part_no_namespace_bareified", "mydb.rsync_public.customers", "", false, "customers"},

		// --- namespaced (HITL): qualify the BARE table with the namespace ---
		{"namespace_qualified_source", "rsync_public.customers", "analytics", false, "analytics.customers"},
		{"namespace_bare_source", "customers", "analytics", false, "analytics.customers"},
		{"namespace_three_part_source", "mydb.rsync_public.customers", "analytics", false, "analytics.customers"},

		// --- object storage: preserve source-derived name (partitioning via dbOrSchema) ---
		{"objectstore_no_namespace_unchanged", "rsync_public.customers", "", true, "rsync_public.customers"},
		{"objectstore_bare_unchanged", "customers", "", true, "customers"},
		// namespace still applies for object storage (matches prior behavior).
		{"objectstore_with_namespace", "rsync_public.customers", "analytics", true, "analytics.customers"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := resolveDestTableName(tc.tableName, tc.namespace, tc.objectStore)
			if got != tc.want {
				t.Errorf("resolveDestTableName(%q, %q, objectStore=%v) = %q; want %q",
					tc.tableName, tc.namespace, tc.objectStore, got, tc.want)
			}
		})
	}
}
