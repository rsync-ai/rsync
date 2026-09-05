package healer

import "testing"

// These guard the pure logic that routes an approved additive drift change to the
// destination connector's ensure_table instead of running source-shaped DDL
// directly against the dest. Broker-free and DB-free — the parse/fold/strip
// helpers are the single source of truth applyMigration uses to decide the route
// and to build the ensure_table params, so asserting them asserts the routing
// decision without standing up Kafka, a DB, or a live connector.

func TestParseAddColumnDDL(t *testing.T) {
	cases := []struct {
		name                       string
		ddl                        string
		wantOK                     bool
		wantTable, wantCol, wantTy string
	}{
		{
			name:      "source-qualified add column (the prod repro)",
			ddl:       "ALTER TABLE pipeline_test.categories ADD COLUMN drift_smoke string",
			wantOK:    true,
			wantTable: "pipeline_test.categories", wantCol: "drift_smoke", wantTy: "string",
		},
		{
			name:      "bare table add column",
			ddl:       "ALTER TABLE categories ADD COLUMN note text",
			wantOK:    true,
			wantTable: "categories", wantCol: "note", wantTy: "text",
		},
		{
			name:      "multi-word type is preserved",
			ddl:       "ALTER TABLE s.t ADD COLUMN amount double precision",
			wantOK:    true,
			wantTable: "s.t", wantCol: "amount", wantTy: "double precision",
		},
		{
			name:      "case-insensitive keywords",
			ddl:       "alter table s.t add column c bigint",
			wantOK:    true,
			wantTable: "s.t", wantCol: "c", wantTy: "bigint",
		},
		// Non-additive / other shapes must NOT match — they fall through to direct apply.
		{name: "modify column falls through", ddl: "ALTER TABLE s.t ALTER COLUMN c TYPE bigint", wantOK: false},
		{name: "drop column falls through", ddl: "ALTER TABLE s.t DROP COLUMN c", wantOK: false},
		{name: "drop table falls through", ddl: "DROP TABLE s.t", wantOK: false},
		{name: "comment-only create_table placeholder", ddl: "-- drift: table s.t appeared in source", wantOK: false},
		{name: "missing type", ddl: "ALTER TABLE s.t ADD COLUMN c", wantOK: false},
		{name: "empty", ddl: "", wantOK: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			table, col, ty, ok := parseAddColumnDDL(tc.ddl)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v (ddl=%q)", ok, tc.wantOK, tc.ddl)
			}
			if !tc.wantOK {
				return
			}
			if table != tc.wantTable || col != tc.wantCol || ty != tc.wantTy {
				t.Errorf("got (%q, %q, %q), want (%q, %q, %q)", table, col, ty, tc.wantTable, tc.wantCol, tc.wantTy)
			}
		})
	}
}

func TestParseModifyColumnDDL(t *testing.T) {
	cases := []struct {
		name                       string
		ddl                        string
		wantOK                     bool
		wantTable, wantCol, wantTy string
	}{
		{
			name:      "source-qualified modify column (the prod repro)",
			ddl:       "ALTER TABLE inventory.products ALTER COLUMN price TYPE number",
			wantOK:    true,
			wantTable: "inventory.products", wantCol: "price", wantTy: "number",
		},
		{
			name:      "bare table modify column",
			ddl:       "ALTER TABLE products ALTER COLUMN qty TYPE integer",
			wantOK:    true,
			wantTable: "products", wantCol: "qty", wantTy: "integer",
		},
		{
			name:      "multi-word type is preserved",
			ddl:       "ALTER TABLE s.t ALTER COLUMN amount TYPE double precision",
			wantOK:    true,
			wantTable: "s.t", wantCol: "amount", wantTy: "double precision",
		},
		{
			name:      "case-insensitive keywords",
			ddl:       "alter table s.t alter column c type bigint",
			wantOK:    true,
			wantTable: "s.t", wantCol: "c", wantTy: "bigint",
		},
		// Other shapes must NOT match — they fall through to the next router / direct apply.
		{name: "add column falls through", ddl: "ALTER TABLE s.t ADD COLUMN c bigint", wantOK: false},
		{name: "drop column falls through", ddl: "ALTER TABLE s.t DROP COLUMN c", wantOK: false},
		{name: "drop table falls through", ddl: "DROP TABLE s.t", wantOK: false},
		{name: "comment-only create_table placeholder", ddl: "-- drift: table s.t appeared in source", wantOK: false},
		{name: "missing type", ddl: "ALTER TABLE s.t ALTER COLUMN c TYPE", wantOK: false},
		{name: "empty", ddl: "", wantOK: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			table, col, ty, ok := parseModifyColumnDDL(tc.ddl)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v (ddl=%q)", ok, tc.wantOK, tc.ddl)
			}
			if !tc.wantOK {
				return
			}
			if table != tc.wantTable || col != tc.wantCol || ty != tc.wantTy {
				t.Errorf("got (%q, %q, %q), want (%q, %q, %q)", table, col, ty, tc.wantTable, tc.wantCol, tc.wantTy)
			}
		})
	}
}

// The add-column and modify-column parsers must be mutually exclusive: each shape
// routes to exactly one connector call, never both, never neither-then-raw. This
// guards the applyMigration routing fork (add_column → widen-safe ensure; modify_column
// → strict ensure) against a future edit that makes one parser greedily match the other.
func TestAddAndModifyParsersAreMutuallyExclusive(t *testing.T) {
	add := "ALTER TABLE s.t ADD COLUMN c bigint"
	mod := "ALTER TABLE s.t ALTER COLUMN c TYPE bigint"

	if _, _, _, ok := parseAddColumnDDL(mod); ok {
		t.Error("parseAddColumnDDL must NOT match a modify_column DDL")
	}
	if _, _, _, ok := parseModifyColumnDDL(add); ok {
		t.Error("parseModifyColumnDDL must NOT match an add_column DDL")
	}
	if _, _, _, ok := parseAddColumnDDL(add); !ok {
		t.Error("parseAddColumnDDL must match its own add_column DDL")
	}
	if _, _, _, ok := parseModifyColumnDDL(mod); !ok {
		t.Error("parseModifyColumnDDL must match its own modify_column DDL")
	}
}

func TestBareTable(t *testing.T) {
	cases := map[string]string{
		"pipeline_test.categories": "categories",
		"categories":               "categories",
		"a.b.c":                    "c",
		"  public.orders  ":        "orders",
		"trailing.":                "trailing.", // no bare segment → returned as-is (trimmed)
	}
	for in, want := range cases {
		if got := bareTable(in); got != want {
			t.Errorf("bareTable(%q) = %q, want %q", in, got, want)
		}
	}
}

// normalizeDestConnector MUST accept the canonical "postgresql" the DB stores —
// the original bug was a switch that matched only "postgres", sending every
// Postgres dest to the "unsupported destination" default and silently skipping
// the apply.
func TestNormalizeDestConnector(t *testing.T) {
	cases := map[string]string{
		"postgres":   "postgresql",
		"postgresql": "postgresql",
		"PostgreSQL": "postgresql",
		"mysql":      "mysql",
		"mariadb":    "mysql",
		" MySQL ":    "mysql",
		"bigquery":   "bigquery", // pass-through for others
	}
	for in, want := range cases {
		if got := normalizeDestConnector(in); got != want {
			t.Errorf("normalizeDestConnector(%q) = %q, want %q", in, got, want)
		}
	}
}
