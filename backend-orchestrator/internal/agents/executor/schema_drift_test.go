package executor

import (
	"sort"
	"strings"
	"testing"

	"github.com/rsync-ai/backend-orchestrator/internal/agents/healer"
)

func tbl(schema, name string, cols ...[2]string) TableMetadata {
	t := TableMetadata{Schema: schema, Name: name}
	for _, c := range cols {
		t.Columns = append(t.Columns, ColumnMetadata{Name: c[0], Type: c[1]})
	}
	return t
}

func col(name, typ string) [2]string { return [2]string{name, typ} }

func TestBaselineKey(t *testing.T) {
	if got := baselineKey("public", "orders"); got != "public.orders" {
		t.Errorf("qualified: got %q", got)
	}
	if got := baselineKey("", "orders"); got != "orders" {
		t.Errorf("bare: got %q", got)
	}
	if got := baselineKey("  public ", "  orders "); got != "public.orders" {
		t.Errorf("trimmed: got %q", got)
	}
}

func TestSelectedTableSet(t *testing.T) {
	// Records both qualified and bare forms regardless of input shape.
	got := selectedTableSet([]interface{}{"public.orders", "customers"})
	for _, want := range []string{"public.orders", "orders", "customers"} {
		if !got[want] {
			t.Errorf("missing %q in selected set %v", want, got)
		}
	}
	// []string input form.
	got2 := selectedTableSet([]string{"sales.invoices"})
	if !got2["sales.invoices"] || !got2["invoices"] {
		t.Errorf("[]string form not normalised: %v", got2)
	}
	// Non-list input -> empty set (no panic).
	if len(selectedTableSet("nope")) != 0 || len(selectedTableSet(nil)) != 0 {
		t.Errorf("non-list input should yield empty set")
	}
}

func TestFilterDiscoveredToSelected_DropsUnrelated(t *testing.T) {
	discovered := []TableMetadata{
		tbl("public", "orders", col("id", "int")),
		tbl("public", "unrelated", col("id", "int")),
		tbl("public", "customers", col("id", "int")),
	}
	selected := selectedTableSet([]interface{}{"orders", "public.customers"})

	got := filterDiscoveredToSelected(discovered, selected)
	names := map[string]bool{}
	for _, x := range got {
		names[baselineKey(x.Schema, x.Name)] = true
	}
	if !names["public.orders"] || !names["public.customers"] {
		t.Errorf("selected tables dropped: %v", names)
	}
	if names["public.unrelated"] {
		t.Errorf("unrelated table must NOT be in scope (RT-fix #3): %v", names)
	}
	// Empty selection keeps nothing (never baseline an unscoped whole-DB snapshot).
	if got := filterDiscoveredToSelected(discovered, map[string]bool{}); len(got) != 0 {
		t.Errorf("empty selection must keep nothing, got %d", len(got))
	}
}

// diffNames returns "changeType:table:column" tuples sorted, for stable assertions.
func diffNames(prior, current []TableMetadata) []string {
	deltas := diffSchemas(tableMapByKey(prior), tableMapByKey(current), "")
	out := make([]string, 0, len(deltas))
	for _, d := range deltas {
		out = append(out, d.ChangeType+":"+d.Table+":"+d.ColumnName)
	}
	sort.Strings(out)
	return out
}

func TestDiffSchemas(t *testing.T) {
	base := []TableMetadata{tbl("public", "orders", col("id", "int"), col("name", "text"))}

	cases := []struct {
		name    string
		prior   []TableMetadata
		current []TableMetadata
		want    []string
	}{
		{
			name:    "no change",
			prior:   base,
			current: []TableMetadata{tbl("public", "orders", col("id", "int"), col("name", "text"))},
			want:    []string{},
		},
		{
			name:    "add column (informational)",
			prior:   base,
			current: []TableMetadata{tbl("public", "orders", col("id", "int"), col("name", "text"), col("total", "numeric"))},
			want:    []string{"add_column:public.orders:total"},
		},
		{
			name:    "drop column (approve)",
			prior:   base,
			current: []TableMetadata{tbl("public", "orders", col("id", "int"))},
			want:    []string{"drop_column:public.orders:name"},
		},
		{
			name:    "modify column type (approve)",
			prior:   base,
			current: []TableMetadata{tbl("public", "orders", col("id", "bigint"), col("name", "text"))},
			want:    []string{"modify_column:public.orders:id"},
		},
		{
			name:    "create table (informational)",
			prior:   base,
			current: []TableMetadata{tbl("public", "orders", col("id", "int"), col("name", "text")), tbl("public", "shipments", col("id", "int"))},
			want:    []string{"create_table:public.shipments:"},
		},
		{
			name:    "drop table (approve)",
			prior:   []TableMetadata{tbl("public", "orders", col("id", "int")), tbl("public", "legacy", col("id", "int"))},
			current: []TableMetadata{tbl("public", "orders", col("id", "int"))},
			want:    []string{"drop_table:public.legacy:"},
		},
		{
			name:    "case-insensitive type = no drift",
			prior:   []TableMetadata{tbl("public", "orders", col("id", "INT"))},
			current: []TableMetadata{tbl("public", "orders", col("id", "int"))},
			want:    []string{},
		},
	}

	allowed := map[string]bool{"add_column": true, "drop_column": true, "modify_column": true, "create_table": true, "drop_table": true}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := diffNames(tc.prior, tc.current)
			if len(got) != len(tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("got[%d]=%q want %q (full: %v)", i, got[i], tc.want[i], got)
				}
			}
			// Every emitted ChangeType must be one of the 5 the healer switch consumes.
			for _, d := range diffSchemas(tableMapByKey(tc.prior), tableMapByKey(tc.current), "") {
				if !allowed[d.ChangeType] {
					t.Errorf("illegal ChangeType %q (would hit healer default branch)", d.ChangeType)
				}
				if d.DDL == "" {
					t.Errorf("empty DDL for %s (breaks UNIQUE(pipeline_id,ddl) dedup)", d.ChangeType)
				}
			}
		})
	}
}

func TestParseSchemaDriftPolicy(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want SchemaDriftPolicy
	}{
		{"absent (empty) -> all enabled", "", SchemaDriftPolicy{Enabled: true, NotifyOnAdd: true, NotifyOnDrop: true}},
		{"jsonb null -> all enabled", "null", SchemaDriftPolicy{Enabled: true, NotifyOnAdd: true, NotifyOnDrop: true}},
		{"empty object -> absent fields default true", "{}", SchemaDriftPolicy{Enabled: true, NotifyOnAdd: true, NotifyOnDrop: true}},
		{"explicit disable", `{"enabled":false}`, SchemaDriftPolicy{Enabled: false, NotifyOnAdd: true, NotifyOnDrop: true}},
		{"opt out of adds only", `{"enabled":true,"notify_on_add":false}`, SchemaDriftPolicy{Enabled: true, NotifyOnAdd: false, NotifyOnDrop: true}},
		{"opt out of drops only", `{"notify_on_drop":false}`, SchemaDriftPolicy{Enabled: true, NotifyOnAdd: true, NotifyOnDrop: false}},
		{"all off", `{"enabled":false,"notify_on_add":false,"notify_on_drop":false}`, SchemaDriftPolicy{Enabled: false, NotifyOnAdd: false, NotifyOnDrop: false}},
		{"invalid json -> fail open to enabled", "{nope", SchemaDriftPolicy{Enabled: true, NotifyOnAdd: true, NotifyOnDrop: true}},
		{"wrong field type -> fail open to enabled", `{"enabled":"yes"}`, SchemaDriftPolicy{Enabled: true, NotifyOnAdd: true, NotifyOnDrop: true}},
		{"whitespace-padded", "  {\"enabled\":false}  ", SchemaDriftPolicy{Enabled: false, NotifyOnAdd: true, NotifyOnDrop: true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := parseSchemaDriftPolicy([]byte(tc.raw)); got != tc.want {
				t.Errorf("parseSchemaDriftPolicy(%q) = %+v, want %+v", tc.raw, got, tc.want)
			}
		})
	}
	if got := parseSchemaDriftPolicy(nil); got != defaultSchemaDriftPolicy() {
		t.Errorf("nil raw must yield defaults, got %+v", got)
	}
}

func TestFilterChangesBySchemaDriftPolicy(t *testing.T) {
	// One delta per ChangeType (built through the real differ so the tuple
	// stays representative of production emissions).
	adds := diffSchemas(
		tableMapByKey([]TableMetadata{tbl("public", "orders", col("id", "int"))}),
		tableMapByKey([]TableMetadata{tbl("public", "orders", col("id", "int"), col("total", "numeric")), tbl("public", "shipments", col("id", "int"))}), "",
	) // add_column + create_table
	drops := diffSchemas(
		tableMapByKey([]TableMetadata{tbl("public", "orders", col("id", "int"), col("name", "text")), tbl("public", "legacy", col("id", "int"))}),
		tableMapByKey([]TableMetadata{tbl("public", "orders", col("id", "int"))}), "",
	) // drop_column + drop_table
	modify := diffSchemas(
		tableMapByKey([]TableMetadata{tbl("public", "orders", col("id", "int"))}),
		tableMapByKey([]TableMetadata{tbl("public", "orders", col("id", "bigint"))}), "",
	) // modify_column
	all := append(append(append([]healer.SchemaChange{}, adds...), drops...), modify...)

	types := func(changes []healer.SchemaChange) map[string]int {
		out := map[string]int{}
		for _, c := range changes {
			out[c.ChangeType]++
		}
		return out
	}

	cases := []struct {
		name   string
		policy SchemaDriftPolicy
		want   map[string]int
	}{
		{"all-on passes everything", defaultSchemaDriftPolicy(),
			map[string]int{"add_column": 1, "create_table": 1, "drop_column": 1, "drop_table": 1, "modify_column": 1}},
		{"notify_on_add=false drops additive only", SchemaDriftPolicy{Enabled: true, NotifyOnAdd: false, NotifyOnDrop: true},
			map[string]int{"drop_column": 1, "drop_table": 1, "modify_column": 1}},
		{"notify_on_drop=false drops drop drift only", SchemaDriftPolicy{Enabled: true, NotifyOnAdd: true, NotifyOnDrop: false},
			map[string]int{"add_column": 1, "create_table": 1, "modify_column": 1}},
		{"both off still keeps modify_column", SchemaDriftPolicy{Enabled: true, NotifyOnAdd: false, NotifyOnDrop: false},
			map[string]int{"modify_column": 1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := types(filterChangesByPolicy(tc.policy, all))
			if len(got) != len(tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
			for k, n := range tc.want {
				if got[k] != n {
					t.Errorf("type %s: got %d, want %d (full: %v)", k, got[k], n, got)
				}
			}
		})
	}
}

func TestTypeBase(t *testing.T) {
	cases := []struct{ in, base, qual string }{
		{"varchar(50)", "varchar", "(50)"},
		{"VARCHAR(50)", "varchar", "(50)"},
		{"varchar", "varchar", ""},
		{"decimal(12, 2)", "decimal", "(12,2)"}, // qualifier whitespace is not a difference
		{"int unsigned", "int unsigned", ""},
		{"  timestamp without time zone  ", "timestamp without time zone", ""},
		{"", "", ""},
	}
	for _, tc := range cases {
		base, qual := typeBase(tc.in)
		if base != tc.base || qual != tc.qual {
			t.Errorf("typeBase(%q) = (%q, %q), want (%q, %q)", tc.in, base, qual, tc.base, tc.qual)
		}
	}
}

// B1. The asymmetric cases are the load-bearing ones: connectors used to report a
// bare "varchar" as source_type and now report "varchar(50)", so a baseline taken
// before that connector upgrade holds the bare form. Treating that as drift would
// file one bogus modify_column per column of every table the first time each
// pipeline ran after the upgrade. A missing qualifier means "the source did not
// tell us", never "there is none" — in either direction, since a rollback puts the
// bare form on the current side instead.
func TestNativeTypeDrifted(t *testing.T) {
	cases := []struct {
		name      string
		prev, cur string
		want      bool
	}{
		{"narrowed varchar", "varchar(50)", "varchar(10)", true},
		{"widened varchar", "varchar(10)", "varchar(50)", true},
		{"narrowed decimal scale", "decimal(12,2)", "decimal(4,2)", true},
		{"different base token", "bigint", "smallint", true},
		{"signedness change", "int", "int unsigned", true},
		{"identical", "varchar(50)", "varchar(50)", false},
		{"case only", "VARCHAR(50)", "varchar(50)", false},
		{"qualifier whitespace only", "decimal(12, 2)", "decimal(12,2)", false},
		{"connector upgrade: bare -> qualified", "varchar", "varchar(50)", false},
		{"connector rollback: qualified -> bare", "varchar(50)", "varchar", false},
		{"prev unknown (pre-B1 baseline)", "", "varchar(10)", false},
		{"current unknown (connector not yet upgraded)", "varchar(50)", "", false},
		{"both unknown", "", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := nativeTypeDrifted(tc.prev, tc.cur); got != tc.want {
				t.Errorf("nativeTypeDrifted(%q, %q) = %v, want %v", tc.prev, tc.cur, got, tc.want)
			}
		})
	}
}

// ntbl builds a one-column table whose canonical type is fixed at "string" and
// whose DECLARED source type varies — the exact shape B1 was invisible in.
func ntbl(colName, sourceType string) []TableMetadata {
	return []TableMetadata{{
		Schema:  "public",
		Name:    "orders",
		Columns: []ColumnMetadata{{Name: colName, Type: "string", SourceType: sourceType}},
	}}
}

// B1: VARCHAR(50) -> VARCHAR(10) is "string" -> "string" canonically, so the old
// detector saw nothing — proven on prod, where that narrowing produced zero drift
// rows while a cross-type change on a sibling column in the same run produced one.
// It must now emit exactly one modify_column, and that row must be an ADVISORY
// (leading "--"), never an executable statement: the destination column is already
// the right canonical type, ensureColumnViaConnector(strict=true) refuses to narrow,
// and narrowing it to match would truncate rows the destination already holds.
func TestDiffSchemas_DeclaredTypeNarrowingIsAdvisoryModifyColumn(t *testing.T) {
	deltas := diffSchemas(
		tableMapByKey(ntbl("note", "varchar(50)")),
		tableMapByKey(ntbl("note", "varchar(10)")), "",
	)
	if len(deltas) != 1 {
		t.Fatalf("want exactly 1 delta for a source-side narrowing, got %d: %+v", len(deltas), deltas)
	}
	d := deltas[0]
	if d.ChangeType != "modify_column" || d.Table != "public.orders" || d.ColumnName != "note" {
		t.Errorf("wrong delta identity: %+v", d)
	}
	if !strings.HasPrefix(strings.TrimSpace(d.DDL), "--") {
		t.Errorf("declared-type drift must be advisory, not an executable statement; got DDL %q", d.DDL)
	}
	if !strings.Contains(d.DDL, "varchar(50)") || !strings.Contains(d.DDL, "varchar(10)") {
		t.Errorf("advisory must name both declared types so the user can act on it; got %q", d.DDL)
	}

	// The transition state must stay silent in both orders (see TestNativeTypeDrifted).
	for _, tc := range [][2]string{{"varchar", "varchar(50)"}, {"varchar(50)", "varchar"}, {"", "varchar(10)"}} {
		if got := diffSchemas(tableMapByKey(ntbl("note", tc[0])), tableMapByKey(ntbl("note", tc[1])), ""); len(got) != 0 {
			t.Errorf("source_type %q -> %q must not drift (half-deployed connector), got %+v", tc[0], tc[1], got)
		}
	}

	// A real canonical change still wins and stays executable — the advisory branch
	// is only reached when the destination needs nothing.
	real := diffSchemas(
		tableMapByKey([]TableMetadata{{Schema: "public", Name: "orders", Columns: []ColumnMetadata{{Name: "id", Type: "string", SourceType: "varchar(50)"}}}}),
		tableMapByKey([]TableMetadata{{Schema: "public", Name: "orders", Columns: []ColumnMetadata{{Name: "id", Type: "number", SourceType: "bigint"}}}}), "",
	)
	if len(real) != 1 || real[0].ChangeType != "modify_column" {
		t.Fatalf("canonical type change must still emit one modify_column, got %+v", real)
	}
	if strings.HasPrefix(strings.TrimSpace(real[0].DDL), "--") {
		t.Errorf("a canonical type change is a real statement, not a notice: %q", real[0].DDL)
	}
}

// A column dropped on an out-of-scope table must produce ZERO drift once filtered.
func TestDiffSchemas_OutOfScopeTableNoEvent(t *testing.T) {
	selected := selectedTableSet([]interface{}{"orders"})
	priorDisc := []TableMetadata{tbl("public", "orders", col("id", "int")), tbl("public", "unrelated", col("a", "int"), col("b", "int"))}
	currentDisc := []TableMetadata{tbl("public", "orders", col("id", "int")), tbl("public", "unrelated", col("a", "int"))} // dropped b

	prior := filterDiscoveredToSelected(priorDisc, selected)
	current := filterDiscoveredToSelected(currentDisc, selected)
	if deltas := diffSchemas(tableMapByKey(prior), tableMapByKey(current), ""); len(deltas) != 0 {
		t.Errorf("out-of-scope table change must emit nothing, got %v", deltas)
	}
}

// The drop DDL is the one string in this file the product tells a human to RUN, and it
// runs against the DESTINATION. Built from the source key it read
// `DROP TABLE public.legacy` — and `public.legacy` is a real, different table on the
// destination, because the pipeline writes into rsync_public_<id8>. This pins the
// qualifier to the destination namespace.
func TestDiffSchemas_DropDDLNamesTheDestinationObject(t *testing.T) {
	prior := []TableMetadata{
		tbl("public", "orders", col("id", "int"), col("name", "text")),
		tbl("public", "legacy", col("id", "int")),
	}
	current := []TableMetadata{tbl("public", "orders", col("id", "int"))}

	deltas := diffSchemas(tableMapByKey(prior), tableMapByKey(current), "rsync_public_a1b2c3d4")

	ddl := map[string]string{}
	for _, d := range deltas {
		ddl[d.ChangeType] = d.DDL
		// Identity stays the SOURCE's: the change was observed there, and the healer's
		// own lookups key off Table/SchemaName.
		if d.SchemaName != "public" {
			t.Errorf("%s: SchemaName = %q, want the source schema", d.ChangeType, d.SchemaName)
		}
	}

	if got, want := ddl["drop_column"], "ALTER TABLE rsync_public_a1b2c3d4.orders DROP COLUMN name"; got != want {
		t.Errorf("drop_column DDL = %q, want %q", got, want)
	}
	if got, want := ddl["drop_table"], "DROP TABLE rsync_public_a1b2c3d4.legacy"; got != want {
		t.Errorf("drop_table DDL = %q, want %q", got, want)
	}
	for ct, d := range ddl {
		if strings.Contains(d, "public.orders") || strings.Contains(d, "public.legacy") {
			t.Errorf("%s DDL still names the SOURCE object: %q", ct, d)
		}
	}
}

// Legacy pipelines have no persisted namespace. The bare table name is what resolves in
// the destination's default schema; the source qualifier must never be the fallback.
func TestDiffSchemas_DropDDLFallsBackToBareTableWithoutNamespace(t *testing.T) {
	prior := []TableMetadata{
		tbl("sales", "orders", col("id", "int"), col("name", "text")),
		tbl("sales", "legacy", col("id", "int")),
	}
	current := []TableMetadata{tbl("sales", "orders", col("id", "int"))}

	for _, ns := range []string{"", "   ", "default", "DEFAULT"} {
		got := map[string]string{}
		for _, d := range diffSchemas(tableMapByKey(prior), tableMapByKey(current), ns) {
			got[d.ChangeType] = d.DDL
		}
		if want := "ALTER TABLE orders DROP COLUMN name"; got["drop_column"] != want {
			t.Errorf("ns=%q drop_column DDL = %q, want %q", ns, got["drop_column"], want)
		}
		if want := "DROP TABLE legacy"; got["drop_table"] != want {
			t.Errorf("ns=%q drop_table DDL = %q, want %q", ns, got["drop_table"], want)
		}
	}
}

// The appliable ALTERs deliberately keep the source key: the healer rewrites them at
// apply time (bareTable + destinationNamespace), and re-texting them would re-key the
// schema_change_approvals UNIQUE(pipeline_id, ddl) dedup for every pending row.
func TestDiffSchemas_AppliableDDLKeepsTheSourceKey(t *testing.T) {
	prior := []TableMetadata{tbl("public", "orders", col("id", "int"))}
	current := []TableMetadata{tbl("public", "orders", col("id", "bigint"), col("total", "numeric"))}

	for _, d := range diffSchemas(tableMapByKey(prior), tableMapByKey(current), "rsync_public_a1b2c3d4") {
		if !strings.Contains(d.DDL, "public.orders") {
			t.Errorf("%s DDL should still carry the source key, got %q", d.ChangeType, d.DDL)
		}
	}
}
