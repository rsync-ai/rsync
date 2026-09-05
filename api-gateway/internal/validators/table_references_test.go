package validators

import (
	"strings"
	"testing"
)

// What this parser feeds is a suggestion an admin confirms, so the tests are weighted
// toward the invented dependency rather than the missed one. A miss costs a suggestion;
// an invention offers to hang a schedule off an unrelated pipeline.

func qualifiedNames(refs []TableRef) []string {
	out := make([]string, 0, len(refs))
	for _, r := range refs {
		out = append(out, r.Qualified())
	}
	return out
}

func assertRefs(t *testing.T, sql string, want ...string) {
	t.Helper()
	got := qualifiedNames(ExtractTableReferences(sql))
	if len(got) != len(want) {
		t.Fatalf("ExtractTableReferences(%q)\n got: %v\nwant: %v", sql, got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("ExtractTableReferences(%q)\n got: %v\nwant: %v", sql, got, want)
		}
	}
}

func TestExtractTableReferences_TheOrdinaryShapes(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		want []string
	}{
		{"bare select", "SELECT * FROM orders", []string{"orders"}},
		{"schema qualified", "SELECT * FROM analytics.orders", []string{"analytics.orders"}},
		{"three parts", "SELECT * FROM warehouse.analytics.orders", []string{"warehouse.analytics.orders"}},
		{"alias without AS", "SELECT o.id FROM orders o", []string{"orders"}},
		{"alias with AS", "SELECT o.id FROM orders AS o", []string{"orders"}},
		{"comma list", "SELECT * FROM orders, customers", []string{"orders", "customers"}},
		{"comma list with aliases", "SELECT * FROM orders o, customers c", []string{"orders", "customers"}},
		{
			"join",
			"SELECT * FROM orders o JOIN customers c ON c.id = o.customer_id",
			[]string{"orders", "customers"},
		},
		{
			"every join flavour",
			`SELECT 1 FROM a
			 LEFT OUTER JOIN b ON TRUE
			 CROSS JOIN c
			 FULL JOIN d ON TRUE`,
			[]string{"a", "b", "c", "d"},
		},
		{
			"union reads both sides",
			"SELECT id FROM a UNION ALL SELECT id FROM b",
			[]string{"a", "b"},
		},
		{
			"derived table does not hide its own source",
			"SELECT * FROM (SELECT id FROM orders) t",
			[]string{"orders"},
		},
		{
			"subquery in WHERE is still a dependency",
			"SELECT * FROM a WHERE id IN (SELECT id FROM b)",
			[]string{"a", "b"},
		},
		{"lowercase keywords", "select * from orders join customers on true", []string{"orders", "customers"}},
		{"repeats collapse", "SELECT * FROM a JOIN a x ON TRUE", []string{"a"}},
		{
			"UPDATE ... FROM reads the joined side",
			"UPDATE targets SET x = s.x FROM staging s WHERE s.id = targets.id",
			[]string{"staging"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) { assertRefs(t, c.sql, c.want...) })
	}
}

func TestExtractTableReferences_QuotedIdentifiers(t *testing.T) {
	// The reason this file does not reuse stripStringLiterals: that helper blanks
	// double-quoted text, and a quoted identifier is a table name, not a string.
	cases := []struct {
		name string
		sql  string
		want []string
	}{
		{"double quotes", `SELECT * FROM "daily orders"`, []string{"daily orders"}},
		{"quoted parts", `SELECT * FROM "My Schema"."My Table"`, []string{"My Schema.My Table"}},
		{"backticks", "SELECT * FROM `analytics`.`orders`", []string{"analytics.orders"}},
		{"sql server brackets", "SELECT * FROM [dbo].[Orders]", []string{"dbo.Orders"}},
		{"mixed quoting", `SELECT * FROM analytics."Daily Orders"`, []string{"analytics.Daily Orders"}},
		{"escaped quote inside", `SELECT * FROM "say ""hi"""`, []string{`say "hi"`}},
		{
			"a dot inside quotes is part of the name, not a separator",
			`SELECT * FROM "orders.2024"`,
			[]string{"orders.2024"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) { assertRefs(t, c.sql, c.want...) })
	}
}

func TestExtractTableReferences_PreservesTheCaseTheEngineCaresAbout(t *testing.T) {
	refs := ExtractTableReferences(`SELECT * FROM "Orders"`)
	if len(refs) != 1 || refs[0].Name() != "Orders" {
		t.Fatalf("a quoted identifier must keep its case: %+v", refs)
	}
	if !refs[0].Quoted {
		t.Fatal("Quoted must be set so a caller knows case was significant here")
	}
	if unquoted := ExtractTableReferences("SELECT * FROM Orders"); unquoted[0].Quoted {
		t.Fatal("an unquoted identifier must not claim it was quoted")
	}
}

// Everything below is a false positive that a naive "word after FROM" scan produces.
// Each one would resolve to some unrelated pipeline and offer to schedule against it.

func TestExtractTableReferences_ACTEIsNeverATable(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		want []string
	}{
		{
			"the CTE name is dropped, the table it reads is kept",
			`WITH daily AS (SELECT * FROM raw_events) SELECT * FROM daily`,
			[]string{"raw_events"},
		},
		{
			"chained CTEs",
			`WITH a AS (SELECT 1 FROM src_one),
			      b AS (SELECT 1 FROM src_two)
			 SELECT * FROM a JOIN b ON TRUE`,
			[]string{"src_one", "src_two"},
		},
		{
			"a recursive CTE does not depend on itself",
			`WITH RECURSIVE tree AS (
			   SELECT id, parent FROM nodes WHERE parent IS NULL
			   UNION ALL
			   SELECT n.id, n.parent FROM nodes n JOIN tree ON n.parent = tree.id
			 ) SELECT * FROM tree`,
			[]string{"nodes"},
		},
		{
			"a column list on the CTE does not shift the parse",
			`WITH daily (d, n) AS (SELECT day, n FROM raw) SELECT * FROM daily`,
			[]string{"raw"},
		},
		{
			"MATERIALIZED between AS and the body",
			`WITH daily AS MATERIALIZED (SELECT * FROM raw) SELECT * FROM daily`,
			[]string{"raw"},
		},
		{
			"NOT MATERIALIZED too",
			`WITH daily AS NOT MATERIALIZED (SELECT * FROM raw) SELECT * FROM daily`,
			[]string{"raw"},
		},
		{
			"a CTE shadowing a real table resolves to the CTE, as SQL scoping says",
			`WITH orders AS (SELECT * FROM raw_orders) SELECT * FROM orders`,
			[]string{"raw_orders"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) { assertRefs(t, c.sql, c.want...) })
	}
}

func TestExtractTableReferences_AQualifiedNameIsNeverACTE(t *testing.T) {
	// CTE names cannot be schema-qualified, so `analytics.daily` is a real table even
	// while a CTE named `daily` is in scope. Dropping it would lose a true dependency
	// on the strength of a name collision.
	assertRefs(t,
		`WITH daily AS (SELECT * FROM raw) SELECT * FROM daily JOIN analytics.daily ON TRUE`,
		"raw", "analytics.daily")
}

func TestExtractTableReferences_FROMInsideAScalarFunctionIsNotAClause(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		want []string
	}{
		{
			"EXTRACT",
			"SELECT EXTRACT(MONTH FROM created_at) FROM orders",
			[]string{"orders"},
		},
		{
			"SUBSTRING",
			"SELECT SUBSTRING(name FROM 1 FOR 3) FROM customers",
			[]string{"customers"},
		},
		{
			"TRIM",
			"SELECT TRIM(BOTH ' ' FROM name) FROM customers",
			[]string{"customers"},
		},
		{
			"a real subquery nested inside such a call is still found",
			"SELECT EXTRACT(YEAR FROM (SELECT MAX(d) FROM calendar)) FROM orders",
			[]string{"calendar", "orders"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) { assertRefs(t, c.sql, c.want...) })
	}
}

func TestExtractTableReferences_AFunctionCallIsNotATable(t *testing.T) {
	assertRefs(t, "SELECT * FROM generate_series(1, 10)")
	assertRefs(t, "SELECT * FROM unnest(ARRAY[1,2]) AS x")
	assertRefs(t,
		"SELECT * FROM orders o JOIN LATERAL jsonb_array_elements(o.items) e ON TRUE",
		"orders")
}

func TestExtractTableReferences_StringsAndCommentsNameNothing(t *testing.T) {
	// A comment that reads as a dependency is the reason this shares the package's
	// comment stripper instead of carrying its own.
	assertRefs(t, "SELECT 'from customers' AS note FROM orders", "orders")
	assertRefs(t, "-- FROM secret_table\nSELECT * FROM orders", "orders")
	assertRefs(t, "SELECT * FROM orders /* JOIN other */", "orders")
	assertRefs(t, "SELECT * FROM orders WHERE note LIKE '%join customers%'", "orders")
}

func TestExtractTableReferences_DeleteTargetIsNotAnInput(t *testing.T) {
	// `DELETE FROM stale` names what the statement writes. Calling it a dependency
	// would say the model rebuilds when the table it empties is refreshed.
	assertRefs(t, "DELETE FROM stale WHERE d < now()")
	assertRefs(t,
		"DELETE FROM stale WHERE id IN (SELECT id FROM to_remove)",
		"to_remove")
}

func TestExtractTableReferences_MergeSourceIsAnInputAndItsTargetIsNot(t *testing.T) {
	// USING carries two unrelated meanings and the parser has to tell them apart on one
	// token of lookahead: MERGE's source table, and JOIN's column list. Both forms are
	// asserted together because a change that fixes either one usually breaks the other.
	assertRefs(t,
		"MERGE INTO tgt USING src ON tgt.id = src.id WHEN MATCHED THEN UPDATE SET x = src.x",
		"src")
	assertRefs(t,
		"MERGE INTO warehouse.dim_customer d USING staging.customer_delta s ON d.id = s.id",
		"staging.customer_delta")

	// The column list names columns, and a column is not a table. `id` here is the whole
	// reason the parenthesis check exists: without it this invents a dependency on a
	// table nobody has, which is the failure this parser is weighted against.
	assertRefs(t, "SELECT * FROM a JOIN b USING (id)", "a", "b")
	assertRefs(t, "SELECT * FROM a JOIN b USING (id, region)", "a", "b")

	// A parenthesized MERGE source is not lost by skipping the `(`: the walk continues
	// into those tokens and the subquery's own FROM names the real input.
	assertRefs(t,
		"MERGE INTO tgt USING (SELECT * FROM raw_events) s ON s.id = tgt.id",
		"raw_events")
}

func TestExtractTableReferences_DoesNotReadClauseKeywordsAsTables(t *testing.T) {
	assertRefs(t, "SELECT * FROM orders WHERE id = 1", "orders")
	assertRefs(t, "SELECT count(*) FROM orders GROUP BY region", "orders")
	assertRefs(t, "SELECT * FROM orders ORDER BY id LIMIT 10", "orders")
	assertRefs(t, "SELECT * FROM orders o JOIN c USING (id)", "orders", "c")
}

func TestExtractTableReferences_MalformedSQLStillInventsNothing(t *testing.T) {
	// Nothing guarantees the SQL reaching this parser is valid. A saved query only has
	// to pass the statement-class and single-statement checks to be stored; whether it
	// parses is discovered when it runs. So a broken FROM/JOIN arrives here, and the
	// answer has to be "no dependency", not "a table called WHERE".
	//
	// Well-formed SQL never reaches this guard — parseTableList refuses to hand a
	// clause keyword on, so only the JOIN path can. That is exactly why it is tested
	// here: a probe over 32 valid queries never touched it.
	assertRefs(t, "SELECT * FROM a JOIN WHERE b.id = 1", "a")
	assertRefs(t, "SELECT * FROM a JOIN ORDER BY x", "a")
	assertRefs(t, "SELECT * FROM a JOIN GROUP BY x", "a")
	assertRefs(t, "SELECT * FROM a JOIN", "a")
}

func TestExtractTableReferences_EmptyAndUnparseableCostNothing(t *testing.T) {
	// Returning nothing is the designed failure. It costs a suggestion; guessing
	// would cost a wrong schedule.
	for _, sql := range []string{"", "   ", "-- just a comment", "SELECT 1", "))) not sql ((("} {
		if refs := ExtractTableReferences(sql); len(refs) != 0 {
			t.Fatalf("ExtractTableReferences(%q) invented %v", sql, qualifiedNames(refs))
		}
	}
}

func TestExtractTableReferences_DoesNotHangOnDeepNesting(t *testing.T) {
	// A cheap guard against an accidental non-advancing loop: the parser walks
	// forward-only, and this fails by timing out rather than by assertion.
	sql := "SELECT * FROM base" + strings.Repeat(" JOIN t ON TRUE", 2000)
	if refs := ExtractTableReferences(sql); len(refs) != 2 {
		t.Fatalf("want base and t, got %v", qualifiedNames(refs))
	}
	deep := strings.Repeat("(", 500) + "SELECT * FROM deep" + strings.Repeat(")", 500)
	if refs := ExtractTableReferences(deep); len(refs) != 1 {
		t.Fatalf("want deep, got %v", qualifiedNames(refs))
	}
}

func TestTableRef_NameShapes(t *testing.T) {
	three := ExtractTableReferences("SELECT * FROM wh.analytics.orders")[0]
	if three.Name() != "orders" {
		t.Fatalf("Name() = %q", three.Name())
	}
	if three.SchemaQualified() != "analytics.orders" {
		t.Fatalf("SchemaQualified() = %q", three.SchemaQualified())
	}
	if three.Qualified() != "wh.analytics.orders" {
		t.Fatalf("Qualified() = %q", three.Qualified())
	}

	one := ExtractTableReferences("SELECT * FROM orders")[0]
	if one.SchemaQualified() != "orders" {
		t.Fatalf("an unqualified name has no schema to add: %q", one.SchemaQualified())
	}
}
