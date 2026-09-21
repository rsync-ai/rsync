package handlers

import (
	"context"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"testing"

	"api-gateway/internal/validators"

	"github.com/DATA-DOG/go-sqlmock"
)

// The endpoint answers with every table a query reads, so it must hide another member's
// private query exactly as GET does — 404, not 403, and nothing from its SQL. Workspace
// membership, which is all requireResourceRole proves, is not enough.
func TestSuggestSavedQueryUpstreams_HidesAnotherMembersPrivateQuery(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()

	mock.ExpectQuery(`FROM saved_queries r\s+JOIN workspace_members`).
		WithArgs(savedQueryID, wsScopeUser, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("admin"))
	mock.ExpectQuery(`FROM saved_queries sq[\s\S]+WHERE sq\.id = \$1`).
		WithArgs(savedQueryID).
		WillReturnRows(savedQueryRows(savedQueryOther, "private",
			"SELECT * FROM analytics.secret_margins", "read"))

	r := savedQueryRouter(http.MethodGet, "/explorer/saved/:id/upstreams", "admin", SuggestSavedQueryUpstreams)
	w := doJSON(r, http.MethodGet, "/explorer/saved/"+savedQueryID+"/upstreams", nil)

	if w.Code != http.StatusNotFound {
		t.Fatalf("another member's private query must 404, got %d: %s", w.Code, w.Body.String())
	}
	if strings.Contains(w.Body.String(), "secret_margins") {
		t.Fatalf("the 404 leaked a table the private query reads: %s", w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("%v", err)
	}
}

// The controls for the test above: the same row answers 200 to its author, and a
// workspace-visible query answers 200 to anyone in the workspace. Without them a 404
// from a mis-set mock would pass for the visibility rule.
func TestSuggestSavedQueryUpstreams_AnswersTheAuthorAndSharedQueries(t *testing.T) {
	for _, tc := range []struct {
		name, createdBy, visibility string
	}{
		{"the author's own private query", wsScopeUser, "private"},
		{"another member's shared query", savedQueryOther, "workspace"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock, cleanup := wsScopeMockDB(t)
			defer cleanup()

			mock.ExpectQuery(`FROM saved_queries r\s+JOIN workspace_members`).
				WithArgs(savedQueryID, wsScopeUser, wsScopeWS).
				WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("viewer"))
			// SELECT 1 reads no table, so the resolver answers without a producer lookup
			// and the only queries are the two this test is about.
			mock.ExpectQuery(`FROM saved_queries sq[\s\S]+WHERE sq\.id = \$1`).
				WithArgs(savedQueryID).
				WillReturnRows(savedQueryRows(tc.createdBy, tc.visibility, "SELECT 1", "read"))

			r := savedQueryRouter(http.MethodGet, "/explorer/saved/:id/upstreams", "viewer", SuggestSavedQueryUpstreams)
			w := doJSON(r, http.MethodGet, "/explorer/saved/"+savedQueryID+"/upstreams", nil)

			if w.Code != http.StatusOK {
				t.Fatalf("want 200, got %d: %s", w.Code, w.Body.String())
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("%v", err)
			}
		})
	}
}

// buildUpstreamSuggestion is database-free for the same reason buildAssetGraph is: the
// Postgres tests behind `//go:build integration_pg` never run in CI, so every rule about
// WHICH producer is offered has to be provable here, in the default suite.

// modelBuilds is a materialized model as the loader hands it over.
func modelBuilds(t *testing.T, id, name, target string) tableProducer {
	t.Helper()
	table, ok := modelProducedTable(id, name, "table", target)
	if !ok {
		t.Fatalf("model %s with target %q builds nothing", id, target)
	}
	return tableProducer{ConnectionID: connA, Kind: assetKindModel, Table: table}
}

func suggest(sqlText string, selfID string, producers ...tableProducer) upstreamSuggestionResponse {
	return buildUpstreamSuggestion(validators.ExtractTableReferences(sqlText), producers, selfID)
}

// offered renders candidates as "kind:name<-reference", sorted.
func offered(resp upstreamSuggestionResponse) []string {
	out := make([]string, 0, len(resp.Candidates))
	for _, c := range resp.Candidates {
		out = append(out, c.Kind+":"+c.Name+"<-"+c.MatchedReference)
	}
	sort.Strings(out)
	return out
}

// A model that builds the table this query reads is offered the way a pipeline is, and
// with everything the picker needs to add it without a lookup.
func TestUpstreamSuggestion_OffersTheModelThatBuildsTheTable(t *testing.T) {
	resp := suggest("SELECT * FROM analytics.customer_dim", "m-self",
		modelBuilds(t, "m-dim", "Customer dim", "analytics.customer_dim"))

	want := []upstreamCandidate{{
		Kind:             assetKindModel,
		ID:               "m-dim",
		Name:             "Customer dim",
		Table:            "analytics.customer_dim",
		MatchedReference: "analytics.customer_dim",
		Qualified:        true,
	}}
	if !reflect.DeepEqual(resp.Candidates, want) {
		t.Fatalf("candidates:\n got %+v\nwant %+v", resp.Candidates, want)
	}
	if len(resp.Unresolved) != 0 || resp.Ambiguous {
		t.Fatalf("one reference, one producer: unresolved=%v ambiguous=%v", resp.Unresolved, resp.Ambiguous)
	}
}

// A model that reads the table it builds is an incremental pattern. It must not be
// offered as its own upstream — but another model building that same table still is.
func TestUpstreamSuggestion_AModelIsNotItsOwnUpstream(t *testing.T) {
	alone := suggest("SELECT * FROM analytics.orders_daily", "m-self",
		modelBuilds(t, "m-self", "Orders daily", "analytics.orders_daily"))
	if len(alone.Candidates) != 0 {
		t.Fatalf("a model was offered as its own upstream: %v", offered(alone))
	}
	if !reflect.DeepEqual(alone.Unresolved, []string{"analytics.orders_daily"}) {
		t.Fatalf("with only itself as producer the reference is unresolved, got %v", alone.Unresolved)
	}

	shared := suggest("SELECT * FROM analytics.orders_daily", "m-self",
		modelBuilds(t, "m-self", "Orders daily", "analytics.orders_daily"),
		modelBuilds(t, "m-other", "Orders daily (v2)", "analytics.orders_daily"))
	if got := offered(shared); !reflect.DeepEqual(got, []string{"model:Orders daily (v2)<-analytics.orders_daily"}) {
		t.Fatalf("want only the other model offered, got %v", got)
	}
}

// Self-exclusion has to happen BEFORE ambiguity is counted. Here the model's own bare
// target and a pipeline's analytics.orders are two distinct tables a bare `orders`
// could mean; with the model removed, only one remains, and there is nothing to be
// uncertain about.
func TestUpstreamSuggestion_ItsOwnTargetDoesNotMakeAReferenceAmbiguous(t *testing.T) {
	resp := suggest("SELECT * FROM orders", "m-self",
		modelBuilds(t, "m-self", "Orders rollup", "orders"),
		pipeWrites("p1", "Orders CDC", connA, "analytics.orders", "orders"))

	if got := offered(resp); !reflect.DeepEqual(got, []string{"pipeline:Orders CDC<-orders"}) {
		t.Fatalf("want only the pipeline offered, got %v", got)
	}
	if resp.Ambiguous {
		t.Fatal("the model's own target made the reference ambiguous")
	}
}

// A model whose target names no schema cannot be placed in one. A reference that DOES
// name a schema is information, and matching it to a bare target would be a guess about
// the model's search_path — the same conservative identity the asset graph uses.
func TestUpstreamSuggestion_ABareModelTargetIsNotGuessedIntoASchema(t *testing.T) {
	bare := modelBuilds(t, "m-bare", "Orders rollup", "orders")

	qualified := suggest("SELECT * FROM analytics.orders", "m-self", bare)
	if len(qualified.Candidates) != 0 {
		t.Fatalf("a bare target matched a schema-qualified reference: %v", offered(qualified))
	}
	if !reflect.DeepEqual(qualified.Unresolved, []string{"analytics.orders"}) {
		t.Fatalf("want analytics.orders unresolved, got %v", qualified.Unresolved)
	}

	unqualified := suggest("SELECT * FROM orders", "m-self", bare)
	if len(unqualified.Candidates) != 1 {
		t.Fatalf("a bare reference should match a bare target, got %v", offered(unqualified))
	}
	if c := unqualified.Candidates[0]; c.Qualified || c.Table != "orders" {
		t.Fatalf("want a weak match reported on the bare table, got %+v", c)
	}
}

// The self-host demo's chain, verbatim: "Demo 2" reads the table "Demo 1" builds, with
// no schema on either side, through an aggregate whose ORDER BY inside string_agg and
// GROUP BY 1 are the parts a table extractor most easily trips on. The v0.1.2 gateway
// looked only at pipelines and answered this with the reference unresolved and no
// candidates, which is how the dependency went unnoticed. The model is offered whether
// its target was saved bare or schema-qualified, because the query names no schema.
func TestUpstreamSuggestion_TheDemoModelChainResolves(t *testing.T) {
	const demo2SQL = `SELECT
  CASE WHEN pct_of_revenue >= 25 THEN 'Core (>=25%)'
       WHEN pct_of_revenue >= 10 THEN 'Growth (10-25%)'
       ELSE 'Long tail (<10%)' END AS revenue_tier,
  count(*) AS categories,
  string_agg(category, ', ' ORDER BY revenue DESC) AS category_list,
  round(sum(revenue), 2) AS tier_revenue,
  round(sum(pct_of_revenue), 1) AS pct_of_total,
  round(avg(avg_line_value), 2) AS avg_line_value,
  now() AS built_at
FROM demo_revenue_by_category
GROUP BY 1
ORDER BY tier_revenue DESC`

	for _, target := range []string{"demo_revenue_by_category", "public.demo_revenue_by_category"} {
		t.Run(target, func(t *testing.T) {
			resp := suggest(demo2SQL, "m-demo2",
				modelBuilds(t, "m-demo1", "Demo 1 · Revenue by category", target),
				modelBuilds(t, "m-demo2", "Demo 2 · Revenue tiers", "demo_revenue_tiers"))

			if !reflect.DeepEqual(resp.References, []string{"demo_revenue_by_category"}) {
				t.Fatalf("references: got %v, want only demo_revenue_by_category", resp.References)
			}
			want := []string{"model:Demo 1 · Revenue by category<-demo_revenue_by_category"}
			if got := offered(resp); !reflect.DeepEqual(got, want) {
				t.Fatalf("candidates: got %v, want %v", got, want)
			}
			if len(resp.Unresolved) != 0 || resp.Ambiguous {
				t.Fatalf("unresolved=%v ambiguous=%v, want none", resp.Unresolved, resp.Ambiguous)
			}
		})
	}
}

// Two producers of ONE table is fan-in: both write it and a schedule can follow both.
// That is not the uncertainty the ambiguous flag exists to report.
func TestUpstreamSuggestion_TwoProducersOfOneTableIsNotAmbiguity(t *testing.T) {
	resp := suggest("SELECT * FROM analytics.orders", "m-self",
		pipeWrites("p1", "Orders CDC", connA, "analytics.orders", "orders"),
		modelBuilds(t, "m-fix", "Orders corrections", "analytics.orders"))

	want := []string{"model:Orders corrections<-analytics.orders", "pipeline:Orders CDC<-analytics.orders"}
	if got := offered(resp); !reflect.DeepEqual(got, want) {
		t.Fatalf("want both producers offered with their kinds, got %v", got)
	}
	if resp.Ambiguous {
		t.Fatal("fan-in into one table was reported as ambiguous")
	}
}

// A bare name that two DIFFERENT tables answer to is the real ambiguity. The qualified
// spelling of the same query, against the same producers, is the control: it names the
// table, so there is no question left.
func TestUpstreamSuggestion_OneNameTwoTablesIsAmbiguous(t *testing.T) {
	producers := []tableProducer{
		pipeWrites("p1", "Orders CDC", connA, "analytics.orders", "orders"),
		modelBuilds(t, "m-stage", "Staged orders", "staging.orders"),
	}

	bare := suggest("SELECT * FROM orders", "m-self", producers...)
	if !bare.Ambiguous {
		t.Fatalf("`orders` could be analytics.orders or staging.orders, got %v", offered(bare))
	}
	if len(bare.Candidates) != 2 {
		t.Fatalf("both readings should be offered for a person to choose, got %v", offered(bare))
	}

	named := suggest("SELECT * FROM staging.orders", "m-self", producers...)
	if named.Ambiguous {
		t.Fatal("a schema-qualified reference was reported as ambiguous")
	}
	if got := offered(named); !reflect.DeepEqual(got, []string{"model:Staged orders<-staging.orders"}) {
		t.Fatalf("want only the staging producer, got %v", got)
	}
}

// Nothing to resolve still answers in JSON-safe empty slices.
func TestUpstreamSuggestion_NothingToResolveIsEmptyNotNull(t *testing.T) {
	resp := buildUpstreamSuggestion(nil, nil, "m-self")
	if resp.References == nil || resp.Unresolved == nil || resp.Candidates == nil {
		t.Fatalf("empty results must be empty slices, not nil: %+v", resp)
	}
}

// modelProducedTable is the table-model half of modelProducedTables, and answers for
// nothing else: a caller holding only a target_table cannot know what a statement writes.
func TestModelProducedTable(t *testing.T) {
	cases := []struct {
		name            string
		materialization string
		target          string
		want            producedTable
		ok              bool
	}{
		{"qualified table", "table", "analytics.orders", producedTable{ProducerID: "m1", ProducerName: "Orders", DestQualified: "analytics.orders", TableName: "orders"}, true},
		{"bare table", "table", "orders", producedTable{ProducerID: "m1", ProducerName: "Orders", TableName: "orders"}, true},
		{"blank target", "table", "   ", producedTable{}, false},
		{"a statement's target_table is not what it writes", "statement", "analytics.orders", producedTable{}, false},
		{"none ignores a leftover target", "none", "analytics.orders", producedTable{}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := modelProducedTable("m1", "Orders", tc.materialization, tc.target)
			if ok != tc.ok || got != tc.want {
				t.Fatalf("got (%+v, %v), want (%+v, %v)", got, ok, tc.want, tc.ok)
			}
		})
	}
}

// Every table a model is a producer of. A statement model used to produce nothing
// because nothing stored which table it writes; its SQL says, and is now read.
func TestModelProducedTables(t *testing.T) {
	built := func(dest, table string) producedTable {
		return producedTable{ProducerID: "m1", ProducerName: "Orders", DestQualified: dest, TableName: table}
	}
	cases := []struct {
		name            string
		materialization string
		target          string
		sqlText         string
		want            []producedTable
	}{
		{"a table model builds its target", matTable, "analytics.orders", "SELECT * FROM raw.orders",
			[]producedTable{built("analytics.orders", "orders")}},
		{"a table model's SQL is the SELECT it wraps, not a write", matTable, "analytics.orders",
			"INSERT INTO analytics.elsewhere SELECT 1",
			[]producedTable{built("analytics.orders", "orders")}},
		{"a table model with a blank target builds nothing", matTable, "  ", "SELECT 1", nil},

		{"a statement model produces the table its SQL writes", matStatement, "",
			"INSERT INTO analytics.orders SELECT * FROM raw.orders",
			[]producedTable{built("analytics.orders", "orders")}},
		{"a statement model's leftover target_table is ignored", matStatement, "analytics.leftover",
			"MERGE INTO analytics.orders o USING staging.orders s ON o.id = s.id WHEN MATCHED THEN DELETE",
			[]producedTable{built("analytics.orders", "orders")}},
		{"every table a statement model writes", matStatement, "",
			"WITH added AS (INSERT INTO analytics.orders SELECT 1 RETURNING customer_id) " +
				"UPDATE analytics.customers SET x = 1 WHERE id IN (SELECT customer_id FROM added)",
			[]producedTable{built("analytics.orders", "orders"), built("analytics.customers", "customers")}},
		{"a bare name stays bare, as a bare target does", matStatement, "",
			"UPDATE orders SET status = 'done'",
			[]producedTable{built("", "orders")}},
		{"a database prefix is dropped, as it is from a target", matStatement, "",
			"INSERT INTO warehouse.analytics.orders SELECT 1",
			[]producedTable{built("analytics.orders", "orders")}},
		{"one table written under two spellings is one table", matStatement, "",
			"WITH fixed AS (UPDATE warehouse.analytics.orders SET x = 1 RETURNING id) " +
				"INSERT INTO Analytics.Orders SELECT id FROM fixed",
			[]producedTable{built("analytics.orders", "orders")}},

		// The runner refuses SQL holding more than one statement, so a model built on it
		// never writes a row. The extractor alone would name a table in every "produces
		// nothing" case here, and the one-statement cases beside them write the same
		// tables, so it is the separator, not the verb, that empties them.
		{"several statements produce nothing: the runner refuses them", matStatement, "",
			"INSERT INTO analytics.orders SELECT 1; UPDATE analytics.customers SET x = 1", nil},
		{"a trailing semicolon is still one statement", matStatement, "",
			"INSERT INTO analytics.orders SELECT 1;",
			[]producedTable{built("analytics.orders", "orders")}},
		{"a semicolon in a trailing comment is still one statement", matStatement, "",
			"INSERT INTO analytics.orders SELECT 1; -- nightly; after the export",
			[]producedTable{built("analytics.orders", "orders")}},
		{"a semicolon inside a string is still one statement", matStatement, "",
			"INSERT INTO analytics.orders SELECT 'a;b'",
			[]producedTable{built("analytics.orders", "orders")}},
		// With backslash escapes the string ends after `it\'`, the way the run check reads
		// it, so this is two statements and the runner refuses it. Read on its own, the
		// extractor would also name analytics.fake.
		{"a backslash-escaped quote that splits the SQL produces nothing", matStatement, "",
			`INSERT INTO analytics.orders SELECT 'it\'s; INSERT INTO analytics.fake SELECT 1'`, nil},
		{"a procedure body with several statements produces nothing", matStatement, "",
			"CREATE PROCEDURE p AS BEGIN INSERT INTO analytics.orders SELECT 1; INSERT INTO analytics.customers SELECT 1; END",
			nil},
		{"a quoted name keeps its case", matStatement, "",
			`INSERT INTO "Analytics"."Orders" SELECT 1`,
			[]producedTable{built("Analytics.Orders", "Orders")}},
		{"a statement model that only reads produces nothing", matStatement, "analytics.orders",
			"SELECT * FROM analytics.orders", nil},
		{"a write inside a string is not a write", matStatement, "",
			"SELECT 'INSERT INTO analytics.orders SELECT 1'", nil},
		{"a statement model with no SQL produces nothing", matStatement, "analytics.orders", "", nil},
		{"a none query produces nothing, whatever its SQL", matNone, "analytics.orders",
			"INSERT INTO analytics.orders SELECT 1", nil},
		{"an unrecognized materialization produces nothing", "view", "analytics.orders",
			"INSERT INTO analytics.orders SELECT 1", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := modelProducedTables("m1", "Orders", tc.materialization, tc.target, tc.sqlText)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %+v\nwant %+v", got, tc.want)
			}
		})
	}
}

// statementWrites is a statement model as a loader using modelProducedTables hands it
// over: one producer per table its SQL writes.
func statementWrites(t *testing.T, id, name, sqlText string) []tableProducer {
	t.Helper()
	tables := modelProducedTables(id, name, matStatement, "", sqlText)
	if len(tables) == 0 {
		t.Fatalf("statement model %s writes nothing: %q", id, sqlText)
	}
	out := make([]tableProducer, 0, len(tables))
	for _, table := range tables {
		out = append(out, tableProducer{ConnectionID: connA, Kind: assetKindModel, Table: table})
	}
	return out
}

// The case the issue was about: a model that reads the table a statement model loads is
// offered that statement model, exactly as it would be offered a table model.
func TestUpstreamSuggestion_OffersTheStatementModelThatWritesTheTable(t *testing.T) {
	loader := statementWrites(t, "m-load", "Load daily orders", `
		WITH recent AS (SELECT * FROM raw.orders WHERE ts > now() - interval '1 day')
		INSERT INTO analytics.orders_daily SELECT * FROM recent`)

	resp := suggest("SELECT sum(total) FROM analytics.orders_daily", "m-self", loader...)
	want := []upstreamCandidate{{
		Kind:             assetKindModel,
		ID:               "m-load",
		Name:             "Load daily orders",
		Table:            "analytics.orders_daily",
		MatchedReference: "analytics.orders_daily",
		Qualified:        true,
	}}
	if !reflect.DeepEqual(resp.Candidates, want) {
		t.Fatalf("candidates:\n got %+v\nwant %+v", resp.Candidates, want)
	}
	if len(resp.Unresolved) != 0 {
		t.Fatalf("the loaded table has a producer; unresolved = %v", resp.Unresolved)
	}

	// The loader READS raw.orders; reading a table does not make it that table's
	// producer, so a model reading raw.orders is offered nothing.
	raw := suggest("SELECT * FROM raw.orders", "m-self", loader...)
	if len(raw.Candidates) != 0 || !reflect.DeepEqual(raw.Unresolved, []string{"raw.orders"}) {
		t.Fatalf("a table the statement only reads was resolved to it: %v unresolved=%v", offered(raw), raw.Unresolved)
	}
}

// An incremental statement model — INSERT INTO t ... WHERE ts > (SELECT max(ts) FROM t) —
// reads the table it writes. It is not its own upstream, but a pipeline that also lands
// that table still is.
func TestUpstreamSuggestion_AStatementModelIsNotItsOwnUpstream(t *testing.T) {
	const incremental = `INSERT INTO analytics.snapshot SELECT * FROM raw.events
		WHERE ts > (SELECT max(ts) FROM analytics.snapshot)`
	self := statementWrites(t, "m-self", "Snapshot", incremental)

	alone := suggest(incremental, "m-self", self...)
	if len(alone.Candidates) != 0 {
		t.Fatalf("a statement model was offered as its own upstream: %v", offered(alone))
	}
	if !reflect.DeepEqual(alone.Unresolved, []string{"raw.events", "analytics.snapshot"}) {
		t.Fatalf("with only itself as producer both inputs are unresolved, got %v", alone.Unresolved)
	}

	withPipeline := suggest(incremental, "m-self",
		append(self, pipeWrites("p1", "Snapshot backfill", connA, "analytics.snapshot", "snapshot"))...)
	if got := offered(withPipeline); !reflect.DeepEqual(got, []string{"pipeline:Snapshot backfill<-analytics.snapshot"}) {
		t.Fatalf("want only the pipeline offered, got %v", got)
	}
}

// One statement model loading two different tables that share a bare name is still two
// tables, so an unqualified reference to that name is ambiguous — and the model is
// offered once, not once per table.
func TestUpstreamSuggestion_OneStatementWritingTwoSameNamedTablesIsAmbiguous(t *testing.T) {
	loader := statementWrites(t, "m-load", "Load orders",
		"WITH staged AS (INSERT INTO staging.orders SELECT * FROM raw.orders RETURNING *) "+
			"INSERT INTO analytics.orders SELECT * FROM staged")
	if len(loader) != 2 {
		t.Fatalf("the statement writes two tables, got %d producers", len(loader))
	}

	bare := suggest("SELECT * FROM orders", "m-self", loader...)
	if !bare.Ambiguous {
		t.Fatalf("`orders` could be analytics.orders or staging.orders: %v", offered(bare))
	}
	if got := offered(bare); !reflect.DeepEqual(got, []string{"model:Load orders<-orders"}) {
		t.Fatalf("want the model offered once, got %v", got)
	}

	named := suggest("SELECT * FROM staging.orders", "m-self", loader...)
	if named.Ambiguous || len(named.Candidates) != 1 || named.Candidates[0].Table != "staging.orders" {
		t.Fatalf("a qualified reference names one table: ambiguous=%v %+v", named.Ambiguous, named.Candidates)
	}
}

// The loader is the half the builder tests above cannot see. Every statement-model rule
// was proven against buildUpstreamSuggestion while resolveUpstreams still selected only
// materialization='table' and never read sql_text, so no statement model ever reached
// the builder; the Postgres tests that would have caught it run only under
// integration_pg, which CI does not pass. This drives the loader in the default suite.
//
// The model rows come back only for a query that asks for statement models, and they
// carry sql_text as a fifth column: a loader filtering to 'table' misses the
// expectation, and one that scans four columns fails on the row.
func TestResolveUpstreams_HandsStatementModelsToTheBuilder(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	in := upstreamLookup{
		SavedQueryID: "m-self",
		SQLText:      "SELECT o.total, d.day FROM analytics.orders_daily o JOIN analytics.day_dim d USING (day)",
		ConnectionID: connA,
		WorkspaceID:  "ws-1",
		UserID:       "u-1",
	}

	mock.ExpectQuery(`FROM pipeline_run_table_stats s`).
		WithArgs(in.WorkspaceID, in.ConnectionID, sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "dest", "table"}))
	mock.ExpectQuery(`sq\.sql_text[\s\S]+FROM saved_queries sq[\s\S]+sq\.materialization IN \('table', 'statement'\)`).
		WithArgs(in.WorkspaceID, in.ConnectionID, in.UserID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "materialization", "target_table", "sql_text"}).
			AddRow("m-load", "Load daily orders", matStatement, "",
				"INSERT INTO analytics.orders_daily SELECT * FROM raw.orders").
			AddRow("m-dim", "Day dim", matTable, "analytics.day_dim", "SELECT 1"))

	resp, err := resolveUpstreams(context.Background(), mockDB, in)
	if err != nil {
		t.Fatalf("resolveUpstreams: %v", err)
	}
	want := []string{
		"model:Day dim<-analytics.day_dim",
		"model:Load daily orders<-analytics.orders_daily",
	}
	if got := offered(resp); !reflect.DeepEqual(got, want) {
		t.Fatalf("candidates: got %v, want %v", got, want)
	}
	if len(resp.Unresolved) != 0 {
		t.Fatalf("both inputs have a producer; unresolved = %v", resp.Unresolved)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("%v", err)
	}
}
