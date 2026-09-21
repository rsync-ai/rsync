//go:build integration_pg

// Real-Postgres coverage for upstream inference — "which pipeline or model produces the
// tables this model reads".
//
// It has to run against a real Postgres because the half of the answer that is easiest
// to get wrong lives in the SQL, not in Go: which COLUMN is compared (the whole point of
// migration 089), which rows the tenancy, connection and visibility predicates exclude,
// and how a NULL destination behaves. A mock returning canned rows would pass with every
// one of those inverted.
//
// The fixture is the shape migration 089 was written about: a MySQL->Postgres CDC
// pipeline whose source schema and destination schema differ. That difference is what
// makes the wrong column look right in every test where they happen to match.
//
// Needs only a bare postgres — the queries touch three tables, so the test creates them
// itself rather than running the migration stack:
//
//	docker run -d --name upstream-pg -e POSTGRES_PASSWORD=verify \
//	    -e POSTGRES_DB=cplane -p 55443:5432 postgres:16
//	UPSTREAM_PG_DSN='postgres://postgres:verify@localhost:55443/cplane?sslmode=disable' \
//	    go test -tags integration_pg ./internal/handlers/ -run PG_Upstream -v
//
// The tests DROP and recreate `pipelines`, `pipeline_run_table_stats` and
// `saved_queries`, so point this at a scratch database — never at one holding a
// migrated schema you care about.

package handlers

import (
	"context"
	"database/sql"
	"os"
	"sort"
	"strings"
	"testing"

	_ "github.com/rsync-ai/shared/pgdriver"
)

func upstreamPGDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("UPSTREAM_PG_DSN")
	if dsn == "" {
		t.Skip("UPSTREAM_PG_DSN not set — see the file header for the command that provides one")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

const (
	upWS       = "aaaaaaaa-1111-0000-0000-000000000001"
	upOtherWS  = "aaaaaaaa-1111-0000-0000-000000000002"
	upWarehse  = "bbbbbbbb-1111-0000-0000-000000000001" // the connection the model reads
	upOtherCn  = "bbbbbbbb-1111-0000-0000-000000000002" // a different destination
	upOrders   = "cccccccc-1111-0000-0000-000000000001" // lands analytics.orders
	upCustomer = "cccccccc-1111-0000-0000-000000000002" // lands analytics.customers
	upForeign  = "cccccccc-1111-0000-0000-000000000003" // lands analytics.orders elsewhere
	upNoDest   = "cccccccc-1111-0000-0000-000000000004" // object-storage: NULL destination

	upMe   = "dddddddd-1111-0000-0000-000000000001" // the user asking
	upThem = "dddddddd-1111-0000-0000-000000000002" // another member of the workspace

	upSelf       = "eeeeeeee-1111-0000-0000-000000000001" // the model being scheduled; builds analytics.daily_mrr
	upDim        = "eeeeeeee-1111-0000-0000-000000000002" // builds analytics.customer_dim
	upDimElse    = "eeeeeeee-1111-0000-0000-000000000003" // builds analytics.customer_dim on another connection
	upDimOtherWS = "eeeeeeee-1111-0000-0000-000000000004" // builds analytics.customer_dim in another workspace
	upMine       = "eeeeeeee-1111-0000-0000-000000000005" // private to me; builds analytics.my_scratch
	upTheirs     = "eeeeeeee-1111-0000-0000-000000000006" // private to them; builds analytics.their_scratch
	upStatement  = "eeeeeeee-1111-0000-0000-000000000007" // a statement model with a leftover target
)

// upstreamFixture builds the three tables and the rows every test below shares.
//
// The CDC detail that matters: every stat row's SOURCE-side qualified_name is
// `shop.<table>` (the MySQL database) while the DESTINATION is `analytics.<table>`.
// A resolver matching qualified_name finds nothing for `analytics.orders` and
// everything for `shop.orders` — which is the bug this fixture exists to catch.
//
// Every model the resolver must NOT offer builds a table that some other test's query
// reads, next to one it must offer, so a predicate dropped from the model query shows
// up as an extra candidate rather than as a quiet pass.
func upstreamFixture(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx := context.Background()

	const drop = `DROP TABLE IF EXISTS pipeline_run_table_stats, pipelines, saved_queries`
	_, _ = db.ExecContext(ctx, drop)
	t.Cleanup(func() { _, _ = db.ExecContext(ctx, drop) })

	mustExec(t, db, `CREATE TABLE pipelines (
		id UUID PRIMARY KEY,
		name TEXT,
		workspace_id UUID,
		destination_connection_id UUID
	)`)
	mustExec(t, db, `CREATE TABLE pipeline_run_table_stats (
		pipeline_id UUID,
		table_name TEXT NOT NULL,
		schema_name TEXT,
		qualified_name TEXT NOT NULL,
		destination_schema TEXT,
		destination_qualified_name TEXT
	)`)
	// The columns and constraints the model query depends on, as migrations 084, 085
	// and 088 define them — including the CHECK that lets a statement model keep a
	// target_table, which is exactly the row the resolver has to ignore.
	mustExec(t, db, `CREATE TABLE saved_queries (
		id UUID PRIMARY KEY,
		workspace_id UUID NOT NULL,
		connection_id UUID NOT NULL,
		name TEXT NOT NULL,
		sql_text TEXT NOT NULL DEFAULT '',
		visibility TEXT NOT NULL CHECK (visibility IN ('private','workspace')),
		created_by UUID NOT NULL,
		materialization TEXT NOT NULL DEFAULT 'none'
			CHECK (materialization IN ('none','table','statement')),
		target_table TEXT,
		CHECK (materialization <> 'table' OR NULLIF(TRIM(target_table), '') IS NOT NULL)
	)`)

	mustExec(t, db, `INSERT INTO pipelines VALUES
		($1,'Orders CDC',      $5, $6),
		($2,'Customers CDC',   $5, $6),
		($3,'Orders (other)',  $5, $7),
		($4,'Bronze dump',     $5, $6)`,
		upOrders, upCustomer, upForeign, upNoDest, upWS, upWarehse, upOtherCn)

	mustExec(t, db, `INSERT INTO pipeline_run_table_stats
		(pipeline_id, table_name, schema_name, qualified_name, destination_schema, destination_qualified_name)
		VALUES
		($1,'orders',   'shop','shop.orders',      'analytics','analytics.orders'),
		($2,'customers','shop','shop.customers',   'analytics','analytics.customers'),
		($3,'orders',   'shop','shop.orders',      'analytics','analytics.orders'),
		($4,'events',   'shop','shop.events',      NULL,        NULL)`,
		upOrders, upCustomer, upForeign, upNoDest)

	mustExec(t, db, `INSERT INTO saved_queries
		(id, workspace_id, connection_id, name, visibility, created_by, materialization, target_table)
		VALUES
		($1, $8, $10, 'Daily MRR',            'workspace', $12, 'table',     'analytics.daily_mrr'),
		($2, $8, $10, 'Customer dim',         'workspace', $13, 'table',     'analytics.customer_dim'),
		($3, $8, $11, 'Customer dim (other)', 'workspace', $12, 'table',     'analytics.customer_dim'),
		($4, $9, $10, 'Customer dim (ws2)',   'workspace', $12, 'table',     'analytics.customer_dim'),
		($5, $8, $10, 'My scratch',           'private',   $12, 'table',     'analytics.my_scratch'),
		($6, $8, $10, 'Their scratch',        'private',   $13, 'table',     'analytics.their_scratch'),
		($7, $8, $10, 'Nightly delete',       'workspace', $12, 'statement', 'analytics.stmt_out')`,
		upSelf, upDim, upDimElse, upDimOtherWS, upMine, upTheirs, upStatement,
		upWS, upOtherWS, upWarehse, upOtherCn, upMe, upThem)
}

func mustExec(t *testing.T, db *sql.DB, q string, args ...any) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(), q, args...); err != nil {
		t.Fatalf("exec %.60s: %v", q, err)
	}
}

// names renders candidates as "producer<-reference" pairs, sorted, so an assertion
// reads as the mapping it is testing rather than as struct literals.
func names(resp upstreamSuggestionResponse) []string {
	out := make([]string, 0, len(resp.Candidates))
	for _, c := range resp.Candidates {
		out = append(out, c.Name+"<-"+c.MatchedReference)
	}
	sort.Strings(out)
	return out
}

// lookup is the model being scheduled, asked about by upMe, in the fixture's workspace.
func lookup(sqlText string) upstreamLookup {
	return upstreamLookup{
		SavedQueryID: upSelf,
		SQLText:      sqlText,
		ConnectionID: upWarehse,
		WorkspaceID:  upWS,
		UserID:       upMe,
	}
}

func resolveAs(t *testing.T, db *sql.DB, in upstreamLookup) upstreamSuggestionResponse {
	t.Helper()
	resp, err := resolveUpstreams(context.Background(), db, in)
	if err != nil {
		t.Fatalf("resolveUpstreams: %v", err)
	}
	return resp
}

func resolve(t *testing.T, db *sql.DB, sqlText string) upstreamSuggestionResponse {
	t.Helper()
	return resolveAs(t, db, lookup(sqlText))
}

func TestPG_UpstreamMatchesDestinationNotSource(t *testing.T) {
	db := upstreamPGDB(t)
	upstreamFixture(t, db)

	// The model runs against the warehouse, so it names analytics.orders. The pipeline
	// that produced it is Orders CDC — whose SOURCE name for the same table is
	// shop.orders. Matching qualified_name would return nothing here and would return
	// a match for the query below; both are asserted so the columns cannot be swapped
	// back without a failure.
	resp := resolve(t, db, "SELECT * FROM analytics.orders")
	if got := names(resp); len(got) != 1 || got[0] != "Orders CDC<-analytics.orders" {
		t.Fatalf("destination-side name did not resolve: %v", got)
	}

	// The source-side name is NOT a table in this warehouse. Answering it would mean
	// the resolver is reading the wrong half of the pipeline.
	resp = resolve(t, db, "SELECT * FROM shop.orders")
	if len(resp.Candidates) != 0 {
		t.Fatalf("source-side name resolved, so the wrong column is being matched: %v", names(resp))
	}
	if len(resp.Unresolved) != 1 || resp.Unresolved[0] != "shop.orders" {
		t.Fatalf("expected shop.orders reported unresolved, got %v", resp.Unresolved)
	}
}

func TestPG_UpstreamQualifiedMissStaysAMissBesideAMatchingSibling(t *testing.T) {
	db := upstreamPGDB(t)
	upstreamFixture(t, db)

	// Two references, same table name, different schemas — one produced, one not. This
	// is the case that separates the two guards keeping a qualified reference off the
	// bare-name path: the query prefilter and the pairing rule. Alone, either is enough
	// for a query whose every reference misses, because a missing name fetches no rows
	// at all. Here the sibling `analytics.orders` DOES fetch the orders row, so the
	// candidate set contains a row named `orders` while `staging.orders` is being
	// resolved — and only the pairing rule can refuse it.
	resp := resolve(t, db, `
		SELECT o.total, s.total
		FROM analytics.orders o
		JOIN staging.orders s ON s.id = o.id`)

	if got := names(resp); len(got) != 1 || got[0] != "Orders CDC<-analytics.orders" {
		t.Fatalf("want only the produced reference resolved, got %v", got)
	}
	if len(resp.Unresolved) != 1 || resp.Unresolved[0] != "staging.orders" {
		t.Fatalf("staging.orders is a different table and must stay unresolved, got %v", resp.Unresolved)
	}
}

func TestPG_UpstreamStaysInsideWorkspaceAndConnection(t *testing.T) {
	db := upstreamPGDB(t)
	upstreamFixture(t, db)

	// "Orders (other)" writes an identically named table into a DIFFERENT destination
	// connection. Same name, different warehouse, different table — suggesting it would
	// hang the schedule off a pipeline that never touches what this model reads.
	resp := resolve(t, db, "SELECT * FROM analytics.orders")
	for _, c := range resp.Candidates {
		if c.ID == upForeign {
			t.Fatal("a pipeline writing to another destination connection was suggested")
		}
	}
	if resp.Ambiguous {
		t.Fatal("marked ambiguous by a candidate that should have been excluded")
	}

	// Same query, asked on behalf of a workspace that owns none of these pipelines.
	other := lookup("SELECT * FROM analytics.orders")
	other.WorkspaceID = upOtherWS
	if resp2 := resolveAs(t, db, other); len(resp2.Candidates) != 0 {
		t.Fatalf("cross-workspace leak: %v", names(resp2))
	}
}

func TestPG_UpstreamCannotSuggestANullDestination(t *testing.T) {
	db := upstreamPGDB(t)
	upstreamFixture(t, db)

	// The bronze dump genuinely writes `events`, but it cannot say into what namespace
	// (migration 089: object-storage destinations record NULL). Unplaceable is reported
	// as unresolved, not guessed at — the dialog then asks the user, which is the same
	// thing it does today.
	resp := resolve(t, db, "SELECT * FROM analytics.events")
	if len(resp.Candidates) != 0 {
		t.Fatalf("a NULL-destination pipeline was suggested: %v", names(resp))
	}
	if len(resp.Unresolved) != 1 || resp.Unresolved[0] != "analytics.events" {
		t.Fatalf("expected analytics.events unresolved, got %v", resp.Unresolved)
	}

	// The same question asked WITHOUT a schema, which is the case the NOT NULL guard
	// actually exists for. A qualified reference is already excluded by the comparison
	// itself — NULL matches no schema.table string — so only this form can reach the
	// bare-name path and be offered a pipeline that cannot say where it writes.
	resp = resolve(t, db, "SELECT * FROM events")
	if len(resp.Candidates) != 0 {
		t.Fatalf("bare name resolved to a NULL-destination pipeline: %v", names(resp))
	}
	if len(resp.Unresolved) != 1 || resp.Unresolved[0] != "events" {
		t.Fatalf("expected events unresolved, got %v", resp.Unresolved)
	}
}

func TestPG_UpstreamResolvesEveryInputOfAJoin(t *testing.T) {
	db := upstreamPGDB(t)
	upstreamFixture(t, db)

	// A model with two inputs has two upstreams. Reporting only the first would make the
	// dialog silently propose a schedule that fires before customers has been refreshed.
	resp := resolve(t, db, `
		SELECT o.id, c.email
		FROM analytics.orders o
		JOIN analytics.customers c ON c.id = o.customer_id
		WHERE o.total > 100`)

	want := []string{"Customers CDC<-analytics.customers", "Orders CDC<-analytics.orders"}
	got := names(resp)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("want %v, got %v", want, got)
	}
	if len(resp.Unresolved) != 0 {
		t.Fatalf("nothing should be unresolved here, got %v", resp.Unresolved)
	}
	if resp.Ambiguous {
		t.Fatal("two references each matching one pipeline is not ambiguity")
	}
}

func TestPG_UpstreamOffersEveryProducerOfOneTableWithoutCallingItAmbiguous(t *testing.T) {
	db := upstreamPGDB(t)
	upstreamFixture(t, db)

	// A pipeline and a model both write analytics.orders. Both really do, and a schedule
	// can follow both, so both are offered — but this is fan-in, not ambiguity: the
	// table is not in question. The asset graph draws the same two edges and calls the
	// same thing fan-in; the two answers must not disagree.
	mustExec(t, db, `INSERT INTO pipelines VALUES ($1,'Orders backfill',$2,$3)`,
		"cccccccc-1111-0000-0000-000000000005", upWS, upWarehse)
	mustExec(t, db, `INSERT INTO pipeline_run_table_stats
		(pipeline_id, table_name, schema_name, qualified_name, destination_schema, destination_qualified_name)
		VALUES ($1,'orders','shop','shop.orders','analytics','analytics.orders')`,
		"cccccccc-1111-0000-0000-000000000005")
	mustExec(t, db, `INSERT INTO saved_queries
		(id, workspace_id, connection_id, name, visibility, created_by, materialization, target_table)
		VALUES ($1,$2,$3,'Orders corrections','workspace',$4,'table','analytics.orders')`,
		"eeeeeeee-1111-0000-0000-000000000008", upWS, upWarehse, upMe)

	resp := resolve(t, db, "SELECT * FROM analytics.orders")
	want := []string{
		"Orders CDC<-analytics.orders",
		"Orders backfill<-analytics.orders",
		"Orders corrections<-analytics.orders",
	}
	if got := names(resp); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("want every producer offered, got %v", got)
	}
	if resp.Ambiguous {
		t.Fatal("three producers of one table were reported as ambiguous")
	}
}

func TestPG_UpstreamReportsAmbiguityRatherThanPickingOne(t *testing.T) {
	db := upstreamPGDB(t)
	upstreamFixture(t, db)

	// `orders` names no schema, and two different tables answer to it. Which one the
	// query reads depends on a search_path nothing here can see, so the flag exists to
	// stop the UI pre-selecting whichever sorted first.
	mustExec(t, db, `INSERT INTO pipelines VALUES ($1,'Staging orders',$2,$3)`,
		"cccccccc-1111-0000-0000-000000000006", upWS, upWarehse)
	mustExec(t, db, `INSERT INTO pipeline_run_table_stats
		(pipeline_id, table_name, schema_name, qualified_name, destination_schema, destination_qualified_name)
		VALUES ($1,'orders','shop','shop.orders','staging','staging.orders')`,
		"cccccccc-1111-0000-0000-000000000006")

	resp := resolve(t, db, "SELECT * FROM orders")
	if !resp.Ambiguous {
		t.Fatalf("a bare name matching two tables must be reported as ambiguous: %v", names(resp))
	}
	if len(resp.Candidates) != 2 {
		t.Fatalf("both readings should be offered, got %v", names(resp))
	}

	// Naming the schema settles it — the control that keeps the flag from being
	// set on every match.
	if named := resolve(t, db, "SELECT * FROM staging.orders"); named.Ambiguous || len(named.Candidates) != 1 {
		t.Fatalf("a qualified reference is not ambiguous: ambiguous=%v %v", named.Ambiguous, names(named))
	}
}

func TestPG_UpstreamUnqualifiedReferencePrefersTheQualifiedMatch(t *testing.T) {
	db := upstreamPGDB(t)
	upstreamFixture(t, db)

	// `FROM orders` names no schema. It still matches on table name alone, because a
	// user's ad-hoc SQL relies on the connection's search_path far more often than it
	// spells out the schema — and the candidate is confirmed by a person either way.
	resp := resolve(t, db, "SELECT * FROM orders")
	if len(resp.Candidates) != 1 || resp.Candidates[0].ID != upOrders {
		t.Fatalf("bare table name did not resolve: %v", names(resp))
	}
	if resp.Candidates[0].Qualified {
		t.Fatal("a name-only match must be flagged as the weaker match it is")
	}
	// The table it reports is the destination name, not the bare reference: the user is
	// choosing between pipelines and needs to see what each one actually writes.
	if resp.Candidates[0].Table != "analytics.orders" {
		t.Fatalf("want the destination table reported, got %q", resp.Candidates[0].Table)
	}
}

func TestPG_UpstreamIgnoresCTEsAndInventsNothing(t *testing.T) {
	db := upstreamPGDB(t)
	upstreamFixture(t, db)

	// `orders` here is a CTE that shadows a real produced table. Treating it as a
	// dependency would attach a schedule to a pipeline this model does not read.
	resp := resolve(t, db, `
		WITH orders AS (SELECT 1 AS id)
		SELECT * FROM orders`)
	if len(resp.Candidates) != 0 {
		t.Fatalf("a CTE was resolved to a pipeline: %v", names(resp))
	}

	// Nothing at all to go on: no crash, no invention, and JSON-safe empty slices
	// rather than nulls the UI would have to special-case.
	resp = resolve(t, db, "SELECT 1")
	if resp.References == nil || resp.Candidates == nil || resp.Unresolved == nil {
		t.Fatal("empty results must be empty slices, not nil")
	}
	if len(resp.References) != 0 {
		t.Fatalf("no tables in this query, got %v", resp.References)
	}
}

func TestPG_UpstreamOffersTheModelThatBuildsTheTable(t *testing.T) {
	db := upstreamPGDB(t)
	upstreamFixture(t, db)

	// Three models build analytics.customer_dim: one here, one on another connection,
	// one in another workspace. Only the first builds the table this model reads; the
	// other two are the same name in a different warehouse, and would arrive as extra
	// candidates if either predicate went missing from the model query.
	resp := resolve(t, db, "SELECT * FROM analytics.customer_dim")
	if got := names(resp); len(got) != 1 || got[0] != "Customer dim<-analytics.customer_dim" {
		t.Fatalf("want only this connection's model offered, got %v", got)
	}
	c := resp.Candidates[0]
	if c.Kind != assetKindModel || c.ID != upDim || c.Table != "analytics.customer_dim" || !c.Qualified {
		t.Fatalf("candidate is not the model as the picker needs it: %+v", c)
	}
}

func TestPG_UpstreamNeverNamesAnotherUsersPrivateModel(t *testing.T) {
	db := upstreamPGDB(t)
	upstreamFixture(t, db)

	sqlText := `SELECT * FROM analytics.my_scratch m JOIN analytics.their_scratch t ON t.id = m.id`

	// My private model is mine to follow. Theirs is theirs: offering it by name would
	// tell me it exists, and what it builds.
	mine := resolve(t, db, sqlText)
	if got := names(mine); len(got) != 1 || got[0] != "My scratch<-analytics.my_scratch" {
		t.Fatalf("want only my own private model offered, got %v", got)
	}
	if len(mine.Unresolved) != 1 || mine.Unresolved[0] != "analytics.their_scratch" {
		t.Fatalf("another user's private model must leave its table unresolved, got %v", mine.Unresolved)
	}

	// The control: the same query asked by its author does find it. Without this, a
	// model query that dropped private models altogether would pass the half above.
	asThem := lookup(sqlText)
	asThem.UserID = upThem
	theirs := resolveAs(t, db, asThem)
	if got := names(theirs); len(got) != 1 || got[0] != "Their scratch<-analytics.their_scratch" {
		t.Fatalf("the author should see their own private model, got %v", got)
	}
}

func TestPG_UpstreamIgnoresAModelThatBuildsNoTable(t *testing.T) {
	db := upstreamPGDB(t)
	upstreamFixture(t, db)

	// A statement model may well write analytics.stmt_out, but nothing records that it
	// does; the target_table left on it from an earlier mode is not a claim.
	resp := resolve(t, db, "SELECT * FROM analytics.stmt_out")
	if len(resp.Candidates) != 0 {
		t.Fatalf("a model that does not materialize a table was offered: %v", names(resp))
	}
}

func TestPG_UpstreamModelIsNotItsOwnUpstream(t *testing.T) {
	db := upstreamPGDB(t)
	upstreamFixture(t, db)

	// Daily MRR reads the table it builds (an incremental model). The saved query's own
	// id has to reach the builder for this to hold; a lookup that lost it would offer
	// the model as its own upstream.
	resp := resolve(t, db, "SELECT * FROM analytics.daily_mrr")
	if len(resp.Candidates) != 0 {
		t.Fatalf("the model was offered as its own upstream: %v", names(resp))
	}
	if len(resp.Unresolved) != 1 || resp.Unresolved[0] != "analytics.daily_mrr" {
		t.Fatalf("expected analytics.daily_mrr unresolved, got %v", resp.Unresolved)
	}
}
