//go:build integration_pg

// Real-Postgres coverage for upstream inference — "which pipeline produces the tables
// this model reads".
//
// It has to run against a real Postgres because the half of the answer that is easiest
// to get wrong lives in the SQL, not in Go: which COLUMN is compared (the whole point of
// migration 089), which rows the tenancy and connection predicates exclude, and how a
// NULL destination behaves. A mock returning canned rows would pass with every one of
// those inverted.
//
// The fixture is the shape migration 089 was written about: a MySQL->Postgres CDC
// pipeline whose source schema and destination schema differ. That difference is what
// makes the wrong column look right in every test where they happen to match.
//
// Needs only a bare postgres — the query touches two tables, so the test creates them
// itself rather than running the migration stack:
//
//	docker run -d --name upstream-pg -e POSTGRES_PASSWORD=verify \
//	    -e POSTGRES_DB=cplane -p 55443:5432 postgres:16
//	UPSTREAM_PG_DSN='postgres://postgres:verify@localhost:55443/cplane?sslmode=disable' \
//	    go test -tags integration_pg ./internal/handlers/ -run PG_Upstream -v
//
// The tests DROP and recreate `pipelines` and `pipeline_run_table_stats`, so point this
// at a scratch database — never at one holding a migrated schema you care about.

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
)

// upstreamFixture builds the two tables and the rows every test below shares.
//
// The CDC detail that matters: every stat row's SOURCE-side qualified_name is
// `shop.<table>` (the MySQL database) while the DESTINATION is `analytics.<table>`.
// A resolver matching qualified_name finds nothing for `analytics.orders` and
// everything for `shop.orders` — which is the bug this fixture exists to catch.
func upstreamFixture(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx := context.Background()

	_, _ = db.ExecContext(ctx, `DROP TABLE IF EXISTS pipeline_run_table_stats, pipelines`)
	t.Cleanup(func() {
		_, _ = db.ExecContext(ctx, `DROP TABLE IF EXISTS pipeline_run_table_stats, pipelines`)
	})

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
}

func mustExec(t *testing.T, db *sql.DB, q string, args ...any) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(), q, args...); err != nil {
		t.Fatalf("exec %.60s: %v", q, err)
	}
}

// names renders candidates as "pipeline<-reference" pairs, sorted, so an assertion
// reads as the mapping it is testing rather than as struct literals.
func names(resp upstreamSuggestionResponse) []string {
	out := make([]string, 0, len(resp.Candidates))
	for _, c := range resp.Candidates {
		out = append(out, c.PipelineName+"<-"+c.MatchedReference)
	}
	sort.Strings(out)
	return out
}

func resolve(t *testing.T, db *sql.DB, sqlText string) upstreamSuggestionResponse {
	t.Helper()
	resp, err := resolveUpstreams(context.Background(), db, sqlText, upWarehse, upWS)
	if err != nil {
		t.Fatalf("resolveUpstreams: %v", err)
	}
	return resp
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
		if c.PipelineID == upForeign {
			t.Fatal("a pipeline writing to another destination connection was suggested")
		}
	}
	if resp.Ambiguous {
		t.Fatal("marked ambiguous by a candidate that should have been excluded")
	}

	// Same query, asked on behalf of a workspace that owns none of these pipelines.
	resp2, err := resolveUpstreams(context.Background(), db, "SELECT * FROM analytics.orders", upWarehse, upOtherWS)
	if err != nil {
		t.Fatalf("resolveUpstreams: %v", err)
	}
	if len(resp2.Candidates) != 0 {
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

func TestPG_UpstreamReportsAmbiguityRatherThanPickingOne(t *testing.T) {
	db := upstreamPGDB(t)
	upstreamFixture(t, db)

	// Two pipelines land the same destination table. Which one a schedule should follow
	// is a real question with no derivable answer, so the flag exists to stop the UI
	// pre-selecting whichever sorted first.
	mustExec(t, db, `INSERT INTO pipelines VALUES ($1,'Orders backfill',$2,$3)`,
		"cccccccc-1111-0000-0000-000000000005", upWS, upWarehse)
	mustExec(t, db, `INSERT INTO pipeline_run_table_stats
		(pipeline_id, table_name, schema_name, qualified_name, destination_schema, destination_qualified_name)
		VALUES ($1,'orders','shop','shop.orders','analytics','analytics.orders')`,
		"cccccccc-1111-0000-0000-000000000005")

	resp := resolve(t, db, "SELECT * FROM analytics.orders")
	if !resp.Ambiguous {
		t.Fatal("two producers of one table must be reported as ambiguous")
	}
	if len(resp.Candidates) != 2 {
		t.Fatalf("both producers should be offered, got %v", names(resp))
	}
}

func TestPG_UpstreamUnqualifiedReferencePrefersTheQualifiedMatch(t *testing.T) {
	db := upstreamPGDB(t)
	upstreamFixture(t, db)

	// `FROM orders` names no schema. It still matches on table name alone, because a
	// user's ad-hoc SQL relies on the connection's search_path far more often than it
	// spells out the schema — and the candidate is confirmed by a person either way.
	resp := resolve(t, db, "SELECT * FROM orders")
	if len(resp.Candidates) != 1 || resp.Candidates[0].PipelineID != upOrders {
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
