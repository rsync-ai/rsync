//go:build integration_pg

// The loader's SQL against a real Postgres. buildAssetGraph is covered without a
// database (asset_graph_test.go); what only a database can prove is which stats rows
// the produced-tables query hands it. Uses the same scratch database as
// saved_query_upstreams_pg_test.go — see that file's header for the command:
//
//	UPSTREAM_PG_DSN='postgres://postgres:verify@localhost:55443/cplane?sslmode=disable' \
//	    go test -tags integration_pg ./internal/handlers/ -run PG_AssetGraph -v

package handlers

import (
	"context"
	"database/sql"
	"reflect"
	"sort"
	"testing"
)

const (
	agWS    = "a0000000-0000-4000-8000-000000000001"
	agGCS   = "a0000000-0000-4000-8000-0000000000c1"
	agWH    = "a0000000-0000-4000-8000-0000000000c2"
	agToGCS = "a0000000-0000-4000-8000-0000000000f1"
	agShop  = "a0000000-0000-4000-8000-0000000000f2"
	agUser  = "a0000000-0000-4000-8000-0000000000e1"
)

func assetGraphFixture(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx := context.Background()

	const drop = `DROP TABLE IF EXISTS saved_query_schedule_upstreams, saved_query_schedules,
		pipeline_run_table_stats, pipelines, saved_queries`
	_, _ = db.ExecContext(ctx, drop)
	t.Cleanup(func() { _, _ = db.ExecContext(ctx, drop) })

	mustExec(t, db, `CREATE TABLE pipelines (
		id UUID PRIMARY KEY,
		name TEXT,
		workspace_id UUID,
		destination_connection_id UUID,
		created_at TIMESTAMPTZ NOT NULL DEFAULT now()
	)`)
	mustExec(t, db, `CREATE TABLE pipeline_run_table_stats (
		pipeline_id UUID,
		table_name TEXT NOT NULL,
		schema_name TEXT,
		qualified_name TEXT NOT NULL,
		destination_schema TEXT,
		destination_qualified_name TEXT
	)`)
	mustExec(t, db, `CREATE TABLE saved_queries (
		id UUID PRIMARY KEY,
		workspace_id UUID NOT NULL,
		connection_id UUID NOT NULL,
		name TEXT NOT NULL,
		sql_text TEXT NOT NULL DEFAULT '',
		visibility TEXT NOT NULL,
		created_by UUID NOT NULL,
		materialization TEXT NOT NULL DEFAULT 'none',
		target_table TEXT,
		updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
	)`)
	mustExec(t, db, `CREATE TABLE saved_query_schedules (
		schedule_id UUID PRIMARY KEY,
		saved_query_id UUID,
		schedule_type TEXT,
		status TEXT
	)`)
	mustExec(t, db, `CREATE TABLE saved_query_schedule_upstreams (
		schedule_id UUID,
		upstream_pipeline_id UUID,
		upstream_saved_query_id UUID
	)`)

	mustExec(t, db, `INSERT INTO pipelines (id, name, workspace_id, destination_connection_id) VALUES
		($1, 'Mongo to GCS', $3, $4),
		($2, 'Shop CDC',     $3, $5)`,
		agToGCS, agShop, agWS, agGCS, agWH)

	// The object-storage pipeline's rows carry no destination name at all; the
	// relational one has a named table, an unnamed sibling, and an unnamed row for
	// the same table it later named.
	mustExec(t, db, `INSERT INTO pipeline_run_table_stats
		(pipeline_id, table_name, schema_name, qualified_name, destination_schema, destination_qualified_name)
		VALUES
		($1, 'orders',    'shop', 'shop.orders',    NULL,        NULL),
		($1, 'customers', 'shop', 'shop.customers', NULL,        NULL),
		($2, 'orders',    'shop', 'shop.orders',    'analytics', 'analytics.orders'),
		($2, 'orders',    'shop', 'shop.orders',    NULL,        NULL),
		($2, 'events',    'shop', 'shop.events',    NULL,        NULL)`,
		agToGCS, agShop)
}

// KI-LINEAGE-WRITES-0-TABLES-WHEN-DEST-NAME-NULL: the query filtered on
// `destination_qualified_name IS NOT NULL`, so these rows never reached the builder.
func TestPG_AssetGraphLoadsRowsWithNoDestinationName(t *testing.T) {
	db := upstreamPGDB(t)
	assetGraphFixture(t, db)

	in, err := loadAssetGraphInput(context.Background(), db, agWS, agUser, "")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	type row struct {
		pipeline, dest, source string
		unplaced               bool
	}
	var got []row
	for _, tp := range in.Produced {
		got = append(got, row{tp.Table.ProducerName, tp.Table.DestQualified, tp.SourceQualified, tp.Unplaced})
	}
	sort.Slice(got, func(i, j int) bool {
		if got[i].pipeline != got[j].pipeline {
			return got[i].pipeline < got[j].pipeline
		}
		if got[i].dest != got[j].dest {
			return got[i].dest < got[j].dest
		}
		return got[i].source < got[j].source
	})
	want := []row{
		{"Mongo to GCS", "", "shop.customers", true},
		{"Mongo to GCS", "", "shop.orders", true},
		{"Shop CDC", "", "shop.events", true},
		{"Shop CDC", "", "shop.orders", true},
		{"Shop CDC", "analytics.orders", "shop.orders", false},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("produced rows =\n  %+v\nwant\n  %+v", got, want)
	}

	// And end to end: both pipelines write their tables, and the old unnamed
	// orders row does not draw a second orders table beside the named one.
	g := buildAssetGraph(in)
	if n := len(writesFrom(g, "pipeline:"+agToGCS)); n != 2 {
		t.Errorf("object-storage pipeline writes %d tables, want 2", n)
	}
	if n := len(writesFrom(g, "pipeline:"+agShop)); n != 2 {
		t.Errorf("relational pipeline writes %d tables, want 2", n)
	}
	if names := tableNames(g); !reflect.DeepEqual(names, []string{"analytics.orders", "shop.customers", "shop.events", "shop.orders"}) {
		t.Errorf("table names = %v", names)
	}
}

// The destination-connection filter still applies to the unnamed rows.
func TestPG_AssetGraphConnectionFilterAppliesToUnnamedRows(t *testing.T) {
	db := upstreamPGDB(t)
	assetGraphFixture(t, db)

	in, err := loadAssetGraphInput(context.Background(), db, agWS, agUser, agGCS)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(in.Produced) != 2 {
		t.Fatalf("produced = %+v, want the GCS pipeline's 2 rows only", in.Produced)
	}
	for _, tp := range in.Produced {
		if tp.Table.ProducerID != agToGCS {
			t.Errorf("row from %s leaked through the connection filter", tp.Table.ProducerID)
		}
	}
}
