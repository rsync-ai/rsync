package executor

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/rsync-ai/backend-orchestrator/internal/mcp"
)

// Issue #14: the table picker listed collections/tables but never said which
// database they came from. With one MongoDB connection per database, a user
// could not tell two pickers apart, and a wrong pick syncs the wrong data. The
// executor's table-selection Result is what the picker is built from, so these
// tests pin `source_database` on that Result and prove the table list itself is
// unchanged.

// discovered parses a discover_schema `tables` payload the same way
// DiscoverSchema does (JSON → []TableMetadata), so the fixtures carry the
// connectors' real field names rather than hand-set struct fields.
func discovered(t *testing.T, payload string) []TableMetadata {
	t.Helper()
	var tables []TableMetadata
	if err := json.Unmarshal([]byte(payload), &tables); err != nil {
		t.Fatalf("fixture does not parse as discover_schema tables: %v", err)
	}
	if len(tables) == 0 {
		t.Fatalf("fixture parsed to zero tables; a zero-table case proves nothing")
	}
	return tables
}

// The MongoDB connector (database/mongodb/versions/v1.0.0/connector.py
// discover_schema) reports each collection with `schema` = the database name,
// and reads the database from config `database`/`db_name`/`db`.
const mongoOrdersCollections = `[
	{"name": "orders",    "schema": "orders_db", "row_count": 1200, "columns": [{"name": "_id", "type": "string"}, {"name": "total", "type": "number"}]},
	{"name": "customers", "schema": "orders_db", "row_count": null, "columns": [{"name": "_id", "type": "string"}]}
]`

// The PostgreSQL connector reports the PG schema (public, sales) as `schema`;
// the database lives only in the connection config.
const postgresAppTables = `[
	{"name": "users",    "schema": "public", "row_count": 10, "columns": [{"name": "id", "type": "integer"}, {"name": "email", "type": "string"}]},
	{"name": "invoices", "schema": "sales",  "row_count": 42, "columns": [{"name": "id", "type": "integer"}]}
]`

func TestTableSelectionResult_MongoDBNamesTheDatabase(t *testing.T) {
	tables := discovered(t, mongoOrdersCollections)
	cfg := map[string]string{"database": "orders_db", "connection_type": "mongodb"}

	res := tableSelectionResult("mongodb", cfg, tables, "Select what to sync from mongodb before execution.")

	if got := res["source_database"]; got != "orders_db" {
		t.Fatalf("source_database = %#v, want %q", got, "orders_db")
	}
	if _, manual := res["allow_manual_entry"]; manual {
		t.Fatalf("a request that offers %d tables must not switch to manual entry", len(tables))
	}
}

func TestTableSelectionResult_SaysWhetherTheSourceIsServerLevel(t *testing.T) {
	tables := discovered(t, mongoOrdersCollections)
	for _, tc := range []struct {
		name       string
		sourceType string
		cfg        map[string]string
		want       bool
	}{
		{"mongodb naming no database", "mongodb", map[string]string{"host": "h"}, true},
		{"mysql naming no database", "mysql", nil, true},
		{"mongodb pinned to a database", "mongodb", map[string]string{"database": "orders_db"}, false},
		{"postgres schemas are never server-level", "postgresql", map[string]string{"host": "h"}, false},
	} {
		res := tableSelectionResult(tc.sourceType, tc.cfg, tables, "Select.")
		if got, ok := res["source_server_level"].(bool); !ok || got != tc.want {
			t.Errorf("%s: source_server_level = %#v, want %v", tc.name, res["source_server_level"], tc.want)
		}
	}
}

func TestTableSelectionResult_MongoDBTablesBeatAStaleConfig(t *testing.T) {
	// The connection was saved against old_db but discovery listed orders_db's
	// collections: the heading must name where the listed tables really are.
	tables := discovered(t, mongoOrdersCollections)

	res := tableSelectionResult("mongodb", map[string]string{"database": "old_db"}, tables, "Select.")

	if got := res["source_database"]; got != "orders_db" {
		t.Fatalf("source_database = %#v, want %q (the listed collections' database)", got, "orders_db")
	}
}

func TestTableSelectionResult_MongoDBDatabaseFromAlternateConfigKey(t *testing.T) {
	// The connector also accepts `db_name`; so must the label.
	res := tableSelectionResult("mongodb", map[string]string{"db_name": "ledger"}, nil, "Schema discovery failed.")
	if got := res["source_database"]; got != "ledger" {
		t.Fatalf("source_database = %#v, want %q", got, "ledger")
	}
	if got := res["allow_manual_entry"]; got != true {
		t.Fatalf("allow_manual_entry = %#v, want true when no tables were listed", got)
	}
}

func TestTableSelectionResult_PostgresNamesTheDatabaseNotASchema(t *testing.T) {
	tables := discovered(t, postgresAppTables)
	cfg := map[string]string{"host": "db.internal", "database": "appdb"}

	res := tableSelectionResult("postgresql", cfg, tables, "Select what to sync.")

	// Two PG schemas are still one database; the label must not become
	// "public" or "sales", nor blank because the schemas differ.
	if got := res["source_database"]; got != "appdb" {
		t.Fatalf("source_database = %#v, want %q", got, "appdb")
	}
}

func TestTableSelectionResult_TableListUnchanged(t *testing.T) {
	// Control: adding source_database must not alter what is offered.
	tables := discovered(t, mongoOrdersCollections)
	res := tableSelectionResult("mongodb", map[string]string{"database": "orders_db"}, tables, "Select.")

	got, ok := res["available_tables"].([]map[string]interface{})
	if !ok {
		t.Fatalf("available_tables has type %T, want []map[string]interface{}", res["available_tables"])
	}
	want := []map[string]interface{}{
		{"name": "orders", "schema": "orders_db", "row_count": int64(1200), "columns": 2},
		{"name": "customers", "schema": "orders_db", "row_count": int64(0), "columns": 1},
	}
	if len(want) == 0 || !reflect.DeepEqual(got, want) {
		t.Fatalf("available_tables = %#v\nwant %#v", got, want)
	}
	for k, v := range map[string]interface{}{
		"source_type":   "mongodb",
		"action_needed": "table_selection",
		"reason":        "Select.",
	} {
		if res[k] != v {
			t.Fatalf("%s = %#v, want %#v", k, res[k], v)
		}
	}
}

func TestTableSelectionResult_AlwaysCarriesSourceDatabase(t *testing.T) {
	// The gateway merges wait details into stored metadata, so an absent key
	// would let the database named by an EARLIER wait label this one.
	res := tableSelectionResult("stripe", map[string]string{"api_key_ref": "x"}, nil, "No tables.")
	v, ok := res["source_database"]
	if !ok {
		t.Fatalf("source_database missing; it must be written even when unknown")
	}
	if v != "" {
		t.Fatalf("source_database = %#v, want \"\" for a source with no database", v)
	}
	empty, ok := res["available_tables"].([]map[string]interface{})
	if !ok || empty == nil || len(empty) != 0 {
		t.Fatalf("available_tables = %#v, want a non-nil empty list (JSON [])", res["available_tables"])
	}
}

// TestTableSelectionPause_ExecuteTaskNamesTheDatabase drives the real CDC and
// batch pause branches of executeTask, not just the helper, so a branch that
// stops passing the source's connection config is caught. The tools directory
// is empty, so the connector lookup fails before any connector is started and
// the pause is the "could not list tables" one: nothing is offered, the user
// types a name, and the database still comes from the connection config.
func TestTableSelectionPause_ExecuteTaskNamesTheDatabase(t *testing.T) {
	cases := []struct {
		name       string
		syncMode   string
		sourceType string
		cfg        map[string]string
		want       string
	}{
		{"mongodb cdc", "cdc", "mongodb", map[string]string{"database": "orders_db", "connection_type": "mongodb"}, "orders_db"},
		{"mongodb batch", "batch", "mongodb", map[string]string{"db_name": "ledger"}, "ledger"},
		{"postgresql cdc", "cdc", "postgresql", map[string]string{"host": "db.internal", "database": "appdb"}, "appdb"},
		{"postgresql batch", "batch", "postgresql", map[string]string{"host": "db.internal", "database": "appdb"}, "appdb"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sm := mcp.NewServerManager(t.TempDir())
			a := &Agent{mcpClient: mcp.NewClient(sm), mcpManager: sm}
			task := ExecutorTask{
				TaskID:      "task-src-db",
				PipelineID:  "pl-src-db",
				Operation:   "export",
				Source:      &ConnectorConfig{Type: tc.sourceType, Config: tc.cfg},
				Destination: &ConnectorConfig{Type: "gcs", Config: map[string]string{"bucket": "lake"}},
				Params:      map[string]interface{}{},
				Payload: map[string]interface{}{
					"plan": map[string]interface{}{"sync_mode": tc.syncMode, "steps": []interface{}{}},
				},
			}

			resp := a.executeTask(context.Background(), task)

			if resp.Status != "waiting_for_table_selection" {
				t.Fatalf("status = %q (error %q), want the table-selection pause", resp.Status, resp.Error)
			}
			if got := resp.Result["source_database"]; got != tc.want {
				t.Fatalf("source_database = %#v, want %q", got, tc.want)
			}
			if got := resp.Result["source_type"]; got != tc.sourceType {
				t.Fatalf("source_type = %#v, want %q", got, tc.sourceType)
			}
			// Control: this pause offers no tables and lets the user type one.
			opts, ok := resp.Result["available_tables"].([]map[string]interface{})
			if !ok || opts == nil || len(opts) != 0 {
				t.Fatalf("available_tables = %#v, want a non-nil empty list", resp.Result["available_tables"])
			}
			if resp.Result["allow_manual_entry"] != true {
				t.Fatalf("allow_manual_entry = %#v, want true", resp.Result["allow_manual_entry"])
			}
		})
	}
}

// discoveryCall records what a stubbed DiscoverSchema was asked for.
type discoveryCall struct {
	connectorType string
	config        map[string]interface{}
}

// agentDiscovering returns an Agent whose DiscoverSchema answers with the given
// tables instead of starting a connector, and the calls it received.
func agentDiscovering(tables []TableMetadata) (*Agent, *[]discoveryCall) {
	calls := &[]discoveryCall{}
	a := &Agent{discoverSchemaStub: func(_ context.Context, connectorType string, config map[string]interface{}) ([]TableMetadata, error) {
		*calls = append(*calls, discoveryCall{connectorType: connectorType, config: config})
		return tables, nil
	}}
	return a, calls
}

// TestTableSelectionPause_DiscoveredTablesNameTheDatabase drives the CDC and
// batch pauses that are reached when discovery SUCCEEDS: one that lists the
// tables and one where nothing usable was found. A PostgreSQL source is used
// because its database lives only in the connection config (its tables report
// PG schemas), so a pause that stops passing that config loses the label.
func TestTableSelectionPause_DiscoveredTablesNameTheDatabase(t *testing.T) {
	pgTables := discovered(t, postgresAppTables)
	onlyInternal := discovered(t, `[
		{"name": "_rsync_pipeline_state", "schema": "public", "columns": [{"name": "id", "type": "integer"}]},
		{"name": "flat_pg_orders",        "schema": "public", "columns": [{"name": "id", "type": "integer"}]}
	]`)

	cases := []struct {
		name       string
		syncMode   string
		tables     []TableMetadata
		wantListed int
	}{
		{"cdc lists the tables", "cdc", pgTables, 2},
		{"batch lists the tables", "batch", pgTables, 2},
		{"cdc found no tables", "cdc", []TableMetadata{}, 0},
		{"batch found only rsync's own tables", "batch", onlyInternal, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a, calls := agentDiscovering(tc.tables)
			task := ExecutorTask{
				TaskID:      "task-src-db",
				PipelineID:  "pl-src-db",
				Operation:   "export",
				Source:      &ConnectorConfig{Type: "postgresql", Config: map[string]string{"host": "db.internal", "database": "appdb"}},
				Destination: &ConnectorConfig{Type: "gcs", Config: map[string]string{"bucket": "lake"}},
				Params:      map[string]interface{}{},
				Payload: map[string]interface{}{
					"plan": map[string]interface{}{"sync_mode": tc.syncMode, "steps": []interface{}{}},
				},
			}

			resp := a.executeTask(context.Background(), task)

			if resp.Status != "waiting_for_table_selection" {
				t.Fatalf("status = %q (error %q), want the table-selection pause", resp.Status, resp.Error)
			}
			// Control: the pause came from the stubbed discovery of this source.
			if len(*calls) != 1 || (*calls)[0].connectorType != "postgresql" || (*calls)[0].config["database"] != "appdb" {
				t.Fatalf("discovery calls = %#v, want one for postgresql with the source's config", *calls)
			}
			if got := resp.Result["source_database"]; got != "appdb" {
				t.Fatalf("source_database = %#v, want %q", got, "appdb")
			}
			opts, ok := resp.Result["available_tables"].([]map[string]interface{})
			if !ok || opts == nil || len(opts) != tc.wantListed {
				t.Fatalf("available_tables = %#v, want %d entries", resp.Result["available_tables"], tc.wantListed)
			}
			_, manual := resp.Result["allow_manual_entry"]
			if manual != (tc.wantListed == 0) {
				t.Fatalf("allow_manual_entry present = %v, want %v", manual, tc.wantListed == 0)
			}
		})
	}
}

// TestTableSelectionPause_StepExportGuardNamesTheDatabase drives the export
// step guard in executePlan: a plan step whose table is the connector's own
// name, on a source with several tables, pauses for a pick. The step has no
// connection id, so the source's config is used, and it must reach the label.
func TestTableSelectionPause_StepExportGuardNamesTheDatabase(t *testing.T) {
	a, calls := agentDiscovering(discovered(t, postgresAppTables))
	task := ExecutorTask{
		TaskID:      "task-step-guard",
		PipelineID:  "pl-step-guard",
		Operation:   "execute",
		Source:      &ConnectorConfig{Type: "postgresql", Config: map[string]string{"host": "db.internal", "database": "appdb"}},
		Destination: &ConnectorConfig{Type: "gcs", Config: map[string]string{"bucket": "lake"}},
		Params:      map[string]interface{}{},
	}
	plan := map[string]interface{}{
		"steps": []interface{}{
			map[string]interface{}{"id": "export_1", "tool": "postgresql", "method": "export", "params": map[string]interface{}{"table": "postgresql"}},
		},
	}

	resp := a.executePlan(context.Background(), task, plan)

	if resp.Status != "waiting_for_table_selection" {
		t.Fatalf("status = %q (error %q), want the table-selection pause", resp.Status, resp.Error)
	}
	if len(*calls) != 1 || (*calls)[0].config["database"] != "appdb" {
		t.Fatalf("discovery calls = %#v, want one with the source's config", *calls)
	}
	if got := resp.Result["source_database"]; got != "appdb" {
		t.Fatalf("source_database = %#v, want %q", got, "appdb")
	}
	if got := resp.Result["source_type"]; got != "postgresql" {
		t.Fatalf("source_type = %#v, want %q", got, "postgresql")
	}
	opts, ok := resp.Result["available_tables"].([]map[string]interface{})
	if !ok || len(opts) != 2 {
		t.Fatalf("available_tables = %#v, want the 2 discovered tables", resp.Result["available_tables"])
	}
}

func TestTableSelectionSourceDatabase(t *testing.T) {
	mongo := discovered(t, mongoOrdersCollections)
	pg := discovered(t, postgresAppTables)
	mysqlTwoDatabases := discovered(t, `[
		{"name": "users",  "schema": "shop",    "columns": []},
		{"name": "events", "schema": "metrics", "columns": []}
	]`)
	mysqlOneDatabase := discovered(t, `[
		{"name": "users",  "schema": " shop ", "columns": []},
		{"name": "orders", "schema": "shop",   "columns": []},
		{"name": "misc",   "schema": "",       "columns": []}
	]`)

	cases := []struct {
		name       string
		sourceType string
		cfg        map[string]string
		tables     []TableMetadata
		want       string
	}{
		{"mongo config and tables agree", "mongodb", map[string]string{"database": "orders_db"}, mongo, "orders_db"},
		{"mongo with no database in config falls back to the shared table schema", "mongodb", map[string]string{}, mongo, "orders_db"},
		{"mongo tables win over a stale config value", "mongodb", map[string]string{"database": "old_db"}, mongo, "orders_db"},
		{"mongo discovery failed uses config", "mongodb", map[string]string{"db": " archive "}, nil, "archive"},
		{"database key beats db_name and db", "mongodb", map[string]string{"database": "a", "db_name": "b", "db": "c"}, nil, "a"},
		{"blank database key falls through to db_name", "mongodb", map[string]string{"database": "  ", "db_name": "b"}, nil, "b"},
		{"postgres uses config, not the schemas", "postgresql", map[string]string{"database": "appdb"}, pg, "appdb"},
		{"postgres without a configured database stays blank", "postgresql", map[string]string{}, pg, ""},
		{"mysql tables spanning two databases do not guess one", "mysql", map[string]string{"database": "shop"}, mysqlTwoDatabases, ""},
		{"mysql one shared database, trimmed, blanks ignored", "mysql", map[string]string{}, mysqlOneDatabase, "shop"},
		{"mariadb is a database namespace too: tables spanning two beat the config", "mariadb", map[string]string{"database": "shop"}, mysqlTwoDatabases, ""},
		{"clickhouse is a database namespace too", "clickhouse", map[string]string{}, mysqlOneDatabase, "shop"},
		{"source type is matched without regard to case", "MongoDB", map[string]string{"database": "old_db"}, mongo, "orders_db"},
		{"a postgres `schema` setting is not the database", "postgresql", map[string]string{"schema": "public", "database": "appdb"}, pg, "appdb"},
		{"a `schema` setting alone names no database", "postgresql", map[string]string{"schema": "sales"}, pg, ""},
		{"nil config and nil tables", "mongodb", nil, nil, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tableSelectionSourceDatabase(tc.sourceType, tc.cfg, tc.tables); got != tc.want {
				t.Fatalf("tableSelectionSourceDatabase(%q) = %q, want %q", tc.sourceType, got, tc.want)
			}
		})
	}
}
