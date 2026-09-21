package executor

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"testing"
)

// Source discovery used to ask connectors for their default of 100 tables and
// never said the list was cut: table 101+ was missing from the picker and ran
// with no PK and untyped columns. These tests pin the higher cap and the
// "N of M tables" signal the picker is built from.

func TestDiscoveryTotals_Truncated(t *testing.T) {
	cases := []struct {
		totals discoveryTotals
		want   bool
	}{
		{discoveryTotals{Discovered: 100, Available: 250}, true},
		{discoveryTotals{Discovered: 250, Available: 250}, false},
		// v1 envelopes report no total; that is not a truncation.
		{discoveryTotals{Discovered: 100, Available: 0}, false},
		{discoveryTotals{}, false},
	}
	for _, tc := range cases {
		if got := tc.totals.truncated(); got != tc.want {
			t.Errorf("%+v.truncated() = %v, want %v", tc.totals, got, tc.want)
		}
	}
}

func TestTableSelectionResult_AlwaysWritesTotals(t *testing.T) {
	tables := discovered(t, postgresAppTables)
	result := tableSelectionResult("postgresql", map[string]string{"database": "appdb"}, tables, "pick")
	if got := result["total_tables_available"]; got != len(tables) {
		t.Fatalf("total_tables_available = %#v, want %d", got, len(tables))
	}
	if got := result["tables_truncated"]; got != false {
		t.Fatalf("tables_truncated = %#v, want false", got)
	}
	// An empty wait still writes both keys, or a stale "truncated" survives the merge.
	empty := tableSelectionResult("postgresql", nil, []TableMetadata{}, "pick")
	if empty["total_tables_available"] != 0 || empty["tables_truncated"] != false {
		t.Fatalf("empty result totals = %#v / %#v, want 0 / false", empty["total_tables_available"], empty["tables_truncated"])
	}
}

func TestWithDiscoveryTotals(t *testing.T) {
	tables := discovered(t, postgresAppTables)

	cut := withDiscoveryTotals(tableSelectionResult("postgresql", nil, tables, "pick"),
		discoveryTotals{Discovered: 5000, Available: 12000})
	if cut["total_tables_available"] != 12000 || cut["tables_truncated"] != true {
		t.Fatalf("truncated: totals = %#v / %#v, want 12000 / true", cut["total_tables_available"], cut["tables_truncated"])
	}

	// Control: a complete list keeps the defaults.
	whole := withDiscoveryTotals(tableSelectionResult("postgresql", nil, tables, "pick"),
		discoveryTotals{Discovered: len(tables), Available: len(tables)})
	if whole["total_tables_available"] != len(tables) || whole["tables_truncated"] != false {
		t.Fatalf("complete: totals = %#v / %#v, want %d / false", whole["total_tables_available"], whole["tables_truncated"], len(tables))
	}
}

func TestMissingSelectedTables(t *testing.T) {
	disc := discovered(t, `[
		{"name": "orders",    "schema": "public", "columns": []},
		{"name": "customers", "schema": "sales",  "columns": []}
	]`)
	cases := []struct {
		name     string
		selected interface{}
		want     []string
	}{
		{"all found, bare and qualified", []string{"orders", "sales.customers", "public.orders"}, nil},
		{"one past the cap", []string{"orders", "sales.invoices"}, []string{"sales.invoices"}},
		{"json-decoded list", []interface{}{"orders", "ledger", 42}, []string{"ledger"}},
		{"blank names ignored", []string{" ", ""}, nil},
		{"no selection", nil, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := missingSelectedTables(tc.selected, disc); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("missingSelectedTables = %#v, want %#v", got, tc.want)
			}
		})
	}
}

// agentDiscoveringTotals is agentDiscovering with connector-reported totals.
func agentDiscoveringTotals(tables []TableMetadata, available int) *Agent {
	return &Agent{discoverTotalsStub: func(_ context.Context, _ string, _ map[string]interface{}) ([]TableMetadata, discoveryTotals, error) {
		return tables, discoveryTotals{Discovered: len(tables), Available: available}, nil
	}}
}

// All three table-selection pauses (CDC, batch, export-step guard) must carry
// the connector's totals, so the picker can say it shows part of the source.
func TestTableSelectionPause_CarriesDiscoveryTotals(t *testing.T) {
	pgTables := discovered(t, postgresAppTables)
	pause := func(t *testing.T, a *Agent, path string) map[string]interface{} {
		t.Helper()
		task := ExecutorTask{
			TaskID:      "task-totals",
			PipelineID:  "pl-totals",
			Operation:   "export",
			Source:      &ConnectorConfig{Type: "postgresql", Config: map[string]string{"host": "db.internal", "database": "appdb"}},
			Destination: &ConnectorConfig{Type: "gcs", Config: map[string]string{"bucket": "lake"}},
			Params:      map[string]interface{}{},
		}
		var resp ExecutorResponse
		if path == "step guard" {
			task.Operation = "execute"
			resp = a.executePlan(context.Background(), task, map[string]interface{}{
				"steps": []interface{}{
					map[string]interface{}{"id": "export_1", "tool": "postgresql", "method": "export", "params": map[string]interface{}{"table": "postgresql"}},
				},
			})
		} else {
			task.Payload = map[string]interface{}{
				"plan": map[string]interface{}{"sync_mode": path, "steps": []interface{}{}},
			}
			resp = a.executeTask(context.Background(), task)
		}
		if resp.Status != "waiting_for_table_selection" {
			t.Fatalf("status = %q (error %q), want the table-selection pause", resp.Status, resp.Error)
		}
		return resp.Result
	}

	for _, path := range []string{"cdc", "batch", "step guard"} {
		t.Run(path, func(t *testing.T) {
			cut := pause(t, agentDiscoveringTotals(pgTables, 7500), path)
			if cut["total_tables_available"] != 7500 || cut["tables_truncated"] != true {
				t.Fatalf("truncated source: totals = %#v / %#v, want 7500 / true", cut["total_tables_available"], cut["tables_truncated"])
			}
			if opts, _ := cut["available_tables"].([]map[string]interface{}); len(opts) != len(pgTables) {
				t.Fatalf("available_tables = %#v, want the %d discovered tables", cut["available_tables"], len(pgTables))
			}

			// Control: a source the connector returned whole is not flagged.
			whole := pause(t, agentDiscoveringTotals(pgTables, len(pgTables)), path)
			if whole["total_tables_available"] != len(pgTables) || whole["tables_truncated"] != false {
				t.Fatalf("whole source: totals = %#v / %#v, want %d / false", whole["total_tables_available"], whole["tables_truncated"], len(pgTables))
			}
		})
	}
}

// The connector call is backed by real containers, so the cap is pinned at the
// source level: both discovery entry points must send
// "max_tables": sourceDiscoveryMaxTables, or connectors fall back to 100.
func TestDiscoverSchemaEntryPointsSendMaxTables(t *testing.T) {
	if sourceDiscoveryMaxTables < 1000 {
		t.Fatalf("sourceDiscoveryMaxTables = %d; the connectors' own default is 100", sourceDiscoveryMaxTables)
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "executor.go", nil, 0)
	if err != nil {
		t.Fatalf("parse executor.go: %v", err)
	}
	want := map[string]bool{"discoverSchemaWithTotals": false, "DiscoverSchemaEnvelope": false}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv == nil || fn.Body == nil {
			continue
		}
		if _, tracked := want[fn.Name.Name]; !tracked {
			continue
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			kv, ok := n.(*ast.KeyValueExpr)
			if !ok {
				return true
			}
			key, ok := kv.Key.(*ast.BasicLit)
			val, vok := kv.Value.(*ast.Ident)
			if ok && vok && key.Value == `"max_tables"` && val.Name == "sourceDiscoveryMaxTables" {
				want[fn.Name.Name] = true
			}
			return true
		})
	}
	for name, found := range want {
		if !found {
			t.Errorf(`(*Agent).%s does not send "max_tables": sourceDiscoveryMaxTables`, name)
		}
	}
}
