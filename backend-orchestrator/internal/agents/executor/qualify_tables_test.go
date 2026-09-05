package executor

import (
	"fmt"
	"reflect"
	"testing"
)

// asStrings normalizes whatever the helper wrote back ([]string or []interface{})
// into a []string for comparison.
func asStrings(v interface{}) []string {
	switch t := v.(type) {
	case []string:
		return t
	case []interface{}:
		out := make([]string, 0, len(t))
		for _, it := range t {
			out = append(out, fmt.Sprintf("%v", it))
		}
		return out
	default:
		return nil
	}
}

// TestQualifySelectedTablesForSource is the regression guard for Bug #13:
// the batch path previously skipped schema qualification (only the plan-less
// CDC branch qualified), so PostgreSQL batch transfers emitted
// `SELECT * FROM <db>.<table>` → "relation does not exist". The shared helper
// now qualifies BOTH paths identically: PG by schema (default "public"),
// MySQL/others by database name. Pre-qualified names pass through untouched.
func TestQualifySelectedTablesForSource(t *testing.T) {
	cases := []struct {
		name    string
		srcType string
		cfg     map[string]string
		in      interface{}
		want    []string
	}{
		{
			name:    "postgres unqualified -> public",
			srcType: "postgresql",
			cfg:     map[string]string{"database": "pipeline_test"},
			in:      []interface{}{"p2m_events", "p2m_orders"},
			want:    []string{"public.p2m_events", "public.p2m_orders"},
		},
		{
			name:    "postgres explicit non-default schema",
			srcType: "postgresql",
			cfg:     map[string]string{"database": "pipeline_test", "schema": "analytics"},
			in:      []interface{}{"events"},
			want:    []string{"analytics.events"},
		},
		{
			name:    "postgres alias 'postgres' normalized",
			srcType: "postgres",
			cfg:     map[string]string{"database": "pipeline_test"},
			in:      []string{"big_table"},
			want:    []string{"public.big_table"},
		},
		{
			name:    "mysql unqualified -> database name (NOT public)",
			srcType: "mysql",
			cfg:     map[string]string{"database": "e2e_db"},
			in:      []interface{}{"big_table", "orders"},
			want:    []string{"e2e_db.big_table", "e2e_db.orders"},
		},
		{
			name:    "already qualified passes through untouched",
			srcType: "postgresql",
			cfg:     map[string]string{"database": "pipeline_test"},
			in:      []interface{}{"public.p2m_events", "tenant_5.orders"},
			want:    []string{"public.p2m_events", "tenant_5.orders"},
		},
		{
			name:    "whitespace trimmed and blanks dropped",
			srcType: "postgresql",
			cfg:     map[string]string{"database": "pipeline_test"},
			in:      []interface{}{" p2m_events ", "", "p2m_orders"},
			want:    []string{"public.p2m_events", "public.p2m_orders"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			task := &ExecutorTask{
				Source: &ConnectorConfig{Type: tc.srcType, Config: tc.cfg},
				Params: map[string]interface{}{"tables": tc.in},
			}
			qualifySelectedTablesForSource(task)

			got := asStrings(task.Params["tables"])
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("tables: got %v, want %v", got, tc.want)
			}
			// selected_tables must stay in lockstep so the sink + Debezium agree.
			gotSel := asStrings(task.Params["selected_tables"])
			if !reflect.DeepEqual(gotSel, tc.want) {
				t.Fatalf("selected_tables: got %v, want %v", gotSel, tc.want)
			}
		})
	}
}

// TestQualifySelectedTablesForSource_NoMutationWhenSourceMissing guards the
// nil-safety contract: the helper is a no-op if Source/Params are unset, so it
// is safe to call unconditionally at the executeDataTransfer chokepoint.
func TestQualifySelectedTablesForSource_NoMutationWhenSourceMissing(t *testing.T) {
	// nil task
	qualifySelectedTablesForSource(nil)

	// nil source
	task := &ExecutorTask{Params: map[string]interface{}{"tables": []string{"t1"}}}
	qualifySelectedTablesForSource(task)
	if got := asStrings(task.Params["tables"]); !reflect.DeepEqual(got, []string{"t1"}) {
		t.Fatalf("expected unchanged with nil source, got %v", got)
	}

	// falls back to selected_tables when "tables" key absent
	task2 := &ExecutorTask{
		Source: &ConnectorConfig{Type: "postgresql", Config: map[string]string{"database": "db"}},
		Params: map[string]interface{}{"selected_tables": []interface{}{"events"}},
	}
	qualifySelectedTablesForSource(task2)
	if got := asStrings(task2.Params["tables"]); !reflect.DeepEqual(got, []string{"public.events"}) {
		t.Fatalf("expected selected_tables fallback to qualify, got %v", got)
	}
}

// TestQualifySelectedTablesForSource_DropsUnresolvedSentinel is the executor
// safety net: a "select entire database" ("*") / "select entire namespace"
// ("<ns>.*") sentinel must be expanded to explicit tables upstream (api-gateway).
// If one slips through it must be DROPPED, never qualified into a bogus
// "<schema>.*" table.
func TestQualifySelectedTablesForSource_DropsUnresolvedSentinel(t *testing.T) {
	// Sentinels dropped, real tables in the same list still qualify.
	task := &ExecutorTask{
		Source: &ConnectorConfig{Type: "postgresql", Config: map[string]string{"database": "db"}},
		Params: map[string]interface{}{"tables": []string{"*", "public.orders", "staging.*"}},
	}
	qualifySelectedTablesForSource(task)
	if got := asStrings(task.Params["tables"]); !reflect.DeepEqual(got, []string{"public.orders"}) {
		t.Fatalf("expected sentinels dropped and real table kept, got %v", got)
	}

	// A list of ONLY sentinels clears the raw list (no bogus "public.*" leaks
	// downstream); the empty-selection handling then applies.
	task2 := &ExecutorTask{
		Source: &ConnectorConfig{Type: "postgresql", Config: map[string]string{"database": "db"}},
		Params: map[string]interface{}{"tables": []string{"*"}},
	}
	qualifySelectedTablesForSource(task2)
	if got := asStrings(task2.Params["tables"]); len(got) != 0 {
		t.Fatalf("expected empty tables after dropping only-sentinel list, got %v", got)
	}
}

// TestQualifySelectedTablesForSource_ObjectStorage guards the object-storage
// table-naming fix: the destination table name must come from the source's
// source_table_name (then bucket/container), never from a connector_type that
// leaked into selected_tables. A live azure-blob run landed its table as
// "postgres" (the destination connector_type) instead of the configured
// source_table_name, because the orchestrator names the dest table only from
// selected_tables and the single file_mapping serves the one table regardless of
// the requested name (so it failed silently instead of erroring).
func TestQualifySelectedTablesForSource_ObjectStorage(t *testing.T) {
	cases := []struct {
		name     string
		srcType  string
		destType string
		cfg      map[string]string
		in       interface{} // nil => no "tables"/"selected_tables" key at all
		want     []string
	}{
		{
			name:     "leaked dest connector_type dropped, source_table_name fallback",
			srcType:  "azure-blob",
			destType: "postgresql", // normalizes to match the leaked "postgres" token
			cfg:      map[string]string{"source_table_name": "azure_items", "bucket": "azure-e2e"},
			in:       []interface{}{"postgres"},
			want:     []string{"azure_items"},
		},
		{
			name:     "empty table list -> source_table_name fallback",
			srcType:  "gcs",
			destType: "postgresql",
			cfg:      map[string]string{"source_table_name": "gcs_items"},
			in:       nil,
			want:     []string{"gcs_items"},
		},
		{
			name:     "legit object-storage table preserved",
			srcType:  "aws-s3",
			destType: "postgresql",
			cfg:      map[string]string{"source_table_name": "items", "bucket": "b"},
			in:       []interface{}{"events"},
			want:     []string{"events"},
		},
		{
			name:     "leaked source connector_type dropped, bucket fallback",
			srcType:  "azure-blob",
			destType: "postgresql",
			cfg:      map[string]string{"bucket": "my-bucket"},
			in:       []interface{}{"azure-blob"},
			want:     []string{"my-bucket"},
		},
		{
			name:     "container fallback when no source_table_name/bucket",
			srcType:  "azure-blob",
			destType: "postgresql",
			cfg:      map[string]string{"container": "blobs"},
			in:       nil,
			want:     []string{"blobs"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			params := map[string]interface{}{}
			if tc.in != nil {
				params["tables"] = tc.in
			}
			task := &ExecutorTask{
				Source:      &ConnectorConfig{Type: tc.srcType, Config: tc.cfg},
				Destination: &ConnectorConfig{Type: tc.destType, Config: map[string]string{}},
				Params:      params,
			}
			qualifySelectedTablesForSource(task)
			if got := asStrings(task.Params["tables"]); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("tables: got %v, want %v", got, tc.want)
			}
			if got := asStrings(task.Params["selected_tables"]); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("selected_tables: got %v, want %v", got, tc.want)
			}
		})
	}
}

// TestQualifySelectedTablesForSource_RelationalGuardScoped proves the leaked
// connector_type guard + source_table_name fallback are scoped to object storage:
// a relational source with a table genuinely named like a connector type is NOT
// dropped (a wrong relational table name fails loud downstream, and "mysql" can be
// a real table). This keeps the fix from changing relational pipeline behavior.
func TestQualifySelectedTablesForSource_RelationalGuardScoped(t *testing.T) {
	task := &ExecutorTask{
		Source:      &ConnectorConfig{Type: "mysql", Config: map[string]string{"database": "e2e_db"}},
		Destination: &ConnectorConfig{Type: "mysql", Config: map[string]string{}},
		Params:      map[string]interface{}{"tables": []interface{}{"mysql"}},
	}
	qualifySelectedTablesForSource(task)
	if got := asStrings(task.Params["tables"]); !reflect.DeepEqual(got, []string{"e2e_db.mysql"}) {
		t.Fatalf("relational source must NOT drop a connector-type-named table: got %v", got)
	}
}
