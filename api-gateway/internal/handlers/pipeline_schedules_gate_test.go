package handlers

import "testing"

// A schedule on a pipeline with no saved table selection is dead on arrival: the
// executor's batch path refuses to infer a table, so the first run parks at the
// table_selection HITL and the wrapper workflow's overlap guard then skips every
// later tick as a "successful" no-op. hasSelectedTables is the gate that stops
// that schedule from being created, so its notion of "selected" has to match the
// one FetchPipelineRunContextActivity uses to hydrate the run.
func TestHasSelectedTables(t *testing.T) {
	cases := []struct {
		name   string
		config string
		want   bool
	}{
		{"nil config", "", false},
		{"empty object", `{}`, false},
		{"null selection", `{"selected_tables": null}`, false},
		{"empty array", `{"selected_tables": []}`, false},
		{"blank entries only", `{"selected_tables": ["", "   "]}`, false},
		{"non-array selection", `{"selected_tables": "orders"}`, false},
		{"wrong element type", `{"selected_tables": [1, 2]}`, false},
		{"malformed json", `{"selected_tables": [`, false},

		{"single table", `{"selected_tables": ["orders"]}`, true},
		{"schema-qualified", `{"selected_tables": ["public.orders"]}`, true},
		{"several tables", `{"selected_tables": ["orders", "customers"]}`, true},
		// One usable entry is enough — the run syncs what it can resolve.
		{"blank alongside real", `{"selected_tables": ["", "orders"]}`, true},
		{"padded entry", `{"selected_tables": ["  orders  "]}`, true},
		// Other config keys must not be mistaken for a selection: `table` is
		// exactly the field the executor's HITL policy refuses to infer from.
		{"table key is not a selection", `{"table": "orders"}`, false},
		{"tables key is not a selection", `{"tables": ["orders"]}`, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var raw []byte
			if tc.config != "" {
				raw = []byte(tc.config)
			}
			if got := hasSelectedTables(raw); got != tc.want {
				t.Fatalf("hasSelectedTables(%q) = %v, want %v", tc.config, got, tc.want)
			}
		})
	}
}
