package executor

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

// KI-NSPROBE-USES-SOURCE-TABLE-NAMES, orchestrator half.
//
// A single-table run whose prompt names a different destination table writes THAT
// table, but the run-boundary namespace lock told api-gateway only the source
// names — so the probe never looked for the table the run was about to write into.
// The bug class is "the name that is probed is not the name that is written", so
// the test drives the real lock call and asserts the body names exactly what
// singleTableDestination pins for the sink, across every routing shape.

func lockBodyFor(t *testing.T, task *ExecutorTask) map[string]interface{} {
	t.Helper()
	var body map[string]interface{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &body)
		_, _ = w.Write([]byte(`{"locked":true,"namespace":"public"}`))
	}))
	defer srv.Close()
	t.Setenv("API_GATEWAY_INTERNAL_URL", srv.URL)
	(&Agent{}).ensureDestinationNamespaceLocked(context.Background(), task)
	if body == nil {
		t.Fatal("the lock was never called")
	}
	return body
}

func TestNamespaceLockProbesTheTableTheSinkWrites(t *testing.T) {
	pg := &ConnectorConfig{Type: "postgresql"}
	my := &ConnectorConfig{Type: "mysql"}
	cases := []struct {
		name string
		task *ExecutorTask
		want []string // destination_tables in the lock body; nil = key absent
	}{
		{
			// The KI: the prompt renames the one table. The sink writes
			// orders_archive; the probe must be asked about orders_archive.
			name: "prompt renames a single table",
			task: &ExecutorTask{
				PipelineID:  "p1",
				Source:      my,
				Destination: pg,
				Params: map[string]interface{}{
					"tables":       []string{"shop.orders"},
					"user_request": "sync from mysql table shop.orders to postgres table orders_archive",
				},
			},
			want: []string{"orders_archive"},
		},
		{
			// Same, with the request only on the Payload — the sink reads both.
			name: "request carried on the payload",
			task: &ExecutorTask{
				PipelineID:  "p1",
				Source:      my,
				Destination: pg,
				Params:      map[string]interface{}{"selected_tables": []interface{}{"shop.orders"}},
				Payload:     map[string]interface{}{"request": "copy orders into analytics_orders"},
			},
			want: []string{"analytics_orders"},
		},
		{
			// No rename: the destination is the bare source name, which api-gateway
			// already derives. Reported anyway — it is what the sink writes.
			name: "single table, no rename",
			task: &ExecutorTask{
				PipelineID: "p1", Source: my, Destination: pg,
				Params: map[string]interface{}{"tables": []string{"shop.orders"}},
			},
			want: []string{"orders"},
		},
		{
			// Multi-table runs route every table to its own name and ignore any
			// prompt override, so there is nothing beyond the source names to report.
			name: "multi-table run ignores the prompt override",
			task: &ExecutorTask{
				PipelineID: "p1", Source: my, Destination: pg,
				Params: map[string]interface{}{
					"tables":       []string{"shop.orders", "shop.customers"},
					"user_request": "sync from mysql table shop.orders to postgres table orders_archive",
				},
			},
			want: nil,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// What the sink will pin as destCfg["table"] for this task.
			sinkWrites := singleTableDestination(taskInferredDestTable(tc.task), taskSinkTables(tc.task))

			body := lockBodyFor(t, tc.task)
			raw, present := body["destination_tables"]
			if tc.want == nil {
				if present {
					t.Fatalf("destination_tables = %v, want absent for a multi-table run", raw)
				}
				if sinkWrites != "" {
					t.Fatalf("sink pins %q on a multi-table run", sinkWrites)
				}
				return
			}
			list, _ := raw.([]interface{})
			if !present || len(list) == 0 {
				t.Fatalf("lock body has no destination_tables (want %v) — the probe would never look at the table this run writes (%q)", tc.want, sinkWrites)
			}
			var got []string
			for _, v := range list {
				got = append(got, v.(string))
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("lock probes destination_tables=%v, want %v — the probe would never look at the table this run writes", got, tc.want)
			}
			if sinkWrites != tc.want[0] {
				t.Fatalf("sink writes %q but the lock reported %v — probed and written names drifted", sinkWrites, got)
			}
			// The source names still go too: api-gateway's ownership rules for
			// multi-table runs and existing owners are built on them.
			if _, ok := body["selected_tables"]; !ok {
				t.Fatal("selected_tables dropped from the lock body")
			}
		})
	}
}

func TestSingleTableDestination(t *testing.T) {
	cases := []struct {
		inferred string
		tables   []string
		want     string
	}{
		{"orders_archive", []string{"shop.orders"}, "orders_archive"},
		{"", []string{"shop.orders"}, "orders"},
		{"", []string{"orders"}, "orders"},
		{"  ", []string{" public.orders "}, "orders"},
		{"orders_archive", []string{"a", "b"}, ""},
		{"orders_archive", nil, ""},
	}
	for _, c := range cases {
		if got := singleTableDestination(c.inferred, c.tables); got != c.want {
			t.Errorf("singleTableDestination(%q, %v) = %q, want %q", c.inferred, c.tables, got, c.want)
		}
	}
}
