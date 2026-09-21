package main

import (
	"encoding/json"
	"testing"
)

// #17: the "destination tool call ok" line logged empty table/rows/rows_written for a
// CDC object write, because it read only response keys the GCS connector never returns.
func TestDestinationCallLogFieldsForObjectStoreCDCWrite(t *testing.T) {
	args := map[string]interface{}{
		"config": map[string]interface{}{"bucket": "b"},
		"key":    "demo/pipe-1/cdc/users/dt=2026-09-16/20260916-101010000-42.parquet",
		"data":   []map[string]interface{}{{"op": "I"}, {"op": "U"}, {"op": "D"}},
		"format": "parquet",
	}
	// The GCS import_data response, round-tripped through JSON like the real call.
	var res map[string]interface{}
	if err := json.Unmarshal([]byte(`{"success":true,"rows_inserted":3,"bytes_written":2048,
		"metadata":{"bucket":"b","key":"demo/pipe-1/cdc/users/dt=2026-09-16/20260916-101010000-42.parquet","format":"parquet"}}`), &res); err != nil {
		t.Fatal(err)
	}

	lf := destinationCallLogFieldsFor(args, res)
	if lf.rows != "3" {
		t.Errorf("rows = %q, want 3 (len of args.data)", lf.rows)
	}
	if lf.rowsWritten != "3" {
		t.Errorf("rows_written = %q, want 3 (from rows_inserted)", lf.rowsWritten)
	}
	if lf.table != "demo/pipe-1/cdc/users" {
		t.Errorf("table = %q, want the object key's table directory demo/pipe-1/cdc/users", lf.table)
	}
	// Hive partition segments can hold row values; the scrub-exempt table label must cut them.
	if got := objectKeyTableLabel("lake/p/shop/orders/region=eu/tier=gold/dt=2026-09-16/x-1.jsonl"); got != "lake/p/shop/orders" {
		t.Errorf("objectKeyTableLabel leaked partition values: %q", got)
	}
	if got := objectKeyTableLabel("lake/p/shop/orders/2026-09-16/x-1.jsonl"); got != "lake/p/shop/orders/2026-09-16" {
		t.Errorf("objectKeyTableLabel(no hive) = %q", got)
	}
	if lf.bytesWritten != "2048" {
		t.Errorf("bytes_written = %q, want 2048", lf.bytesWritten)
	}

	// The log line itself carries the values (not only the helper).
	rec := captureLogEvent(t, func() {
		logEvent("info", "destination tool call ok", "table", lf.table, "rows_written", lf.rowsWritten, "rows", lf.rows)
	})
	for _, k := range []string{"table", "rows_written", "rows"} {
		if s, _ := rec[k].(string); s == "" {
			t.Errorf("log field %q is empty: %v", k, rec)
		}
	}
}

func TestDestinationCallLogFieldsForRelationalWrite(t *testing.T) {
	args := map[string]interface{}{
		"table": "analytics.orders",
		"data":  []interface{}{map[string]interface{}{"id": 1}, map[string]interface{}{"id": 2}},
	}
	res := map[string]interface{}{"success": true, "rows_upserted": float64(2)}
	lf := destinationCallLogFieldsFor(args, res)
	if lf.table != "analytics.orders" || lf.rows != "2" || lf.rowsWritten != "2" {
		t.Fatalf("got %+v", lf)
	}
	// A response table wins over the request's; explicit rows_written wins over rows_inserted.
	res = map[string]interface{}{"table": "analytics.orders_v2", "rows_written": float64(5), "rows_inserted": float64(9)}
	lf = destinationCallLogFieldsFor(args, res)
	if lf.table != "analytics.orders_v2" || lf.rowsWritten != "5" {
		t.Fatalf("got %+v", lf)
	}
	// Control: nothing to report stays empty rather than inventing a value.
	lf = destinationCallLogFieldsFor(map[string]interface{}{}, map[string]interface{}{"success": true})
	if lf.table != "" || lf.rows != "" || lf.rowsWritten != "" {
		t.Fatalf("empty request/response produced %+v", lf)
	}
	// nil maps must not panic.
	_ = destinationCallLogFieldsFor(nil, nil)
}
