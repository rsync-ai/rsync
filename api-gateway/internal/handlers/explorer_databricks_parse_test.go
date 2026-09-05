package handlers

import (
	"encoding/json"
	"testing"
)

// Regression test for the Data Explorer Databricks result parser. The bug:
// columns were read from `result.schema.columns`, but the Databricks SQL
// Statement Execution API returns them under the TOP-LEVEL
// `manifest.schema.columns` (there is no `result.schema`). The old struct
// therefore produced zero columns, and every row was built as an empty map —
// the Data Explorer returned `columns:[]`, `rows:[{}]` for a valid query.
//
// This payload is the real shape returned live by
// POST https://<host>/api/2.0/sql/statements for
// `SELECT COUNT(*) AS total_trips, ROUND(AVG(trip_distance),3) AS avg_trip_distance,
//  ROUND(AVG(fare_amount),3) AS avg_fare_amount FROM samples.nyctaxi.trips`.
const realDatabricksStatementJSON = `{
  "statement_id": "01ef-abc",
  "status": {"state": "SUCCEEDED"},
  "manifest": {
    "format": "JSON_ARRAY",
    "schema": {
      "column_count": 3,
      "columns": [
        {"name": "total_trips",       "type_text": "BIGINT", "type_name": "LONG",   "position": 0},
        {"name": "avg_trip_distance", "type_text": "DOUBLE", "type_name": "DOUBLE", "position": 1},
        {"name": "avg_fare_amount",   "type_text": "DOUBLE", "type_name": "DOUBLE", "position": 2}
      ]
    }
  },
  "result": {"data_array": [["21932", "2.853", "12.349"]]}
}`

func TestDatabricksStatementResponse_ColumnsFromManifest(t *testing.T) {
	var resp databricksStatementResponse
	if err := json.Unmarshal([]byte(realDatabricksStatementJSON), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	// Columns must come from manifest.schema.columns.
	if got := len(resp.Manifest.Schema.Columns); got != 3 {
		t.Fatalf("manifest columns = %d, want 3 (the pre-fix result.schema path yielded 0)", got)
	}
	if resp.Result == nil || len(resp.Result.DataArray) != 1 {
		t.Fatalf("result.data_array missing: %+v", resp.Result)
	}

	// Mirror executeDatabricksQueryWithRedact's cols+rows extraction and assert
	// the previously-blank output is now populated.
	var cols []string
	for _, c := range resp.Manifest.Schema.Columns {
		if c.Name != "" {
			cols = append(cols, c.Name)
		}
	}
	want := []string{"total_trips", "avg_trip_distance", "avg_fare_amount"}
	if len(cols) != len(want) {
		t.Fatalf("cols = %v, want %v", cols, want)
	}
	for i := range want {
		if cols[i] != want[i] {
			t.Fatalf("cols[%d] = %q, want %q", i, cols[i], want[i])
		}
	}

	row := map[string]interface{}{}
	arr := resp.Result.DataArray[0]
	for i, name := range cols {
		if i < len(arr) {
			row[name] = arr[i]
		}
	}
	if row["total_trips"] != "21932" || row["avg_trip_distance"] != "2.853" || row["avg_fare_amount"] != "12.349" {
		t.Fatalf("row = %+v, want the real nyctaxi answer (21932 / 2.853 / 12.349)", row)
	}
}
