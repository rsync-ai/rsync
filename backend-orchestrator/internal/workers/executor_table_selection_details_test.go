package workers

import (
	"reflect"
	"testing"
)

// Issue #14: the executor names the database a table-selection request lists
// (`source_database`), but this worker forwards only an allowlist of Result keys
// into the PIPELINE_WAITING details the picker reads. These tests pin that the
// database survives the allowlist and that nothing else about the details moved.

func tableSelectionResultFixture() map[string]interface{} {
	return map[string]interface{}{
		"available_tables": []map[string]interface{}{
			{"name": "orders", "schema": "orders_db", "row_count": int64(1200), "columns": 2},
			{"name": "customers", "schema": "orders_db", "row_count": int64(0), "columns": 1},
		},
		"source_type":     "mongodb",
		"source_database": "orders_db",
		"action_needed":   "table_selection",
		"reason":          "Select what to sync from mongodb before execution.",
	}
}

// taskPayloadFixture is the part of an executor task payload the details read:
// the pipeline's source and destination connections.
func taskPayloadFixture() map[string]interface{} {
	return map[string]interface{}{
		"source_connection_id":      "conn-mongo-orders",
		"destination_connection_id": "conn-gcs-lake",
		"user_id":                   "user-1",
	}
}

func TestTableSelectionWaitingDetails_ForwardsSourceDatabase(t *testing.T) {
	details := tableSelectionWaitingDetails(tableSelectionResultFixture(), taskPayloadFixture())

	if got := details["source_database"]; got != "orders_db" {
		t.Fatalf("source_database = %#v, want %q", got, "orders_db")
	}

	// A padded value is shown as the bare name, so "Tables in  orders_db " never
	// reads as a different database.
	padded := tableSelectionResultFixture()
	padded["source_database"] = "  orders_db\t"
	if got := tableSelectionWaitingDetails(padded, nil)["source_database"]; got != "orders_db" {
		t.Fatalf("padded source_database = %#v, want %q", got, "orders_db")
	}
}

func TestTableSelectionWaitingDetails_TableListAndKeysUnchanged(t *testing.T) {
	result := tableSelectionResultFixture()
	details := tableSelectionWaitingDetails(result, taskPayloadFixture())

	tables, ok := details["available_tables"].([]map[string]interface{})
	if !ok || len(tables) == 0 {
		t.Fatalf("available_tables = %#v, want the executor's non-empty list", details["available_tables"])
	}
	if !reflect.DeepEqual(details["available_tables"], result["available_tables"]) {
		t.Fatalf("available_tables changed: %#v", details["available_tables"])
	}
	want := map[string]interface{}{
		"request_type":     "table_selection",
		"available_tables": result["available_tables"],
		"source_type":      "mongodb",
		"source_database":  "orders_db",
		// A result without totals (an older executor) reads as "not truncated".
		"total_tables_available": 0,
		"tables_truncated":       false,
		"action_needed":          "table_selection",
		"button_text":            "Select Tables",
		// The picker fetches the SOURCE connection's name and schema; a swap
		// would show the destination's instead.
		"source_connection_id":      "conn-mongo-orders",
		"destination_connection_id": "conn-gcs-lake",
	}
	if !reflect.DeepEqual(details, want) {
		t.Fatalf("details = %#v\nwant %#v", details, want)
	}
}

func TestTableSelectionWaitingDetails_AlwaysWritesSourceDatabase(t *testing.T) {
	// An executor that did not name a database (older build, or a source with
	// none) must still overwrite the stored key, which the gateway merges.
	result := tableSelectionResultFixture()
	delete(result, "source_database")

	details := tableSelectionWaitingDetails(result, map[string]interface{}{"source_connection_id": "", "user_id": "user-1"})

	v, ok := details["source_database"]
	if !ok || v != "" {
		t.Fatalf("source_database = %#v (present=%v), want \"\" present", v, ok)
	}
	if _, ok := details["source_connection_id"]; ok {
		t.Fatalf("source_connection_id must be omitted when unknown")
	}
	if _, ok := details["destination_connection_id"]; ok {
		t.Fatalf("destination_connection_id must be omitted when unknown")
	}
}

// Discovery returns at most 5000 tables; the picker says "N of M" from these.
// They are always written, or a truncated earlier wait would label this one.
func TestTableSelectionWaitingDetails_ForwardsDiscoveryTotals(t *testing.T) {
	result := tableSelectionResultFixture()
	result["total_tables_available"] = 7500
	result["tables_truncated"] = true
	details := tableSelectionWaitingDetails(result, nil)
	if details["total_tables_available"] != 7500 || details["tables_truncated"] != true {
		t.Fatalf("totals = %#v / %#v, want 7500 / true", details["total_tables_available"], details["tables_truncated"])
	}

	// After a JSON round trip the count is a float64.
	result["total_tables_available"] = float64(7500)
	if got := tableSelectionWaitingDetails(result, nil)["total_tables_available"]; got != 7500 {
		t.Fatalf("float64 total = %#v, want 7500", got)
	}

	// Control: a whole list is written as not truncated.
	result["total_tables_available"] = 2
	result["tables_truncated"] = false
	details = tableSelectionWaitingDetails(result, nil)
	if details["total_tables_available"] != 2 || details["tables_truncated"] != false {
		t.Fatalf("whole list totals = %#v / %#v, want 2 / false", details["total_tables_available"], details["tables_truncated"])
	}
}
