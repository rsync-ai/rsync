package executor

import (
	"encoding/json"
	"testing"
)

func rowsWithIDs(ids ...interface{}) []map[string]interface{} {
	out := make([]map[string]interface{}, 0, len(ids))
	for _, id := range ids {
		out = append(out, map[string]interface{}{"id": id, "v": "x"})
	}
	return out
}

// decode mimics the MCP response path: the result arrives as JSON.
func decode(t *testing.T, s string) map[string]interface{} {
	t.Helper()
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestReadPageCursorOffsetModeIsNotAKeysetCursor(t *testing.T) {
	// warehouse_adapters / clickhouse / databricks / snowflake without a
	// cursor_column: next_cursor is the NEXT OFFSET as a string.
	res := decode(t, `{"paging_mode":"offset","has_more":true,"next_cursor":"10000"}`)
	pc := readPageCursor(res, rowsWithIDs(1, 2))
	if !pc.offsetPaged || pc.next != nil || pc.highWater != nil {
		t.Fatalf("offset page read as keyset: %+v", pc)
	}
	// With the cursor dropped, advancePage steps the offset — page two starts
	// at row 10000 instead of re-reading page one.
	off, _ := advancePage(0, 0, 10000, pc.next != nil)
	if off != 10000 {
		t.Fatalf("offset did not advance: %d", off)
	}
}

func TestReadPageCursorKeyset(t *testing.T) {
	res := decode(t, `{"paging_mode":"keyset","cursor_column":"id","next_cursor":42}`)
	pc := readPageCursor(res, rowsWithIDs(41.0, 42.0))
	if pc.offsetPaged || pc.next != float64(42) || pc.highWater != float64(42) {
		t.Fatalf("%+v", pc)
	}
	// Connectors that report no paging_mode at all (SaaS cursors, older
	// connectors) keep the pre-existing behavior: next_cursor is the cursor.
	pc = readPageCursor(decode(t, `{"next_cursor":"opaque-abc"}`), nil)
	if pc.next != "opaque-abc" || pc.offsetPaged {
		t.Fatalf("%+v", pc)
	}
}

func TestReadPageCursorFinalPageWithoutNextCursorStillReportsHighWater(t *testing.T) {
	// A keyset connector that emits next_cursor only while has_more (redshift,
	// mongodb before this fix): the final short page must still move the PK
	// high-water to its last row, or the next Resume re-copies that page.
	res := decode(t, `{"paging_mode":"keyset","cursor_column":"id","has_more":false}`)
	pc := readPageCursor(res, rowsWithIDs(float64(7), float64(9)))
	if pc.next != nil || pc.highWater != float64(9) {
		t.Fatalf("%+v", pc)
	}
	// No cursor_column named: nothing is guessed.
	pc = readPageCursor(decode(t, `{"paging_mode":"keyset"}`), rowsWithIDs(float64(9)))
	if pc.highWater != nil {
		t.Fatalf("guessed a key column: %+v", pc)
	}
}

func TestDropRowsAtOrBelowSinceCursor(t *testing.T) {
	keyset := decode(t, `{"paging_mode":"keyset","cursor_column":"id"}`)
	rows := rowsWithIDs(float64(3), float64(4), float64(5), float64(6))

	kept, dropped := dropRowsAtOrBelowSinceCursor(rows, keyset, float64(4), "")
	if dropped != 2 || len(kept) != 2 || kept[0]["id"] != float64(5) {
		t.Fatalf("kept=%v dropped=%d", kept, dropped)
	}
	// The caller's page is untouched (pagination counts SOURCE rows).
	if len(rows) != 4 {
		t.Fatalf("input mutated: %v", rows)
	}
	// json.Number and int shapes compare the same.
	if _, d := dropRowsAtOrBelowSinceCursor(rows, keyset, json.Number("4"), ""); d != 2 {
		t.Fatalf("json.Number since: dropped %d", d)
	}

	cases := []struct {
		name             string
		res              map[string]interface{}
		since            interface{}
		incrementalSince string
		rows             []map[string]interface{}
	}{
		{"no since_cursor", keyset, nil, "", rows},
		// updated_at > X OR pk > since: an UPDATED old row legitimately returns.
		{"incremental since present", keyset, float64(4), "2026-09-26T00:00:00Z", rows},
		{"offset paging", decode(t, `{"paging_mode":"offset","cursor_column":"id"}`), float64(4), "", rows},
		{"no paging_mode", decode(t, `{"cursor_column":"id"}`), float64(4), "", rows},
		{"no cursor_column", decode(t, `{"paging_mode":"keyset"}`), float64(4), "", rows},
		// String keys: the source's collation decides order, not Go's.
		{"string since", keyset, "user-4", "", rowsWithIDs("user-3", "user-5")},
		{"extjson since (mongodb ObjectId)", keyset, `{"_id": {"$oid": "65ab"}}`, "", rows},
		// Past 2^53 two keys can decode to the same float64.
		{"inexact since", keyset, float64(1 << 60), "", rows},
		{"fractional since", keyset, 4.5, "", rows},
	}
	for _, c := range cases {
		if kept, d := dropRowsAtOrBelowSinceCursor(c.rows, c.res, c.since, c.incrementalSince); d != 0 || len(kept) != len(c.rows) {
			t.Errorf("%s: dropped %d rows, want none", c.name, d)
		}
	}

	// A row whose key cannot be compared is KEPT, never dropped.
	mixed := []map[string]interface{}{{"id": "abc"}, {"id": nil}, {"v": 1}, {"id": float64(2)}, {"id": float64(1 << 60)}}
	kept, dropped = dropRowsAtOrBelowSinceCursor(mixed, keyset, float64(4), "")
	if dropped != 1 || len(kept) != 4 {
		t.Fatalf("kept=%v dropped=%d", kept, dropped)
	}
}
