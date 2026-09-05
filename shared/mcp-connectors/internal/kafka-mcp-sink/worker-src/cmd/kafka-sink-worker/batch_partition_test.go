package main

import "testing"

func TestPartKeyWithPartSegs(t *testing.T) {
	// partSegs="" + partSuffix="" reproduces the legacy batch layout (back-compat).
	got := partKey("prefix", "ds", "public", "users", "", "2026-01-25", 7, "", "jsonl")
	want := "prefix/ds/public/users/dt=2026-01-25/part-000007.jsonl"
	if got != want {
		t.Errorf("legacy partKey = %q, want %q", got, want)
	}

	// Hive segments are inserted between the table and the dt bucket.
	got = partKey("prefix", "ds", "public", "users", "region=us/tier=gold/", "2026-01-25", 7, "", "jsonl")
	want = "prefix/ds/public/users/region=us/tier=gold/dt=2026-01-25/part-000007.jsonl"
	if got != want {
		t.Errorf("partitioned partKey = %q, want %q", got, want)
	}

	// File-rolling chunk suffix appends "-NNNN" to the part name (max_file_* on).
	got = partKey("prefix", "ds", "public", "users", "", "2026-01-25", 7, "0003", "jsonl")
	want = "prefix/ds/public/users/dt=2026-01-25/part-000007-0003.jsonl"
	if got != want {
		t.Errorf("rolled partKey = %q, want %q", got, want)
	}
}

func TestSplitRowsForObjectPartition(t *testing.T) {
	rows := []map[string]interface{}{
		{"id": 1, "region": "us", "tier": "gold"},
		{"id": 2, "region": "eu", "tier": "gold"},
		{"id": 3, "region": "us", "tier": "gold"},
		{"id": 4, "region": "us"}, // missing tier → default-partition sentinel
	}

	// No partition_by → a single group with all rows and empty partSegs.
	g := splitRowsForObjectPartition(map[string]interface{}{}, rows)
	if len(g) != 1 || g[0].partSegs != "" || len(g[0].rows) != 4 {
		t.Fatalf("unpartitioned: expected 1 group of 4 with empty partSegs, got %+v", g)
	}

	// partition_by=region,tier → grouped by tuple, first-seen order preserved.
	g = splitRowsForObjectPartition(map[string]interface{}{"partition_by": "region,tier"}, rows)
	if len(g) != 3 {
		t.Fatalf("expected 3 partition groups, got %d (%+v)", len(g), g)
	}
	// Group 1: region=us/tier=gold/ (rows 1 and 3), first seen.
	if g[0].partSegs != "region=us/tier=gold/" || len(g[0].rows) != 2 {
		t.Errorf("group[0] = %q with %d rows, want region=us/tier=gold/ with 2", g[0].partSegs, len(g[0].rows))
	}
	// Group 2: region=eu/tier=gold/ (row 2).
	if g[1].partSegs != "region=eu/tier=gold/" || len(g[1].rows) != 1 {
		t.Errorf("group[1] = %q with %d rows, want region=eu/tier=gold/ with 1", g[1].partSegs, len(g[1].rows))
	}
	// Group 3: row 4 has no tier → default sentinel for the missing column.
	if g[2].partSegs != "region=us/tier=__HIVE_DEFAULT_PARTITION__/" || len(g[2].rows) != 1 {
		t.Errorf("group[2] = %q with %d rows, want region=us/tier=__HIVE_DEFAULT_PARTITION__/ with 1", g[2].partSegs, len(g[2].rows))
	}

	// Row counts across groups must sum to the input (no rows dropped/duplicated).
	total := 0
	for _, grp := range g {
		total += len(grp.rows)
	}
	if total != len(rows) {
		t.Errorf("partition split lost rows: sum=%d input=%d", total, len(rows))
	}
}
