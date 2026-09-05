package main

import "testing"

func TestObjectFileRollLimits(t *testing.T) {
	// Nil / empty config → rolling off.
	if r, b := objectFileRollLimits(nil); r != 0 || b != 0 {
		t.Errorf("nil cfg = (%d,%d), want (0,0)", r, b)
	}
	if r, b := objectFileRollLimits(map[string]interface{}{}); r != 0 || b != 0 {
		t.Errorf("empty cfg = (%d,%d), want (0,0)", r, b)
	}

	// max_file_rows arrives as float64 (JSON), int, or string — all parse.
	for _, v := range []interface{}{float64(500), 500, "500"} {
		if r, _ := objectFileRollLimits(map[string]interface{}{"max_file_rows": v}); r != 500 {
			t.Errorf("max_file_rows=%v(%T) → %d, want 500", v, v, r)
		}
	}

	// max_file_mb is converted to bytes (MB → MiB).
	if _, b := objectFileRollLimits(map[string]interface{}{"max_file_mb": float64(128)}); b != 128*1024*1024 {
		t.Errorf("max_file_mb=128 → %d bytes, want %d", b, 128*1024*1024)
	}

	// Both together.
	r, b := objectFileRollLimits(map[string]interface{}{"max_file_rows": 1000, "max_file_mb": 256})
	if r != 1000 || b != 256*1024*1024 {
		t.Errorf("both = (%d,%d), want (1000,%d)", r, b, 256*1024*1024)
	}

	// Zero / negative → off (treated as unset).
	if r, bb := objectFileRollLimits(map[string]interface{}{"max_file_rows": 0, "max_file_mb": -5}); r != 0 || bb != 0 {
		t.Errorf("zero/neg = (%d,%d), want (0,0)", r, bb)
	}
}

func TestChunkRowsForFileRolling(t *testing.T) {
	mk := func(n int) []map[string]interface{} {
		out := make([]map[string]interface{}, n)
		for i := range out {
			out[i] = map[string]interface{}{"id": i, "name": "row"}
		}
		return out
	}

	// Rolling off → a single chunk holding every row (legacy behavior).
	rows := mk(5)
	if c := chunkRowsForFileRolling(rows, 0, 0); len(c) != 1 || len(c[0]) != 5 {
		t.Fatalf("rolling off → %d chunks (want 1 of 5)", len(c))
	}

	// Empty input → a single (empty) chunk, never nil-deref downstream.
	if c := chunkRowsForFileRolling(nil, 100, 0); len(c) != 1 {
		t.Fatalf("empty input → %d chunks, want 1", len(c))
	}

	// max_file_rows=2 over 5 rows → 2,2,1 and no row lost or duplicated.
	chunks := chunkRowsForFileRolling(mk(5), 2, 0)
	if len(chunks) != 3 || len(chunks[0]) != 2 || len(chunks[1]) != 2 || len(chunks[2]) != 1 {
		t.Fatalf("maxRows=2 → sizes %v, want [2 2 1]", chunkSizes(chunks))
	}
	assertNoRowLoss(t, mk(5), chunks)

	// max_file_mb cap (bytes): a tiny byte budget rolls roughly every row.
	// Each row marshals to >10 bytes, so maxBytes=20 yields multiple chunks.
	byteChunks := chunkRowsForFileRolling(mk(4), 0, 20)
	if len(byteChunks) < 2 {
		t.Errorf("maxBytes=20 over 4 rows → %d chunks, want >= 2", len(byteChunks))
	}
	assertNoRowLoss(t, mk(4), byteChunks)

	// A single row larger than maxBytes still occupies its own chunk (never split).
	big := []map[string]interface{}{{"blob": string(make([]byte, 200))}}
	if c := chunkRowsForFileRolling(big, 0, 10); len(c) != 1 || len(c[0]) != 1 {
		t.Errorf("oversized single row → %d chunks, want 1 of 1", len(c))
	}
}

func chunkSizes(chunks [][]map[string]interface{}) []int {
	out := make([]int, len(chunks))
	for i, c := range chunks {
		out[i] = len(c)
	}
	return out
}

func assertNoRowLoss(t *testing.T, in []map[string]interface{}, chunks [][]map[string]interface{}) {
	t.Helper()
	total := 0
	for _, c := range chunks {
		total += len(c)
	}
	if total != len(in) {
		t.Errorf("file rolling lost rows: chunked=%d input=%d", total, len(in))
	}
}
