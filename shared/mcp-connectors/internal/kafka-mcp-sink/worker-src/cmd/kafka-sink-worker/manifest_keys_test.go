package main

// The EOF _MANIFEST.json lists every object the execution wrote. A Resume that
// re-reads the rows of a dead-lettered batch runs under the same execution id
// and rewrites the batches that had landed, so the same key is written twice.
// On GKE rsync-v016 (2026-09-27) the manifest then listed part-010000.parquet
// twice and said total_rows 14000 for a folder holding 12,000 rows.

import (
	"reflect"
	"testing"
)

func TestRecordWritesListsARewrittenObjectOnce(t *testing.T) {
	st := &tableWriteState{}
	// First attempt: the 10,000-row batch was dead-lettered, the 2,000-row one landed.
	st.recordWrites([]string{"t/part-010000.parquet"}, []int64{2000})
	// Resume re-reads everything under the same execution id.
	st.recordWrites([]string{"t/part-000000.parquet"}, []int64{10000})
	st.recordWrites([]string{"t/part-010000.parquet"}, []int64{2000})

	wantKeys := []string{"t/part-010000.parquet", "t/part-000000.parquet"}
	if !reflect.DeepEqual(st.keys, wantKeys) {
		t.Fatalf("keys = %v, want %v", st.keys, wantKeys)
	}
	if got := sumInt64(st.rowCounts); got != 12000 {
		t.Fatalf("total rows = %d, want 12000 (rowCounts %v)", got, st.rowCounts)
	}
}

func TestRecordWritesTakesTheNewerCountForARewrittenObject(t *testing.T) {
	st := &tableWriteState{}
	st.recordWrites([]string{"a", "b"}, []int64{5, 7})
	st.recordWrites([]string{"b"}, []int64{9})
	if !reflect.DeepEqual(st.keys, []string{"a", "b"}) || !reflect.DeepEqual(st.rowCounts, []int64{5, 9}) {
		t.Fatalf("keys=%v counts=%v, want [a b] [5 9]", st.keys, st.rowCounts)
	}
}

func TestRecordWritesKeepsDistinctPartitionFiles(t *testing.T) {
	// partition_by: one batch writes several part-files; all must be listed.
	st := &tableWriteState{}
	st.recordWrites([]string{"p=1/x", "p=2/x", "p=3/x"}, []int64{1, 2, 3})
	if len(st.keys) != 3 || sumInt64(st.rowCounts) != 6 {
		t.Fatalf("keys=%v counts=%v", st.keys, st.rowCounts)
	}
}
