package main

// CDC-B1: a CDC record the destination genuinely cannot write is parked in the DLQ,
// its offset is committed, and the stream continues (flushBatchPerRow). That is correct
// runtime behavior, but the count used to die in sink metrics — so the row was never
// counted as captured AND never reported as lost, and captured-vs-applied (the one pair
// that exists to detect loss) reconciled perfectly across the gap with Failed at 0.
//
// These tests pin the contract that closes it: the per-table DLQ count rides the
// TABLE_STATS event, and a table shedding rows reports degraded, not running.

import (
	"testing"
)

func statsCounts(t *testing.T, ev map[string]interface{}) map[string]interface{} {
	t.Helper()
	meta, ok := ev["metadata"].(map[string]interface{})
	if !ok {
		t.Fatalf("event has no metadata map: %#v", ev)
	}
	counts, ok := meta["counts"].(map[string]interface{})
	if !ok {
		t.Fatalf("metadata has no counts map: %#v", meta)
	}
	return counts
}

func statsStatus(t *testing.T, ev map[string]interface{}) string {
	t.Helper()
	meta, ok := ev["metadata"].(map[string]interface{})
	if !ok {
		t.Fatalf("event has no metadata map: %#v", ev)
	}
	s, _ := meta["status"].(string)
	return s
}

func TestCDCTableStats_DLQRowsReported(t *testing.T) {
	sm := &SinkMessage{PipelineID: "p1", ExecutionID: "e1", Table: "public.orders"}

	// Clean stream: dlq_rows is present and zero. Present-and-zero matters — an omitted
	// field makes "nothing lost" indistinguishable from "this sink can't tell you".
	clean := buildCDCTableStatsEvent(sm, 10, 5, 2, 1024, 0)
	if got, ok := statsCounts(t, clean)["dlq_rows"]; !ok {
		t.Error("counts.dlq_rows missing on a clean stream; projector cannot clear a stale value")
	} else if got.(int64) != 0 {
		t.Errorf("counts.dlq_rows = %v, want 0", got)
	}
	if got := statsStatus(t, clean); got != "running" {
		t.Errorf("clean stream status = %q, want %q", got, "running")
	}

	// Shedding rows: the count is carried and the table is NOT reported as running.
	shed := buildCDCTableStatsEvent(sm, 10, 5, 2, 1024, 3)
	if got := statsCounts(t, shed)["dlq_rows"].(int64); got != 3 {
		t.Errorf("counts.dlq_rows = %d, want 3", got)
	}
	if got := statsStatus(t, shed); got != "degraded" {
		t.Errorf("status with 3 dropped rows = %q, want %q — reporting a shedding table as running is the defect", got, "degraded")
	}
}

func TestCDCTableStats_DLQRowsNotFoldedIntoLandedCounts(t *testing.T) {
	sm := &SinkMessage{PipelineID: "p1", ExecutionID: "e1", Table: "public.orders"}

	clean := statsCounts(t, buildCDCTableStatsEvent(sm, 10, 5, 2, 1024, 0))
	shed := statsCounts(t, buildCDCTableStatsEvent(sm, 10, 5, 2, 1024, 7))

	// read_rows/inserted_rows/total_events mean "what landed". Folding a dropped row
	// into any of them would restore the exact reconciliation that hid the loss.
	for _, k := range []string{"read_rows", "inserted_rows", "total_events", "inserts", "updates", "deletes"} {
		if clean[k] != shed[k] {
			t.Errorf("counts[%q] changed with dlq_rows (%v → %v); landed-row counters must not absorb dropped rows", k, clean[k], shed[k])
		}
	}
}

func TestDLQTableOf_AttributesLossToATable(t *testing.T) {
	batch := &cdcDBBatch{
		targetTable: "orders",
		sms: []*SinkMessage{
			{Table: "public.orders"},
			nil,            // sm unavailable for this row
			{Table: "   "}, // blank source table
		},
	}
	if got := dlqTableOf(batch, 0); got != "public.orders" {
		t.Errorf("dlqTableOf(0) = %q, want %q", got, "public.orders")
	}
	// Falls back to the destination-side name rather than "" — an unattributed loss is
	// a loss no per-table surface can report.
	if got := dlqTableOf(batch, 1); got != "orders" {
		t.Errorf("dlqTableOf(1) = %q, want fallback %q", got, "orders")
	}
	if got := dlqTableOf(batch, 2); got != "orders" {
		t.Errorf("dlqTableOf(2) = %q, want fallback %q", got, "orders")
	}
	if got := dlqTableOf(batch, 99); got != "orders" {
		t.Errorf("dlqTableOf(out-of-range) = %q, want fallback %q", got, "orders")
	}
}
