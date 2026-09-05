package kafka

import "testing"

// TestComputeConsumerGroupLag_ScopesToCommittedTopics locks the fix for the cluster-wide
// phantom-lag bug. GetConsumerGroupLag used to add EVERY cluster topic to the offset-fetch
// request and treat a never-committed offset (-1) as a full-topic backlog, so a single
// per-pipeline sink group reported the sum of every OTHER pipeline's topic size as its own
// "lag" (the observed 14,851,173-event phantom on pipeline 2cb685ed). A per-pipeline sink
// consumer group only commits offsets for the topics it actually consumes, so a
// never-committed (offset < 0) partition belongs to some other pipeline and must contribute
// ZERO lag — and its topic must not even appear in the per-topic breakdown.
func TestComputeConsumerGroupLag_ScopesToCommittedTopics(t *testing.T) {
	// Two topics this sink actually consumes (committed offsets) + one huge foreign topic
	// the group never committed to (offset -1). Names cover BOTH source families:
	//   dbz.pipeline_test.orders → MySQL-style (db.table)
	//   dbz.public.customers     → PostgreSQL-style (schema.table)
	committed := map[string]map[int32]int64{
		"dbz.pipeline_test.orders": {0: 100, 1: 40}, // own (MySQL-style): lag 30 + 10 = 40
		"dbz.public.customers":     {0: 50},         // own (PG-style): fully drained → lag 0
		"foreign.other_pipeline":   {0: -1},         // never committed → must be ignored
	}
	logEnd := map[string]map[int32]int64{
		"dbz.pipeline_test.orders": {0: 130, 1: 50}, // 130-100=30, 50-40=10
		"dbz.public.customers":     {0: 50},         // 50-50=0
		"foreign.other_pipeline":   {0: 9_000_000},  // the phantom the old code summed
	}

	got := computeConsumerGroupLag(committed, logEnd)

	// The foreign topic must be entirely absent — not present with 0, absent.
	if _, ok := got["foreign.other_pipeline"]; ok {
		t.Errorf("foreign never-committed topic leaked into lag map: %v", got)
	}
	// Own topics: exact per-topic lag, including the legitimately-zero one.
	if got["dbz.pipeline_test.orders"] != 40 {
		t.Errorf("orders lag = %d, want 40", got["dbz.pipeline_test.orders"])
	}
	if v, ok := got["dbz.public.customers"]; !ok || v != 0 {
		t.Errorf("customers lag = %d (present=%v), want 0 present=true", v, ok)
	}
	// Total (what the sentinel sums) must be the own-topics backlog only, NOT the phantom.
	var total int64
	for _, v := range got {
		total += v
	}
	if total != 40 {
		t.Fatalf("total lag = %d, want 40 (the old cluster-wide bug would report 9_000_040)", total)
	}
}

// TestComputeConsumerGroupLag_GuardsMissingAndRacyOffsets locks two invariants: a committed
// topic stays present in the map even when its computed lag is 0, and a high-watermark that
// is missing (GetOffset failed) or below the committed offset (a racy read) clamps to 0
// rather than producing negative lag.
func TestComputeConsumerGroupLag_GuardsMissingAndRacyOffsets(t *testing.T) {
	committed := map[string]map[int32]int64{
		"own.topic": {0: 100, 1: 200},
	}
	logEnd := map[string]map[int32]int64{
		"own.topic": {0: 90}, // p0 high-watermark BELOW committed (racy) → 0; p1 missing → 0
	}

	got := computeConsumerGroupLag(committed, logEnd)

	if v, ok := got["own.topic"]; !ok {
		t.Errorf("a committed topic must remain present even at 0 lag")
	} else if v != 0 {
		t.Errorf("racy/missing offsets should clamp to 0 lag, got %d", v)
	}
}
