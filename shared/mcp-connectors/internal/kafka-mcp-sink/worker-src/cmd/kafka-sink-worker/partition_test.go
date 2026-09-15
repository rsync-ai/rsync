package main

import (
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

// fixedTS is 2026-01-25T14:30:00Z, used for deterministic time-bucket assertions.
var fixedTS = time.Date(2026, 1, 25, 14, 30, 0, 0, time.UTC).UnixMilli()

func TestParsePartitionColumns(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"", nil},
		{"   ", nil},
		{"region", []string{"region"}},
		{"region,tier", []string{"region", "tier"}},
		{" region , tier ", []string{"region", "tier"}},
		{"region,,tier,", []string{"region", "tier"}}, // empty tokens dropped
	}
	for _, c := range cases {
		got := parsePartitionColumns(c.in)
		if len(got) != len(c.want) {
			t.Fatalf("parsePartitionColumns(%q) = %v, want %v", c.in, got, c.want)
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Fatalf("parsePartitionColumns(%q)[%d] = %q, want %q", c.in, i, got[i], c.want[i])
			}
		}
	}
}

func TestHivePartitionSegments(t *testing.T) {
	row := map[string]interface{}{
		"region": "us-east",
		"tier":   "gold",
		"count":  42,
		"weird":  "a/b/c", // must be sanitized
	}

	if got := hivePartitionSegments(row, nil); got != "" {
		t.Errorf("no columns should yield empty, got %q", got)
	}

	if got := hivePartitionSegments(row, []string{"region", "tier"}); got != "region=us-east/tier=gold/" {
		t.Errorf("got %q", got)
	}

	// Non-string value is stringified.
	if got := hivePartitionSegments(row, []string{"count"}); got != "count=42/" {
		t.Errorf("got %q", got)
	}

	// Slashes in a value are sanitized so they never split the path.
	if got := hivePartitionSegments(row, []string{"weird"}); got != "weird=a_b_c/" {
		t.Errorf("got %q", got)
	}

	// Missing / nil row value -> Hive default-partition sentinel.
	if got := hivePartitionSegments(row, []string{"missing"}); got != "missing=__HIVE_DEFAULT_PARTITION__/" {
		t.Errorf("got %q", got)
	}
	if got := hivePartitionSegments(nil, []string{"region"}); got != "region=__HIVE_DEFAULT_PARTITION__/" {
		t.Errorf("nil row got %q", got)
	}
}

func TestTimePartitionSegment(t *testing.T) {
	cases := []struct {
		gran string
		want string
	}{
		// Unconfigured stays on the legacy plain folder, byte-identical to the keys
		// written before Hive segments existed — an already-registered table must not
		// have its layout move underneath it.
		{"", "2026-01-25"},
		{"none", "2026-01-25"},
		// An explicit granularity opts into the Hive layout the option has always been
		// documented to produce.
		{"day", "dt=2026-01-25"},
		{"DAY", "dt=2026-01-25"}, // case-insensitive
		{"hour", "dt=2026-01-25/hour=14"},
		{"month", "dt=2026-01"},
	}
	for _, c := range cases {
		if got := timePartitionSegment(fixedTS, c.gran); got != c.want {
			t.Errorf("timePartitionSegment(_, %q) = %q, want %q", c.gran, got, c.want)
		}
	}

	// tsMs<=0 falls back to "now" — assert only the structural shape, not the value.
	if got := timePartitionSegment(0, "day"); len(got) != len("dt=2006-01-02") {
		t.Errorf("zero ts fallback malformed: %q", got)
	}
	if got := timePartitionSegment(0, "none"); len(got) != len("2006-01-02") {
		t.Errorf("zero ts fallback malformed for none: %q", got)
	}
}

// The reason the change above exists, asserted as a property rather than a literal:
// BigQuery's hive_partitioning_mode (and Athena's partition projection) recognise a
// partition only from a literal "key=value" path segment. A bare "2026-01-25" folder is
// read as another level of the table path, so the column does not exist and no query can
// prune on it. Every configured granularity must therefore yield key=value segments, and
// each value must round-trip back to the timestamp it came from.
func TestConfiguredGranularityIsHiveParseable(t *testing.T) {
	for _, gran := range []string{"day", "hour", "month"} {
		seg := timePartitionSegment(fixedTS, gran)
		for _, part := range strings.Split(seg, "/") {
			k, v, ok := strings.Cut(part, "=")
			if !ok || k == "" || v == "" {
				t.Errorf("granularity %q produced segment %q; %q is not key=value, so BigQuery reads it as a path level and not a partition column", gran, seg, part)
			}
		}
	}

	// Control: the whole point of key=value is that a bare date FAILS this check. Without
	// this the assertion above would look equally satisfied by a format that never changed.
	if _, _, ok := strings.Cut(timePartitionSegment(fixedTS, "none"), "="); ok {
		t.Error("the 'none' segment now contains '='; the control is no longer discriminating and the test above proves nothing")
	}
}

// The end-to-end shape a BigQuery external table is pointed at: with a granularity set, a
// CDC object key must carry dt= between the table prefix and the leaf.
func TestCDCObjectKeyIsHivePartitionedWhenGranularitySet(t *testing.T) {
	got := cdcObjectKey("bronze", "shop", "orders", timePartitionSegment(fixedTS, "day"), "", fixedTS, 0, 7, 7, "jsonl", "none")
	want := "bronze/shop/orders/dt=2026-01-25/20260125-143000000-7.jsonl"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestCDCObjectKeyWithPartition(t *testing.T) {
	// Hive partition segments are inserted between the table and the date folder;
	// a non-zero Kafka partition folds into the leaf (-p2), then the offset tiebreaker.
	got := cdcObjectKey("prefix", "public", "users", "2026-01-25/14", "region=us-east/tier=gold/", fixedTS, 2, 5, 9, "jsonl", "none")
	want := "prefix/public/users/region=us-east/tier=gold/2026-01-25/14/20260125-143000000-p2-5.jsonl"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestCDCObjectKeyNoPartitionCols(t *testing.T) {
	// Default: no partSegs, plain date folder, partition 0 → DMS-style timestamp leaf.
	got := cdcObjectKey("prefix", "public", "users", "2026-01-25", "", fixedTS, 0, 1, 1, "jsonl", "none")
	want := "prefix/public/users/2026-01-25/20260125-143000000-1.jsonl"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestCDCPartitionContext(t *testing.T) {
	// Configured partition_by + granularity, values from the after-image.
	destCfg := map[string]interface{}{
		"partition_by":               "region,tier",
		"partition_time_granularity": "hour",
	}
	sm := &SinkMessage{
		After:    map[string]interface{}{"region": "eu", "tier": "silver"},
		SourceTS: fixedTS,
	}
	partSegs, timeSeg := cdcPartitionContext(destCfg, sm)
	if partSegs != "region=eu/tier=silver/" {
		t.Errorf("partSegs = %q", partSegs)
	}
	// Explicitly configured granularity -> Hive key=value segments.
	if timeSeg != "dt=2026-01-25/hour=14" {
		t.Errorf("timeSeg = %q", timeSeg)
	}

	// Delete event (no after-image) -> default partition; timestamp still buckets.
	del := &SinkMessage{After: nil, CDCOp: "d", SourceTS: fixedTS}
	partSegs, timeSeg = cdcPartitionContext(destCfg, del)
	if partSegs != "region=__HIVE_DEFAULT_PARTITION__/tier=__HIVE_DEFAULT_PARTITION__/" {
		t.Errorf("delete partSegs = %q", partSegs)
	}
	if timeSeg != "dt=2026-01-25/hour=14" {
		t.Errorf("delete timeSeg = %q", timeSeg)
	}

	// No partition config -> empty partSegs + plain day-level date bucket.
	partSegs, timeSeg = cdcPartitionContext(map[string]interface{}{}, sm)
	if partSegs != "" {
		t.Errorf("unconfigured partSegs should be empty, got %q", partSegs)
	}
	if timeSeg != "2026-01-25" {
		t.Errorf("unconfigured timeSeg = %q", timeSeg)
	}

	// Falls back to Data[0] when After is unset (snapshot/insert variants).
	smData := &SinkMessage{
		Data:     []map[string]interface{}{{"region": "ap", "tier": "bronze"}},
		SourceTS: fixedTS,
	}
	partSegs, _ = cdcPartitionContext(destCfg, smData)
	if partSegs != "region=ap/tier=bronze/" {
		t.Errorf("Data[0] partSegs = %q", partSegs)
	}
}

func TestCDCPartitionContextDropsMissingColumn(t *testing.T) {
	// partition_by naming a column that isn't in the row schema (the classic
	// event_timestamp misconfig) must DROP that column rather than bucket every row
	// into __HIVE_DEFAULT_PARTITION__. A present column is still partitioned.
	destCfg := map[string]interface{}{"partition_by": "region,event_timestamp"}
	sm := &SinkMessage{After: map[string]interface{}{"region": "eu"}, SourceTS: fixedTS}
	partSegs, _ := cdcPartitionContext(destCfg, sm)
	if partSegs != "region=eu/" {
		t.Errorf("missing column should be dropped, got partSegs = %q", partSegs)
	}

	// A present-but-null value is a legitimate null → keep the sentinel.
	sm2 := &SinkMessage{After: map[string]interface{}{"region": nil}, SourceTS: fixedTS}
	partSegs2, _ := cdcPartitionContext(map[string]interface{}{"partition_by": "region"}, sm2)
	if partSegs2 != "region=__HIVE_DEFAULT_PARTITION__/" {
		t.Errorf("present-but-null should keep sentinel, got %q", partSegs2)
	}

	// filterPresentPartitionColumns unit behavior: empty row → no judgement (pass-through).
	if got := filterPresentPartitionColumns(nil, []string{"a", "b"}, nil); len(got) != 2 {
		t.Errorf("empty row should pass cols through, got %v", got)
	}
}

func TestBatchKeySeparatesByPartition(t *testing.T) {
	b := &cdcObjectBatcher{}
	msg := kafka.Message{Topic: "t", Partition: 0}
	sm := &SinkMessage{Table: "users"}

	base := b.batchKey(msg, sm, "jsonl", "none", "region=us/", "dt=2026-01-25")
	same := b.batchKey(msg, sm, "jsonl", "none", "region=us/", "dt=2026-01-25")
	if base != same {
		t.Fatalf("identical inputs must yield identical keys: %q vs %q", base, same)
	}

	// Different partition-column value -> different batch (so it lands in a different object).
	if other := b.batchKey(msg, sm, "jsonl", "none", "region=eu/", "dt=2026-01-25"); other == base {
		t.Errorf("different partSegs must produce different batch key")
	}
	// Different time bucket -> different batch.
	if other := b.batchKey(msg, sm, "jsonl", "none", "region=us/", "dt=2026-01-26"); other == base {
		t.Errorf("different timeSeg must produce different batch key")
	}
}
