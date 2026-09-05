package main

import (
	"strings"
	"testing"

	"github.com/segmentio/kafka-go"
)

// Group D: cdc_include_op controls whether the op (I/U/D) field is emitted in the
// bronze envelope. Default true (current behavior); false → append-only after-image.
func TestBuildBronzeCDCEvent_IncludeOp(t *testing.T) {
	msg := kafka.Message{Topic: "dbserver.mydb.users", Partition: 0, Offset: 1}
	sm := &SinkMessage{
		Table: "public.users",
		CDCOp: "c",
		PK:    map[string]interface{}{"id": 1},
		After: map[string]interface{}{"id": 1, "name": "Alice"},
	}

	// Default (no config) → op present.
	evt, _, err := buildBronzeCDCEvent(&WorkerConfig{}, msg, sm, "upsert", "jsonl")
	if err != nil {
		t.Fatalf("default: unexpected error: %v", err)
	}
	if evt["op"] != "I" {
		t.Errorf("default cdc_include_op: expected op=I, got %v", evt["op"])
	}

	// cdc_include_op=false → op omitted, after-image still present.
	cfg := &WorkerConfig{DestinationConfig: map[string]interface{}{"cdc_include_op": false}}
	evt, _, err = buildBronzeCDCEvent(cfg, msg, sm, "upsert", "jsonl")
	if err != nil {
		t.Fatalf("include_op=false: unexpected error: %v", err)
	}
	if _, ok := evt["op"]; ok {
		t.Errorf("cdc_include_op=false: expected op omitted, got %v", evt["op"])
	}
	if evt["after"] == nil {
		t.Errorf("cdc_include_op=false: after-image should still be present")
	}
}

// Group D: cdc_partition_by_op prepends an op=<I|U|D>/ segment to the CDC partition
// path, composing with partition_by. Off by default.
func TestCDCPartitionContext_PartitionByOp(t *testing.T) {
	insert := &SinkMessage{CDCOp: "c", After: map[string]interface{}{"region": "us"}}
	del := &SinkMessage{CDCOp: "d"}

	// Off (default) → no op segment.
	if seg, _ := cdcPartitionContext(map[string]interface{}{}, insert); seg != "" {
		t.Errorf("default: expected no partSegs, got %q", seg)
	}

	// On, no partition_by → just the op segment.
	seg, _ := cdcPartitionContext(map[string]interface{}{"cdc_partition_by_op": true}, insert)
	if seg != "op=I/" {
		t.Errorf("partition_by_op insert: expected op=I/, got %q", seg)
	}
	// Delete → op=D/ (delete has no after-image; op still derives from CDCOp).
	seg, _ = cdcPartitionContext(map[string]interface{}{"cdc_partition_by_op": true}, del)
	if seg != "op=D/" {
		t.Errorf("partition_by_op delete: expected op=D/, got %q", seg)
	}

	// On + partition_by → op is the OUTERMOST segment, then the column tuple.
	seg, _ = cdcPartitionContext(map[string]interface{}{
		"cdc_partition_by_op": true,
		"partition_by":        "region",
	}, insert)
	if seg != "op=I/region=us/" {
		t.Errorf("partition_by_op + partition_by: expected op=I/region=us/, got %q", seg)
	}
	if !strings.HasPrefix(seg, "op=") {
		t.Errorf("op segment must be outermost, got %q", seg)
	}
}
