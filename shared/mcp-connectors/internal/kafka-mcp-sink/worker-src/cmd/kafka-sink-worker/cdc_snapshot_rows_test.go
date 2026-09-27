package main

// Snapshot reads (op r) used to be counted as inserts, so a re-snapshot of a
// 1,000-row table reported 1,000 new inserts. They now travel as their own
// counts.snapshot_rows; inserts is op c only. read_rows and inserted_rows keep the
// values they always had (they are "what landed", and a snapshot read lands).

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

func TestCDCTableStats_SnapshotRowsApartFromInserts(t *testing.T) {
	sm := &SinkMessage{PipelineID: "p1", ExecutionID: "e1", Table: "public.orders"}

	// 3 creates, 5 updates, 2 deletes, 4 snapshot reads. Before the split the sink
	// reported inserts=7 (3+4): read_rows 14, inserted_rows 12 — those must not move.
	counts := statsCounts(t, buildCDCTableStatsEvent(sm, 3, 5, 2, 1024, 0, 4))
	want := map[string]int64{
		"inserts":       3,
		"snapshot_rows": 4,
		"updates":       5,
		"deletes":       2,
		"total_events":  14,
		"read_rows":     14,
		"inserted_rows": 12,
	}
	for k, w := range want {
		got, ok := counts[k].(int64)
		if !ok || got != w {
			t.Errorf("counts[%q] = %v, want %d", k, counts[k], w)
		}
	}

	// Present and zero on a stream with no snapshot, so the projector can tell
	// "none" from "an older sink that does not report it".
	if got, ok := statsCounts(t, buildCDCTableStatsEvent(sm, 1, 0, 0, 8, 0, 0))["snapshot_rows"]; !ok || got.(int64) != 0 {
		t.Errorf("counts.snapshot_rows = %v (present=%v), want present and 0", got, ok)
	}
}

// Every lane that applies a snapshot read counts it as a snapshot row, never an insert.
func TestCDCCounters_SnapshotReadsAreNotInserts(t *testing.T) {
	lanes := map[string]func(t *testing.T, c cdcTestCounters, sms []*SinkMessage, msgs []kafka.Message){
		"object batcher": func(t *testing.T, c cdcTestCounters, sms []*SinkMessage, msgs []kafka.Message) {
			flushObjectBatch(t, nil, c, sms, msgs)
		},
		"db batcher": func(t *testing.T, c cdcTestCounters, sms []*SinkMessage, msgs []kafka.Message) {
			flushDBBatch(t, nil, c, sms, msgs, dbDestination{})
		},
		"db batcher per-row recovery": func(t *testing.T, c cdcTestCounters, sms []*SinkMessage, msgs []kafka.Message) {
			flushDBBatch(t, nil, c, sms, msgs, dbDestination{rejectMultiRow: true})
		},
		"single event": func(t *testing.T, c cdcTestCounters, sms []*SinkMessage, msgs []kafka.Message) {
			cfg := &WorkerConfig{PipelineID: ackFKPipelineID, SinkMode: "cdc", DestinationConnector: "mongodb", DestinationConfig: map[string]interface{}{}}
			client := &http.Client{Transport: dbDestination{}}
			for i, sm := range sms {
				sm.Data = []map[string]interface{}{{"id": msgs[i].Offset}}
				sm.PK = map[string]interface{}{"id": msgs[i].Offset}
				sm.KeyFields = []string{"id"}
				commit, err := processCDCEvent(context.Background(), newHighWaterTracker(), nil, client, cfg,
					&DDLSupport{Enabled: false, resolved: true}, &kafka.Writer{}, nil, msgs[i], sm, c.metrics,
					c.inserts, c.updates, c.deletes, &sync.Map{}, time.Minute)
				if err != nil || !commit {
					t.Fatalf("snapshot read at offset %d was not applied (commit=%v): %v", msgs[i].Offset, commit, err)
				}
			}
		},
	}
	for name, deliver := range lanes {
		t.Run(name, func(t *testing.T) {
			c := newCDCTestCounters()
			sms, msgs := ledgerTestMessages(0, 4, "r")
			deliver(t, c, sms, msgs)
			if got := loadCounter(c.snapshotRows, "public.orders"); got != 4 {
				t.Errorf("snapshot rows = %d, want 4", got)
			}
			if got := loadCounter(c.inserts, "public.orders"); got != 0 {
				t.Errorf("inserts = %d, want 0: a snapshot read is not an insert", got)
			}
			if got := c.metrics.cdcReads; got != 4 {
				t.Errorf("process-wide reads = %d, want 4", got)
			}
		})
	}
}
