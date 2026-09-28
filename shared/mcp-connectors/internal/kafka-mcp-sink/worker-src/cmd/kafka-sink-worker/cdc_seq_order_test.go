package main

import (
	"encoding/json"
	"testing"

	"github.com/segmentio/kafka-go"
)

// KI-CDC-SEQ-UNORDERED-FOR-MONGODB. Bug class: "the ordering token ignores a
// source family's own tie-breaker", so two changes that share a source
// timestamp get the same _rsync_cdc_seq and MAX(_rsync_cdc_seq) cannot pick the
// later one. Every source family the sink parses is run through the real parser
// (parseSinkMessage) with two changes at the SAME source.ts_ms, in commit order;
// the second must sort strictly after the first. The per-family want values pin
// the LSN half for PostgreSQL / MySQL / SQL Server so a MongoDB fix cannot move
// them. A new source family gets the check by adding a row here.
func TestCDCSeq_OrdersTwoChangesInOneSourceTimestamp_EveryFamily(t *testing.T) {
	const ts = int64(1704067200000) // MongoDB's source.ts_ms is whole seconds

	type change struct {
		source map[string]interface{}
		after  interface{}
		want   int64 // expected sm.LSN
	}
	families := []struct {
		name  string
		topic string
		first change
		later change
	}{
		{
			name:  "postgresql lsn",
			topic: "srv.public.t",
			first: change{map[string]interface{}{"connector": "postgresql", "db": "d", "schema": "public", "table": "t", "ts_ms": ts, "lsn": 24023128, "txId": 555}, map[string]interface{}{"id": 1, "v": "a"}, 24023128},
			later: change{map[string]interface{}{"connector": "postgresql", "db": "d", "schema": "public", "table": "t", "ts_ms": ts, "lsn": 24023400, "txId": 556}, map[string]interface{}{"id": 1, "v": "b"}, 24023400},
		},
		{
			name:  "mysql binlog pos",
			topic: "srv.d.t",
			first: change{map[string]interface{}{"connector": "mysql", "db": "d", "table": "t", "ts_ms": ts, "file": "mysql-bin.000003", "pos": 154}, map[string]interface{}{"id": 1, "v": "a"}, 154},
			later: change{map[string]interface{}{"connector": "mysql", "db": "d", "table": "t", "ts_ms": ts, "file": "mysql-bin.000003", "pos": 890}, map[string]interface{}{"id": 1, "v": "b"}, 890},
		},
		{
			name:  "sqlserver commit_lsn",
			topic: "srv.d.dbo.t",
			first: change{map[string]interface{}{"connector": "sqlserver", "db": "d", "schema": "dbo", "table": "t", "ts_ms": ts, "commit_lsn": "0000002a:00000948:0003"}, map[string]interface{}{"id": 1, "v": "a"}, parseSQLServerLSN("0000002a:00000948:0003")},
			later: change{map[string]interface{}{"connector": "sqlserver", "db": "d", "schema": "dbo", "table": "t", "ts_ms": ts, "commit_lsn": "0000002a:00000950:0001"}, map[string]interface{}{"id": 1, "v": "b"}, parseSQLServerLSN("0000002a:00000950:0001")},
		},
		{
			// The bug: MongoDB carries no lsn/pos; its oplog ordinal inside the
			// second is source.ord, and before the fix it was never read.
			name:  "mongodb ord",
			topic: "srv.app.users",
			first: change{map[string]interface{}{"connector": "mongodb", "db": "app", "collection": "users", "ts_ms": ts, "ord": 1}, `{"_id":{"$oid":"5f1a2b3c4d5e6f7a8b9c0d1e"},"v":"a"}`, 1},
			later: change{map[string]interface{}{"connector": "mongodb", "db": "app", "collection": "users", "ts_ms": ts, "ord": 7}, `{"_id":{"$oid":"5f1a2b3c4d5e6f7a8b9c0d1e"},"v":"b"}`, 7},
		},
	}

	for _, f := range families {
		t.Run(f.name, func(t *testing.T) {
			cfg := &WorkerConfig{PipelineID: "p", Topic: f.topic, SinkMode: "cdc", DestinationConnector: "gcs"}
			parse := func(c change, offset int64) *SinkMessage {
				raw, _ := json.Marshal(map[string]interface{}{
					"op": "u", "source": c.source, "before": nil, "after": c.after, "ts_ms": ts + 5,
				})
				sm, err := parseSinkMessage(cfg, kafka.Message{Topic: f.topic, Value: raw, Offset: offset})
				if err != nil {
					t.Fatalf("parseSinkMessage: %v", err)
				}
				if sm.SourceTS != ts {
					t.Fatalf("SourceTS=%d, want source.ts_ms %d", sm.SourceTS, ts)
				}
				if sm.LSN != c.want {
					t.Errorf("LSN=%d, want %d", sm.LSN, c.want)
				}
				return sm
			}
			a, b := cdcSeq(parse(f.first, 1)), cdcSeq(parse(f.later, 2))
			if !(a < b) {
				t.Errorf("two changes in one source timestamp are not ordered: first %s, later %s (MAX(_rsync_cdc_seq) cannot pick the later change)", a, b)
			}
		})
	}
}
