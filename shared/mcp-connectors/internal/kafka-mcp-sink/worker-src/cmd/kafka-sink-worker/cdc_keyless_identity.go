package main

import (
	"fmt"

	"github.com/segmentio/kafka-go"
)

// Keyless rows on a document destination (KI-MONGODB-DEST-KEYLESS-REPLAY-DUPLICATES).
//
// A table with no declared key used to reach MongoDB as a plain insert_many, which
// let the database mint each _id. MongoDB records its Kafka high-water mark AFTER
// the data (Tier B: no transaction on a standalone mongod), so a redelivery of a
// batch whose offset was not recorded — a crash between the two writes — inserted
// every row again under new _ids.
//
// The fix gives each such row the identity it already has: the Kafka record it came
// from. The sink sets _id from the record's coordinates and writes the row with
// upsert_data keyed on _id, so a redelivered record replaces its own document
// instead of adding one. _id is always uniquely indexed, so this costs no index and
// no connector change. One record is one document, so a keyless table keeps its
// append-like history: an update of a keyless row is a new document, as before.

// keylessDocumentID is the deterministic _id of row `idx` of Kafka record `m`.
//
// pipeline_id, topic and partition name the offset space, as they do in
// _rsync_cdc_offsets. The record timestamp is there for a topic that is deleted and
// recreated while the collection is kept: its offsets restart at 0, and without the
// timestamp a new record would take an old document's _id and replace it. It is the
// broker-stored timestamp, so a redelivery carries the same value.
func keylessDocumentID(pipelineID string, m kafka.Message, idx int) string {
	ts := int64(0)
	if !m.Time.IsZero() {
		ts = m.Time.UnixMilli()
	}
	return fmt.Sprintf("%s|%s|%d|%d|%d|%d", pipelineID, m.Topic, m.Partition, m.Offset, ts, idx)
}

// withKeylessDocumentID returns a copy of row carrying keylessDocumentID as _id.
// A row that already has an _id (a source column of that name) is returned as is:
// that column is what the destination would use as the identity anyway.
func withKeylessDocumentID(row map[string]interface{}, pipelineID string, m kafka.Message, idx int) map[string]interface{} {
	if row == nil {
		return nil
	}
	if _, ok := row["_id"]; ok {
		return row
	}
	out := make(map[string]interface{}, len(row)+1)
	for k, v := range row {
		out[k] = v
	}
	out["_id"] = keylessDocumentID(pipelineID, m, idx)
	return out
}

// keylessDocumentKey is the key a keyless row is written on at a document destination.
var keylessDocumentKey = []string{"_id"}
