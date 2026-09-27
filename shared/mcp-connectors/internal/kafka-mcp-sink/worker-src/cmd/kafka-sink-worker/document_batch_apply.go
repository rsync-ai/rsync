package main

// Two guarantees the batched CDC apply path (cdcDBBatcher.flushBatch) owes a
// document destination (isDocumentDBConnector — MongoDB):
//
//  1. One write per key per batch (#22). The MongoDB connector applies a batch as
//     bulk_write(ordered=False) of ReplaceOne(upsert=True) ops, and an unordered bulk
//     write promises no order between ops: a batch holding c(k1,v1) then u(k1,v2)
//     could land v1 last and leave the stale version behind. Every ReplaceOne
//     replaces the WHOLE document, so writing only the last event per key ends in
//     exactly the state ordered application would. That holds for a partial row too
//     (a column filterDebeziumUnavailable dropped): the replace discards it either
//     way, so folding older versions in would CHANGE the result, not preserve it.
//
//  2. A skipped row is a lost row (#24). The connector skips a document that is
//     missing a key field and still answers success. It names those rows in
//     skipped_indexes, and the sink parks exactly those in the DLQ (landFlushedBatch).

import (
	"encoding/json"
	"sort"
)

// lastEventPerKey returns the positions of the rows to write so that each key is
// written once, by its LAST event, with the survivors kept in batch order — which is
// offset order, since add() appends one (topic, partition)'s messages as they are
// fetched. A row whose key cannot be read (a key field absent or null) is never
// merged with another: it is written exactly as before, and a destination that
// cannot key it reports it skipped. nil means "write every row".
func lastEventPerKey(rows []map[string]interface{}, keyFields []string) []int {
	if len(keyFields) == 0 || len(rows) < 2 {
		return nil
	}
	keys := make([]string, len(rows))
	last := make(map[string]int, len(rows))
	for i, row := range rows {
		if k, ok := rowKeyIdentity(row, keyFields); ok {
			keys[i] = k
			last[k] = i
		}
	}
	keep := make([]int, 0, len(last))
	for i := range rows {
		if keys[i] == "" || last[keys[i]] == i {
			keep = append(keep, i)
		}
	}
	if len(keep) == len(rows) {
		return nil
	}
	return keep
}

// rowKeyIdentity renders a row's key values as one comparable string. JSON keeps a
// string "1" apart from a number 1 (different documents) while a 1 decoded as
// float64 still equals a 1 held as int (the same document to MongoDB).
func rowKeyIdentity(row map[string]interface{}, keyFields []string) (string, bool) {
	vals := make([]interface{}, len(keyFields))
	for i, f := range keyFields {
		v, ok := row[f]
		if !ok || v == nil {
			return "", false
		}
		vals[i] = v
	}
	b, err := json.Marshal(vals)
	if err != nil {
		return "", false
	}
	return string(b), true
}

// destSkippedRows reads the rows a destination reports it skipped — skipped (a
// count) and skipped_indexes (positions in the data it was sent) — and maps them
// back to batch positions through sent (nil: the batch was sent as-is; otherwise
// sent[j] is the batch position of the j-th row sent). ok=false means a skip was
// reported that cannot be pinned to rows: a count without as many indexes, or an
// index that is not a position of what was sent.
func destSkippedRows(res map[string]interface{}, sent []int, batchLen int) (skipped []int, ok bool) {
	count := toInt64(res["skipped"])
	raw, _ := res["skipped_indexes"].([]interface{})
	if count <= 0 && len(raw) == 0 {
		return nil, true
	}
	if count > 0 && int64(len(raw)) != count {
		return nil, false
	}
	n := batchLen
	if sent != nil {
		n = len(sent)
	}
	seen := make(map[int]bool, len(raw))
	for _, v := range raw {
		f, isNum := v.(float64)
		j := int(f)
		if !isNum || float64(j) != f || j < 0 || j >= n || seen[j] {
			return nil, false
		}
		seen[j] = true
		if sent != nil {
			j = sent[j]
		}
		skipped = append(skipped, j)
	}
	sort.Ints(skipped)
	return skipped, true
}
