package executor

// The batch export loop carries two paging numbers that used to be one.
//
//	offset     — positions the SOURCE QUERY. Under keyset paging the cursor
//	             positions the query instead, so offset is pinned at 0.
//	keyOrdinal — the batch's downstream IDENTITY. It is published as
//	             `batch_offset` and becomes the destination object key
//	             (`part-%06d`, kafka-sink-worker main.go partKey), the outbox
//	             ON CONFLICT key, and the ack ledger's batch column.
//
// Collapsing the two meant every page of a keyset export published
// batch_offset=0, so each page overwrote the previous page's object and the
// run still reported `completed`. Two downstream consumers had already worked
// around the collapse rather than fix it (the ack ledger's idempotency key in
// migration 054, and the sink's running totals), which is why it survived.
//
// These helpers are split out of the loop so the invariant is unit-testable:
// the loop is inside a multi-thousand-line function with no seam.

// seedKeyOrdinal resolves the starting key ordinal from a resume checkpoint.
//
// Checkpoints written before `key_ordinal` existed carry only `offset`.
// Falling back to it is correct in both paging modes: for an offset-paged
// table the two are equal by construction, and for a keyset-paged one the
// stored offset is the frozen 0 that is the bug being fixed — so the first
// post-deploy run restarts numbering from 0 and is monotonic from then on.
func seedKeyOrdinal(position map[string]interface{}, startOffset int) int {
	if koVal, ok := position["key_ordinal"].(float64); ok && koVal > 0 {
		return int(koVal)
	}
	return startOffset
}

// advancePage steps both paging numbers after a page of sourceRowCount rows
// has been consumed from the source.
//
// keyOrdinal advances in BOTH paging modes — that is the whole fix. Under
// offset paging the two move in lockstep from the same seed, so every
// published value (and therefore every existing object key) is byte-identical
// to what it was before; under keyset paging offset stays pinned while
// keyOrdinal advances, which is what keeps each page's object key distinct.
//
// It also advances when a page is fully filtered out by transforms and nothing
// is published: those rows were still consumed from the source, so the next
// page's ordinal has moved on.
func advancePage(offset, keyOrdinal, sourceRowCount int, keysetPaged bool) (int, int) {
	if !keysetPaged {
		offset += sourceRowCount
	}
	return offset, keyOrdinal + sourceRowCount
}
