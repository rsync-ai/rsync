package executor

import "time"

// sweepEndPosition is the checkpoint to re-save when a page comes back EMPTY after
// a full one — the end of a table whose row count is an exact multiple of the
// batch size. The per-batch save marks `table_complete` only on a SHORT page, so
// without this the last full page's `table_complete: false` is the final word:
// the next run resumes "mid-table" behind the paging cursor instead of starting a
// new sweep, and an UPDATE to an already-synced row never comes back.
//
// It copies the last saved position (so batch_idx/offset/key_ordinal/cursor and the
// run_start rewind point are unchanged) and returns nil when there is nothing to
// mark: no checkpoint yet, or one that already says complete.
func sweepEndPosition(last map[string]interface{}, executionID string, now time.Time) map[string]interface{} {
	if last == nil {
		return nil
	}
	if done, _ := last["table_complete"].(bool); done {
		return nil
	}
	pos := make(map[string]interface{}, len(last)+3)
	for k, v := range last {
		pos[k] = v
	}
	pos["table_complete"] = true
	pos["updated_at"] = now.UTC().Format(time.RFC3339)
	pos["execution_id"] = executionID
	return pos
}
