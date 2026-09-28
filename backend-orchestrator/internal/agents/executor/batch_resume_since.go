package executor

// sinceWatermarkKey holds the incremental `since` a sweep STARTED with, saved with
// every checkpoint of that sweep. It is to the watermark what since_cursor is to the
// paging cursor: the per-batch `watermark` is the highest updated_at the sweep has
// seen SO FAR, and the sweep pages in key order, not updated_at order.
const sinceWatermarkKey = "since_watermark"

// resumeIncrementalSince picks the `since` for a resumed table.
//
// A new sweep (the previous one reached the end) filters on the last watermark —
// that is the point of incremental sync. A mid-table continuation must keep the
// baseline its sweep started with: filtering the unread rest of the table on the
// sweep's own running maximum skipped every unread row whose updated_at was older
// than a row already read, and the run still reported success.
//
// A continuation whose checkpoint predates since_watermark has no recorded
// baseline; it reads the rest of the table unfiltered. The paging cursor still
// bounds it to rows this sweep has not read, so that is a superset, never a
// duplicate.
func resumeIncrementalSince(pos map[string]interface{}, midTable bool, watermarkSince string) string {
	if !midTable {
		return watermarkSince
	}
	baseline, _ := pos[sinceWatermarkKey].(string)
	return baseline
}

// tableRowsSoFarKey is the source rows THIS table has exported across the chunks
// of its current sweep. The runaway backstop (EXECUTOR_TABLE_MAX_ROWS) is per
// table; it used to resume from `rows_so_far`, which is the whole run's total
// (every table), so on a multi-table resume a table could trip the backstop
// early. `rows_so_far` stays as it is for the Checkpoints card.
const tableRowsSoFarKey = "table_rows_so_far"

// resumeTableRowsSoFar reads the per-table count back. A checkpoint that predates
// the key starts from 0 — never from the run-wide `rows_so_far`.
func resumeTableRowsSoFar(pos map[string]interface{}) int {
	if n, ok := pos[tableRowsSoFarKey].(float64); ok && n > 0 {
		return int(n)
	}
	return 0
}
