package executor

// reloadContinuesTable: a table checkpoint that executionID wrote. A batch
// reload bigger than one chunk returns needs_continuation, and the workflow
// re-dispatches the same task, run_mode=reload and execution_id included. Each
// dispatch used to delete the checkpoints and clean the destination again, so
// the reload restarted at offset 0 forever. A continuation now resumes from its
// own checkpoint; a checkpoint from any other run (or with no execution_id,
// written before the field existed) is not this reload's, and the table is
// rebuilt.
func reloadContinuesTable(position map[string]interface{}, executionID string) bool {
	if executionID == "" || position == nil {
		return false
	}
	written, _ := position["execution_id"].(string)
	return written == executionID
}

// reloadFinishedTable: this reload already read the whole table in an earlier
// chunk (its own checkpoint says table_complete), so a continuation skips it.
// Resuming it at its end instead re-queried an empty page on every continuation
// of a multi-table reload, 40 times for a 500-row table next to a 40,000-row one.
func reloadFinishedTable(position map[string]interface{}, executionID string) bool {
	if !reloadContinuesTable(position, executionID) {
		return false
	}
	complete, _ := position["table_complete"].(bool)
	return complete
}
