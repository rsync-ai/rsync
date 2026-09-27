import {
  rowsWrittenForTable,
  TABLE_STATUS_WAITING_FOR_DATA,
  type TableStatRow,
  type TableStatsRollup,
} from "../executionSummary"

/**
 * Builds a TableStatsRollup from per-table rows, for the dataMovementVerdict
 * tests. Production reads the server's summary instead (rollupFromSummary), so
 * this fold lives with the tests rather than in the bundle.
 */
export function rollupTableStats(tables: TableStatRow[]): TableStatsRollup {
  let rowsRead: number | null = null
  let bytesCommitted: number | null = null
  let rowsWritten = 0
  let dlqRows = 0
  let failedTables = 0
  let degradedTables = 0
  let runningTables = 0
  let waitingForDataTables = 0

  for (const t of tables) {
    rowsWritten += rowsWrittenForTable(t)
    dlqRows += t.dlq_rows ?? 0

    if (typeof t.read_rows === "number") rowsRead = (rowsRead ?? 0) + t.read_rows
    if (typeof t.bytes_committed === "number") bytesCommitted = (bytesCommitted ?? 0) + t.bytes_committed

    if (t.status === "failed") failedTables++
    else if (t.status === "degraded") degradedTables++
    else if (t.status === "running") runningTables++
    else if (t.status === TABLE_STATUS_WAITING_FOR_DATA) waitingForDataTables++
  }

  return {
    tableCount: tables.length,
    rowsRead,
    rowsWritten,
    dlqRows,
    bytesCommitted,
    failedTables,
    degradedTables,
    runningTables,
    waitingForDataTables,
  }
}
