// Which result columns did the server mask?
//
// api-gateway redacts secret- and PII-looking columns on the *grid* path only:
// ExecuteExplorerQuery calls executePostgresQuery / executeMySQLQuery with
// redact=true, and connections.go redactForPreview rewrites every value in a
// matched column to its first two characters plus "***" ("ab***"), or to a bare
// "***" when the value is four characters or shorter. NULL becomes "".
//
// The response carries no flag naming the masked columns, and the keyword list
// that decides them (isSecretColumnName / isPIIColumnName, connections.go
// ~2230) is long, has per-token special cases and keeps growing — a TypeScript
// copy would be a second source of truth that drifts silently. So we read the
// mask off the values the user is actually looking at.
//
// This matters because the Download path is deliberately NOT redacted
// (ExportQueryHandler calls the …Unredacted executors), so the UI has to say
// which columns are masked on screen *and* that the file will not be.

/** The suffix redactForPreview appends to every masked value. */
const MASK = "***"

/**
 * looksMasked reports whether a single value has redactForPreview's exact
 * shape: the mask suffix preceded by either nothing ("***") or precisely two
 * kept characters ("ab***"). Any other length of prefix is a real value that
 * merely ends in asterisks.
 */
export function looksMasked(value: unknown): boolean {
  if (typeof value !== "string") return false
  if (!value.endsWith(MASK)) return false
  const kept = value.length - MASK.length
  return kept === 0 || kept === 2
}

/**
 * maskedColumns returns the columns whose every non-empty value looks masked.
 *
 * Requiring *all* of them keeps a lone real "ab***" from flagging a clean
 * column; requiring at least one keeps an all-NULL column (redaction turns
 * NULL into "", and so does JSON for a genuinely absent field) from being
 * reported as masked when there is no evidence either way. Pure, and safe to
 * call inside a memo on every result.
 */
export function maskedColumns(
  columns: string[] | undefined | null,
  rows: Array<Record<string, unknown>> | undefined | null,
): string[] {
  if (!columns?.length || !rows?.length) return []
  const masked: string[] = []
  for (const col of columns) {
    let sawValue = false
    let allMasked = true
    for (const row of rows) {
      const value = row?.[col]
      if (value === null || value === undefined || value === "") continue
      sawValue = true
      if (!looksMasked(value)) {
        allMasked = false
        break
      }
    }
    if (sawValue && allMasked) masked.push(col)
  }
  return masked
}
