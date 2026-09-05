// Curated natural-language example prompts for the Data Explorer NL box.
// They teach users the kinds of questions the NL→SQL pipeline handles, and
// are tailored to the connected schema when (non-internal) table names are
// available.

import { isInternalExplorerTable } from "./internalTables"

export interface ExamplePrompt {
  /** Short chip label. */
  label: string
  /** Full natural-language question dropped into the NL input on click. */
  prompt: string
}

export interface ExampleColumn {
  name: string
  type?: string
}

export interface ExampleTable {
  name: string
  schema?: string
  row_count?: number
  columns?: ExampleColumn[]
}

/** Generic, schema-free examples used when no real business table is known. */
function genericExamples(): ExamplePrompt[] {
  return [
    { label: "Count rows", prompt: "How many rows are in each table?" },
    { label: "Top 10", prompt: "Show the top 10 records by total amount" },
    { label: "Group & count", prompt: "Count records grouped by status" },
    { label: "Monthly trend", prompt: "Show record counts per month" },
  ]
}

/** True when a column looks date/time-like (gates the monthly-trend chip). */
function hasDateLikeColumn(columns?: ExampleColumn[]): boolean {
  if (!columns) return false
  return columns.some((c) => {
    const type = (c.type ?? "").toLowerCase()
    const name = c.name.toLowerCase()
    return (
      /date|time|timestamp/.test(type) ||
      /_at$/.test(name) ||
      /(^|_)(date|time)s?($|_)/.test(name) ||
      /^(created|updated|modified)/.test(name)
    )
  })
}

/**
 * Build example NL prompts. Internal `_rsync_*` bookkeeping tables are excluded
 * so the chips reference real business data (or stay generic when none exists).
 * Candidates are ranked by row count (highest signal first, name as tiebreak);
 * a join example appears only when a DISTINCT second table exists; the monthly
 * chip only when a date-like column exists. Grammar-safe phrasing reads for
 * singular/snake_case names. Pure + deterministic.
 */
export function getExamplePrompts(tables: ExampleTable[] = []): ExamplePrompt[] {
  const candidates = tables
    .filter((t) => t.name && !isInternalExplorerTable(t.name))
    .slice()
    .sort((a, b) => {
      const rc = (b.row_count ?? 0) - (a.row_count ?? 0)
      return rc !== 0 ? rc : a.name.localeCompare(b.name)
    })

  const first = candidates[0]
  if (!first) return genericExamples()

  // Qualify the display name only when the bare name is non-unique among
  // candidates (otherwise the chip is ambiguous, e.g. two `orders`).
  const bareCount = (name: string) =>
    candidates.reduce((n, t) => (t.name === name ? n + 1 : n), 0)
  const displayName = (t: ExampleTable) =>
    bareCount(t.name) > 1 && t.schema ? `${t.schema}.${t.name}` : t.name

  const t1 = displayName(first)
  const examples: ExamplePrompt[] = [
    { label: `Count ${t1}`, prompt: `How many rows are in ${t1}?` },
    { label: `Recent ${t1}`, prompt: `Show the 10 most recent rows from ${t1}` },
    { label: "Group & count", prompt: `Count rows in ${t1} grouped by a category, highest first` },
  ]

  if (hasDateLikeColumn(first.columns)) {
    examples.push({ label: "Monthly trend", prompt: `Show the number of rows in ${t1} per month` })
  }

  // Join example only when there is a genuinely different second table.
  const second = candidates.find((t) => t.name !== first.name)
  if (second) {
    const t2 = displayName(second)
    examples.push({
      label: `Join ${t1} + ${t2}`,
      prompt: `Join ${t1} with ${t2} and show the combined details`,
    })
  }

  return examples
}
