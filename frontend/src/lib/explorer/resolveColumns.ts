/**
 * Client for POST /api/v1/explorer/nl/resolve-columns — the LLM column-linking
 * step, registered at main.go:1205 and implemented end to end
 * (explorer.go:3347 → llm-service /api/v1/explorer/nl/resolve-columns) with
 * ZERO callers anywhere in the repo.
 *
 * What the explorer did instead: the "Column Link" step of the run timeline
 * listed every column of every linked table and reported a hard-coded
 * confidence of 0.85. That is a fabricated step — it cannot fail, it cannot
 * disagree with the user, and its 0.85 is not a measurement of anything. This
 * module replaces it with the real answer.
 *
 * PRIVACY (CLAUDE.md "LLM data privacy — metadata only"): the body carries
 * table names, column names, column types and nullability. No row values, no
 * query results. The gateway re-projects the payload before forwarding
 * (explorer.go:3384-3400) and only passes `name`, `schema` and the four column
 * fields on to llm-service — so anything extra sent here is silently dropped,
 * which is a reason to keep it minimal rather than a licence to widen it.
 *
 * FIELD-NAME TRAP: the gateway binds `selected_tables` to
 * []cache.ExplorerTableIndex, whose column struct tags nullability as
 * `is_nullable` (explorer_cache.go:58). The explorer page's own in-memory
 * column shape calls it `nullable`. Sending the page's shape verbatim would
 * bind every column as nullable=false without any error. `toIndexTables` is
 * the translation.
 */

import { authFetch } from "@/lib/api/auth-fetch"

/** The page's in-memory column shape (note: `nullable`, not `is_nullable`). */
export type PageColumn = {
  name: string
  type?: string
  is_primary_key?: boolean
  nullable?: boolean
}

export type PageTable = {
  name: string
  schema?: string
  columns?: PageColumn[]
}

/** The wire shape the gateway binds — cache.ExplorerTableIndex. */
export type IndexTable = {
  name: string
  schema: string
  columns: Array<{
    name: string
    type: string
    is_primary_key: boolean
    is_nullable: boolean
  }>
}

export type ColumnMapping = {
  select_cols?: string[]
  where_cols?: string[]
  group_by_cols?: string[]
  order_by_cols?: string[]
}

export type JoinPlanEntry = {
  join_type?: string
  left_table?: string
  right_table?: string
  condition?: string
}

export type ResolveColumnsResponse = {
  columns: ColumnMapping
  join_plan?: JoinPlanEntry[]
  confidence: number
  needs_hitl: boolean
  hitl_reason?: string
  ambiguous_columns?: string[]
}

/**
 * Translate the page's tables into the struct the gateway binds.
 * Defaults mirror the Go zero values, so a column the page knows nothing about
 * arrives as a non-PK, non-nullable column of unknown type rather than as a
 * bind error that would fail the whole request.
 */
export function toIndexTables(tables: PageTable[]): IndexTable[] {
  return tables.map((t) => ({
    name: String(t.name || ""),
    schema: String(t.schema || "public"),
    columns: (t.columns || []).map((c) => ({
      name: String(c.name || ""),
      type: String(c.type || ""),
      is_primary_key: Boolean(c.is_primary_key),
      // The rename that a naive pass-through would get wrong.
      is_nullable: Boolean(c.nullable),
    })),
  }))
}

/**
 * The columns the model actually chose, in the order they matter for reading a
 * query — projection first, then filters, then grouping, then ordering — with
 * duplicates removed. A column used in both SELECT and WHERE is one column.
 */
export function flattenResolvedColumns(columns: ColumnMapping | null | undefined): string[] {
  const out: string[] = []
  const seen = new Set<string>()
  for (const group of [
    columns?.select_cols,
    columns?.where_cols,
    columns?.group_by_cols,
    columns?.order_by_cols,
  ]) {
    for (const raw of group || []) {
      const c = String(raw || "").trim()
      if (!c || seen.has(c)) continue
      seen.add(c)
      out.push(c)
    }
  }
  return out
}

/**
 * One line saying what the column-linking step concluded. `needs_hitl` and its
 * reason come first when set: that is the model telling the operator it is not
 * sure, and burying it under a column count would waste the only signal this
 * step produces that the old fabricated version could never produce.
 */
export function describeColumnLink(resp: ResolveColumnsResponse): string {
  const n = flattenResolvedColumns(resp.columns).length
  const joins = resp.join_plan?.length || 0
  const parts: string[] = [`${n} ${n === 1 ? "column" : "columns"} linked`]
  if (joins > 0) parts.push(`${joins} ${joins === 1 ? "join" : "joins"} planned`)
  if (resp.needs_hitl) {
    const why = String(resp.hitl_reason || "").trim()
    const ambiguous = resp.ambiguous_columns || []
    const tail = ambiguous.length > 0 ? ` (ambiguous: ${ambiguous.join(", ")})` : ""
    return `Needs review — ${why || "the model was not confident about the column mapping"}${tail}`
  }
  return parts.join(", ")
}

/**
 * Resolve the columns. Throws on any non-2xx; the caller decides whether that
 * is fatal. It is not fatal in the explorer today: nothing downstream of the
 * column-link step consumes its output — SQL generation is given the schema
 * context directly — so a failure here degrades the timeline entry rather than
 * the run.
 */
export async function resolveExplorerColumns(args: {
  connectionId: string
  question: string
  tables: PageTable[]
  conversationId?: string
  signal?: AbortSignal
}): Promise<ResolveColumnsResponse> {
  const body: Record<string, unknown> = {
    connection_id: args.connectionId,
    // binding:"required" on the gateway — an empty string is a 400, so a
    // raw-SQL run with no natural-language question sends a stand-in the same
    // way fetchNextStepSuggestions does.
    question: args.question.trim() || "Columns relevant to this query",
    selected_tables: toIndexTables(args.tables),
  }
  if (args.conversationId) body.conversation_id = args.conversationId

  const res = await authFetch("/api/v1/explorer/nl/resolve-columns", {
    method: "POST",
    headers: { "Content-Type": "application/json", Accept: "application/json" },
    body: JSON.stringify(body),
    signal: args.signal,
  })

  const data = (await res.json().catch(() => null)) as
    | (ResolveColumnsResponse & { error?: string; message?: string; details?: string })
    | null

  if (!res.ok) {
    throw new Error(data?.error || data?.message || `HTTP ${res.status}`)
  }

  return {
    columns: data?.columns || {},
    join_plan: data?.join_plan || [],
    confidence: typeof data?.confidence === "number" ? data.confidence : 0,
    needs_hitl: Boolean(data?.needs_hitl),
    hitl_reason: data?.hitl_reason,
    ambiguous_columns: data?.ambiguous_columns,
  }
}
