/**
 * Rows of GET /pipelines/:id/table-stats as the Tables card and "Edit tables"
 * read them, as pure functions so the rules are testable apart from the panel.
 *
 * A table no longer selected (`status: "removed"`) keeps its stats row; it is
 * split out so the table editor can mark it "previously loaded" (#5). A CDC
 * stats row never carries run counts: `inserted_rows` is batch-only, so for CDC
 * it is ignored even if present (#9).
 */

/** One row of GET /pipelines/:id/table-stats, read loosely (older backends vary). */
export type TableStatsRow = Record<string, unknown>

export type ParsedTableStats = {
  /** Selected tables named by the stats, deduplicated, removed ones left out. */
  names: string[]
  /** Last-run written rows per table (batch only), by qualified AND bare name. */
  written: Record<string, number>
  /** Tables no longer selected (`status: "removed"`) — the "previously loaded" ones. */
  removed: string[]
}

function str(v: unknown): string | undefined {
  return typeof v === "string" ? v : undefined
}

function count(v: unknown): number | undefined {
  const n = typeof v === "number" ? v : typeof v === "string" && v.trim() ? Number(v) : undefined
  return typeof n === "number" && Number.isFinite(n) && n >= 0 ? n : undefined
}

export function parseTableStatsRows(rows: TableStatsRow[], opts: { cdc?: boolean } = {}): ParsedTableStats {
  const names: string[] = []
  const seen = new Set<string>()
  const written: Record<string, number> = {}
  const removed: string[] = []
  for (const t of rows) {
    if (!t || typeof t !== "object") continue
    const q = str(t["qualified_name"])
    const schema = str(t["schema_name"]) ?? str(t["schema"])
    const name = str(t["table_name"]) ?? str(t["name"])
    const built = schema && name ? `${schema}.${name}` : name
    const v = String(q || built || "").trim()
    if (!v) continue

    if (String(t["status"] || "").toLowerCase() === "removed") {
      removed.push(v)
      continue
    }
    if (!seen.has(v)) {
      seen.add(v)
      names.push(v)
    }

    const cdc = opts.cdc || String(t["mode"] || "").toLowerCase() === "cdc"
    if (cdc) continue
    const w = count(t["inserted_rows"])
    if (w !== undefined) {
      written[v] = w
      // also store unqualified name for matching convenience
      if (name) written[name.trim()] = w
    }
  }
  return { names, written, removed }
}

/**
 * Does `name` (as the selection or the metadata spells it) refer to one of
 * `tables` (as the stats spell them)? Exact, or on the last two segments
 * ("db.public.orders" vs "public.orders"); a bare name matches only a bare one
 * or the last segment of a single table, so "users" never matches two schemas.
 */
export function makeTableMatcher(tables: string[]): (name: string) => boolean {
  const exact = new Set<string>()
  const lastTwo = new Set<string>()
  const bareCount = new Map<string, number>()
  for (const raw of tables) {
    const t = String(raw || "").trim().toLowerCase()
    if (!t) continue
    exact.add(t)
    const parts = t.split(".")
    if (parts.length >= 2) lastTwo.add(parts.slice(-2).join("."))
    const bare = parts[parts.length - 1]
    bareCount.set(bare, (bareCount.get(bare) ?? 0) + 1)
  }
  return (name: string) => {
    const n = String(name || "").trim().toLowerCase()
    if (!n) return false
    if (exact.has(n)) return true
    const parts = n.split(".")
    if (parts.length >= 2) return lastTwo.has(parts.slice(-2).join("."))
    return bareCount.get(n) === 1
  }
}
