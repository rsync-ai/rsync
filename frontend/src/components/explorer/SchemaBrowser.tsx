"use client"

import { useMemo, useState } from "react"
import { ChevronDown, ChevronRight, KeyRound, Loader2, Plus, Search } from "lucide-react"
import { groupTablesByDatabase, type SchemaTableLike } from "@/lib/explorer/schemaTree"
import { cn } from "@/lib/utils"

export interface SchemaBrowserProps {
  /** Flat table list from the schema-index API (each carries a `schema`). */
  tables: SchemaTableLike[]
  /** Show a spinner instead of the tree while the schema loads. */
  loading?: boolean
  /** Message shown when there are no tables (e.g. no connection picked). */
  emptyHint?: string
  /** Schema-qualified names that are currently pre-selected (checkboxes). */
  selectedTables?: string[]
  /** Render a selection checkbox per table and call this on toggle. */
  onToggleTable?: (qualifiedName: string) => void
  /** Override the key used for selection state + onToggleTable; defaults to
   *  the schema-qualified name. (Insert always uses the qualified name.) */
  selectionKey?: (table: SchemaTableLike) => string
  /** Insert `schema.table` (or `table`) into the SQL editor. */
  onInsertTable?: (qualifiedName: string) => void
  /** Insert a bare column name into the SQL editor. */
  onInsertColumn?: (column: string) => void
  className?: string
}

const qualify = (t: SchemaTableLike): string => (t.schema ? `${t.schema}.${t.name}` : t.name)

/**
 * SchemaBrowser — the Data Explorer left panel, Athena-style.
 *
 * A **Database** dropdown selects one namespace at a time; its tables are
 * listed directly under a "Tables (N)" header (no node to expand first), a
 * filter box narrows the list, and each table expands to reveal its columns.
 * Per-table selection checkboxes + click-to-insert are preserved so pipeline /
 * NL→SQL table picking is unchanged.
 *
 * Grouping comes from the pure `groupTablesByDatabase` helper, so the database
 * → table shape is unit-tested independently of this view. Internal `_rsync_*`
 * tables are filtered upstream (the parent passes `visibleTables`).
 */
export function SchemaBrowser({
  tables,
  loading,
  emptyHint,
  selectedTables,
  onToggleTable,
  selectionKey,
  onInsertTable,
  onInsertColumn,
  className,
}: SchemaBrowserProps) {
  const [selectedDb, setSelectedDb] = useState("")
  const [filter, setFilter] = useState("")
  const [openTables, setOpenTables] = useState<Set<string>>(new Set())

  const groups = useMemo(() => groupTablesByDatabase(tables), [tables])
  const databases = useMemo(() => groups.map((g) => g.database), [groups])

  // Keep the user's pick while it still exists; otherwise fall back to the
  // first database (covers the initial render + a connection switch).
  const activeDb = databases.includes(selectedDb) ? selectedDb : databases[0] ?? ""
  const activeTables = useMemo(
    () => groups.find((g) => g.database === activeDb)?.tables ?? [],
    [groups, activeDb],
  )

  const query = filter.trim().toLowerCase()
  const visibleTables = useMemo(
    () => (query ? activeTables.filter((t) => t.name.toLowerCase().includes(query)) : activeTables),
    [activeTables, query],
  )

  const toggleTable = (key: string) =>
    setOpenTables((prev) => {
      const next = new Set(prev)
      if (next.has(key)) next.delete(key)
      else next.add(key)
      return next
    })

  if (loading) {
    return (
      <div
        className={cn("flex items-center gap-2 px-2 py-6 text-sm text-muted-foreground", className)}
      >
        <Loader2 className="h-4 w-4 animate-spin" />
        Loading schema…
      </div>
    )
  }

  if (databases.length === 0) {
    return (
      <p className={cn("px-2 py-6 text-center text-sm text-muted-foreground", className)}>
        {emptyHint ?? "No tables found."}
      </p>
    )
  }

  return (
    <div className={cn("flex flex-col gap-3", className)}>
      {/* Database selector — pick one namespace; its tables list below. */}
      <div className="flex flex-col gap-1">
        <label htmlFor="schema-database" className="text-xs font-medium text-muted-foreground">
          Database
        </label>
        <select
          id="schema-database"
          value={activeDb}
          onChange={(e) => {
            setSelectedDb(e.target.value)
            setOpenTables(new Set())
            setFilter("")
          }}
          className="h-9 w-full rounded-md border bg-background px-2 text-sm outline-none focus:ring-1 focus:ring-ring"
        >
          {databases.map((db) => (
            <option key={db} value={db}>
              {db}
            </option>
          ))}
        </select>
      </div>

      {/* Tables (N) header */}
      <div className="flex items-center justify-between px-0.5">
        <span className="text-sm font-semibold">Tables ({activeTables.length})</span>
      </div>

      {/* Filter */}
      <div className="relative">
        <Search className="pointer-events-none absolute left-2 top-2.5 h-4 w-4 text-muted-foreground" />
        <input
          type="text"
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          placeholder="Filter tables…"
          className="w-full rounded-md border bg-background py-1.5 pl-8 pr-2 text-sm outline-none focus:ring-1 focus:ring-ring"
        />
      </div>

      {/* Table list (scrollable) */}
      {visibleTables.length === 0 ? (
        <p className="px-2 py-6 text-center text-sm text-muted-foreground">
          No tables match “{filter}”.
        </p>
      ) : (
        <ul className="max-h-[320px] space-y-0.5 overflow-y-auto">
          {visibleTables.map((table) => {
            const key = qualify(table)
            const tOpen = openTables.has(key)
            const selKey = selectionKey ? selectionKey(table) : key
            return (
              <li key={key}>
                <div className="group flex min-w-0 items-center gap-1">
                  {onToggleTable && (
                    <input
                      type="checkbox"
                      checked={selectedTables?.includes(selKey) ?? false}
                      onChange={() => onToggleTable(selKey)}
                      aria-label={`Select ${table.name}`}
                      className="shrink-0"
                    />
                  )}
                  <button
                    type="button"
                    onClick={() => toggleTable(key)}
                    aria-expanded={tOpen}
                    title={table.name}
                    className="flex min-w-0 flex-1 items-center gap-1 rounded px-1 py-1 text-sm hover:bg-muted"
                  >
                    {tOpen ? (
                      <ChevronDown className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
                    ) : (
                      <ChevronRight className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
                    )}
                    <span className="truncate">{table.name}</span>
                  </button>
                  {typeof table.row_count === "number" && (
                    <span className="shrink-0 text-xs tabular-nums text-muted-foreground">
                      {table.row_count.toLocaleString()}
                    </span>
                  )}
                  {onInsertTable && (
                    <button
                      type="button"
                      aria-label={`Insert table ${table.name}`}
                      title="Add to SQL"
                      onClick={() => onInsertTable(key)}
                      className="shrink-0 rounded p-0.5 text-muted-foreground opacity-0 hover:bg-muted hover:text-foreground group-hover:opacity-100"
                    >
                      <Plus className="h-3.5 w-3.5" />
                    </button>
                  )}
                </div>

                {tOpen && table.columns && table.columns.length > 0 && (
                  <ul className="ml-5 space-y-0.5 border-l pl-2">
                    {table.columns.map((col) => (
                      <li
                        key={col.name}
                        className="group/col flex min-w-0 items-center gap-1.5 px-1 py-0.5 text-xs"
                      >
                        {col.is_primary_key ? (
                          <KeyRound className="h-3 w-3 shrink-0 text-amber-500" />
                        ) : (
                          <span className="w-3 shrink-0" />
                        )}
                        <span className="min-w-0 truncate" title={col.name}>{col.name}</span>
                        {col.type && (
                          <span className="shrink-0 text-muted-foreground">{col.type}</span>
                        )}
                        {onInsertColumn && (
                          <button
                            type="button"
                            aria-label={`Insert column ${col.name}`}
                            title="Add column to SQL"
                            onClick={() => onInsertColumn(col.name)}
                            className="ml-auto shrink-0 rounded p-0.5 text-muted-foreground opacity-0 hover:bg-muted hover:text-foreground group-hover/col:opacity-100"
                          >
                            <Plus className="h-3 w-3" />
                          </button>
                        )}
                      </li>
                    ))}
                  </ul>
                )}
              </li>
            )
          })}
        </ul>
      )}
    </div>
  )
}
