"use client"

import { useMemo, useState } from "react"
import { ChevronDown, ChevronRight, KeyRound, Link2, Loader2, Plus, Search } from "lucide-react"
import {
  groupForeignKeysByTable,
  groupTablesByDatabase,
  isInferredForeignKey,
  relationshipsFor,
  type SchemaForeignKeyLike,
  type SchemaTableLike,
} from "@/lib/explorer/schemaTree"
import { cn } from "@/lib/utils"

export interface SchemaBrowserProps {
  /** Flat table list from the schema-index API (each carries a `schema`). */
  tables: SchemaTableLike[]
  /** Foreign keys from the same schema-index response. Rendered per table so
   *  the joins the AI writes against are visible, inferred ones marked. */
  foreignKeys?: SchemaForeignKeyLike[]
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
  /** Plural noun for the listed items (header, filter, no-match text). */
  itemLabel?: string
  /** Tooltip for the per-table insert button. */
  insertTitle?: string
  /** What the per-table button does, as the start of its accessible name
   *  ("Open collection" → "Open collection events"). A document source has no
   *  SQL to insert into, so "Insert table" misnamed the control there. */
  insertLabel?: string
  /** Label for the namespace dropdown. A Postgres namespace is a schema under one
   *  database, so "Database" over "public" named the wrong thing (#54). */
  namespaceLabel?: string
  className?: string
}

const qualify = (t: SchemaTableLike): string => (t.schema ? `${t.schema}.${t.name}` : t.name)

// The row buttons stay out of the way until hovered, but a keyboard user has
// to be able to see where focus went: the same reveal on focus, plus a ring.
const REVEAL_ON_FOCUS =
  "focus-visible:opacity-100 focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring"

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
  foreignKeys,
  loading,
  emptyHint,
  selectedTables,
  onToggleTable,
  selectionKey,
  onInsertTable,
  onInsertColumn,
  itemLabel = "tables",
  insertTitle = "Add to SQL",
  insertLabel = "Insert table",
  namespaceLabel = "Database",
  className,
}: SchemaBrowserProps) {
  const [selectedDb, setSelectedDb] = useState("")
  const [filter, setFilter] = useState("")
  const [openTables, setOpenTables] = useState<Set<string>>(new Set())

  const groups = useMemo(() => groupTablesByDatabase(tables), [tables])
  const fkIndex = useMemo(() => groupForeignKeysByTable(foreignKeys), [foreignKeys])
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
        className={cn(
          "flex h-full items-center justify-center gap-2 px-2 py-6 text-sm text-muted-foreground",
          className,
        )}
      >
        <Loader2 className="h-4 w-4 animate-spin" />
        Loading schema…
      </div>
    )
  }

  if (databases.length === 0) {
    return (
      <div
        className={cn(
          "flex h-full items-center justify-center px-2 py-6 text-center text-sm text-muted-foreground",
          className,
        )}
      >
        {emptyHint ?? "No tables found."}
      </div>
    )
  }

  return (
    <div className={cn("flex h-full min-h-0 flex-col gap-3", className)}>
      {/* Database selector — pick one namespace; its tables list below. */}
      <div className="flex flex-col gap-1">
        <label htmlFor="schema-database" className="text-xs font-medium text-muted-foreground">
          {namespaceLabel}
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
        <span className="text-sm font-semibold">
          {itemLabel.charAt(0).toUpperCase() + itemLabel.slice(1)} ({activeTables.length})
        </span>
      </div>

      {/* Filter */}
      <div className="relative">
        <Search className="pointer-events-none absolute left-2 top-2.5 h-4 w-4 text-muted-foreground" />
        <input
          type="text"
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          placeholder={`Filter ${itemLabel}…`}
          className="w-full rounded-md border bg-background py-1.5 pl-8 pr-2 text-sm outline-none focus:ring-1 focus:ring-ring"
        />
      </div>

      {/* Table list (scrollable) */}
      {visibleTables.length === 0 ? (
        <p className="px-2 py-6 text-center text-sm text-muted-foreground">
          No {itemLabel} match “{filter}”.
        </p>
      ) : (
        <ul className="min-h-0 flex-1 space-y-0.5 overflow-y-auto">
          {visibleTables.map((table) => {
            const key = qualify(table)
            const tOpen = openTables.has(key)
            const selKey = selectionKey ? selectionKey(table) : key
            const rel = relationshipsFor(fkIndex, table)
            const relCount = rel.outgoing.length + rel.incoming.length
            const inferredCount = [...rel.outgoing, ...rel.incoming].filter(isInferredForeignKey).length
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
                  {relCount > 0 && (
                    <span
                      className="flex shrink-0 items-center gap-0.5 text-[10px] tabular-nums text-sky-600 dark:text-sky-400"
                      title={
                        `${relCount} relationship${relCount === 1 ? "" : "s"}` +
                        (inferredCount > 0 ? ` (${inferredCount} inferred)` : "") +
                        " — expand to see them"
                      }
                    >
                      <Link2 className="h-3 w-3" />
                      {relCount}
                    </span>
                  )}
                  {typeof table.row_count === "number" && (
                    <span className="shrink-0 text-xs tabular-nums text-muted-foreground">
                      {table.row_count.toLocaleString()}
                    </span>
                  )}
                  {onInsertTable && (
                    <button
                      type="button"
                      aria-label={`${insertLabel} ${table.name}`}
                      title={insertTitle}
                      onClick={() => onInsertTable(key)}
                      className={cn(
                        "shrink-0 rounded p-0.5 text-muted-foreground opacity-0 hover:bg-muted hover:text-foreground group-hover:opacity-100",
                        REVEAL_ON_FOCUS,
                      )}
                    >
                      <Plus className="h-3.5 w-3.5" />
                    </button>
                  )}
                </div>

                {tOpen && relCount > 0 && (
                  <ul
                    aria-label={`Relationships for ${table.name}`}
                    className="ml-5 space-y-0.5 border-l border-dashed pl-2"
                  >
                    {rel.outgoing.map((fk) => {
                      const target = fk.to_schema ? `${fk.to_schema}.${fk.to_table}` : fk.to_table
                      const inferred = isInferredForeignKey(fk)
                      return (
                        <li
                          key={`out:${fk.from_column}:${target}.${fk.to_column}`}
                          className="flex min-w-0 items-center gap-1.5 px-1 py-0.5 text-[11px] text-muted-foreground"
                        >
                          <Link2 className="h-3 w-3 shrink-0 text-sky-500" />
                          <span
                            className="min-w-0 truncate"
                            title={`${table.name}.${fk.from_column} → ${target}.${fk.to_column}`}
                          >
                            {fk.from_column} → {target}.{fk.to_column}
                          </span>
                          {inferred && <InferredTag />}
                        </li>
                      )
                    })}
                    {rel.incoming.map((fk) => {
                      const source = fk.from_schema ? `${fk.from_schema}.${fk.from_table}` : fk.from_table
                      const inferred = isInferredForeignKey(fk)
                      return (
                        <li
                          key={`in:${source}.${fk.from_column}:${fk.to_column}`}
                          className="flex min-w-0 items-center gap-1.5 px-1 py-0.5 text-[11px] text-muted-foreground"
                        >
                          <Link2 className="h-3 w-3 shrink-0 text-sky-500/60" />
                          <span
                            className="min-w-0 truncate"
                            title={`${source}.${fk.from_column} → ${table.name}.${fk.to_column}`}
                          >
                            {source}.{fk.from_column} → {fk.to_column}
                          </span>
                          {inferred && <InferredTag />}
                        </li>
                      )
                    })}
                  </ul>
                )}

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
                            className={cn(
                              "ml-auto shrink-0 rounded p-0.5 text-muted-foreground opacity-0 hover:bg-muted hover:text-foreground group-hover/col:opacity-100",
                              REVEAL_ON_FOCUS,
                            )}
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

      {/* The joins the AI writes come from these relationships, so say where
          they came from: a real constraint, or a name-and-type guess. */}
      {fkIndex.size > 0 && (
        <p className="flex shrink-0 items-start gap-1 px-1 text-[10px] leading-tight text-muted-foreground">
          <Link2 className="mt-px h-3 w-3 shrink-0 text-sky-500" />
          <span>
            Relationships the AI joins on. <span className="font-medium">inferred</span> ones were
            guessed from column names, not declared in the database.
          </span>
        </p>
      )}
    </div>
  )
}

/** Marks a relationship that the Phase 3.5 heuristics guessed (confidence &lt; 1). */
function InferredTag() {
  return (
    <span
      className="shrink-0 rounded bg-amber-100 px-1 text-[9px] font-medium text-amber-700 dark:bg-amber-950/40 dark:text-amber-300"
      title="Guessed from column names and types — not a foreign key declared in the database. Check it before relying on the join."
    >
      inferred
    </span>
  )
}
