"use client"

// A table's columns on the Lineage page. They come from the warehouse's schema index
// (GET /api/v1/explorer/connections/{id}/schema-index, what the Data Explorer lists), one
// request per warehouse, made the first time a reader opens a table's columns.

import { useCallback, useMemo, useRef, useState, type ReactNode } from "react"
import { Loader2, Search } from "lucide-react"

import { cn } from "@/lib/utils"
import { authFetch } from "@/lib/api/auth-fetch"
import type { SchemaColumnLike, SchemaTableLike } from "@/lib/explorer/schemaTree"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { findWarehouseTable, type AssetNode } from "@/components/explorer/assetLineage"

/** Columns listed at once; a search or the pager reaches the rest. */
export const COLUMNS_PAGE_SIZE = 10

export type WarehouseTables =
  | { status: "loading" }
  | { status: "ready"; tables: SchemaTableLike[] }
  | { status: "error"; message: string }

export interface WarehouseTablesCache {
  tablesOf: (connectionId: string) => WarehouseTables | undefined
  /** Starts the request, unless one is running or has answered. A failed one is asked again. */
  load: (connectionId: string) => void
}

export function useWarehouseTables(): WarehouseTablesCache {
  const [byWarehouse, setByWarehouse] = useState<ReadonlyMap<string, WarehouseTables>>(new Map())
  // Read synchronously, so two tables opened at once make one request.
  const asked = useRef(new Map<string, WarehouseTables>())

  const load = useCallback((connectionId: string) => {
    const settle = (state: WarehouseTables) => {
      asked.current.set(connectionId, state)
      setByWarehouse(new Map(asked.current))
    }
    const known = asked.current.get(connectionId)
    if (known && known.status !== "error") return
    settle({ status: "loading" })
    authFetch(`/api/v1/explorer/connections/${encodeURIComponent(connectionId)}/schema-index`)
      .then(async (res) => {
        if (!res.ok) {
          settle({ status: "error", message: `The warehouse's schema could not be read (HTTP ${res.status}).` })
          return
        }
        const body = (await res.json()) as { tables?: unknown }
        settle({ status: "ready", tables: Array.isArray(body?.tables) ? (body.tables as SchemaTableLike[]) : [] })
      })
      .catch(() => settle({ status: "error", message: "Could not reach the server to read the warehouse's schema." }))
  }, [])

  return useMemo(() => ({ tablesOf: (id: string) => byWarehouse.get(id), load }), [byWarehouse, load])
}

/** What the cache holds for the warehouse a table is in. */
export function tablesFor(node: AssetNode, cache: WarehouseTablesCache): WarehouseTables | undefined {
  return node.connection_id ? cache.tablesOf(node.connection_id) : undefined
}

/** What a table's column list shows, from what its warehouse's schema says. */
type PanelContent =
  | { show: "note"; note: string }
  | { show: "loading" }
  | { show: "error"; message: string }
  | { show: "columns"; columns: SchemaColumnLike[] }

function panelContent(node: AssetNode, tables: WarehouseTables | undefined): PanelContent {
  if (!node.connection_id) return { show: "note", note: "The graph does not say which warehouse this table is in." }
  if (!tables || tables.status === "loading") return { show: "loading" }
  if (tables.status === "error") return { show: "error", message: tables.message }
  const lookup = findWarehouseTable(node.name, tables.tables)
  if (lookup.found === "none") {
    return { show: "note", note: "Not in this warehouse's schema. Refreshing it in the Data Explorer may find it." }
  }
  if (lookup.found === "many") {
    return { show: "note", note: `${lookup.count} tables in different schemas have this name, so which one is meant is unclear.` }
  }
  const columns = lookup.table.columns ?? []
  return columns.length === 0
    ? { show: "note", note: "The warehouse lists no columns for this table." }
    : { show: "columns", columns }
}

// The heights ColumnsPanel's parts are drawn at, from their classes.
/** A note: at most three lines of text-xs at a canvas card's width. */
const NOTE_HEIGHT = 48
/** h-6: the search box, the pager, Retry. */
const CONTROL_HEIGHT = 24
/** h-5: one column. */
const ROW_HEIGHT = 20
/** gap-1.5 between the search box, the list and the pager. */
const GAP = 6
/** space-y-2 between an error and its Retry. */
const RETRY_GAP = 8

/**
 * How tall ColumnsPanel is for this table, so a canvas card is laid out as tall as what it
 * shows. A search does not change it: the card keeps the room its unfiltered page takes.
 */
export function columnsPanelHeight(node: AssetNode, tables: WarehouseTables | undefined): number {
  const content = panelContent(node, tables)
  switch (content.show) {
    case "columns": {
      const rows = Math.min(content.columns.length, COLUMNS_PAGE_SIZE)
      const pager = content.columns.length > COLUMNS_PAGE_SIZE ? GAP + CONTROL_HEIGHT : 0
      return CONTROL_HEIGHT + GAP + rows * ROW_HEIGHT + pager
    }
    case "error":
      return NOTE_HEIGHT + RETRY_GAP + CONTROL_HEIGHT
    default:
      return NOTE_HEIGHT
  }
}

/**
 * The columns of the warehouse table a table asset names, searchable and ten at a time.
 * On the canvas it is inside a card the page hides from screen readers and a pointer can
 * drag, so its controls stay out of the tab order and keep the pointer to themselves.
 */
export function ColumnsPanel({
  node,
  tables,
  onRetry,
  inCanvas = false,
  className,
}: {
  node: AssetNode
  tables: WarehouseTables | undefined
  onRetry: () => void
  inCanvas?: boolean
  className?: string
}) {
  const [query, setQuery] = useState("")
  const [page, setPage] = useState(0)
  const control = inCanvas ? { tabIndex: -1 } : {}
  const controlClass = inCanvas ? "nodrag nopan" : undefined

  const content = panelContent(node, tables)
  let body: ReactNode
  if (content.show === "note") {
    body = <Note>{content.note}</Note>
  } else if (content.show === "loading") {
    body = (
      <Note>
        <Loader2 className="mr-1.5 inline h-3.5 w-3.5 animate-spin align-[-2px]" aria-hidden />
        Loading columns…
      </Note>
    )
  } else if (content.show === "error") {
    body = (
      <div className="space-y-2">
        <Note>{content.message}</Note>
        <Button size="sm" variant="outline" className={cn("h-6 px-2 text-xs", controlClass)} onClick={onRetry} {...control}>
          Retry
        </Button>
      </div>
    )
  } else {
    const { columns } = content
    const q = query.trim().toLowerCase()
    const matching = q ? columns.filter((c) => c.name.toLowerCase().includes(q)) : columns
    const pages = Math.max(1, Math.ceil(matching.length / COLUMNS_PAGE_SIZE))
    const at = Math.min(page, pages - 1)
    const shown = matching.slice(at * COLUMNS_PAGE_SIZE, (at + 1) * COLUMNS_PAGE_SIZE)
    body = (
      <div className="flex min-h-0 flex-1 flex-col gap-1.5">
        <div className="relative">
          <Search className="pointer-events-none absolute left-2 top-1.5 h-3.5 w-3.5 text-zinc-400" aria-hidden />
          <Input
            type="search"
            value={query}
            onChange={(e) => {
              setQuery(e.target.value)
              setPage(0)
            }}
            placeholder={`Find one of ${columns.length} columns`}
            aria-label={`Find a column in ${node.name}`}
            className={cn("h-6 pl-7 text-xs", controlClass)}
            {...control}
          />
        </div>
        {shown.length === 0 ? (
          <Note>No column matches.</Note>
        ) : (
          <ul aria-label={`Columns of ${node.name}`} className="min-h-0 flex-1 overflow-hidden text-xs">
            {shown.map((c) => (
              <li key={c.name} className="flex h-5 items-center gap-2">
                <span className="truncate font-mono text-zinc-900 dark:text-white" title={c.name}>
                  {c.name}
                </span>
                {c.is_primary_key && (
                  <span className="shrink-0 rounded bg-zinc-100 px-1 text-[10px] text-zinc-600 dark:bg-zinc-800 dark:text-zinc-300">
                    key
                  </span>
                )}
                <span className="ml-auto shrink-0 truncate text-[11px] text-zinc-500 dark:text-zinc-400" title={c.type}>
                  {c.type}
                </span>
              </li>
            ))}
          </ul>
        )}
        {pages > 1 && (
          <div className="flex items-center justify-between gap-2 text-[11px] text-zinc-500 dark:text-zinc-400">
            <span>
              {at * COLUMNS_PAGE_SIZE + 1}–{at * COLUMNS_PAGE_SIZE + shown.length} of {matching.length}
            </span>
            <span className="flex gap-1">
              <Button
                size="sm"
                variant="outline"
                className={cn("h-6 px-2 text-xs", controlClass)}
                disabled={at === 0}
                onClick={() => setPage(at - 1)}
                {...control}
              >
                Previous
              </Button>
              <Button
                size="sm"
                variant="outline"
                className={cn("h-6 px-2 text-xs", controlClass)}
                disabled={at >= pages - 1}
                onClick={() => setPage(at + 1)}
                {...control}
              >
                Next
              </Button>
            </span>
          </div>
        )}
      </div>
    )
  }

  return <div className={cn("flex flex-col", className)}>{body}</div>
}

function Note({ children }: { children: ReactNode }) {
  return <p className="text-xs text-zinc-500 dark:text-zinc-400">{children}</p>
}
