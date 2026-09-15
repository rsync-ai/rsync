"use client"

import { Fragment, useCallback, useEffect, useRef, useState } from "react"
import { AlertCircle, Braces, ChevronDown, ChevronRight, Loader2, Play, Table2 } from "lucide-react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Textarea } from "@/components/ui/textarea"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { authFetch } from "@/lib/api/auth-fetch"
import { cn } from "@/lib/utils"
import type { SchemaTableLike } from "@/lib/explorer/schemaTree"
import {
  buildFindBody,
  cellKind,
  DOCUMENT_FIND_DEFAULT_LIMIT,
  DOCUMENT_FIND_MAX_LIMIT,
  formatCell,
  mergeColumns,
  nextPageParams,
  normalizeResult,
  type CellKind,
  type DocumentFindResult,
  type FindField,
} from "@/lib/explorer/documentSpec"

export interface DocumentExplorerProps {
  connectionId: string
  /** Collections from the schema index (one entry per collection). */
  collections: SchemaTableLike[]
  collection: string
  onCollectionChange: (name: string) => void
  loadingCollections?: boolean
}

interface FindError {
  message: string
  field?: FindField
  path?: string
  code?: string
}

const KIND_CLASS: Record<CellKind, string> = {
  missing: "text-zinc-300 dark:text-zinc-600",
  null: "italic text-zinc-400",
  string: "",
  number: "text-sky-700 dark:text-sky-400",
  boolean: "text-amber-700 dark:text-amber-400",
  objectId: "text-violet-700 dark:text-violet-400",
  date: "text-emerald-700 dark:text-emerald-400",
  object: "text-zinc-500",
  array: "text-zinc-500",
  special: "text-rose-700 dark:text-rose-400",
}

/**
 * Document browse for MongoDB connections: a read-only find with filter, projection,
 * sort and paging, shown as a grid (one column per top-level field) or as raw JSON.
 */
export function DocumentExplorer({
  connectionId,
  collections,
  collection,
  onCollectionChange,
  loadingCollections,
}: DocumentExplorerProps) {
  const [filter, setFilter] = useState("")
  const [projection, setProjection] = useState("")
  const [sort, setSort] = useState("")
  const [limit, setLimit] = useState(DOCUMENT_FIND_DEFAULT_LIMIT)
  const [result, setResult] = useState<DocumentFindResult | null>(null)
  const [loading, setLoading] = useState(false)
  const [loadingMore, setLoadingMore] = useState(false)
  const [error, setError] = useState<FindError | null>(null)
  const [view, setView] = useState<"table" | "json">("table")
  const [expanded, setExpanded] = useState<Set<number>>(new Set())
  const abortRef = useRef<AbortController | null>(null)

  const runFind = useCallback(
    async (append: boolean) => {
      if (!collection) return
      const page = append && result ? nextPageParams(result) : null
      if (append && !page) return
      const built = buildFindBody({ connectionId, collection, filter, projection, sort, limit, ...(page ?? {}) })
      if (!built.ok) {
        setError({ message: built.error, field: built.field })
        return
      }

      abortRef.current?.abort()
      const ac = new AbortController()
      abortRef.current = ac
      setError(null)
      if (append) setLoadingMore(true)
      else setLoading(true)
      try {
        const res = await authFetch("/api/v1/explorer/documents/find", {
          method: "POST",
          headers: { "Content-Type": "application/json", Accept: "application/json" },
          body: built.body,
          signal: ac.signal,
        })
        const raw = await res.text()
        let data: Record<string, unknown> = {}
        try {
          data = raw ? JSON.parse(raw) : {}
        } catch {
          data = { error: "The server returned a non-JSON response." }
        }
        if (!res.ok) {
          setError({
            message: typeof data.error === "string" && data.error ? data.error : `Find failed (HTTP ${res.status})`,
            path: typeof data.path === "string" && data.path ? data.path : undefined,
            code: typeof data.error_code === "string" ? data.error_code : undefined,
          })
          if (!append) setResult(null)
          return
        }
        const next = normalizeResult(data)
        if (append) {
          setResult((prev) =>
            prev
              ? {
                  ...next,
                  documents: [...prev.documents, ...next.documents],
                  columns: mergeColumns(prev.columns, next.columns),
                  returned: prev.returned + next.returned,
                  warnings: next.warnings,
                }
              : next,
          )
        } else {
          setResult(next)
          setExpanded(new Set())
        }
      } catch (e) {
        if (ac.signal.aborted) return
        setError({ message: e instanceof Error ? e.message : "Find failed" })
      } finally {
        if (abortRef.current === ac) {
          setLoading(false)
          setLoadingMore(false)
        }
      }
    },
    [collection, connectionId, filter, projection, sort, limit, result],
  )

  // Browsing starts as soon as a collection is picked. The ref keeps the effect keyed
  // on the collection alone, so typing a filter does not re-run the find.
  const runFindRef = useRef(runFind)
  useEffect(() => {
    runFindRef.current = runFind
  }, [runFind])
  useEffect(() => {
    if (collection) void runFindRef.current(false)
  }, [connectionId, collection])
  useEffect(() => () => abortRef.current?.abort(), [])

  // Default to the first collection, and drop a pick that no longer exists.
  useEffect(() => {
    if (loadingCollections) return
    if (collections.some((c) => c.name === collection)) return
    onCollectionChange(collections[0]?.name ?? "")
  }, [collections, collection, loadingCollections, onCollectionChange])

  const onKeyDown = (e: React.KeyboardEvent) => {
    if ((e.metaKey || e.ctrlKey) && e.key === "Enter") {
      e.preventDefault()
      e.stopPropagation()
      void runFind(false)
    }
  }

  const toggleRow = (i: number) =>
    setExpanded((prev) => {
      const next = new Set(prev)
      if (next.has(i)) next.delete(i)
      else next.add(i)
      return next
    })

  const docs = result?.documents ?? []
  const shownForCollection = result && result.collection === collection
  const canLoadMore = Boolean(result && nextPageParams(result))

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader className="px-4 pt-4 pb-3">
          <CardTitle className="flex items-center gap-2 text-base">
            <Braces className="h-4 w-4 text-violet-500" />
            Document Browser
          </CardTitle>
          <CardDescription className="text-xs mt-1">
            Read-only find on a collection. Filters use MongoDB query syntax with Extended JSON values, e.g.{" "}
            <code className="font-mono">{`{"_id": {"$oid": "…"}}`}</code>. JavaScript operators such as $where are
            not allowed.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4 px-4 pb-4" onKeyDown={onKeyDown}>
          <div className="grid gap-3 sm:grid-cols-[1fr_120px]">
            <div className="space-y-1.5">
              <Label htmlFor="doc-collection" className="text-xs">
                Collection
              </Label>
              <select
                id="doc-collection"
                value={collection}
                onChange={(e) => onCollectionChange(e.target.value)}
                disabled={loadingCollections || collections.length === 0}
                className={cn(
                  "h-9 w-full rounded-md border border-input bg-background px-2 text-sm",
                  error?.field === "collection" && "border-red-400",
                )}
              >
                {collections.length === 0 && <option value="">No collections</option>}
                {collections.map((c) => (
                  <option key={`${c.schema ?? ""}.${c.name}`} value={c.name}>
                    {c.name}
                  </option>
                ))}
              </select>
            </div>
            <div className="space-y-1.5">
              <Label htmlFor="doc-limit" className="text-xs">
                Limit
              </Label>
              <Input
                id="doc-limit"
                type="number"
                min={1}
                max={DOCUMENT_FIND_MAX_LIMIT}
                value={limit}
                onChange={(e) => setLimit(Number(e.target.value))}
                className="h-9"
              />
            </div>
          </div>

          <JsonInput
            id="doc-filter"
            label="Filter"
            value={filter}
            onChange={setFilter}
            invalid={error?.field === "filter"}
            placeholder={`{ "status": "paid", "total": { "$gte": 100 } }`}
            rows={4}
          />
          <div className="grid gap-3 md:grid-cols-2">
            <JsonInput
              id="doc-projection"
              label="Projection"
              value={projection}
              onChange={setProjection}
              invalid={error?.field === "projection"}
              placeholder={`{ "status": 1, "total": 1 }`}
              rows={2}
            />
            <JsonInput
              id="doc-sort"
              label="Sort"
              value={sort}
              onChange={setSort}
              invalid={error?.field === "sort"}
              placeholder={`{ "created_at": -1 }`}
              rows={2}
            />
          </div>

          <div className="flex flex-wrap items-center justify-between gap-2">
            <span className="text-[11px] text-zinc-500">
              Cmd+Enter to run · the default _id order pages by cursor; a custom sort pages through the first 10,000
              documents
            </span>
            <Button size="sm" onClick={() => void runFind(false)} disabled={!collection || loading}>
              {loading ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <Play className="mr-2 h-4 w-4" />}
              Find
            </Button>
          </div>

          {error && (
            <div
              role="alert"
              className="flex gap-2 rounded-md border border-red-200 bg-red-50 p-3 text-xs text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300"
            >
              <AlertCircle className="mt-0.5 h-4 w-4 shrink-0" />
              <div className="min-w-0 break-words">
                <div className="font-medium">{error.message}</div>
                {error.path && <div className="mt-0.5 font-mono">at {error.path}</div>}
              </div>
            </div>
          )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader className="px-4 pt-4 pb-3">
          <div className="flex flex-wrap items-center justify-between gap-2">
            <div>
              <CardTitle className="text-base">Documents</CardTitle>
              <CardDescription className="text-xs mt-1" data-testid="doc-summary">
                {shownForCollection
                  ? `${docs.length} document${docs.length === 1 ? "" : "s"}${result.has_more ? " (more available)" : ""}` +
                    (result.execution_time_ms != null ? ` · ${result.execution_time_ms} ms` : "")
                  : "Pick a collection to browse its documents"}
              </CardDescription>
            </div>
            <div className="flex items-center gap-1">
              <Button
                variant={view === "table" ? "secondary" : "ghost"}
                size="sm"
                onClick={() => setView("table")}
                aria-pressed={view === "table"}
              >
                <Table2 className="mr-1.5 h-4 w-4" />
                Table
              </Button>
              <Button
                variant={view === "json" ? "secondary" : "ghost"}
                size="sm"
                onClick={() => setView("json")}
                aria-pressed={view === "json"}
              >
                <Braces className="mr-1.5 h-4 w-4" />
                JSON
              </Button>
            </div>
          </div>
        </CardHeader>
        <CardContent className="space-y-3 px-4 pb-4">
          {loading && !result ? (
            <div className="flex justify-center py-10">
              <Loader2 className="h-5 w-5 animate-spin text-violet-500" />
            </div>
          ) : !result ? null : docs.length === 0 ? (
            <div className="py-10 text-center text-sm text-zinc-500">No documents match this filter</div>
          ) : view === "json" ? (
            <pre className="max-h-[560px] overflow-auto rounded-md border bg-zinc-50 p-3 text-xs dark:bg-zinc-900">
              {JSON.stringify(docs, null, 2)}
            </pre>
          ) : (
            <div className="max-h-[560px] overflow-auto rounded-md border">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead className="w-8" />
                    {result.columns.map((col) => (
                      <TableHead key={col} className="whitespace-nowrap font-mono text-xs">
                        {col}
                      </TableHead>
                    ))}
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {docs.map((doc, i) => (
                    <Fragment key={i}>
                      <TableRow className="cursor-pointer" onClick={() => toggleRow(i)} data-testid="doc-row">
                        <TableCell className="w-8 px-2">
                          {expanded.has(i) ? (
                            <ChevronDown className="h-3.5 w-3.5 text-zinc-400" />
                          ) : (
                            <ChevronRight className="h-3.5 w-3.5 text-zinc-400" />
                          )}
                        </TableCell>
                        {result.columns.map((col) => {
                          const value = doc[col]
                          const kind = cellKind(value)
                          return (
                            <TableCell
                              key={col}
                              className={cn("max-w-[280px] truncate whitespace-nowrap font-mono text-xs", KIND_CLASS[kind])}
                              title={kind === "missing" ? "Field not present in this document" : formatCell(value)}
                            >
                              {kind === "missing" ? "—" : formatCell(value)}
                            </TableCell>
                          )
                        })}
                      </TableRow>
                      {expanded.has(i) && (
                        <TableRow>
                          <TableCell colSpan={result.columns.length + 1} className="bg-zinc-50 dark:bg-zinc-900">
                            <pre className="max-h-[360px] overflow-auto text-xs">{JSON.stringify(doc, null, 2)}</pre>
                          </TableCell>
                        </TableRow>
                      )}
                    </Fragment>
                  ))}
                </TableBody>
              </Table>
            </div>
          )}

          {result && (result.warnings.length > 0 || result.truncated_bytes) && (
            <ul className="space-y-1 text-xs text-amber-700 dark:text-amber-400">
              {result.truncated_bytes && (
                <li>This page stopped early to stay under the response size limit; add a projection to see more per page.</li>
              )}
              {result.warnings.map((w, i) => (
                <li key={i}>{w}</li>
              ))}
            </ul>
          )}

          {canLoadMore && (
            <div className="flex justify-center">
              <Button variant="outline" size="sm" onClick={() => void runFind(true)} disabled={loadingMore || loading}>
                {loadingMore && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
                Load more
              </Button>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  )
}

function JsonInput({
  id,
  label,
  value,
  onChange,
  invalid,
  placeholder,
  rows,
}: {
  id: string
  label: string
  value: string
  onChange: (v: string) => void
  invalid?: boolean
  placeholder: string
  rows: number
}) {
  return (
    <div className="space-y-1.5">
      <Label htmlFor={id} className="text-xs">
        {label}
      </Label>
      <Textarea
        id={id}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={placeholder}
        rows={rows}
        spellCheck={false}
        aria-invalid={invalid || undefined}
        className={cn("min-h-0 font-mono text-xs", invalid && "border-red-400 focus-visible:ring-red-400")}
      />
    </div>
  )
}
