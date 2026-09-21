"use client"

/**
 * SuggestTablesCard: the Data Explorer's empty state. It asks what the user wants to
 * find out and suggests the tables most likely to hold it, via
 * POST /api/v1/explorer/connections/:id/tables/recommend (explorer.go
 * GetRecommendedTablesForExplorer → connections.go GetRecommendedTables).
 *
 * The request runs a schema discovery and then an LLM ranking, so it can take a while and
 * only runs when asked. When the LLM is unreachable the backend falls back to a keyword
 * match on table names (ranking_method "heuristic"), and the card says so, because those
 * suggestions are much rougher.
 */

import { useState } from "react"
import { AlertCircle, Check, Loader2, Plus, RefreshCw, Sparkles, SquareTerminal } from "lucide-react"

import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { authFetch } from "@/lib/api/auth-fetch"

export interface TableSuggestion {
  name: string
  schema?: string
  row_count: number
  key_columns: string[]
  reason: string
  category: string
  has_pii: boolean
}

export interface TableSuggestions {
  intent: string
  recommendations: TableSuggestion[]
  total_available: number
  ranking_method: string
}

export const MAX_SUGGESTIONS = 8

/** The response, defaulted: a Go nil slice arrives as null. */
export function parseSuggestions(body: unknown): TableSuggestions {
  const b = (body ?? {}) as Record<string, unknown>
  const recs = Array.isArray(b.recommendations) ? (b.recommendations as Record<string, unknown>[]) : []
  return {
    intent: String(b.intent ?? ""),
    recommendations: recs
      .filter((r) => typeof r?.name === "string" && r.name !== "")
      .map((r) => ({
        name: String(r.name),
        schema: typeof r.schema === "string" && r.schema !== "" ? r.schema : undefined,
        row_count: typeof r.row_count === "number" && Number.isFinite(r.row_count) ? r.row_count : 0,
        key_columns: Array.isArray(r.key_columns) ? r.key_columns.map(String) : [],
        reason: String(r.reason ?? ""),
        category: String(r.category ?? ""),
        has_pii: r.has_pii === true,
      })),
    total_available: typeof b.total_available === "number" ? b.total_available : 0,
    ranking_method: String(b.ranking_method ?? ""),
  }
}

/** schema.name, as the schema browser writes it into the editor (SchemaBrowser.tsx qualify). */
export function qualifiedName(t: { name: string; schema?: string }): string {
  return t.schema ? `${t.schema}.${t.name}` : t.name
}

function categoryLabel(category: string): string {
  const c = category.replace(/_/g, " ").trim()
  return c === "" || c === "general" ? "" : c
}

type State =
  | { phase: "idle" }
  | { phase: "loading" }
  | { phase: "error"; message: string }
  | { phase: "done"; result: TableSuggestions }

async function errorMessage(res: Response): Promise<string> {
  let body: Record<string, unknown> = {}
  try {
    body = ((await res.json()) ?? {}) as Record<string, unknown>
  } catch {
    // not JSON; the status says enough
  }
  const error = typeof body.error === "string" ? body.error : ""
  const details = typeof body.details === "string" ? body.details : ""
  if (error && details) return `${error}: ${details}`
  if (error) return error
  return `Could not suggest tables (HTTP ${res.status}).`
}

export function SuggestTablesCard({
  connectionId,
  selectedTables,
  tableKey,
  onToggleTable,
  onStartQuery,
}: {
  connectionId: string
  /** Tables picked for the AI prompt, by `tableKey`. */
  selectedTables: string[]
  tableKey: (t: { name: string; schema?: string }) => string
  onToggleTable: (key: string) => void
  /** Puts a statement in the empty SQL editor. */
  onStartQuery: (sql: string) => void
}) {
  const [intent, setIntent] = useState("")
  const [state, setState] = useState<State>({ phase: "idle" })

  const suggest = async () => {
    setState({ phase: "loading" })
    try {
      const res = await authFetch(
        `/api/v1/explorer/connections/${encodeURIComponent(connectionId)}/tables/recommend`,
        { method: "POST", body: JSON.stringify({ intent: intent.trim(), max_tables: MAX_SUGGESTIONS }) },
      )
      if (!res.ok) {
        setState({ phase: "error", message: await errorMessage(res) })
        return
      }
      setState({ phase: "done", result: parseSuggestions(await res.json()) })
    } catch {
      setState({ phase: "error", message: "Could not reach the server to suggest tables." })
    }
  }

  const loading = state.phase === "loading"

  return (
    <Card data-testid="suggest-tables">
      <CardHeader className="px-4 pt-4 pb-3">
        <CardTitle className="flex items-center gap-2 text-base">
          <Sparkles className="h-4 w-4 text-violet-500" aria-hidden />
          Not sure which table to start with?
        </CardTitle>
        <CardDescription>
          Say what you want to find out, and get the tables in this connection most likely to hold it.
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-4 px-4 pb-4">
        <form
          className="flex flex-col gap-2 sm:flex-row"
          onSubmit={(e) => {
            e.preventDefault()
            if (!loading) void suggest()
          }}
        >
          <Input
            value={intent}
            onChange={(e) => setIntent(e.target.value)}
            placeholder="e.g. monthly revenue by customer (optional)"
            aria-label="What do you want to find out?"
            maxLength={500}
            className="sm:flex-1"
          />
          <Button type="submit" disabled={loading} className="shrink-0">
            {loading ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <Sparkles className="mr-2 h-4 w-4" />}
            Suggest tables
          </Button>
        </form>

        {state.phase === "loading" && (
          <p role="status" className="text-sm text-muted-foreground">
            Reading the schema and ranking its tables. On a large database this can take a minute.
          </p>
        )}

        {state.phase === "error" && (
          <div role="alert" className="flex flex-wrap items-center gap-3 text-sm text-red-700 dark:text-red-400">
            <AlertCircle className="h-4 w-4 shrink-0" aria-hidden />
            <span className="min-w-0 flex-1 break-words">{state.message}</span>
            <Button variant="outline" size="sm" onClick={() => void suggest()}>
              <RefreshCw className="mr-2 h-3.5 w-3.5" />
              Retry
            </Button>
          </div>
        )}

        {state.phase === "done" && (
          <SuggestionList
            result={state.result}
            selectedTables={selectedTables}
            tableKey={tableKey}
            onToggleTable={onToggleTable}
            onStartQuery={onStartQuery}
          />
        )}
      </CardContent>
    </Card>
  )
}

function SuggestionList({
  result,
  selectedTables,
  tableKey,
  onToggleTable,
  onStartQuery,
}: {
  result: TableSuggestions
  selectedTables: string[]
  tableKey: (t: { name: string; schema?: string }) => string
  onToggleTable: (key: string) => void
  onStartQuery: (sql: string) => void
}) {
  const heuristic = result.ranking_method === "heuristic"
  const recs = result.recommendations

  if (recs.length === 0) {
    return (
      <p className="text-sm text-muted-foreground">
        No table stood out
        {result.intent ? (
          <>
            {" "}
            for <span className="font-medium text-foreground">{result.intent}</span>
          </>
        ) : null}
        . Try describing it another way, or browse the schema on the left.
      </p>
    )
  }

  return (
    <div className="space-y-3">
      {heuristic && (
        <p className="rounded-md border border-amber-300 bg-amber-50 px-3 py-2 text-xs text-amber-900 dark:border-amber-800 dark:bg-amber-950/30 dark:text-amber-200">
          The AI ranking was unavailable, so these were picked by matching table names against a few
          keywords and by size. Treat them as a rough start.
        </p>
      )}
      <ul aria-label="Suggested tables" className="divide-y rounded-md border">
        {recs.map((t) => {
          const qualified = qualifiedName(t)
          const key = tableKey(t)
          const picked = selectedTables.includes(key)
          const category = categoryLabel(t.category)
          return (
            <li key={qualified} data-suggestion={qualified} className="space-y-1.5 px-3 py-2.5">
              <div className="flex flex-wrap items-center gap-x-2 gap-y-1">
                <span className="break-all font-mono text-sm font-medium">{qualified}</span>
                {t.row_count > 0 && (
                  <span className="text-xs text-muted-foreground">{t.row_count.toLocaleString()} rows</span>
                )}
                {category && (
                  <Badge variant="outline" className="text-[10px] capitalize">
                    {category}
                  </Badge>
                )}
                {t.has_pii && (
                  <Badge variant="warning" className="text-[10px]">
                    May hold personal data
                  </Badge>
                )}
              </div>
              {t.reason && <p className="text-sm text-muted-foreground">{t.reason}</p>}
              {t.key_columns.length > 0 && (
                <p className="text-xs text-muted-foreground">
                  Columns: <span className="font-mono">{t.key_columns.join(", ")}</span>
                </p>
              )}
              <div className="flex flex-wrap gap-2 pt-1">
                <Button
                  variant="outline"
                  size="sm"
                  className="h-7 text-xs"
                  title={`Put SELECT * FROM ${qualified} LIMIT 100 in the editor`}
                  aria-label={`Start a query on ${qualified}`}
                  onClick={() => onStartQuery(`SELECT * FROM ${qualified} LIMIT 100`)}
                >
                  <SquareTerminal className="mr-1.5 h-3.5 w-3.5" />
                  Start a query
                </Button>
                <Button
                  variant={picked ? "secondary" : "outline"}
                  size="sm"
                  className="h-7 text-xs"
                  aria-pressed={picked}
                  aria-label={`Use ${qualified} in the AI prompt`}
                  title="The AI writes SQL against the tables you pick"
                  onClick={() => onToggleTable(key)}
                >
                  {picked ? <Check className="mr-1.5 h-3.5 w-3.5" /> : <Plus className="mr-1.5 h-3.5 w-3.5" />}
                  {picked ? "In the AI prompt" : "Use in AI prompt"}
                </Button>
              </div>
            </li>
          )
        })}
      </ul>
      <p className="text-xs text-muted-foreground">
        {recs.length} of {result.total_available.toLocaleString()} tables.{" "}
        {heuristic
          ? "Ranked by name and size."
          : "Ranked by AI from table names, column names and row counts; no row values are sent."}
      </p>
    </div>
  )
}
