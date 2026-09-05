"use client"

import { useState, useEffect, useCallback, useMemo, useRef } from "react"
import { PageHeader } from "@/components/layout/PageHeader"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Badge } from "@/components/ui/badge"
import { Textarea } from "@/components/ui/textarea"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import {
  Database,
  Play,
  History,
  Bookmark,
  Code,
  Loader2,
  Sparkles,
  AlertCircle,
  CheckCircle2,
  RefreshCw,
  Clock,
  Copy,
  Download,
  X,
  BarChart3,
  ExternalLink,
  GitBranch,
  Server,
  PanelLeftClose,
  PanelLeftOpen,
} from "lucide-react"
import { Switch } from "@/components/ui/switch"
import { toast } from "sonner"
import { authFetch } from "@/lib/api/auth-fetch"
import { API_ENDPOINTS } from "@/lib/config/api"
import { cn } from "@/lib/utils"
import { useWorkspaceRole } from "@/contexts/WorkspaceContext"
import { captureWorkspace, onActiveWorkspaceChange } from "@/lib/workspace/active-workspace"
import {
  canRunExplorerStatement,
  classifyExplorerStatement,
  destructiveLabel,
  firstSqlVerb,
  minRoleForStmtClass,
} from "@/lib/explorer/statementClass"
import {
  resolveRunTarget,
  spliceRunTarget,
  type RunTarget,
} from "@/lib/explorer/sqlStatements"
import {
  ExplorerStepTimeline,
  ExplorerRun,
  ExplorerStep,
  createInitialExplorerSteps,
  updateExplorerStep,
  HITLTablePicker,
  TableCandidate as HITLTableCandidate,
  HITLMetricPicker,
  type MetricChoice,
  SqlEditor,
  type SqlEditorHandle,
  NlExamplePrompts,
  SchemaBrowser,
  SavedQueries,
} from "@/components/explorer"
import { groupTablesByDatabase } from "@/lib/explorer/schemaTree"
import { handleExplorerRunShortcut } from "@/lib/explorer/runShortcut"
import { isInternalExplorerTable } from "@/lib/explorer/internalTables"

// Types
interface Connection {
  id: string
  name: string
  connector_type: string
  type: string
  status: string
  is_connected?: boolean
  // Backend-computed Data Explorer capability (ResolveExplorerCapability). The
  // dropdown filters on supports_explorer instead of a hardcoded connector allowlist,
  // so a new warehouse becomes explorable server-side with no frontend change.
  supports_explorer?: boolean
  explorer_mode?: string // "sql" (document mode reserved for a future Document Explorer)
  sql_dialect?: string
}

interface TableMetadata {
  name: string
  schema?: string
  row_count?: number
  columns?: ColumnMetadata[]
}

interface ColumnMetadata {
  name: string
  type: string
  nullable?: boolean
  is_primary_key?: boolean
}

interface ForeignKeyMetadata {
  from_schema?: string
  from_table: string
  from_column: string
  to_schema?: string
  to_table: string
  to_column: string
  confidence?: number
}

interface ExplorerError {
  code: string   // "auth" | "llm_offline" | "db_error" | "table_resolve_failed" | "sql_gen_failed" | "network" | "validation" | "schema_failed" | "unknown"
  title: string  // shown as the error heading
  message: string // raw server message or fallback
  hint?: string  // actionable suggestion shown below
}

// Carry a structured ExplorerError through throw so the catch block
// can set it directly rather than re-parsing a plain string.
class ExplorerApiError extends Error {
  constructor(public readonly explorerError: ExplorerError) {
    super(explorerError.message)
    this.name = "ExplorerApiError"
  }
}

interface QueryResult {
  columns: string[]
  rows: Record<string, unknown>[]
  row_count: number
  execution_time_ms: number
  truncated: boolean
  warnings?: string[]
  // Set for write statements: the number of rows the statement changed (undefined for
  // reads and for engines that don't report a count).
  rows_affected?: number
  // The classified statement (SELECT, INSERT, DROP, …) — drives write-vs-grid rendering.
  statement_type?: string
}

interface QueryHistory {
  id: string
  question?: string
  sql: string
  timestamp: Date
  executionTimeMs?: number
  rowCount?: number
}

// The three exploration panes. "history" is the automatic, per-browser
// localStorage scratchpad; "saved" is the workspace-scoped server resource
// (migration 084). They coexist on purpose — one is disposable, one is shared.
type ExplorerPanelTab = "steps" | "history" | "saved"

type ExportFormat = "csv" | "tsv" | "json" | "xlsx"

// Tiny English-only pluralization helper. Centralized so every label
// across the page reads "1 row" vs "12 rows" the same way; locale-aware
// formatting (commas, narrow non-breaking space) is bundled in.
function pluralizeRows(n: number): string {
  const count = new Intl.NumberFormat("en-US").format(n)
  return `${count} ${n === 1 ? "row" : "rows"}`
}

type VisualizationTool = "metabase" | "superset" | "looker" | "powerbi" | "tableau" | "grafana"

interface VisualizationToolDef {
  id: VisualizationTool
  name: string
  description: string
  implemented: boolean
  accentClass: string
}

const VISUALIZATION_TOOLS: VisualizationToolDef[] = [
  { id: "metabase", name: "Metabase",  description: "Open-source BI",         implemented: true,  accentClass: "border-blue-400 bg-blue-50 dark:bg-blue-950 text-blue-700 dark:text-blue-300" },
  // Superset export is not wired: the client posts to /proxy/explorer/superset/dashboard, which does not exist (only metabase does). Marked unimplemented until a real proxy route ships.
  { id: "superset", name: "Superset",  description: "Apache data exploration", implemented: false, accentClass: "border-cyan-400 bg-cyan-50 dark:bg-cyan-950 text-cyan-700 dark:text-cyan-300" },
  { id: "looker",   name: "Looker",    description: "Google BI platform",      implemented: false, accentClass: "border-indigo-400 bg-indigo-50 dark:bg-indigo-950 text-indigo-700 dark:text-indigo-300" },
  { id: "powerbi",  name: "Power BI",  description: "Microsoft analytics",     implemented: false, accentClass: "border-yellow-400 bg-yellow-50 dark:bg-yellow-950 text-yellow-700 dark:text-yellow-300" },
  { id: "tableau",  name: "Tableau",   description: "Visual analytics",        implemented: false, accentClass: "border-orange-400 bg-orange-50 dark:bg-orange-950 text-orange-700 dark:text-orange-300" },
]

function normalizeTableName(input: unknown): string {
  const raw = String(input ?? "").trim()
  if (!raw) return ""
  // Handle schema.table or db.schema.table by taking the last segment.
  const last = raw.split(".").pop() || ""
  // Strip common quoting.
  return last.replace(/^["'`]+/, "").replace(/["'`]+$/, "").trim()
}

function parseQualifiedTable(input: unknown): { schema: string; table: string } {
  const raw = String(input ?? "").trim()
  if (!raw) return { schema: "", table: "" }
  const parts = raw.split(".").map((p) => p.trim()).filter(Boolean)
  if (parts.length <= 1) return { schema: "", table: normalizeTableName(raw) }
  const table = normalizeTableName(parts[parts.length - 1])
  const schema = parts.slice(0, -1).join(".").replace(/^["'`]+/, "").replace(/["'`]+$/, "").trim()
  return { schema, table }
}

// True when the connection is a MySQL/MariaDB family source. These are
// per-server connections where each "namespace" is a sibling database, so the
// Explorer can offer a "show all databases" scope. Postgres namespaces are
// schemas under one database and are always fully browsable.
function isMySQLFamily(conn?: { connector_type?: string; type?: string }): boolean {
  if (!conn) return false
  return /mysql|mariadb/i.test(`${conn.connector_type || ""} ${conn.type || ""}`)
}

function tableKeyFromMeta(t: { name: string; schema?: string }): string {
  const table = normalizeTableName(t.name)
  const schema = String(t.schema || "").trim()
  return schema ? `${schema}.${table}` : table
}

function normalizeTableKey(input: unknown): string {
  const { schema, table } = parseQualifiedTable(input)
  if (!table) return ""
  return schema ? `${schema}.${table}` : table
}

function qualifyTableKeyIfPossible(input: unknown, allTables: TableMetadata[]): string {
  const key = normalizeTableKey(input)
  if (!key) return ""
  const parsed = parseQualifiedTable(key)
  if (parsed.schema) return `${parsed.schema}.${parsed.table}`

  // No schema: if the table name is unique in this connection's schema index,
  // we can safely upgrade it to schema.table.
  const matches = (allTables || []).filter((t) => normalizeTableName(t.name) === parsed.table)
  if (matches.length === 1) return tableKeyFromMeta(matches[0])
  return parsed.table
}

function formatTableKeyForDisplay(key: string): string {
  const { schema, table } = parseQualifiedTable(key)
  return schema ? `${schema}.${table}` : table
}

function qualifySqlUsingSelectedTables(sql: string, selectedTableKeys: string[]): string {
  const mappings = (selectedTableKeys || [])
    .map((k) => parseQualifiedTable(k))
    .filter((p) => p.schema && p.table)

  if (mappings.length === 0) return sql

  // Best-effort qualification for common SELECT patterns.
  // We only rewrite unqualified identifiers in FROM/JOIN positions.
  const re = /\b(from|join)\s+([a-zA-Z0-9_]+)\b/gi
  return sql.replace(re, (full, kw: string, ident: string) => {
    const m = mappings.find((x) => x.table.toLowerCase() === String(ident).toLowerCase())
    if (!m) return full
    return `${kw} ${m.schema}.${m.table}`
  })
}

export default function ExplorerPage() {
  const DEFAULT_ROW_LIMIT = 100
  const MAX_ROW_LIMIT = 1000
  const SCHEMA_CACHE_TTL_MS = 15 * 60 * 1000

  // Connection state
  const [connections, setConnections] = useState<Connection[]>([])
  const [selectedConnection, setSelectedConnection] = useState<string>("")
  const [loadingConnections, setLoadingConnections] = useState(true)

  // Schema state
  const [tables, setTables] = useState<TableMetadata[]>([])
  const [foreignKeys, setForeignKeys] = useState<ForeignKeyMetadata[]>([])

  // Internal `_rsync_*` bookkeeping tables are noise in the explorer UI; hide
  // them from the schema tree, NL example chips, and SQL autocomplete. The raw
  // `tables` list stays available for selection / key logic. (I2b)
  const visibleTables = useMemo(
    () => tables.filter((t) => !isInternalExplorerTable(t.name)),
    [tables],
  )
  const [loadingSchema, setLoadingSchema] = useState(false)
  const [schemaError, setSchemaError] = useState<ExplorerError | null>(null)
  // CachedLabel: true when the visible schema came from the in-memory
  // schemaCacheRef rather than a fresh fetch. Shown as a small "Cached"
  // pill near the Tables count so the user knows whether to hit the
  // refresh button before composing queries against potentially-stale
  // schema. Reset to false on any fresh load + on connection change.
  const [schemaFromCache, setSchemaFromCache] = useState(false)
  // Age of the cached schema being displayed, so the badge can read "Cached ·
  // 7m" instead of a bare "Cached". "Cached" alone doesn't tell you whether to
  // distrust the row counts; the age does.
  const [schemaCachedAt, setSchemaCachedAt] = useState<number | null>(null)
  // Ticks only while a cached schema is on screen, so the age label doesn't
  // freeze at whenever the page last happened to re-render.
  const [schemaCacheAgeTick, setSchemaCacheAgeTick] = useState(0)
  useEffect(() => {
    if (schemaCachedAt === null) return
    const t = window.setInterval(() => setSchemaCacheAgeTick((n) => n + 1), 30_000)
    return () => window.clearInterval(t)
  }, [schemaCachedAt])
  const schemaCacheAgeLabel = useMemo(() => {
    void schemaCacheAgeTick
    if (schemaCachedAt === null) return ""
    const mins = Math.floor((Date.now() - schemaCachedAt) / 60_000)
    return mins < 1 ? "" : `${mins}m`
  }, [schemaCachedAt, schemaCacheAgeTick])
  const [selectedTables, setSelectedTables] = useState<string[]>([])
  // allDatabases (MySQL only) switches the schema-index fetch to server scope
  // (every database, not just the configured one). Tree expansion + search now
  // live inside <SchemaBrowser>.
  const [allDatabases, setAllDatabases] = useState(false)

  // Cache schema + analysis per connection (in-memory, per session).
  const schemaCacheRef = useRef(
    new Map<
      string,
      {
        tables: TableMetadata[]
        foreignKeys: ForeignKeyMetadata[]
        cachedAt: number
      }
    >()
  )
  const tableIndexCacheRef = useRef(new Map<string, { tablesRef: TableMetadata[]; index: TableIndex }>())

  // Query state
  const [nlInput, setNlInput] = useState("")
  const [sqlQuery, setSqlQuery] = useState("")
  const [generatingSql, setGeneratingSql] = useState(false)
  const [executingQuery, setExecutingQuery] = useState(false)
  const [queryResult, setQueryResult] = useState<QueryResult | null>(null)
  const [queryError, setQueryError] = useState<ExplorerError | null>(null)
  // Destructive statements (DROP/TRUNCATE) require an owner AND a typed confirmation.
  const [showDestructiveConfirm, setShowDestructiveConfirm] = useState(false)
  const [destructiveConfirmText, setDestructiveConfirmText] = useState("")
  // The run target the destructive dialog is holding. Captured when the dialog opens so
  // the typed-confirmation and the eventual execute both refer to the statement the user
  // was looking at, even if the caret moves behind the modal.
  const [pendingRunTarget, setPendingRunTarget] = useState<RunTarget | null>(null)
  // The editor's caret/selection, mirrored so the Run button, the statement badge and the
  // role warning can all describe the statement that will actually run.
  const [editorSelection, setEditorSelection] = useState<{ from: number; to: number }>({
    from: 0,
    to: 0,
  })
  // The single statement a Run action would execute: the selection, else the statement
  // under the caret, else the whole buffer. EVERY gate below reads runTarget.sql — never
  // sqlQuery — because with several queries in the buffer those are different strings and
  // gating on the wrong one is how a DROP under the caret slips past a confirm dialog that
  // only ever saw the leading SELECT.
  const runTarget = useMemo(
    () => resolveRunTarget(sqlQuery, editorSelection.from, editorSelection.to),
    [sqlQuery, editorSelection.from, editorSelection.to]
  )
  // The caller's workspace role gates which statement classes can run (advisory; the
  // api-gateway independently enforces via validators.ValidateExplorerStatement).
  const { role: workspaceRole } = useWorkspaceRole()
  const currentStmtClass = classifyExplorerStatement(runTarget.sql)
  const canRunCurrentStatement = canRunExplorerStatement(workspaceRole, runTarget.sql)
  // Stable identity: SqlEditor holds this in a memoized extension array, so a new
  // function every render would rebuild the CodeMirror extensions on every keystroke.
  const handleEditorSelectionChange = useCallback(
    (sel: { from: number; to: number }) => setEditorSelection(sel),
    []
  )
  // Name what Run will actually do. With one query this stays "Run Query"; with several
  // it has to say which, or the button is a coin flip.
  const runButtonLabel = useMemo(() => {
    if (runTarget.multi) return "Run Query"
    if (runTarget.source === "selection") {
      return currentStmtClass === "destructive" ? "Run Selection…" : "Run Selection"
    }
    if (runTarget.total > 1) {
      return currentStmtClass === "destructive"
        ? `Run Statement ${runTarget.index}…`
        : `Run Statement ${runTarget.index}`
    }
    return currentStmtClass === "destructive" ? "Run Query…" : "Run Query"
  }, [runTarget.multi, runTarget.source, runTarget.total, runTarget.index, currentStmtClass])
  const [executionLimit, setExecutionLimit] = useState<number>(DEFAULT_ROW_LIMIT)
  const [exportingFormat, setExportingFormat] = useState<ExportFormat | null>(null)

  // History state
  const [queryHistory, setQueryHistory] = useState<QueryHistory[]>([])
  const [showHistory, setShowHistory] = useState(false)

  // Cell viewer (for long values so UI truncation isn't confused with masking)
  const [cellViewerOpen, setCellViewerOpen] = useState(false)
  const [cellViewerTitle, setCellViewerTitle] = useState<string>("")
  const [cellViewerValue, setCellViewerValue] = useState<string>("")

  // BI dashboard creation state
  const [creatingDashboard, setCreatingDashboard] = useState(false)
  const [showDashboardDialog, setShowDashboardDialog] = useState(false)
  const [dashboardName, setDashboardName] = useState("")
  const [dashboardDescription, setDashboardDescription] = useState("")
  const [selectedBiTool, setSelectedBiTool] = useState<VisualizationTool>("metabase")

  // Exploration run state (for step DAG/timeline)
  const [currentRun, setCurrentRun] = useState<ExplorerRun | null>(null)
  const [rightPanelTab, setRightPanelTab] = useState<ExplorerPanelTab>("steps")

  // The schema browser (left rail) collapses to a thin strip so power users
  // writing raw SQL can reclaim the full editor width. Open by default.
  const [leftRailOpen, setLeftRailOpen] = useState(true)

  // HITL (Tables)
  const [tableHITLOpen, setTableHITLOpen] = useState(false)
  const [tableHITLCandidates, setTableHITLCandidates] = useState<HITLTableCandidate[]>([])

  // HITL (Metric)
  const [metricHITLOpen, setMetricHITLOpen] = useState(false)
  const [metricChoice, setMetricChoice] = useState<MetricChoice | null>(null)
  const [metricHitlOptions, setMetricHitlOptions] = useState<
    Array<{ id: MetricChoice; title: string; description: string; default?: boolean }>
  >([])
  const pendingGenerateRef = useRef<{ tablesOverride?: string[] } | null>(null)
  const metricChoiceRef = useRef<MetricChoice | null>(null)
  const lastMetricQuestionRef = useRef<string>("")

  const nlTextareaRef = useRef<HTMLTextAreaElement>(null)
  // Imperative handle on the CodeMirror SQL editor — used by the
  // sidebar's "insert table/column at cursor" buttons. Without this
  // the click-to-insert flow would require replicating CodeMirror's
  // selection API at the page level.
  const sqlEditorRef = useRef<SqlEditorHandle>(null)
  // AbortController for the currently-running query. Replaced on each
  // Run Query press; .abort() cancels both the fetch AND the
  // server-side query via the request context (the Go driver honors
  // ctx.Done() on connections).
  const executeAbortRef = useRef<AbortController | null>(null)
  // Target for the top-right Steps/History buttons. Without this, clicking
  // those buttons changes rightPanelTab state but the Exploration Details
  // card sits below the query editor + results, so the user sees no visible
  // change. We smooth-scroll the panel into view after the tab switch.
  const explorationPanelRef = useRef<HTMLDivElement>(null)

  // Set rightPanelTab AND scroll the Exploration Details card into view.
  // Wrapped in requestAnimationFrame so the scroll happens after React
  // flushes the tab-state render (otherwise the ref's bounding box can be
  // stale and the browser scrolls to the wrong position).
  const focusExplorationPanel = useCallback((tab: ExplorerPanelTab) => {
    setRightPanelTab(tab)
    if (tab === "history") {
      setShowHistory(true)
    }
    requestAnimationFrame(() => {
      explorationPanelRef.current?.scrollIntoView({ behavior: "smooth", block: "start" })
    })
  }, [])

  const HISTORY_KEY_PREFIX = "rsync_explorer_history_v1"

  const loadConnections = useCallback(async () => {
    // A switch mid-flight makes this response the previous workspace's list.
    const isStale = captureWorkspace()
    try {
      setLoadingConnections(true)
      const res = await authFetch(API_ENDPOINTS.CONNECTIONS.LIST)
      const data = await res.json()
      if (isStale()) return
      // The backend computes supports_explorer per connection (ResolveExplorerCapability)
      // and is the single source of truth — a new warehouse becomes explorable with a
      // resolver entry and no frontend edit. Fall back to substring matching only for
      // older gateways that don't yet emit the flag. Non-SQL connectors (mongodb,
      // object stores, SaaS) resolve supports_explorer=false and are excluded — they
      // 400 on query.
      const sqlConnectors = (data.connections || []).filter((c: Connection) => {
        if (typeof c.supports_explorer === "boolean") return c.supports_explorer
        const t = (c.connector_type || "").toLowerCase()
        return t.includes("postgres") || t.includes("mysql") || t.includes("mariadb")
          || t.includes("redshift") || t.includes("databricks")
          || t.includes("sqlserver") || t.includes("mssql") || t.includes("bigquery")
      })

      const usable = sqlConnectors.sort((a: Connection, b: Connection) => {
        // Sources before destinations; within each type, tested-success connections first, then stable by name.
        if (a.type !== b.type) return a.type === "source" ? -1 : 1
        const aOk = a.is_connected ? 0 : 1
        const bOk = b.is_connected ? 0 : 1
        if (aOk !== bOk) return aOk - bOk
        return String(a.name || "").localeCompare(String(b.name || ""))
      })

      setConnections(usable)

      // Auto-select a default connection.
      if (usable.length > 0 && !selectedConnection) {
        setSelectedConnection(usable[0].id)
      }
      if (usable.length === 0) {
        toast.message("No queryable connections found", {
          description: "Create a PostgreSQL, MySQL, SQL Server, Databricks, or Redshift connection (source or destination) to use Explorer.",
        })
      }
    } catch (err) {
      const e = classifyApiError(
        (err as { statusCode?: number })?.statusCode,
        { error: err instanceof Error ? err.message : String(err) },
        "connections"
      )
      toast.error(e.title, { description: e.hint ?? e.message })
    } finally {
      setLoadingConnections(false)
    }
  }, [selectedConnection])

  // Load per-connection query history from localStorage
  useEffect(() => {
    if (!selectedConnection) return
    const key = `${HISTORY_KEY_PREFIX}:${selectedConnection}`
    try {
      const raw = localStorage.getItem(key)
      if (!raw) {
        setQueryHistory([])
        return
      }
      const parsed = JSON.parse(raw)
      if (!Array.isArray(parsed)) {
        setQueryHistory([])
        return
      }
      const hydrated: QueryHistory[] = parsed
        .map((e: any) => ({
          id: String(e.id || ""),
          question: e.question ? String(e.question) : undefined,
          sql: String(e.sql || ""),
          timestamp: new Date(e.timestamp || Date.now()),
          executionTimeMs: typeof e.executionTimeMs === "number" ? e.executionTimeMs : undefined,
          rowCount: typeof e.rowCount === "number" ? e.rowCount : undefined,
        }))
        .filter((e) => e.id && e.sql)
      setQueryHistory(hydrated)
    } catch {
      setQueryHistory([])
    }
  }, [selectedConnection])

  // Persist query history to localStorage
  useEffect(() => {
    if (!selectedConnection) return
    const key = `${HISTORY_KEY_PREFIX}:${selectedConnection}`
    try {
      const serializable = queryHistory.map((e) => ({
        ...e,
        timestamp: e.timestamp instanceof Date ? e.timestamp.toISOString() : new Date().toISOString(),
      }))
      localStorage.setItem(key, JSON.stringify(serializable))
    } catch {
      // ignore
    }
  }, [selectedConnection, queryHistory])

  const loadSchema = useCallback(
    async (opts?: { force?: boolean }) => {
    if (!selectedConnection) return
    // Cache + request key are scoped: the server-scope (all databases) index is
    // a different result set than the default database-scope one, so they must
    // not share an in-memory cache entry.
    const scopeSuffix = allDatabases ? "::server" : "::db"
    const cacheKey = `${selectedConnection}${scopeSuffix}`
    try {
      const cached = schemaCacheRef.current.get(cacheKey)
      const isFresh = cached && Date.now() - cached.cachedAt < SCHEMA_CACHE_TTL_MS
      if (!opts?.force && cached && isFresh) {
        setTables(cached.tables || [])
        setForeignKeys(cached.foreignKeys || [])
        setSchemaFromCache(true)
        setSchemaCachedAt(cached.cachedAt)
        return
      }

      setLoadingSchema(true)
      setSchemaError(null)
      setSchemaFromCache(false)
      setSchemaCachedAt(null)
      setTables([])
      // Use Explorer schema-index (cached + includes types + FK relationships).
      // NOTE: fetch directly to /api/v1/ — the /api/explorer/* Next.js proxy routes are
      // intercepted by Traefik before reaching Next.js, so we call the api-gateway directly.
      const params = new URLSearchParams()
      if (opts?.force) params.set("refresh", "true")
      if (allDatabases) params.set("scope", "server")
      const qs = params.toString()
      const res = await fetch(
        `/api/v1/explorer/connections/${selectedConnection}/schema-index${qs ? `?${qs}` : ""}`
      )
      if (!res.ok) {
        const raw = await res.text().catch(() => "")
        let parsed: any = {}
        try { parsed = raw ? JSON.parse(raw) : {} } catch { parsed = { error: raw } }
        throw classifyApiError(res.status, parsed, "schema")
      }
      const data = await res.json()
      const nextTables = Array.isArray(data?.tables) ? data.tables : []
      const nextFks = Array.isArray(data?.foreign_keys) ? data.foreign_keys : []
      setTables(nextTables)
      setForeignKeys(nextFks)
      schemaCacheRef.current.set(cacheKey, {
        tables: nextTables,
        foreignKeys: nextFks,
        cachedAt: Date.now(),
      })
    } catch (err) {
      const e: ExplorerError =
        err && typeof err === "object" && "code" in err && "title" in err
          ? (err as ExplorerError)
          : classifyApiError(undefined, { error: err instanceof Error ? err.message : "Unknown error" }, "schema")
      setSchemaError(e)
      toast.error(e.title, { description: e.hint ?? e.message })
    } finally {
      setLoadingSchema(false)
    }
    },
    [selectedConnection, allDatabases]
  )

  // Load connections on mount
  useEffect(() => {
    loadConnections()
  }, [loadConnections])

  // Reload connections when the active workspace changes (header switcher). This is a
  // client page, so router.refresh() won't re-run the fetch. We also drop the selected
  // connection — it belongs to the previous workspace and would 404 on query — so the
  // effects keyed on selectedConnection clear the schema and the reload re-defaults to
  // the new workspace's first queryable connection.
  useEffect(() => {
    return onActiveWorkspaceChange(() => {
      setSelectedConnection("")
      void loadConnections()
    })
  }, [loadConnections])

  // Load schema when connection changes
  useEffect(() => {
    if (selectedConnection) {
      loadSchema()
    } else {
      setTables([])
      setForeignKeys([])
      setSelectedTables([])
      setSchemaFromCache(false)
    }
  }, [selectedConnection, loadSchema])

  // Reset server-scope browsing when the connection changes — scope is a
  // MySQL-only, per-connection choice and shouldn't leak across connections.
  useEffect(() => {
    setAllDatabases(false)
  }, [selectedConnection])

  // When the user focuses NL/SQL inputs, pause step updates and flush them on blur.
  const editorInteractingRef = useRef(false)
  const pendingRunStepUpdatesRef = useRef<Array<{ stepId: string; updates: Partial<ExplorerStep> }>>([])

  const flushPendingRunStepUpdates = useCallback(() => {
    const queued = pendingRunStepUpdatesRef.current
    if (queued.length === 0) return
    pendingRunStepUpdatesRef.current = []

    setCurrentRun((prev) => {
      if (!prev) return prev
      let next = prev
      for (const u of queued) {
        next = updateExplorerStep(next, u.stepId, u.updates)
      }
      return next
    })
  }, [])

  // Helper to update a step in the current run
  const updateRunStep = useCallback(
    (stepId: string, updates: Partial<ExplorerStep>) => {
      // Avoid re-rendering the entire explorer while the user is actively editing NL/SQL.
      // This prevents focus/caret jitter and makes automated UI interactions less flaky.
      if (editorInteractingRef.current) {
        pendingRunStepUpdatesRef.current.push({ stepId, updates })
        // Keep only the most recent ~50 updates to avoid unbounded growth in worst-case loops.
        if (pendingRunStepUpdatesRef.current.length > 50) {
          pendingRunStepUpdatesRef.current = pendingRunStepUpdatesRef.current.slice(-50)
        }
        return
      }
      setCurrentRun((prev) => {
        if (!prev) return prev
        return updateExplorerStep(prev, stepId, updates)
      })
    },
    []
  )

  // Start a new exploration run
  const startExplorationRun = useCallback((question: string): ExplorerRun => {
    const runId = `run-${Date.now()}`
    const run: ExplorerRun = {
      id: runId,
      question,
      steps: createInitialExplorerSteps(),
      startedAt: new Date(),
      status: "running",
    }
    setCurrentRun(run)
    setRightPanelTab("steps")
    return run
  }, [])

  const tableIndex = useMemo(() => {
    if (!selectedConnection) return buildTableIndex([])
    const cached = tableIndexCacheRef.current.get(selectedConnection)
    if (cached && cached.tablesRef === tables) return cached.index
    const idx = buildTableIndex(tables)
    tableIndexCacheRef.current.set(selectedConnection, { tablesRef: tables, index: idx })
    return idx
  }, [selectedConnection, tables])

  // Map the selected connection's connector_type onto a CodeMirror SQL
  // dialect so the editor highlights and autocompletes against the
  // right keyword set + identifier rules.
  const sqlDialect: "postgresql" | "mysql" | "redshift" | "databricks" | "generic" = useMemo(() => {
    const ct = (connections.find((c) => c.id === selectedConnection)?.connector_type || "").toLowerCase()
    if (ct.includes("mysql") || ct.includes("mariadb")) return "mysql"
    if (ct.includes("redshift")) return "redshift"
    if (ct.includes("databricks")) return "databricks"
    if (ct.includes("postgres")) return "postgresql"
    return "generic"
  }, [connections, selectedConnection])

  const generateSQL = async (opts?: { tablesOverride?: string[]; metricChoiceOverride?: MetricChoice }) => {
    if (!nlInput.trim()) {
      toast.error("Please enter a question")
      return
    }

    try {
      // Metric choice is per-question. If the prompt changed, clear prior choice.
      if (lastMetricQuestionRef.current !== nlInput) {
        lastMetricQuestionRef.current = nlInput
        metricChoiceRef.current = null
        setMetricChoice(null)
        setMetricHitlOptions([])
      }

      setGeneratingSql(true)
      setQueryError(null)

      const requestedLimitFromNL = extractRequestedRowLimit(nlInput)
      const desiredLimit = clampInt(
        requestedLimitFromNL ?? DEFAULT_ROW_LIMIT,
        1,
        MAX_ROW_LIMIT
      )

      // Start exploration run with step tracking
      startExplorationRun(nlInput)

      // Step 1: Schema Fetch (already loaded, mark as success)
      const schemaFetchStart = Date.now()
      updateRunStep("schema_fetch", {
        status: "success",
        startedAt: new Date(schemaFetchStart),
        durationMs: Date.now() - schemaFetchStart,
        completedAt: new Date(),
        inputs: [{ key: "connection_id", value: selectedConnection }],
        outputs: [
          { key: "tables_loaded", value: tables.length },
          { key: "selected_tables_pre", value: selectedTables },
          { key: "requested_row_limit", value: requestedLimitFromNL ?? DEFAULT_ROW_LIMIT },
        ],
      })

      // Step 2: Table Link (LLM-assisted when user didn't preselect tables)
      const tableLinkStart = Date.now()
      updateRunStep("table_link", {
        status: "running",
        startedAt: new Date(),
        inputs: [
          { key: "question", value: nlInput },
          { key: "available_tables", value: tables.map((t) => tableKeyFromMeta(t)) },
        ],
      })

      const overrideTables = Array.isArray(opts?.tablesOverride) ? opts?.tablesOverride : undefined
      const hasOverride = Boolean(overrideTables && overrideTables.length > 0)
      const normalizedSelectedTables = selectedTables.map((t) => qualifyTableKeyIfPossible(t, tables)).filter(Boolean)
      // Upgrade any legacy unqualified selection to schema-qualified keys when we can.
      if (!hasOverride && selectedTables.length > 0 && !sameTableSet(selectedTables, normalizedSelectedTables)) {
        setSelectedTables(normalizedSelectedTables)
      }

      // Keep selected tables schema-aware so generated SQL and execution use the correct database.
      let linkedTables: string[] = (hasOverride ? (overrideTables as string[]) : normalizedSelectedTables)
        .map((t) => qualifyTableKeyIfPossible(t, tables))
        .filter(Boolean)
      let tableLinkConfidence = linkedTables.length > 0 ? 1.0 : 0.0
      let tableLinkReason = hasOverride
        ? "User-confirmed tables"
        : selectedTables.length > 0
          ? "User-selected tables"
          : "LLM table link"

      // Always attempt table linking from the question unless the user already confirmed tables via HITL.
      // This prevents the UI from getting "stuck" on a previously-selected table when the question changes.
      if (!hasOverride) {
        const resp = await authFetch("/api/v1/explorer/nl/resolve-tables", {
          method: "POST",
          headers: { "Content-Type": "application/json", Accept: "application/json" },
          body: JSON.stringify({
            connection_id: selectedConnection,
            question: nlInput,
            tables: tables.slice(0, 30).map((t) => ({
              name: t.name,
              schema: t.schema || "public",
              row_count: t.row_count,
              columns: (t.columns || []).map((c) => ({
                name: c.name,
                type: c.type,
                is_primary_key: c.is_primary_key ?? false,
              })),
            })),
          }),
        })

        const data = await resp.json()
        if (!resp.ok) {
          throw new ExplorerApiError(classifyApiError(resp.status, data, "table_resolve"))
        }

        const candidates = Array.isArray(data?.candidates) ? data.candidates : []

        // Local schema-aware ranking (generic): use table + column names to detect when
        // the LLM's linked tables conflict with strong keyword matches (e.g. question says "invoices"
        // but a suggested table looks like "payments").
        const localRanked = rankTablesForQuestion(nlInput, tableIndex)
        const localTop = localRanked.slice(0, 5)

        const needsHitlFromServer = Boolean(data?.needs_hitl) || candidates.length === 0
        const suggestedTables = candidates
          .slice(0, 3)
          .map((c: any) => {
            const raw = String(c.table || "")
            const parsed = parseQualifiedTable(raw)
            const tableName = parsed.table || normalizeTableName(raw)
            const schemaName = String(c.schema_name || c.schema || "").trim() || parsed.schema || ""
            const key = schemaName ? `${schemaName}.${tableName}` : tableName
            return qualifyTableKeyIfPossible(key, tables)
          })
          .filter((t: unknown): t is string => typeof t === "string" && t.trim().length > 0)
        const suggestedConfidence = typeof data?.confidence_overall === "number" ? data.confidence_overall : 0.7

        // Enrich candidates with column previews from already-loaded metadata
        const enriched: HITLTableCandidate[] = candidates.map((c: any) => {
          const rawTable = String(c.table || "")
          const parsed = parseQualifiedTable(rawTable)
          const tableName = parsed.table
          // Prefer explicit schema fields, otherwise infer from the qualified table if present.
          let schemaName = String(c.schema_name || c.schema || "").trim()
          if (!schemaName || schemaName.toLowerCase() === "public") {
            if (parsed.schema) schemaName = parsed.schema
          }

          const tableMeta = tables.find((t) => {
            if (t.name !== tableName) return false
            if (!t.schema || !schemaName) return true
            return String(t.schema).trim() === schemaName
          })
          schemaName = schemaName || String(tableMeta?.schema || "").trim() || parsed.schema || ""
          schemaName = schemaName || "public"
          return {
            table: tableName,
            schema_name: schemaName,
            confidence: typeof c.confidence === "number" ? c.confidence : 0.5,
            reason: String(c.reason || ""),
            row_count: typeof tableMeta?.row_count === "number" ? tableMeta?.row_count : undefined,
            columns: (tableMeta?.columns || []).map((col) => ({
              name: col.name,
              type: col.type,
              is_primary_key: col.is_primary_key,
            })),
          }
        })

        const mergedCandidates = mergeTableCandidates({
          serverCandidates: enriched,
          localTop,
          tables: tableIndex.tables,
        })

        const suggestedSet = new Set<string>(suggestedTables.map((t: string) => normalizeTableKey(t)))
        const userSelectedSet = new Set<string>(normalizedSelectedTables.map((t: string) => normalizeTableKey(t)))

        // Decide whether to ask the user to confirm tables:
        // - Server says HITL, OR
        // - Local ranking is uncertain (close top-2) OR
        // - User-selected tables conflict with strong local top table(s), OR
        // - Suggested tables don't include the strongest local match but the match is strong.
        const tableAsk = decideTableHitl({
          question: nlInput,
          userSelected: Array.from(userSelectedSet),
          suggested: Array.from(suggestedSet),
          localTop,
          serverConfidence: suggestedConfidence,
          serverNeedsHitl: needsHitlFromServer,
        })

        if (tableAsk.shouldAsk) {
          setTableHITLCandidates(mergedCandidates)
          setTableHITLOpen(true)
          updateRunStep("table_link", {
            status: "waiting_hitl",
            durationMs: Date.now() - tableLinkStart,
            completedAt: new Date(),
            confidence: suggestedConfidence,
            reason: tableAsk.reason || String(data?.hitl_reason || "Need confirmation on tables"),
            outputs: [
              { key: "current_selected_tables", value: selectedTables },
              { key: "candidates", value: mergedCandidates.map((e) => `${e.schema_name}.${e.table}`) },
            ],
          })
          setCurrentRun((prev) => (prev ? { ...prev, status: "waiting" } : prev))
          return
        }

        // If user selected tables but the linker suggests different ones, either auto-switch (high confidence)
        // or ask for confirmation (lower confidence).
        if (normalizedSelectedTables.length > 0 && !sameTableSet(normalizedSelectedTables, suggestedTables) && suggestedTables.length > 0) {
          if (suggestedConfidence >= 0.85) {
            linkedTables = suggestedTables
            tableLinkConfidence = suggestedConfidence
            tableLinkReason = "Auto-selected tables (high confidence)"
            setSelectedTables(suggestedTables)
            toast.message("Updated tables for your question", {
              description: `Using: ${suggestedTables.join(", ")}`,
            })
          } else {
            setTableHITLCandidates(mergedCandidates)
            setTableHITLOpen(true)
            updateRunStep("table_link", {
              status: "waiting_hitl",
              durationMs: Date.now() - tableLinkStart,
              completedAt: new Date(),
              confidence: suggestedConfidence,
              reason: "Your selected tables may not match the question — confirm tables to use",
              outputs: [
                { key: "current_selected_tables", value: selectedTables },
                { key: "suggested_tables", value: suggestedTables },
              ],
            })
            setCurrentRun((prev) => (prev ? { ...prev, status: "waiting" } : prev))
            return
          }
        } else if (selectedTables.length === 0) {
          // No user selection: accept suggested tables.
          linkedTables = suggestedTables
          tableLinkConfidence = suggestedConfidence
          tableLinkReason = "LLM-selected tables"
          setSelectedTables(linkedTables)
        }
      }

      updateRunStep("table_link", {
        status: "success",
        durationMs: Date.now() - tableLinkStart,
        completedAt: new Date(),
        confidence: tableLinkConfidence,
        reason: tableLinkReason,
        outputs: [{ key: "selected_tables", value: linkedTables }],
      })

      // Build schema context from linked tables (fallback to first few tables if needed)
      const linkedKeys = new Set(linkedTables.map((t) => normalizeTableKey(t)).filter(Boolean))
      const tablesToUse =
        linkedKeys.size > 0
          ? tables.filter((t) => linkedKeys.has(tableKeyFromMeta(t))).slice(0, 20)
          : tables.slice(0, 10)

      if (tablesToUse.length === 0) {
        throw new ExplorerApiError({
          code: "no_tables",
          title: "No tables available",
          message: "Could not find any tables to generate SQL from.",
          hint: "Refresh the schema using the button in the sidebar, or select specific tables manually.",
        })
      }

      const schemaContext = tablesToUse
        .map((t) => {
          const cols = (t.columns || []).map((c) => c.name).join(", ")
          return `${tableKeyFromMeta(t)}(${cols})`
        })
        .join("; ")

      // Typed schema payload (preferred by backend when present).
      // Keep this minimal to avoid leaking large fields (e.g. search_tokens).
      const tablesTyped = tablesToUse.map((t) => ({
        name: t.name,
        schema: (t.schema || "public") as string,
        row_count: typeof (t as unknown as Record<string, unknown>)["row_count"] === "number" ? (t as unknown as Record<string, unknown>)["row_count"] as number : undefined,
        primary_key: Array.isArray((t as unknown as Record<string, unknown>)["primary_key"]) ? (t as unknown as Record<string, unknown>)["primary_key"] as string[] : undefined,
        columns: (t.columns || []).map((c) => ({
          name: c.name,
          type: c.type,
          is_primary_key: c.is_primary_key,
          nullable: c.nullable,
        })),
      }))

      const fkKey = (schema: unknown, table: unknown) => {
        const s = String(schema || "").trim()
        const t = normalizeTableName(table)
        return s ? `${s}.${t}` : t
      }
      const tableKeySet = new Set(tablesToUse.map((t) => tableKeyFromMeta(t)))
      const foreignKeysForSelection = (foreignKeys || []).filter((fk) => {
        return tableKeySet.has(fkKey(fk.from_schema, fk.from_table)) && tableKeySet.has(fkKey(fk.to_schema, fk.to_table))
      })

      // Step 3: Column Link (simplified for now)
      const colLinkStart = Date.now()
      updateRunStep("column_link", {
        status: "running",
        startedAt: new Date(),
        inputs: [{ key: "tables", value: linkedTables }],
      })

      const linkedColumns = tablesToUse.flatMap((t) => (t.columns || []).map((c) => `${tableKeyFromMeta(t)}.${c.name}`))
      
      updateRunStep("column_link", {
        status: "success",
        durationMs: Date.now() - colLinkStart,
        completedAt: new Date(),
        confidence: 0.85,
        outputs: [{ key: "columns", value: linkedColumns.slice(0, 10) }],
      })

      // Step 4: SQL Generation
      const sqlGenStart = Date.now()
      updateRunStep("sql_generate", {
        status: "running",
        startedAt: new Date(),
        inputs: [
          { key: "question", value: nlInput },
          { key: "schema", value: schemaContext.slice(0, 100) + "..." },
        ],
      })

      // Generic metric disambiguation using question + schema:
      // e.g. "total ... per month" could mean COUNT(rows) or SUM(amount-like column).
      const metricToUse = opts?.metricChoiceOverride ?? metricChoiceRef.current ?? metricChoice
      const metricDecision = decideMetricHitl({
        question: nlInput,
        selectedTables: linkedTables,
        tableIndex,
        currentChoice: metricToUse,
      })
      if (metricDecision.shouldAsk && !metricHITLOpen) {
        pendingGenerateRef.current = { tablesOverride: linkedTables }
        setMetricHitlOptions(metricDecision.options)
        setMetricHITLOpen(true)
        updateRunStep("sql_generate", {
          status: "waiting_hitl",
          durationMs: Date.now() - sqlGenStart,
          completedAt: new Date(),
          confidence: metricDecision.confidence,
          reason: metricDecision.reason,
          outputs: [
            { key: "selected_tables", value: linkedTables },
            { key: "metric_candidates", value: metricDecision.options.map((o) => o.id) },
          ],
        })
        setCurrentRun((prev) => (prev ? { ...prev, status: "waiting" } : prev))
        return
      }

      const conn = connections.find((c) => c.id === selectedConnection)
      const dialect =
        (conn?.connector_type || "").toLowerCase().includes("mysql") ||
        (conn?.connector_type || "").toLowerCase().includes("mariadb")
          ? "mysql"
          : "postgresql"

      const questionForGen = applyMetricHintToQuestion(nlInput, metricToUse, {
        selectedTables: linkedTables,
        tableIndex,
      })

      const res = await authFetch("/api/v1/sql/generate", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json",
        },
        body: JSON.stringify({
          question: questionForGen,
          dialect,
          schema: schemaContext,
          tables: linkedTables.join(","),
          tables_typed: tablesTyped,
          foreign_keys: foreignKeysForSelection,
        }),
      })

      const raw = await res.text()
      const data = safeJsonParse(raw) ?? (raw ? { error: raw } : {})

      if (res.ok && data.sql) {
        const sqlWithDesiredLimit =
          requestedLimitFromNL !== null ? setOrReplaceLimit(data.sql, desiredLimit) : data.sql

        updateRunStep("sql_generate", {
          status: "success",
          durationMs: Date.now() - sqlGenStart,
          completedAt: new Date(),
          confidence: data.confidence || 0.8,
          outputs: [
            {
              key: "sql",
              value:
                sqlWithDesiredLimit.slice(0, 100) +
                (sqlWithDesiredLimit.length > 100 ? "..." : ""),
            },
            { key: "explanation", value: data.explanation || "SQL query generated" },
            { key: "requested_row_limit", value: requestedLimitFromNL ?? DEFAULT_ROW_LIMIT },
          ],
        })

        setSqlQuery(sqlWithDesiredLimit)
        setExecutionLimit(desiredLimit)

        // Step 5: Safety Check
        const safetyStart = Date.now()
        updateRunStep("safety_check", {
          status: "running",
          startedAt: new Date(),
          inputs: [{ key: "sql", value: sqlWithDesiredLimit.slice(0, 50) + "..." }],
        })

        // Simulate safety check (actual check happens server-side during execution)
        const isSafe = isSelectOnlySql(sqlWithDesiredLimit)

        updateRunStep("safety_check", {
          status: isSafe ? "success" : "failed",
          durationMs: Date.now() - safetyStart,
          completedAt: new Date(),
          confidence: 1.0,
          outputs: [
            { key: "is_select_only", value: isSafe },
            { key: "has_limit", value: sqlWithDesiredLimit.toUpperCase().includes("LIMIT") },
            { key: "execution_row_cap", value: desiredLimit },
          ],
          ...(isSafe ? {} : { error: "Query must be SELECT-only" }),
        })

        if (isSafe) {
          // Mark remaining steps as pending
          updateRunStep("execute", { status: "pending" })
          updateRunStep("next_steps", { status: "pending" })

          // Update run status
          setCurrentRun(prev => prev ? { ...prev, status: "completed", completedAt: new Date() } : prev)
          toast.success("SQL generated successfully")
        } else {
          updateRunStep("execute", { status: "skipped" })
          updateRunStep("next_steps", { status: "skipped" })
          setCurrentRun(prev => prev ? { ...prev, status: "failed", completedAt: new Date() } : prev)
          toast.error("Generated SQL was rejected: only SELECT queries are allowed")
        }
      } else if (data.error || !res.ok) {
        const e = classifyApiError(res.status, data, "generate")
        updateRunStep("sql_generate", {
          status: "failed",
          durationMs: Date.now() - sqlGenStart,
          completedAt: new Date(),
          error: e.message,
        })
        updateRunStep("safety_check", { status: "skipped" })
        updateRunStep("execute", { status: "skipped" })
        updateRunStep("next_steps", { status: "skipped" })
        setCurrentRun(prev => prev ? { ...prev, status: "failed", completedAt: new Date() } : prev)
        setQueryError(e)
        toast.error(e.title, { description: e.hint ?? e.message })
      } else {
        // 200 but no SQL field — unexpected shape.
        const e: ExplorerError = {
          code: "sql_gen_failed",
          title: "No SQL returned",
          message: "The generator returned a successful response but did not include a SQL query.",
          hint: "Try rephrasing your question.",
        }
        updateRunStep("sql_generate", {
          status: "failed",
          durationMs: Date.now() - sqlGenStart,
          completedAt: new Date(),
          error: e.message,
        })
        updateRunStep("safety_check", { status: "skipped" })
        updateRunStep("execute", { status: "skipped" })
        updateRunStep("next_steps", { status: "skipped" })
        setCurrentRun(prev => prev ? { ...prev, status: "failed", completedAt: new Date() } : prev)
        setQueryError(e)
        toast.error(e.title, { description: e.hint ?? e.message })
      }
    } catch (err) {
      console.error("Failed to generate SQL:", err)
      const e: ExplorerError =
        err instanceof ExplorerApiError
          ? err.explorerError
          : classifyApiError(undefined, { error: err instanceof Error ? err.message : String(err) }, "generate")
      updateRunStep("sql_generate", {
        status: "failed",
        error: e.message,
        completedAt: new Date(),
      })
      setCurrentRun(prev => prev ? { ...prev, status: "failed", completedAt: new Date() } : prev)
      setQueryError(e)
      toast.error(e.title, { description: e.hint ?? e.message })
    } finally {
      setGeneratingSql(false)
    }
  }

  // Cancel an in-flight Run Query. Triggered by the explicit "Cancel"
  // button on the Run Query row and by Cmd/Ctrl+. inside the editor.
  // The AbortController.signal we pass to fetch propagates to the
  // server-side request context, which cancels the running DB query.
  const cancelQuery = useCallback(() => {
    const ctl = executeAbortRef.current
    if (!ctl) return
    ctl.abort()
    executeAbortRef.current = null
  }, [])

  // DX-ClickToInsert: insert a snippet at the SQL editor's cursor.
  // Used by the sidebar's per-table and per-column quick-insert
  // affordances. If the editor is empty we drop the snippet at
  // position 0 (focus + insertAtCursor handles both cases).
  const insertIntoEditor = useCallback((snippet: string) => {
    const handle = sqlEditorRef.current
    if (!handle) return
    // Add a leading space if the editor already has content and the
    // last character isn't whitespace — keeps inserts from joining
    // accidentally with the previous token.
    let toInsert = snippet
    if (sqlQuery.length > 0 && !/\s$/.test(sqlQuery)) {
      toInsert = " " + snippet
    }
    handle.insertAtCursor(toInsert)
    if (queryError) setQueryError(null)
  }, [sqlQuery, queryError])

  // attemptRunQuery routes the Run action: destructive statements (DROP/TRUNCATE) open a
  // typed-confirmation dialog first; everything else runs immediately. executeQuery still
  // re-checks the role gate as insurance.
  const attemptRunQuery = () => {
    const target = runTarget
    // A selection spanning a `;` is the one case we refuse outright. The server rejects
    // stacked statements by design (validators.ValidateExplorerStatement, before the
    // class/role gate), so say why here instead of surfacing its generic message.
    if (target.multi) {
      const msg =
        "The selection contains more than one statement. Select a single statement, or put the cursor inside one and run without a selection."
      setQueryError({ code: "validation", title: "Select one statement", message: msg })
      toast.error(msg)
      return
    }
    if (classifyExplorerStatement(target.sql) === "destructive") {
      setDestructiveConfirmText("")
      setPendingRunTarget(target)
      setShowDestructiveConfirm(true)
      return
    }
    executeQuery(target)
  }

  // `target` is the statement to run. It is always passed explicitly by the callers above
  // so the string that passed the gates is the string that gets executed; the default is
  // only for callers that legitimately mean "whatever the editor is pointing at now".
  const executeQuery = async (target: RunTarget = runTarget) => {
    const targetSql = target.sql
    if (!targetSql.trim()) {
      toast.error("Please enter a SQL query")
      return
    }

    if (!selectedConnection) {
      toast.error("Please select a connection")
      return
    }

    // Guardrail: gate the statement class on the caller's workspace role. Reads run for
    // any member; writes need admin, DROP/TRUNCATE need owner. Advisory — the api-gateway
    // re-checks via validators.ValidateExplorerStatement — but keeps the UX honest.
    const stmtClass = classifyExplorerStatement(targetSql)
    if (stmtClass === "blocked") {
      const msg = `${firstSqlVerb(targetSql)} statements can't be run from Explorer.`
      setQueryError({ code: "validation", title: "Statement not allowed", message: msg })
      toast.error(msg)
      return
    }
    if (!canRunExplorerStatement(workspaceRole, targetSql)) {
      const min = minRoleForStmtClass(stmtClass)
      const msg = `This statement requires the ${min ?? "owner"} role. Ask a workspace ${min ?? "owner"} to run it.`
      setQueryError({ code: "validation", title: "Insufficient role", message: msg })
      toast.error(msg)
      return
    }

    // If a previous query was somehow still tracked (shouldn't happen
    // under normal use, but is cheap insurance), abort it before we
    // start a new one so we don't leak.
    if (executeAbortRef.current) {
      executeAbortRef.current.abort()
    }
    const abortController = new AbortController()
    executeAbortRef.current = abortController

    try {
      setExecutingQuery(true)
      setQueryError(null)
      setQueryResult(null)
      setRightPanelTab("steps")

      // Ensure manual SQL runs still show a step timeline.
      // (Without NL "Generate SQL", `currentRun` can be null and the UI misleadingly says
      // "No exploration run yet" even though the query executed.)
      if (!currentRun) {
        startExplorationRun(nlInput.trim() || "Manual SQL query")
        const t = Date.now()
        updateRunStep("schema_fetch", {
          status: "success",
          startedAt: new Date(t),
          completedAt: new Date(t),
          durationMs: 0,
        })
        updateRunStep("sql_generate", { status: "skipped" })
        updateRunStep("safety_check", {
          status: "success",
          startedAt: new Date(t),
          completedAt: new Date(t),
          durationMs: 0,
        })
      }

      const sqlLimit = extractLimitFromSQL(targetSql)
      const nlLimit = extractRequestedRowLimit(nlInput)
      const effectiveLimit = clampInt(
        sqlLimit ?? nlLimit ?? executionLimit ?? DEFAULT_ROW_LIMIT,
        1,
        MAX_ROW_LIMIT
      )

      const sqlToRun = qualifySqlUsingSelectedTables(targetSql, selectedTables)
      if (sqlToRun !== targetSql) {
        // Keep the editor aligned with what we actually execute (prevents confusing "wrong DB"
        // errors). Splice the rewritten statement back over its own range — overwriting the
        // whole buffer here would destroy the user's other queries.
        setSqlQuery((prev) => spliceRunTarget(prev, target, sqlToRun))
      }

      // Update execute step in current run
      const executeStart = Date.now()
      updateRunStep("execute", {
        status: "running",
        startedAt: new Date(),
        inputs: [
          { key: "connection_id", value: selectedConnection },
          { key: "sql", value: sqlToRun.slice(0, 100) + (sqlToRun.length > 100 ? "..." : "") },
          { key: "limit", value: effectiveLimit },
        ],
      })

      const res = await authFetch("/api/v1/explorer/query", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json",
        },
        body: JSON.stringify({
          connection_id: selectedConnection,
          sql: sqlToRun,
          limit: effectiveLimit,
        }),
        signal: abortController.signal,
      })

      const raw = await res.text()
      let data: Record<string, unknown> = {}
      try {
        data = raw ? JSON.parse(raw) : {}
      } catch {
        throw new ExplorerApiError({
          code: "network",
          title: "Unexpected server response",
          message: raw || "The server returned a non-JSON response.",
          hint: "This may be a temporary issue. Try again.",
        })
      }

      if (data.error || !res.ok) {
        const e = classifyApiError(res.status, data, "execute")
        updateRunStep("execute", {
          status: "failed",
          durationMs: Date.now() - executeStart,
          completedAt: new Date(),
          error: e.message,
          recovery: e.hint ? { action: "Suggestion", description: e.hint } : undefined,
        })
        updateRunStep("next_steps", { status: "skipped" })
        setCurrentRun(prev => prev ? { ...prev, status: "failed", completedAt: new Date() } : prev)
        setQueryError(e)
        toast.error(e.title, { description: e.hint ?? e.message })
      } else {
        const rowCount = typeof data.row_count === "number" ? data.row_count : 0
        const executionTimeMs = typeof data.execution_time_ms === "number" ? data.execution_time_ms : 0
        const truncated = Boolean(data.truncated)
        const columns = Array.isArray(data.columns) ? data.columns : []
        // Write statements return an affected-row count + statement type instead of rows.
        const rowsAffected = typeof data.rows_affected === "number" ? data.rows_affected : undefined
        const statementType = typeof data.statement_type === "string" ? data.statement_type : undefined

        updateRunStep("execute", {
          status: "success",
          durationMs: Date.now() - executeStart,
          completedAt: new Date(),
          outputs: [
            { key: "row_count", value: rowCount },
            { key: "columns", value: columns.slice(0, 5) },
            { key: "execution_time_ms", value: executionTimeMs },
            { key: "truncated", value: truncated },
            { key: "limit", value: effectiveLimit },
          ],
        })

        // Update next_steps
        const nextStepsStart = Date.now()
      updateRunStep("next_steps", {
        status: "success",
        startedAt: new Date(nextStepsStart),
        durationMs: Date.now() - nextStepsStart,
        completedAt: new Date(),
        outputs: [{ key: "suggestions", value: ["Create Metabase Dashboard", "Download CSV", "Share via Slack"] }],
      })
      setCurrentRun((prev) => (prev ? { ...prev, status: "completed", completedAt: new Date() } : prev))

        const nextResult: QueryResult = {
          columns,
          rows: Array.isArray((data as Record<string, unknown>)?.["rows"]) ? ((data as Record<string, unknown>)["rows"] as Record<string, unknown>[]) : [],
          row_count: rowCount,
          execution_time_ms: executionTimeMs,
          truncated,
          rows_affected: rowsAffected,
          statement_type: statementType,
        }
        setQueryResult(nextResult)
        // Add to history
        const historyEntry: QueryHistory = {
          id:
            (globalThis.crypto as unknown as { randomUUID?: () => string })?.randomUUID?.() ||
            `${Date.now()}-${Math.random().toString(16).slice(2)}`,
          question: nlInput || undefined,
          // The statement that ran — not the whole editor buffer, which may hold several.
          sql: sqlToRun,
          timestamp: new Date(),
          executionTimeMs,
          rowCount,
        }
        setQueryHistory(prev => [historyEntry, ...prev.slice(0, 19)])
        // Make history visible
        setShowHistory(true)
        if (isWriteResult(statementType)) {
          toast.success(
            rowsAffected !== undefined
              ? `${statementType} succeeded — ${pluralizeRows(rowsAffected)} affected`
              : `${statementType} executed successfully`
          )
          // The write just invalidated the schema we're showing. Row counts, and
          // the table list itself after a CREATE/DROP, are both wrong now — the
          // sidebar kept a row count of 3 for a table that had grown to 8 and
          // then been dropped. Drop both scope entries (the write may have hit a
          // table outside the current scope) and re-fetch past the server cache.
          if (selectedConnection) {
            schemaCacheRef.current.delete(`${selectedConnection}::db`)
            schemaCacheRef.current.delete(`${selectedConnection}::server`)
            tableIndexCacheRef.current.delete(selectedConnection)
            void loadSchema({ force: true })
          }
        } else {
          toast.success(`Query executed in ${executionTimeMs}ms`)
        }
      }
    } catch (err) {
      // Cancellation is a normal user action, not an error — show
      // a neutral status and don't push an ExplorerError into the UI.
      if (err instanceof DOMException && err.name === "AbortError") {
        updateRunStep("execute", {
          status: "failed",
          error: "Cancelled by user",
          completedAt: new Date(),
        })
        updateRunStep("next_steps", { status: "skipped" })
        setCurrentRun((prev) =>
          prev ? { ...prev, status: "failed", completedAt: new Date() } : prev,
        )
        toast.info("Query cancelled")
        return
      }
      console.error("Failed to execute query:", err)
      const e: ExplorerError =
        err instanceof ExplorerApiError
          ? err.explorerError
          : classifyApiError(undefined, { error: err instanceof Error ? err.message : String(err) }, "execute")
      updateRunStep("execute", {
        status: "failed",
        error: e.message,
        completedAt: new Date(),
      })
      updateRunStep("next_steps", { status: "skipped" })
      setCurrentRun(prev => prev ? { ...prev, status: "failed", completedAt: new Date() } : prev)
      setQueryError(e)
      toast.error(e.title, { description: e.hint ?? e.message })
    } finally {
      // Only clear our ref if this is still the active query — a new
      // Run Query press will have already replaced it.
      if (executeAbortRef.current === abortController) {
        executeAbortRef.current = null
      }
      setExecutingQuery(false)
    }
  }

  // Page-level Cmd/Ctrl+Enter fallback, for when focus is NOT in a control that owns the
  // shortcut itself (schema browser, a button, the page body). The decision lives in
  // lib/explorer/runShortcut so it can be tested, and so the deps type can withhold
  // executeQuery — see the defect write-up there.
  const handleKeyDown = (e: React.KeyboardEvent) => {
    handleExplorerRunShortcut(e, {
      hasNlInput: Boolean(nlInput.trim()),
      hasSql: Boolean(sqlQuery.trim()),
      generateSQL,
      attemptRunQuery,
    })
  }

  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text)
    toast.success("Copied to clipboard")
  }

  const downloadBlob = (blob: Blob, filename: string) => {
    const url = window.URL.createObjectURL(blob)
    const a = document.createElement("a")
    a.href = url
    a.download = filename
    document.body.appendChild(a)
    a.click()
    a.remove()
    window.URL.revokeObjectURL(url)
  }

  const parseFilenameFromDisposition = (disposition: string | null, fallback: string): string => {
    if (!disposition) return fallback
    // e.g. attachment; filename=export.csv OR attachment; filename="export.csv"
    const m = disposition.match(/filename\*?=(?:UTF-8''|")?([^\";]+)\"?/i)
    const raw = m?.[1]?.trim()
    if (!raw) return fallback
    try {
      return decodeURIComponent(raw)
    } catch {
      return raw
    }
  }

  const exportResults = async (format: ExportFormat) => {
    if (!selectedConnection || !sqlQuery.trim()) {
      toast.error("Run a query first")
      return
    }
    try {
      setExportingFormat(format)
      // Re-run export server-side with a safe cap (keeps downloads consistent even if UI is truncated).
      // /proxy/ prefix so Traefik doesn't intercept it (only routes /api/* to api-gateway)
      const res = await fetch("/proxy/explorer/export", {
        method: "POST",
        headers: { "Content-Type": "application/json", Accept: "*/*" },
        body: JSON.stringify({
          connection_id: selectedConnection,
          sql: sqlQuery,
          format,
          limit: 10000,
        }),
      })

      if (!res.ok) {
        const raw = await res.text().catch(() => "")
        let data: Record<string, unknown> = {}
        try { data = raw ? JSON.parse(raw) : {} } catch { data = { error: raw || `HTTP ${res.status}` } }
        const e = classifyApiError(res.status, data, "export")
        toast.error(e.title, { description: e.hint ?? e.message })
        return
      }

      const blob = await res.blob()
      const ext = format === "xlsx" ? "xlsx" : format
      const fallback = `explorer-export.${ext}`
      const filename = parseFilenameFromDisposition(res.headers.get("Content-Disposition"), fallback)
      downloadBlob(blob, filename)
      toast.success(`Downloaded ${format.toUpperCase()}`)
    } catch (e) {
      const err = classifyApiError(undefined, { error: e instanceof Error ? e.message : String(e) }, "export")
      toast.error(err.title, { description: err.hint ?? err.message })
    } finally {
      setExportingFormat(null)
    }
  }

  const createVisualizationDashboard = async () => {
    if (!sqlQuery.trim() || !dashboardName.trim()) {
      toast.error("Please enter a dashboard name")
      return
    }

    const toolDef = VISUALIZATION_TOOLS.find((t) => t.id === selectedBiTool)
    if (!toolDef?.implemented) {
      toast.info(`${toolDef?.name ?? selectedBiTool} integration coming soon`, {
        description: "Only Metabase is supported today. More tools are on the roadmap.",
      })
      return
    }

    try {
      setCreatingDashboard(true)

      const res = await fetch(`/proxy/explorer/${selectedBiTool}/dashboard`, {
        method: "POST",
        headers: { "Content-Type": "application/json", Accept: "application/json" },
        body: JSON.stringify({
          sql: sqlQuery,
          name: dashboardName,
          description: dashboardDescription || `Generated from Explorer: ${nlInput || "SQL query"}`,
        }),
      })

      let data: Record<string, unknown> = {}
      try {
        const raw = await res.text()
        data = raw ? JSON.parse(raw) : {}
      } catch {
        data = { error: `HTTP ${res.status}: unexpected response from server` }
      }

      if (!res.ok || !data.success) {
        const e = classifyApiError(res.status, data, "bi_tool")
        toast.error(e.title, { description: e.hint ?? e.message })
        return
      }

      toast.success(`Dashboard created in ${toolDef.name}!`)
      setShowDashboardDialog(false)
      setDashboardName("")
      setDashboardDescription("")
      setSelectedBiTool("metabase")
      if (data.dashboard_url) {
        window.open(data.dashboard_url as string, "_blank")
      }
    } catch (err) {
      const e = classifyApiError(undefined, { error: err instanceof Error ? err.message : String(err) }, "bi_tool")
      toast.error(e.title, { description: e.hint ?? e.message })
    } finally {
      setCreatingDashboard(false)
    }
  }

  const loadFromHistory = (entry: QueryHistory) => {
    setSqlQuery(entry.sql)
    if (entry.question) {
      setNlInput(entry.question)
    }
    setShowHistory(false)
    toast.info("Query loaded from history")
  }

  // Engine-aware naming: MySQL exposes sibling databases, Postgres exposes
  // schemas under one database. The tier label and the "all databases" toggle
  // key off this.
  const selectedConn = connections.find((c) => c.id === selectedConnection)
  const isMySQLConn = isMySQLFamily(selectedConn)
  const namespaceLabel = isMySQLConn ? "databases" : "schemas"
  // Database/schema count for the panel header label. Counts the *visible*
  // tables (rsync-internal `_rsync_*` excluded) so the header total matches the
  // tree below it. The full tree (grouping, search, expand/collapse) lives in
  // <SchemaBrowser>, which is also fed visibleTables.
  const databaseCount = groupTablesByDatabase(visibleTables).length

  const toggleTableSelection = (tableKey: string) => {
    setSelectedTables(prev =>
      prev.includes(tableKey)
        ? prev.filter(t => t !== tableKey)
        : [...prev, tableKey]
    )
  }

  return (
    <div className="space-y-6" onKeyDown={handleKeyDown}>
      <PageHeader
        heading="Data Explorer"
        description="Query and explore your connected data sources using natural language or SQL"
      />

      {/* Workspace: collapsible schema rail · full-width query editor + results.
          Flex (not grid) so the schema rail can collapse to a thin strip by
          animating its width. Stacks vertically on mobile. */}
      <div className="flex flex-col gap-3 lg:flex-row lg:items-start">
        {/* ── Left rail: schema browser ── */}
        <aside
          className={cn(
            "w-full shrink-0 transition-[width] duration-200",
            leftRailOpen ? "lg:w-[280px]" : "lg:w-[48px]"
          )}
        >
        {leftRailOpen ? (
        <Card>
          <CardHeader className="px-3 pt-4 pb-3">
            <div className="flex items-center justify-between gap-2">
              <CardTitle className="flex items-center gap-2 text-base">
                <Database className="h-4 w-4 text-violet-500" />
                Schema
              </CardTitle>
              <Button
                variant="ghost"
                size="sm"
                className="hidden h-7 w-7 p-0 lg:inline-flex"
                onClick={() => setLeftRailOpen(false)}
                title="Collapse schema browser"
                aria-label="Collapse schema browser"
              >
                <PanelLeftClose className="h-4 w-4" />
              </Button>
            </div>
            <CardDescription className="text-xs">
              Browse databases, schemas &amp; tables
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-3 px-3 pb-3">
            {/* Connection Selector */}
            <div className="space-y-2">
              <Label className="text-xs">Connection</Label>
              <Select
                value={selectedConnection}
                onValueChange={setSelectedConnection}
                disabled={loadingConnections}
              >
                <SelectTrigger className="h-9">
                  <SelectValue placeholder="Select connection..." />
                </SelectTrigger>
                <SelectContent>
                  {connections.map((conn) => (
                    <SelectItem key={conn.id} value={conn.id}>
                      <div className="flex items-center gap-2">
                        <Database className="h-3 w-3" />
                        <span className="truncate">{conn.name}</span>
                        <Badge variant="secondary" className="text-[10px] px-1 py-0">
                          {conn.type}
                        </Badge>
                      </div>
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            {/* Schema browser controls */}
            {selectedConnection && (
              <div className="space-y-2">
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <Label className="text-xs capitalize">
                      {databaseCount} {namespaceLabel} · {visibleTables.length} tables
                    </Label>
                    {schemaFromCache && visibleTables.length > 0 && (
                      <Badge
                        variant="outline"
                        className="text-[9px] py-0 h-4 px-1.5 text-zinc-500 border-zinc-300 dark:border-zinc-700"
                        title="Schema and row counts are from a local cache (≤10 min old) and do not reflect changes made outside this tab. Click refresh to re-fetch from the database."
                      >
                        {schemaCacheAgeLabel ? `Cached · ${schemaCacheAgeLabel}` : "Cached"}
                      </Badge>
                    )}
                  </div>
                  <Button
                    variant="ghost"
                    size="sm"
                    className="h-6 px-1.5"
                    onClick={() => loadSchema({ force: true })}
                    disabled={loadingSchema}
                    title="Refresh schema from the database"
                  >
                    <RefreshCw className={cn("h-3 w-3", loadingSchema && "animate-spin")} />
                  </Button>
                </div>
                {/* MySQL server-scope toggle: browse every database on the
                    server, not just the one the connection points at. */}
                {isMySQLConn && (
                  <label
                    className="flex items-center justify-between gap-2 rounded-md border border-dashed border-zinc-200 dark:border-zinc-800 px-2 py-1.5 cursor-pointer"
                    title="Browse every database on this MySQL server from one connection"
                  >
                    <div className="flex items-center gap-1.5 text-[11px] text-zinc-600 dark:text-zinc-300">
                      <Server className="h-3 w-3 text-violet-500" />
                      Show all databases on server
                    </div>
                    <Switch
                      checked={allDatabases}
                      onCheckedChange={setAllDatabases}
                      disabled={loadingSchema}
                      aria-label="Show all databases on server"
                    />
                  </label>
                )}
              </div>
            )}

            {/* Schema tree: namespace → table → columns */}
            <div className="h-[340px] overflow-auto rounded-md border">
              {loadingSchema ? (
                <div className="flex items-center justify-center py-8">
                  <Loader2 className="h-5 w-5 animate-spin text-violet-500" />
                </div>
              ) : schemaError ? (
                <div className="flex flex-col items-center justify-center py-8 px-4 gap-2 text-center">
                  <AlertCircle className="h-5 w-5 text-red-400 shrink-0" />
                  <div className="text-xs font-semibold text-red-600 dark:text-red-400">{schemaError.title}</div>
                  <div className="text-xs text-zinc-500 break-words">{schemaError.message}</div>
                  {schemaError.hint && (
                    <div className="text-[11px] text-zinc-400 italic">{schemaError.hint}</div>
                  )}
                  <Button
                    variant="outline"
                    size="sm"
                    className="mt-1 h-7 text-xs"
                    onClick={() => loadSchema({ force: true })}
                  >
                    <RefreshCw className="h-3 w-3 mr-1" />
                    Retry
                  </Button>
                </div>
              ) : (
                <SchemaBrowser
                  tables={visibleTables}
                  selectedTables={selectedTables}
                  selectionKey={(t) => tableKeyFromMeta(t)}
                  onToggleTable={toggleTableSelection}
                  onInsertTable={insertIntoEditor}
                  onInsertColumn={insertIntoEditor}
                  emptyHint={
                    selectedConnection
                      ? "No tables found"
                      : "Select a connection to browse tables"
                  }
                  className="p-1.5"
                />
              )}
            </div>

            {selectedTables.length > 0 && (
              <div className="flex items-center gap-2 flex-wrap">
                <span className="text-xs text-zinc-500">Selected:</span>
                {selectedTables.map(t => (
                  <Badge key={t} variant="secondary" className="text-[10px] gap-1">
                    {formatTableKeyForDisplay(t)}
                    <X
                      className="h-2.5 w-2.5 cursor-pointer hover:text-red-500"
                      onClick={() => toggleTableSelection(t)}
                    />
                  </Badge>
                ))}
              </div>
            )}
          </CardContent>
        </Card>
        ) : (
          <div className="hidden flex-col items-center gap-3 rounded-lg border bg-card py-3 lg:flex">
            <Button
              variant="ghost"
              size="sm"
              className="h-7 w-7 p-0"
              onClick={() => setLeftRailOpen(true)}
              title="Show schema browser"
              aria-label="Show schema browser"
            >
              <PanelLeftOpen className="h-4 w-4 text-violet-500" />
            </Button>
            <span className="text-[10px] font-medium text-zinc-500 [writing-mode:vertical-rl]">
              Schema
            </span>
          </div>
        )}
        </aside>

        {/* ── Center: NL → SQL → results ── */}
        <main className="w-full min-w-0 space-y-6 lg:flex-1">
          {/* Query Editor */}
          <Card>
            <CardHeader className="px-4 pt-4 pb-3">
              <div className="flex items-center justify-between">
                <div>
                  <CardTitle className="flex items-center gap-2 text-base">
                    <Code className="h-4 w-4 text-violet-500" />
                    Query Editor
                  </CardTitle>
                  <CardDescription className="text-xs mt-1">
                    Ask in natural language (Cmd+Enter) or write SQL directly
                  </CardDescription>
                </div>
                <div className="flex items-center gap-2">
                  <Button
                    variant={rightPanelTab === "steps" ? "secondary" : "outline"}
                    size="sm"
                    onClick={() => focusExplorationPanel("steps")}
                  >
                    <GitBranch className="h-4 w-4 mr-2" />
                    Steps
                  </Button>
                  <Button
                    variant={rightPanelTab === "history" ? "secondary" : "outline"}
                    size="sm"
                    onClick={() => focusExplorationPanel("history")}
                  >
                    <History className="h-4 w-4 mr-2" />
                    History
                  </Button>
                </div>
              </div>
            </CardHeader>
            <CardContent className="space-y-4 px-4 pb-4">
              {/* NL Input */}
              <div className="space-y-2">
                <Label htmlFor="explorer-nl" className="text-xs flex items-center gap-2">
                  <Sparkles className="h-3 w-3 text-violet-500" />
                  Natural Language Query
                </Label>
                <div className="flex gap-2">
                  <Textarea
                    id="explorer-nl"
                    ref={nlTextareaRef}
                    placeholder={"e.g., Show me total revenue by month for 2024...\nYou can use multiple lines and bullet points."}
                    value={nlInput}
                    onChange={(e) => setNlInput(e.target.value)}
                    onFocus={() => {
                      editorInteractingRef.current = true
                    }}
                    onBlur={() => {
                      editorInteractingRef.current = false
                      flushPendingRunStepUpdates()
                    }}
                    onKeyDown={(e) => {
                      if ((e.metaKey || e.ctrlKey) && e.key === "Enter") {
                        e.preventDefault()
                        generateSQL()
                      }
                    }}
                    rows={3}
                    className="flex-1 text-sm resize-y"
                  />
                  <Button
                    onClick={() => generateSQL()}
                    disabled={generatingSql || !nlInput.trim()}
                    className="bg-gradient-to-r from-violet-600 to-indigo-600"
                  >
                    {generatingSql ? (
                      <Loader2 className="h-4 w-4 animate-spin" />
                    ) : (
                      <>
                        <Sparkles className="h-4 w-4 mr-2" />
                        Generate SQL
                      </>
                    )}
                  </Button>
                </div>
                {/* Example NL prompts — teach users how to phrase questions.
                    Tailored to the connected schema; click to fill the box. */}
                <NlExamplePrompts
                  tables={visibleTables}
                  onSelect={(prompt) => {
                    setNlInput(prompt)
                    nlTextareaRef.current?.focus()
                  }}
                />
              </div>

              {/* SQL Editor */}
              <div className="space-y-2">
                <div className="flex items-center justify-between">
                  <Label htmlFor="explorer-sql" className="text-xs">SQL Query</Label>
                  {sqlQuery && (
                    <Button
                      variant="ghost"
                      size="sm"
                      className="h-6 px-2"
                      onClick={() => copyToClipboard(sqlQuery)}
                    >
                      <Copy className="h-3 w-3 mr-1" />
                      Copy
                    </Button>
                  )}
                </div>
                <SqlEditor
                  ref={sqlEditorRef}
                  value={sqlQuery}
                  onChange={(next) => {
                    setSqlQuery(next)
                    if (queryError) setQueryError(null)
                  }}
                  tables={visibleTables}
                  foreignKeys={foreignKeys}
                  dialect={sqlDialect}
                  placeholder="SELECT * FROM table_name LIMIT 100"
                  className="rounded-md border border-zinc-200 dark:border-zinc-800 overflow-hidden"
                  onSubmit={() => {
                    // Gate on the resolved target, matching the Run button. `multi` still
                    // calls through so attemptRunQuery can explain why it won't run.
                    if (
                      !executingQuery &&
                      runTarget.sql.trim() &&
                      selectedConnection &&
                      (canRunCurrentStatement || runTarget.multi)
                    ) {
                      attemptRunQuery()
                    }
                  }}
                  onCancel={() => {
                    if (executingQuery) cancelQuery()
                  }}
                  onFocus={() => {
                    editorInteractingRef.current = true
                  }}
                  onBlur={() => {
                    editorInteractingRef.current = false
                    flushPendingRunStepUpdates()
                  }}
                  onSelectionChange={handleEditorSelectionChange}
                />
              </div>

              {/* Actions */}
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-2 text-xs text-zinc-500">
                  <kbd className="px-1.5 py-0.5 bg-zinc-100 dark:bg-zinc-800 rounded text-[10px]">⌘</kbd>
                  <span>+</span>
                  <kbd className="px-1.5 py-0.5 bg-zinc-100 dark:bg-zinc-800 rounded text-[10px]">Enter</kbd>
                  <span>to generate or run</span>
                  {runTarget.total > 1 && (
                    <>
                      <span className="mx-1 text-zinc-300">·</span>
                      <span className="text-zinc-600 dark:text-zinc-400">
                        {runTarget.source === "selection"
                          ? "runs the selection"
                          : `runs statement ${runTarget.index} of ${runTarget.total}`}
                      </span>
                    </>
                  )}
                  {executingQuery && (
                    <>
                      <span className="mx-1 text-zinc-300">·</span>
                      <kbd className="px-1.5 py-0.5 bg-zinc-100 dark:bg-zinc-800 rounded text-[10px]">⌘</kbd>
                      <span>+</span>
                      <kbd className="px-1.5 py-0.5 bg-zinc-100 dark:bg-zinc-800 rounded text-[10px]">.</kbd>
                      <span>to cancel</span>
                    </>
                  )}
                </div>
                {executingQuery ? (
                  <Button
                    onClick={cancelQuery}
                    variant="outline"
                    className="border-red-300 text-red-700 hover:bg-red-50 dark:border-red-700 dark:text-red-300 dark:hover:bg-red-950/30"
                  >
                    <X className="h-4 w-4 mr-2" />
                    Cancel
                  </Button>
                ) : (
                  <Button
                    onClick={attemptRunQuery}
                    disabled={
                      !runTarget.sql.trim() ||
                      !selectedConnection ||
                      (!canRunCurrentStatement && !runTarget.multi)
                    }
                    className="bg-gradient-to-r from-emerald-600 to-teal-600"
                    title={runTarget.total > 1 ? runTarget.sql : undefined}
                  >
                    <Play className="h-4 w-4 mr-2" />
                    {runButtonLabel}
                  </Button>
                )}
              </div>
              {runTarget.multi ? (
                <div className="text-xs text-amber-700 dark:text-amber-300">
                  Your selection covers more than one statement. Queries run one at a time —
                  select a single statement, or clear the selection and put the cursor inside
                  the one you want.
                </div>
              ) : runTarget.sql.trim() && !canRunCurrentStatement ? (
                <div className="text-xs text-amber-700 dark:text-amber-300">
                  {currentStmtClass === "blocked" ? (
                    <>
                      <span className="font-medium">{firstSqlVerb(runTarget.sql)}</span>{" "}
                      statements can&apos;t be run from Explorer.
                    </>
                  ) : (
                    <>
                      This statement needs the{" "}
                      <span className="font-medium">{minRoleForStmtClass(currentStmtClass) ?? "owner"}</span>{" "}
                      role. Your role is <span className="font-medium">{workspaceRole || "unknown"}</span>.
                    </>
                  )}
                </div>
              ) : currentStmtClass === "destructive" && runTarget.sql.trim() ? (
                <div className="text-xs text-red-700 dark:text-red-300">
                  <span className="font-medium">{destructiveLabel(runTarget.sql)}</span>{" "}
                  is destructive and irreversible — you&apos;ll be asked to confirm.
                </div>
              ) : null}
            </CardContent>
          </Card>

          {/* Error Display */}
          {queryError && (
            <Card className="border-red-200 bg-red-50 dark:bg-red-950/20">
              <CardContent className="py-4">
                <div className="flex items-start gap-3">
                  <AlertCircle className="h-5 w-5 text-red-500 mt-0.5 shrink-0" />
                  <div className="flex-1 min-w-0 space-y-1">
                    <div className="font-semibold text-sm text-red-700 dark:text-red-400">
                      {queryError.title}
                    </div>
                    <div className="text-sm text-red-600 dark:text-red-300 font-mono break-words">
                      {queryError.message}
                    </div>
                    {queryError.hint && (
                      <div className="flex items-start gap-1.5 mt-2 text-xs text-zinc-600 dark:text-zinc-400 bg-white/60 dark:bg-zinc-900/60 rounded px-2 py-1.5">
                        <span className="shrink-0 mt-px">💡</span>
                        <span>{queryError.hint}</span>
                      </div>
                    )}
                  </div>
                </div>
              </CardContent>
            </Card>
          )}

          {/* Results */}
          {queryResult && (
            <Card>
              <CardHeader className="pb-3">
                <div className="flex items-center justify-between">
                  <CardTitle className="text-base flex items-center gap-2">
                    <CheckCircle2 className="h-4 w-4 text-emerald-500" />
                    Results
                  </CardTitle>
                  <div className="flex items-center gap-4">
                    <div className="flex items-center gap-4 text-xs text-zinc-500">
                      {isWriteResult(queryResult.statement_type) ? (
                        <span>
                          {queryResult.statement_type}
                          {queryResult.rows_affected !== undefined
                            ? ` — ${pluralizeRows(queryResult.rows_affected)} affected`
                            : " executed"}
                        </span>
                      ) : (
                        <span>{pluralizeRows(queryResult.row_count)}</span>
                      )}
                      <span>{queryResult.execution_time_ms}ms</span>
                      {queryResult.truncated && (
                        <Badge
                          variant="outline"
                          className="text-[10px]"
                          title="Result set exceeded the row cap — increase LIMIT or add filters to see more."
                        >
                          Truncated
                        </Badge>
                      )}
                    </div>
                    {/* Export + BI act on a returned result set — hidden for write
                        statements, which return an affected-row count, not rows. */}
                    {!isWriteResult(queryResult.statement_type) && (
                      <>
                        <DropdownMenu>
                          <DropdownMenuTrigger asChild>
                            <Button variant="outline" size="sm" disabled={!sqlQuery.trim() || exportingFormat !== null}>
                              <Download className="h-4 w-4 mr-2" />
                              {exportingFormat ? "Preparing…" : "Download"}
                            </Button>
                          </DropdownMenuTrigger>
                          <DropdownMenuContent align="end">
                            <DropdownMenuItem onClick={() => exportResults("csv")}>CSV</DropdownMenuItem>
                            <DropdownMenuItem onClick={() => exportResults("tsv")}>TSV</DropdownMenuItem>
                            <DropdownMenuItem onClick={() => exportResults("json")}>JSON</DropdownMenuItem>
                            {/* Excel (.xlsx) export intentionally not exposed: the api-gateway has no Go xlsx renderer wired up.
                                If demand surfaces, swap in xlsx-populate or excelize-go and add a `xlsx` branch to ExportQueryHandler. */}
                          </DropdownMenuContent>
                        </DropdownMenu>
                        <Button
                          variant="outline"
                          size="sm"
                          onClick={() => {
                            setSelectedBiTool("metabase")
                            setDashboardName(nlInput || "Query Results")
                            setShowDashboardDialog(true)
                          }}
                        >
                          <BarChart3 className="h-4 w-4 mr-2" />
                          Send to BI Tool
                        </Button>
                      </>
                    )}
                  </div>
                </div>
              </CardHeader>
              <CardContent>
                {isWriteResult(queryResult.statement_type) ? (
                  // WriteOutcome: a write/DDL statement returns no result grid — show a
                  // clear "N rows affected" confirmation instead.
                  <div className="rounded-md border border-emerald-200 dark:border-emerald-900 bg-emerald-50/60 dark:bg-emerald-950/20 py-12 px-6 text-center">
                    <CheckCircle2 className="mx-auto h-8 w-8 text-emerald-500" />
                    <div className="mt-3 text-sm font-medium text-zinc-800 dark:text-zinc-100">
                      {queryResult.statement_type} statement executed
                    </div>
                    <div className="mt-1 text-xs text-zinc-500 max-w-md mx-auto">
                      {queryResult.rows_affected !== undefined
                        ? `${pluralizeRows(queryResult.rows_affected)} affected.`
                        : "The statement completed successfully."}
                    </div>
                  </div>
                ) : queryResult.rows.length === 0 ? (
                  // ZeroRowState: a successful query that returned no rows is
                  // a common, expected outcome (filter too narrow, table really
                  // is empty, etc.). Show a friendly empty state instead of
                  // an awkward blank table so the user knows the run succeeded.
                  <div className="rounded-md border border-dashed border-zinc-200 dark:border-zinc-800 py-12 px-6 text-center">
                    <Database className="mx-auto h-8 w-8 text-zinc-300 dark:text-zinc-700" />
                    <div className="mt-3 text-sm font-medium text-zinc-700 dark:text-zinc-200">
                      Query ran successfully — no rows matched
                    </div>
                    <div className="mt-1 text-xs text-zinc-500 max-w-md mx-auto">
                      {queryResult.columns.length > 0
                        ? `The result set has ${queryResult.columns.length} column${queryResult.columns.length === 1 ? "" : "s"} but zero rows. Loosen your WHERE clause, or pick a different time range, then run again.`
                        : "The query returned zero rows. Loosen your WHERE clause or pick a different time range, then run again."}
                    </div>
                  </div>
                ) : (
                  <div className="h-[400px] overflow-auto rounded-md border">
                    <Table>
                      <TableHeader>
                        <TableRow>
                          {queryResult.columns.map((col) => (
                            <TableHead key={col} className="text-xs font-medium">
                              {col}
                            </TableHead>
                          ))}
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {queryResult.rows.map((row, i) => (
                          <TableRow key={i}>
                            {queryResult.columns.map((col) => (
                              <TableCell
                                key={col}
                                className={cn(
                                  "text-xs font-mono max-w-[520px]",
                                  typeof row[col] === "string" && String(row[col]).length > 200
                                    ? "cursor-pointer hover:bg-zinc-50 dark:hover:bg-zinc-900"
                                    : ""
                                )}
                                title={
                                  typeof row[col] === "string" && String(row[col]).length > 200
                                    ? "Click to view full value"
                                    : undefined
                                }
                                onClick={() => {
                                  const full = formatCellValue(row[col], { truncate: false })
                                  if (typeof full === "string" && full.length > 200) {
                                    setCellViewerTitle(col)
                                    setCellViewerValue(full)
                                    setCellViewerOpen(true)
                                  }
                                }}
                              >
                                {formatCellValue(row[col], { truncate: true })}
                              </TableCell>
                            ))}
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </div>
                )}
                {queryResult.warnings && queryResult.warnings.length > 0 && (
                  <div className="mt-4 p-3 bg-amber-50 dark:bg-amber-950/20 rounded-lg">
                    <div className="text-xs text-amber-700 dark:text-amber-400">
                      {queryResult.warnings.join("; ")}
                    </div>
                  </div>
                )}
              </CardContent>
            </Card>
          )}

          {/* Exploration Details (below results) */}
          {/* explorationPanelRef target — Steps/History buttons scroll here */}
          <Card ref={explorationPanelRef}>
            <CardHeader className="pb-3">
              <Tabs value={rightPanelTab} onValueChange={(v) => setRightPanelTab(v as ExplorerPanelTab)}>
                <TabsList className="grid w-full grid-cols-3">
                  <TabsTrigger value="steps" className="text-xs">
                    <GitBranch className="h-3 w-3 mr-1" />
                    Exploration Steps
                  </TabsTrigger>
                  <TabsTrigger value="history" className="text-xs">
                    <History className="h-3 w-3 mr-1" />
                    Query History
                  </TabsTrigger>
                  <TabsTrigger value="saved" className="text-xs">
                    <Bookmark className="h-3 w-3 mr-1" />
                    Saved Queries
                  </TabsTrigger>
                </TabsList>
              </Tabs>
            </CardHeader>
            <CardContent>
              {rightPanelTab === "steps" ? (
                <ExplorerStepTimeline
                  run={currentRun}
                  onRetry={() => {
                    if (nlInput.trim()) {
                      generateSQL()
                    }
                  }}
                />
              ) : rightPanelTab === "saved" ? (
                <SavedQueries
                  connectionId={selectedConnection}
                  currentSql={sqlQuery}
                  currentQuestion={nlInput}
                  onLoad={(sql, question) => {
                    setSqlQuery(sql)
                    if (question) setNlInput(question)
                  }}
                />
              ) : (
                <div className="h-[400px] overflow-auto rounded-md border p-1">
                  {queryHistory.length === 0 ? (
                    <div className="text-sm text-zinc-500 py-6 text-center">
                      No history yet. Run a query to populate this list.
                    </div>
                  ) : (
                    <div className="space-y-2">
                      {queryHistory.map((entry) => (
                        <div
                          key={entry.id}
                          role="button"
                          tabIndex={0}
                          className="p-3 border rounded-lg hover:bg-zinc-50 dark:hover:bg-zinc-900 cursor-pointer transition-colors"
                          onClick={() => loadFromHistory(entry)}
                          onKeyDown={(e) => {
                            if (e.key === "Enter" || e.key === " ") {
                              e.preventDefault()
                              loadFromHistory(entry)
                            }
                          }}
                        >
                          {entry.question && (
                            <div className="text-xs text-violet-600 mb-1 flex items-center gap-1">
                              <Sparkles className="h-3 w-3" />
                              {entry.question}
                            </div>
                          )}
                          <div className="font-mono text-xs text-zinc-600 dark:text-zinc-400 truncate">
                            {entry.sql}
                          </div>
                          <div className="flex items-center gap-3 mt-2 text-[10px] text-zinc-400">
                            <span className="flex items-center gap-1">
                              <Clock className="h-3 w-3" />
                              {entry.executionTimeMs}ms
                            </span>
                            <span>{typeof entry.rowCount === "number" ? pluralizeRows(entry.rowCount) : "—"}</span>
                            <span>{entry.timestamp.toLocaleTimeString()}</span>
                          </div>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              )}
            </CardContent>
          </Card>
        </main>
      </div>

      {/* Send to BI Tool Dialog */}
      <Dialog open={showDashboardDialog} onOpenChange={setShowDashboardDialog}>
        <DialogContent className="sm:max-w-lg">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              <BarChart3 className="h-5 w-5 text-violet-500" />
              Send to BI Tool
            </DialogTitle>
            <DialogDescription>
              Export your SQL query as a saved dashboard in your visualization tool.
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-4 py-4">
            {/* Tool selector */}
            <div className="space-y-2">
              <Label>Visualization Tool</Label>
              <div className="grid grid-cols-3 gap-2">
                {VISUALIZATION_TOOLS.map((tool) => (
                  <button
                    key={tool.id}
                    type="button"
                    onClick={() => setSelectedBiTool(tool.id)}
                    className={cn(
                      "relative flex flex-col items-start gap-0.5 rounded-lg border p-3 text-left transition-all",
                      selectedBiTool === tool.id
                        ? tool.accentClass + " border-2"
                        : "border-zinc-200 dark:border-zinc-700 hover:border-zinc-300 dark:hover:border-zinc-600",
                      !tool.implemented && "opacity-60"
                    )}
                  >
                    <span className="text-sm font-medium leading-tight">{tool.name}</span>
                    <span className="text-[10px] text-zinc-500 leading-tight">{tool.description}</span>
                    {!tool.implemented && (
                      <span className="absolute top-1.5 right-1.5 text-[9px] font-medium text-zinc-400 bg-zinc-100 dark:bg-zinc-800 px-1 rounded">
                        soon
                      </span>
                    )}
                  </button>
                ))}
              </div>
            </div>

            <div className="space-y-2">
              <Label htmlFor="dashboard-name">Dashboard Name</Label>
              <Input
                id="dashboard-name"
                placeholder="e.g., Monthly Revenue Report"
                value={dashboardName}
                onChange={(e) => setDashboardName(e.target.value)}
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="dashboard-description">Description (optional)</Label>
              <Textarea
                id="dashboard-description"
                placeholder="What does this dashboard show?"
                value={dashboardDescription}
                onChange={(e) => setDashboardDescription(e.target.value)}
                rows={2}
              />
            </div>
            <div className="p-3 bg-zinc-50 dark:bg-zinc-900 rounded-lg">
              <div className="text-xs text-zinc-500 mb-1">SQL Query</div>
              <div className="font-mono text-xs text-zinc-600 dark:text-zinc-400 truncate">
                {sqlQuery.slice(0, 120)}{sqlQuery.length > 120 ? "…" : ""}
              </div>
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setShowDashboardDialog(false)}>
              Cancel
            </Button>
            <Button
              onClick={createVisualizationDashboard}
              disabled={creatingDashboard || !dashboardName.trim()}
              className="bg-gradient-to-r from-violet-600 to-indigo-600"
            >
              {creatingDashboard ? (
                <Loader2 className="h-4 w-4 animate-spin mr-2" />
              ) : (
                <ExternalLink className="h-4 w-4 mr-2" />
              )}
              {(() => {
                const tool = VISUALIZATION_TOOLS.find((t) => t.id === selectedBiTool)
                return tool?.implemented ? `Create in ${tool.name}` : `${tool?.name ?? "Tool"} — Coming Soon`
              })()}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Cell Viewer Dialog (for long values) */}
      <Dialog open={cellViewerOpen} onOpenChange={setCellViewerOpen}>
        <DialogContent className="sm:max-w-2xl">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              <Code className="h-5 w-5 text-violet-500" />
              {cellViewerTitle}
            </DialogTitle>
            <DialogDescription>Full cell value</DialogDescription>
          </DialogHeader>
          <div className="space-y-2">
            <Textarea value={cellViewerValue} readOnly className="font-mono text-xs min-h-[240px]" />
            <div className="flex justify-end">
              <Button
                variant="outline"
                size="sm"
                onClick={() => copyToClipboard(cellViewerValue)}
              >
                <Copy className="h-4 w-4 mr-2" />
                Copy
              </Button>
            </div>
          </div>
        </DialogContent>
      </Dialog>

      {/* Destructive-statement confirmation (DROP / TRUNCATE): owner-only, type-to-confirm.
          The backend independently enforces owner via validators.ValidateExplorerStatement;
          this dialog is the UX guardrail against an accidental irreversible run. */}
      <Dialog
        open={showDestructiveConfirm}
        onOpenChange={(open) => {
          setShowDestructiveConfirm(open)
          if (!open) {
            setDestructiveConfirmText("")
            setPendingRunTarget(null)
          }
        }}
      >
        <DialogContent className="sm:max-w-lg">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2 text-red-600 dark:text-red-400">
              <AlertCircle className="h-5 w-5" />
              Confirm destructive statement
            </DialogTitle>
            <DialogDescription>
              This runs a{" "}
              <span className="font-semibold">{destructiveLabel(pendingRunTarget?.sql ?? "")}</span>{" "}
              against the selected connection. It is irreversible and can permanently delete data
              or schema.
            </DialogDescription>
          </DialogHeader>
          {/* Show the exact statement being confirmed. With several queries in the buffer the
              verb alone is ambiguous — the user needs to see WHICH DROP this is. */}
          {pendingRunTarget && (
            <pre className="max-h-32 overflow-auto rounded-md border bg-muted/50 p-2 font-mono text-xs whitespace-pre-wrap break-all">
              {pendingRunTarget.sql}
            </pre>
          )}
          <div className="space-y-2">
            <Label htmlFor="destructive-confirm" className="text-xs">
              Type{" "}
              <span className="font-mono font-semibold">
                {firstSqlVerb(pendingRunTarget?.sql ?? "")}
              </span>{" "}
              to confirm
            </Label>
            <Input
              id="destructive-confirm"
              value={destructiveConfirmText}
              onChange={(e) => setDestructiveConfirmText(e.target.value)}
              placeholder={firstSqlVerb(pendingRunTarget?.sql ?? "")}
              autoComplete="off"
            />
          </div>
          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => {
                setShowDestructiveConfirm(false)
                setDestructiveConfirmText("")
                setPendingRunTarget(null)
              }}
            >
              Cancel
            </Button>
            <Button
              variant="destructive"
              disabled={
                !pendingRunTarget ||
                destructiveConfirmText.trim().toUpperCase() !== firstSqlVerb(pendingRunTarget.sql)
              }
              onClick={() => {
                // Run the target the dialog was opened for. Falling back to the live
                // runTarget here would re-introduce the confirm-one/run-another gap.
                const target = pendingRunTarget
                setShowDestructiveConfirm(false)
                setDestructiveConfirmText("")
                setPendingRunTarget(null)
                if (target) executeQuery(target)
              }}
            >
              <Play className="h-4 w-4 mr-2" />
              Run {firstSqlVerb(pendingRunTarget?.sql ?? "")}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* HITL: Table selection when table linking is ambiguous */}
      <HITLTablePicker
        open={tableHITLOpen}
        onOpenChange={setTableHITLOpen}
        candidates={tableHITLCandidates}
        question={nlInput}
        onConfirm={(selectedFullTables) => {
          // Preserve schema-qualified tables so execution uses the right database.
          const selected = (selectedFullTables || []).map((s) => normalizeTableKey(s)).filter(Boolean)
          setSelectedTables(selected)
          // Immediately retry using the confirmed tables (avoid stale state).
          generateSQL({ tablesOverride: selected })
        }}
        onCancel={() => {
          setTableHITLOpen(false)
        }}
      />

      <HITLMetricPicker
        open={metricHITLOpen}
        onOpenChange={setMetricHITLOpen}
        question={nlInput}
        options={metricHitlOptions.length > 0 ? metricHitlOptions : undefined}
        onConfirm={(choice) => {
          setMetricChoice(choice)
          metricChoiceRef.current = choice
          setMetricHITLOpen(false)
          const pending = pendingGenerateRef.current
          pendingGenerateRef.current = null
          // Retry with same tables; metricChoice is read from state when building question hint.
          if (pending?.tablesOverride) {
            generateSQL({ tablesOverride: pending.tablesOverride, metricChoiceOverride: choice })
          } else {
            generateSQL({ metricChoiceOverride: choice })
          }
        }}
        onCancel={() => {
          pendingGenerateRef.current = null
          setMetricHITLOpen(false)
        }}
      />
    </div>
  )
}

// Helper to format cell values for display
function formatCellValue(value: unknown, opts?: { truncate?: boolean }): string {
  if (value === null || value === undefined) return "NULL"
  if (typeof value === "object") return JSON.stringify(value)
  if (typeof value === "boolean") return value ? "true" : "false"
  const str = String(value)
  if (opts?.truncate && str.length > 200) return str.slice(0, 200) + "…"
  return str
}

function safeJsonParse(raw: string): any | null {
  const s = String(raw ?? "").trim()
  if (!s) return null
  try {
    return JSON.parse(s)
  } catch {
    return null
  }
}

// Extract the human-readable message from any API response shape.
function extractRawMessage(data: any): string {
  if (typeof data === "string") return data.trim()
  if (!data || typeof data !== "object") return ""
  const direct = data.error || data.message || (typeof data.detail === "string" ? data.detail : "")
  if (direct) return String(direct).trim()
  if (Array.isArray(data.detail)) {
    return data.detail
      .map((d: any) => {
        if (typeof d === "string") return d
        const loc = Array.isArray(d.loc) ? d.loc.filter((x: any) => x !== "body").join(".") : ""
        const msg = d.msg || d.message || ""
        return loc ? `${loc}: ${msg}` : msg
      })
      .filter(Boolean)
      .join("; ")
  }
  return ""
}

// Single classification function: HTTP status + response body + call context → ExplorerError.
// All error paths in this file use this instead of ad-hoc string concatenation.
function classifyApiError(
  status: number | undefined,
  data: any,
  context: "table_resolve" | "generate" | "execute" | "schema" | "connections" | "export" | "bi_tool" | "general"
): ExplorerError {
  const raw = extractRawMessage(data)
  const lower = raw.toLowerCase()
  // Use hint from the response body when the server provided one
  const serverHint: string | undefined =
    data && typeof data === "object" && typeof data.hint === "string" ? data.hint.trim() : undefined

  // Plan quota (Ship 3 Phase 2): a 402 from /sql/generate means the workspace is
  // over its monthly NL→SQL allowance. Direct SQL stays unlimited, so point there too.
  if (status === 402) {
    const errorCode =
      data && typeof data === "object" && typeof data.error === "string" ? data.error : undefined
    if (errorCode === "query_limit_reached") {
      return {
        code: "quota",
        title: "Query limit reached",
        message: raw || "You've used all the NL→SQL queries included in your plan this month.",
        hint: serverHint || "Upgrade your plan to generate more queries, or run direct SQL (unlimited).",
      }
    }
    return {
      code: "quota",
      title: "Plan limit reached",
      message: raw || "This action isn't available on your current plan.",
      hint: serverHint || "Upgrade your plan to continue.",
    }
  }

  if (status === 401 || status === 403) {
    // Role/permission rejections from the Explorer statement gate are NOT session
    // problems — surface them as a permission error with the server's hint, not
    // "refresh to sign in".
    const errorCode =
      data && typeof data === "object" && typeof data.error_code === "string" ? data.error_code : undefined
    if (errorCode === "INSUFFICIENT_ROLE_FOR_STATEMENT" || errorCode === "STATEMENT_NOT_ALLOWED") {
      return {
        code: "validation",
        title: "Not permitted",
        message: raw || "You don't have permission to run this statement.",
        hint: serverHint || "Ask a workspace admin or owner to run this, or request a higher role.",
      }
    }
    return {
      code: "auth",
      title: "Authentication error",
      message: raw || "Your session may have expired.",
      hint: serverHint || "Refresh the page to sign in again.",
    }
  }

  if (status === 503) {
    const isLlmContext = context === "table_resolve" || context === "generate"
    return {
      code: isLlmContext ? "llm_offline" : "service_unavailable",
      title: isLlmContext ? "AI service unavailable" : "Service unavailable",
      message: raw || "The service could not be reached.",
      hint: serverHint || (
        isLlmContext
          ? "The LLM service (Ollama) may not be running. Ask your admin to check the llm-service."
          : "A backend service is temporarily unavailable. Try again shortly."
      ),
    }
  }

  if (status === 422) {
    return {
      code: "validation",
      title: "Invalid request",
      message: raw || "The server rejected the request due to missing or invalid fields.",
      hint: serverHint || "This may indicate a bug. Try refreshing the page and re-running your query.",
    }
  }

  if (!status) {
    return {
      code: "network",
      title: "Connection failed",
      message: raw || "Could not reach the server.",
      hint: serverHint || "Check your network connection and try again.",
    }
  }

  if (lower.includes("llm") || lower.includes("ollama") || lower.includes("chat completion") || lower.includes("model not")) {
    return {
      code: "llm_error",
      title: "AI service error",
      message: raw,
      hint: "The LLM service may be unavailable or the model may not be loaded. Check the llm-service logs.",
    }
  }

  if (context === "execute") {
    // Missing TABLE and missing COLUMN are different fixes, so decide on the
    // engine's own error code before falling back to substring matching. The
    // codes are unambiguous; the words are not — a missing-table error is
    // routinely wrapped in text that mentions columns ("failed to read
    // columns: …"), which used to make every dropped table render the
    // "referenced column may no longer exist" hint.
    // undefined_table: PG 42P01 · MySQL 1146/42S02 · SQL Server 42S02 · Oracle ORA-00942
    // undefined_column: PG 42703 · MySQL 1054/42S22 · SQL Server 42S22 · Oracle ORA-00904
    const missingTableCode = /\b(42P01|42S02|ORA-00942)\b/i.test(raw) || /\b1146\b/.test(raw)
    const missingColumnCode = /\b(42703|42S22|ORA-00904)\b/i.test(raw) || /\b1054\b/.test(raw)
    const looksMissing = lower.includes("not exist") || lower.includes("doesn't exist") || lower.includes("unknown") || lower.includes("invalid object")
    const hint = missingTableCode
      ? "A referenced table was not found. Try refreshing the schema."
      : missingColumnCode
      ? "A referenced column may no longer exist. Try refreshing the schema."
      // No code to go on — check table-ness first, since its errors are the
      // ones that carry stray "column" wrapper text.
      : looksMissing && (lower.includes("relation") || lower.includes("table"))
      ? "A referenced table was not found. Try refreshing the schema."
      : lower.includes("column")
      ? "A referenced column may no longer exist. Try refreshing the schema."
      : lower.includes("relation") || lower.includes("does not exist")
      ? "A referenced table was not found. Try refreshing the schema."
      : lower.includes("timeout") || lower.includes("statement canceled") || lower.includes("canceling")
      ? "The query timed out. Add WHERE filters to reduce the amount of data scanned."
      : lower.includes("syntax")
      ? "The SQL has a syntax error. Edit the query and try again."
      : lower.includes("permission") || lower.includes("denied")
      ? "Your database user may not have permission to read this table."
      : undefined
    return { code: "db_error", title: "Query failed", message: raw || "The database returned an error.", hint }
  }

  if (context === "table_resolve") {
    return {
      code: "table_resolve_failed",
      title: "Could not identify tables",
      message: raw || "The AI could not determine which tables are relevant to your question.",
      hint: "Select tables manually in the sidebar, or rephrase your question to mention specific table names.",
    }
  }

  if (context === "generate") {
    return {
      code: "sql_gen_failed",
      title: "SQL generation failed",
      message: raw || "Could not generate SQL for this question.",
      hint: "Try rephrasing your question or selecting specific tables first.",
    }
  }

  if (context === "schema") {
    const isDnsFailure = lower.includes("no such host") || lower.includes("name or service not known")
    const isConnRefused = lower.includes("connection refused") || lower.includes("connect: refused")
    const hint = isDnsFailure
      ? "The database hostname could not be resolved. If running locally, check that the database Docker container is running (`docker compose up -d`)."
      : isConnRefused
      ? "The database server is unreachable. Check that it is running and the host/port are correct."
      : "Check that the database connection is active, then click the refresh button to try again."
    return {
      code: "schema_failed",
      title: "Failed to load schema",
      message: raw || `Schema fetch failed (${status}).`,
      hint,
    }
  }

  if (context === "export") {
    return {
      code: "export_failed",
      title: "Export failed",
      message: raw || `Could not export results (HTTP ${status}).`,
      hint: lower.includes("timeout") || lower.includes("timed out")
        ? "The query took too long. Try adding a LIMIT clause before exporting."
        : "Try re-running the query and exporting again.",
    }
  }

  if (context === "bi_tool") {
    return {
      code: "bi_tool_failed",
      title: "Could not create dashboard",
      message: raw || "The BI tool returned an unexpected error.",
      hint: serverHint || (
        lower.includes("connect") || lower.includes("refused")
          ? "Check that the BI tool is running and its URL is configured correctly."
          : lower.includes("token") || lower.includes("auth")
          ? "API credentials may be invalid. Check the relevant API key in your environment."
          : "Make sure the BI tool is accessible and your API key is valid."
      ),
    }
  }

  return {
    code: "unknown",
    title: "Something went wrong",
    message: raw || "An unexpected error occurred.",
    hint: status >= 500 ? "This appears to be a server-side error. Try again in a moment." : undefined,
  }
}

function extractDetailsLines(data: any): string[] {
  const out: string[] = []

  // Common patterns:
  // - { details: string[] }
  // - { details: [{ message, suggestion? }, ...] }
  // - FastAPI style: { detail: [{ loc, msg, type }, ...] }
  const primary = data?.details ?? (Array.isArray(data?.detail) ? data.detail : undefined)
  const items = Array.isArray(primary) ? primary : primary ? [primary] : []

  for (const it of items) {
    if (!it) continue
    if (typeof it === "string") {
      out.push(it)
      continue
    }
    if (typeof it === "object") {
      const msg = typeof it.message === "string" ? it.message : typeof it.msg === "string" ? it.msg : ""
      const sug = typeof it.suggestion === "string" ? it.suggestion : ""
      const loc = Array.isArray(it.loc) ? it.loc.map((x: any) => String(x)).filter(Boolean).join(".") : ""
      if (loc && msg) {
        out.push(`${loc}: ${msg}${sug ? ` (${sug})` : ""}`)
        continue
      }
      if (msg) {
        out.push(`${msg}${sug ? ` (${sug})` : ""}`)
        continue
      }
      try {
        const raw = JSON.stringify(it)
        if (raw) out.push(raw.length > 500 ? raw.slice(0, 500) + "…" : raw)
      } catch {
        // ignore
      }
    }
  }

  // Some services return `validation_errors: string[]`
  if (Array.isArray(data?.validation_errors)) {
    for (const v of data.validation_errors) {
      if (typeof v === "string" && v.trim()) out.push(v.trim())
    }
  }

  // Deduplicate while preserving order.
  const seen = new Set<string>()
  return out.filter((s) => {
    const k = String(s || "").trim()
    if (!k) return false
    if (seen.has(k)) return false
    seen.add(k)
    return true
  })
}

function clampInt(n: number, min: number, max: number): number {
  if (!Number.isFinite(n)) return min
  const x = Math.trunc(n)
  if (x < min) return min
  if (x > max) return max
  return x
}

function sameTableSet(a: string[], b: string[]): boolean {
  const norm = (s: string) => String(s || "").trim().toLowerCase()
  const as = new Set((a || []).map(norm).filter(Boolean))
  const bs = new Set((b || []).map(norm).filter(Boolean))
  if (as.size !== bs.size) return false
  for (const k of as) {
    if (!bs.has(k)) return false
  }
  return true
}

// Extract "N rows" / "top N" / "limit N" style hints from NL prompts.
function extractRequestedRowLimit(text: string): number | null {
  const t = String(text || "").trim()
  if (!t) return null

  const patterns: RegExp[] = [
    /\b(?:show|give)\s+me\s+(\d{1,6})\s+rows?\b/i,
    /\b(?:top|first)\s+(\d{1,6})\b/i,
    /\blimit\s+(\d{1,6})\b/i,
    /\b(\d{1,6})\s+rows?\b/i,
  ]

  for (const re of patterns) {
    const m = t.match(re)
    if (m?.[1]) {
      const n = Number(m[1])
      if (Number.isFinite(n) && n > 0) return Math.trunc(n)
    }
  }
  return null
}

function extractLimitFromSQL(sql: string): number | null {
  const s = String(sql || "")
  const m = s.match(/\bLIMIT\s+(\d+)\b/i)
  if (!m?.[1]) return null
  const n = Number(m[1])
  if (!Number.isFinite(n) || n <= 0) return null
  return Math.trunc(n)
}

function setOrReplaceLimit(sql: string, limit: number): string {
  const n = clampInt(limit, 1, Number.MAX_SAFE_INTEGER)
  const trimmed = String(sql || "").trim().replace(/;+\s*$/, "")
  if (!trimmed) return `SELECT 1 LIMIT ${n}`

  if (/\bLIMIT\s+\d+\b/i.test(trimmed)) {
    return trimmed.replace(/\bLIMIT\s+\d+\b/i, `LIMIT ${n}`)
  }
  return `${trimmed} LIMIT ${n}`
}

function isSelectOnlySql(sql: string): boolean {
  const s = String(sql || "").trim()
  if (!s) return false
  const upper = s.toUpperCase()
  return upper.startsWith("SELECT") || upper.startsWith("WITH")
}

/** A result is a write outcome (render "N rows affected", hide export/BI) when its
 *  statement_type is anything other than a read. */
function isWriteResult(statementType?: string): boolean {
  if (!statementType) return false
  const t = statementType.toUpperCase()
  return t !== "SELECT" && t !== "WITH"
}

// ==========================
// Schema-aware disambiguation
// ==========================

type TableIndex = {
  tables: TableMetadata[]
  byKey: Map<string, TableMetadata>
  tokensByKey: Map<string, { schemaTokens: Set<string>; tableTokens: Set<string>; columnTokens: Set<string> }>
  displayByKey: Map<string, { schema: string; table: string }>
}

type RankedTable = {
  key: string
  schema_name: string
  table: string
  score: number
  confidence: number
  reason: string
}

function buildTableIndex(all: TableMetadata[]): TableIndex {
  const byKey = new Map<string, TableMetadata>()
  const tokensByKey = new Map<string, { schemaTokens: Set<string>; tableTokens: Set<string>; columnTokens: Set<string> }>()
  const displayByKey = new Map<string, { schema: string; table: string }>()

  for (const t of all || []) {
    const key = tableKeyFromMeta(t)
    byKey.set(key, t)
    const schema = String(t.schema || "").trim() || "public"
    const table = normalizeTableName(t.name)

    const schemaTokens = new Set(tokenizeForMatch(schema))
    const tableTokens = new Set(tokenizeForMatch(table))
    const columnTokens = new Set(
      (t.columns || [])
        .flatMap((c) => tokenizeForMatch(c.name))
        .filter(Boolean)
    )

    tokensByKey.set(key, { schemaTokens, tableTokens, columnTokens })
    displayByKey.set(key, { schema, table })
  }

  return { tables: all || [], byKey, tokensByKey, displayByKey }
}

function tokenizeForMatch(s: string): string[] {
  const raw = String(s || "")
    .toLowerCase()
    .replace(/[`"'()[\]{}]/g, " ")
    .replace(/[^a-z0-9_]+/g, " ")
    .trim()
  if (!raw) return []
  const parts = raw
    .split(/\s+/)
    .flatMap((p) => p.split("_"))
    .map((x) => x.trim())
    .filter(Boolean)

  const stop = new Set([
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "by",
    "for",
    "from",
    "in",
    "is",
    "it",
    "me",
    "of",
    "on",
    "or",
    "per",
    "show",
    "the",
    "to",
    "with",
    "what",
    "when",
    "where",
    "who",
    "how",
    "total",
  ])

  return parts
    .map((p) => (p.endsWith("s") && p.length > 3 ? p.slice(0, -1) : p))
    .filter((p) => p.length >= 2 && !stop.has(p))
}

function rankTablesForQuestion(question: string, index: TableIndex): RankedTable[] {
  const qTokens = tokenizeForMatch(question)
  if (qTokens.length === 0) return []

  const scored: RankedTable[] = []
  for (const [key, toks] of index.tokensByKey.entries()) {
    let score = 0
    const matched: string[] = []

    for (const tok of qTokens) {
      let inc = 0
      if (toks.tableTokens.has(tok)) inc = Math.max(inc, 4)
      if (toks.schemaTokens.has(tok)) inc = Math.max(inc, 3)
      if (toks.columnTokens.has(tok)) inc = Math.max(inc, 1)
      if (inc > 0) matched.push(tok)
      score += inc
    }

    if (score <= 0) continue
    const disp = index.displayByKey.get(key) || { schema: "public", table: key }
    scored.push({
      key,
      schema_name: disp.schema,
      table: disp.table,
      score,
      confidence: 0,
      reason: matched.length ? `Matched: ${Array.from(new Set(matched)).slice(0, 6).join(", ")}` : "Schema match",
    })
  }

  scored.sort((a, b) => b.score - a.score)
  const max = scored[0]?.score || 1
  for (const s of scored) {
    s.confidence = Math.max(0, Math.min(1, s.score / max))
  }
  return scored
}

function decideTableHitl(args: {
  question: string
  userSelected: string[]
  suggested: string[]
  localTop: RankedTable[]
  serverConfidence: number
  serverNeedsHitl: boolean
}): { shouldAsk: boolean; reason?: string } {
  if (args.serverNeedsHitl) return { shouldAsk: true, reason: "Need confirmation on which tables to use" }

  const top = args.localTop[0]
  const second = args.localTop[1]
  const userSet = new Set((args.userSelected || []).map((t) => normalizeTableKey(t)))
  const suggestedSet = new Set((args.suggested || []).map((t) => normalizeTableKey(t)))

  // If question doesn't strongly match anything, don't pop extra UI.
  if (!top || top.score < 4) return { shouldAsk: false }

  // If the best match is not among suggested tables (and server isn't very confident), ask.
  if (!suggestedSet.has(normalizeTableKey(`${top.schema_name}.${top.table}`)) && args.serverConfidence < 0.85) {
    return { shouldAsk: true, reason: `Your question seems to match ${top.schema_name}.${top.table} more than the suggested tables` }
  }

  // If user selected tables but the best match differs, ask rather than silently switching domains.
  if (userSet.size > 0 && !userSet.has(normalizeTableKey(`${top.schema_name}.${top.table}`))) {
    return { shouldAsk: true, reason: `Your question matches ${top.schema_name}.${top.table} more than your current selection` }
  }

  // If top-2 are close, ask.
  if (second && top.score - second.score <= 2 && second.score >= 3) {
    return { shouldAsk: true, reason: "Multiple tables look relevant — confirm which ones to use" }
  }

  return { shouldAsk: false }
}

function mergeTableCandidates(args: {
  serverCandidates: HITLTableCandidate[]
  localTop: RankedTable[]
  tables: TableMetadata[]
}): HITLTableCandidate[] {
  const out = new Map<string, HITLTableCandidate>()

  for (const c of args.serverCandidates || []) {
    const key = `${String(c.schema_name || "public")}.${String(c.table || "")}`
    out.set(key, c)
  }

  for (const r of args.localTop || []) {
    const key = `${r.schema_name}.${r.table}`
    if (out.has(key)) continue
    out.set(key, {
      table: r.table,
      schema_name: r.schema_name,
      confidence: r.confidence,
      reason: r.reason || "Schema match",
    })
  }

  return Array.from(out.values()).sort((a, b) => (b.confidence || 0) - (a.confidence || 0))
}

function decideMetricHitl(args: {
  question: string
  selectedTables: string[]
  tableIndex: TableIndex
  currentChoice: MetricChoice | null
}): {
  shouldAsk: boolean
  confidence: number
  reason: string
  options: Array<{ id: MetricChoice; title: string; description: string; default?: boolean }>
} {
  const q = String(args.question || "").toLowerCase()
  const already = args.currentChoice
  if (already) {
    return { shouldAsk: false, confidence: 1, reason: "Metric already chosen", options: [] }
  }

  const wantsCount = /\b(count|how many|number of)\b/.test(q)
  const wantsAmount = /\b(amount|revenue|gmv|value|sum|\$|cents|usd)\b/.test(q)
  const mentionsTotal = /\btotal\b/.test(q)

  // Detect if selected (linked) tables have any "amount-like" columns, making "total" ambiguous.
  const amountLike = new Set(["amount", "revenue", "price", "total", "value", "cents", "usd"])
  const linkedKeys = (args.selectedTables || []).map((t) => normalizeTableKey(t)).filter(Boolean)
  const entityLabel = inferEntityLabel(linkedKeys, args.tableIndex)
  const amountCol = findBestAmountLikeColumn(linkedKeys, args.tableIndex, amountLike)
  const hasAmountColumn = Boolean(amountCol)

  const countTitle = entityLabel ? `Count of ${entityLabel}` : "Count of rows"
  const sumTitle = amountCol
    ? `Sum of ${humanizeIdentifier(amountCol.column)}`
    : "Sum of amount/value"
  const sumDesc = amountCol
    ? `Sum ${amountCol.column} from ${amountCol.tableDisplay} (SUM).`
    : "Sum an amount/value-like column (SUM)."

  const options = [
    {
      id: "count" as const,
      title: countTitle,
      description: entityLabel ? `How many ${entityLabel} (COUNT).` : "How many rows/records (COUNT).",
      default: true,
    },
    { id: "sum_amount" as const, title: sumTitle, description: sumDesc },
  ]

  // If user explicitly asked for count or amount, don't ask.
  if (wantsCount) return { shouldAsk: false, confidence: 0.9, reason: "Explicit count intent", options }
  if (wantsAmount) return { shouldAsk: false, confidence: 0.9, reason: "Explicit amount intent", options }

  // If "total" appears with no explicit amount words, and schema has amount-like fields, ask.
  if (mentionsTotal && hasAmountColumn) {
    return { shouldAsk: true, confidence: 0.55, reason: "“Total” can mean count or sum — please confirm", options }
  }

  return { shouldAsk: false, confidence: 0.8, reason: "Metric unambiguous", options }
}

function applyMetricHintToQuestion(
  question: string,
  choice: MetricChoice | null | undefined,
  ctx?: { selectedTables: string[]; tableIndex: TableIndex }
): string {
  const q = String(question || "")
  if (!choice) return q
  const linked = (ctx?.selectedTables || []).map((t) => normalizeTableKey(t)).filter(Boolean)
  const amountLike = new Set(["amount", "revenue", "price", "total", "value", "cents", "usd"])
  const amountCol = ctx?.tableIndex ? findBestAmountLikeColumn(linked, ctx.tableIndex, amountLike) : null

  const hint =
    choice === "count"
      ? "Metric: COUNT rows/records. Do NOT SUM any amount/value column."
      : amountCol
        ? `Metric: SUM(${amountCol.column}) from ${amountCol.tableDisplay}.`
        : "Metric: SUM an amount/value-like column (e.g., amount, total, revenue)."
  return `${q}\n\n${hint}`
}

function humanizeIdentifier(input: string): string {
  const s = String(input || "").trim()
  if (!s) return ""
  return s
    .replace(/[`"'()[\]{}]/g, "")
    .replace(/_/g, " ")
    .replace(/\s+/g, " ")
    .trim()
}

function inferEntityLabel(linkedTableKeys: string[], index: TableIndex): string {
  const first = linkedTableKeys[0]
  if (!first) return ""
  const disp = index.displayByKey.get(first)
  const table = disp?.table || parseQualifiedTable(first).table
  const human = humanizeIdentifier(table)
  if (!human) return ""
  // Example: "order_items" -> "order items"
  return human
}

function findBestAmountLikeColumn(
  linkedTableKeys: string[],
  index: TableIndex,
  amountLike: Set<string>
): { tableKey: string; tableDisplay: string; column: string; score: number } | null {
  let best: { tableKey: string; tableDisplay: string; column: string; score: number } | null = null

  for (const k of linkedTableKeys || []) {
    const meta = index.byKey.get(k) || index.byKey.get(qualifyTableKeyIfPossible(k, index.tables))
    if (!meta) continue
    const disp = index.displayByKey.get(tableKeyFromMeta(meta)) || index.displayByKey.get(k)
    const tableDisplay = disp ? `${disp.schema}.${disp.table}` : k

    for (const c of meta.columns || []) {
      const toks = tokenizeForMatch(c.name)
      const hits = toks.filter((t) => amountLike.has(t))
      if (hits.length === 0) continue
      // Prefer more specific columns (e.g. amount_cents) over generic (e.g. total).
      const score = hits.length + (String(c.name).toLowerCase().includes("amount") ? 2 : 0)
      if (!best || score > best.score) {
        best = { tableKey: k, tableDisplay, column: c.name, score }
      }
    }
  }

  return best
}
