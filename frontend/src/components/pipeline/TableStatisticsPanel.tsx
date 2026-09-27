"use client"

import { useCallback, useEffect, useRef, useState, type ReactNode } from "react"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import {
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow
} from "@/components/ui/table"
import {
  DropdownMenu,
  DropdownMenuCheckboxItem,
  DropdownMenuContent,
  DropdownMenuLabel,
  DropdownMenuRadioGroup,
  DropdownMenuRadioItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"
import {
  CheckCircle2,
  XCircle,
  Loader2,
  Download,
  RefreshCw,
  Search,
  Database,
  AlertTriangle,
  Clock,
  ArrowDown,
  ArrowUp,
  ArrowUpDown,
  ChevronLeft,
  ChevronRight,
  Settings2,
  Pause,
  MinusCircle,
} from "lucide-react"
import { API_ENDPOINTS } from "@/lib/config/api"
import { onPipelineRefresh } from "@/lib/events/pipelineRefresh"
import { normalizePipelineStatus } from "@/lib/pipeline/statusNormalization"
import { toast } from "sonner"
import { authFetch } from "@/lib/api/auth-fetch"
import { classifyError } from "@/lib/utils/error-handling"
import { cn, formatAbsoluteTime } from "@/lib/utils"
import { formatDurationBetween } from "@/lib/duration"
import { TABLE_STATUS_WAITING_FOR_DATA } from "./executionSummary"

type TableStat = {
  schema_name?: string
  table_name: string
  qualified_name: string
  mode: "batch" | "cdc"
  status: string

  // Batch fields
  read_rows?: number
  inserted_rows?: number

  // CDC fields. `inserts` counts real inserts (op c) only; rows a snapshot
  // READ (op r) are in `snapshot_rows` (older rows may still mix them in).
  inserts?: number
  updates?: number
  deletes?: number
  total_events?: number
  last_event_ts?: string
  snapshot_rows?: number
  applied_snapshot_rows?: number
  // "streaming_only" = added via Edit tables without loading its existing rows.
  load_mode?: string

  // CDC applied fields (destination-truth)
  applied_inserts?: number
  applied_updates?: number
  applied_deletes?: number
  applied_total_events?: number
  last_applied_ts?: string

  // Rows the destination will never receive — parked in the sink's dead-letter queue.
  // Every other counter here reports what landed; this is the only one that reports
  // what did not. Always present (the API does not omit a zero).
  dlq_rows?: number

  // Timestamps
  started_at?: string
  completed_at?: string
  updated_at: string
}

export type TableStatsSummary = {
  mode: string // "batch" | "cdc" | "mixed"
  total_tables: number
  tables_completed: number
  tables_failed: number
  tables_running: number
  tables_degraded?: number
  // Selected CDC tables with nothing captured or applied yet (not in tables_running).
  tables_waiting_for_data?: number
  // No longer selected. NOT in total_tables, but still in every total below:
  // their rows are still at the destination.
  tables_removed?: number

  // Batch aggregates
  total_read_rows?: number
  total_inserted_rows?: number

  // CDC aggregates
  total_inserts?: number
  total_updates?: number
  total_deletes?: number
  total_cdc_events?: number

  // CDC applied aggregates
  total_applied_inserts?: number
  total_applied_updates?: number
  total_applied_deletes?: number
  total_applied_cdc_events?: number
  total_snapshot_rows?: number
  total_applied_snapshot_rows?: number

  // Dropped-row aggregates (both modes)
  total_dlq_rows?: number
  tables_with_dlq?: number
}

/** The last batch run that read or wrote a row (table_stats.go lastBatchRunThatMovedRows). */
export type LastDataRun = {
  execution_id: string
  tables?: number
  rows_read?: number
  rows_written?: number
  finished_at?: string
}

/**
 * A finished batch run that read and wrote nothing: every table completed at 0.
 * That is a Resume with nothing new at the source, not a failed copy.
 */
export function batchRunMovedNothing(summary: TableStatsSummary | null | undefined): boolean {
  if (!summary || summary.mode !== "batch" || summary.total_tables <= 0) return false
  return (
    summary.tables_running === 0 &&
    summary.tables_failed === 0 &&
    (summary.tables_degraded ?? 0) === 0 &&
    (summary.total_read_rows ?? 0) === 0 &&
    (summary.total_inserted_rows ?? 0) === 0 &&
    !summary.total_dlq_rows
  )
}

type Props = {
  pipelineId: string
  executionId?: string
  // The pipeline's status (the monitoring panel passes the reconciled one).
  // "paused" relabels the tables that would otherwise read Running (#14).
  pipelineStatus?: string
  blockingReasonType?: string
  mode?: "batch" | "cdc"
}

function shortId(id?: string): string {
  return (id || "").slice(0, 8)
}

/** Rendered where a metric exists but has no value yet — never for a real 0. */
const NO_VALUE = "—"

function formatNumber(num?: number | null): string {
  // `!num` also caught 0, so "the destination rejected every row" and "this
  // column was never populated" printed the same character. They call for
  // opposite responses from an operator, so they get different glyphs. Callers
  // that genuinely mean zero say so (`table.dlq_rows ?? 0`).
  if (num === undefined || num === null) return NO_VALUE
  return new Intl.NumberFormat("en-US").format(num)
}

const NO_DATA_YET = "No data yet"

function isWaitingForData(table: Pick<TableStat, "status">): boolean {
  return table.status === TABLE_STATUS_WAITING_FOR_DATA
}

/**
 * A table taken out of the selection. It streams nothing, but it is kept: its
 * rows are still at the destination and its counts are still in the totals.
 */
const TABLE_STATUS_REMOVED = "removed"

/**
 * A CDC table added via Edit tables without loading its existing rows. Its
 * counters start at the add, and the destination holds only what changed since:
 * the one thing about a table no counter on the row can say.
 */
const CHANGES_ONLY_HELP =
  "Added without loading its existing rows: the destination holds only the changes made since it was added."

function isChangesOnly(table: Pick<TableStat, "mode" | "status" | "load_mode">): boolean {
  return (
    table.mode === "cdc" &&
    table.status !== TABLE_STATUS_REMOVED &&
    String(table.load_mode || "").toLowerCase() === "streaming_only"
  )
}

// What each counter measures (#7/#11). None of them is the number of rows the
// destination holds now, which is the question they were being read as answering.
const CAPTURED_HELP = "Change events Debezium wrote to Kafka for this table."
const APPLIED_HELP = "Events the sink wrote to the destination for this table."
const SNAPSHOT_HELP =
  "Rows read from the source by the initial load and by re-snapshots. A re-snapshot reads every row again, " +
  "so this can exceed the table's row count."
const IUD_HELP = "Inserts / updates / deletes streamed after the load."
export const CDC_COUNTER_LEGEND =
  "Captured = change events Debezium wrote to Kafka · Applied = events the sink wrote to the destination · " +
  "Snapshot rows = rows read from the source by the initial load and re-snapshots (a re-snapshot reads every " +
  "row again, so this can exceed the table's row count) · I / U / D = inserts, updates and deletes streamed " +
  "after the load. None of these is the destination's current row count."

/** A count cell: "No data yet" for a table that has reported nothing, else the number. */
function formatCount(table: Pick<TableStat, "status">, num?: number | null): string {
  if (isWaitingForData(table) && (num === undefined || num === null)) return NO_DATA_YET
  return formatNumber(num)
}

// In the viewer's zone, with the zone named. This used to print the UTC wall-clock
// time ("10:00:00") with no zone, which a viewer outside UTC reads as their own.
function formatTimestamp(ts?: string): string {
  return ts ? formatAbsoluteTime(ts) : ""
}

/**
 * Blank, not a dash, when a table has no start yet: this is a dense per-table grid
 * where an empty cell is quieter than a row of dashes. The words are the shared
 * ones — this panel used to render a stopwatch ("15:26") beside the Overview's
 * "15m 26s" for the same table, which reads as two different measurements.
 */
function formatElapsed(start?: string, end?: string): string {
  if (!start) return ""
  const out = formatDurationBetween(start, end)
  return out === "—" ? "" : out
}

function splitQualifiedName(q?: string): { schema?: string; name: string } {
  const raw = String(q || "").trim()
  if (!raw) return { name: "" }
  let parts = raw.split(".").map((p) => p.trim()).filter(Boolean)
  // Normalize common duplication patterns (e.g. "db.db.table") so UI doesn't show schema twice.
  while (parts.length >= 2 && parts[0] && parts[0] === parts[1]) {
    parts = [parts[0], ...parts.slice(2)]
  }
  if (parts.length <= 1) return { name: raw }
  return { schema: parts.slice(0, -1).join("."), name: parts[parts.length - 1] }
}

/** Schema and table name for the two sticky columns. Older rows may lack qualified_name. */
function tableIdentity(table: TableStat): { schema?: string; name: string } {
  const { schema, name } = splitQualifiedName(table.qualified_name)
  if (schema) return { schema, name }
  return { schema: table.schema_name || undefined, name: table.table_name || name }
}

/**
 * `paused`: the pipeline is paused, so a table the stats call "running" is not
 * moving. Showing a spinner there read as "still streaming" (#14).
 */
export function StatusBadge({ status, paused }: { status: string; paused?: boolean }) {
  if (paused && status === "running") {
    return (
      <Badge variant="outline" className="gap-1 text-muted-foreground" title="The pipeline is paused">
        <Pause className="h-3 w-3" />
        Paused
      </Badge>
    )
  }
  switch (status) {
    case "completed":
      return (
        <Badge variant="outline" className="gap-1">
          <CheckCircle2 className="h-3 w-3 text-green-600" />
          Completed
        </Badge>
      )
    case "failed":
      return (
        <Badge variant="destructive" className="gap-1">
          <XCircle className="h-3 w-3" />
          Failed
        </Badge>
      )
    case "running":
      return (
        <Badge variant="default" className="gap-1">
          <Loader2 className="h-3 w-3 animate-spin" />
          Running
        </Badge>
      )
    case "degraded":
      return (
        <Badge variant="outline" className="gap-1 border-amber-400 bg-amber-50 text-amber-700 dark:border-amber-600 dark:bg-amber-950/30 dark:text-amber-400">
          <AlertTriangle className="h-3 w-3" />
          Degraded
        </Badge>
      )
    case TABLE_STATUS_WAITING_FOR_DATA:
      return (
        <Badge variant="outline" className="gap-1 text-muted-foreground">
          <Clock className="h-3 w-3" />
          {NO_DATA_YET}
        </Badge>
      )
    case TABLE_STATUS_REMOVED:
      return (
        <Badge
          variant="outline"
          className="gap-1 text-muted-foreground"
          title="No longer selected: it streams nothing, but its data at the destination stays and its counts are kept"
        >
          <MinusCircle className="h-3 w-3" />
          Removed
        </Badge>
      )
    default:
      return <Badge variant="outline">{status}</Badge>
  }
}

// ---------------------------------------------------------------------------
// Sorting — only the keys the API implements (`table_stats.go`), each in the
// one direction the API sorts it. There is no direction parameter, so a header
// click never pretends to reverse an order the server cannot produce.
// ---------------------------------------------------------------------------

type SortKey = "qualified_name" | "status" | "updated_at" | "inserts" | "inserted_rows"

const SORT_DIRECTION: Record<SortKey, "ascending" | "descending" | "other"> = {
  qualified_name: "ascending",
  // failed → degraded → running → … : worst first, not alphabetical.
  status: "other",
  updated_at: "descending",
  inserts: "descending",
  inserted_rows: "descending",
}

const SORT_TITLE: Record<SortKey, string> = {
  qualified_name: "Sort by table name, A–Z",
  status: "Sort by status, failed and degraded tables first",
  updated_at: "Sort by most recently updated",
  inserts: "Sort by captured inserts, most first",
  inserted_rows: "Sort by rows written, most first",
}

// ---------------------------------------------------------------------------
// Viewer preferences — page size and hidden column groups. Per browser, and a
// convenience only: storage that throws or is empty leaves the defaults.
// ---------------------------------------------------------------------------

type ColumnGroup = "timing" | "captured" | "applied" | "updated"

const COLUMN_GROUPS: { id: ColumnGroup; label: string; cdcOnly?: boolean }[] = [
  { id: "timing", label: "Start / End / Elapsed" },
  { id: "captured", label: "Captured (source)", cdcOnly: true },
  { id: "applied", label: "Applied (destination)", cdcOnly: true },
  { id: "updated", label: "Updated" },
]

const PAGE_SIZES = [10, 25, 50, 100] as const
const DEFAULT_PAGE_SIZE = 25
const PREFS_KEY = "rsync.tableStats.prefs.v1"

type Prefs = { pageSize: number; hidden: ColumnGroup[] }
const DEFAULT_PREFS: Prefs = { pageSize: DEFAULT_PAGE_SIZE, hidden: [] }

function readPrefs(): Prefs {
  try {
    const raw = localStorage.getItem(PREFS_KEY)
    if (!raw) return DEFAULT_PREFS
    const parsed = JSON.parse(raw) as Partial<Prefs> | null
    const pageSize = PAGE_SIZES.includes(parsed?.pageSize as (typeof PAGE_SIZES)[number])
      ? (parsed!.pageSize as number)
      : DEFAULT_PAGE_SIZE
    const hidden = Array.isArray(parsed?.hidden)
      ? parsed!.hidden.filter((g): g is ColumnGroup => COLUMN_GROUPS.some((c) => c.id === g))
      : []
    return { pageSize, hidden }
  } catch {
    return DEFAULT_PREFS
  }
}

function writePrefs(prefs: Prefs) {
  try {
    localStorage.setItem(PREFS_KEY, JSON.stringify(prefs))
  } catch {}
}

/**
 * The page buttons to draw: the first, the last, and the current page with its
 * neighbours. A skipped run of one page is drawn as that page, not as "…".
 */
export function pageWindow(current: number, count: number): (number | "gap")[] {
  if (count <= 7) return Array.from({ length: count }, (_, i) => i + 1)
  const pages = Array.from(new Set([1, current - 1, current, current + 1, count]))
    .filter((p) => p >= 1 && p <= count)
    .sort((a, b) => a - b)
  const out: (number | "gap")[] = []
  pages.forEach((p, i) => {
    const prev = pages[i - 1]
    if (prev !== undefined && p - prev === 2) out.push(prev + 1)
    else if (prev !== undefined && p - prev > 2) out.push("gap")
    out.push(p)
  })
  return out
}

// ---------------------------------------------------------------------------
// Columns — one declarative list drives the header AND every row, so a row can
// never have a different cell count than the header.
//
// In a mixed pipeline the header carries batch columns AND CDC columns, and a
// row renders a dash under the other mode's columns. When each mode's cells
// were emitted by hand, a batch row left out the 10 CDC cells, and every value
// after the gap rendered under a heading that belonged to the other mode —
// "Dropped" appearing beneath "Captured I" (F-281). The numbers were real; the
// column was a lie. Driving both from this list makes that unrepresentable.
// ---------------------------------------------------------------------------

type Column = {
  id: string
  header: string
  /** Tooltip on the heading: what the number measures. */
  headerTitle?: string
  /** Hideable from the settings menu. Columns without a group are always shown. */
  group?: ColumnGroup
  /** Only rows of this mode have a value here; the others show a dash. */
  mode?: "batch" | "cdc"
  numeric?: boolean
  sort?: SortKey
  cellClassName?: string | ((t: TableStat) => string)
  cellTitle?: (t: TableStat) => string | undefined
  cell: (t: TableStat) => ReactNode
}

const NUMERIC_CELL = "text-right font-mono text-sm"
const TIME_CELL = "text-xs text-muted-foreground"

function buildColumns(
  isBatch: boolean,
  isCDC: boolean,
  hidden: ColumnGroup[],
  opts: { paused?: boolean; snapshot?: boolean } = {},
): Column[] {
  const cols: Column[] = [
    {
      id: "status",
      header: "Status",
      sort: "status",
      cell: (t) => (
        <div className="flex flex-col gap-1">
          <StatusBadge status={t.status} paused={opts.paused} />
          {isChangesOnly(t) && (
            <Badge variant="outline" className="w-fit text-muted-foreground" title={CHANGES_ONLY_HELP}>
              Changes only
            </Badge>
          )}
          {t.status === "degraded" && t.mode === "batch" && (
            <span className="text-xs text-amber-600 dark:text-amber-400">
              {(t.read_rows ?? 0) > 0 && (t.inserted_rows ?? 0) === 0
                ? "Destination wrote 0 rows"
                : `${formatNumber(t.inserted_rows)} of ${formatNumber(t.read_rows)} written`}
            </span>
          )}
        </div>
      ),
    },
    { id: "start", header: "Start", group: "timing", cellClassName: TIME_CELL, cell: (t) => formatTimestamp(t.started_at) },
    { id: "end", header: "End", group: "timing", cellClassName: TIME_CELL, cell: (t) => formatTimestamp(t.completed_at) },
    {
      id: "elapsed",
      header: "Elapsed",
      group: "timing",
      cellClassName: `${TIME_CELL} font-mono`,
      cell: (t) => formatElapsed(t.started_at, t.completed_at),
    },
  ]

  if (isBatch) {
    cols.push(
      { id: "read", header: "Read", mode: "batch", numeric: true, cell: (t) => formatNumber(t.read_rows) },
      {
        id: "written",
        header: "Written",
        mode: "batch",
        numeric: true,
        sort: "inserted_rows",
        cell: (t) => formatNumber(t.inserted_rows),
      },
    )
  }

  if (isCDC) {
    const cap = `${CAPTURED_HELP} ${IUD_HELP}`
    const app = `${APPLIED_HELP} ${IUD_HELP}`
    // Snapshot rows sit in front of I/U/D (#7): an insert count used to carry
    // every row the load READ, so a re-snapshot looked like a burst of inserts.
    // Only drawn when the backend reports them; an older one never does.
    const capSnapshot: Column[] = opts.snapshot
      ? [
          {
            id: "cap_snap",
            header: "Captured Snapshot",
            headerTitle: `${CAPTURED_HELP} ${SNAPSHOT_HELP}`,
            group: "captured",
            mode: "cdc",
            numeric: true,
            cell: (t) => formatCount(t, t.snapshot_rows),
          },
        ]
      : []
    const appSnapshot: Column[] = opts.snapshot
      ? [
          {
            id: "app_snap",
            header: "Applied Snapshot",
            headerTitle: `${APPLIED_HELP} ${SNAPSHOT_HELP}`,
            group: "applied",
            mode: "cdc",
            numeric: true,
            cell: (t) => formatCount(t, t.applied_snapshot_rows),
          },
        ]
      : []
    cols.push(
      ...capSnapshot,
      { id: "cap_i", header: "Captured I", headerTitle: cap, group: "captured", mode: "cdc", numeric: true, sort: "inserts", cell: (t) => formatCount(t, t.inserts) },
      { id: "cap_u", header: "Captured U", headerTitle: cap, group: "captured", mode: "cdc", numeric: true, cell: (t) => formatCount(t, t.updates) },
      { id: "cap_d", header: "Captured D", headerTitle: cap, group: "captured", mode: "cdc", numeric: true, cell: (t) => formatCount(t, t.deletes) },
      { id: "cap_total", header: "Captured Total", headerTitle: CAPTURED_HELP, group: "captured", mode: "cdc", numeric: true, cell: (t) => formatCount(t, t.total_events) },
      { id: "cap_last", header: "Last Captured", group: "captured", mode: "cdc", cellClassName: TIME_CELL, cell: (t) => formatTimestamp(t.last_event_ts) },
      ...appSnapshot,
      { id: "app_i", header: "Applied I", headerTitle: app, group: "applied", mode: "cdc", numeric: true, cell: (t) => formatCount(t, t.applied_inserts) },
      { id: "app_u", header: "Applied U", headerTitle: app, group: "applied", mode: "cdc", numeric: true, cell: (t) => formatCount(t, t.applied_updates) },
      { id: "app_d", header: "Applied D", headerTitle: app, group: "applied", mode: "cdc", numeric: true, cell: (t) => formatCount(t, t.applied_deletes) },
      { id: "app_total", header: "Applied Total", headerTitle: APPLIED_HELP, group: "applied", mode: "cdc", numeric: true, cell: (t) => formatCount(t, t.applied_total_events) },
      { id: "app_last", header: "Last Applied", group: "applied", mode: "cdc", cellClassName: TIME_CELL, cell: (t) => formatTimestamp(t.last_applied_ts) },
    )
  }

  cols.push(
    // Shown in both modes, and never hideable — a dropped row is not a CDC-only
    // concept, and it is the one counter that reports what did NOT land.
    {
      id: "dropped",
      header: "Dropped",
      numeric: true,
      cellClassName: (t) =>
        cn(
          "text-right font-mono text-sm",
          (t.dlq_rows ?? 0) > 0 ? "font-semibold text-red-600 dark:text-red-400" : "text-muted-foreground",
        ),
      cellTitle: (t) =>
        (t.dlq_rows ?? 0) > 0
          ? "Rows the destination rejected after every retry. Parked in the dead-letter queue; they will not arrive on their own."
          : isWaitingForData(t)
            ? "Nothing has been captured for this table yet"
            : "No rows dropped",
      cell: (t) => (isWaitingForData(t) && !(t.dlq_rows ?? 0) ? NO_DATA_YET : formatNumber(t.dlq_rows ?? 0)),
    },
    {
      id: "updated",
      header: "Updated",
      group: "updated",
      sort: "updated_at",
      cellClassName: TIME_CELL,
      cell: (t) => formatTimestamp(t.updated_at),
    },
  )

  return cols.filter((c) => !c.group || !hidden.includes(c.group))
}

// Sticky header and sticky name columns. Explicit zinc colours on purpose: the
// theme tokens (`bg-background`, `bg-muted`) are not mapped in this Tailwind
// setup, and a transparent sticky cell lets the scrolled columns show through.
const HEAD_CELL =
  "sticky top-0 z-20 h-9 px-3 text-xs bg-zinc-50 dark:bg-zinc-900 shadow-[inset_0_-1px_0_rgb(228_228_231)] dark:shadow-[inset_0_-1px_0_rgb(39_39_42)]"
const BODY_CELL = "px-3 py-2"
const STICKY_BODY = "sticky z-10 bg-white group-hover:bg-zinc-50 dark:bg-zinc-950 dark:group-hover:bg-zinc-900"
// The right edge of the frozen block, drawn as a shadow: a border on a sticky
// cell scrolls away under `border-collapse`.
const FROZEN_EDGE = "shadow-[inset_-1px_0_0_rgb(228_228_231)] dark:shadow-[inset_-1px_0_0_rgb(39_39_42)]"
const FROZEN_EDGE_HEAD =
  "shadow-[inset_0_-1px_0_rgb(228_228_231),inset_-1px_0_0_rgb(228_228_231)] dark:shadow-[inset_0_-1px_0_rgb(39_39_42),inset_-1px_0_0_rgb(39_39_42)]"
// Fixed width, so the Table column's `left` offset is exact.
const SCHEMA_WIDTH = "w-[120px] min-w-[120px] max-w-[120px]"

function SortIcon({ active, direction }: { active: boolean; direction: "ascending" | "descending" | "other" }) {
  if (!active) return <ArrowUpDown aria-hidden className="h-3 w-3 opacity-40" />
  if (direction === "ascending") return <ArrowUp aria-hidden className="h-3 w-3" />
  return <ArrowDown aria-hidden className="h-3 w-3" />
}

function SummaryTile({ label, value, className }: { label: string; value: ReactNode; className?: string }) {
  return (
    <div>
      <div className="text-xs text-muted-foreground">{label}</div>
      <div className={cn("text-lg font-semibold tabular-nums", className)}>{value}</div>
    </div>
  )
}

export function TableStatisticsPanel({ pipelineId, executionId, pipelineStatus, blockingReasonType, mode }: Props) {
  const [loading, setLoading] = useState(true)
  const [fetching, setFetching] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [summary, setSummary] = useState<TableStatsSummary | null>(null)
  const [tables, setTables] = useState<TableStat[]>([])
  const [total, setTotal] = useState(0)
  const [search, setSearch] = useState("")
  const [sortBy, setSortBy] = useState<SortKey>("qualified_name")
  const [offset, setOffset] = useState(0)
  // Read once, before the first fetch, so a stored page size of 100 does not
  // first fetch 25 rows and then refetch. Hydration-safe: the first render is
  // the loading card, which no pref affects (on the server `localStorage`
  // throws and `readPrefs` falls back to the defaults).
  const [prefs, setPrefs] = useState<Prefs>(readPrefs)
  const pageSize = prefs.pageSize
  // Clicking through pages faster than the API answers must not let an older
  // page's response overwrite a newer one.
  const requestSeq = useRef(0)
  // The latest run (executionId) moved no rows and an earlier run did: `emptyRun`
  // names that earlier run, and `shownRun` is set while the grid shows it instead.
  const [shownRun, setShownRun] = useState<string | null>(null)
  const [emptyRun, setEmptyRun] = useState<LastDataRun | null>(null)
  const [showLatest, setShowLatest] = useState(false)
  useEffect(() => {
    setShownRun(null)
    setEmptyRun(null)
    setShowLatest(false)
  }, [executionId])
  const viewedExecutionId = shownRun ?? executionId

  const fetchStats = useCallback(async () => {
    const seq = ++requestSeq.current
    setFetching(true)
    try {
      setError(null)
      const params = new URLSearchParams()
      if (viewedExecutionId) params.set("execution_id", viewedExecutionId)
      if (mode) params.set("mode", mode)
      if (search) params.set("q", search)
      params.set("sort", sortBy)
      params.set("limit", String(pageSize))
      params.set("offset", String(offset))

      const url = `${API_ENDPOINTS.PIPELINES.GET(pipelineId)}/table-stats?${params.toString()}`
      const res = await authFetch(url, { cache: "no-store" })
      if (!res.ok) {
        throw new Error(`Failed to load table stats (${res.status})`)
      }
      const data = await res.json()
      if (seq !== requestSeq.current) return
      // Batch: a run that found no new rows would fill the page with zeros while the
      // pipeline's data is all there. Show the last run that moved rows instead.
      if (!shownRun && executionId && !search) {
        const last = data.last_data_run as LastDataRun | undefined
        const redirect = batchRunMovedNothing(data.summary) && last?.execution_id && last.execution_id !== executionId
        setEmptyRun(redirect ? last : null)
        if (redirect && !showLatest) {
          setOffset(0)
          setShownRun(last.execution_id)
          return
        }
      }
      const nextTotal = Number(data.total || 0)
      // Tables can disappear between refreshes; never strand the viewer on an
      // empty page past the end. Changing the offset fetches the last page.
      if (nextTotal > 0 && offset >= nextTotal) {
        setOffset(Math.floor((nextTotal - 1) / pageSize) * pageSize)
        return
      }
      setSummary(data.summary || null)
      const rawTables: TableStat[] = Array.isArray(data.tables) ? data.tables : []
      // Deduplicate by (mode, qualified_name) to prevent React key collisions.
      const seen = new Set<string>()
      const uniq: TableStat[] = []
      for (const t of rawTables) {
        const key = `${String(t?.mode || "")}:${String(t?.qualified_name || "")}`
        if (!t?.qualified_name || seen.has(key)) continue
        seen.add(key)
        uniq.push(t)
      }
      setTables(uniq)
      setTotal(nextTotal)
    } catch (e: any) {
      if (seq !== requestSeq.current) return
      setError(String(e?.message || e || "Failed to load table stats"))
    } finally {
      if (seq === requestSeq.current) {
        setLoading(false)
        setFetching(false)
      }
    }
  }, [pipelineId, executionId, viewedExecutionId, shownRun, showLatest, mode, search, sortBy, offset, pageSize])

  useEffect(() => {
    fetchStats()
  }, [fetchStats])

  // Edit tables, Re-snapshot, Pause and Resume announce themselves on the
  // refresh bus. Without listening, a table just added or removed stayed off
  // (or on) this list until the page was reloaded (#15).
  useEffect(
    () =>
      onPipelineRefresh((pid) => {
        if (pid === pipelineId) void fetchStats()
      }),
    [pipelineId, fetchStats],
  )

  const exportCSV = useCallback(async () => {
    const params = new URLSearchParams()
    if (viewedExecutionId) params.set("execution_id", viewedExecutionId)
    if (mode) params.set("mode", mode)
    params.set("export", "csv")
    params.set("limit", "10000") // Export all

    const url = `${API_ENDPOINTS.PIPELINES.GET(pipelineId)}/table-stats?${params.toString()}`
    try {
      const res = await authFetch(url, {
        method: "GET",
        cache: "no-store",
        headers: {
          Accept: "text/csv",
        },
      })
      if (!res.ok) {
        throw new Error(`Export failed (${res.status})`)
      }
      const blob = await res.blob()
      const objectUrl = URL.createObjectURL(blob)
      const link = document.createElement("a")
      link.href = objectUrl
      link.download = `table-stats-${pipelineId}.csv`
      link.click()
      URL.revokeObjectURL(objectUrl)
    } catch (e) {
      // A download that produces no file and no message is read as "the button
      // is broken" — or worse, as "there was nothing to export".
      const err = classifyError(e, "pipeline.export")
      toast.error("Could not export table statistics", { description: err.hint ?? err.message })
    }
  }, [pipelineId, viewedExecutionId, mode])

  const updatePrefs = (next: Prefs) => {
    setPrefs(next)
    writePrefs(next)
  }
  const changePageSize = (size: number) => {
    updatePrefs({ ...prefs, pageSize: size })
    setOffset(0)
  }
  const toggleGroup = (group: ColumnGroup, visible: boolean) => {
    const hidden = visible ? prefs.hidden.filter((g) => g !== group) : [...prefs.hidden, group]
    updatePrefs({ ...prefs, hidden })
  }
  const changeSort = (key: SortKey) => {
    setSortBy(key)
    setOffset(0)
  }

  const resolvedMode = summary?.mode || mode || "batch"
  const isBatch = resolvedMode === "batch" || resolvedMode === "mixed"
  const isCDC = resolvedMode === "cdc" || resolvedMode === "mixed"
  // Every table is still waiting for its first event: the CDC totals are not a
  // measured 0, so they say so.
  const allTablesWaitingForData =
    !!summary && summary.total_tables > 0 && (summary.tables_waiting_for_data ?? 0) === summary.total_tables
  const formatSummaryTotal = (num?: number | null) =>
    allTablesWaitingForData ? NO_DATA_YET : formatNumber(num)
  // Paused (CDC "paused", or a batch run paused by the user): nothing moves, so
  // nothing may read Running (#14).
  const pipelinePaused =
    normalizePipelineStatus(pipelineStatus) === "paused" ||
    (pipelineStatus === "waiting_for_user" && blockingReasonType === "paused_by_user")
  // Snapshot columns only when this backend reports snapshot counts at all.
  const hasSnapshotCounts =
    summary?.total_snapshot_rows !== undefined ||
    summary?.total_applied_snapshot_rows !== undefined ||
    tables.some((t) => t.snapshot_rows !== undefined || t.applied_snapshot_rows !== undefined)

  if (loading) {
    return (
      <Card>
        <CardContent className="p-6">
          <div className="flex items-center gap-2 text-sm text-muted-foreground">
            <Loader2 className="h-4 w-4 animate-spin" />
            Loading table statistics…
          </div>
        </CardContent>
      </Card>
    )
  }

  if (error) {
    return (
      <Card>
        <CardContent className="p-6">
          <div className="text-sm text-red-600">{error}</div>
        </CardContent>
      </Card>
    )
  }

  // Only when there is genuinely nothing: a search that matches no table keeps
  // the list (and its search box) on screen, or the viewer could not clear it.
  if (total === 0 && !search) {
    const waitingForTables =
      pipelineStatus === "waiting_for_user" && blockingReasonType === "table_selection"
    return (
      <Card>
        <CardContent className="p-6">
          <div className="text-sm text-muted-foreground">
            {waitingForTables
              ? "This run is waiting for table selection. Select one or more tables to start execution — table statistics will appear as tables are processed."
              : resolvedMode === "cdc"
                ? "No CDC activity captured yet. Make an INSERT/UPDATE/DELETE on a selected source table — stats will appear once events are captured/applied."
                : "No table statistics available for this run yet. Statistics will appear as tables are processed (or after you re-run older executions)."}
          </div>
        </CardContent>
      </Card>
    )
  }

  const columns = buildColumns(isBatch, isCDC, prefs.hidden, { paused: pipelinePaused, snapshot: hasSnapshotCounts })
  const identities = tables.map(tableIdentity)
  const showSchema = identities.some((id) => !!id.schema)
  const columnCount = columns.length + (showSchema ? 2 : 1)
  const pageCount = Math.max(1, Math.ceil(total / pageSize))
  const page = Math.min(pageCount, Math.floor(offset / pageSize) + 1)
  const goToPage = (p: number) => setOffset((Math.min(Math.max(p, 1), pageCount) - 1) * pageSize)
  const availableGroups = COLUMN_GROUPS.filter((g) => !g.cdcOnly || isCDC)

  const runBanner = emptyRun ? (
    <div
      data-testid="table-stats-run-banner"
      className="flex flex-wrap items-start justify-between gap-2 rounded-lg border border-sky-300 bg-sky-50 px-3 py-2.5 text-sm dark:border-sky-800 dark:bg-sky-950/30"
    >
      <div className="text-sky-900 dark:text-sky-200">
        {shownRun ? (
          <>
            <span className="font-medium">
              The latest run (<span className="font-mono">{shortId(executionId)}</span>) found no new rows to copy.
            </span>{" "}
            Showing the last run that moved data: <span className="font-mono">{shortId(emptyRun.execution_id)}</span>
          </>
        ) : (
          <>
            <span className="font-medium">This run found no new rows to copy.</span> The last run that moved data was{" "}
            <span className="font-mono">{shortId(emptyRun.execution_id)}</span>
          </>
        )}
        {emptyRun.finished_at ? <>, finished {formatTimestamp(emptyRun.finished_at)}</> : null}
        {" — "}
        {formatNumber(emptyRun.rows_read ?? 0)} rows read, {formatNumber(emptyRun.rows_written ?? 0)} written
        {typeof emptyRun.tables === "number" ? (
          <>
            {" "}
            across {emptyRun.tables} table{emptyRun.tables !== 1 ? "s" : ""}
          </>
        ) : null}
        .
      </div>
      <Button
        variant="outline"
        size="sm"
        className="h-7 shrink-0"
        onClick={() => {
          setOffset(0)
          if (shownRun) {
            setShowLatest(true)
            setShownRun(null)
          } else {
            setShowLatest(false)
            setShownRun(emptyRun.execution_id)
          }
        }}
      >
        {shownRun ? "Show latest run" : "Show that run"}
      </Button>
    </div>
  ) : null

  return (
    <div className="space-y-3">
      {runBanner}
      {/* Summary — compact, so the per-table grid below starts near the top. */}
      {summary && (
        <Card>
          <CardHeader className="p-4 pb-3">
            <CardTitle className="flex flex-wrap items-center gap-x-2 text-sm font-semibold">
              <Database className="h-4 w-4" />
              Summary
              <span className="font-normal text-muted-foreground">
                {resolvedMode === "batch" && "· Batch pipeline table migration status"}
                {resolvedMode === "cdc" && "· CDC pipeline real-time activity by table"}
                {resolvedMode === "mixed" && "· Mixed batch and CDC activity"}
              </span>
            </CardTitle>
          </CardHeader>
          <CardContent className="p-4 pt-0">
            {/* Failed tables alert banner */}
            {summary.tables_failed > 0 && (
              <div className="mb-3 flex items-start gap-2 rounded-lg border border-red-300 bg-red-50 px-3 py-2.5 text-sm dark:border-red-700 dark:bg-red-950/30">
                <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0 text-red-600 dark:text-red-400" />
                <div>
                  <span className="font-medium text-red-800 dark:text-red-300">
                    {summary.tables_failed} table{summary.tables_failed !== 1 ? "s" : ""} failed to sync
                  </span>
                  <span className="ml-1 text-red-700 dark:text-red-400">
                    — sort by Status to bring them to the top, and review connector logs.
                  </span>
                </div>
              </div>
            )}

            {/* Dropped-rows banner. Deliberately ABOVE (and visually louder than) the
                degraded banner: a degraded table means rows are behind, this means rows
                are GONE. Nothing else on this page reports it — every other counter is a
                count of what landed, so a dropped row leaves captured/applied reconciling
                perfectly with Failed at 0. */}
            {Boolean(summary.total_dlq_rows) && (
              <div className="mb-3 flex items-start gap-2 rounded-lg border border-red-300 bg-red-50 px-3 py-2.5 text-sm dark:border-red-700 dark:bg-red-950/30">
                <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0 text-red-600 dark:text-red-400" />
                <div>
                  <span className="font-medium text-red-800 dark:text-red-300">
                    {formatNumber(summary.total_dlq_rows)} row
                    {summary.total_dlq_rows !== 1 ? "s" : ""} dropped across{" "}
                    {summary.tables_with_dlq ?? 0} table
                    {(summary.tables_with_dlq ?? 0) !== 1 ? "s" : ""}
                  </span>
                  <span className="ml-1 text-red-700 dark:text-red-400">
                    — the destination rejected these rows after every retry, so they were
                    parked in the dead-letter queue and the stream moved on. They are not
                    counted anywhere else on this page and will not arrive on their own.
                    Inspect the <code>.dlq</code> topic for the rejected records and the
                    reason.
                  </span>
                </div>
              </div>
            )}

            {/* Degraded warning banner */}
            {Boolean(summary.tables_degraded) && (
              <div className="mb-3 flex items-start gap-2 rounded-lg border border-amber-300 bg-amber-50 px-3 py-2.5 text-sm dark:border-amber-700 dark:bg-amber-950/30">
                <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0 text-amber-600 dark:text-amber-400" />
                <div>
                  <span className="font-medium text-amber-800 dark:text-amber-300">
                    {summary.tables_degraded} table{summary.tables_degraded !== 1 ? "s" : ""} degraded
                  </span>
                  <span className="ml-1 text-amber-700 dark:text-amber-400">
                    — rows were read from source but not written to destination. Check destination connector logs or credentials.
                  </span>
                </div>
              </div>
            )}

            <div className="grid grid-cols-2 gap-x-4 gap-y-3 sm:grid-cols-4 lg:grid-cols-6">
              <SummaryTile label="Total Tables" value={summary.total_tables} />
              <SummaryTile label="Completed" value={summary.tables_completed} className="text-green-600" />
              {pipelinePaused ? (
                <SummaryTile label="Paused" value={summary.tables_running} className="text-muted-foreground" />
              ) : (
                <SummaryTile label="Running" value={summary.tables_running} className="text-blue-600" />
              )}
              <SummaryTile label="Degraded" value={summary.tables_degraded ?? 0} className="text-amber-600" />
              <SummaryTile label="Failed" value={summary.tables_failed} className="text-red-600" />
              {(summary.tables_waiting_for_data ?? 0) > 0 && (
                <SummaryTile
                  label={NO_DATA_YET}
                  value={summary.tables_waiting_for_data}
                  className="text-muted-foreground"
                />
              )}
              {/* Not in Total Tables, but still in the totals: their rows are still there. */}
              {(summary.tables_removed ?? 0) > 0 && (
                <SummaryTile label="Removed" value={summary.tables_removed} className="text-muted-foreground" />
              )}

              {isBatch && (summary.total_read_rows !== undefined || summary.total_inserted_rows !== undefined) && (
                <>
                  <SummaryTile label="Total Rows Read" value={formatNumber(summary.total_read_rows)} />
                  <SummaryTile label="Total Rows Written" value={formatNumber(summary.total_inserted_rows)} />
                </>
              )}

              {isCDC && (
                <>
                  {summary.total_snapshot_rows !== undefined && (
                    <SummaryTile label="Captured Snapshot Rows" value={formatSummaryTotal(summary.total_snapshot_rows)} />
                  )}
                  {summary.total_applied_snapshot_rows !== undefined && (
                    <SummaryTile
                      label="Applied Snapshot Rows"
                      value={formatSummaryTotal(summary.total_applied_snapshot_rows)}
                    />
                  )}
                  {summary.total_inserts !== undefined && (
                    <SummaryTile label="Captured Inserts" value={formatSummaryTotal(summary.total_inserts)} />
                  )}
                  {summary.total_updates !== undefined && (
                    <SummaryTile label="Captured Updates" value={formatSummaryTotal(summary.total_updates)} />
                  )}
                  {summary.total_deletes !== undefined && (
                    <SummaryTile label="Captured Deletes" value={formatSummaryTotal(summary.total_deletes)} />
                  )}
                  {summary.total_applied_inserts !== undefined && (
                    <SummaryTile label="Applied Inserts" value={formatSummaryTotal(summary.total_applied_inserts)} />
                  )}
                  {summary.total_applied_updates !== undefined && (
                    <SummaryTile label="Applied Updates" value={formatSummaryTotal(summary.total_applied_updates)} />
                  )}
                  {summary.total_applied_deletes !== undefined && (
                    <SummaryTile label="Applied Deletes" value={formatSummaryTotal(summary.total_applied_deletes)} />
                  )}
                </>
              )}
            </div>
            {isCDC && (
              <p data-testid="cdc-counter-legend" className="mt-3 text-xs text-muted-foreground">
                {CDC_COUNTER_LEGEND}
              </p>
            )}
          </CardContent>
        </Card>
      )}

      {/* Table List */}
      <Card>
        <CardHeader className="space-y-3 p-4">
          <div className="flex flex-wrap items-center justify-between gap-2">
            <CardTitle className="text-base">Table Statistics ({total})</CardTitle>
            <div className="flex items-center gap-2">
              <Button variant="outline" size="sm" onClick={() => fetchStats()} disabled={fetching}>
                <RefreshCw className={cn("mr-2 h-4 w-4", fetching && "animate-spin")} />
                Refresh
              </Button>
              <Button variant="outline" size="sm" onClick={exportCSV}>
                <Download className="h-4 w-4 mr-2" />
                Export CSV
              </Button>
              <DropdownMenu>
                <DropdownMenuTrigger asChild>
                  <Button variant="outline" size="icon-sm" aria-label="Table settings" title="Rows per page and columns">
                    <Settings2 className="h-4 w-4" />
                  </Button>
                </DropdownMenuTrigger>
                <DropdownMenuContent align="end" className="w-56">
                  <DropdownMenuLabel>Rows per page</DropdownMenuLabel>
                  <DropdownMenuRadioGroup value={String(pageSize)} onValueChange={(v) => changePageSize(Number(v))}>
                    {PAGE_SIZES.map((n) => (
                      <DropdownMenuRadioItem key={n} value={String(n)}>
                        {n}
                      </DropdownMenuRadioItem>
                    ))}
                  </DropdownMenuRadioGroup>
                  <DropdownMenuSeparator />
                  <DropdownMenuLabel>Columns</DropdownMenuLabel>
                  {availableGroups.map((g) => (
                    <DropdownMenuCheckboxItem
                      key={g.id}
                      checked={!prefs.hidden.includes(g.id)}
                      onCheckedChange={(on) => toggleGroup(g.id, on === true)}
                      // Keep the menu open so several groups can be toggled in one go.
                      onSelect={(e) => e.preventDefault()}
                    >
                      {g.label}
                    </DropdownMenuCheckboxItem>
                  ))}
                </DropdownMenuContent>
              </DropdownMenu>
            </div>
          </div>
          <div className="flex flex-wrap items-center gap-x-3 gap-y-2">
            <div className="relative min-w-[200px] flex-1">
              <Search className="absolute left-2 top-2.5 h-4 w-4 text-muted-foreground" />
              <Input
                placeholder="Search tables…"
                aria-label="Search tables"
                value={search}
                onChange={(e) => {
                  setSearch(e.target.value)
                  setOffset(0)
                }}
                className="pl-8"
              />
            </div>
            {total > 0 && (
              <div className="text-sm text-muted-foreground tabular-nums">
                {offset + 1}–{Math.min(offset + pageSize, total)} of {total}
              </div>
            )}
            {pageCount > 1 && (
              <nav aria-label="Table statistics pages" className="flex items-center gap-1">
                <Button
                  variant="outline"
                  size="icon-sm"
                  aria-label="Previous page"
                  onClick={() => goToPage(page - 1)}
                  disabled={page <= 1}
                >
                  <ChevronLeft className="h-4 w-4" />
                </Button>
                {pageWindow(page, pageCount).map((p, i) =>
                  p === "gap" ? (
                    <span key={`gap-${i}`} aria-hidden className="px-1 text-sm text-muted-foreground">
                      …
                    </span>
                  ) : (
                    <Button
                      key={p}
                      variant={p === page ? "default" : "ghost"}
                      size="sm"
                      className="min-w-8 px-2 tabular-nums"
                      aria-label={`Page ${p}`}
                      aria-current={p === page ? "page" : undefined}
                      onClick={() => goToPage(p)}
                    >
                      {p}
                    </Button>
                  ),
                )}
                <Button
                  variant="outline"
                  size="icon-sm"
                  aria-label="Next page"
                  onClick={() => goToPage(page + 1)}
                  disabled={page >= pageCount}
                >
                  <ChevronRight className="h-4 w-4" />
                </Button>
              </nav>
            )}
          </div>
        </CardHeader>
        <CardContent className="p-4 pt-0">
          {/*
            One scroll box for both axes: the header row and the name columns stay
            put while the counters scroll under them, and the horizontal scrollbar
            is always at the bottom of the visible box rather than below every row.
          */}
          <div className="max-h-[70vh] overflow-auto rounded-md border" aria-busy={fetching}>
            <table className="min-w-full w-max caption-bottom text-sm [&_th]:whitespace-nowrap [&_td]:whitespace-nowrap">
              <TableHeader>
                <TableRow className="hover:bg-transparent">
                  {showSchema && (
                    <TableHead className={cn(HEAD_CELL, SCHEMA_WIDTH, "left-0 z-30")}>Schema</TableHead>
                  )}
                  <TableHead
                    className={cn(HEAD_CELL, FROZEN_EDGE_HEAD, "z-30 min-w-[180px]", showSchema ? "left-[120px]" : "left-0")}
                    aria-sort={sortBy === "qualified_name" ? SORT_DIRECTION.qualified_name : "none"}
                  >
                    <SortButton label="Table" sortKey="qualified_name" active={sortBy === "qualified_name"} onSort={changeSort} />
                  </TableHead>
                  {columns.map((c) => (
                    <TableHead
                      key={c.id}
                      className={cn(HEAD_CELL, c.numeric && "text-right")}
                      title={c.headerTitle}
                      aria-sort={c.sort ? (sortBy === c.sort ? SORT_DIRECTION[c.sort] : "none") : undefined}
                    >
                      {c.sort ? (
                        <SortButton
                          label={c.header}
                          sortKey={c.sort}
                          active={sortBy === c.sort}
                          onSort={changeSort}
                          alignEnd={c.numeric}
                        />
                      ) : (
                        c.header
                      )}
                    </TableHead>
                  ))}
                </TableRow>
              </TableHeader>
              <TableBody className={cn("transition-opacity", fetching && "opacity-60")}>
                {tables.length === 0 ? (
                  <TableRow className="hover:bg-transparent">
                    <TableCell colSpan={columnCount} className="py-8 text-center text-sm text-muted-foreground">
                      No tables match “{search}”.
                    </TableCell>
                  </TableRow>
                ) : (
                  tables.map((table, i) => {
                    const { schema, name } = identities[i]
                    return (
                      <TableRow key={`${table.mode}:${table.qualified_name}`} className="group hover:bg-zinc-50 dark:hover:bg-zinc-900">
                        {showSchema && (
                          <TableCell
                            className={cn(BODY_CELL, STICKY_BODY, SCHEMA_WIDTH, "left-0 font-mono text-xs text-muted-foreground")}
                            title={schema}
                          >
                            <div className="truncate">{schema}</div>
                          </TableCell>
                        )}
                        <TableCell
                          className={cn(BODY_CELL, STICKY_BODY, FROZEN_EDGE, "min-w-[180px] font-mono text-sm", showSchema ? "left-[120px]" : "left-0")}
                          title={table.qualified_name}
                        >
                          <div className="max-w-[280px] truncate">{name}</div>
                        </TableCell>
                        {columns.map((c) =>
                          c.mode && c.mode !== table.mode ? (
                            <TableCell key={c.id} className={cn(BODY_CELL, "text-right text-sm text-muted-foreground")}>
                              {NO_VALUE}
                            </TableCell>
                          ) : (
                            <TableCell
                              key={c.id}
                              className={cn(
                                BODY_CELL,
                                typeof c.cellClassName === "function"
                                  ? c.cellClassName(table)
                                  : c.cellClassName ?? (c.numeric ? NUMERIC_CELL : undefined),
                              )}
                              title={c.cellTitle?.(table)}
                            >
                              {c.cell(table)}
                            </TableCell>
                          ),
                        )}
                      </TableRow>
                    )
                  })
                )}
              </TableBody>
            </table>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}

function SortButton({
  label,
  sortKey,
  active,
  onSort,
  alignEnd,
}: {
  label: string
  sortKey: SortKey
  active: boolean
  onSort: (key: SortKey) => void
  alignEnd?: boolean
}) {
  return (
    <button
      type="button"
      onClick={() => onSort(sortKey)}
      title={SORT_TITLE[sortKey]}
      className={cn(
        "inline-flex items-center gap-1 rounded-sm hover:text-zinc-900 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-zinc-400 dark:hover:text-zinc-50",
        active && "text-zinc-900 dark:text-zinc-50",
        alignEnd && "flex-row-reverse",
      )}
    >
      {label}
      <SortIcon active={active} direction={SORT_DIRECTION[sortKey]} />
    </button>
  )
}
