"use client"

import { useEffect, useMemo, useRef, useState } from "react"
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter } from "@/components/ui/dialog"
import { Button } from "@/components/ui/button"
import { Checkbox } from "@/components/ui/checkbox"
import { Label } from "@/components/ui/label"
import { Input } from "@/components/ui/input"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Badge } from "@/components/ui/badge"
import { Loader2, Table as TableIcon, Database, CheckCircle2, Eye, ChevronDown, ChevronUp, Sparkles } from "lucide-react"

import { resumePipelineTables, type DestinationConfig } from "@/lib/api/pipelines"
import { authFetch } from "@/lib/api/auth-fetch"
import { API_ENDPOINTS } from "@/lib/config/api"
import { kindMeta, validateNamespace, namespaceKindForType, defaultNamespaceForTypes } from "@/lib/pipeline/destinationNamespace"
import { isInternalExplorerTable } from "@/lib/explorer/internalTables"

export type AvailableTable = {
  name: string
  schema?: string
  row_count?: number
  is_exact_count?: boolean
  columns?: number
}

// AI table suggestion surfaced by the backend in
// blocking_reason.details.suggested_tables (computed once at park time from
// the user's pipeline intent). Advisory only — the user's own selection
// always wins.
export type SuggestedTable = {
  name: string
  schema?: string
  reason?: string
  confidence?: number
}

// normalizeSuggestedTables narrows the untyped details.suggested_tables
// payload into SuggestedTable[]. Shared with useHITLState.
export function normalizeSuggestedTables(input: unknown): SuggestedTable[] {
  if (!Array.isArray(input)) return []
  const out: SuggestedTable[] = []
  for (const item of input) {
    if (!item || typeof item !== "object" || Array.isArray(item)) continue
    const rec = item as Record<string, unknown>
    const name = String(rec["name"] || "").trim()
    if (!name) continue
    out.push({
      name,
      schema: rec["schema"] ? String(rec["schema"]).trim() : undefined,
      reason: typeof rec["reason"] === "string" ? rec["reason"] : undefined,
      confidence: typeof rec["confidence"] === "number" ? rec["confidence"] : undefined,
    })
  }
  return out.slice(0, 10)
}

// Minimum ranker confidence for a table to be AI pre-checked. Mirrors the Data
// Explorer's HITLTablePicker gate (>= 0.85).
export const AUTO_SELECT_MIN_CONFIDENCE = 0.85

// computeAutoPreselectKeys decides which AI-suggested table keys to pre-check on
// park. Mirrors HITLTablePicker's gate, generalized to multi-select: a key
// qualifies only when the ranker is confident about it (>= threshold). The
// ranker is prompted "top N of N", so with a handful of tables it often returns
// them ALL at similar confidence — pre-checking that whole set silently syncs
// unintended tables (the reported bug). So if the confident set is not a strict
// narrowing of what's available (it covers every table, or nothing clears the
// bar), return [] and let the user choose. `suggestionMatch` maps tableKey →
// suggestion; `totalAvailable` is the count of real (non-internal) tables shown.
export function computeAutoPreselectKeys(
  suggestionMatch: Map<string, SuggestedTable>,
  totalAvailable: number,
): string[] {
  const confidentKeys = Array.from(suggestionMatch.entries())
    .filter(([, s]) => (s.confidence ?? 0) >= AUTO_SELECT_MIN_CONFIDENCE)
    .map(([key]) => key)
  const isNarrowing = confidentKeys.length > 0 && confidentKeys.length < totalAvailable
  return isNarrowing ? confidentKeys : []
}

function TablePreviewPanel({ connectionId, tableName }: { connectionId: string; tableName: string }) {
  const [rows, setRows] = useState<Record<string, unknown>[]>([])
  const [cols, setCols] = useState<string[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    setLoading(true)
    setError(null)
    authFetch(`${API_ENDPOINTS.CONNECTIONS.GET(connectionId)}/sample?table=${encodeURIComponent(tableName)}&limit=5`, { cache: "no-store" })
      .then((r) => {
        if (r.status === 429) throw new Error("Rate limited — try again in a moment")
        if (!r.ok) throw new Error(`HTTP ${r.status}`)
        return r.json()
      })
      .then((data) => {
        const fetchedRows: Record<string, unknown>[] = data.rows || []
        const fetchedCols = fetchedRows.length > 0 ? Object.keys(fetchedRows[0]) : (data.columns || [])
        setRows(fetchedRows)
        setCols(fetchedCols)
      })
      .catch((e) => setError(e?.message || "Failed to load preview"))
      .finally(() => setLoading(false))
  }, [connectionId, tableName])

  if (loading) return (
    <div className="flex items-center gap-2 p-3 text-xs text-zinc-500">
      <Loader2 className="h-3 w-3 animate-spin" /> Loading preview…
    </div>
  )
  if (error) return <div className="p-3 text-xs text-red-500">{error}</div>
  if (rows.length === 0) return <div className="p-3 text-xs text-zinc-500">No sample rows available</div>

  return (
    <div className="overflow-auto max-h-40 rounded border border-zinc-100 dark:border-zinc-800">
      <table className="min-w-full text-xs">
        <thead className="sticky top-0 bg-zinc-50 dark:bg-zinc-900">
          <tr>
            {cols.map((c) => (
              <th key={c} className="px-2 py-1.5 text-left font-medium text-zinc-500 border-b border-zinc-100 dark:border-zinc-800 whitespace-nowrap">{c}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => (
            <tr key={i} className="border-b border-zinc-50 dark:border-zinc-900 last:border-0">
              {cols.map((c) => (
                <td key={c} className="px-2 py-1 font-mono text-zinc-700 dark:text-zinc-300 whitespace-nowrap max-w-[160px] truncate">
                  {row[c] == null ? <span className="text-zinc-400 italic">null</span> : String(row[c])}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

export function PipelineTableSelector(props: {
  isOpen: boolean
  onClose: () => void
  onResolved?: () => void
  onTablesSelected?: (tables: string[], destinationConfig?: DestinationConfig) => Promise<void> | void
  pipelineId: string
  executionId?: string
  sourceType?: string
  sourceConnectionId?: string
  availableTables: AvailableTable[]
  initialSelectedTables?: string[]
  // AI suggestions to pre-select (advisory). ONLY when the prop is undefined
  // does the selector fetch them itself from /state — the backend computes
  // them asynchronously right after the table-selection park, so they can
  // arrive a poll or two after the modal opens. Passing an array (even an
  // empty one) means the caller owns delivery (it polls /state itself) and
  // suppresses the redundant self-fetch.
  suggestedTables?: SuggestedTable[]
  title?: string
  loading?: boolean
  // CDC-only: DMS-like behavior to backfill newly added tables.
  showCdcBackfillToggle?: boolean
  cdcBackfillNewTables?: boolean
  onCdcBackfillNewTablesChange?: (enabled: boolean) => void
  // Discovery hint from the backend so we can distinguish "we couldn't list
  // tables" (transport/auth failure) from "the database is empty" (success
  // with 0 rows). Without this we show the wrong message and the user thinks
  // something is broken when it's just an empty schema.
  discoveryStatus?: "ok" | "empty" | "failed"
  sourceDatabase?: string
  discoveryReason?: string
  // Destination mapping (PR-C): when provided, render a namespace field + create
  // toggle inside this first-run HITL and send the confirmed mapping alongside the
  // table selection. Pipeline-scoped; pre-filled from the pipeline's destination_config.
  destinationConfig?: DestinationConfig | null
  destinationType?: string
  // When true, poll the pipeline's /state while this dialog is open and auto-close
  // it once the table_selection gate is resolved out-of-band (a direct API call,
  // or another browser tab answering the same gate). Without this the dialog has
  // no way to know the executor already moved on and lingers as a stale modal.
  autoDismissOnResolve?: boolean
}) {
  const {
    isOpen,
    onClose,
    onResolved,
    onTablesSelected,
    pipelineId,
    executionId,
    sourceType,
    sourceConnectionId,
    availableTables,
    initialSelectedTables,
    suggestedTables,
    title = "Select Tables to Sync",
    loading = false,
    showCdcBackfillToggle = false,
    cdcBackfillNewTables = true,
    onCdcBackfillNewTablesChange,
    discoveryStatus,
    sourceDatabase,
    discoveryReason,
    destinationConfig,
    destinationType,
    autoDismissOnResolve = false,
  } = props

  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [expandedPreview, setExpandedPreview] = useState<string | null>(null)
  const [selected, setSelected] = useState<Record<string, boolean>>({})
  const [manualTables, setManualTables] = useState("")
  const [query, setQuery] = useState("")
  const [schemaFilter, setSchemaFilter] = useState<string>("__all__")
  // Whole-source ("*") and whole-namespace ("<ns>.*") selection sentinels. These
  // submit an INTENT the server resolves to the full, namespace-qualified table
  // list — bypassing the 100-table discovery preview cap — rather than
  // client-side ticking every row (which the cap makes impossible anyway).
  const [wholeDatabase, setWholeDatabase] = useState(false)
  const [namespaceWildcards, setNamespaceWildcards] = useState<Record<string, boolean>>({})
  const [visibleCount, setVisibleCount] = useState(50)
  const [discoveredTables, setDiscoveredTables] = useState<AvailableTable[]>([])
  const [discovering, setDiscovering] = useState(false)
  const [discoveryError, setDiscoveryError] = useState<string | null>(null)
  const [discoveryRanEmpty, setDiscoveryRanEmpty] = useState(false)
  // AI suggestions fetched from /state when the caller didn't pass any.
  const [fetchedSuggestions, setFetchedSuggestions] = useState<SuggestedTable[]>([])
  // How many tables we auto-checked from AI suggestions (drives the hint line).
  const [aiPreselectedCount, setAiPreselectedCount] = useState(0)

  // Destination mapping (PR-C): only shown when the caller opts in by passing the
  // `destinationConfig` prop (undefined = hidden). Pre-filled from the pipeline's
  // seeded default; the user can edit before first provisioning.
  const showMapping = destinationConfig !== undefined
  // For object-storage / file destinations the namespace is ALWAYS a path/prefix
  // — the connector type is authoritative. Early S3 pipelines were seeded with a
  // stale namespace_kind="schema" (before "aws-s3" was mapped), which would
  // otherwise mislabel the field "Schema name" and wrongly require a value. So a
  // path/prefix type-kind wins over the persisted kind; for relational engines the
  // persisted kind (which the user may have customized) still takes precedence.
  const destTypeKind = namespaceKindForType(destinationType)
  const destKind =
    destTypeKind === "path" || destTypeKind === "prefix"
      ? destTypeKind
      : (destinationConfig?.namespace_kind || "").trim() || destTypeKind
  const destMeta = kindMeta(destKind)
  // Prefer the stored config value; fall back to the destination-aware default
  // (e.g. "public" for MySQL→PostgreSQL, "" for BigQuery so the user is prompted).
  const [destNamespace, setDestNamespace] = useState(
    destinationConfig?.namespace || defaultNamespaceForTypes(sourceType, destinationType)
  )
  // destNamespaceError is computed further down, once the table selection state
  // (wholeDatabase / namespace wildcards / checked tables) is known — whether a
  // destination namespace is required depends on how many source schemas the
  // selection spans.

  // Tracks whether the user has typed in the namespace field. Until they do, the
  // field holds a seeded default we're free to adjust (e.g. blank it for a
  // multi-schema selection); once touched, we never override their choice.
  const namespaceTouchedRef = useRef(false)
  // The auto-seeded default namespace: the pipeline's stored value, else the
  // destination-aware fallback. NOTE this is NOT necessarily an engine default —
  // SQL Server / Snowflake / Oracle / Databricks sources seed the source slug
  // ("sqlserver"/…). It is a SEED, never a deliberate choice, until touched.
  const seededNamespace = destinationConfig?.namespace || defaultNamespaceForTypes(sourceType, destinationType)

  // Seed the mapping field on open; re-apply a late-arriving destinationConfig
  // (both callers fetch it async AFTER the modal opens) — but NEVER clobber a
  // value the user already typed (namespaceTouchedRef).
  const mappingWasOpenRef = useRef(false)
  useEffect(() => {
    if (!isOpen || !showMapping) {
      mappingWasOpenRef.current = false
      return
    }
    const firstOpen = !mappingWasOpenRef.current
    mappingWasOpenRef.current = true
    if (firstOpen) {
      namespaceTouchedRef.current = false
      setDestNamespace(seededNamespace)
    } else if (!namespaceTouchedRef.current) {
      setDestNamespace(seededNamespace)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isOpen, showMapping, seededNamespace])

  // ── Auto-dismiss when the table_selection gate is resolved out-of-band ──────
  // The same gate can be answered elsewhere (a direct API call, or another tab).
  // When that happens the executor moves on, but this dialog has no way to know
  // and lingers as a stale, confusing modal. When the caller opts in
  // (autoDismissOnResolve), poll the pipeline's /state while we're open and close
  // ourselves once the gate is no longer pending.
  //
  // Safety: we only ARM dismissal after we've actually observed the gate pending
  // while this dialog was open. That makes the poll a no-op in "edit tables" mode
  // (pipeline running/idle, never waiting_for_user) — so editing tables on a live
  // pipeline can never be auto-closed out from under the user.
  const onCloseRef = useRef(onClose)
  const onResolvedRef = useRef(onResolved)
  useEffect(() => {
    onCloseRef.current = onClose
    onResolvedRef.current = onResolved
  })
  const sawGatePendingRef = useRef(false)
  useEffect(() => {
    if (!isOpen) {
      sawGatePendingRef.current = false // reset for the next open
      return
    }
    if (!autoDismissOnResolve || !pipelineId) return

    let cancelled = false
    let timer: ReturnType<typeof setTimeout> | null = null

    const poll = async () => {
      try {
        const res = await authFetch(`${API_ENDPOINTS.PIPELINES.GET(pipelineId)}/state`, { cache: "no-store" })
        if (res.ok) {
          const data = (await res.json()) as {
            status?: string
            blocking_reason?: { type?: string } | null
            blocking_reason_type?: string
          }
          const status = String(data?.status || "")
          const brType = String(data?.blocking_reason?.type || data?.blocking_reason_type || "").toLowerCase()
          const gatePending = status === "waiting_for_user" && brType === "table_selection"
          if (gatePending) {
            sawGatePendingRef.current = true
          } else if (sawGatePendingRef.current && !cancelled) {
            // Gate was pending while we were open and is now resolved elsewhere.
            onResolvedRef.current?.()
            onCloseRef.current()
            return // stop polling — we're closing
          }
        }
      } catch {
        // Network blip: never close on an error (that would dismiss a still-valid
        // dialog). Keep polling.
      }
      if (!cancelled) timer = setTimeout(poll, 3000)
    }

    // Small initial delay so we don't race the open / the parent's first fetch.
    timer = setTimeout(poll, 3000)
    return () => {
      cancelled = true
      if (timer) clearTimeout(timer)
    }
    // onClose/onResolved are read via refs so the parents' own ~2.5s state polls
    // (which re-render this component) don't reset the timer below.
  }, [isOpen, autoDismissOnResolve, pipelineId])

  // Auto-discover tables from the source connection when the modal opens
  // and the backend hasn't pre-populated availableTables.
  useEffect(() => {
    if (!isOpen) return
    if (availableTables.length > 0 || discoveryStatus === "empty") return

    setDiscoveredTables([])
    setDiscoveryError(null)
    setDiscoveryRanEmpty(false)
    setDiscovering(true)

    async function discover() {
      // Resolve the connection ID: use prop if given, otherwise fetch from pipeline.
      let connId = sourceConnectionId || ""
      if (!connId && pipelineId) {
        try {
          const pr = await authFetch(API_ENDPOINTS.PIPELINES.GET(pipelineId), { cache: "no-store" })
          if (pr.ok) {
            const pd = (await pr.json()) as Record<string, unknown>
            const meta = pd["metadata"] && typeof pd["metadata"] === "object" ? (pd["metadata"] as Record<string, unknown>) : {}
            connId = String(pd["source_connection_id"] || meta["source_connection_id"] || "")
          }
        } catch {
          // ignore — will fall through to manual input
        }
      }

      if (!connId) {
        setDiscoveryError(null) // not an error — just no connection configured yet
        setDiscovering(false)
        return
      }

      try {
        const r = await authFetch(`${API_ENDPOINTS.CONNECTIONS.GET(connId)}/metadata`, { cache: "no-store" })
        if (!r.ok) throw new Error(`HTTP ${r.status}`)
        const data = (await r.json()) as Record<string, unknown>
        const raw = Array.isArray(data?.["tables"]) ? (data["tables"] as Record<string, unknown>[]) : []
        const parsed = raw
          .map((t) => ({
            name: String(t["name"] || t["table_name"] || ""),
            schema: t["schema"] ? String(t["schema"]) : undefined,
            row_count: typeof t["row_count"] === "number" ? t["row_count"] : undefined,
            columns:
              typeof t["column_count"] === "number"
                ? t["column_count"]
                : Array.isArray(t["columns"])
                ? (t["columns"] as unknown[]).length
                : undefined,
          }))
          .filter((t) => t.name)
        setDiscoveredTables(parsed)
        if (parsed.length === 0) setDiscoveryRanEmpty(true)
      } catch (e) {
        setDiscoveryError(String((e as Error)?.message || "Couldn't fetch tables from source"))
      } finally {
        setDiscovering(false)
      }
    }

    void discover()
  }, [isOpen, sourceConnectionId, pipelineId, availableTables.length, discoveryStatus])

  // ── AI table suggestions (self-fetch fallback) ──────────────────────────────
  // The backend computes suggestions asynchronously right after the pipeline
  // parks at table_selection, so blocking_reason.details.suggested_tables may
  // land a poll or two after this modal opens. When the caller didn't hand us
  // suggestions, poll /state a few times for them. Advisory + fail-soft: any
  // error simply means no suggestions.
  // The prop being PRESENT (even as an empty array) means the caller owns
  // suggestion delivery — render sites poll /state themselves, so the
  // self-fetch below would only duplicate those GETs. Self-fetch runs ONLY
  // when the prop is undefined (caller has no suggestion wiring for us).
  const hasPropSuggestions = suggestedTables !== undefined
  useEffect(() => {
    if (!isOpen) {
      setFetchedSuggestions([]) // reset for the next open
      return
    }
    if (hasPropSuggestions || !pipelineId) return

    let cancelled = false
    let attempts = 0
    let timer: ReturnType<typeof setTimeout> | null = null

    const fetchSuggestions = async () => {
      try {
        const res = await authFetch(`${API_ENDPOINTS.PIPELINES.GET(pipelineId)}/state`, { cache: "no-store" })
        if (res.ok) {
          const data = (await res.json()) as {
            status?: string
            blocking_reason?: { type?: string; details?: { suggested_tables?: unknown } } | null
          }
          const normalized = normalizeSuggestedTables(data?.blocking_reason?.details?.suggested_tables)
          if (normalized.length > 0) {
            if (!cancelled) setFetchedSuggestions(normalized)
            return
          }
          // Keep retrying only while the table-selection gate is actually
          // pending (that's the window where the backend may still be computing).
          const brType = String(data?.blocking_reason?.type || "").toLowerCase()
          if (!(String(data?.status || "") === "waiting_for_user" && brType.includes("table"))) return
        }
      } catch {
        // fail-soft: suggestions are advisory
      }
      attempts += 1
      if (!cancelled && attempts < 5) timer = setTimeout(fetchSuggestions, 3000)
    }

    void fetchSuggestions()
    return () => {
      cancelled = true
      if (timer) clearTimeout(timer)
    }
  }, [isOpen, hasPropSuggestions, pipelineId])

  function buildInitialSelection(
    tables: AvailableTable[],
    initialSelectedTables?: string[]
  ): Record<string, boolean> {
    const initial: Record<string, boolean> = {}
    if (!Array.isArray(initialSelectedTables) || initialSelectedTables.length === 0) return initial

    const raw = initialSelectedTables
      .map((t) => String(t || "").trim())
      .filter(Boolean)

    if (raw.length === 0) return initial

    // Build candidate match strings:
    // - exact value
    // - last segment (unqualified table)
    // - last two segments (schema.table)
    const candidates = new Set<string>()
    const candidatesLower = new Set<string>()
    for (const s of raw) {
      candidates.add(s)
      candidatesLower.add(s.toLowerCase())
      const parts = s.split(".").map((p) => p.trim()).filter(Boolean)
      if (parts.length >= 1) {
        const last = parts[parts.length - 1]
        candidates.add(last)
        candidatesLower.add(last.toLowerCase())
      }
      if (parts.length >= 2) {
        const lastTwo = `${parts[parts.length - 2]}.${parts[parts.length - 1]}`
        candidates.add(lastTwo)
        candidatesLower.add(lastTwo.toLowerCase())
      }
    }

    for (const t of tables || []) {
      const key = tableKey(t)
      const name = String(t.name || "").trim()
      const qualified = key
      const qualifiedLower = qualified.toLowerCase()
      const nameLower = name.toLowerCase()

      if (
        candidates.has(qualified) ||
        candidates.has(name) ||
        candidatesLower.has(qualifiedLower) ||
        candidatesLower.has(nameLower)
      ) {
        initial[qualified] = true
      }
    }

    // If we couldn't match against the provided table list (e.g. tables not loaded yet),
    // fall back to using the raw strings as keys.
    if (Object.keys(initial).length === 0) {
      for (const s of raw) initial[s] = true
    }

    return initial
  }

  function tableKey(t: AvailableTable): string {
    const schema = (t.schema || "").trim()
    const name = (t.name || "").trim()
    return schema ? `${schema}.${name}` : name
  }

  function tableDisplayName(t: AvailableTable): string {
    const schema = (t.schema || "").trim()
    const name = (t.name || "").trim()
    return schema ? `${schema}.${name}` : name
  }

  const sortedTables = useMemo(() => {
    const merged = availableTables.length > 0 ? availableTables : discoveredTables
    // Hide rsync's own bookkeeping/staging tables (`_rsync_*`, `flat_*`) — they
    // are not user data and must never be shown, ranked, or pre-selected. Mirrors
    // the Data Explorer, which already filters them via the same predicate; this
    // also covers pipelines that parked before the executor-side filter shipped.
    const visible = merged.filter((t) => !isInternalExplorerTable(t.name))
    return [...visible].sort((a, b) => tableDisplayName(a).localeCompare(tableDisplayName(b)))
  }, [availableTables, discoveredTables])
  const hasTables = sortedTables.length > 0

  // Map of table key → AI suggestion for rows that match a suggestion
  // (drives the "AI suggested" badge and the initial pre-selection).
  const effectiveSuggestions = hasPropSuggestions ? (suggestedTables as SuggestedTable[]) : fetchedSuggestions
  const suggestionMatch = useMemo(() => {
    const map = new Map<string, SuggestedTable>()
    if (effectiveSuggestions.length === 0 || sortedTables.length === 0) return map
    for (const s of effectiveSuggestions) {
      const qualified = (s.schema ? `${s.schema}.${s.name}` : s.name).toLowerCase()
      for (const t of sortedTables) {
        const key = tableKey(t)
        if (map.has(key)) continue
        const keyLower = key.toLowerCase()
        const nameLower = (t.name || "").trim().toLowerCase()
        if (keyLower === qualified || (!s.schema && nameLower === qualified)) {
          map.set(key, s)
        }
      }
    }
    return map
  }, [effectiveSuggestions, sortedTables])
  const isLoadingTables = (Boolean(loading) || discovering) && !hasTables

  const schemaOptions = useMemo(() => {
    const set = new Set<string>()
    for (const t of sortedTables) {
      const s = (t.schema || "").trim()
      if (s) set.add(s)
    }
    return Array.from(set).sort((a, b) => a.localeCompare(b))
  }, [sortedTables])

  // Source-aware label for a namespace unit: schema (pg/oracle/sqlserver/redshift),
  // database (mysql/mongo/clickhouse), dataset (bigquery).
  const namespaceNoun = useMemo(() => {
    const t = (sourceType || "").toLowerCase()
    if (t.includes("bigquery")) return "dataset"
    if (t.includes("mysql") || t.includes("mongo") || t.includes("clickhouse")) return "database"
    return "schema"
  }, [sourceType])

  const selectedNamespaces = useMemo(
    () => Object.keys(namespaceWildcards).filter((ns) => namespaceWildcards[ns]),
    [namespaceWildcards]
  )
  const hasNamespaceWildcards = selectedNamespaces.length > 0

  const filteredTables = useMemo(() => {
    const q = query.trim().toLowerCase()
    return sortedTables.filter((t) => {
      const schema = (t.schema || "").trim()
      if (schemaFilter !== "__all__") {
        if ((schema || "__default__") !== schemaFilter) return false
      }

      if (!q) return true
      const hay = `${schema} ${t.name}`.toLowerCase()
      return hay.includes(q)
    })
  }, [sortedTables, query, schemaFilter])

  const visibleTables = useMemo(() => {
    return filteredTables.slice(0, visibleCount)
  }, [filteredTables, visibleCount])

  // Reset discovered tables when closed so the next open re-fetches.
  useEffect(() => {
    if (!isOpen) {
      setDiscoveredTables([])
      setDiscoveryError(null)
      setDiscoveryRanEmpty(false)
      setWholeDatabase(false)
      setNamespaceWildcards({})
    }
  }, [isOpen])

  // Important: this dialog is often shown while the pipeline page is polling.
  // Polling can refresh `availableTables` which changes `sortedTables`, and we must NOT
  // clear the user's current selection on those updates.
  const wasOpenRef = useRef(false)
  // Who currently owns the checkbox state:
  //   "none"    — nothing meaningful selected yet (or only the single-table default)
  //   "initial" — persisted pipeline selection (initialSelectedTables) applied
  //   "ai"      — AI suggestions pre-selected
  //   "user"    — the user touched a checkbox / Select all / Clear
  // Arrival-order contract: AI may only claim a "none" selection; a persisted
  // selection overwrites "ai" (it ALWAYS wins regardless of arrival order);
  // nothing ever overwrites "user".
  const selectionOwnerRef = useRef<"none" | "initial" | "ai" | "user">("none")

  useEffect(() => {
    if (!isOpen) {
      wasOpenRef.current = false
      return
    }
    // Only initialize form state on the transition closed -> open.
    // Do not reset when `sortedTables` updates while open.
    if (wasOpenRef.current) return
    wasOpenRef.current = true

    setError(null)
    setQuery("")
    setSchemaFilter("__all__")
    setVisibleCount(50)
    setManualTables("")
    // Default selection: if list is small, select first; otherwise none.
    const initial: Record<string, boolean> = buildInitialSelection(sortedTables, initialSelectedTables)
    selectionOwnerRef.current = Object.keys(initial).length > 0 ? "initial" : "none"
    if (sortedTables.length === 1) {
      // Only auto-select single table if nothing is preselected.
      if (!Object.values(initial).some(Boolean)) {
        initial[tableKey(sortedTables[0])] = true
      }
    }
    setSelected(initial)
  }, [isOpen, pipelineId, executionId, sortedTables, initialSelectedTables])

  // If the modal opened before pipeline config finished loading (initialSelectedTables
  // arrives later — parents fetch it asynchronously), apply it. The persisted
  // selection ALWAYS wins over AI pre-selection regardless of arrival order —
  // it overwrites an "ai"-owned selection (and the single-table default) —
  // but never overwrites checkboxes the user has touched ("user"-owned).
  useEffect(() => {
    if (!isOpen) return
    if (!Array.isArray(initialSelectedTables) || initialSelectedTables.length === 0) return
    if (selectionOwnerRef.current === "user") return
    const next = buildInitialSelection(sortedTables, initialSelectedTables)
    if (Object.keys(next).length === 0) return
    selectionOwnerRef.current = "initial"
    setAiPreselectedCount(0) // the selection is no longer AI's — drop the hint
    setSelected((prev) => {
      const nextKeys = Object.keys(next)
      const prevKeys = Object.keys(prev).filter((k) => prev[k])
      const same = prevKeys.length === nextKeys.length && nextKeys.every((k) => prev[k])
      return same ? prev : next // avoid re-render churn on every poll refresh
    })
  }, [isOpen, sortedTables, initialSelectedTables])

  // If tables arrive after opening and there's exactly one option, preselect it
  // only if the user hasn't started selecting anything yet.
  useEffect(() => {
    if (!isOpen) return
    if (sortedTables.length !== 1) return
    const onlyKey = tableKey(sortedTables[0])
    setSelected((prev) => {
      const hasAny = Object.values(prev).some(Boolean)
      if (hasAny) return prev
      return { [onlyKey]: true }
    })
  }, [isOpen, sortedTables])

  // AI pre-selection: check the suggested tables, but ONLY when nothing else
  // owns the selection — a persisted selection (initialSelectedTables) or the
  // user's own choices always win. Suggestions are CONSUMED on their FIRST
  // arrival (the ref is set even when application is skipped), so this effect
  // can never re-fire later and fight an explicit user "Clear". If a persisted
  // selection arrives AFTER we applied, the effect above overwrites us.
  const appliedSuggestionsRef = useRef(false)
  useEffect(() => {
    if (!isOpen) {
      appliedSuggestionsRef.current = false
      setAiPreselectedCount(0)
      return
    }
    if (appliedSuggestionsRef.current) return
    if (suggestionMatch.size === 0) return
    // First arrival: mark consumed BEFORE deciding whether to apply.
    appliedSuggestionsRef.current = true
    if (Array.isArray(initialSelectedTables) && initialSelectedTables.length > 0) return
    if (selectionOwnerRef.current !== "none") return
    if (Object.values(selected).some(Boolean)) return
    // Confidence gate: only pre-check tables the ranker is confident AND
    // discriminating about (see computeAutoPreselectKeys). Prevents the "all
    // tables pre-checked" bug where a "top N of N" ranking silently selects
    // unintended tables. Empty → user chooses.
    const keys = computeAutoPreselectKeys(suggestionMatch, sortedTables.length)
    if (keys.length === 0) return
    selectionOwnerRef.current = "ai"
    setAiPreselectedCount(keys.length)
    const next: Record<string, boolean> = {}
    for (const key of keys) next[key] = true
    setSelected(next)
  }, [isOpen, suggestionMatch, initialSelectedTables, selected, sortedTables])

  const selectedTables = useMemo(() => {
    return Object.entries(selected)
      .filter(([, v]) => v)
      .map(([k]) => k)
  }, [selected])

  const manualSelectedTables = useMemo(() => parseManualTables(manualTables), [manualTables])

  // How many distinct source schemas the current selection spans. Whole-database
  // is the strongest signal (every schema); otherwise count the namespace
  // wildcards plus the distinct schema prefixes of individually-checked tables.
  const distinctSelectedSchemas = useMemo(() => {
    const set = new Set<string>()
    for (const ns of selectedNamespaces) set.add(ns)
    // Mirror onConfirm's list resolution: checked tables win; otherwise the
    // manual free-text entries are what actually gets submitted, so they must
    // also drive the multi-schema detection (and thus the preserve directive).
    const keys = selectedTables.length > 0 ? selectedTables : manualSelectedTables
    for (const key of keys) {
      const dot = key.indexOf(".")
      const schema = dot > 0 ? key.slice(0, dot).trim() : ""
      if (schema) set.add(schema)
    }
    return set.size
  }, [selectedNamespaces, selectedTables, manualSelectedTables])

  // When the selection spans more than one source schema we mirror each source
  // schema at the destination (a blank destination namespace → per-schema
  // layout), which is exactly what the backend does
  // (executor.preserveSourceSchemaLayout auto-preserves when the selection spans
  // >1 source schema and no deliberate namespace is set). Forcing a single
  // target namespace here would either block the user or silently flatten every
  // schema into one namespace (same-named tables collide → data loss, PR #549).
  const preservesSourceSchemas = showMapping && (wholeDatabase || distinctSelectedSchemas > 1)
  // A destination namespace is required only when the destination has a real
  // namespace concept (schema/database/dataset — createable) AND the selection
  // is single-schema. Path/prefix destinations (object storage, sqlite) and
  // multi-schema selections leave it optional (blank is valid).
  const namespaceRequired = showMapping && destMeta.createable && !preservesSourceSchemas
  const destNamespaceError = showMapping
    ? validateNamespace(destNamespace, { required: namespaceRequired })
    : ""

  // Safe default for a multi-schema selection: blank ⇒ preserve each source
  // schema at the destination. The field is seeded with the destination's
  // default namespace — an engine default ("public"/"dbo"/…) OR, for SQL Server
  // / Snowflake / Oracle / Databricks sources, the SOURCE SLUG ("sqlserver"/…).
  // ANY untouched seed left in place for a multi-schema selection would FLATTEN
  // every source schema into that one namespace (same-named tables across
  // schemas collide → data loss, PR #549 / #552). The seed is never a
  // deliberate choice until touched — so blank ANY untouched value while the
  // selection spans >1 schema, so "leave blank to keep each source schema
  // separate" (what the help text promises) is the real default. Only a name
  // the user TYPES means "merge into this one". When the selection narrows back
  // to a single schema (which needs an explicit name) restore the seed we
  // cleared, as long as the user still hasn't typed one.
  useEffect(() => {
    if (!isOpen || !showMapping) return
    if (namespaceTouchedRef.current) return
    if (preservesSourceSchemas) {
      if (destNamespace.trim() !== "") setDestNamespace("")
    } else if (destNamespace.trim() === "" && seededNamespace) {
      setDestNamespace(seededNamespace)
    }
  }, [isOpen, showMapping, preservesSourceSchemas, destNamespace, seededNamespace])

  // Copy helpers for the destination-mapping help text. The goal is that the
  // single-schema and "entire database"/multi-schema cases read as clearly
  // different, and that it's obvious a destination schema/database/dataset gets
  // CREATED (or, for object storage, which folder objects land in).
  // destIsPathStyle: object storage / sqlite — a folder/prefix, not a created
  // container (no "created automatically" wording, blank = destination root).
  const destIsPathStyle = !destMeta.createable
  // How many distinct source schemas the current selection spans (for the
  // "N source schemas" phrasing). Whole-database uses the discovered schema
  // count; an explicit selection uses the checked/typed schemas.
  const distinctSourceSchemaCount = wholeDatabase ? schemaOptions.length : distinctSelectedSchemas
  const sourceSchemaCountLabel =
    distinctSourceSchemaCount === 1
      ? `1 source ${namespaceNoun}`
      : distinctSourceSchemaCount > 1
      ? `${distinctSourceSchemaCount} source ${namespaceNoun}s`
      : `multiple source ${namespaceNoun}s`
  // A couple of real schema names to make the behavior concrete (illustrative;
  // whole-DB syncs every schema, not only the previewed ones).
  const exampleSchemas = schemaOptions.slice(0, 2).join(", ")

  function parseManualTables(input: string): string[] {
    return String(input || "")
      .split(/[,\s]+/)
      .map((s) => s.trim())
      .filter(Boolean)
  }

  async function onConfirm() {
    const manual = parseManualTables(manualTables)
    const nsWildcards = selectedNamespaces.map((ns) => `${ns}.*`)
    // "Select entire database" wins: emit the whole-source sentinel and let the
    // server resolve it to every schema.table (bypasses the 100-table cap).
    const toSend = wholeDatabase
      ? ["*"]
      : [...nsWildcards, ...(selectedTables.length > 0 ? selectedTables : manual)]
    if (toSend.length === 0) {
      setError(hasTables ? "Select at least one table to continue." : "Enter at least one table name to continue.")
      return
    }
    // Destination mapping (PR-C): block on an obviously-invalid namespace before submit.
    if (showMapping && destNamespaceError) {
      setError(destNamespaceError)
      return
    }
    // Destination mapping — three cases:
    //  1. The user typed a namespace  → send it as the single target (flatten
    //     every selected table into that one schema/database/dataset).
    //  2. Blank + the selection spans MULTIPLE source schemas → send an explicit
    //     schema_mode="preserve" directive (empty namespace) so each source schema
    //     is mirrored at the destination. This is the backend's HIGHEST-priority
    //     override (executor.preserveSourceSchemaLayout step 1), so it holds even
    //     for sources whose seeded destination namespace is NOT an engine default
    //     (e.g. SQL Server / Snowflake seed the source slug) — where merely
    //     omitting destination_config would leave that non-default seed in place
    //     and silently FLATTEN (same-named tables across schemas collide → data
    //     loss, PR #549).
    //  3. Blank + single-schema (only reachable for path/prefix destinations,
    //     which don't require a name) → omit destination_config entirely; there is
    //     no cross-schema collision to guard against.
    // Auto-create the namespace when the engine supports it (schema/database/
    // dataset); for prefix/path destinations there's nothing to create.
    const trimmedNamespace = destNamespace.trim()
    // A namespace only counts as a deliberate merge target when the user TYPED
    // it (namespaceTouchedRef). An untouched auto-seed on a multi-schema
    // selection must NOT flatten — that seed (an engine default OR a source slug
    // like "sqlserver"/"snowflake") is not a choice, so it preserves each source
    // schema instead. Single-schema selections require an explicit name, so an
    // untouched seed there IS the intended target and is used as-is.
    const namespaceIsDeliberate =
      trimmedNamespace !== "" && (!preservesSourceSchemas || namespaceTouchedRef.current)
    let destCfg: DestinationConfig | undefined
    if (showMapping && namespaceIsDeliberate) {
      destCfg = {
        namespace: trimmedNamespace,
        namespace_kind: destKind,
        create_if_not_exists: destMeta.createable,
        // For a MULTI-schema selection, a typed name means "merge everything into
        // this one namespace" — send an explicit "flatten" so it overrides any
        // preserve mode a prior blank run may have stuck on the pipeline
        // (destination_schema_mode is sticky; without this the executor would keep
        // mirroring per source schema and ignore the name just typed). Single-schema
        // selections need no directive (there is nothing to preserve/flatten).
        ...(preservesSourceSchemas ? { schema_mode: "flatten" as const } : {}),
      }
    } else if (showMapping && preservesSourceSchemas) {
      destCfg = {
        namespace: "",
        namespace_kind: destKind,
        create_if_not_exists: destMeta.createable,
        schema_mode: "preserve",
      }
    } else {
      destCfg = undefined
    }
    setSubmitting(true)
    setError(null)

    // If parent provided onTablesSelected callback, use it (for suggestions flow)
    // Otherwise, resume immediately (backward compatibility)
    if (onTablesSelected) {
      try {
        await onTablesSelected(toSend, destCfg)
        onClose()
      } catch (e: any) {
        setError(String(e?.message || e || "Failed to process table selection"))
      } finally {
        setSubmitting(false)
      }
      return
    }

    // Legacy direct resume (no suggestions)
    try {
      await resumePipelineTables(pipelineId, {
        execution_id: executionId,
        selected_tables: toSend,
        ...(destCfg ? { destination_config: destCfg } : {}),
      })
      // Optimistically clear HITL in parent UI (signal processing is async)
      onResolved?.()
      // Close modal immediately (optimistic: signal is async, backend will process in ~1-3s)
      onClose()
    } catch (e: any) {
      // On error, show it to user and keep modal open
      setError(String(e?.message || e || "Failed to resume pipeline"))
      setSubmitting(false)
    }
  }

  return (
    <Dialog open={isOpen} onOpenChange={(open) => (!open ? onClose() : null)}>
      <DialogContent className="max-w-3xl max-h-[90vh] flex flex-col overflow-hidden">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <TableIcon className="h-5 w-5 text-violet-600" />
            {title}
          </DialogTitle>
        </DialogHeader>

        {/* Scrollable body so the primary "Sync N tables" CTA in the footer stays
            reachable at short viewports (it scrolled off below ~812px). */}
        <div className="flex-1 min-h-0 overflow-y-auto -mx-6 px-6">
        <div className="space-y-3">
          {/* Source database header */}
          {(sourceDatabase || sourceType) && (
            <div className="flex items-center gap-2 px-3 py-2 rounded-lg bg-zinc-50 dark:bg-zinc-900 border border-zinc-200 dark:border-zinc-800 text-sm">
              <Database className="h-4 w-4 text-blue-500 shrink-0" />
              <span className="text-zinc-500">Source:</span>
              <span className="font-medium capitalize">
                {sourceDatabase || sourceType}
              </span>
              {hasTables && (
                <Badge variant="secondary" className="ml-auto">{sortedTables.length} table{sortedTables.length !== 1 ? "s" : ""} found</Badge>
              )}
            </div>
          )}

          {/* Destination mapping (PR-C): choose where this pipeline writes on the
              destination. Saved per pipeline; only shown at first provisioning. */}
          {showMapping && (
            <div className="space-y-3 rounded-lg border border-violet-200 dark:border-violet-900/50 bg-violet-50/50 dark:bg-violet-950/20 p-3">
              <div className="flex items-center gap-2 text-sm font-medium">
                <Database className="h-4 w-4 text-violet-500 shrink-0" />
                Destination mapping
                {destinationType && (
                  <span className="ml-auto text-xs font-normal text-zinc-500">
                    Destination: <span className="font-medium capitalize text-zinc-700 dark:text-zinc-300">{destinationType}</span>
                  </span>
                )}
              </div>
              <div className="space-y-1.5">
                <Label htmlFor="dest-namespace" className="text-sm">
                  {destMeta.noun} name
                  {!namespaceRequired && <span className="font-normal text-zinc-400"> (optional)</span>}
                </Label>
                <Input
                  id="dest-namespace"
                  value={destNamespace}
                  onChange={(e) => {
                    namespaceTouchedRef.current = true
                    setDestNamespace(e.target.value)
                  }}
                  placeholder={
                    preservesSourceSchemas
                      ? `Leave blank to keep each source ${namespaceNoun} separate`
                      : destIsPathStyle
                      ? "Leave blank to write at the destination root"
                      : `Name the ${destMeta.noun.toLowerCase()} to create`
                  }
                  aria-invalid={!!destNamespaceError && (destNamespace.trim() !== "" || namespaceRequired)}
                />
                {/* Help text: explicit about WHAT gets created/where data lands, and
                    clearly distinct for the single-schema vs multi-schema cases. */}
                {preservesSourceSchemas ? (
                  <p className="text-xs text-violet-600 dark:text-violet-400">
                    Your selection spans <span className="font-medium">{sourceSchemaCountLabel}</span>
                    {!wholeDatabase && selectedTables.length > 0 ? ` (${selectedTables.length} table${selectedTables.length !== 1 ? "s" : ""})` : ""}.{" "}
                    {destIsPathStyle ? (
                      <>
                        <span className="font-medium">Leave blank</span> and each source {namespaceNoun} is written to its own
                        folder on the destination{exampleSchemas ? <> (e.g. <span className="font-mono">{exampleSchemas.split(", ")[0]}/…</span>)</> : null},
                        so nothing is mixed together. <span className="font-medium">Type a name</span> to write every {namespaceNoun}
                        {" "}under one shared folder instead.
                      </>
                    ) : (
                      <>
                        <span className="font-medium">Leave blank</span> and rsync creates a matching {destMeta.noun.toLowerCase()} for
                        each one on the destination{exampleSchemas ? <> (e.g. <span className="font-mono">{exampleSchemas}</span>)</> : null},
                        so same-named tables never overwrite each other. <span className="font-medium">Type a name</span> to merge every
                        {" "}source {namespaceNoun} into that single {destMeta.noun.toLowerCase()} instead.
                      </>
                    )}
                  </p>
                ) : destIsPathStyle ? (
                  <p className="text-xs text-zinc-500 dark:text-zinc-400">
                    The folder your objects are written under on the destination. Leave blank to write at the root.
                  </p>
                ) : (
                  <p className="text-xs text-zinc-500 dark:text-zinc-400">
                    rsync <span className="font-medium">creates this {destMeta.noun.toLowerCase()}</span>{" "}
                    on the destination (if it doesn&apos;t already exist) and lands your tables in it.
                  </p>
                )}
                {destNamespaceError && (destNamespace.trim() !== "" || namespaceRequired) && (
                  <p className="text-xs text-red-500" role="alert">{destNamespaceError}</p>
                )}
              </div>
              <p className="text-xs text-zinc-500 dark:text-zinc-400">
                Applies to this pipeline only — it never changes the shared connection.
              </p>
            </div>
          )}

          <p className="text-sm text-zinc-600 dark:text-zinc-400">
            {hasTables
              ? "Select which tables to include in this pipeline."
              : discovering
              ? `Discovering tables from ${sourceType || "source"}…`
              : discoveryStatus === "empty"
              ? discoveryReason ||
                (sourceDatabase
                  ? `"${sourceDatabase}" has no tables. Enter a qualified name like \`otherdb.users\` below.`
                  : `${sourceType || "Source"} has no tables. Enter a table name manually.`)
              : discoveryError
              ? `${discoveryError}. Enter table name(s) manually below.`
              : discoveryRanEmpty
              ? `No tables found in this ${sourceType || "source"} database. Enter a table name manually (e.g. mydb.users).`
              : sourceType
              ? `Couldn't list tables from ${sourceType}. Enter the table name(s) manually.`
              : "Couldn't list tables. Enter the table name(s) manually."}
          </p>

          {aiPreselectedCount > 0 && (
            <p className="text-xs text-violet-600 dark:text-violet-400 flex items-center gap-1">
              <Sparkles className="h-3 w-3 shrink-0" />
              AI pre-selected {aiPreselectedCount} table{aiPreselectedCount !== 1 ? "s" : ""} — adjust freely
            </p>
          )}

          {isLoadingTables ? (
            <div className="flex items-center gap-2 text-sm text-zinc-600 dark:text-zinc-400">
              <Loader2 className="h-4 w-4 animate-spin" />
              Loading tables…
            </div>
          ) : !hasTables ? (
            <div className="space-y-2">
              <Label htmlFor="manual-tables" className="text-sm">
                Table names (comma-separated)
              </Label>
              <Input
                id="manual-tables"
                value={manualTables}
                onChange={(e) => setManualTables(e.target.value)}
                placeholder="e.g. users, orders or mydb.users"
              />
              <div className="text-xs text-zinc-500 dark:text-zinc-400">
                Tip: for MySQL connections without a default database, use a qualified name like{" "}
                <span className="font-mono">mydb.users</span>.
              </div>
            </div>
          ) : null}

          {/* Large-list helpers: search + optional namespace filter (schema/db/workspace/etc.) */}
          {hasTables ? (
            <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
              <Input value={query} onChange={(e) => setQuery(e.target.value)} placeholder="Search tables…" />
              <Select value={schemaFilter} onValueChange={setSchemaFilter}>
                <SelectTrigger>
                  <SelectValue placeholder="All namespaces" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="__all__">All namespaces</SelectItem>
                  {schemaOptions.length > 0 ? (
                    <>
                      <SelectItem value="__default__">No namespace</SelectItem>
                      {schemaOptions.map((s) => (
                        <SelectItem key={s} value={s}>
                          {s}
                        </SelectItem>
                      ))}
                    </>
                  ) : (
                    <SelectItem value="__default__">No namespace</SelectItem>
                  )}
                </SelectContent>
              </Select>
            </div>
          ) : null}

          {hasTables ? (
            <div className="space-y-1.5">
              {/* Whole-database selection: emits a server-resolved "*" sentinel,
                  so it is NOT bounded by the 100-table discovery preview below. */}
              <label
                className={`flex items-start gap-2 rounded-md border p-2.5 cursor-pointer transition-colors ${
                  wholeDatabase
                    ? "border-violet-400 bg-violet-50 dark:border-violet-500 dark:bg-violet-950/30"
                    : "border-zinc-200 dark:border-zinc-800 hover:bg-zinc-50 dark:hover:bg-zinc-900"
                }`}
              >
                <Checkbox
                  checked={wholeDatabase}
                  onCheckedChange={(v) => {
                    selectionOwnerRef.current = "user"
                    setWholeDatabase(Boolean(v))
                  }}
                  className="mt-0.5"
                  id="whole-database"
                />
                <span className="text-sm">
                  <span className="font-medium">Select entire database</span>
                  <span className="text-zinc-500 dark:text-zinc-400">
                    {" "}— all {schemaOptions.length || 1} {namespaceNoun}
                    {(schemaOptions.length || 1) !== 1 ? "s" : ""}, every table
                  </span>
                </span>
              </label>
              {wholeDatabase && (
                <p className="text-xs text-violet-600 dark:text-violet-400">
                  Every table in every {namespaceNoun} will be synced. The full list is resolved on
                  the server at sync time, so tables beyond the {sortedTables.length}-table preview are included.
                </p>
              )}
              {!wholeDatabase && sortedTables.length >= 100 && (
                <p className="text-xs text-amber-600 dark:text-amber-500">
                  Showing the first {sortedTables.length} tables (discovery preview is capped). Use
                  “Select entire database” above to include everything.
                </p>
              )}
            </div>
          ) : null}

          {hasTables ? (
            <div className={`flex flex-wrap items-center justify-between gap-2 text-xs text-zinc-500 ${wholeDatabase ? "opacity-50 pointer-events-none" : ""}`}>
              <span>
                Showing{" "}
                <span className="font-medium text-zinc-700 dark:text-zinc-200">{Math.min(visibleTables.length, filteredTables.length)}</span> of{" "}
                <span className="font-medium text-zinc-700 dark:text-zinc-200">{filteredTables.length}</span>
                {query.trim() ? " (filtered)" : ""}
                {/* Always-visible selection count — the checked rows may be
                    scrolled out of the window (e.g. AI pre-selected tables that
                    sort below the first page), so surface the total here so the
                    selection never looks empty. */}
                {selectedTables.length > 0 ? (
                  <>
                    {" · "}
                    <span className="font-medium text-violet-600 dark:text-violet-400">
                      {selectedTables.length} selected
                    </span>
                  </>
                ) : null}
              </span>
              <div className="flex gap-2">
                {schemaFilter !== "__all__" && schemaFilter !== "__default__" && (
                  <Button
                    type="button"
                    size="sm"
                    variant={namespaceWildcards[schemaFilter] ? "default" : "outline"}
                    onClick={() => {
                      selectionOwnerRef.current = "user"
                      setNamespaceWildcards((prev) => ({ ...prev, [schemaFilter]: !prev[schemaFilter] }))
                    }}
                    title={`Select every table in ${schemaFilter} (resolved server-side)`}
                  >
                    {namespaceWildcards[schemaFilter] ? `✓ Entire ${schemaFilter}` : `Select entire ${schemaFilter}`}
                  </Button>
                )}
                <Button
                  type="button"
                  size="sm"
                  variant="outline"
                  onClick={() => {
                    selectionOwnerRef.current = "user"
                    const next: Record<string, boolean> = { ...selected }
                    for (const t of filteredTables) {
                      next[tableKey(t)] = true
                    }
                    setSelected(next)
                  }}
                  disabled={filteredTables.length === 0}
                >
                  Select all shown
                </Button>
                <Button
                  type="button"
                  size="sm"
                  variant="outline"
                  onClick={() => {
                    selectionOwnerRef.current = "user" // an explicit Clear is never fought
                    setSelected({})
                  }}
                  disabled={Object.keys(selected).length === 0}
                >
                  Clear
                </Button>
              </div>
            </div>
          ) : null}

          {error && (
            <div className="text-sm text-red-600 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-md p-3">
              {error}
            </div>
          )}

          {hasTables ? (
            <div className={`max-h-96 overflow-auto border rounded-lg divide-y divide-zinc-100 dark:divide-zinc-800 ${wholeDatabase ? "opacity-50 pointer-events-none" : ""}`}>
              {visibleTables.map((t) => {
                const key = tableKey(t)
                const checked = !!selected[key]
                const isPreviewing = expandedPreview === key
                return (
                  <div key={key} className={`transition-colors ${checked ? "bg-violet-50 dark:bg-violet-950/30" : "hover:bg-zinc-50 dark:hover:bg-zinc-900"}`}>
                    <label
                      htmlFor={`tbl-${key}`}
                      className="flex items-center gap-3 px-3 py-2.5 cursor-pointer"
                    >
                      <Checkbox
                        checked={checked}
                        onCheckedChange={(v) => {
                          selectionOwnerRef.current = "user"
                          setSelected((prev) => ({ ...prev, [key]: Boolean(v) }))
                        }}
                        id={`tbl-${key}`}
                      />
                      <TableIcon className={`h-4 w-4 shrink-0 ${checked ? "text-violet-500" : "text-zinc-400"}`} />
                      <span className="flex-1 text-sm font-medium truncate">
                        {tableDisplayName(t)}
                      </span>
                      <div className="flex items-center gap-1.5 shrink-0">
                        {suggestionMatch.has(key) && (
                          <Badge
                            variant="secondary"
                            className="text-xs font-normal py-0 bg-violet-100 text-violet-700 dark:bg-violet-900/40 dark:text-violet-300"
                            title={suggestionMatch.get(key)?.reason || "Suggested by AI from your pipeline intent"}
                          >
                            <Sparkles className="h-2.5 w-2.5 mr-0.5" />
                            AI suggested
                          </Badge>
                        )}
                        {typeof t.columns === "number" && (
                          <Badge variant="outline" className="text-xs font-normal py-0">
                            {t.columns} col{t.columns !== 1 ? "s" : ""}
                          </Badge>
                        )}
                        <Badge variant="secondary" className="text-xs font-normal py-0">
                          {typeof t.row_count === "number" && t.row_count >= 0
                            ? `${t.is_exact_count ? "" : "~"}${t.row_count.toLocaleString()} rows`
                            : "? rows"}
                        </Badge>
                        {checked && <CheckCircle2 className="h-4 w-4 text-violet-500" />}
                        {sourceConnectionId && (
                          <button
                            type="button"
                            onClick={(e) => {
                              e.preventDefault()
                              setExpandedPreview(isPreviewing ? null : key)
                            }}
                            className="ml-1 p-1 rounded hover:bg-zinc-200 dark:hover:bg-zinc-700 text-zinc-400 hover:text-zinc-600 dark:hover:text-zinc-300"
                            title="Preview sample rows"
                          >
                            {isPreviewing ? <ChevronUp className="h-3.5 w-3.5" /> : <Eye className="h-3.5 w-3.5" />}
                          </button>
                        )}
                      </div>
                    </label>
                    {isPreviewing && sourceConnectionId && (
                      <div className="px-3 pb-3">
                        <TablePreviewPanel connectionId={sourceConnectionId} tableName={key} />
                      </div>
                    )}
                  </div>
                )
              })}
            </div>
          ) : null}

          {hasTables && filteredTables.length > visibleCount ? (
            <div className="flex justify-center">
              <Button type="button" variant="outline" onClick={() => setVisibleCount((n) => n + 50)}>
                Load more (+50)
              </Button>
            </div>
          ) : null}
        </div>

        {showCdcBackfillToggle ? (
          <div className="mt-3 rounded-md border bg-zinc-50 dark:bg-zinc-900 p-3 flex items-start gap-3">
            <Checkbox
              checked={cdcBackfillNewTables}
              onCheckedChange={(v) => onCdcBackfillNewTablesChange?.(Boolean(v))}
              className="mt-0.5"
            />
            <div className="flex-1">
              <div className="text-sm font-medium">Backfill existing rows for newly added tables</div>
              <div className="text-xs text-zinc-500 dark:text-zinc-400">
                Recommended. Existing tables won&apos;t be reloaded—only tables you add now will be backfilled.
              </div>
            </div>
          </div>
        ) : null}
        </div>

        <DialogFooter className="gap-2 items-center shrink-0 border-t border-zinc-200 dark:border-zinc-800 pt-4 mt-1">
          {selectedTables.length > 0 && (
            <span className="text-sm text-zinc-500 mr-auto">
              {selectedTables.length} table{selectedTables.length !== 1 ? "s" : ""} selected
            </span>
          )}
          <Button variant="outline" onClick={onClose} disabled={submitting}>
            Cancel
          </Button>
          <Button
            onClick={onConfirm}
            disabled={submitting || (showMapping && !!destNamespaceError) || (!wholeDatabase && !hasNamespaceWildcards && selectedTables.length === 0 && manualSelectedTables.length === 0)}
            className="bg-gradient-to-r from-violet-600 to-indigo-600 hover:from-violet-700 hover:to-indigo-700"
          >
            {submitting ? (
              <>
                <Loader2 className="h-4 w-4 animate-spin mr-2" />
                Continuing…
              </>
            ) : wholeDatabase ? (
              "Sync entire database"
            ) : hasNamespaceWildcards ? (
              // Both whole-schema wildcards AND individually-checked tables are
              // submitted (onConfirm merges them) — count both so the label
              // never undercounts the real selection.
              `Sync ${selectedNamespaces.length} ${namespaceNoun}${selectedNamespaces.length !== 1 ? "s" : ""}${
                selectedTables.length > 0 ? ` + ${selectedTables.length} table${selectedTables.length !== 1 ? "s" : ""}` : ""
              }`
            ) : selectedTables.length > 0 ? (
              `Sync ${selectedTables.length} table${selectedTables.length !== 1 ? "s" : ""}`
            ) : manualSelectedTables.length > 0 ? (
              `Sync ${manualSelectedTables.length} table${manualSelectedTables.length !== 1 ? "s" : ""}`
            ) : (
              "Continue"
            )}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}


