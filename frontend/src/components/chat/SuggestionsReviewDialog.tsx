"use client"

import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
} from "@/components/ui/dialog"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Checkbox } from "@/components/ui/checkbox"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { ScrollArea } from "@/components/ui/scroll-area"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import {
  Loader2,
  Sparkles,
  Shield,
  Zap,
  AlertTriangle,
  CheckCircle2,
  Eye,
  Info,
  ChevronDown,
  ChevronRight,
  Settings2,
} from "lucide-react"
import { Skeleton } from "@/components/ui/skeleton"
import { toast } from "sonner"
import { API_ENDPOINTS } from "@/lib/config/api"
import { authFetch } from "@/lib/api/auth-fetch"
import {
  generateSuggestions,
  type SuggestionsResponse,
  type PIIColumnSuggestion,
  type TransformSuggestion,
  type OptimizationSuggestion,
} from "@/lib/api/suggestions"
import { TRANSFORM_TYPE_BLURBS, transformTypeLabel } from "@/lib/transform-display"

export interface TableMetadata {
  name: string
  schema?: string
  columns: Array<{
    name: string
    type: string
    nullable?: boolean
    is_primary_key?: boolean
    is_foreign_key?: boolean
  }>
  row_count?: number
  primary_keys?: string[]
  foreign_keys?: Array<{
    column: string
    references_table: string
    references_column: string
  }>
  indexes?: Array<{
    name: string
    columns: string[]
    unique: boolean
  }>
}

interface SuggestionsReviewDialogProps {
  isOpen: boolean
  // Skip/close handler. May be async (it resumes the parked pipeline) — the dialog
  // awaits it so it can show a spinner and stay open on failure.
  onClose: () => void | Promise<void>
  pipelineId: string
  executionId?: string
  sourceConnectionId: string
  selectedTables: string[]
  onApplyContinue: (transforms: TransformSuggestion[]) => Promise<void>
  // If the parent already fetched schema (pre-fetch on table selection), pass it here
  // to skip the schema fetch step and jump straight to LLM suggestions.
  prefetchedSchema?: TableMetadata[]
}

// ---------------------------------------------------------------------------
// Type-specific transform editor cards (P4). Each receives the *effective*
// config (suggestion config merged with any prior user edits) and reports
// changes back via onChange, which the parent merges into editedConfigs[idx].
// Unknown types fall through to a read-only JSON view.
// ---------------------------------------------------------------------------

type ConfigEditor = {
  config: Record<string, unknown>
  onChange: (patch: Record<string, unknown>) => void
}

function RenameColumnsCard({ config, onChange }: ConfigEditor) {
  const mappings = (config.mappings && typeof config.mappings === "object"
    ? (config.mappings as Record<string, string>)
    : {})
  const entries = Object.entries(mappings)
  if (entries.length === 0) {
    return <div className="text-xs text-muted-foreground mt-2">No column mappings.</div>
  }
  return (
    <div className="space-y-2 mt-2">
      {entries.map(([from, to]) => (
        <div key={from} className="flex items-center gap-2">
          <span className="text-xs font-mono text-muted-foreground truncate max-w-[40%]" title={from}>
            {from}
          </span>
          <span className="text-xs text-muted-foreground">→</span>
          <Input
            className="h-8 text-xs flex-1"
            value={to}
            onChange={(e) =>
              onChange({ mappings: { ...mappings, [from]: e.target.value } })
            }
          />
        </div>
      ))}
    </div>
  )
}

function JSONFlattenCard({ config, onChange }: ConfigEditor) {
  const column = String(config.column ?? "")
  const prefix = String(config.prefix ?? "")
  const maxDepth = config.max_depth === undefined ? 2 : Number(config.max_depth)
  return (
    <div className="space-y-2 mt-2">
      <div className="text-xs">
        <span className="text-muted-foreground">Column: </span>
        <span className="font-mono">{column}</span>
      </div>
      <div className="flex items-center gap-2">
        <Label className="text-xs w-20 shrink-0">Prefix</Label>
        <Input
          className="h-8 text-xs flex-1"
          value={prefix}
          onChange={(e) => onChange({ prefix: e.target.value })}
        />
      </div>
      <div className="flex items-center gap-2">
        <Label className="text-xs w-20 shrink-0">Max depth</Label>
        <select
          className="h-8 rounded-md border border-input bg-background px-2 text-xs"
          value={String(maxDepth)}
          onChange={(e) => onChange({ max_depth: Number(e.target.value) })}
        >
          <option value="1">1</option>
          <option value="2">2</option>
          <option value="3">3</option>
          <option value="0">All</option>
        </select>
      </div>
    </div>
  )
}

function ArrayExpandCard({ config, onChange }: ConfigEditor) {
  const column = String(config.column ?? "")
  const outputPrefix = String(config.output_prefix ?? "")
  const maxElements = config.max_elements === undefined ? 10 : Number(config.max_elements)
  return (
    <div className="space-y-2 mt-2">
      <div className="text-xs">
        <span className="text-muted-foreground">Column: </span>
        <span className="font-mono">{column}</span>
      </div>
      <div className="flex items-center gap-2">
        <Label className="text-xs w-24 shrink-0">Output prefix</Label>
        <Input
          className="h-8 text-xs flex-1"
          value={outputPrefix}
          onChange={(e) => onChange({ output_prefix: e.target.value })}
        />
      </div>
      <div className="flex items-center gap-2">
        <Label className="text-xs w-24 shrink-0">Max elements</Label>
        <Input
          type="number"
          min={1}
          max={100}
          className="h-8 text-xs w-24"
          value={Number.isFinite(maxElements) ? maxElements : 10}
          onChange={(e) => {
            const n = parseInt(e.target.value, 10)
            onChange({ max_elements: Number.isFinite(n) ? Math.min(100, Math.max(1, n)) : 10 })
          }}
        />
      </div>
    </div>
  )
}

function TransformConfigEditor({
  transform,
  config,
  onChange,
}: ConfigEditor & { transform: TransformSuggestion }) {
  switch (transform.type) {
    case "rename_columns":
      return <RenameColumnsCard config={config} onChange={onChange} />
    case "json_flatten":
      return <JSONFlattenCard config={config} onChange={onChange} />
    case "array_expand":
      return <ArrayExpandCard config={config} onChange={onChange} />
    default:
      return (
        <div className="text-xs font-mono bg-zinc-100 dark:bg-zinc-800 p-2 rounded mt-2 whitespace-pre-wrap">
          {JSON.stringify(config, null, 2)}
        </div>
      )
  }
}

// TRANSFORM_TYPE_LABELS / transformTypeLabel / TRANSFORM_TYPE_BLURBS now live in
// @/lib/transform-display so the post-creation Transforms tab renders identically.

// The single column name a transform targets, for the compact row label.
function transformColumn(t: TransformSuggestion): string {
  const c = (t.config as Record<string, unknown> | undefined)?.column
  return typeof c === "string" && c ? c : t.summary || t.type
}

export function SuggestionsReviewDialog({
  isOpen,
  onClose,
  pipelineId,
  executionId,
  sourceConnectionId,
  selectedTables,
  onApplyContinue,
  prefetchedSchema,
}: SuggestionsReviewDialogProps) {
  const [loading, setLoading] = useState(false)
  const [loadStep, setLoadStep] = useState<"schema" | "suggestions">("schema")
  const [retryCount, setRetryCount] = useState(0)
  const [error, setError] = useState<string | null>(null)
  const [suggestions, setSuggestions] = useState<SuggestionsResponse | null>(null)
  const [schema, setSchema] = useState<TableMetadata[]>(prefetchedSchema ?? [])
  
  // Transform toggles and edits
  const [enabledPII, setEnabledPII] = useState<Record<string, boolean>>({})
  const [enabledTransforms, setEnabledTransforms] = useState<Record<number, boolean>>({})
  const [piiActions, setPiiActions] = useState<Record<string, string>>({}) // column -> action
  // Per-transform user edits (keyed by suggestion index) and per-PII-column hash
  // algorithm selection, merged into the transform config at build time (P4).
  const [editedConfigs, setEditedConfigs] = useState<Record<number, Record<string, unknown>>>({})
  const [piiHashFunctions, setPiiHashFunctions] = useState<Record<string, string>>({}) // column -> hash_function
  
  // Preview state
  const [previewOpen, setPreviewOpen] = useState(false)
  const [previewTable, setPreviewTable] = useState<string>("")
  const [previewData, setPreviewData] = useState<{
    original: any[]
    transformed: any[]
    stats: any
    warnings: string[]
  } | null>(null)
  const [previewLoading, setPreviewLoading] = useState(false)
  
  const [submitting, setSubmitting] = useState(false)
  // Skipping/closing triggers an async pipeline-resume in the parent that takes a
  // few seconds before it unmounts this dialog. Track it so the Skip button shows
  // an immediate spinner + disables the actions — otherwise the click looks dead.
  const [closing, setClosing] = useState(false)
  // Apply and Skip both resume the SAME parked node. Keeping Skip clickable during
  // Apply (the escape hatch) opens a double-submit window, so this ref claims the
  // single resume: whichever fires first sets it, and the other aborts — preventing
  // two conflicting resume signals (transforms vs. skip) racing to the backend.
  const committedRef = useRef(false)

  // Transform-tab view state. Cards default to COLLAPSED so a long list (e.g.
  // 37 json_flatten suggestions) reads as a few grouped rows, not a wall of
  // config editors. expandedRows tracks which individual configs the user has
  // opened; collapsedGroups tracks which type-groups are folded shut.
  const [expandedRows, setExpandedRows] = useState<Record<number, boolean>>({})
  const [collapsedGroups, setCollapsedGroups] = useState<Record<string, boolean>>({})

  // Group transforms by type while preserving each suggestion's ORIGINAL index
  // (enabledTransforms/editedConfigs are keyed by that index, so it must survive
  // grouping). Order of first appearance is kept for stable rendering.
  const transformGroups = useMemo(() => {
    const order: string[] = []
    const byType: Record<string, Array<{ transform: TransformSuggestion; idx: number }>> = {}
    const list = suggestions?.transforms || []
    for (let i = 0; i < list.length; i++) {
      const t = list[i]
      const key = t.type || "other"
      if (!byType[key]) {
        byType[key] = []
        order.push(key)
      }
      byType[key].push({ transform: t, idx: i })
    }
    return order.map((type) => ({ type, items: byType[type] }))
  }, [suggestions])

  function normalizeValue(v: unknown): string {
    if (v == null) return ""
    if (typeof v === "string") return v
    if (typeof v === "number" || typeof v === "boolean") return String(v)
    try {
      return JSON.stringify(v)
    } catch {
      return String(v)
    }
  }

  function inferColumnsForPreview(table: string, original: any[], transformed: any[]): string[] {
    // Prefer schema-defined column ordering if available.
    const lower = String(table || "").toLowerCase()
    const schemaMatch =
      schema.find((t) => `${t.schema ? `${t.schema}.` : ""}${t.name}`.toLowerCase() === lower) ||
      schema.find((t) => String(t.name || "").toLowerCase() === lower) ||
      null
    const colsFromSchema = schemaMatch?.columns?.map((c) => c.name).filter(Boolean) || []

    const keys = new Set<string>()
    for (const c of colsFromSchema) keys.add(c)
    const addKeys = (rows: any[]) => {
      for (const r of rows || []) {
        if (!r || typeof r !== "object") continue
        for (const k of Object.keys(r)) keys.add(k)
      }
    }
    addKeys((original || []).slice(0, 10))
    addKeys((transformed || []).slice(0, 10))

    return Array.from(keys)
  }

  function PreviewGrid(props: { title: string; columns: string[]; rows: any[]; compareTo?: any[] }) {
    const { title, columns, rows, compareTo } = props
    const limited = (rows || []).slice(0, 10)
    return (
      <Card>
        <CardHeader>
          <CardTitle className="text-sm">{title}</CardTitle>
        </CardHeader>
        <CardContent>
          {limited.length === 0 ? (
            <div className="text-sm text-zinc-600 dark:text-zinc-400">No rows to display</div>
          ) : (
            <div className="overflow-auto max-h-96 border border-zinc-200 dark:border-zinc-800 rounded">
              <table className="min-w-full text-xs">
                <thead className="sticky top-0 bg-white dark:bg-zinc-950">
                  <tr>
                    {columns.map((c) => (
                      <th
                        key={c}
                        className="text-left font-medium px-3 py-2 border-b border-zinc-200 dark:border-zinc-800 whitespace-nowrap"
                      >
                        {c}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {limited.map((row, idx) => {
                    const base = compareTo && Array.isArray(compareTo) ? compareTo[idx] : undefined
                    return (
                      <tr key={idx} className="border-b border-zinc-100 dark:border-zinc-900">
                        {columns.map((c) => {
                          const v = row?.[c]
                          const bv = base?.[c]
                          const changed = base !== undefined && normalizeValue(v) !== normalizeValue(bv)
                          return (
                            <td
                              key={c}
                              className={`px-3 py-2 align-top whitespace-nowrap ${
                                changed ? "bg-amber-50 dark:bg-amber-950/20" : ""
                              }`}
                            >
                              <span className="font-mono">{normalizeValue(v)}</span>
                            </td>
                          )
                        })}
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>
          )}
        </CardContent>
      </Card>
    )
  }

  // Fetch schema and generate suggestions when dialog opens (or retryCount increments).
  // retryCount in deps means clicking Retry re-runs this effect without closing/reopening.
  useEffect(() => {
    if (!isOpen) {
      setSuggestions(null)
      setSchema(prefetchedSchema ?? [])
      setError(null)
      setEnabledPII({})
      setEnabledTransforms({})
      setPiiActions({})
      setEditedConfigs({})
      setPiiHashFunctions({})
      setPreviewOpen(false)
      setPreviewData(null)
      setClosing(false)
      committedRef.current = false
      return
    }

    const fetchAndGenerate = async () => {
      setLoading(true)
      setError(null)

      try {
        // Step 1: schema — skip if parent already fetched it.
        let tables: TableMetadata[] = prefetchedSchema ?? schema
        if (tables.length === 0) {
          setLoadStep("schema")
          const tablesParam = selectedTables.join(",")
          const schemaRes = await authFetch(
            `${API_ENDPOINTS.CONNECTIONS.GET(sourceConnectionId)}/metadata?tables=${encodeURIComponent(
              tablesParam
            )}&include_columns=true`,
            { cache: "no-store" }
          )
          if (!schemaRes.ok) throw new Error("Failed to fetch schema")
          const schemaData = await schemaRes.json()
          tables = schemaData.tables || []
          setSchema(tables)
        }

        if (tables.length === 0) {
          throw new Error("No schema information available for selected tables")
        }

        // Step 2: AI suggestions
        setLoadStep("suggestions")
        const suggestionsData = await generateSuggestions({
          schema: {
            tables: tables.map((t) => ({
              name: t.name,
              columns: t.columns.map((c) => ({
                name: c.name,
                type: c.type,
                nullable: c.nullable,
                description: "",
              })),
            })),
          },
          intent: {
            use_case: "sync",
            description: "Data pipeline sync",
            source_type: "database",
            destination_type: "storage",
          },
        })

        setSuggestions(suggestionsData)

        const piiEnabled: Record<string, boolean> = {}
        const piiActs: Record<string, string> = {}
        for (const pii of suggestionsData.pii_columns || []) {
          piiEnabled[pii.column] = true
          piiActs[pii.column] = pii.suggested_action || "mask"
        }
        setEnabledPII(piiEnabled)
        setPiiActions(piiActs)

        const transformEnabled: Record<number, boolean> = {}
        for (let i = 0; i < (suggestionsData.transforms || []).length; i++) {
          transformEnabled[i] = true
        }
        setEnabledTransforms(transformEnabled)
      } catch (err: any) {
        setError(err?.message || "Failed to generate suggestions")
      } finally {
        setLoading(false)
      }
    }

    void fetchAndGenerate()
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isOpen, sourceConnectionId, selectedTables, retryCount])

  const buildActiveTransforms = useCallback((): TransformSuggestion[] => {
    const transforms: TransformSuggestion[] = []

    if (!suggestions) return transforms

    // PII transforms
    for (const pii of suggestions.pii_columns || []) {
      if (enabledPII[pii.column]) {
        const action = piiActions[pii.column] || pii.suggested_action || "mask"
        // "none" means user explicitly chose to keep the field as-is — skip transform
        if (action === "none") continue
        const maskType = action === "mask" ? "hash" : action
        const config: Record<string, unknown> = {
          column: pii.column,
          mask_type: maskType,
          pii_type: pii.pii_types?.join(",") || "unknown",
        }
        // Only carry a hash_function when the engine will actually hash. For
        // redact/mask the value is replaced, so the algorithm is irrelevant.
        if (maskType === "hash") {
          config.hash_function = piiHashFunctions[pii.column] || "sha256"
        }
        transforms.push({
          type: "mask_pii",
          config,
          summary: `${action.charAt(0).toUpperCase() + action.slice(1)} ${pii.column}`,
          reason: `PII detected: ${pii.pii_types?.join(", ")}`,
        })
      }
    }

    // Other transforms — merge any per-card user edits over the suggested config.
    // Default to enabled (?? true) to match the render-side default at the group
    // checkbox and per-row toggle, so a never-touched suggestion that shows as
    // CHECKED is also persisted (not silently dropped).
    for (let i = 0; i < (suggestions.transforms || []).length; i++) {
      if (enabledTransforms[i] ?? true) {
        const base = suggestions.transforms[i]
        const edits = editedConfigs[i]
        if (edits && Object.keys(edits).length > 0) {
          transforms.push({ ...base, config: { ...base.config, ...edits } })
        } else {
          transforms.push(base)
        }
      }
    }

    return transforms
  }, [suggestions, enabledPII, enabledTransforms, piiActions, editedConfigs, piiHashFunctions])

  const handlePreview = useCallback(
    async (table: string) => {
      if (!sourceConnectionId || !table) return

      const activeTransforms = buildActiveTransforms()
      if (activeTransforms.length === 0) {
        // Nothing meaningful to show — tell user to enable at least one transform.
        setPreviewTable(table)
        setPreviewOpen(true)
        setPreviewData(null)
        setPreviewLoading(false)
        return
      }

      setPreviewLoading(true)
      setPreviewTable(table)
      setPreviewOpen(true)
      setPreviewData(null)

      try {
        // Single call: backend samples the table and applies transforms server-side.
        const previewRes = await authFetch(`${API_ENDPOINTS.API_GATEWAY_URL}/api/v1/transforms/preview`, {
          method: "POST",
          body: JSON.stringify({
            transforms: activeTransforms,
            source_connection_id: sourceConnectionId,
            table,
            limit: 10,
          }),
        })

        if (!previewRes.ok) {
          throw new Error(`Preview failed (${previewRes.status})`)
        }

        const previewResult = await previewRes.json()
        const original: any[] = previewResult.original_rows || previewResult.sample_data || []
        const transformed: any[] = previewResult.preview || []
        const applied = Number(previewResult.transforms_applied || 0)
        const warnings: string[] = Array.isArray(previewResult.warnings) ? previewResult.warnings : []
        if (applied === 0 && activeTransforms.length > 0) {
          warnings.unshift("Transforms selected but none applied — check column names match your schema.")
        }
        setPreviewData({
          original,
          transformed,
          stats: {
            rows_before: original.length,
            rows_after: transformed.length,
            columns_before: Object.keys(original[0] || {}).length,
            columns_after: Object.keys(transformed[0] || {}).length,
          },
          warnings,
        })
      } catch (err: any) {
        setPreviewData({
          original: [],
          transformed: [],
          stats: { rows_before: 0, rows_after: 0, columns_before: 0, columns_after: 0 },
          warnings: [err?.message || "Preview failed"],
        })
      } finally {
        setPreviewLoading(false)
      }
    },
    [sourceConnectionId, buildActiveTransforms]
  )

  const handleApply = useCallback(async () => {
    // A resume (apply or a raced-in skip) already owns this node — don't start.
    if (committedRef.current) return
    setSubmitting(true)
    setError(null)

    try {
      const transforms = buildActiveTransforms()

      // Persist transforms to pipeline for audit/future edits
      if (transforms.length > 0 && pipelineId) {
        const saveRes = await authFetch(`${API_ENDPOINTS.API_GATEWAY_URL}/api/v1/transforms/pipeline/${pipelineId}`, {
          method: "POST",
          // Bound the save so a stalled socket can't pin the "Applying…" spinner
          // forever — the catch surfaces the error and reopens the actions.
          timeoutMs: 30000,
          // Persist EACH transform as both a producer and a consumer row.
          // The modal has no access to pipeline mode (batch/CDC/hybrid): the
          // batch executor reads transform_type='producer'; the CDC sink reads
          // transform_type='consumer'. Emitting both guarantees the transform
          // runs regardless of mode (and covers hybrid). SavePipelineTransforms
          // is DELETE-then-INSERT, so re-apply is idempotent.
          body: JSON.stringify({
            transforms: transforms.flatMap((t, idx) => {
              const transform_config = {
                operation: t.type,
                ...t.config,
              }
              return [
                {
                  id: crypto.randomUUID(),
                  pipeline_id: pipelineId,
                  transform_type: "producer",
                  transform_order: idx * 2,
                  transform_config,
                  enabled: true,
                },
                {
                  id: crypto.randomUUID(),
                  pipeline_id: pipelineId,
                  transform_type: "consumer",
                  transform_order: idx * 2 + 1,
                  transform_config,
                  enabled: true,
                },
              ]
            }),
          }),
        })
        if (!saveRes.ok) {
          const body = await saveRes.json().catch(() => ({}))
          throw new Error(body?.error || `Failed to save transforms (HTTP ${saveRes.status})`)
        }
      }

      // If a Skip raced in while the save POST was in flight, it has already claimed
      // the single resume — abort so we don't fire a second, conflicting signal.
      if (committedRef.current) return
      committedRef.current = true

      if (transforms.length > 0 && pipelineId) {
        // The CDC sink caches consumer transforms for up to 30s, so changes to a
        // pipeline that is already running propagate within that window.
        toast.success("Transforms saved. Changes propagate to running pipelines within 30 seconds.")
      }

      // Resume pipeline with transforms in metadata. onApplyContinue already
      // closes this dialog (unmounts it) once the resume signal is sent, so we do
      // NOT also call onClose() here — onClose is the SKIP handler, and calling it
      // fired a spurious second (skip) resume on top of the apply.
      await onApplyContinue(transforms)
    } catch (err: any) {
      // Release the claim so the user can retry from the still-open modal.
      committedRef.current = false
      setError(err?.message || "Failed to apply transforms")
    } finally {
      setSubmitting(false)
    }
  }, [buildActiveTransforms, onApplyContinue, pipelineId])

  // Single entry point for skip / backdrop / escape / X. Awaits the parent's async
  // resume so it can (a) show a "Skipping…" spinner while it runs, and (b) stay open
  // with the user's selection intact if the resume fails. `setSubmitting(false)` keeps
  // Skip a real escape hatch even if an Apply is mid-flight — the user must always be
  // able to bail out. Guards only against a repeated skip (`closing`), not `submitting`.
  const requestClose = useCallback(async () => {
    // Already skipping, or an Apply has already committed the resume (its signal is
    // in flight) — ignore, so Skip can't fire a second, conflicting resume.
    if (closing || committedRef.current) return
    committedRef.current = true
    setSubmitting(false)
    setError(null)
    setClosing(true)
    try {
      await onClose()
      // On success the parent unmounts this dialog; nothing else to do.
    } catch (err: any) {
      // Resume failed — release the claim, reopen the actions, and surface the error.
      committedRef.current = false
      setClosing(false)
      setError(err?.message || "Failed to skip suggestions")
    }
  }, [closing, onClose])

  // ---- Transform group helpers (operate on original indices) ----
  // Tri-state group checkbox: true if all enabled, false if none, "indeterminate" otherwise.
  const groupCheckState = (items: Array<{ idx: number }>): boolean | "indeterminate" => {
    const on = items.filter((it) => enabledTransforms[it.idx] ?? true).length
    if (on === 0) return false
    if (on === items.length) return true
    return "indeterminate"
  }

  const setGroupEnabled = (items: Array<{ idx: number }>, enabled: boolean) => {
    setEnabledTransforms((prev) => {
      const next = { ...prev }
      for (const it of items) next[it.idx] = enabled
      return next
    })
  }

  // Apply one max_depth to every json_flatten suggestion in a group at once.
  const setGroupMaxDepth = (items: Array<{ idx: number }>, depth: number) => {
    setEditedConfigs((prev) => {
      const next = { ...prev }
      for (const it of items) {
        next[it.idx] = { ...(next[it.idx] || {}), max_depth: depth }
      }
      return next
    })
  }

  const totalSuggestions =
    (suggestions?.pii_columns?.length || 0) + (suggestions?.transforms?.length || 0)
  const activeSuggestions =
    Object.values(enabledPII).filter(Boolean).length +
    Object.values(enabledTransforms).filter(Boolean).length

  return (
    <>
      <Dialog open={isOpen} onOpenChange={(open) => { if (!open) void requestClose() }}>
        <DialogContent className="w-[95vw] max-w-4xl max-h-[90vh] overflow-hidden flex flex-col">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              <Sparkles className="h-5 w-5 text-violet-600" />
              AI Suggestions &amp; Transforms
            </DialogTitle>
            <DialogDescription>
              Review and customize AI-suggested transforms before continuing. These will be applied to
              your data during pipeline execution.
            </DialogDescription>
          </DialogHeader>

          {loading ? (
            <div className="space-y-4 py-2">
              <div className="flex items-center gap-3 text-sm text-zinc-500 dark:text-zinc-400 mb-2">
                <Loader2 className="h-4 w-4 animate-spin text-violet-500 shrink-0" />
                {loadStep === "schema"
                  ? "Step 1/2 — Fetching table schema…"
                  : "Step 2/2 — Analyzing for PII and optimizations…"}
              </div>
              {/* Skeleton cards — one per expected suggestion type */}
              {[1, 2, 3].map((i) => (
                <div key={i} className="rounded-xl border border-zinc-100 dark:border-zinc-800 p-4 space-y-2">
                  <div className="flex items-center gap-3">
                    <Skeleton className="h-5 w-5 rounded" />
                    <Skeleton className="h-4 w-48" />
                    <Skeleton className="h-5 w-16 ml-auto rounded-full" />
                  </div>
                  <Skeleton className="h-3 w-full" />
                  <Skeleton className="h-3 w-3/4" />
                </div>
              ))}
            </div>
          ) : error ? (
            // Error case used to render only the red message — leaving the user
            // trapped because the footer buttons live in the success branch only.
            // Surface explicit Skip and Retry actions here so the dialog is
            // never an unrecoverable dead-end.
            <div className="space-y-3">
              <div className="p-6 border border-red-200 bg-red-50 dark:bg-red-900/20 dark:border-red-800 rounded-md">
                <div className="flex items-start gap-3">
                  <AlertTriangle className="h-5 w-5 text-red-600 shrink-0" />
                  <div className="flex-1">
                    <div className="font-medium text-red-900 dark:text-red-100">
                      Failed to generate suggestions
                    </div>
                    <div className="text-sm text-red-700 dark:text-red-300 mt-1">{error}</div>
                    <div className="text-xs text-red-700/80 dark:text-red-300/80 mt-2">
                      You can continue without AI-generated transforms — the pipeline will sync the
                      raw schema as-is. You can always add transforms later.
                    </div>
                  </div>
                </div>
              </div>
              <div className="flex items-center justify-end gap-2">
                <Button
                  variant="outline"
                  onClick={() => {
                    // Retry: reset state and re-fetch. Never blocked by
                    // a stuck `submitting` flag — that flag's only purpose
                    // is preventing double-submit on the primary action.
                    setError(null)
                    setSubmitting(false)
                    setSchema(prefetchedSchema ?? [])
                    setRetryCount((n) => n + 1)
                  }}
                >
                  Retry
                </Button>
                <Button
                  variant="outline"
                  onClick={async () => {
                    // Continue without transforms. Guard against double-
                    // click via the in-handler check, NOT the disabled
                    // prop — a hung onApplyContinue would otherwise leave
                    // the user with no escape (no clickable Cancel either).
                    if (submitting) return
                    setSubmitting(true)
                    try {
                      // onApplyContinue already dismisses this dialog. Do NOT also
                      // call onClose() — that's the SKIP handler and would fire a
                      // spurious second resume on top of the continue.
                      await onApplyContinue([])
                    } catch (e) {
                      // Reset so the user can retry or cancel.
                      setSubmitting(false)
                    }
                  }}
                >
                  Continue without transforms
                </Button>
                <Button
                  variant="ghost"
                  onClick={() => {
                    // Cancel is an escape hatch — always clickable.
                    // Force-resetting `submitting` here means a hung
                    // primary-action call doesn't trap the user.
                    setSubmitting(false)
                    onClose()
                  }}
                >
                  Cancel
                </Button>
              </div>
            </div>
          ) : (
            <>
              <div className="flex items-center justify-between px-1 py-2">
                <div className="text-sm text-zinc-600 dark:text-zinc-400">
                  <span className="font-medium text-foreground">{activeSuggestions}</span> of{" "}
                  {totalSuggestions} suggestions will be applied
                  {selectedTables.length > 0 && (
                    <span className="text-muted-foreground">
                      {" "}
                      to {selectedTables.length} table{selectedTables.length > 1 ? "s" : ""}
                    </span>
                  )}
                </div>
                {selectedTables.length > 0 && (
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => handlePreview(selectedTables[0])}
                  >
                    <Eye className="h-4 w-4 mr-2" />
                    Preview
                  </Button>
                )}
              </div>

              {/*
                Partial-success banner. The backend now distinguishes between
                a hard failure (no usable suggestions → caught by the error
                branch above) and a degraded response (e.g. LLM transform
                call timed out but PII/optimization heuristics still ran).
                For the degraded case we show an informational amber banner
                so the user understands why the Transforms tab might be
                empty and can choose to continue with the remaining
                suggestions instead of staring at a generic blank state.
              */}
              {suggestions?.degraded && suggestions?.error && (
                <div className="mx-1 mb-2 px-3 py-2 text-sm border border-amber-200 bg-amber-50 dark:bg-amber-900/20 dark:border-amber-800 rounded-md text-amber-900 dark:text-amber-100 flex items-start gap-2">
                  <Info className="h-4 w-4 mt-0.5 shrink-0 text-amber-600" />
                  <span>{suggestions.error}</span>
                </div>
              )}

              {/*
                Scroll model: the Tabs container is the `flex-1 min-h-0` child of
                the DialogContent flex column, so it's bounded by `max-h-[90vh]`.
                The TabsList is `shrink-0` (always pinned/visible), and ONLY the
                content sits in a native `overflow-y-auto` region. We use a plain
                overflow container instead of Radix ScrollArea here because the
                ScrollArea viewport did not reliably resolve its height inside the
                flex column, leaving long lists (PII / 40+ transforms) unscrollable.
                Native overflow with `flex-1 min-h-0` is bulletproof and keeps the
                tab headers and the Skip/Apply footer fixed while the list scrolls.
              */}
              <Tabs defaultValue="pii" className="flex-1 min-h-0 flex flex-col w-full">
                <TabsList className="grid w-full grid-cols-3 shrink-0">
                    <TabsTrigger value="pii">
                      <Shield className="h-4 w-4 mr-2" />
                      PII ({suggestions?.pii_columns?.length || 0})
                    </TabsTrigger>
                    <TabsTrigger value="transforms">
                      <Zap className="h-4 w-4 mr-2" />
                      Transforms ({suggestions?.transforms?.length || 0})
                    </TabsTrigger>
                    <TabsTrigger value="optimizations">
                      <Sparkles className="h-4 w-4 mr-2" />
                      Optimizations ({suggestions?.optimizations?.length || 0})
                    </TabsTrigger>
                  </TabsList>

                <div className="flex-1 min-h-0 overflow-y-auto pr-3">
                  <TabsContent value="pii" className="space-y-3 mt-4">
                    {!suggestions?.pii_detected || (suggestions.pii_columns || []).length === 0 ? (
                      <Card className="bg-green-50 border-green-200 dark:bg-green-950/20 dark:border-green-800">
                        <CardContent className="pt-6">
                          <div className="flex items-center gap-3">
                            <CheckCircle2 className="h-5 w-5 text-green-600 dark:text-green-400" />
                            <div className="text-sm text-green-800 dark:text-green-200">
                              No PII detected in selected tables
                            </div>
                          </div>
                        </CardContent>
                      </Card>
                    ) : (
                      (suggestions.pii_columns || []).map((pii, idx) => (
                        <Card key={idx} className="border-amber-200 dark:border-amber-800">
                          <CardHeader className="pb-3">
                            <div className="flex items-start justify-between">
                              <div className="flex items-center gap-3">
                                <Checkbox
                                  checked={enabledPII[pii.column] ?? true}
                                  onCheckedChange={(checked) =>
                                    setEnabledPII((prev) => ({ ...prev, [pii.column]: !!checked }))
                                  }
                                />
                                <div>
                                  <CardTitle className="text-sm font-medium">
                                    {pii.column.includes(".") ? (
                                      <>
                                        <span className="text-muted-foreground font-normal">
                                          {pii.column.slice(0, pii.column.lastIndexOf("."))}.
                                        </span>
                                        <span className="font-semibold">
                                          {pii.column.slice(pii.column.lastIndexOf(".") + 1)}
                                        </span>
                                      </>
                                    ) : (
                                      pii.column
                                    )}
                                  </CardTitle>
                                  <div className="flex gap-2 mt-1">
                                    {(pii.pii_types || []).map((type, i) => (
                                      <Badge key={i} variant="secondary" className="text-xs">
                                        {type}
                                      </Badge>
                                    ))}
                                    <Badge
                                      variant={
                                        pii.confidence === "high"
                                          ? "default"
                                          : pii.confidence === "medium"
                                          ? "secondary"
                                          : "outline"
                                      }
                                      className="text-xs"
                                    >
                                      {pii.confidence} confidence
                                    </Badge>
                                  </div>
                                </div>
                              </div>
                              {enabledPII[pii.column] ? (
                                <div className="flex flex-col gap-2 items-end">
                                  <select
                                    className="h-10 w-36 rounded-md border border-input bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-2"
                                    value={piiActions[pii.column] || pii.suggested_action || "mask"}
                                    onChange={(e) =>
                                      setPiiActions((prev) => ({ ...prev, [pii.column]: e.target.value }))
                                    }
                                  >
                                    <option value="none">Keep original</option>
                                    <option value="mask">Mask</option>
                                    <option value="hash">Hash</option>
                                    <option value="redact">Redact</option>
                                    <option value="review">Review</option>
                                  </select>
                                  {/* Hash algorithm selector — only meaningful when the action
                                      hashes the value. "mask" maps to mask_type=hash at build time,
                                      so show the selector for both "mask" and "hash". */}
                                  {(() => {
                                    const act = piiActions[pii.column] || pii.suggested_action || "mask"
                                    return act === "hash" || act === "mask"
                                  })() && (
                                    <select
                                      className="h-9 w-36 rounded-md border border-input bg-background px-3 py-1.5 text-xs focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-2"
                                      value={piiHashFunctions[pii.column] || "sha256"}
                                      onChange={(e) =>
                                        setPiiHashFunctions((prev) => ({ ...prev, [pii.column]: e.target.value }))
                                      }
                                      title="Hash algorithm"
                                    >
                                      <option value="sha256">SHA-256 (recommended)</option>
                                      <option value="hmac_sha256">HMAC-SHA256 (keyed)</option>
                                      <option value="md5">MD5 (legacy)</option>
                                    </select>
                                  )}
                                </div>
                              ) : (
                                <span className="text-xs text-muted-foreground italic px-2">Not applying</span>
                              )}
                            </div>
                          </CardHeader>
                        </Card>
                      ))
                    )}
                  </TabsContent>

                  <TabsContent value="transforms" className="space-y-3 mt-4">
                    {(suggestions?.transforms || []).length > 0 && (
                      <div className="flex items-start gap-2 rounded-md border border-teal-200 dark:border-teal-900/60 bg-teal-50 dark:bg-teal-950/30 px-3 py-2.5">
                        <Sparkles className="h-4 w-4 text-teal-600 dark:text-teal-400 mt-0.5 shrink-0" />
                        <p className="text-xs text-teal-800 dark:text-teal-200">
                          We auto-selected{" "}
                          <span className="font-medium">{(suggestions?.transforms || []).length} transforms</span>{" "}
                          from your schema. These are safe defaults — uncheck anything you don&apos;t want, then continue.
                        </p>
                      </div>
                    )}
                    {(suggestions?.transforms || []).length === 0 ? (
                      <Card>
                        <CardContent className="pt-6">
                          <div className="text-sm text-zinc-600 dark:text-zinc-400 text-center">
                            No additional transforms suggested
                          </div>
                        </CardContent>
                      </Card>
                    ) : (
                      // Grouped by transform type. Each group is a folder: a header
                      // (tri-state select-all + count + bulk controls) over a list of
                      // compact rows that expand to the per-column config on demand.
                      transformGroups.map((group) => {
                        const folded = collapsedGroups[group.type] ?? false
                        const checkState = groupCheckState(group.items)
                        const isFlatten = group.type === "json_flatten"
                        // Surface the dominant bulk depth (empty if items disagree).
                        const depths = new Set(
                          group.items.map((it) =>
                            String(
                              (editedConfigs[it.idx]?.max_depth ??
                                (it.transform.config as Record<string, unknown>)?.max_depth ??
                                2) as number
                            )
                          )
                        )
                        const bulkDepth = depths.size === 1 ? [...depths][0] : ""
                        return (
                          <Card key={group.type} className="overflow-hidden">
                            {/* Group header */}
                            <div className="flex items-center gap-3 px-3 py-2.5 bg-zinc-50 dark:bg-zinc-900/40 border-b border-zinc-100 dark:border-zinc-800">
                              <Checkbox
                                checked={checkState}
                                onCheckedChange={(checked) => setGroupEnabled(group.items, !!checked)}
                                aria-label={`Toggle all ${transformTypeLabel(group.type)}`}
                              />
                              <button
                                type="button"
                                onClick={() =>
                                  setCollapsedGroups((prev) => ({ ...prev, [group.type]: !folded }))
                                }
                                className="flex items-center gap-1.5 flex-1 min-w-0 text-left"
                              >
                                {folded ? (
                                  <ChevronRight className="h-4 w-4 shrink-0 text-muted-foreground" />
                                ) : (
                                  <ChevronDown className="h-4 w-4 shrink-0 text-muted-foreground" />
                                )}
                                <span className="text-sm font-medium truncate">
                                  {transformTypeLabel(group.type)}
                                </span>
                                <Badge variant="secondary" className="text-xs shrink-0">
                                  {group.items.length}
                                </Badge>
                              </button>
                              {/* Bulk depth control for json_flatten — one setting, whole group */}
                              {isFlatten && (
                                <div className="flex items-center gap-1.5 shrink-0">
                                  <Label className="text-xs text-muted-foreground hidden sm:inline">
                                    Depth
                                  </Label>
                                  <select
                                    className="h-8 rounded-md border border-input bg-background px-2 text-xs"
                                    value={bulkDepth}
                                    onChange={(e) =>
                                      setGroupMaxDepth(group.items, Number(e.target.value))
                                    }
                                  >
                                    {bulkDepth === "" && <option value="">Mixed</option>}
                                    <option value="1">1</option>
                                    <option value="2">2</option>
                                    <option value="3">3</option>
                                    <option value="0">All</option>
                                  </select>
                                </div>
                              )}
                            </div>

                            {!folded && (
                              <>
                                {TRANSFORM_TYPE_BLURBS[group.type] && (
                                  <div className="px-3 pt-2 text-xs text-muted-foreground">
                                    {TRANSFORM_TYPE_BLURBS[group.type]}
                                  </div>
                                )}
                                <div className="divide-y divide-zinc-100 dark:divide-zinc-800">
                                  {group.items.map(({ transform, idx }) => {
                                    const rowOpen = expandedRows[idx] ?? false
                                    const enabled = enabledTransforms[idx] ?? true
                                    return (
                                      <div key={idx} className="px-3 py-2">
                                        <div className="flex items-center gap-3">
                                          <Checkbox
                                            checked={enabled}
                                            onCheckedChange={(checked) =>
                                              setEnabledTransforms((prev) => ({
                                                ...prev,
                                                [idx]: !!checked,
                                              }))
                                            }
                                          />
                                          <span
                                            className={`text-xs font-mono flex-1 min-w-0 truncate ${
                                              enabled ? "" : "text-muted-foreground line-through"
                                            }`}
                                            title={transformColumn(transform)}
                                          >
                                            {transformColumn(transform)}
                                          </span>
                                          <button
                                            type="button"
                                            onClick={() =>
                                              setExpandedRows((prev) => ({ ...prev, [idx]: !rowOpen }))
                                            }
                                            className="flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground shrink-0"
                                          >
                                            <Settings2 className="h-3.5 w-3.5" />
                                            <span className="hidden sm:inline">
                                              {rowOpen ? "Hide" : "Edit"}
                                            </span>
                                          </button>
                                        </div>
                                        {rowOpen && (
                                          <div className="pl-7 pt-1">
                                            <TransformConfigEditor
                                              transform={transform}
                                              config={{
                                                ...(transform.config || {}),
                                                ...(editedConfigs[idx] || {}),
                                              }}
                                              onChange={(patch) =>
                                                setEditedConfigs((prev) => ({
                                                  ...prev,
                                                  [idx]: { ...(prev[idx] || {}), ...patch },
                                                }))
                                              }
                                            />
                                          </div>
                                        )}
                                      </div>
                                    )
                                  })}
                                </div>
                              </>
                            )}
                          </Card>
                        )
                      })
                    )}
                  </TabsContent>

                  <TabsContent value="optimizations" className="space-y-3 mt-4">
                    {(suggestions?.optimizations || []).length === 0 ? (
                      <Card>
                        <CardContent className="pt-6">
                          <div className="text-sm text-zinc-600 dark:text-zinc-400 text-center">
                            No optimization suggestions
                          </div>
                        </CardContent>
                      </Card>
                    ) : (
                      (suggestions?.optimizations || []).map((opt, idx) => (
                        <Card key={idx} className="border-blue-200 dark:border-blue-800">
                          <CardContent className="pt-4">
                            <div className="flex items-start gap-3">
                              <Sparkles className="h-4 w-4 text-blue-600 dark:text-blue-400 mt-1" />
                              <div className="flex-1">
                                <div className="text-sm font-medium">
                                  {opt.description || opt.suggestion || opt.type}
                                </div>
                                {(opt.reason || opt.impact) && (
                                  <div className="text-xs text-zinc-600 dark:text-zinc-400 mt-1">
                                    {opt.reason || opt.impact}
                                  </div>
                                )}
                              </div>
                            </div>
                          </CardContent>
                        </Card>
                      ))
                    )}
                  </TabsContent>
                </div>
              </Tabs>

              {error && (
                <div className="px-1 py-2 text-sm text-red-600 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-md">
                  {error}
                </div>
              )}

              <DialogFooter className="gap-2">
                <Button
                  variant="outline"
                  onClick={requestClose}
                  // Only disabled while a skip is already in flight — NOT during Apply,
                  // so Skip stays a usable escape hatch if an Apply request hangs.
                  disabled={closing}
                >
                  {closing ? (
                    <>
                      <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                      Skipping…
                    </>
                  ) : (
                    "Skip suggestions"
                  )}
                </Button>
                <Button onClick={handleApply} disabled={submitting || closing || loading}>
                  {submitting ? (
                    <>
                      <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                      Applying...
                    </>
                  ) : (
                    <>Apply &amp; Continue ({activeSuggestions})</>
                  )}
                </Button>
              </DialogFooter>
            </>
          )}
        </DialogContent>
      </Dialog>

      {/* Preview Dialog */}
      <Dialog open={previewOpen} onOpenChange={setPreviewOpen}>
        <DialogContent className="w-[95vw] max-w-6xl max-h-[90vh] overflow-hidden flex flex-col">
          <DialogHeader>
            <DialogTitle>Transform Preview: {previewTable}</DialogTitle>
            <DialogDescription>
              Showing up to 10 rows from {previewTable} with active transforms applied
            </DialogDescription>
          </DialogHeader>

          {previewLoading ? (
            <div className="flex items-center justify-center py-12">
              <Loader2 className="h-6 w-6 animate-spin text-violet-600" />
              <span className="ml-3 text-sm text-zinc-500">Fetching sample rows and applying transforms…</span>
            </div>
          ) : !previewLoading && previewOpen && previewData === null ? (
            <div className="flex flex-col items-center justify-center py-12 gap-3 text-center">
              <Info className="h-8 w-8 text-zinc-400" />
              <div>
                <div className="font-medium text-zinc-700 dark:text-zinc-200">No transforms selected</div>
                <div className="text-sm text-zinc-500 mt-1">
                  Enable at least one PII mask or transform to preview before/after rows.
                </div>
              </div>
            </div>
          ) : previewData ? (
            <ScrollArea className="flex-1">
              <div className="space-y-4">
                {/* Stats */}
                <Card>
                  <CardContent className="pt-4">
                    <div className="grid grid-cols-4 gap-4 text-sm">
                      <div>
                        <div className="text-zinc-500">Rows</div>
                        <div className="font-medium">
                          {previewData.stats.rows_before} → {previewData.stats.rows_after}
                        </div>
                      </div>
                      <div>
                        <div className="text-zinc-500">Columns</div>
                        <div className="font-medium">
                          {previewData.stats.columns_before} → {previewData.stats.columns_after}
                        </div>
                      </div>
                      <div className="col-span-2">
                        {previewData.warnings.length > 0 && (
                          <div className="text-amber-600 dark:text-amber-400 text-xs">
                            {previewData.warnings.join(", ")}
                          </div>
                        )}
                      </div>
                    </div>
                  </CardContent>
                </Card>

                {/* Data comparison */}
                <div className="grid grid-cols-2 gap-4">
                  {(() => {
                    const cols = inferColumnsForPreview(previewTable, previewData.original, previewData.transformed)
                    return (
                      <>
                        <PreviewGrid title="Original" columns={cols} rows={previewData.original} compareTo={previewData.transformed} />
                        <PreviewGrid title="Transformed" columns={cols} rows={previewData.transformed} compareTo={previewData.original} />
                      </>
                    )
                  })()}
                </div>
              </div>
            </ScrollArea>
          ) : null}

          <DialogFooter>
            <Button variant="outline" onClick={() => setPreviewOpen(false)}>
              Close
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  )
}
