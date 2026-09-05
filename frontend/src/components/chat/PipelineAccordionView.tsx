/**
 * Accordion-based Pipeline Progress View
 * 
 * Generic execution_plan-driven UI that renders chronologically:
 * - User input at top
 * - Steps flow downward
 * - Current step at bottom (expanded + highlighted)
 * - Completed steps collapsed and de-emphasized
 * - Failed steps expanded and highlighted with error
 */

'use client'

import { useEffect, useMemo, useRef, useState } from 'react'
import Link from 'next/link'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Progress } from '@/components/ui/progress'
import { Button } from '@/components/ui/button'
import { Alert, AlertTitle, AlertDescription } from '@/components/ui/alert'
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from '@/components/ui/accordion'
import { 
  Loader2, 
  CheckCircle2, 
  XCircle, 
  Clock, 
  AlertCircle,
  RefreshCw,
  StopCircle,
  ChevronDown
} from 'lucide-react'
import { cn } from '@/lib/utils'
import { extractErrorMessage } from '@/lib/pipeline/errorUtils'
import { GenericHitlForm, type JsonSchema } from './GenericHitlForm'
import { getPipelineCDCStatus, restartPipelineCDC } from '@/lib/api/pipelines'
import { ConnectionLogo } from '@/components/connectors/ConnectionLogo'
import { displayConnectorName } from '@/lib/connector-display'
import { usePipelineRuntime, type RuntimeDep, type RuntimeHealth } from '@/lib/hooks/usePipelineRuntime'
import { authFetch } from '@/lib/api/auth-fetch'
import { API_ENDPOINTS } from '@/lib/config/api'
import { stageDurationMs } from '@/components/pipeline/dagHelpers'

// Interface matching backend PipelineState shape
export interface PipelineState {
  schema_version?: number
  pipeline_id: string
  execution_id?: string
  status: string // processing, waiting_for_user, completed, failed
  current_stage?: string
  stage_group?: string
  state?: string
  started_at?: string
  last_heartbeat_at?: string
  duration_ms?: number
  attempt?: number
  max_attempts?: number
  summary?: string
  progress: {
    percent: number
    current_step?: number
    total_steps?: number
    stage?: string
  }
  blocking_reason?: {
    type: string
    description: string
    estimated_seconds?: number
    details?: Record<string, unknown>
  }
  message?: string
  execution_plan?: {
    stages: Array<{
      id: string
      display_name: string
      group?: string
      order?: number
      status: 'pending' | 'running' | 'waiting' | 'complete' | 'failed'
      progress?: number
      started_at?: string
      completed_at?: string
      actual_duration_ms?: number
      /** SECONDS, legacy — see the note on ExecutionPlanStage in DAGVisualization. */
      actual_duration?: number
      estimated_duration?: number
      error_message?: string
      metadata?: Record<string, unknown>
    }>
  }
  metadata?: Record<string, unknown>
  created_at: string
  updated_at: string
  // New staleness fields
  is_stale?: boolean
  stale_reason?: string
  stale_elapsed_seconds?: number
  cancel_recommended?: boolean
  // New error field
  error_message?: string
}

interface PipelineAccordionViewProps {
  state: PipelineState
  userIntent?: string
  onRefresh?: () => void
  onCancel?: () => void
  onSelectTables?: (details?: Record<string, unknown>) => void
  onConfigureConnections?: (details?: Record<string, unknown>) => void
  onGenerateConnectors?: (details?: Record<string, unknown>) => void
  onViewMonitoring?: () => void
  // Set to true briefly after user responds to HITL to show "Resuming…" instead of
  // the stale blocking card while Temporal processes the signal (~5-7s lag).
  resumingFromHITL?: boolean
}

function formatDuration(ms: number | undefined): string | null {
  if (typeof ms !== 'number' || ms < 0) return null
  if (ms < 1000) return `${Math.round(ms)}ms`
  const sec = Math.floor(ms / 1000)
  if (sec < 60) return `${sec}s`
  const min = Math.floor(sec / 60)
  const rem = sec % 60
  return `${min}m ${rem}s`
}

function elapsedSince(iso?: string): string | null {
  if (!iso) return null
  const t = new Date(iso).getTime()
  if (Number.isNaN(t)) return null
  const ms = Math.max(0, Date.now() - t)
  return formatDuration(ms)
}

function formatAgo(iso?: string): string | null {
  if (!iso) return null
  const t = new Date(iso).getTime()
  if (isNaN(t)) return null
  const diffSec = Math.max(0, Math.floor((Date.now() - t) / 1000))
  if (diffSec < 5) return 'just now'
  if (diffSec < 60) return `${diffSec}s ago`
  const diffMin = Math.floor(diffSec / 60)
  if (diffMin < 60) return `${diffMin}m ago`
  const diffHr = Math.floor(diffMin / 60)
  return `${diffHr}h ago`
}

function StageIcon({ status }: { status: string }) {
  switch (status) {
    case 'complete':
      return <CheckCircle2 className="h-5 w-5 text-green-500" />
    case 'failed':
      return <XCircle className="h-5 w-5 text-red-500" />
    case 'running':
      return <Loader2 className="h-5 w-5 text-blue-500 animate-spin" />
    case 'waiting':
      return <Clock className="h-5 w-5 text-amber-500" />
    case 'pending':
    default:
      return <Clock className="h-5 w-5 text-gray-400" />
  }
}

function PipelineStatusBadge({ status }: { status: string }) {
  switch (status) {
    case 'completed':
      return <Badge className="bg-green-600 text-white text-xs">Completed</Badge>
    case 'failed':
      return <Badge className="bg-red-600 text-white text-xs">Failed</Badge>
    case 'processing':
      return <Badge className="bg-blue-600 text-white text-xs">Running</Badge>
    case 'waiting_for_user':
      return <Badge className="bg-amber-500 text-white text-xs">Waiting</Badge>
    case 'cancelled':
    case 'cancelling':
      return <Badge className="bg-gray-500 text-white text-xs">Cancelled</Badge>
    case 'pending':
    default:
      return <Badge className="bg-gray-400 text-white text-xs">Pending</Badge>
  }
}

function isJsonSchema(value: unknown): value is JsonSchema {
  if (!value || typeof value !== 'object') return false
  const v = value as Record<string, unknown>
  return typeof v.type === 'string'
}

type ConnectorDirection = 'source' | 'destination'
interface RequiredConnectorRow {
  direction: ConnectorDirection
  connectorType: string
  isMissing: boolean
}

function normalizeConnectorId(raw: unknown): string {
  const s = String(raw || '').trim()
  if (!s) return ''
  return s.toLowerCase().replace(/\s+/g, '-').replace(/_/g, '-')
}

function formatConnectorDisplay(id: string): string {
  const n = normalizeConnectorId(id)
  const names: Record<string, string> = {
    mysql: 'MySQL',
    postgresql: 'PostgreSQL',
    postgres: 'PostgreSQL',
    'aws-s3': 'AWS S3',
    s3: 'S3',
    snowflake: 'Snowflake',
    bigquery: 'BigQuery',
    mongodb: 'MongoDB',
  }
  return names[n] || (id ? id.charAt(0).toUpperCase() + id.slice(1) : 'Unknown')
}

function extractRequiredConnectorRows(details: Record<string, unknown>): RequiredConnectorRow[] {
  const rows: RequiredConnectorRow[] = []

  const required = (details.required_connectors && typeof details.required_connectors === 'object')
    ? (details.required_connectors as Record<string, unknown>)
    : null

  const missingDetailedRaw = details.missing_connectors_detailed
  const missingDetailed: Array<{ direction?: string; connector_type?: string }> = Array.isArray(missingDetailedRaw)
    ? (missingDetailedRaw as Array<{ direction?: string; connector_type?: string }>)
    : []

  const missingSet = new Set<string>()
  for (const m of missingDetailed) {
    if (m && typeof m.connector_type === 'string') {
      missingSet.add(normalizeConnectorId(m.connector_type))
    }
  }
  const missingListRaw = details.missing_connectors
  if (Array.isArray(missingListRaw)) {
    for (const v of (missingListRaw as unknown[])) {
      if (typeof v === 'string') missingSet.add(normalizeConnectorId(v))
    }
  }

  const sourceType = normalizeConnectorId(required?.source || details.source_connector || details.source_type || '')
  const destType = normalizeConnectorId(required?.destination || details.destination_connector || details.destination_type || '')

  if (sourceType) {
    rows.push({
      direction: 'source',
      connectorType: sourceType,
      isMissing: missingSet.has(sourceType) || missingDetailed.some((m) => m?.direction === 'source' && normalizeConnectorId(m.connector_type) === sourceType),
    })
  }
  if (destType) {
    rows.push({
      direction: 'destination',
      connectorType: destType,
      isMissing: missingSet.has(destType) || missingDetailed.some((m) => m?.direction === 'destination' && normalizeConnectorId(m.connector_type) === destType),
    })
  }

  // If we still couldn't infer directions but we have missing connectors, show them as generic rows.
  if (rows.length === 0 && missingSet.size > 0) {
    for (const id of Array.from(missingSet)) {
      rows.push({ direction: 'source', connectorType: id, isMissing: true })
    }
  }

  return rows
}

function getCurrentStageId(state: PipelineState): string | null {
  const stages = state.execution_plan?.stages || []
  if (stages.length === 0) return null

  // Sort by order (stable fallback)
  const sortedStages = [...stages].sort((a, b) => (a.order || 0) - (b.order || 0))

  // 1) If current_stage exists AND matches a stage id, trust it (fast path).
  if (state.current_stage) {
    const idx = sortedStages.findIndex((s) => s.id === state.current_stage)
    if (idx !== -1) return sortedStages[idx].id
  }

  // 2) Prefer any active/failed stage from the plan itself (most reliable).
  // If multiple match, pick the last one by order (highest index).
  for (let i = sortedStages.length - 1; i >= 0; i--) {
    const st = sortedStages[i]
    if (st.status === 'running' || st.status === 'waiting' || st.status === 'failed') {
      return st.id
    }
  }

  // 3) If nothing is active yet, treat the first pending stage as the \"current\" stage.
  const firstPending = sortedStages.find((s) => s.status === 'pending')
  if (firstPending) return firstPending.id

  // 4) If all are complete, the last stage is effectively current.
  return sortedStages[sortedStages.length - 1].id
}

// infra_preflight is run by the executor worker before data transfer, NOT the planner.
// It is never added to execution_plan.stages, so we inject it as a synthetic stage
// between validator and executor when the current_stage is infra_preflight or executor.
function withInfraPreflight(
  stages: NonNullable<PipelineState['execution_plan']>['stages'],
  currentStage: string | null,
): NonNullable<PipelineState['execution_plan']>['stages'] {
  if (stages.some((s) => s.id === 'infra_preflight')) return stages // already present

  const executorIdx = stages.findIndex((s) => s.id === 'executor')
  if (executorIdx < 0) return stages // no executor stage — nothing to inject before

  const executorOrder = stages[executorIdx].order ?? executorIdx
  const prevOrder = executorIdx > 0 ? (stages[executorIdx - 1].order ?? executorIdx - 1) : executorOrder - 1
  const syntheticOrder = (prevOrder + executorOrder) / 2

  const isActive = currentStage === 'infra_preflight'
  const isPast = currentStage === 'executor' ||
    stages.some((s) => s.id === 'executor' && (s.status === 'running' || s.status === 'complete'))

  const synthetic = {
    id: 'infra_preflight',
    display_name: 'Infra Preflight',
    description: 'Starting MCP servers, Kafka Connect, and required infrastructure',
    icon: '⚙️',
    order: syntheticOrder,
    status: isActive ? 'running' : isPast ? 'complete' : 'pending',
  } as NonNullable<PipelineState['execution_plan']>['stages'][number]

  const result = [...stages]
  result.splice(executorIdx, 0, synthetic)
  return result
}

function getVisibleStages(state: PipelineState): NonNullable<PipelineState['execution_plan']>['stages'] {
  const stages = state.execution_plan?.stages || []

  const currentStageId = getCurrentStageId(state) ?? state.current_stage ?? null

  // Sort by order (stable fallback)
  const sortedStages = withInfraPreflight(
    [...stages]
      // Backward compatibility: older state projections may accidentally insert a non-plan stage id
      // "connection_validator" (agent name) in addition to the canonical plan stage "connection_validation".
      .filter((s) => s?.id !== 'connection_validator')
      .sort((a, b) => (a.order || 0) - (b.order || 0)),
    currentStageId,
  )

  // If pipeline finished, show all stages (audit trail)
  if (state.status === 'completed' || state.status === 'failed') {
    return sortedStages
  }

  // While running/waiting, show completed + current only (progressive disclosure).
  if (!currentStageId) return []

  const currentIndex = sortedStages.findIndex((s) => s.id === currentStageId)
  if (currentIndex < 0) {
    // If we can't resolve, fail-safe to only show the first stage (never show all).
    return sortedStages.slice(0, 1)
  }

  return sortedStages.slice(0, currentIndex + 1)
}

interface DisplayProgress {
  percent: number
  text: string
  showSuccessBanner: boolean
  showCDCLiveBanner: boolean
}

function getDisplayProgress(state: PipelineState, looksLikeCDC: boolean): DisplayProgress {
  // CRITICAL: Only show success if status is explicitly 'completed'
  const isCompleted = state.status === 'completed'
  const msg = String(state.message || state.summary || '').toLowerCase()
  const isLiveStreaming = state.status === 'running' && msg.includes('streaming pipeline started')

  if (isCompleted) {
    // Force 100% for completed pipelines
    if (state.progress?.percent !== 100) {
      console.warn(
        `[Pipeline ${state.pipeline_id}] Status is 'completed' but progress is ${state.progress?.percent}%. Normalizing to 100%.`
      )
    }

    // CDC pipelines: Temporal workflow exits "completed" once streaming setup is done,
    // but the stream is still live. Show CDC live banner instead of "done" banner.
    if (looksLikeCDC) {
      return { percent: 100, text: 'LIVE (streaming)', showSuccessBanner: false, showCDCLiveBanner: true }
    }

    return {
      percent: 100,
      text: '100% Complete',
      showSuccessBanner: true,
      showCDCLiveBanner: false,
    }
  }

  if (isLiveStreaming) {
    // CDC streaming pipelines are continuous; backend percent is "setup progress" only.
    return { percent: 100, text: 'LIVE (streaming)', showSuccessBanner: false, showCDCLiveBanner: true }
  }

  // For all other statuses, calculate progress
  const rawPercent = state.progress?.percent

  if (
    rawPercent == null ||
    isNaN(rawPercent) ||
    rawPercent < 0 ||
    rawPercent > 100
  ) {
    // Fallback: estimate from visible stages
    const visibleStages = getVisibleStages(state)
    const completed = visibleStages.filter((s) => s.status === 'complete').length
    const estimated =
      visibleStages.length > 0
        ? Math.round((completed / visibleStages.length) * 100)
        : 0

    console.warn(
      `[Pipeline ${state.pipeline_id}] Invalid progress (${rawPercent}). Using estimated: ${estimated}%`
    )

    return {
      percent: Math.max(0, Math.min(99, estimated)), // Never show 100% unless completed
      text: `~${estimated}% Complete`,
      showSuccessBanner: false,
      showCDCLiveBanner: false,
    }
  }

  // Valid progress, but cap at 99% to avoid confusion
  const clampedPercent = Math.max(0, Math.min(99, rawPercent))

  return {
    percent: clampedPercent,
    text: `${clampedPercent}% Complete`,
    showSuccessBanner: false,
    showCDCLiveBanner: false,
  }
}

// Lightweight per-stage activity panel — shown inside any running stage's dropdown.
// Shows elapsed time, an animated bar, and the latest pipeline event message.
function StageActivityPanel({
  pipelineId,
  stageId,
  startedAt,
  label,
  onViewMonitoring,
}: {
  pipelineId: string
  stageId: string
  startedAt?: string
  label?: string
  onViewMonitoring?: () => void
}) {
  const [elapsed, setElapsed] = useState<string>("")
  const [lastEvent, setLastEvent] = useState<string | null>(null)
  const isExecutor = stageId === 'executor'

  // Tick elapsed time every second
  useEffect(() => {
    const base = startedAt ? new Date(startedAt).getTime() : Date.now()
    const tick = () => {
      const secs = Math.max(0, Math.floor((Date.now() - base) / 1000))
      if (secs < 60) setElapsed(`${secs}s`)
      else {
        const m = Math.floor(secs / 60)
        const s = secs % 60
        setElapsed(`${m}m ${s}s`)
      }
    }
    tick()
    const t = setInterval(tick, 1000)
    return () => clearInterval(t)
  }, [startedAt])

  // Poll recent pipeline events for a live status line
  useEffect(() => {
    if (!pipelineId) return
    let cancelled = false
    const poll = async () => {
      try {
        const res = await authFetch(
          `${API_ENDPOINTS.PIPELINES.GET(pipelineId)}/events?limit=3`,
          { cache: "no-store" }
        )
        if (!res.ok || cancelled) return
        const data = await res.json()
        const events: any[] = data.events || data || []
        if (events.length > 0) {
          const ev = events[0]
          const rawMsg = ev.message || ev.description
          const eventTypeLabel: Record<string, string> = {
            STAGE_COMPLETED: 'Stage completed',
            STAGE_PROGRESS: 'Stage in progress',
            STAGE_STARTED: 'Stage started',
            STAGE_FAILED: 'Stage failed',
            PIPELINE_WAITING: 'Waiting for input',
            PIPELINE_COMPLETED: 'Pipeline completed',
            PIPELINE_FAILED: 'Pipeline failed',
            PIPELINE_STARTED: 'Pipeline started',
          }
          const msg = rawMsg || (ev.event_type ? (eventTypeLabel[ev.event_type] ?? null) : null)
          if (!cancelled && msg) setLastEvent(String(msg))
        }
      } catch { /* best-effort */ }
    }
    void poll()
    const t = setInterval(poll, isExecutor ? 5000 : 3000)
    return () => { cancelled = true; clearInterval(t) }
  }, [pipelineId, isExecutor])

  const accentColor = isExecutor ? 'violet' : 'blue'

  return (
    <div className={cn(
      "rounded-xl border p-4 space-y-3",
      isExecutor
        ? "border-violet-200 bg-violet-50/40 dark:border-violet-900/40 dark:bg-violet-950/10"
        : "border-blue-200 bg-blue-50/40 dark:border-blue-900/40 dark:bg-blue-950/10"
    )}>
      <div className="flex items-center justify-between text-sm">
        <div className={cn(
          "flex items-center gap-2 font-medium",
          isExecutor ? "text-violet-700 dark:text-violet-400" : "text-blue-700 dark:text-blue-400"
        )}>
          <span className="relative flex h-2.5 w-2.5">
            <span className={cn(
              "animate-ping absolute inline-flex h-full w-full rounded-full opacity-75",
              isExecutor ? "bg-violet-500" : "bg-blue-500"
            )} />
            <span className={cn(
              "relative inline-flex rounded-full h-2.5 w-2.5",
              isExecutor ? "bg-violet-600" : "bg-blue-600"
            )} />
          </span>
          {label ?? (isExecutor ? 'Preparing data transfer' : 'Agent working…')}
        </div>
        {elapsed && (
          <span className="text-xs text-zinc-500 tabular-nums">{elapsed} elapsed</span>
        )}
      </div>
      {/* Indeterminate animated bar */}
      <div className={cn(
        "h-1.5 w-full rounded-full overflow-hidden",
        isExecutor ? "bg-violet-100 dark:bg-violet-900/40" : "bg-blue-100 dark:bg-blue-900/40"
      )}>
        <div
          className={cn("h-full w-1/3 rounded-full", isExecutor ? "bg-violet-500" : "bg-blue-500")}
          style={{ animation: "rsync-slide 1.8s ease-in-out infinite" }}
        />
        <style>{`@keyframes rsync-slide { 0%{transform:translateX(-100%)} 100%{transform:translateX(400%)} }`}</style>
      </div>
      {lastEvent && (
        <p className="text-xs text-zinc-500 truncate">
          <span className="font-medium">Latest:</span> {lastEvent}
        </p>
      )}
      {!lastEvent && isExecutor && (
        <p className="text-xs text-zinc-500">
          Data is transferring in the background. Large tables may take several minutes.
        </p>
      )}
      {onViewMonitoring && isExecutor && (
        <button
          onClick={onViewMonitoring}
          className="text-xs font-medium text-violet-600 dark:text-violet-400 underline underline-offset-2 hover:text-violet-800"
        >
          Open Monitoring for live events →
        </button>
      )}
    </div>
  )
}

// Keep old name as alias for backward compat within this file
function LiveExecutionPanel(props: Parameters<typeof StageActivityPanel>[0]) {
  return <StageActivityPanel {...props} />
}

export function PipelineAccordionView({
  state,
  userIntent,
  onRefresh,
  onCancel,
  onSelectTables,
  onConfigureConnections,
  onGenerateConnectors,
  onViewMonitoring,
  resumingFromHITL = false,
}: PipelineAccordionViewProps) {
  // Scroll the card into view when this accordion first mounts (pipeline just started).
  // Double-rAF ensures React has committed the DOM and any parent scroll containers
  // have recalculated before we attempt scrollIntoView.
  const cardRef = useRef<HTMLDivElement>(null)
  useEffect(() => {
    requestAnimationFrame(() => {
      requestAnimationFrame(() => {
        cardRef.current?.scrollIntoView({ behavior: 'smooth', block: 'end' })
      })
    })
  }, [])

  // Normalize status defensively: some backends may lag setting `status=completed`
  // even when progress/current_stage indicates completion.
  const normalizedState = useMemo(() => {
    const raw = String(state.status || '')
    const pct = Number(state.progress?.percent)
    const stage = String(state.current_stage || '')
    const shouldForceCompleted = raw !== 'completed' && (stage === 'completed' || pct === 100)
    if (!shouldForceCompleted) return state
    return { ...state, status: 'completed' }
  }, [state])

  // Resolve the source/destination connector types from the best available data so
  // the "Understood as" strip stays visible across ALL stages — not just after the
  // backend propagates them onto top-level state.metadata (which lands late). The
  // intent/capability_resolver stages emit these into their OWN stage metadata as
  // soon as they complete, and the connector-check / missing-connector HITL stages
  // carry them in blocking-reason details (required_connectors / *_connector).
  const resolvedConnectors = useMemo(() => {
    const pick = (v: unknown) => (typeof v === 'string' && v.trim() ? v.trim() : '')
    const meta = state.metadata || {}
    let source = pick(meta.source_connector_type)
    let destination = pick(meta.destination_connector_type)

    if (!source || !destination) {
      for (const st of state.execution_plan?.stages || []) {
        const sm = (st.metadata || {}) as Record<string, unknown>
        if (!source) source = pick(sm.source_connector_type)
        if (!destination) destination = pick(sm.destination_connector_type)
        if (source && destination) break
      }
    }

    if (!source || !destination) {
      const bd = (state.blocking_reason?.details || {}) as Record<string, unknown>
      for (const r of extractRequiredConnectorRows(bd)) {
        if (!source && r.direction === 'source') source = r.connectorType
        if (!destination && r.direction === 'destination') destination = r.connectorType
      }
    }

    return { source, destination }
  }, [state.metadata, state.execution_plan, state.blocking_reason])

  // Get visible stages (progressive disclosure) — memoized to keep reference stable
  const visibleStages = useMemo(() => getVisibleStages(normalizedState), [normalizedState])

  const [expandedStages, setExpandedStages] = useState<string[]>(() => {
    return visibleStages
      // Auto-expand the active stage (running/waiting) or failed stage.
      .filter((s) => s.status === 'running' || s.status === 'waiting' || s.status === 'failed')
      .map((s) => s.id)
  })

  // Auto-follow the currently active stage: expand it, collapse the previous one.
  // This prevents stale open/close state when stages transition (the flickering/wrong-order bug).
  const activeStageId = useMemo(() => {
    return (
      visibleStages.find((s) => s.status === 'running' || s.status === 'waiting')?.id ??
      visibleStages.find((s) => s.status === 'failed')?.id ??
      null
    )
  }, [visibleStages])

  const prevActiveStageIdRef = useRef<string | null>(null)
  useEffect(() => {
    if (!activeStageId) return
    if (activeStageId === prevActiveStageIdRef.current) return
    const prev = prevActiveStageIdRef.current
    prevActiveStageIdRef.current = activeStageId
    setExpandedStages((cur) => {
      // Add the new active stage; remove the previous one only if the user hasn't
      // explicitly kept other stages open (i.e. we only auto-manage the auto-opened one).
      const withoutPrev = prev ? cur.filter((id) => id !== prev) : cur
      return withoutPrev.includes(activeStageId) ? withoutPrev : [...withoutPrev, activeStageId]
    })
  }, [activeStageId])

  // Canonical runtime view (server-computed). When available, this is the
  // single source of truth for phase/health/mode. The local heuristics below
  // remain as a fallback for older backends without /runtime, and are silently
  // overridden when runtime is present.
  const { runtime } = usePipelineRuntime(state.pipeline_id, { pollMs: 5000 })

  // CDC status + restart (demo-grade self-healing)
  const looksLikeCDCFallback = useMemo(() => {
    const meta = state.metadata || {}
    const metaSync = String(meta["sync_mode"] || meta["mode"] || '').toLowerCase()
    if (metaSync === 'cdc' || metaSync === 'streaming') return true
    const stages = state.execution_plan?.stages || []
    return stages.some((s) => {
      const hay = `${s.id} ${s.display_name} ${s.group || ''}`.toLowerCase()
      return hay.includes('cdc') || hay.includes('debezium') || hay.includes('stream')
    })
  }, [state.execution_plan?.stages, state.metadata])

  // Prefer server-computed mode when /runtime is reachable.
  const looksLikeCDC = runtime ? runtime.mode === 'cdc' : looksLikeCDCFallback

  const [cdcInfo, setCdcInfo] = useState<any | null>(null)
  const [cdcError, setCdcError] = useState<string | null>(null)
  const [cdcLoading, setCdcLoading] = useState(false)
  const [cdcRestarting, setCdcRestarting] = useState(false)

  const refreshCDC = useMemo(() => {
    return async () => {
      if (!looksLikeCDC) return
      setCdcLoading(true)
      setCdcError(null)
      try {
        const res = await getPipelineCDCStatus(state.pipeline_id)
        setCdcInfo(res)
      } catch (e: any) {
        setCdcInfo(null)
        setCdcError(e?.message || 'Failed to fetch CDC status')
      } finally {
        setCdcLoading(false)
      }
    }
  }, [looksLikeCDC, state.pipeline_id])

  useEffect(() => {
    let cancelled = false
    if (!looksLikeCDC) return

    const run = async () => {
      if (cancelled) return
      await refreshCDC()
    }

    void run()
    const t = window.setInterval(run, 15000)
    return () => {
      cancelled = true
      window.clearInterval(t)
    }
  }, [looksLikeCDC, refreshCDC])

  const cdcStatus = useMemo(() => {
    // API returns: { success, pipeline_id, connector_name, result?, error? }
    const success = Boolean(cdcInfo?.success)
    const connectorName = String(cdcInfo?.connector_name || '')
    const connectorState = String(cdcInfo?.result?.connector_state || '')
    const healthy = Boolean(cdcInfo?.result?.healthy)
    const rawErr = String(cdcInfo?.error || '')
    // "connect_unavailable" means Kafka Connect is not running — not a connector failure.
    const connectUnavailable = rawErr === 'connect_unavailable' || cdcInfo?.result?.connect_available === false
    // "not_found" during pipeline setup is expected — Debezium connector is registered at the
    // end of the workflow (step ~7/8). Suppress it as an error while setup is in progress.
    // Use runtime.phase when available; fall back to pipeline status not being terminal.
    const isLiveStreaming = runtime?.phase === 'streaming' || runtime?.phase === 'idle'
    const isTerminal = ['completed', 'failed', 'cancelled'].includes(
      String(normalizedState.status || '').toLowerCase()
    )
    const isSettingUp = !isTerminal && !isLiveStreaming
    const err = connectUnavailable || (rawErr === 'not_found' && isSettingUp) ? '' : rawErr
    const settingUp = rawErr === 'not_found' && isSettingUp
    return { success, connectorName, connectorState, healthy, err, connectUnavailable, settingUp }
  }, [cdcInfo, normalizedState.status, runtime?.phase])

  const error = extractErrorMessage({
    errorMessage: normalizedState.error_message,
    status: normalizedState.status,
    metadata: normalizedState.metadata,
    executionPlan: normalizedState.execution_plan,
  })

  const lastUpdate = formatAgo(normalizedState.last_heartbeat_at || normalizedState.updated_at)
  const localProgress = getDisplayProgress(normalizedState, looksLikeCDC)
  // When runtime is available, let it dictate which banner shows. The local
  // percent/text remain because /runtime doesn't expose stage-by-stage progress.
  const runtimePhase = runtime?.phase
  const runtimeOverridesBanners = Boolean(runtime)
  const showSuccessBanner = runtimeOverridesBanners
    ? runtimePhase === 'completed'
    : localProgress.showSuccessBanner
  // Never show the "CDC stream is live" banner while the pipeline is blocked at
  // a HITL (e.g. table-selection) — it visually replaces the table picker the
  // user needs to act on. Suppress regardless of runtime phase.
  const awaitingUserInput = normalizedState.status === 'waiting_for_user'
  const showCDCLiveBanner = !awaitingUserInput && (runtimeOverridesBanners
    ? runtimePhase === 'streaming' || runtimePhase === 'idle'
    : localProgress.showCDCLiveBanner)
  const showRuntimeFailureBanner = runtimeOverridesBanners && runtimePhase === 'failed'
  const { percent, text: progressText } = localProgress
  const blockingType = String(normalizedState.blocking_reason?.type || '')
  const blockingDetails = (normalizedState.blocking_reason?.details && typeof normalizedState.blocking_reason.details === 'object')
    ? (normalizedState.blocking_reason.details as Record<string, unknown>)
    : {}
  const requestType = String(
    (blockingDetails.request_type as string) ||
    (blockingDetails.action_needed as string) ||
    blockingType
  )

  // Server-computed guidance for when to offer cancel (keeps render pure/idempotent).
  const showCancelButton = Boolean(onCancel) && (Boolean(normalizedState.cancel_recommended) || normalizedState.status === "processing")

  return (
    <Card ref={cardRef} className="w-full animate-slideIn border-primary/20">
      <CardHeader>
        {/* ChatGPT-style intent bubble */}
        {userIntent && (
          <div className="rounded-2xl bg-gradient-to-br from-violet-600 to-indigo-600 p-4 text-white shadow-sm mb-3">
            <div className="text-xs uppercase tracking-wider text-white/90">You asked</div>
            <div className="mt-1 text-base font-medium leading-relaxed">&ldquo;{userIntent}&rdquo;</div>
          </div>
        )}

        {/* "Understood as" — AI interpretation strip. Resolved from state metadata,
            per-stage metadata, and blocking details so it stays visible in EVERY
            stage once the source/destination are known (not just late stages). */}
        {Boolean(resolvedConnectors.source || resolvedConnectors.destination) && (
          <div className="flex items-center gap-2 rounded-xl border border-violet-200 bg-violet-50 dark:bg-violet-950/30 dark:border-violet-800 px-3 py-2 mb-4 text-sm">
            <span className="text-xs text-muted-foreground shrink-0">Understood as</span>
            <div className="flex items-center gap-1.5 min-w-0">
              {Boolean(resolvedConnectors.source) && (
                <span className="flex items-center gap-1">
                  <ConnectionLogo connectorType={resolvedConnectors.source} size="sm" />
                  <span className="font-medium text-foreground truncate">
                    {displayConnectorName(resolvedConnectors.source)}
                  </span>
                </span>
              )}
              {Boolean(resolvedConnectors.source) && Boolean(resolvedConnectors.destination) && (
                <span className="text-muted-foreground mx-0.5">→</span>
              )}
              {Boolean(resolvedConnectors.destination) && (
                <span className="flex items-center gap-1">
                  <ConnectionLogo connectorType={resolvedConnectors.destination} size="sm" />
                  <span className="font-medium text-foreground truncate">
                    {displayConnectorName(resolvedConnectors.destination)}
                  </span>
                </span>
              )}
              {Boolean(state.metadata?.run_mode) && (
                <Badge variant="secondary" className="ml-2 text-xs capitalize shrink-0">
                  {String(state.metadata!.run_mode)}
                </Badge>
              )}
            </div>
          </div>
        )}
        <div className="flex items-start justify-between gap-4">
          <div className="min-w-0">
            <div className="flex items-center gap-2">
              <PipelineStatusBadge status={normalizedState.status} />
              <CardTitle className="text-lg truncate">
                Pipeline {normalizedState.pipeline_id.slice(0, 8)}
              </CardTitle>
            </div>
            <p className="text-sm text-muted-foreground mt-1">
              {normalizedState.message || normalizedState.summary || 'Processing pipeline...'}
            </p>
            <div className="flex items-center gap-3 mt-2 text-xs text-muted-foreground">
              {typeof normalizedState.progress?.current_step === 'number' && 
               typeof normalizedState.progress?.total_steps === 'number' && (
                <span>{normalizedState.progress.current_step}/{normalizedState.progress.total_steps} steps</span>
              )}
              {lastUpdate && <span>Updated {lastUpdate}</span>}
            </div>
          </div>

          <div className="flex flex-col items-end gap-2">
            <Badge variant="secondary" className="text-sm">
              {progressText}
            </Badge>
            <div className="flex items-center gap-2">
              {onRefresh && (
                <Button variant="outline" size="sm" onClick={onRefresh}>
                  <RefreshCw className="h-3 w-3 mr-1" />
                  Refresh
                </Button>
              )}
              {showCancelButton && onCancel && (
                <Button variant="outline" size="sm" onClick={onCancel}>
                  <StopCircle className="h-3 w-3 mr-1" />
                  Cancel
                </Button>
              )}
            </div>
          </div>
        </div>

        {/* Overall Progress Bar */}
        <div className="mt-4 space-y-2">
          <Progress value={percent} className="h-2" />
        </div>

        {/* CDC status / restart (only for CDC-like pipelines) */}
        {looksLikeCDC && (
          <div className="mt-4 rounded-lg border border-zinc-200 dark:border-zinc-800 p-3">
            <div className="flex items-center justify-between gap-3">
              <div className="min-w-0">
                <div className="flex items-center gap-2">
                  <Badge variant="secondary" className="text-xs">
                    CDC
                  </Badge>
                  {cdcLoading ? (
                    <span className="text-xs text-muted-foreground flex items-center gap-1">
                      <Loader2 className="h-3 w-3 animate-spin" />
                      Checking…
                    </span>
                  ) : cdcStatus.settingUp ? (
                    <span className="text-xs text-muted-foreground flex items-center gap-1">
                      <Loader2 className="h-3 w-3 animate-spin" />
                      Setting up connector…
                    </span>
                  ) : cdcStatus.connectorState ? (
                    <span className="text-xs text-muted-foreground">
                      {cdcStatus.connectorState}
                      {cdcStatus.healthy ? ' (healthy)' : ' (unhealthy)'}
                    </span>
                  ) : cdcStatus.connectUnavailable ? (
                    <span className="text-xs text-muted-foreground">Kafka Connect not running</span>
                  ) : cdcError ? (
                    <span className="text-xs text-red-600 truncate">{cdcError}</span>
                  ) : cdcStatus.err ? (
                    <span className="text-xs text-red-600 truncate">{cdcStatus.err}</span>
                  ) : (
                    <span className="text-xs text-muted-foreground">Status unavailable</span>
                  )}
                </div>
                {cdcStatus.connectorName && (
                  <div className="text-xs text-muted-foreground truncate mt-1">
                    Connector: <span className="font-mono">{cdcStatus.connectorName}</span>
                  </div>
                )}
              </div>

              <div className="flex items-center gap-2">
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => void refreshCDC()}
                  disabled={cdcLoading || cdcRestarting}
                >
                  <RefreshCw className="h-3 w-3 mr-1" />
                  Refresh
                </Button>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={async () => {
                    setCdcRestarting(true)
                    setCdcError(null)
                    try {
                      await restartPipelineCDC(state.pipeline_id)
                      await refreshCDC()
                    } catch (e: any) {
                      setCdcError(e?.message || 'Failed to restart CDC')
                    } finally {
                      setCdcRestarting(false)
                    }
                  }}
                  disabled={cdcLoading || cdcRestarting}
                >
                  {cdcRestarting ? (
                    <>
                      <Loader2 className="h-3 w-3 animate-spin mr-1" />
                      Restarting…
                    </>
                  ) : (
                    <>
                      <RefreshCw className="h-3 w-3 mr-1" />
                      Restart CDC
                    </>
                  )}
                </Button>
              </div>
            </div>
          </div>
        )}
      </CardHeader>

      <CardContent className="space-y-4">
        {/* Staleness Banner */}
        {normalizedState.status !== 'completed' && normalizedState.status !== 'failed' && normalizedState.is_stale && normalizedState.stale_reason && (
          <Alert variant="default" className="border-amber-500/50 bg-amber-50/50 dark:bg-amber-950/20">
            <AlertCircle className="h-4 w-4" />
            <AlertTitle>Pipeline May Be Stuck</AlertTitle>
            <AlertDescription>
              <p className="text-sm mb-3">{normalizedState.stale_reason}</p>
              <div className="flex gap-2">
                {onRefresh && (
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={onRefresh}
                  >
                    <RefreshCw className="h-3 w-3 mr-1" />
                    Refresh Status
                  </Button>
                )}
                {showCancelButton && onCancel && (
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={onCancel}
                  >
                    <StopCircle className="h-3 w-3 mr-1" />
                    Cancel Pipeline
                  </Button>
                )}
                {normalizedState.current_stage && (
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => {
                      if (normalizedState.current_stage) {
                        setExpandedStages([normalizedState.current_stage])
                      }
                    }}
                  >
                    <ChevronDown className="h-3 w-3 mr-1" />
                    View Details
                  </Button>
                )}
              </div>
            </AlertDescription>
          </Alert>
        )}

        {/* Error Display */}
        {error && (
          <Alert variant="destructive">
            <XCircle className="h-4 w-4" />
            <AlertTitle>Pipeline Failed</AlertTitle>
            <AlertDescription>
              <p className="font-mono text-sm">{error}</p>
            </AlertDescription>
          </Alert>
        )}

        {/* Execution Plan Accordion */}
        {visibleStages.length > 0 ? (
          <Accordion 
            type="multiple" 
            value={expandedStages}
            onValueChange={setExpandedStages}
            className="w-full"
          >
            {visibleStages.map((stage) => {
              const isCurrent = stage.id === state.current_stage
              const isFailed = stage.status === 'failed'
              const isCompleted = stage.status === 'complete'
              // This view was the one reader that had the adapter's unit right.
              // It now goes through the same helper as every other reader, so
              // there is one place that knows which unit a stage carries.
              const durationLabel =
                formatDuration(stageDurationMs(stage) ?? undefined) ||
                formatDuration(stage.metadata?.duration_ms as number | undefined) ||
                null
              const elapsedLabel =
                stage.status === "running"
                  ? elapsedSince(stage.started_at) || (normalizedState.started_at ? elapsedSince(normalizedState.started_at) : null)
                  : null
              const isBlockingHere =
                state.status === 'waiting_for_user' &&
                Boolean(state.blocking_reason) &&
                isCurrent

              return (
                <AccordionItem 
                  key={stage.id} 
                  value={stage.id}
                  className={cn(
                    'border rounded-lg transition-all mb-2',
                    isCurrent && 'ring-2 ring-purple-500',
                    isFailed && 'ring-2 ring-red-500',
                    isCompleted && 'bg-gray-50 dark:bg-gray-900/20'
                  )}
                >
                  <AccordionTrigger className="px-4 hover:no-underline">
                    <div className="flex items-center gap-3 w-full">
                      <StageIcon status={stage.status} />
                      <div className="flex-1 text-left">
                        <h4 className="font-medium text-sm">{stage.display_name}</h4>
                        <div className="flex items-center gap-2 mt-0.5">
                          {isCompleted && (() => {
                            const meta = stage.metadata || {}
                            // Extract meaningful completion summary from metadata
                            const resultSummary = typeof meta.result_summary === 'string' ? meta.result_summary : null
                            const rowsSynced = meta.rows_synced ?? meta.written_rows ?? meta.inserted_rows
                            const bytesProcessed = meta.bytes_processed
                            const srcType = typeof meta.source_connector_type === 'string' ? meta.source_connector_type : null
                            const dstType = typeof meta.destination_connector_type === 'string' ? meta.destination_connector_type : null
                            const destWarning = typeof meta.destination_schema_warning === 'string' ? meta.destination_schema_warning : null
                            const stageId = stage.id

                            // Stage-specific summaries
                            if (stageId === 'executor' && looksLikeCDC) {
                              return (
                                <p className="text-xs text-green-700 dark:text-green-400 font-medium flex items-center gap-1">
                                  <span className="relative flex h-2 w-2">
                                    <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-green-500 opacity-75" />
                                    <span className="relative inline-flex rounded-full h-2 w-2 bg-green-600" />
                                  </span>
                                  Live streaming{durationLabel ? ` · setup took ${durationLabel}` : ''}
                                </p>
                              )
                            }
                            if (stageId === 'executor' && (rowsSynced || bytesProcessed)) {
                              const parts: string[] = []
                              if (rowsSynced) parts.push(`${Number(rowsSynced).toLocaleString()} rows`)
                              if (bytesProcessed) parts.push(`${Math.round(Number(bytesProcessed) / 1024)}KB`)
                              return (
                                <p className="text-xs text-green-700 dark:text-green-400 font-medium">
                                  ✓ {parts.join(' · ')}{durationLabel ? ` in ${durationLabel}` : ''}
                                </p>
                              )
                            }
                            if ((stageId === 'intent' || stageId === 'capability_resolver') && (srcType || dstType)) {
                              return (
                                <p className="text-xs text-muted-foreground">
                                  ✓ {srcType && dstType ? `${srcType.replace(/-/g,' ')} → ${dstType.replace(/-/g,' ')}` : (srcType || dstType)}
                                  {durationLabel ? ` · ${durationLabel}` : ''}
                                </p>
                              )
                            }
                            if (resultSummary) {
                              return (
                                <p className="text-xs text-muted-foreground">
                                  ✓ {resultSummary}{durationLabel ? ` · ${durationLabel}` : ''}
                                </p>
                              )
                            }
                            if (destWarning) {
                              return (
                                <p className="text-xs text-amber-600 dark:text-amber-400">
                                  ⚠ Validated with warnings{durationLabel ? ` · ${durationLabel}` : ''}
                                </p>
                              )
                            }
                            return durationLabel ? (
                              <p className="text-xs text-muted-foreground">✓ Completed in {durationLabel}</p>
                            ) : null
                          })()}
                          {/* When HITL is blocking, suppress the running subtitle and show a waiting label instead */}
                          {stage.status === 'running' && isBlockingHere && (
                            <p className="text-xs text-amber-600 dark:text-amber-400">
                              Waiting for your input…
                            </p>
                          )}
                          {stage.status === 'running' && !isBlockingHere && (() => {
                            const stageId = stage.id
                            const timeStr = durationLabel || (elapsedLabel && elapsedLabel !== '0s' ? elapsedLabel : null)
                            // Pull connection names from stage metadata (emitted by connection_validator worker)
                            const srcName = typeof stage.metadata?.source_connection_name === 'string'
                              ? stage.metadata.source_connection_name : null
                            const dstName = typeof stage.metadata?.destination_connection_name === 'string'
                              ? stage.metadata.destination_connection_name : null
                            // Also check top-level pipeline state metadata for connector types
                            const srcType = typeof state.metadata?.source_connector_type === 'string'
                              ? (state.metadata.source_connector_type as string).replace(/-/g, ' ') : null
                            const dstType = typeof state.metadata?.destination_connector_type === 'string'
                              ? (state.metadata.destination_connector_type as string).replace(/-/g, ' ') : null

                            let msg = 'Working…'
                            if (stageId === 'intent') {
                              msg = 'Understanding your request…'
                            } else if (stageId === 'capability_resolver') {
                              msg = srcType && dstType
                                ? `Resolving ${srcType} → ${dstType} connectors…`
                                : 'Resolving connector types…'
                            } else if (stageId === 'connector_check') {
                              msg = 'Checking available connectors…'
                            } else if (stageId === 'connector_generation') {
                              msg = 'Generating missing connectors…'
                            } else if (stageId === 'connection_validation' || stageId === 'connection_validator') {
                              if (srcName && dstName) {
                                msg = `Validating "${srcName}" → "${dstName}"…`
                              } else if (srcName || dstName) {
                                msg = `Validating ${srcName || dstName}…`
                              } else {
                                msg = 'Testing source & destination connections…'
                              }
                            } else if (stageId === 'planner') {
                              msg = 'Analyzing schema & building execution plan…'
                            } else if (stageId === 'validator') {
                              msg = 'Validating plan compatibility…'
                            } else if (stageId === 'infra_preflight') {
                              msg = 'Starting MCP servers and Kafka Connect…'
                            } else if (stageId === 'executor') {
                              // The executor stage covers a few sub-phases:
                              //   1) waiting for infra_preflight to finish
                              //   2) waiting for the user to pick tables (HITL — handled
                              //      separately by the isBlockingHere branch above)
                              //   3) actually moving data
                              //
                              // The default label used to say "Transferring data…" /
                              // "Preparing data transfer…" — both imply data is moving
                              // BEFORE the user has even picked tables. Table selection is
                              // sub-phase (2) of this executor stage, so there is a
                              // poll-lag window where the stage is 'running' but the
                              // table-selection HITL block has not hydrated yet (status is
                              // not yet 'waiting_for_user', so isBlockingHere is false and
                              // the "Waiting for your input…" branch above can't fire).
                              // Use a phase-neutral "Preparing…" fallback that is truthful
                              // whether the executor is about to request table selection or
                              // about to transfer; only switch to "Syncing X → Y…" once we
                              // have evidence the transfer is past prep (connection names
                              // emitted by the connection_validator stage upstream).
                              if (state.current_stage === 'infra_preflight') {
                                msg = 'Waiting for infrastructure…'
                              } else if (srcName && dstName) {
                                msg = `Syncing "${srcName}" → "${dstName}"…`
                              } else {
                                msg = 'Preparing…'
                              }
                            }
                            return (
                              <p className="text-xs text-blue-600 dark:text-blue-400">
                                {msg}{timeStr ? ` (${timeStr})` : ''}
                              </p>
                            )
                          })()}
                          {isFailed && (
                            <p className="text-xs text-red-600">✗ Failed{stage.error_message ? '' : ''}</p>
                          )}
                        </div>
                      </div>
                      {/* Optional progress indicator (only show when we have meaningful % > 0) */}
                      {typeof stage.progress === 'number' && stage.status === 'running' && stage.progress > 0 && (
                        <div className="flex items-center gap-2">
                          <span className="text-sm text-muted-foreground">
                            {stage.progress}%
                          </span>
                          <Progress value={stage.progress} className="w-24 h-2" />
                        </div>
                      )}
                    </div>
                  </AccordionTrigger>

                  <AccordionContent className="px-4 pb-4 border-t">
                    {/* Stage details */}
                    <div className="space-y-2 pt-3">
                      {/* Activity panel: shown for ALL running stages, not just the current one.
                          executor gets the full violet panel with monitoring link;
                          other stages get a lighter blue panel with the latest event. */}
                      {stage.status === "running" && !isBlockingHere && (
                        <StageActivityPanel
                          pipelineId={state.pipeline_id}
                          stageId={stage.id}
                          startedAt={stage.started_at || normalizedState.started_at}
                          label={
                            stage.id === 'infra_preflight'
                              ? 'Starting infrastructure…'
                              : stage.id === 'executor' && state.current_stage === 'infra_preflight'
                                ? 'Waiting for infrastructure to be ready…'
                                : undefined
                          }
                          onViewMonitoring={stage.id === 'executor' ? onViewMonitoring : undefined}
                        />
                      )}
                      {/* HITL action inline (keeps it in-sequence, not pinned at top) */}
                      {Boolean(isBlockingHere) && resumingFromHITL && (
                        <div className="rounded-lg border border-violet-200 bg-violet-50/50 p-3 text-sm dark:border-violet-900/40 dark:bg-violet-950/20 flex items-center gap-3">
                          <Loader2 className="h-4 w-4 animate-spin text-violet-500 shrink-0" />
                          <div>
                            <div className="font-medium text-violet-900 dark:text-violet-100">Resuming pipeline…</div>
                            <div className="text-xs text-violet-600 dark:text-violet-400 mt-0.5">Processing your input, this usually takes a few seconds.</div>
                          </div>
                        </div>
                      )}
                      {Boolean(isBlockingHere) && !resumingFromHITL && (
                        <div className="rounded-lg border border-blue-200 bg-blue-50/50 p-3 text-sm dark:border-blue-900/40 dark:bg-blue-950/20">
                          <div className="font-medium text-zinc-900 dark:text-zinc-100">
                            Action required
                          </div>
                          <div className="mt-1 text-zinc-600 dark:text-zinc-400">
                            {state.blocking_reason?.description || 'Waiting for your input.'}
                          </div>
                          <div className="mt-3">
                            {(() => {
                              const uiHints =
                                blockingDetails.ui_hints && typeof blockingDetails.ui_hints === 'object'
                                  ? (blockingDetails.ui_hints as Record<string, unknown>)
                                  : {}
                              const widget = String(uiHints.widget || '')

                              // Prefer the existing, rich popup selectors for known HITL types.
                              // This avoids requiring users to type table names manually.
                              if ((widget === 'table_selector' || requestType.includes('table')) && onSelectTables) {
                                return (
                                  <div className="flex flex-wrap gap-2">
                                    <Button size="sm" onClick={() => onSelectTables(blockingDetails)}>
                                      Select Tables
                                    </Button>
                                  </div>
                                )
                              }

                              if (
                                (widget === 'connection_configurator' ||
                                  requestType.includes('connection') ||
                                  requestType.includes('connections')) &&
                                onConfigureConnections
                              ) {
                                return (
                                  <div className="flex flex-wrap gap-2">
                                    <Button size="sm" onClick={() => onConfigureConnections(blockingDetails)}>
                                      Configure Connections
                                    </Button>
                                  </div>
                                )
                              }

                              if ((widget === 'connector_generator' || requestType.includes('connector') || requestType.includes('connectors')) && onGenerateConnectors) {
                                const rows = extractRequiredConnectorRows(blockingDetails)
                                const missingCount = rows.filter((r) => r.isMissing).length
                                const firstMissing = rows.find((r) => r.isMissing) || null
                                return (
                                  <div className="space-y-3">
                                    <div className="rounded-lg border border-zinc-200 dark:border-zinc-800 bg-white/60 dark:bg-zinc-950/20 p-3">
                                      <div className="text-sm font-medium text-zinc-900 dark:text-zinc-100">
                                        Required connectors
                                      </div>
                                      <div className="mt-2 space-y-2">
                                        {rows.length > 0 ? (
                                          rows.map((r) => (
                                            <div key={`${r.direction}:${r.connectorType}`} className="flex items-center justify-between gap-3">
                                              <div className="flex items-center gap-2 min-w-0">
                                                <ConnectionLogo connectorType={r.connectorType} size="sm" />
                                                <div className="min-w-0">
                                                  <div className="text-sm text-zinc-900 dark:text-zinc-100 truncate">
                                                    <span className="font-medium">{r.direction === 'source' ? 'Source' : 'Destination'}:</span>{' '}
                                                    {formatConnectorDisplay(r.connectorType)}
                                                  </div>
                                                  <div className="text-xs text-zinc-500 dark:text-zinc-400 truncate">
                                                    {r.connectorType}
                                                  </div>
                                                </div>
                                              </div>
                                              {r.isMissing ? (
                                                <Badge className="bg-amber-500 text-white text-xs shrink-0">Missing</Badge>
                                              ) : (
                                                <Badge className="bg-green-600 text-white text-xs shrink-0">Available</Badge>
                                              )}
                                            </div>
                                          ))
                                        ) : (
                                          <div className="text-xs text-zinc-600 dark:text-zinc-400">
                                            We couldn’t determine which connectors are missing from the pipeline state.
                                            Click “Generate connectors” to proceed.
                                          </div>
                                        )}
                                      </div>
                                    </div>

                                    <div className="text-xs text-zinc-600 dark:text-zinc-400">
                                      Next step: generate the <span className="font-mono">{firstMissing?.connectorType || 'missing'}</span> connector,
                                      then come back and click <span className="font-medium">Resume pipeline</span>.
                                    </div>

                                    <div className="flex flex-wrap gap-2">
                                      <Button
                                        size="sm"
                                        onClick={() => {
                                          // HITL contract: this stage means connector(s) are NOT installed yet.
                                          // We must first take the user to the generator UI, not resume the workflow.
                                          if (!firstMissing?.connectorType) {
                                            window.location.href = '/connectors/generate?return=' + encodeURIComponent('/chat')
                                            return
                                          }
                                          const key = firstMissing.direction === 'destination' ? 'destination' : 'source'
                                          window.location.href =
                                            '/connectors/generate?' +
                                            key +
                                            '=' +
                                            encodeURIComponent(firstMissing.connectorType) +
                                            '&return=' +
                                            encodeURIComponent('/chat')
                                        }}
                                      >
                                        Open connector generator
                                      </Button>
                                      <Button size="sm" variant="outline" onClick={() => onGenerateConnectors(blockingDetails)}>
                                        Resume pipeline
                                      </Button>
                                      <Button
                                        variant="outline"
                                        size="sm"
                                        onClick={() => {
                                          window.location.href = '/connectors'
                                        }}
                                      >
                                        View installed connectors
                                      </Button>
                                    </div>
                                  </div>
                                )
                              }

                              // Otherwise, fall back to a minimal JSON-schema based form for unknown/new agents.
                              if (isJsonSchema(blockingDetails.input_schema)) {
                                return (
                                  <GenericHitlForm
                                    schema={blockingDetails.input_schema}
                                    onSubmit={(data) => {
                                      // Generic submit: pass payload to the best available callback.
                                      if (onSelectTables) onSelectTables({ ...blockingDetails, ...data })
                                      else if (onConfigureConnections) onConfigureConnections({ ...blockingDetails, ...data })
                                      else if (onGenerateConnectors) onGenerateConnectors({ ...blockingDetails, ...data })
                                    }}
                                  />
                                )
                              }

                              return null
                            })()}
                          </div>
                        </div>
                      )}
                      {stage.error_message && (
                        <>
                          <Alert variant="destructive" className="text-sm">
                            <AlertCircle className="h-4 w-4" />
                            <AlertDescription>{stage.error_message}</AlertDescription>
                          </Alert>
                          {/* Connection validation failure: guide user to fix the connection */}
                          {(stage.id === 'connection_validation' || stage.id === 'connection_validator') && onConfigureConnections && (
                            <div className="flex items-center gap-2 mt-2">
                              <Button
                                size="sm"
                                variant="outline"
                                onClick={() => onConfigureConnections({ stage_id: stage.id })}
                                className="border-red-300 text-red-700 hover:bg-red-50 dark:border-red-700 dark:text-red-400"
                              >
                                Configure connections
                              </Button>
                              <span className="text-xs text-muted-foreground">Fix credentials or network access, then re-run the pipeline.</span>
                            </div>
                          )}
                        </>
                      )}

                      {/* Destination schema warning */}
                      {Boolean(stage.metadata?.destination_schema_warning) && (
                        <Alert className="border-amber-300 bg-amber-50/60 dark:border-amber-800 dark:bg-amber-950/20 text-sm">
                          <AlertCircle className="h-4 w-4 text-amber-600" />
                          <AlertTitle className="text-amber-800 dark:text-amber-300 text-xs font-semibold">Destination warning</AlertTitle>
                          <AlertDescription className="text-amber-700 dark:text-amber-400 text-xs">
                            {String(stage.metadata?.destination_schema_warning ?? '')}
                          </AlertDescription>
                        </Alert>
                      )}

                      {/* Stage-specific detail cards (structured, not raw key-value dump) */}
                      {Boolean(stage.metadata && Object.keys(stage.metadata).length > 0) && (() => {
                        // Fields already shown elsewhere (header strip, warnings above) — skip them in the detail section
                        const SKIP_KEYS = new Set([
                          'duration_ms', 'result_summary', 'destination_schema_warning',
                          'source_connector_type', 'destination_connector_type',
                          'source_connection_name', 'destination_connection_name',
                          'execution_plan', 'run_mode',
                        ])
                        // Human-readable labels for known keys
                        const LABELS: Record<string, string> = {
                          rows_synced: 'Rows synced',
                          written_rows: 'Rows written',
                          inserted_rows: 'Rows inserted',
                          bytes_processed: 'Data processed',
                          tables_processed: 'Tables processed',
                          error_message: 'Error',
                          plan_summary: 'Plan summary',
                          source_tables: 'Source tables',
                          selected_tables: 'Selected tables',
                          validation_warnings: 'Warnings',
                        }
                        const visibleEntries = Object.entries(stage.metadata ?? {}).filter(([k]) => !SKIP_KEYS.has(k))
                        if (visibleEntries.length === 0) return null
                        return (
                          <div className="rounded-lg border border-border bg-muted/30 px-3 py-2 space-y-1 text-xs">
                            {visibleEntries.map(([key, value]) => {
                              const label = LABELS[key] || key.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase())
                              const displayVal = typeof value === 'number' && (key.includes('rows') || key.includes('count'))
                                ? value.toLocaleString()
                                : typeof value === 'number' && key.includes('bytes')
                                  ? `${(value / 1024).toFixed(1)} KB`
                                  : typeof value === 'object'
                                    ? JSON.stringify(value)
                                    : String(value)
                              return (
                                <div key={key} className="flex justify-between gap-4">
                                  <span className="text-muted-foreground shrink-0">{label}</span>
                                  <span className="font-mono text-right break-all">{displayVal}</span>
                                </div>
                              )
                            })}
                          </div>
                        )
                      })()}
                    </div>
                  </AccordionContent>
                </AccordionItem>
              )
            })}
          </Accordion>
        ) : (
          // Pipeline just started — no execution plan yet
          <div className="space-y-3 py-2">
            {(state.status === 'running' || state.status === 'pending') ? (
              <>
                <div className="flex items-center gap-3 rounded-lg border border-violet-200 dark:border-violet-800 bg-violet-50 dark:bg-violet-950/20 p-3">
                  <Loader2 className="h-4 w-4 text-violet-600 animate-spin shrink-0" />
                  <div className="min-w-0">
                    <p className="text-sm font-medium text-violet-900 dark:text-violet-100">
                      Agent is analyzing your request…
                    </p>
                    <p className="text-xs text-violet-700 dark:text-violet-300 mt-0.5">
                      Understanding intent, resolving connectors and building an execution plan. This usually takes 10–20 seconds.
                    </p>
                  </div>
                </div>
                <div className="space-y-2">
                  {(['Understanding request', 'Resolving connectors', 'Building execution plan'] as const).map((step, i) => (
                    <div key={step} className="flex items-center gap-2 text-xs text-muted-foreground">
                      <div className={cn(
                        "h-1.5 w-1.5 rounded-full shrink-0",
                        i === 0 ? "bg-violet-500 animate-pulse" : "bg-muted-foreground/30"
                      )} />
                      <span className={i === 0 ? "text-foreground" : ""}>{step}</span>
                    </div>
                  ))}
                </div>
              </>
            ) : (
              <div className="text-sm text-muted-foreground text-center py-4">
                <p>Execution plan not available. Pipeline is {state.status}.</p>
                {state.current_stage && (
                  <p className="mt-2">Current stage: {state.current_stage}</p>
                )}
              </div>
            )}
          </div>
        )}

        {/* Runtime failure banner — server detected a dead dependency.
            Trumps the green CDC banner so we never lie about "streaming"
            when a required MCP container is down. */}
        {showRuntimeFailureBanner && runtime && (
          <div className="bg-red-50 rounded-lg p-4 border border-red-200 dark:bg-red-950/20 dark:border-red-900/40">
            <div className="flex items-center gap-2">
              <XCircle className="h-5 w-5 text-red-600" />
              <p className="font-medium text-red-900 dark:text-red-200">
                Pipeline is not running — a required dependency is unreachable
              </p>
            </div>
            <p className="text-sm text-red-700 dark:text-red-400 mt-1 ml-7">
              Check the dependency panel below for details. The orchestrator will
              attempt to auto-restart the dead component within ~60 seconds.
            </p>
          </div>
        )}

        {/* Diagnose button — read-only LLM-powered triage. Always offered;
            most useful when something is degraded or the user is confused. */}
        <DiagnoseButton pipelineId={state.pipeline_id} />

        {/* Dependency health panel — only renders when /runtime is available
            and reports at least one dependency. Replaces guessing about why
            a CDC pipeline is producing 0 rows. */}
        {runtime && runtime.dependencies && runtime.dependencies.length > 0 && (
          <DependencyHealthPanel deps={runtime.dependencies} aggregate={runtime.health} />
        )}

        {/* CDC live banner — streaming pipeline is active */}
        {showCDCLiveBanner && (
          <div className="bg-green-50 rounded-lg p-4 border border-green-200 dark:bg-green-950/20 dark:border-green-900/40">
            <div className="flex items-center gap-2">
              <span className="relative flex h-3 w-3 shrink-0">
                <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-green-500 opacity-75" />
                <span className="relative inline-flex rounded-full h-3 w-3 bg-green-600" />
              </span>
              <p className="font-medium text-green-900 dark:text-green-200">CDC stream is live</p>
            </div>
            <p className="text-sm text-green-700 dark:text-green-400 mt-1 ml-5">
              Changes from your source are being streamed in real-time. Check the Monitoring tab to see event throughput.
            </p>
            <div className="flex gap-2 mt-3 ml-5">
              <Button asChild size="sm" className="bg-gradient-to-r from-violet-600 to-indigo-600 text-white">
                <Link href={`/pipelines/${state.pipeline_id}`}>View Pipeline</Link>
              </Button>
              <Button asChild size="sm" variant="outline">
                <Link href="/chat">New Pipeline</Link>
              </Button>
            </div>
          </div>
        )}

        {/* Success banner - ONLY if explicitly completed (non-CDC) */}
        {showSuccessBanner && (
          <div className="bg-green-50 rounded-lg p-4 border border-green-200 dark:bg-green-950/20 dark:border-green-900/40">
            <div className="flex items-center gap-2">
              <CheckCircle2 className="h-5 w-5 text-green-600" />
              <p className="font-medium text-green-900 dark:text-green-200">Pipeline completed successfully!</p>
            </div>
            <div className="flex gap-2 mt-3">
              <Button asChild size="sm" className="bg-gradient-to-r from-violet-600 to-indigo-600 text-white">
                <Link href={`/pipelines/${state.pipeline_id}`}>View Pipeline</Link>
              </Button>
              <Button asChild size="sm" variant="outline">
                <Link href="/chat">New Pipeline</Link>
              </Button>
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  )
}

// Maps a connector identifier ("postgresql@v1.0.14") and dep kind into a
// human-readable label.
function depLabel(dep: RuntimeDep): string {
  const [name] = dep.identifier.split('@')
  const kindLabel: Record<string, string> = {
    mcp_source: 'Source connector',
    mcp_dest: 'Destination connector',
    debezium_task: 'CDC task',
    kafka_sink_worker: 'Sink worker',
  }
  const prefix = kindLabel[dep.kind] || dep.kind
  return name ? `${prefix} · ${name.replace(/-/g, ' ')}` : prefix
}

function depDot(status: RuntimeHealth): string {
  switch (status) {
    case 'healthy': return 'bg-green-500'
    case 'degraded': return 'bg-amber-500'
    case 'unhealthy': return 'bg-red-500'
    default: return 'bg-muted-foreground/40'
  }
}

function DependencyHealthPanel({ deps, aggregate }: { deps: RuntimeDep[]; aggregate: RuntimeHealth }) {
  const wrapperTone =
    aggregate === 'unhealthy'
      ? 'border-red-200 bg-red-50/50 dark:border-red-900/40 dark:bg-red-950/10'
      : aggregate === 'degraded'
        ? 'border-amber-200 bg-amber-50/50 dark:border-amber-900/40 dark:bg-amber-950/10'
        : 'border-border bg-muted/30'
  return (
    <div className={cn('rounded-lg border p-3 space-y-2', wrapperTone)}>
      <div className="flex items-center justify-between">
        <p className="text-xs font-medium text-foreground">Runtime dependencies</p>
        <span className="text-[10px] uppercase tracking-wide text-muted-foreground">{aggregate}</span>
      </div>
      <ul className="space-y-1.5">
        {deps.map((dep) => (
          <li key={`${dep.kind}:${dep.identifier}`} className="flex items-start gap-2 text-xs">
            <span className={cn('mt-1.5 h-2 w-2 rounded-full shrink-0', depDot(dep.status))} />
            <div className="min-w-0 flex-1">
              <div className="flex items-center justify-between gap-2">
                <span className="truncate text-foreground">{depLabel(dep)}</span>
                <span className="text-muted-foreground capitalize shrink-0">{dep.status}</span>
              </div>
              {dep.status !== 'healthy' && dep.last_error ? (
                <p className="text-[11px] text-muted-foreground mt-0.5 break-words">{dep.last_error}</p>
              ) : null}
            </div>
          </li>
        ))}
      </ul>
    </div>
  )
}

// DiagnoseButton triggers an LLM-powered triage of the pipeline. Read-only:
// the model only explains, it never acts. Result is rendered inline below
// the button so on-call doesn't have to leave the chat to reason about
// what's wrong.
type DiagnoseVerdict = {
  summary?: string
  root_cause?: string
  suggested_action?: string
  confidence?: string
  evidence_pointers?: string[]
  raw?: string
  evidence?: Record<string, unknown>
}

function DiagnoseButton({ pipelineId }: { pipelineId: string }) {
  const [loading, setLoading] = useState(false)
  const [verdict, setVerdict] = useState<DiagnoseVerdict | null>(null)
  const [error, setError] = useState<string | null>(null)

  const run = async () => {
    setLoading(true)
    setError(null)
    try {
      const res = await authFetch(API_ENDPOINTS.PIPELINES.DIAGNOSE(pipelineId), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: '{}',
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok) {
        setError(data?.message || data?.error || `diagnose ${res.status}`)
        // Server returns evidence even on LLM failure — surface it.
        if (data?.evidence) setVerdict({ evidence: data.evidence })
        return
      }
      setVerdict(data as DiagnoseVerdict)
    } catch (e) {
      setError(String((e as Error)?.message ?? e))
    } finally {
      setLoading(false)
    }
  }

  const confidenceTone =
    verdict?.confidence === 'high'
      ? 'text-green-700 dark:text-green-400'
      : verdict?.confidence === 'medium'
        ? 'text-amber-700 dark:text-amber-400'
        : 'text-muted-foreground'

  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between gap-2">
        <p className="text-xs text-muted-foreground">
          Need help interpreting this run? Run a quick LLM-powered triage.
        </p>
        <Button
          size="sm"
          variant="outline"
          onClick={run}
          disabled={loading}
        >
          {loading ? (
            <>
              <Loader2 className="h-3 w-3 mr-1 animate-spin" />
              Diagnosing…
            </>
          ) : (
            'Diagnose'
          )}
        </Button>
      </div>

      {error && (
        <div className="rounded border border-red-200 bg-red-50/60 p-2 text-xs text-red-700 dark:border-red-900/40 dark:bg-red-950/20 dark:text-red-300">
          {error}
        </div>
      )}

      {verdict && (verdict.summary || verdict.root_cause || verdict.raw) && (
        <div className="rounded-lg border border-border bg-muted/30 p-3 space-y-2">
          <div className="flex items-center justify-between">
            <p className="text-xs font-medium text-foreground">Diagnosis</p>
            {verdict.confidence ? (
              <span className={cn('text-[10px] uppercase tracking-wide', confidenceTone)}>
                {verdict.confidence} confidence
              </span>
            ) : null}
          </div>
          {verdict.summary && (
            <p className="text-sm font-medium text-foreground">{verdict.summary}</p>
          )}
          {verdict.root_cause && (
            <p className="text-xs text-muted-foreground whitespace-pre-wrap">{verdict.root_cause}</p>
          )}
          {verdict.suggested_action && (
            <div className="rounded border border-violet-200 bg-violet-50/60 p-2 dark:border-violet-900/40 dark:bg-violet-950/20">
              <p className="text-[11px] uppercase tracking-wide text-violet-700 dark:text-violet-300 mb-0.5">Suggested next step</p>
              <p className="text-xs text-violet-900 dark:text-violet-100">{verdict.suggested_action}</p>
            </div>
          )}
          {verdict.evidence_pointers && verdict.evidence_pointers.length > 0 && (
            <div className="text-[11px] text-muted-foreground">
              <span className="uppercase tracking-wide mr-1">evidence:</span>
              {verdict.evidence_pointers.join(' · ')}
            </div>
          )}
          {verdict.raw && !verdict.summary && (
            <pre className="text-[11px] text-muted-foreground whitespace-pre-wrap">{verdict.raw}</pre>
          )}
        </div>
      )}
    </div>
  )
}
