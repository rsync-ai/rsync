"use client"

import { useCallback, useSyncExternalStore } from "react"

import { authFetch } from "@/lib/api/auth-fetch"
import { API_ENDPOINTS } from "@/lib/config/api"
import { onPipelineRefresh } from "@/lib/events/pipelineRefresh"

// Mirror of api-gateway/internal/handlers/pipeline_runtime.go.
// Keep in sync with the Go struct — this is the canonical "what is this
// pipeline doing right now" shape that replaces UI-side state derivation
// (looksLikeCDC / normalizedState / getDisplayProgress / ...).
//
// waiting_for_data (issue #20): a CDC stream that finished setting up but has not
// delivered a single row past the grace period (cdcLivenessPhase). It is not
// terminal — the next poll can turn it into streaming — so it must keep polling.
export type RuntimePhase =
  | "initializing"
  | "planning"
  | "validating"
  | "syncing"
  | "streaming"
  | "waiting_for_data"
  | "idle"
  | "completed"
  | "failed"
  | "paused"
  | "stopped"

export type PhasePolling = "keep" | "stop" | "stop-unless-cdc"

// Whether a phase ends polling. A Record over RuntimePhase rather than a list of
// terminal phases, so a new phase fails `tsc` here until someone decides — a
// phase that silently stopped polling would freeze the header on it forever.
// For CDC, "failed" means a dependency is unhealthy right now, which can recover;
// a batch run latches on it.
// Exported so a type-level test can hold this table to every RuntimePhase: the
// Record annotation is the whole safety net, and nothing else would notice it
// being loosened.
export const RUNTIME_PHASE_POLLING: Record<RuntimePhase, PhasePolling> = {
  initializing: "keep",
  planning: "keep",
  validating: "keep",
  syncing: "keep",
  streaming: "keep",
  waiting_for_data: "keep",
  idle: "keep",
  completed: "stop",
  failed: "stop-unless-cdc",
  paused: "keep",
  stopped: "keep",
}

/**
 * runtimePhaseEndsPolling is true when no further /runtime update is expected.
 * A phase this client does not know (a newer gateway) keeps polling: guessing
 * "terminal" would freeze the page, guessing "live" costs one request per tick.
 */
export function runtimePhaseEndsPolling(phase: string, mode: "batch" | "cdc"): boolean {
  const rule: PhasePolling | undefined = Object.prototype.hasOwnProperty.call(RUNTIME_PHASE_POLLING, phase)
    ? RUNTIME_PHASE_POLLING[phase as RuntimePhase]
    : undefined
  if (rule === "stop") return true
  if (rule === "stop-unless-cdc") return mode !== "cdc"
  return false
}

export type RuntimeHealth = "healthy" | "degraded" | "unhealthy" | "unknown"

export interface RuntimeProgress {
  percent: number
  current_step?: number
  total_steps?: number
}

export interface RuntimeLiveness {
  last_event_at?: string
  last_healthy_at?: string
  stale_seconds?: number
  // Captured-minus-applied across the pipeline's CDC tables: changes the source
  // recorded that the destination has not written yet. The gateway always sends
  // it (no omitempty), so 0 means "nothing waiting"; it is optional here only for
  // an older gateway that predates the field.
  pending_events?: number

  // The CDC sink's live Kafka drain reading (pipeline_sink_lag, migration 116),
  // recorded by the orchestrator Sentinel each tick.
  //
  // pending_events and sink_lag_messages measure DIFFERENT things and the page needs
  // both. pending_events is captured-minus-applied out of our own counters, so it is
  // 0 whenever nothing has been counted — which is why a pipeline with no stats rows
  // renders a confident "Caught up". sink_lag_messages is the broker's own answer, so
  // it holds even when our counters are empty.
  //
  // sink_committed_moving is what makes a lag of 0 interpretable at all: a sink caught
  // up because there is nothing to do and a sink caught up because its producer died
  // both read 0 (KI-DEBEZIUM-WORKER-DEATH-NOT-SURFACED), and differ only in whether
  // the committed offset ever advances.
  //
  // Every field is optional because ABSENT means "no reading" and must render as
  // unknown, never as a healthy zero. Do not default these to 0.
  sink_lag_messages?: number
  sink_lag_measured_at?: string
  sink_committed_moving?: boolean
  sink_stalled?: boolean
  sink_stalled_seconds?: number
  sink_consumer_group?: string
}

export interface RuntimeBlocker {
  type: string
  description?: string
  details?: Record<string, unknown>
}

export interface RuntimeDep {
  kind: string                    // mcp_source | mcp_dest | debezium_task | kafka_sink_worker | ...
  identifier: string              // e.g. "postgresql@v1.0.14"
  status: RuntimeHealth
  last_checked_at?: string
  last_healthy_at?: string
  consecutive_failures?: number
  last_error?: string
  details?: Record<string, unknown>
}

// A CDC pipeline's initial (full) load, as the orchestrator recorded it
// (cdc_snapshot_requests, source 'initial'). ABSENT means no load was recorded,
// e.g. a pipeline that streamed before the record existed; it never means the
// load finished.
export interface RuntimeLoad {
  status: "sent" | "started" | "completed" | "unconfirmed" | "failed"
  mode: "blocking" | "incremental"
  tables_total: number
  tables_done: number
  started_at?: string
  completed_at?: string
  last_error?: string
  // Distinct tables an open Re-snapshot / Edit tables / new-table request is
  // loading again.
  reloading_tables: number
  // Snapshot rows read from the source that the destination has not written
  // yet. A load completes when the source read ends, so a completed load with
  // rows waiting is still being written. Absent from an older gateway.
  snapshot_rows_waiting?: number
}

export interface PipelineRuntime {
  pipeline_id: string
  execution_id?: string
  mode: "batch" | "cdc"
  phase: RuntimePhase
  health: RuntimeHealth
  message?: string
  progress?: RuntimeProgress
  liveness?: RuntimeLiveness
  blocker?: RuntimeBlocker
  load?: RuntimeLoad
  dependencies: RuntimeDep[]
  updated_at: string
}

interface Options {
  pollMs?: number          // default 5000; 0 disables polling
  enabled?: boolean        // default true; set false when no pipelineId yet
}

export interface PipelineRuntimeState {
  runtime: PipelineRuntime | null
  loading: boolean
  error: string | null
}

const IDLE: PipelineRuntimeState = Object.freeze({ runtime: null, loading: false, error: null })
const STARTING: PipelineRuntimeState = Object.freeze({ runtime: null, loading: true, error: null })

// One poller per (pipeline, interval), shared by every reader on the page.
//
// The detail page reads /runtime from up to six places at once — the header, the
// status badge, the overflow menu, the CDC actions, the Data flow tab and the live
// stream strip. Each used to own a 5 s interval, so one open page sent six
// identical GETs a tick, and went on sending them from a background tab. Readers
// now subscribe to a shared entry; the first one starts it, the last one to leave
// stops it and drops it, so a remount starts clean (a 404 is retried then, as it
// was when each reader owned its poll).
interface Poller {
  state: PipelineRuntimeState
  listeners: Set<() => void>
  stop: () => void
}

const pollers = new Map<string, Poller>()

// A /runtime request gives up after this long. The interval tick waits for the
// request in flight instead of cancelling it, so without a bound one hung request
// would hold the poll forever.
export const RUNTIME_REQUEST_TIMEOUT_MS = 10_000

function isHidden(): boolean {
  return typeof document !== "undefined" && document.visibilityState === "hidden"
}

function startPoller(pipelineId: string, pollMs: number): Poller {
  const poller: Poller = { state: STARTING, listeners: new Set(), stop: () => {} }
  let stopped = false
  // Stop polling when the endpoint 404s (not-yet-deployed route) or the run reaches a
  // terminal phase — avoids hammering the runtime endpoint forever on a finished pipeline.
  let disabled = false
  // The 404 half of `disabled`, kept apart because a refresh event must not
  // undo it: a missing route stays missing, a terminal phase can change.
  let notFound = false
  let inflight: AbortController | null = null

  const update = (next: Partial<PipelineRuntimeState>) => {
    if (stopped) return
    const merged = { ...poller.state, ...next }
    const cur = poller.state
    if (merged.runtime === cur.runtime && merged.loading === cur.loading && merged.error === cur.error) return
    poller.state = merged
    poller.listeners.forEach((l) => l())
  }

  // `supersede` is for a refresh-bus read: something just changed, so a request
  // sent before it may carry the old answer — cancel it and ask again. A timer
  // tick never cancels: when /runtime answers slower than the interval, aborting
  // on every tick meant no answer ever landed and the store kept its last one (a
  // transient CDC "failed") with no error shown (U-18). The tick skips instead.
  const fetchOnce = async (supersede = false) => {
    if (stopped || disabled) return
    if (inflight && !supersede) return
    inflight?.abort()
    const ac = new AbortController()
    inflight = ac
    update({ loading: true })
    try {
      const res = await authFetch(API_ENDPOINTS.PIPELINES.RUNTIME(pipelineId), {
        cache: "no-store",
        signal: ac.signal,
        timeoutMs: RUNTIME_REQUEST_TIMEOUT_MS,
      })
      if (!res.ok) {
        if (res.status === 404) {
          // Endpoint not available on this backend — stop polling silently.
          disabled = true
          notFound = true
          return
        }
        update({ error: `runtime ${res.status}` })
        return
      }
      const data = (await res.json()) as PipelineRuntime
      if (stopped) return
      update({ runtime: data, error: null })
      // Stop polling once the run reaches a stable terminal phase — no further updates
      // are expected. See RUNTIME_PHASE_POLLING for which phases those are. The
      // mode matters: a CDC "failed" can recover, so it must not latch.
      if (runtimePhaseEndsPolling(data.phase, data.mode)) disabled = true
    } catch (e) {
      if ((e as { name?: string })?.name === "AbortError") return
      update({ error: String((e as Error)?.message ?? e) })
    } finally {
      // A request a newer read aborted must not clear the newer one's loading flag.
      if (inflight === ac) {
        inflight = null
        update({ loading: false })
      }
    }
  }

  void fetchOnce()

  // Pause, Resume, Edit tables and Re-snapshot announce themselves on the
  // refresh bus. Without listening, the status strip kept the old phase until
  // the next tick — or forever, once a terminal phase had stopped the poll —
  // so a paused pipeline read "Streaming" until a full reload (#13). A refresh
  // re-arms a poll that a phase stopped, but not one a 404 stopped.
  const unsubscribeRefresh = onPipelineRefresh((pid) => {
    if (pid !== pipelineId) return
    if (!notFound) disabled = false
    void fetchOnce(true)
  })

  let timer: number | undefined
  let onVisibility: (() => void) | undefined
  if (pollMs > 0 && typeof window !== "undefined") {
    // A hidden tab skips its ticks; on return it asks at once rather than showing
    // an answer up to a full interval old.
    timer = window.setInterval(() => {
      if (!isHidden()) void fetchOnce()
    }, pollMs)
    onVisibility = () => {
      if (!isHidden()) void fetchOnce(true)
    }
    document.addEventListener("visibilitychange", onVisibility)
  }

  poller.stop = () => {
    stopped = true
    unsubscribeRefresh()
    if (timer !== undefined) window.clearInterval(timer)
    if (onVisibility) document.removeEventListener("visibilitychange", onVisibility)
    inflight?.abort()
  }
  return poller
}

function subscribePoller(key: string, pipelineId: string, pollMs: number, listener: () => void) {
  let poller = pollers.get(key)
  if (!poller) {
    poller = startPoller(pipelineId, pollMs)
    pollers.set(key, poller)
  }
  const p = poller
  p.listeners.add(listener)
  return () => {
    p.listeners.delete(listener)
    if (p.listeners.size > 0) return
    p.stop()
    if (pollers.get(key) === p) pollers.delete(key)
  }
}

export function usePipelineRuntime(pipelineId: string | null | undefined, opts: Options = {}): PipelineRuntimeState {
  const { pollMs = 5000, enabled = true } = opts
  const id = enabled && pipelineId ? pipelineId : null
  const key = id ? `${id}|${pollMs}` : null

  const subscribe = useCallback(
    (listener: () => void) => (key && id ? subscribePoller(key, id, pollMs, listener) : () => {}),
    [key, id, pollMs],
  )
  // Keyed by pipeline, so a navigation from one pipeline to the next reads the new
  // pipeline's entry — or STARTING — on the very first render. `/pipelines/[id]` is
  // one route segment, so per-component state used to carry the PREVIOUS pipeline's
  // answer across that navigation; on prod (2026-09-20) that read as a flap: a
  // healthy stream showed "Failed · last event 2d ago" for about a second.
  //
  // A failed poll is a different question: it keeps the last good answer, because a
  // missed tick is still the same pipeline.
  const getSnapshot = useCallback(() => (key ? pollers.get(key)?.state ?? STARTING : IDLE), [key])
  return useSyncExternalStore(subscribe, getSnapshot, getSnapshot)
}
