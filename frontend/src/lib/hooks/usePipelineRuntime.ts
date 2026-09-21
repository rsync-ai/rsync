"use client"

import { useEffect, useRef, useState } from "react"

import { authFetch } from "@/lib/api/auth-fetch"
import { API_ENDPOINTS } from "@/lib/config/api"

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
  dependencies: RuntimeDep[]
  updated_at: string
}

interface Options {
  pollMs?: number          // default 5000; 0 disables polling
  enabled?: boolean        // default true; set false when no pipelineId yet
}

export function usePipelineRuntime(pipelineId: string | null | undefined, opts: Options = {}) {
  const { pollMs = 5000, enabled = true } = opts
  const [runtime, setRuntime] = useState<PipelineRuntime | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const inflightRef = useRef<AbortController | null>(null)
  // Stop polling when the endpoint 404s (not-yet-deployed route) or the run reaches a
  // terminal phase — avoids hammering the runtime endpoint forever on a finished pipeline.
  const disabledRef = useRef(false)

  // The pipeline this state describes. `/pipelines/[id]` is one route segment,
  // so React keeps this hook's state across a navigation from one pipeline to
  // the next, and every consumer then renders the PREVIOUS pipeline's answer
  // until the new one arrives. On prod (2026-09-20) that read as a flap: a
  // healthy stream showed "Failed · last event 2d ago" and "Unhealthy — no MCP
  // server registered with orchestrator" for about a second, both of them the
  // pipeline the user had come from.
  //
  // Adjusted during render, not in an effect: an effect runs after the commit,
  // so the stale frame would still be painted once.
  //
  // This is a different question from a failed poll, which deliberately keeps
  // the last good answer — a missed tick is still the same pipeline.
  const [subject, setSubject] = useState(pipelineId)
  if (pipelineId !== subject) {
    setSubject(pipelineId)
    setRuntime(null)
    setError(null)
    setLoading(Boolean(enabled && pipelineId))
  }

  useEffect(() => {
    if (!enabled || !pipelineId) {
      setRuntime(null)
      return
    }

    disabledRef.current = false
    let cancelled = false
    const fetchOnce = async () => {
      if (disabledRef.current) return
      // Cancel any in-flight request from the previous tick.
      inflightRef.current?.abort()
      const ac = new AbortController()
      inflightRef.current = ac
      setLoading(true)
      try {
        const res = await authFetch(API_ENDPOINTS.PIPELINES.RUNTIME(pipelineId), {
          cache: "no-store",
          signal: ac.signal,
        })
        if (!res.ok) {
          if (res.status === 404) {
            // Endpoint not available on this backend — stop polling silently.
            disabledRef.current = true
            return
          }
          if (!cancelled) setError(`runtime ${res.status}`)
          return
        }
        const data = (await res.json()) as PipelineRuntime
        if (!cancelled) {
          setRuntime(data)
          setError(null)
          // Stop polling once the run reaches a stable terminal phase — no further updates
          // are expected. See RUNTIME_PHASE_POLLING for which phases those are. The
          // mode matters: a CDC "failed" can recover, so it must not latch.
          if (runtimePhaseEndsPolling(data.phase, data.mode)) {
            disabledRef.current = true
          }
        }
      } catch (e) {
        if ((e as { name?: string })?.name === "AbortError") return
        if (!cancelled) setError(String((e as Error)?.message ?? e))
      } finally {
        if (!cancelled) setLoading(false)
      }
    }

    void fetchOnce()
    if (pollMs <= 0) return () => { cancelled = true; inflightRef.current?.abort() }

    const t = window.setInterval(() => { void fetchOnce() }, pollMs)
    return () => {
      cancelled = true
      window.clearInterval(t)
      inflightRef.current?.abort()
    }
  }, [pipelineId, pollMs, enabled])

  return { runtime, loading, error }
}
