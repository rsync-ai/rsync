"use client"

import { useEffect, useRef, useState } from "react"

import { authFetch } from "@/lib/api/auth-fetch"
import { API_ENDPOINTS } from "@/lib/config/api"

// Mirror of api-gateway/internal/handlers/pipeline_runtime.go.
// Keep in sync with the Go struct — this is the canonical "what is this
// pipeline doing right now" shape that replaces UI-side state derivation
// (looksLikeCDC / normalizedState / getDisplayProgress / ...).
export type RuntimePhase =
  | "initializing"
  | "planning"
  | "validating"
  | "syncing"
  | "streaming"
  | "idle"
  | "completed"
  | "failed"
  | "paused"

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
          // are expected. For CDC, "failed" means a dependency (Debezium/MCP/sink) is
          // currently unhealthy, which can recover, so we keep polling to self-correct;
          // "streaming"/"idle" are likewise non-terminal. Only "completed" ends a stream.
          // Batch runs latch on both "completed" and "failed".
          const terminalForMode =
            data.phase === "completed" ||
            (data.phase === "failed" && data.mode !== "cdc")
          if (terminalForMode) {
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
