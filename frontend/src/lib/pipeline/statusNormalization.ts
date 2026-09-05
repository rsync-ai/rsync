import type { ExecutionState } from "@/lib/pipeline/stageDefinitions"

// NOTE: pipeline "control-plane" statuses are not identical to stageDefinitions.ExecutionState.
// The API Gateway `/pipelines/:id/state` endpoint can return extra states like "paused".
export type NormalizedPipelineStatus = ExecutionState | "paused" | "unknown"

export function normalizePipelineStatus(raw?: string): NormalizedPipelineStatus {
  const s = String(raw || "").trim().toLowerCase()
  if (!s) return "unknown"

  if (s === "processing") return "running"
  if (s === "waiting_for_user") return "waiting_for_user"
  if (s === "paused") return "paused"
  if (s === "completed") return "completed"
  if (s === "failed") return "failed"
  if (s === "idle") return "idle"
  if (s === "running") return "running"

  // common synonyms
  if (s === "canceled") return "cancelled"
  if (s === "cancelled") return "cancelled"
  if (s === "stopped") return "cancelled"

  return "unknown"
}

export function isTerminalPipelineStatus(s: NormalizedPipelineStatus): boolean {
  return s === "completed" || s === "failed" || s === "cancelled"
}

/**
 * reconcilePipelineStatus merges what `/state` says with what `/runtime` observed.
 *
 * `/state` reports the control plane's view and freezes at "running" when a stream
 * dies underneath it — it has no way to see that the source connector stopped or
 * that no event has arrived in an hour. `/runtime` does, via the dependency
 * rollup, so its "failed" (dependencies unhealthy) and "idle" (feed stale)
 * verdicts escalate a `/state` that is only still saying "running" because nothing
 * told it otherwise.
 *
 * Escalation is deliberately limited to "running". Every other `/state` value —
 * waiting_for_user, paused, completed, cancelled — is MORE specific than any
 * runtime phase and wins outright. When `/runtime` is unavailable (still loading,
 * or 404 on an older backend) the `/state` value passes through unchanged.
 *
 * This lives here because four components had their own copy of these three lines
 * and a fifth — the monitoring panel — had none, which is why the Overview tab and
 * the Table statistics tab could show "idle" and "running" for the same execution
 * on the same page load. One definition, one answer.
 */
export function reconcilePipelineStatus(
  liveStatus: NormalizedPipelineStatus,
  runtimePhase?: string | null
): NormalizedPipelineStatus {
  if (liveStatus !== "running") return liveStatus
  if (runtimePhase === "failed") return "failed"
  if (runtimePhase === "idle") return "idle"
  return liveStatus
}

export function pipelineStatusLabel(s: NormalizedPipelineStatus): string {
  switch (s) {
    case "idle":
      return "Idle"
    case "running":
      return "Running"
    case "waiting_for_user":
      return "Needs input"
    case "paused":
      return "Paused"
    case "completed":
      return "Completed"
    case "failed":
      return "Failed"
    case "cancelled":
      return "Stopped"
    default:
      return "Unknown"
  }
}

