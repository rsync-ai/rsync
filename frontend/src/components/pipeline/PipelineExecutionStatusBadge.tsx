"use client"

import { useMemo } from "react"
import { Badge } from "@/components/ui/badge"
import { cn } from "@/lib/utils"
import {
  pipelineStatusLabel,
  reconcilePipelineStatus,
  type NormalizedPipelineStatus,
} from "@/lib/pipeline/statusNormalization"
import { usePipelineRuntime } from "@/lib/hooks/usePipelineRuntime"
import { usePipelineStatePoll } from "@/lib/hooks/usePipelineStatePoll"
import { CheckCircle2, Loader2, Pause, Square, XCircle } from "lucide-react"

export function PipelineExecutionStatusBadge(props: { pipelineId: string; className?: string }) {
  const { pipelineId, className } = props
  // One shared /state poll for the whole header (usePipelineStatePoll). It also
  // re-reads on the refresh bus, so Pause/Resume flip the pill at once (#13).
  const live = usePipelineStatePoll(pipelineId)
  const status: NormalizedPipelineStatus = live.status ?? "unknown"
  // "The last refresh failed", not "the pipeline is broken". Keeping the last
  // known status is right — flickering to Unknown on one dropped poll would be
  // worse — but a spinning "Running" pill is a claim that liveness was
  // confirmed moments ago, and once the reads stop coming back nothing backs it.
  const stale = live.error !== null
  // The dependency-aware /runtime endpoint is the source of truth for whether a
  // (CDC) stream is actually alive. /state can freeze at "running" after the feed
  // dies, so it alone would paint a dead stream as "Running".
  const { runtime } = usePipelineRuntime(pipelineId)

  // Escalate to /runtime's verdict when it reports the stream failed or idle —
  // see reconcilePipelineStatus for why, and for the surfaces that must agree.
  const effectiveStatus: NormalizedPipelineStatus = useMemo(
    () => reconcilePipelineStatus(status, runtime?.phase),
    [runtime?.phase, status]
  )

  const cfg = useMemo(() => {
    const label = pipelineStatusLabel(effectiveStatus)
    if (effectiveStatus === "running") return { label, variant: "default" as const, icon: Loader2, spin: true }
    if (effectiveStatus === "waiting_for_user") return { label, variant: "secondary" as const, icon: Pause, spin: false }
    if (effectiveStatus === "paused") return { label, variant: "secondary" as const, icon: Pause, spin: false }
    if (effectiveStatus === "completed") return { label, variant: "default" as const, icon: CheckCircle2, spin: false }
    if (effectiveStatus === "failed") return { label, variant: "destructive" as const, icon: XCircle, spin: false }
    if (effectiveStatus === "cancelled") return { label, variant: "secondary" as const, icon: Square, spin: false }
    return { label, variant: "outline" as const, icon: Square, spin: false }
  }, [effectiveStatus])

  const Icon = cfg.icon

  return (
    <Badge
      variant={cfg.variant}
      className={cn("gap-1", stale && "opacity-60", className)}
      title={stale ? "Status may be out of date — the last refresh failed" : undefined}
    >
      {/* The spinner stops when the reads stop: it is the part of this pill
          that says "confirmed live just now", so it must not outlive the
          confirmation. The label is kept — the last known status is still the
          best answer available, it just isn't a current one. */}
      <Icon className={cn("h-3 w-3", cfg.spin && !stale && "animate-spin")} />
      {cfg.label}
      {stale && <span aria-hidden="true">·&nbsp;stale</span>}
    </Badge>
  )
}

