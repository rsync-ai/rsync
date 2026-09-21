"use client"

import { useEffect, useId, useState } from "react"

import { usePipelineRuntime, type PipelineRuntime, type RuntimeHealth } from "@/lib/hooks/usePipelineRuntime"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { DiagnosePanel } from "@/components/pipeline/DiagnosePanel"
import { RUNTIME_PHASE_WAITING_FOR_DATA, runtimePhaseLabel } from "@/lib/pipeline/statusNormalization"
import { cn } from "@/lib/utils"

// green / amber / red / gray for healthy / degraded / unhealthy / unknown.
function healthDot(health: RuntimeHealth): string {
  switch (health) {
    case "healthy":
      return "bg-emerald-500"
    case "degraded":
      return "bg-amber-500"
    case "unhealthy":
      return "bg-red-500"
    default:
      return "bg-muted-foreground/40"
  }
}

// Whole-seconds "Ns ago" / "Nm ago" — finer than utils.formatRelativeTime,
// which collapses sub-minute values to "Just now".
function relativeFromSeconds(totalSeconds: number): string {
  const s = Math.max(0, Math.floor(totalSeconds))
  if (s < 60) return `${s}s ago`
  const m = Math.floor(s / 60)
  if (m < 60) return `${m}m ago`
  const h = Math.floor(m / 60)
  if (h < 24) return `${h}h ago`
  return `${Math.floor(h / 24)}d ago`
}

const compactCount = new Intl.NumberFormat("en-US", { notation: "compact", maximumFractionDigits: 1 })
const exactCount = new Intl.NumberFormat("en-US")

// Past this many seconds without a write, a waiting backlog is the stuck
// signature — the same 300 s cdcLivenessPhase (pipeline_runtime.go) uses to
// call the stream idle.
const BACKLOG_STALE_SECONDS = 300

export interface BacklogVital {
  text: string
  tone: "ok" | "muted" | "warn"
  title: string
}

/**
 * backlogVital turns liveness.pending_events into the header's "changes waiting"
 * segment. It is what tells a quiet CDC stream (nothing waiting) from a stuck
 * one (changes waiting, nothing written). Returns null when there is nothing
 * honest to say:
 *  - batch pipelines, or a CDC stream with no liveness yet;
 *  - an older gateway that does not send the field;
 *  - 0 waiting while health is not "healthy". A degraded source can freeze both
 *    counters, so that zero may be false (see cdcLivenessPhase).
 */
export function backlogVital(runtime: PipelineRuntime): BacklogVital | null {
  if (runtime.mode !== "cdc" || !runtime.liveness) return null
  const pending = runtime.liveness.pending_events
  if (typeof pending !== "number" || !Number.isFinite(pending) || pending < 0) return null
  if (pending === 0) {
    if (runtime.health !== "healthy") return null
    return {
      text: "caught up",
      tone: "ok",
      title: "Every change the source recorded has been written to the destination.",
    }
  }
  const stale = runtime.liveness.stale_seconds ?? 0
  const noun = pending === 1 ? "change" : "changes"
  return {
    text: `${compactCount.format(pending)} ${noun} waiting`,
    tone: stale > BACKLOG_STALE_SECONDS ? "warn" : "muted",
    title: `${exactCount.format(pending)} ${noun} the source recorded ${pending === 1 ? "has" : "have"} not been written to the destination yet.`,
  }
}

export function PipelineHealthHeader({ pipelineId }: { pipelineId: string }) {
  const { runtime, loading, error } = usePipelineRuntime(pipelineId)
  const [showDiagnose, setShowDiagnose] = useState(false)
  const diagnosePanelId = useId()
  // Tick so the "Ns ago" vital stays current between runtime polls without
  // calling Date.now() during render (keeps the render pure).
  const [now, setNow] = useState(() => Date.now())
  useEffect(() => {
    const t = window.setInterval(() => setNow(Date.now()), 1000)
    return () => window.clearInterval(t)
  }, [])

  if (!runtime) {
    return (
      <div className="flex items-center gap-2 rounded-lg border border-border bg-muted/30 px-3 py-2 text-xs text-muted-foreground">
        <span className="h-2.5 w-2.5 rounded-full bg-muted-foreground/40 animate-pulse" />
        {loading ? "Loading pipeline health…" : error ? "Pipeline health unavailable" : "No runtime data"}
      </div>
    )
  }

  const health = runtime.health
  // Issue #20: a CDC stream that set up but has delivered nothing used to read
  // "Streaming · Streaming pipeline active" here indefinitely.
  const waitingForData = runtime.phase === RUNTIME_PHASE_WAITING_FOR_DATA

  // One-line vital.
  let vital: string
  if (runtime.mode === "cdc") {
    const stale = runtime.liveness?.stale_seconds
    const lastAt = runtime.liveness?.last_event_at
    if (typeof stale === "number") {
      vital = `last event ${relativeFromSeconds(stale)}`
    } else if (lastAt) {
      const secs = (now - new Date(lastAt).getTime()) / 1000
      vital = `last event ${relativeFromSeconds(secs)}`
    } else if (waitingForData) {
      vital = runtime.message || "no data has reached the destination yet"
    } else {
      vital = runtime.message || "streaming"
    }
  } else {
    const p = runtime.progress
    // On a finished run the header must read 100% / step N/N. Previously
    // runtime.progress.percent was rendered verbatim, so a Completed run could
    // still show e.g. "88% · step 7/8" (the last progress tick before the
    // terminal event). Clamp to 100 always (a >100% is never right) and, when
    // the phase is terminal-completed, force 100% and the final step count.
    const done = runtime.phase === "completed"
    if (p && typeof p.percent === "number") {
      const pct = done ? 100 : Math.min(100, Math.round(p.percent))
      const step =
        typeof p.total_steps === "number"
          ? ` · step ${done ? p.total_steps : (p.current_step ?? p.total_steps)}/${p.total_steps}`
          : ""
      vital = `${pct}%${step}`
    } else if (done) {
      vital = "100%"
    } else {
      vital = runtime.message || "—"
    }
  }

  const backlog = backlogVital(runtime)

  const wrapperTone =
    health === "unhealthy"
      ? "border-red-200 bg-red-50/50 dark:border-red-900/40 dark:bg-red-950/10"
      : health === "degraded"
        ? "border-amber-200 bg-amber-50/50 dark:border-amber-900/40 dark:bg-amber-950/10"
        : "border-border bg-muted/30"

  return (
    <div className={cn("rounded-lg border", wrapperTone)}>
      <div className="flex flex-wrap items-center gap-3 px-3 py-2">
        <span className={cn("h-2.5 w-2.5 rounded-full shrink-0", healthDot(health))} />
        <Badge
          variant="outline"
          className={cn(
            waitingForData && "border-amber-300 text-amber-800 dark:border-amber-800 dark:text-amber-300"
          )}
        >
          {runtimePhaseLabel(runtime.phase)}
        </Badge>
        <span className="flex min-w-0 flex-1 items-center gap-1.5 text-xs text-muted-foreground">
          <span className="truncate">{vital}</span>
          {backlog && (
            <>
              <span aria-hidden="true">·</span>
              <span
                data-testid="pipeline-backlog"
                title={backlog.title}
                className={cn(
                  "shrink-0 whitespace-nowrap",
                  backlog.tone === "ok" && "text-emerald-700 dark:text-emerald-400",
                  backlog.tone === "warn" && "font-medium text-amber-700 dark:text-amber-400"
                )}
              >
                {backlog.text}
              </span>
            </>
          )}
        </span>

        {/* Both controls are disclosure toggles for the same panel — neither runs
            anything on its own. DiagnosePanel does not diagnose on mount; it
            renders its own "Run diagnosis" button, which is what issues the
            POST. The labels therefore say "show/hide", not "run": the old
            "Run health check" wording promised a check that never fired and left
            users waiting on a result that was one more click away. */}
        {health !== "healthy" ? (
          <Button
            size="sm"
            variant={health === "unhealthy" ? "destructive" : "default"}
            aria-expanded={showDiagnose}
            aria-controls={diagnosePanelId}
            onClick={() => setShowDiagnose((v) => !v)}
          >
            {showDiagnose ? "Hide diagnostics" : "Diagnose"}
          </Button>
        ) : (
          <button
            type="button"
            aria-expanded={showDiagnose}
            aria-controls={diagnosePanelId}
            onClick={() => setShowDiagnose((v) => !v)}
            className="text-xs text-muted-foreground underline-offset-2 hover:underline"
          >
            {showDiagnose ? "Hide diagnostics" : "Show diagnostics"}
          </button>
        )}
      </div>

      {showDiagnose && (
        <div id={diagnosePanelId} className="border-t border-border/60 px-3 py-2">
          <DiagnosePanel pipelineId={pipelineId} />
        </div>
      )}
    </div>
  )
}
