"use client"

import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import { useRouter } from "next/navigation"
import { Button } from "@/components/ui/button"
import { Play, Pause, Loader2, Square, RefreshCw, AlertTriangle } from "lucide-react"
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog"
import { API_ENDPOINTS } from "@/lib/config/api"
import { authFetch } from "@/lib/api/auth-fetch"
import {
  AssessmentRequiredError,
  PlanLimitError,
  executePipelineWithRunMode,
  getPipeline,
  type AssessmentReport,
  type PlanLimitPayload,
} from "@/lib/api/pipelines"
import { PreMigrationAssessmentModal } from "@/components/pipeline/PreMigrationAssessmentModal"
import { UpgradeModal } from "@/components/plan/UpgradeModal"
import { toast } from "sonner"
import { normalizePipelineStatus, type NormalizedPipelineStatus } from "@/lib/pipeline/statusNormalization"
import { emitPipelineRefresh } from "@/lib/events/pipelineRefresh"
import { classifyError } from "@/lib/utils/error-handling"
import type { ApiErrorBody } from "@/lib/api/types"

interface PipelineActionsProps {
  pipelineId: string
  // Best-effort initial status from the server render. The component reconciles against /state.
  status?: string
}

export function PipelineActions({ pipelineId, status }: PipelineActionsProps) {
  const router = useRouter()
  const [isRunning, setIsRunning] = useState(false)
  const [runMode, setRunMode] = useState<"resume" | "reload" | null>(null)
  const [isActing, setIsActing] = useState(false)
  const inFlightRef = useRef(false)
  const [defaultRunMode, setDefaultRunMode] = useState<"resume" | "reload">("resume")
  const [liveStatus, setLiveStatus] = useState<NormalizedPipelineStatus>(() => normalizePipelineStatus(status))
  // Destination namespace, surfaced in the Reload confirm dialog so the user sees
  // exactly what gets dropped + rebuilt.
  const [destinationNamespace, setDestinationNamespace] = useState<string | null>(null)
  // Destructive-reload confirm dialog. Reload drops + recreates the destination and
  // re-copies all data, so we gate it behind an explicit confirmation (parity with CDC).
  const [reloadConfirmOpen, setReloadConfirmOpen] = useState(false)

  // Pre-migration assessment modal state
  const [assessmentReport, setAssessmentReport] = useState<AssessmentReport | null>(null)
  const [pendingRunMode, setPendingRunMode] = useState<"resume" | "reload" | null>(null)
  const [submittingProceed, setSubmittingProceed] = useState(false)

  // Plan limit modal state
  const [planLimitPayload, setPlanLimitPayload] = useState<PlanLimitPayload | null>(null)

  const fetchLiveStatus = useCallback(async () => {
    try {
      const res = await authFetch(`${API_ENDPOINTS.PIPELINES.GET(pipelineId)}/state`, { cache: "no-store" })
      if (!res.ok) return
      const data = (await res.json()) as { status?: string }
      setLiveStatus(normalizePipelineStatus(data?.status))
    } catch {
      // ignore; keep last known status
    }
  }, [pipelineId])

  useEffect(() => {
    let cancelled = false
    const run = async () => {
      try {
        const p = await getPipeline(pipelineId)
        const drm = String(p.default_run_mode || p.data_loading_strategy?.rerun_default || "")
          .trim()
          .toLowerCase()
        if (!cancelled && (drm === "resume" || drm === "reload")) setDefaultRunMode(drm)
        if (!cancelled && p.destination_namespace) setDestinationNamespace(p.destination_namespace)
      } catch {
        // ignore; default stays resume
      }
    }
    void run()
    return () => {
      cancelled = true
    }
  }, [pipelineId])

  // Keep the header actions consistent with the authoritative pipeline state.
  // This avoids cases where the server-rendered pipeline.status lags behind /state (e.g. execution completes).
  useEffect(() => {
    let cancelled = false
    const run = async () => {
      await fetchLiveStatus()
      if (cancelled) return
      // Poll lightly; the live panel uses WS/polling too, but actions need to flip promptly.
      const t = window.setInterval(() => void fetchLiveStatus(), 4000)
      return () => window.clearInterval(t)
    }
    const cleanupPromise = run()
    return () => {
      cancelled = true
      void cleanupPromise.then((cleanup) => cleanup?.())
    }
  }, [fetchLiveStatus])

  const primaryMode: "resume" | "reload" = useMemo(() => defaultRunMode, [defaultRunMode])
  const secondaryMode: "resume" | "reload" = useMemo(
    () => (primaryMode === "resume" ? "reload" : "resume"),
    [primaryMode]
  )

  // Run the pipeline, surfacing the pre-migration assessment modal if
  // the server gates on warnings. Splits into two phases:
  //   1. First attempt with ackWarnings: false (the default).
  //   2. On 422 + assessment, set state to render the modal; the
  //      modal's Proceed callback re-invokes this with ackWarnings: true.
  const runViaWizard = async (
    mode: "resume" | "reload",
    ackWarnings: boolean,
    nominatedKeys?: Record<string, string[]>,
  ) => {
    if (inFlightRef.current) return { ok: false as const, awaitingAck: false }
    inFlightRef.current = true
    setIsRunning(true)
    setRunMode(mode)
    try {
      await executePipelineWithRunMode(pipelineId, mode, { ackWarnings, nominatedKeys })
      toast.success(mode === "reload" ? "Pipeline reload started" : "Pipeline run started")
      router.refresh()
      void fetchLiveStatus()
      emitPipelineRefresh(pipelineId)
      return { ok: true as const }
    } catch (error) {
      if (error instanceof AssessmentRequiredError) {
        setAssessmentReport(error.report)
        setPendingRunMode(mode)
        return { ok: false as const, awaitingAck: true }
      }
      if (error instanceof PlanLimitError) {
        setPlanLimitPayload(error.payload)
        return { ok: false as const, awaitingAck: false }
      }
      const e = classifyError(error, "pipeline.run")
      toast.error(e.title, { description: e.hint ?? e.message })
      return { ok: false as const, awaitingAck: false }
    } finally {
      setIsRunning(false)
      setRunMode(null)
      inFlightRef.current = false
    }
  }

  const handleRun = (mode: "resume" | "reload" = "resume") => {
    // Reload is destructive (drop + recreate destination, full re-copy) — gate it
    // behind an explicit confirmation. Resume goes straight through.
    if (mode === "reload") {
      setReloadConfirmOpen(true)
      return
    }
    // The destination namespace is confirmed once at first provisioning (in the
    // table-selection HITL), not on every run. Re-runs go straight to the
    // assessment gate, which still validates the destination server-side.
    void runViaWizard(mode, false)
  }

  const handleReloadConfirmed = () => {
    setReloadConfirmOpen(false)
    void runViaWizard("reload", false)
  }

  const handlePause = async () => {
    if (inFlightRef.current) return
    inFlightRef.current = true
    setIsActing(true)
    try {
      const response = await authFetch(API_ENDPOINTS.PIPELINES.PAUSE(pipelineId), {
        method: "POST",
        body: JSON.stringify({}),
      })

      if (response.ok) {
        toast.success("Pipeline paused")
        router.refresh()
        void fetchLiveStatus()
        emitPipelineRefresh(pipelineId)
      } else {
        const body = await response.json().catch(() => ({}))
        const e = classifyError(Object.assign(new Error(), { statusCode: response.status, message: (body as ApiErrorBody)?.error || (body as ApiErrorBody)?.message || response.statusText }), "pipeline.pause")
        toast.error(e.title, { description: e.hint ?? e.message })
      }
    } catch (error) {
      const e = classifyError(error, "pipeline.pause")
      toast.error(e.title, { description: e.hint ?? e.message })
    } finally {
      setIsActing(false)
      inFlightRef.current = false
    }
  }

  const handleResume = async () => {
    if (inFlightRef.current) return
    inFlightRef.current = true
    setIsActing(true)
    try {
      const response = await authFetch(API_ENDPOINTS.PIPELINES.RESUME(pipelineId), {
        method: "POST",
        body: JSON.stringify({}),
      })

      if (response.ok) {
        toast.success("Pipeline resumed")
        router.refresh()
        void fetchLiveStatus()
        emitPipelineRefresh(pipelineId)
      } else {
        const body = await response.json().catch(() => ({}))
        const e = classifyError(Object.assign(new Error(), { statusCode: response.status, message: (body as ApiErrorBody)?.error || (body as ApiErrorBody)?.message || response.statusText }), "pipeline.resume")
        toast.error(e.title, { description: e.hint ?? e.message })
      }
    } catch (error) {
      const e = classifyError(error, "pipeline.resume")
      toast.error(e.title, { description: e.hint ?? e.message })
    } finally {
      setIsActing(false)
      inFlightRef.current = false
    }
  }

  const handleStop = async () => {
    if (inFlightRef.current) return
    inFlightRef.current = true
    setIsActing(true)
    try {
      const response = await authFetch(API_ENDPOINTS.PIPELINES.STOP(pipelineId), { method: "POST" })

      if (response.ok) {
        toast.success("Pipeline stopped")
        router.refresh()
        void fetchLiveStatus()
        emitPipelineRefresh(pipelineId)
      } else {
        const body = await response.json().catch(() => ({}))
        const e = classifyError(Object.assign(new Error(), { statusCode: response.status, message: (body as ApiErrorBody)?.error || (body as ApiErrorBody)?.message || response.statusText }), "pipeline.stop")
        toast.error(e.title, { description: e.hint ?? e.message })
      }
    } catch (error) {
      const e = classifyError(error, "pipeline.stop")
      toast.error(e.title, { description: e.hint ?? e.message })
    } finally {
      setIsActing(false)
      inFlightRef.current = false
    }
  }

  if (liveStatus === "running" || liveStatus === "waiting_for_user") {
    return (
      <div className="flex items-center gap-2">
        <Button
          variant="outline"
          onClick={handlePause}
          disabled={isActing}
          title="Pause the pipeline (best-effort between steps)."
        >
        <Pause className="h-4 w-4 mr-2" />
        Pause
      </Button>
        <Button variant="outline" onClick={handleStop} disabled={isActing} title="Stop the current execution.">
          <Square className="h-4 w-4 mr-2" />
          Stop
        </Button>
      </div>
    )
  }

  if (liveStatus === "paused") {
    return (
      <Button
        onClick={handleResume}
        disabled={isActing}
        className="bg-gradient-to-r from-violet-600 to-indigo-600"
      >
        {isActing ? (
          <>
            <Loader2 className="h-4 w-4 mr-2 animate-spin" />
            Resuming...
          </>
        ) : (
          <>
            <Play className="h-4 w-4 mr-2" />
            Resume
          </>
        )}
      </Button>
    )
  }

  return (
    <div className="flex items-center gap-2">
      <Button
        onClick={() => handleRun(primaryMode)}
        disabled={isRunning}
        className="bg-gradient-to-r from-violet-600 to-indigo-600"
        title={
          primaryMode === "reload"
            ? "Run (Default: Reload): rebuild from scratch (may re-copy full history)."
            : "Run (Default: Resume): continue from the last checkpoint when available."
        }
        aria-label={
          primaryMode === "reload" ? "Run pipeline (default reload from scratch)" : "Run pipeline (default resume)"
        }
      >
        {isRunning && runMode === primaryMode ? (
          <>
            <Loader2 className="h-4 w-4 mr-2 animate-spin" />
            Starting...
          </>
        ) : (
          <>
            <Play className="h-4 w-4 mr-2" />
            {primaryMode === "reload" ? "Run (Reload)" : "Run (Resume)"}
          </>
        )}
      </Button>
      <Button
        variant="outline"
        onClick={() => handleRun(secondaryMode)}
        disabled={isRunning}
        title={
          secondaryMode === "reload"
            ? "Reload: rebuild from scratch (may re-copy full history)."
            : "Resume: continue from the last checkpoint when available."
        }
        aria-label={secondaryMode === "reload" ? "Reload pipeline (rebuild from scratch)" : "Resume pipeline"}
      >
        {isRunning && runMode === secondaryMode ? (
          <>
            <Loader2 className="h-4 w-4 mr-2 animate-spin" />
            {secondaryMode === "reload" ? "Reloading..." : "Resuming..."}
          </>
        ) : (
          <>{secondaryMode === "reload" ? "Reload" : "Resume"}</>
        )}
      </Button>

      {/* Reload confirm — destructive: DROP + recreate the destination, full re-copy. */}
      <AlertDialog open={reloadConfirmOpen} onOpenChange={setReloadConfirmOpen}>
        <AlertDialogContent className="sm:max-w-[480px]">
          <AlertDialogHeader>
            <AlertDialogTitle className="flex items-center gap-2 text-zinc-900 dark:text-zinc-100">
              <div className="p-2 rounded-full bg-amber-100 dark:bg-amber-900/30">
                <AlertTriangle className="h-5 w-5 text-amber-600 dark:text-amber-400" />
              </div>
              Reload from scratch?
            </AlertDialogTitle>
            <AlertDialogDescription asChild>
              <div className="space-y-2 text-zinc-600 dark:text-zinc-400">
                <p>
                  This drops and recreates the destination tables
                  {destinationNamespace ? (
                    <>
                      {" "}
                      in <span className="font-medium text-zinc-800 dark:text-zinc-200">{destinationNamespace}</span>
                    </>
                  ) : null}{" "}
                  and re-copies <span className="font-medium">all</span> data from scratch.
                </p>
                <p>
                  Use <span className="font-medium">Resume</span> instead to continue from the last checkpoint.
                </p>
              </div>
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter className="mt-4">
            <AlertDialogCancel disabled={isRunning} className="font-medium">
              Cancel
            </AlertDialogCancel>
            <AlertDialogAction
              onClick={handleReloadConfirmed}
              disabled={isRunning}
              className="bg-amber-600 hover:bg-amber-700 text-white font-medium"
            >
              <RefreshCw className="h-4 w-4 mr-2" />
              Reload
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      <PreMigrationAssessmentModal
        open={!!assessmentReport}
        onOpenChange={(open) => {
          if (!open) {
            setAssessmentReport(null)
            setPendingRunMode(null)
            setSubmittingProceed(false)
          }
        }}
        report={assessmentReport}
        submitting={submittingProceed}
        onProceed={async (nominatedKeys) => {
          if (!pendingRunMode) return
          setSubmittingProceed(true)
          const result = await runViaWizard(pendingRunMode, true, nominatedKeys)
          // Close on success or non-ack errors; keep open on a fresh
          // 422 (the modal report will have been replaced in state).
          if (!result.awaitingAck) {
            setAssessmentReport(null)
            setPendingRunMode(null)
          }
          setSubmittingProceed(false)
        }}
      />

      <UpgradeModal
        payload={planLimitPayload}
        onClose={() => setPlanLimitPayload(null)}
      />
    </div>
  )
}
