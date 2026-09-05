"use client"

import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import { useRouter } from "next/navigation"
import { Button } from "@/components/ui/button"
import { Play, Pause, Loader2, RotateCcw, RefreshCw, AlertTriangle, LifeBuoy } from "lucide-react"
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
import { toast } from "sonner"
import { API_ENDPOINTS } from "@/lib/config/api"
import { authFetch } from "@/lib/api/auth-fetch"
import {
  AssessmentRequiredError,
  PlanLimitError,
  executePipelineWithRunMode,
  getPipelineCDCStatus,
  recoverPipelineCDC,
  restartPipelineCDC,
  type AssessmentReport,
  type CDCRecoverPlan,
  type PlanLimitPayload,
} from "@/lib/api/pipelines"
import { PreMigrationAssessmentModal } from "@/components/pipeline/PreMigrationAssessmentModal"
import { UpgradeModal } from "@/components/plan/UpgradeModal"
import {
  normalizePipelineStatus,
  pipelineStatusLabel,
  reconcilePipelineStatus,
  type NormalizedPipelineStatus,
} from "@/lib/pipeline/statusNormalization"
import { usePipelineRuntime } from "@/lib/hooks/usePipelineRuntime"
import { emitPipelineRefresh } from "@/lib/events/pipelineRefresh"
import { readResponseErrorMessage } from "@/lib/utils/error-handling"

interface CDCPipelineActionsProps {
  pipelineId: string
  pipelineName: string
  // Best-effort initial status from server render. We reconcile against /state + /runtime.
  status: string
  // Destination connection name, surfaced in the Reload confirm dialog so the user
  // sees exactly which destination will be dropped and rebuilt.
  destinationName?: string
}

export function CDCPipelineActions({ pipelineId, status, destinationName }: CDCPipelineActionsProps) {
  const router = useRouter()
  const [isLoading, setIsLoading] = useState(false)
  const [loadingAction, setLoadingAction] = useState<string | null>(null)
  const [liveStatus, setLiveStatus] = useState<NormalizedPipelineStatus>(() => normalizePipelineStatus(status))
  // Guards against double-invocation (rapid clicks / re-renders) for every action.
  const inFlightRef = useRef(false)

  // Pre-migration assessment modal state (mirrors PipelineActions so CDC pipelines
  // surface source-readiness findings instead of a bare toast).
  const [assessmentReport, setAssessmentReport] = useState<AssessmentReport | null>(null)
  const [pendingRunMode, setPendingRunMode] = useState<"resume" | "reload">("resume")
  const [submittingProceed, setSubmittingProceed] = useState(false)

  // Plan limit modal state (mirrors PipelineActions; surfaces 402 upgrade prompts).
  const [planLimitPayload, setPlanLimitPayload] = useState<PlanLimitPayload | null>(null)

  // Destructive-reload confirm dialog. Reload drops + recreates the destination and
  // re-copies all data, so we gate it behind an explicit confirmation.
  const [reloadConfirmOpen, setReloadConfirmOpen] = useState(false)

  // Operator-guarded CDC recovery — repositions/re-snapshots a FAILED connector
  // without a full destination rebuild. Dormant unless the backend advertises
  // recovery_enabled (CDC_RECOVERY_ENABLED). We dry-run first to preview the plan,
  // then execute on confirm.
  const [recoveryEnabled, setRecoveryEnabled] = useState(false)
  const [recoverOpen, setRecoverOpen] = useState(false)
  const [recoverPlan, setRecoverPlan] = useState<CDCRecoverPlan | null>(null)

  // The dependency-aware /runtime endpoint is the source of truth for whether the CDC
  // stream is actually alive. /state can freeze at "running" after the feed dies, so
  // gating actions on it alone paints a dead stream as "running" (→ Pause) instead of
  // offering recovery. Mirror PipelineExecutionStatusBadge's reconciliation.
  const { runtime } = usePipelineRuntime(pipelineId)

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
    void fetchLiveStatus()
    const t = window.setInterval(() => void fetchLiveStatus(), 4000)
    return () => window.clearInterval(t)
  }, [fetchLiveStatus])

  // Discover whether operator-guarded CDC recovery is enabled on this deployment.
  // One-shot + best-effort: the flag is deployment-static, so there is no need to
  // poll, and a missing/unavailable status simply leaves recovery hidden.
  useEffect(() => {
    let cancelled = false
    void (async () => {
      try {
        const s = await getPipelineCDCStatus(pipelineId)
        if (!cancelled) setRecoveryEnabled(s?.recovery_enabled === true)
      } catch {
        // best-effort: leave recovery hidden when status is unavailable
      }
    })()
    return () => {
      cancelled = true
    }
  }, [pipelineId])

  // Escalate a frozen "running" to the /runtime verdict — the same reconciliation
  // the status pill uses (#673), now shared (see reconcilePipelineStatus).
  const effectiveStatus: NormalizedPipelineStatus = useMemo(
    () => reconcilePipelineStatus(liveStatus, runtime?.phase),
    [liveStatus, runtime?.phase]
  )

  const statusLabel = useMemo(() => pipelineStatusLabel(effectiveStatus), [effectiveStatus])

  const handlePause = async () => {
    if (inFlightRef.current) return
    inFlightRef.current = true
    setIsLoading(true)
    setLoadingAction("pause")
    try {
      const response = await authFetch(API_ENDPOINTS.PIPELINES.CDC_PAUSE(pipelineId), { method: "POST" })
      if (response.ok) {
        toast.success("CDC pipeline paused — Kafka connector stopped")
        router.refresh()
        void fetchLiveStatus()
        emitPipelineRefresh(pipelineId)
      } else {
        toast.error(await readResponseErrorMessage(response, "Pause"))
      }
    } catch (error) {
      toast.error("Failed to pause CDC pipeline")
    } finally {
      setIsLoading(false)
      setLoadingAction(null)
      inFlightRef.current = false
    }
  }

  // Resume the paused Kafka connector (CDC_RESUME). Distinct from a run-mode "resume",
  // which re-executes the workflow from its last checkpoint (see runWithAssessment).
  const handleResumeConnector = async () => {
    if (inFlightRef.current) return
    inFlightRef.current = true
    setIsLoading(true)
    setLoadingAction("resume")
    try {
      const response = await authFetch(API_ENDPOINTS.PIPELINES.CDC_RESUME(pipelineId), { method: "POST" })
      if (response.ok) {
        toast.success("CDC pipeline resumed — Kafka connector restarted")
        router.refresh()
        void fetchLiveStatus()
        emitPipelineRefresh(pipelineId)
      } else {
        toast.error(await readResponseErrorMessage(response, "Resume"))
      }
    } catch (error) {
      toast.error("Failed to resume CDC pipeline")
    } finally {
      setIsLoading(false)
      setLoadingAction(null)
      inFlightRef.current = false
    }
  }

  // Run the CDC pipeline with an explicit run_mode, surfacing the pre-migration
  // assessment modal when the server gates on source-readiness warnings (HTTP 422)
  // and the upgrade modal on plan limits (HTTP 402). Two phases:
  //   1. First attempt with ackWarnings: false.
  //   2. On AssessmentRequiredError, render the modal; its Proceed callback
  //      re-invokes this with ackWarnings: true and the same run_mode.
  //   - resume: continue from the last checkpoint (least destructive re-run).
  //   - reload: rebuild from scratch (drop + recreate destination, full re-copy).
  const runWithAssessment = async (
    mode: "resume" | "reload",
    ackWarnings: boolean,
    nominatedKeys?: Record<string, string[]>,
  ): Promise<{ awaitingAck: boolean }> => {
    if (inFlightRef.current) return { awaitingAck: false }
    inFlightRef.current = true
    setIsLoading(true)
    setLoadingAction(mode === "reload" ? "reload" : "start")
    try {
      await executePipelineWithRunMode(pipelineId, mode, { ackWarnings, nominatedKeys })
      toast.success(mode === "reload" ? "Pipeline reload started" : "Pipeline resumed")
      router.refresh()
      void fetchLiveStatus()
      emitPipelineRefresh(pipelineId)
      return { awaitingAck: false }
    } catch (error) {
      if (error instanceof AssessmentRequiredError) {
        setAssessmentReport(error.report)
        setPendingRunMode(mode)
        return { awaitingAck: true }
      }
      if (error instanceof PlanLimitError) {
        setPlanLimitPayload(error.payload)
        return { awaitingAck: false }
      }
      console.error("Failed to run pipeline:", error)
      toast.error(error instanceof Error ? error.message : "Failed to run pipeline")
      return { awaitingAck: false }
    } finally {
      setIsLoading(false)
      setLoadingAction(null)
      inFlightRef.current = false
    }
  }

  const handleStart = async () => {
    // The destination namespace is confirmed once at first provisioning (in the
    // table-selection HITL), not on every start. Re-runs go straight to the
    // assessment gate, which still validates the destination server-side.
    await runWithAssessment("resume", false)
  }

  const handleReloadConfirmed = async () => {
    setReloadConfirmOpen(false)
    await runWithAssessment("reload", false)
  }

  // Restart the Debezium / Kafka Connect connector without re-copying data — the
  // "bounce the engine" rung of the recovery ladder, above resume-from-checkpoint.
  const handleRestartCDC = async () => {
    if (inFlightRef.current) return
    inFlightRef.current = true
    setIsLoading(true)
    setLoadingAction("restart")
    try {
      await restartPipelineCDC(pipelineId)
      toast.success("CDC restart requested — reconnecting the change stream")
      router.refresh()
      void fetchLiveStatus()
      emitPipelineRefresh(pipelineId)
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "Failed to restart CDC")
    } finally {
      setIsLoading(false)
      setLoadingAction(null)
      inFlightRef.current = false
    }
  }

  // Recovery request shape — audit metadata + the least-destructive reposition
  // defaults (snapshot.mode=recovery, no offset reset). The dry-run previews the
  // exact plan before anything is applied.
  const buildRecoverRequest = (dryRun: boolean) =>
    ({
      operator: "pipeline-detail-ui",
      reason: "Operator-initiated CDC recovery from pipeline detail",
      snapshot_mode: "recovery",
      reset_offsets: false,
      dry_run: dryRun,
    }) as const

  // Step 1: dry-run to fetch the recovery plan, then open the confirm dialog. No
  // side effects — the backend only validates and returns the planned actions.
  const handleRecoverPreview = async () => {
    if (inFlightRef.current) return
    inFlightRef.current = true
    setIsLoading(true)
    setLoadingAction("recover")
    try {
      const resp = await recoverPipelineCDC(pipelineId, buildRecoverRequest(true))
      setRecoverPlan(resp?.plan ?? null)
      setRecoverOpen(true)
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "Failed to preview CDC recovery")
    } finally {
      setIsLoading(false)
      setLoadingAction(null)
      inFlightRef.current = false
    }
  }

  // Step 2: execute the previewed recovery (dry_run omitted → applied).
  const handleRecoverConfirmed = async () => {
    setRecoverOpen(false)
    if (inFlightRef.current) return
    inFlightRef.current = true
    setIsLoading(true)
    setLoadingAction("recover")
    try {
      const resp = await recoverPipelineCDC(pipelineId, buildRecoverRequest(false))
      toast.success(resp?.message || "CDC recovery triggered — reconnecting the change stream")
      router.refresh()
      void fetchLiveStatus()
      emitPipelineRefresh(pipelineId)
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "Failed to recover CDC pipeline")
    } finally {
      setIsLoading(false)
      setLoadingAction(null)
      inFlightRef.current = false
    }
  }

  const isRunning = effectiveStatus === "running" || effectiveStatus === "waiting_for_user"
  const isPaused = effectiveStatus === "paused"
  // Pause keys off the RAW state, not the escalated one. `effectiveStatus` demotes a
  // running stream to "idle" as soon as /runtime reports an idle phase — but that is
  // also what a perfectly healthy stream looks like while the source happens to be
  // quiet. Gating Pause on it therefore withdrew the only non-destructive stop from a
  // live connector and left Delete as the sole way to end the stream. So: the recovery
  // cluster below keeps using `effectiveStatus` (reacting to the dependency-aware phase
  // is its entire job), while Pause uses `liveStatus` — anything actually streaming can
  // always be stopped. When the two disagree, both render, which is the honest answer:
  // "this may be stuck — recover it, or stop it."
  const canPause = liveStatus === "running" || liveStatus === "waiting_for_user"
  // Unhealthy = the change stream broke (failed) or went stale (idle). These states
  // get the "Restart CDC" rung (bounce the connector without touching data).
  const isUnhealthy = effectiveStatus === "failed" || effectiveStatus === "idle"
  // The recovery cluster shows for any state where the pipeline has run and can be
  // re-run or rebuilt (failed / idle / cancelled / completed). A pristine draft
  // (unknown) keeps the single Start button — there is nothing to resume or reload.
  const hasRun = isUnhealthy || effectiveStatus === "cancelled" || effectiveStatus === "completed"
  // Recovering from a broken/stopped stream reads as "Resume" (continue from the last
  // checkpoint); a completed or fresh pipeline reads as "Start".
  const primaryRunLabel =
    effectiveStatus === "failed" || effectiveStatus === "idle" || effectiveStatus === "cancelled" ? "Resume" : "Start"

  return (
    <>
      <div className="flex items-center gap-2">
        {canPause && (
          <Button variant="outline" size="sm" onClick={handlePause} disabled={isLoading}>
            {loadingAction === "pause" ? (
              <Loader2 className="h-4 w-4 mr-2 animate-spin" />
            ) : (
              <Pause className="h-4 w-4 mr-2" />
            )}
            Pause
          </Button>
        )}
        {isRunning ? null : isPaused ? (
          <Button
            size="sm"
            className="bg-gradient-to-r from-violet-600 to-indigo-600"
            onClick={handleResumeConnector}
            disabled={isLoading}
          >
            {loadingAction === "resume" ? (
              <Loader2 className="h-4 w-4 mr-2 animate-spin" />
            ) : (
              <Play className="h-4 w-4 mr-2" />
            )}
            Resume
          </Button>
        ) : hasRun ? (
          // Recovery ladder for a broken / stopped / completed CDC stream, ordered
          // least → most destructive: Resume (checkpoint) · Restart CDC (bounce the
          // connector) · Reload (drop + rebuild, confirm-gated).
          <>
            <Button
              size="sm"
              className="bg-gradient-to-r from-violet-600 to-indigo-600"
              onClick={handleStart}
              disabled={isLoading}
              title={
                primaryRunLabel === "Resume"
                  ? "Resume: continue the change stream from the last checkpoint."
                  : "Start: run the change stream."
              }
            >
              {loadingAction === "start" ? (
                <Loader2 className="h-4 w-4 mr-2 animate-spin" />
              ) : (
                <Play className="h-4 w-4 mr-2" />
              )}
              {primaryRunLabel}
            </Button>
            {isUnhealthy && (
              <Button
                variant="outline"
                size="sm"
                onClick={handleRestartCDC}
                disabled={isLoading}
                title="Restart CDC: bounce the change-data-capture connector without re-copying data."
              >
                {loadingAction === "restart" ? (
                  <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                ) : (
                  <RotateCcw className="h-4 w-4 mr-2" />
                )}
                Restart CDC
              </Button>
            )}
            {recoveryEnabled && isUnhealthy && (
              <Button
                variant="outline"
                size="sm"
                onClick={handleRecoverPreview}
                disabled={isLoading}
                title="Recover: reposition or re-snapshot the change stream to fix a failed connector, without a full reload."
              >
                {loadingAction === "recover" ? (
                  <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                ) : (
                  <LifeBuoy className="h-4 w-4 mr-2" />
                )}
                Recover
              </Button>
            )}
            <Button
              variant="outline"
              size="sm"
              onClick={() => setReloadConfirmOpen(true)}
              disabled={isLoading}
              title="Reload: drop and rebuild the destination, re-copying all data from scratch."
            >
              {loadingAction === "reload" ? (
                <Loader2 className="h-4 w-4 mr-2 animate-spin" />
              ) : (
                <RefreshCw className="h-4 w-4 mr-2" />
              )}
              Reload
            </Button>
          </>
        ) : (
          <Button
            size="sm"
            className="bg-gradient-to-r from-violet-600 to-indigo-600"
            onClick={handleStart}
            disabled={isLoading}
            title={statusLabel}
          >
            {loadingAction === "start" ? (
              <Loader2 className="h-4 w-4 mr-2 animate-spin" />
            ) : (
              <Play className="h-4 w-4 mr-2" />
            )}
            Start
          </Button>
        )}
      </div>

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
                  {destinationName ? (
                    <>
                      {" "}
                      in <span className="font-medium text-zinc-800 dark:text-zinc-200">{destinationName}</span>
                    </>
                  ) : null}{" "}
                  and re-copies <span className="font-medium">all</span> data from scratch.
                </p>
                <p>
                  Any change-stream checkpoint is discarded. Use <span className="font-medium">Resume</span> instead to
                  continue from the last checkpoint.
                </p>
              </div>
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter className="mt-4">
            <AlertDialogCancel disabled={isLoading} className="font-medium">
              Cancel
            </AlertDialogCancel>
            <AlertDialogAction
              onClick={handleReloadConfirmed}
              disabled={isLoading}
              className="bg-amber-600 hover:bg-amber-700 text-white font-medium"
            >
              <RefreshCw className="h-4 w-4 mr-2" />
              Reload
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      {/* Recover confirm — repositions/re-snapshots a FAILED connector without a full
          reload. Opened after a dry-run so the operator reviews the exact plan. */}
      <AlertDialog open={recoverOpen} onOpenChange={setRecoverOpen}>
        <AlertDialogContent className="sm:max-w-[520px]">
          <AlertDialogHeader>
            <AlertDialogTitle className="flex items-center gap-2 text-zinc-900 dark:text-zinc-100">
              <div className="p-2 rounded-full bg-indigo-100 dark:bg-indigo-900/30">
                <LifeBuoy className="h-5 w-5 text-indigo-600 dark:text-indigo-400" />
              </div>
              Recover the change stream?
            </AlertDialogTitle>
            <AlertDialogDescription asChild>
              <div className="space-y-3 text-zinc-600 dark:text-zinc-400">
                <p>
                  This repositions the change-data-capture connector and resumes streaming{" "}
                  <span className="font-medium">without dropping the destination</span> or re-copying all
                  data.
                </p>
                {recoverPlan ? (
                  <dl className="rounded-lg border border-zinc-200 dark:border-zinc-800 divide-y divide-zinc-100 dark:divide-zinc-800 text-sm">
                    {recoverPlan.connector_name ? (
                      <div className="flex justify-between gap-4 px-3 py-2">
                        <dt className="text-zinc-500">Connector</dt>
                        <dd className="font-medium text-zinc-800 dark:text-zinc-200 truncate">
                          {recoverPlan.connector_name}
                        </dd>
                      </div>
                    ) : null}
                    {recoverPlan.connector_state ? (
                      <div className="flex justify-between gap-4 px-3 py-2">
                        <dt className="text-zinc-500">Current state</dt>
                        <dd className="font-medium text-zinc-800 dark:text-zinc-200">
                          {recoverPlan.connector_state}
                        </dd>
                      </div>
                    ) : null}
                    {recoverPlan.snapshot_mode ? (
                      <div className="flex justify-between gap-4 px-3 py-2">
                        <dt className="text-zinc-500">Snapshot mode</dt>
                        <dd className="font-medium text-zinc-800 dark:text-zinc-200">
                          {recoverPlan.snapshot_mode}
                        </dd>
                      </div>
                    ) : null}
                    {typeof recoverPlan.table_count === "number" ? (
                      <div className="flex justify-between gap-4 px-3 py-2">
                        <dt className="text-zinc-500">Tables</dt>
                        <dd className="font-medium text-zinc-800 dark:text-zinc-200">
                          {recoverPlan.table_count}
                        </dd>
                      </div>
                    ) : null}
                    <div className="flex justify-between gap-4 px-3 py-2">
                      <dt className="text-zinc-500">Reset offsets</dt>
                      <dd className="font-medium text-zinc-800 dark:text-zinc-200">
                        {recoverPlan.reset_offsets ? "Yes" : "No"}
                      </dd>
                    </div>
                  </dl>
                ) : null}
                <p className="text-xs">
                  If the stream cannot be repositioned, use <span className="font-medium">Reload</span> to
                  rebuild from scratch.
                </p>
              </div>
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter className="mt-4">
            <AlertDialogCancel disabled={isLoading} className="font-medium">
              Cancel
            </AlertDialogCancel>
            <AlertDialogAction
              onClick={handleRecoverConfirmed}
              disabled={isLoading}
              className="bg-indigo-600 hover:bg-indigo-700 text-white font-medium"
            >
              <LifeBuoy className="h-4 w-4 mr-2" />
              Run recovery
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      {/* Pre-migration assessment modal — source-readiness findings/remediation */}
      <PreMigrationAssessmentModal
        open={!!assessmentReport}
        onOpenChange={(open) => {
          if (!open) {
            setAssessmentReport(null)
            setSubmittingProceed(false)
          }
        }}
        report={assessmentReport}
        submitting={submittingProceed}
        onProceed={async (nominatedKeys) => {
          setSubmittingProceed(true)
          const result = await runWithAssessment(pendingRunMode, true, nominatedKeys)
          // Close on success or non-ack errors; keep open on a fresh 422
          // (the modal report will have been replaced in state).
          if (!result.awaitingAck) {
            setAssessmentReport(null)
          }
          setSubmittingProceed(false)
        }}
      />

      <UpgradeModal
        payload={planLimitPayload}
        onClose={() => setPlanLimitPayload(null)}
      />
    </>
  )
}
