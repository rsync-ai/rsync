"use client"

import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import Link from "next/link"
import { usePathname, useRouter, useSearchParams } from "next/navigation"
import { toast } from "sonner"
import { BellRing, Download, Edit, FileText, Pencil, Square, Trash2, MoreHorizontal, Loader2, AlertTriangle } from "lucide-react"

import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"
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
  normalizePipelineStatus,
  reconcilePipelineStatus,
  type NormalizedPipelineStatus,
} from "@/lib/pipeline/statusNormalization"
import { usePipelineRuntime } from "@/lib/hooks/usePipelineRuntime"
import { emitPipelineRefresh } from "@/lib/events/pipelineRefresh"
import { readResponseErrorMessage } from "@/lib/utils/error-handling"
import { deleteWarningToast, readDeleteWarnings } from "@/lib/utils/delete-warnings"
import { exportPipelineConfig, pipelineLogsHref } from "@/lib/pipeline/pipelineMenuActions"

export function PipelineHeaderOverflowMenu(props: {
  pipelineId: string
  pipelineName: string
  // When "cdc", this menu owns the "Stop Pipeline" action (CDC pipelines have no
  // inline Stop button — their primary button is Pause). ETL pipelines keep their
  // inline Stop in PipelineActions, so we don't duplicate it here.
  pipelineType?: string
  // Best-effort initial status from the server render; reconciled against /state.
  status?: string
  // The latest execution, for View Logs; none yet opens the Monitor tab.
  lastExecutionId?: string | null
}) {
  const { pipelineId, pipelineName, pipelineType, status, lastExecutionId } = props
  const router = useRouter()
  const pathname = usePathname()
  const searchParams = useSearchParams()

  const [deleteOpen, setDeleteOpen] = useState(false)
  const [deleting, setDeleting] = useState(false)
  // How many schedules the delete is about to remove. Fetched only when the
  // confirm dialog opens — the dialog is the one place it matters, and an
  // "active schedule" is the consequence users are most surprised by.
  const [activeScheduleCount, setActiveScheduleCount] = useState<number | null>(null)
  // The Pipelines list's row menu opens Rename here with ?rename=1 (#52).
  const renameOnLoad = searchParams?.get("rename") === "1"
  const [renameOpen, setRenameOpen] = useState(renameOnLoad)
  const [renameValue, setRenameValue] = useState(renameOnLoad ? pipelineName || "" : "")
  const [renaming, setRenaming] = useState(false)
  const [stopping, setStopping] = useState(false)

  // Live status drives the "Stop Pipeline" item's visibility for CDC pipelines.
  // Poll /state (mirrors CDCPipelineActions) only when we actually own Stop.
  const isCDC = (pipelineType || "").toLowerCase() === "cdc"
  const [liveStatus, setLiveStatus] = useState<NormalizedPipelineStatus>(() => normalizePipelineStatus(status))
  const inFlightRef = useRef(false)
  // /state can freeze at "running" after a CDC feed dies; the dependency-aware
  // /runtime endpoint is the source of truth. Reconcile so a dead stream isn't
  // offered "Stop" (it's not running) — same escalation the status pill and the
  // inline actions use (#673). Only polled for CDC (where we own Stop).
  const { runtime } = usePipelineRuntime(pipelineId, { enabled: isCDC })
  useEffect(() => {
    if (!isCDC) return
    let cancelled = false
    const fetchLiveStatus = async () => {
      try {
        const res = await authFetch(`${API_ENDPOINTS.PIPELINES.GET(pipelineId)}/state`, { cache: "no-store" })
        if (!res.ok) return
        const data = (await res.json()) as { status?: string }
        if (!cancelled) setLiveStatus(normalizePipelineStatus(data?.status))
      } catch {
        // ignore; keep last known status
      }
    }
    void fetchLiveStatus()
    const t = window.setInterval(() => void fetchLiveStatus(), 4000)
    return () => {
      cancelled = true
      window.clearInterval(t)
    }
  }, [isCDC, pipelineId])

  // Escalate a frozen "running" to the /runtime verdict before gating Stop — the
  // shared reconciliation, so this menu cannot drift from the badge beside it.
  const effectiveStatus: NormalizedPipelineStatus = useMemo(
    () => reconcilePipelineStatus(liveStatus, runtime?.phase),
    [liveStatus, runtime?.phase]
  )

  // Count the schedules that would disappear with this pipeline. Deleting the
  // pipeline cascades pipeline_schedules AND removes the external Temporal
  // schedule, so a recurring sync just stops — say so before, not after.
  useEffect(() => {
    if (!deleteOpen) return
    let cancelled = false
    void (async () => {
      try {
        const res = await authFetch(`${API_ENDPOINTS.PIPELINES.GET(pipelineId)}/schedules`, { cache: "no-store" })
        if (!res.ok) return
        const data = (await res.json()) as { schedules?: Array<{ status?: string }> }
        const count = (data?.schedules || []).filter((s) => (s?.status || "").toLowerCase() === "active").length
        if (!cancelled) setActiveScheduleCount(count)
      } catch {
        // Best-effort: the dialog still lists everything else delete removes.
      }
    })()
    return () => {
      cancelled = true
    }
  }, [deleteOpen, pipelineId])

  const runIsInFlight = effectiveStatus === "running" || effectiveStatus === "waiting_for_user"

  // Stop is offered only while the CDC pipeline is actually running (mirrors the
  // old CDCPipelineActions "Stop Pipeline" gate: running or waiting_for_user). A
  // dead stream (failed/idle) no longer qualifies — recovery lives in the inline
  // actions cluster instead. The one exception is a stream /runtime calls failed
  // while /state still has it running: it is still up, and a failed pipeline has
  // no inline Pause (CDCPipelineActions), so Stop here is its non-destructive way
  // down. Without it, Delete would be the only one.
  const canStop =
    isCDC && (runIsInFlight || (effectiveStatus === "failed" && liveStatus === "running"))

  const handleStop = useCallback(async () => {
    if (inFlightRef.current) return
    inFlightRef.current = true
    setStopping(true)
    try {
      const res = await authFetch(API_ENDPOINTS.PIPELINES.STOP(pipelineId), { method: "POST" })
      if (!res.ok) {
        toast.error("Stop failed", { description: await readResponseErrorMessage(res) })
        return
      }
      toast.success("Pipeline stopped")
      setLiveStatus("cancelled")
      router.refresh()
      emitPipelineRefresh(pipelineId)
    } catch {
      toast.error("Stop failed", { description: "Unexpected error stopping pipeline." })
    } finally {
      setStopping(false)
      inFlightRef.current = false
    }
  }, [pipelineId, router])

  const pushWithParams = useCallback(
    (patch: Record<string, string | null | undefined>) => {
      const next = new URLSearchParams(searchParams?.toString() || "")
      for (const [k, v] of Object.entries(patch)) {
        if (v === null || v === undefined || String(v).trim() === "") next.delete(k)
        else next.set(k, String(v))
      }
      const qs = next.toString()
      router.push(qs ? `${pathname}?${qs}` : pathname)
    },
    [router, pathname, searchParams]
  )

  const handleEdit = useCallback(() => {
    // Bring user to the Table statistics tab and open the "Edit tables" modal there.
    // Use a unique value so repeated clicks re-open the modal even if already on that tab.
    pushWithParams({ tab: "table-stats", editTables: String(Date.now()) })
  }, [pushWithParams])

  const openRename = useCallback(() => {
    setRenameValue(pipelineName || "")
    setRenameOpen(true)
  }, [pipelineName])

  // The dialog opened from ?rename=1 starts open; drop the param so a refresh or
  // Back doesn't reopen it.
  useEffect(() => {
    if (!renameOnLoad) return
    const next = new URLSearchParams(searchParams?.toString() || "")
    next.delete("rename")
    const qs = next.toString()
    router.replace(qs ? `${pathname}?${qs}` : pathname)
  }, [renameOnLoad, searchParams, router, pathname])

  const handleRename = useCallback(async () => {
    const next = renameValue.trim()
    if (!next) {
      toast.error("Name cannot be empty")
      return
    }
    if (next === pipelineName) {
      setRenameOpen(false)
      return
    }
    setRenaming(true)
    try {
      const res = await authFetch(API_ENDPOINTS.PIPELINES.UPDATE(pipelineId), {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name: next }),
      })
      if (!res.ok) {
        toast.error("Rename failed", { description: await readResponseErrorMessage(res) })
        return
      }
      toast.success("Pipeline renamed")
      setRenameOpen(false)
      router.refresh()
    } catch (e) {
      toast.error("Rename failed", { description: "Unexpected error renaming pipeline." })
    } finally {
      setRenaming(false)
    }
  }, [pipelineId, pipelineName, renameValue, router])

  const handleDelete = useCallback(async () => {
    setDeleting(true)
    try {
      const res = await authFetch(API_ENDPOINTS.PIPELINES.DELETE(pipelineId), { method: "DELETE" })
      if (!res.ok) {
        toast.error("Delete failed", { description: await readResponseErrorMessage(res) })
        return
      }
      // Same as the pipelines list: a 200 may still report cleanup that did not
      // finish, and the user is about to navigate away from the only page that
      // could have shown it.
      const warnings = await readDeleteWarnings(res)
      if (warnings.length > 0) {
        const { title, description } = deleteWarningToast(warnings)
        toast.warning(title, { description, duration: 12000 })
      } else {
        toast.success("Pipeline deleted")
      }
      setDeleteOpen(false)
      router.push("/pipelines")
    } catch (e) {
      toast.error("Delete failed", { description: "Unexpected error deleting pipeline." })
    } finally {
      setDeleting(false)
    }
  }, [pipelineId, router])

  const deleteTitle = useMemo(() => `Delete ${pipelineName || "pipeline"}?`, [pipelineName])

  return (
    <>
      <DropdownMenu>
        <DropdownMenuTrigger asChild>
          <Button variant="outline" size="sm" aria-label="Open pipeline menu" title="Pipeline actions">
            <MoreHorizontal className="h-4 w-4" />
          </Button>
        </DropdownMenuTrigger>
        <DropdownMenuContent align="end" className="z-50">
          <DropdownMenuItem onSelect={openRename}>
            <Pencil className="h-4 w-4 mr-2" />
            Rename
          </DropdownMenuItem>
          <DropdownMenuItem onSelect={handleEdit}>
            <Edit className="h-4 w-4 mr-2" />
            Edit Tables
          </DropdownMenuItem>
          {/* The drift policy lives on the schema-changes page, above the changes it
              decides about; this is the one route to it when nothing is pending
              (the header's drift badge only renders while something is). */}
          <DropdownMenuItem asChild>
            <Link href={`/pipelines/${pipelineId}/schema-changes`}>
              <BellRing className="h-4 w-4 mr-2" />
              Schema change alerts…
            </Link>
          </DropdownMenuItem>
          {/* Stop is owned here for CDC pipelines (they have no inline Stop button —
              their primary control is Pause). ETL pipelines keep their inline Stop
              in PipelineActions, so this item never renders for them. */}
          {canStop && (
            <DropdownMenuItem
              onSelect={(e) => {
                e.preventDefault()
                void handleStop()
              }}
              disabled={stopping}
            >
              {stopping ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <Square className="h-4 w-4 mr-2" />}
              Stop Pipeline
            </DropdownMenuItem>
          )}
          {/* The list's row menu carries these same items (#52). */}
          <DropdownMenuSeparator />
          <DropdownMenuItem asChild>
            <Link href={pipelineLogsHref(pipelineId, lastExecutionId)}>
              <FileText className="h-4 w-4 mr-2" />
              View Logs
            </Link>
          </DropdownMenuItem>
          <DropdownMenuItem onSelect={() => void exportPipelineConfig(pipelineId)}>
            <Download className="h-4 w-4 mr-2" />
            Export Config
          </DropdownMenuItem>
          <DropdownMenuSeparator />
          <DropdownMenuItem
            className="text-red-600 focus:text-red-600"
            onSelect={(e) => {
              // Keep menu from "selecting" in a way that can steal focus;
              // we just want to open the confirm dialog.
              e.preventDefault()
              setDeleteOpen(true)
            }}
          >
            <Trash2 className="h-4 w-4 mr-2" />
            Delete
          </DropdownMenuItem>
        </DropdownMenuContent>
      </DropdownMenu>

      <AlertDialog open={deleteOpen} onOpenChange={(open) => (!deleting ? setDeleteOpen(open) : null)}>
        <AlertDialogContent className="sm:max-w-[460px]">
          <AlertDialogHeader>
            <AlertDialogTitle className="flex items-center gap-2 text-zinc-900 dark:text-zinc-100">
              <div className="p-2 rounded-full bg-red-100 dark:bg-red-900/30">
                <AlertTriangle className="h-5 w-5 text-red-600 dark:text-red-400" />
              </div>
              {deleteTitle}
            </AlertDialogTitle>
            <AlertDialogDescription asChild>
              <div className="space-y-2 text-zinc-600 dark:text-zinc-400">
                <p>This will delete the pipeline and its configuration. This action cannot be undone.</p>
                <p>It also removes, in this order:</p>
                <ul className="list-disc pl-5 space-y-1 text-sm">
                  {runIsInFlight && (
                    <li className="text-amber-700 dark:text-amber-400">
                      the run currently in flight — it is cancelled, not allowed to finish
                    </li>
                  )}
                  {activeScheduleCount !== null && activeScheduleCount > 0 && (
                    <li className="text-amber-700 dark:text-amber-400">
                      {activeScheduleCount === 1
                        ? "its active schedule — the recurring sync stops"
                        : `its ${activeScheduleCount} active schedules — the recurring syncs stop`}
                    </li>
                  )}
                  <li>its run history and per-run stats</li>
                  {isCDC && <li>its replication slot, publication and CDC connector on the source database</li>}
                  <li>its Kafka topics and consumer groups</li>
                </ul>
                <p className="text-sm">
                  Data already written to the destination is <span className="font-medium">not</span> touched — the
                  synced tables and their rows stay exactly as they are.
                </p>
              </div>
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter className="mt-4">
            <AlertDialogCancel disabled={deleting} className="font-medium">
              Cancel
            </AlertDialogCancel>
            <AlertDialogAction
              onClick={handleDelete}
              disabled={deleting}
              className="bg-red-600 hover:bg-red-700 text-white font-medium"
            >
              {deleting ? (
                <>
                  <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                  Deleting...
                </>
              ) : (
                <>
                  <Trash2 className="h-4 w-4 mr-2" />
                  Delete
                </>
              )}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      {/* Rename dialog — simple text-input flow, fires PATCH /pipelines/:id { name } */}
      <Dialog open={renameOpen} onOpenChange={(open) => (!renaming ? setRenameOpen(open) : null)}>
        <DialogContent className="sm:max-w-[460px]">
          <DialogHeader>
            <DialogTitle>Rename pipeline</DialogTitle>
          </DialogHeader>
          <div className="space-y-3 py-2">
            <Label htmlFor="pipeline-rename-input" className="text-sm text-zinc-700 dark:text-zinc-300">
              New name
            </Label>
            <Input
              id="pipeline-rename-input"
              value={renameValue}
              onChange={(e) => setRenameValue(e.target.value)}
              maxLength={255}
              disabled={renaming}
              autoFocus
              onKeyDown={(e) => {
                if (e.key === "Enter" && !renaming) {
                  void handleRename()
                }
              }}
            />
          </div>
          <div className="flex justify-end gap-2 mt-2">
            <Button variant="outline" onClick={() => setRenameOpen(false)} disabled={renaming}>
              Cancel
            </Button>
            <Button onClick={handleRename} disabled={renaming || !renameValue.trim()}>
              {renaming ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : null}
              Save
            </Button>
          </div>
        </DialogContent>
      </Dialog>
    </>
  )
}

