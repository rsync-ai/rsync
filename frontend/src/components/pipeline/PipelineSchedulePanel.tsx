"use client"

import { useMemo, useRef, useState, useEffect } from "react"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { RadioGroup, RadioGroupItem } from "@/components/ui/radio-group"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Checkbox } from "@/components/ui/checkbox"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogTrigger,
} from "@/components/ui/alert-dialog"
import { Clock, Play, Pause, Trash2, Plus, Calendar, Pencil, Loader2, AlertTriangle } from "lucide-react"
import { API_ENDPOINTS } from "@/lib/config/api"
import { authFetch, authFetchOrThrow } from "@/lib/api/auth-fetch"
import { classifyError } from "@/lib/utils/error-handling"
import { toast } from "sonner"
import {
  onPipelineScheduleCreate,
  onPipelineSchedulesChanged,
  emitPipelineSchedulesChanged,
} from "@/lib/events/scheduleCreate"
import type { ApiErrorBody } from "@/lib/api/types"

type TriggerStatus = "active" | "paused"
type ScheduleType = "cron" | "interval"
type CronUiMode = "simple" | "advanced"
type SimpleCadence = "hour" | "day" | "week"
type Weekday = "0" | "1" | "2" | "3" | "4" | "5" | "6" // 0=Sunday

function pad2(n: number): string {
  return String(n).padStart(2, "0")
}

function formatUtcOffsetFromMinutes(totalMinutes: number): string {
  const sign = totalMinutes >= 0 ? "+" : "-"
  const abs = Math.abs(totalMinutes)
  const hh = Math.floor(abs / 60)
  const mm = abs % 60
  return `UTC${sign}${pad2(hh)}:${pad2(mm)}`
}

function getTimezoneOffsetMinutes(date: Date, timeZone: string): number | null {
  try {
    // Compute offset minutes by formatting the same instant in a target TZ,
    // then interpreting those parts as if they were UTC.
    const dtf = new Intl.DateTimeFormat("en-US", {
      timeZone,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hour12: false,
    })
    const parts = dtf.formatToParts(date)
    const map: Record<string, string> = {}
    for (const p of parts) map[p.type] = p.value
    const asUTC = Date.UTC(
      Number(map.year),
      Number(map.month) - 1,
      Number(map.day),
      Number(map.hour),
      Number(map.minute),
      Number(map.second)
    )
    return Math.round((asUTC - date.getTime()) / 60000)
  } catch {
    return null
  }
}

function buildSimpleCron(args: { cadence: SimpleCadence; hour: string; minute: string; weekday: Weekday }): string {
  const minute = String(args.minute || "0").trim()
  const hour = String(args.hour || "0").trim()
  switch (args.cadence) {
    case "hour":
      return `${minute} * * * *`
    case "week":
      return `${minute} ${hour} * * ${args.weekday}`
    case "day":
    default:
      return `${minute} ${hour} * * *`
  }
}

function parseSimpleCron(cron: string): null | { cadence: SimpleCadence; hour: string; minute: string; weekday: Weekday } {
  const parts = String(cron || "")
    .trim()
    .split(/\s+/)
    .filter(Boolean)
  if (parts.length < 5) return null
  const [min, hour, dom, mon, dow] = parts

  // Hourly: M * * * *
  if (/^\d+$/.test(min) && hour === "*" && dom === "*" && mon === "*" && dow === "*") {
    return { cadence: "hour", minute: pad2(Number(min)), hour: "00", weekday: "0" }
  }

  // Daily: M H * * *
  if (/^\d+$/.test(min) && /^\d+$/.test(hour) && dom === "*" && mon === "*" && dow === "*") {
    return { cadence: "day", minute: pad2(Number(min)), hour: pad2(Number(hour)), weekday: "0" }
  }

  // Weekly: M H * * D
  if (/^\d+$/.test(min) && /^\d+$/.test(hour) && dom === "*" && mon === "*" && /^\d+$/.test(dow)) {
    const d = Number(dow)
    if (d >= 0 && d <= 6) return { cadence: "week", minute: pad2(Number(min)), hour: pad2(Number(hour)), weekday: String(d) as Weekday }
  }

  return null
}

interface Schedule {
  schedule_id: string
  pipeline_id: string
  schedule_type: "cron" | "interval"
  schedule_spec: {
    cron?: string
    every_seconds?: number
    timezone?: string
  }
  temporal_schedule_id: string
  status: "active" | "paused" | "deleted"
  created_by: string
  created_at: string
  updated_at: string
  paused_at?: string
  paused_reason?: string
  // Computed server-side, not stored: an `active` schedule whose pipeline is
  // parked on a HITL prompt keeps firing but every tick is skipped, so the
  // badge alone would read "active" while no runs happen.
  blocked?: boolean
  blocked_reason?: string
}

interface PipelineSchedulePanelProps {
  pipelineId: string
}

async function createPipelineSchedule(pipelineId: string, scheduleType: string, spec: any): Promise<Schedule | null> {
  const response = await authFetch(`${API_ENDPOINTS.PIPELINES.GET(pipelineId)}/schedules`, {
    method: "POST",
    body: JSON.stringify({
      schedule_type: scheduleType,
      schedule_spec: spec,
    }),
  })

  if (!response.ok) {
    const err = await response.json().catch(() => null)
    throw new Error((err as ApiErrorBody)?.error || "Failed to create schedule")
  }

  return (await response.json().catch(() => null)) as Schedule | null
}

export function PipelineScheduleCreateDialogLauncher({ pipelineId }: { pipelineId: string }) {
  const [open, setOpen] = useState(false)

  // Open schedule dialog immediately from header action (no navigation).
  useEffect(() => {
    return onPipelineScheduleCreate((pid) => {
      if (pid !== pipelineId) return
      setOpen(true)
    })
  }, [pipelineId])

  // Back-compat deep-link support: /pipelines/:id?createSchedule=1
  useEffect(() => {
    try {
      const url = new URL(window.location.href)
      const createSchedule = url.searchParams.get("createSchedule")
      if (!createSchedule) return
      if (!["1", "true", "yes"].includes(createSchedule.toLowerCase())) return
      setOpen(true)
      url.searchParams.delete("createSchedule")
      window.history.replaceState({}, "", url.toString())
    } catch {
      // ignore
    }
  }, [])

  const handleCreateSchedule = async (scheduleType: string, spec: any) => {
    try {
      const created = await createPipelineSchedule(pipelineId, scheduleType, spec)
      toast.success("Schedule created successfully")
      setOpen(false)

      // If user chose "Paused", create then pause immediately (best effort).
      if (created?.schedule_id && spec?.__desired_status === "paused") {
        try {
          await authFetch(`${API_ENDPOINTS.PIPELINES.GET(pipelineId)}/schedules/${created.schedule_id}/pause`, {
            method: "PATCH",
            body: JSON.stringify({ reason: "Paused on creation" }),
          })
        } catch (e) {
          console.warn("Schedule created but failed to pause:", e)
        }
      }

      // Emit last, after the optional pause, so listeners refetch the final state
      // rather than the transient active-then-paused one.
      emitPipelineSchedulesChanged(pipelineId)
    } catch (e) {
      toast.error(e instanceof Error ? e.message : "Failed to create schedule")
      console.error(e)
    }
  }

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      {/* Keep a stable client boundary even when closed */}
      <div className="hidden" aria-hidden />
      <CreateScheduleDialog mode="create" onSubmit={handleCreateSchedule} onCancel={() => setOpen(false)} />
    </Dialog>
  )
}

export function PipelineSchedulePanel({ pipelineId }: PipelineSchedulePanelProps) {
  const [schedules, setSchedules] = useState<Schedule[]>([])
  const [loading, setLoading] = useState(true)
  const [showCreateDialog, setShowCreateDialog] = useState(false)
  const [showEditDialog, setShowEditDialog] = useState(false)
  const [editingSchedule, setEditingSchedule] = useState<Schedule | null>(null)
  const [highlightScheduleId, setHighlightScheduleId] = useState<string | null>(null)
  const [busyScheduleId, setBusyScheduleId] = useState<string | null>(null)
  const [loadError, setLoadError] = useState<string | null>(null)
  const schedulesSectionRef = useRef<HTMLDivElement | null>(null)

  useEffect(() => {
    fetchSchedules()
  }, [pipelineId])

  // Schedules are also mutated outside this panel — the header's "Create schedule"
  // action renders its own dialog (PipelineScheduleCreateDialogLauncher). That path had
  // no way to tell us anything changed, so creating from the header left this panel
  // reading "No schedules configured" until a hard reload, while deleting from the
  // panel's own trash icon refreshed instantly. Same state, two answers, depending on
  // which button you used.
  useEffect(() => {
    return onPipelineSchedulesChanged((pid) => {
      if (pid !== pipelineId) return
      fetchSchedules()
    })
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [pipelineId])

  // Mutations refetch locally AND broadcast, so the strategy card ("Batch (scheduled)"
  // vs "Batch (manual)") re-derives too. The subscription above deliberately calls the
  // bare fetchSchedules so a broadcast can never trigger another broadcast.
  const refreshSchedules = () => {
    fetchSchedules()
    emitPipelineSchedulesChanged(pipelineId)
  }

  // Deep-link support: /pipelines/:id?scheduleId=...#schedules
  useEffect(() => {
    try {
      const url = new URL(window.location.href)
      const sid = url.searchParams.get("scheduleId")
      if (sid) {
        setHighlightScheduleId(sid)
        // Scroll schedules section into view
        schedulesSectionRef.current?.scrollIntoView({ behavior: "smooth", block: "start" })
        // Clear highlight after a short time
        const t = window.setTimeout(() => setHighlightScheduleId(null), 4500)
        return () => window.clearTimeout(t)
      }
    } catch {
      // ignore
    }
  }, [])

  const fetchSchedules = async () => {
    try {
      const response = await authFetchOrThrow(`${API_ENDPOINTS.PIPELINES.GET(pipelineId)}/schedules`, {
        cache: "no-store",
      })
      const data = await response.json()
      setSchedules(data.schedules || [])
      setLoadError(null)
    } catch (error) {
      // A failed read used to leave `schedules` empty, which renders "No
      // schedules configured" plus a Create Schedule button — so the user's
      // next move is to create a second schedule for a pipeline that already
      // has one, and the backend's one-per-pipeline rule rejects it.
      setLoadError(classifyError(error, "pipeline.load").message)
    } finally {
      setLoading(false)
    }
  }

  const handleCreateSchedule = async (scheduleType: string, spec: any) => {
    try {
      const created = await createPipelineSchedule(pipelineId, scheduleType, spec)
        toast.success("Schedule created successfully")
        setShowCreateDialog(false)
        // If user chose "Paused", create then pause immediately.
        if (created?.schedule_id && spec?.__desired_status === "paused") {
          try {
            await authFetchOrThrow(`${API_ENDPOINTS.PIPELINES.GET(pipelineId)}/schedules/${created.schedule_id}/pause`, {
              method: "PATCH",
              body: JSON.stringify({ reason: "Paused on creation" }),
            })
          } catch (e) {
            // Not cosmetic: the user asked for a PAUSED schedule and got an
            // ACTIVE one. Swallowing this means the pipeline runs on a cadence
            // nobody agreed to, and the success toast above already fired.
            toast.error("Schedule created, but it is ACTIVE — pausing it failed", {
              description: classifyError(e, "general").message,
            })
          }
        }
        refreshSchedules()
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "Failed to create schedule")
      console.error(error)
    }
  }

  const handleUpdateSchedule = async (scheduleId: string, scheduleType: string, spec: any) => {
    try {
      if (busyScheduleId) return
      setBusyScheduleId(scheduleId)
      const response = await authFetch(`${API_ENDPOINTS.PIPELINES.GET(pipelineId)}/schedules/${scheduleId}`, {
        method: "PATCH",
        body: JSON.stringify({
          schedule_type: scheduleType,
          schedule_spec: spec,
        }),
      })

      if (response.ok) {
        toast.success("Schedule updated")
        // Apply desired status change (best effort).
        const desired = spec?.__desired_status as TriggerStatus | undefined
        const current = editingSchedule?.status as TriggerStatus | undefined
        if (desired && desired !== current) {
          try {
            if (desired === "paused") {
              await authFetchOrThrow(`${API_ENDPOINTS.PIPELINES.GET(pipelineId)}/schedules/${scheduleId}/pause`, {
                method: "PATCH",
                body: JSON.stringify({ reason: "Paused by user" }),
              })
            } else {
              await authFetchOrThrow(`${API_ENDPOINTS.PIPELINES.GET(pipelineId)}/schedules/${scheduleId}/resume`, {
                method: "PATCH",
              })
            }
          } catch (e) {
            // Same shape as the create path: the timing changes landed but the
            // active/paused switch did not, and "Schedule updated" already said
            // otherwise.
            toast.error(
              desired === "paused"
                ? "Schedule updated, but it is still ACTIVE — pausing it failed"
                : "Schedule updated, but it is still PAUSED — resuming it failed",
              { description: classifyError(e, "general").message }
            )
          }
        }
        setShowEditDialog(false)
        setEditingSchedule(null)
        refreshSchedules()
      } else {
        const error = await response.json().catch(() => ({}))
        toast.error(error.error || "Failed to update schedule")
      }
    } catch (error) {
      toast.error("Failed to update schedule")
      console.error(error)
    } finally {
      setBusyScheduleId(null)
    }
  }

  const handlePauseSchedule = async (scheduleId: string) => {
    try {
      if (busyScheduleId) return
      setBusyScheduleId(scheduleId)
      const response = await authFetch(
        `${API_ENDPOINTS.PIPELINES.GET(pipelineId)}/schedules/${scheduleId}/pause`,
        {
          method: "PATCH",
          body: JSON.stringify({ reason: "Paused by user" }),
        }
      )

      if (response.ok) {
        toast.success("Schedule paused")
        refreshSchedules()
      } else {
        toast.error("Failed to pause schedule")
      }
    } catch (error) {
      toast.error("Failed to pause schedule")
      console.error(error)
    } finally {
      setBusyScheduleId(null)
    }
  }

  const handleResumeSchedule = async (scheduleId: string) => {
    try {
      if (busyScheduleId) return
      setBusyScheduleId(scheduleId)
      const response = await authFetch(
        `${API_ENDPOINTS.PIPELINES.GET(pipelineId)}/schedules/${scheduleId}/resume`,
        {
          method: "PATCH",
        }
      )

      if (response.ok) {
        toast.success("Schedule resumed")
        refreshSchedules()
      } else {
        toast.error("Failed to resume schedule")
      }
    } catch (error) {
      toast.error("Failed to resume schedule")
      console.error(error)
    } finally {
      setBusyScheduleId(null)
    }
  }

  const handleTriggerSchedule = async (scheduleId: string) => {
    try {
      if (busyScheduleId) return
      setBusyScheduleId(scheduleId)
      const response = await authFetch(
        `${API_ENDPOINTS.PIPELINES.GET(pipelineId)}/schedules/${scheduleId}/trigger`,
        {
          method: "POST",
        }
      )

      if (response.ok) {
        toast.success("Pipeline execution triggered")
      } else {
        toast.error("Failed to trigger execution")
      }
    } catch (error) {
      toast.error("Failed to trigger execution")
      console.error(error)
    } finally {
      setBusyScheduleId(null)
    }
  }

  const handleDeleteSchedule = async (scheduleId: string) => {
    try {
      if (busyScheduleId) return
      setBusyScheduleId(scheduleId)
      const response = await authFetch(
        `${API_ENDPOINTS.PIPELINES.GET(pipelineId)}/schedules/${scheduleId}`,
        {
          method: "DELETE",
        }
      )

      if (response.ok) {
        toast.success("Schedule deleted")
        refreshSchedules()
      } else {
        toast.error("Failed to delete schedule")
      }
    } catch (error) {
      toast.error("Failed to delete schedule")
      console.error(error)
    } finally {
      setBusyScheduleId(null)
    }
  }

  if (loading) {
    return <div className="text-sm text-muted-foreground">Loading schedules...</div>
  }

  return (
    <div id="schedules" ref={schedulesSectionRef} className="space-y-4 scroll-mt-24">
      <div className="flex items-center justify-between">
        <div>
          <h3 className="text-lg font-semibold">Pipeline Schedules</h3>
          <p className="text-sm text-muted-foreground">Automate pipeline executions with recurring schedules</p>
        </div>
        {/* Only one schedule per pipeline (backend-enforced). If a schedule exists, show Edit/Pause/Delete on the card.
            Hidden while the list is unknown: offering Create on top of a failed
            read invites a duplicate the backend will reject. */}
        {schedules.length === 0 && !loadError && (
          <Button size="sm" onClick={() => setShowCreateDialog(true)}>
            <Plus className="mr-2 h-4 w-4" />
            Create Schedule
          </Button>
        )}
        {showCreateDialog && (
          <Dialog open={showCreateDialog} onOpenChange={setShowCreateDialog}>
            <CreateScheduleDialog
              mode="create"
              onSubmit={handleCreateSchedule}
              onCancel={() => setShowCreateDialog(false)}
            />
          </Dialog>
        )}
        {showEditDialog && editingSchedule && (
          <Dialog open={showEditDialog} onOpenChange={setShowEditDialog}>
            <CreateScheduleDialog
              mode="edit"
              initialSchedule={editingSchedule}
              onSubmit={(scheduleType, spec) => handleUpdateSchedule(editingSchedule.schedule_id, scheduleType, spec)}
              onCancel={() => {
                setShowEditDialog(false)
                setEditingSchedule(null)
              }}
            />
          </Dialog>
        )}
      </div>

      {loadError ? (
        <Card>
          <CardContent className="flex flex-col items-center justify-center py-12 gap-3">
            <AlertTriangle className="h-10 w-10 text-red-500" />
            <p className="text-sm text-center text-red-700 dark:text-red-400">
              Could not load schedules — {loadError}
              <br />
              <span className="text-muted-foreground">
                This is not the same as having none; the existing schedule may still be active.
              </span>
            </p>
            <Button size="sm" variant="outline" onClick={fetchSchedules}>
              Retry
            </Button>
          </CardContent>
        </Card>
      ) : schedules.length === 0 ? (
        <Card>
          <CardContent className="flex flex-col items-center justify-center py-12">
            <Calendar className="h-12 w-12 text-muted-foreground mb-4" />
            <p className="text-sm text-muted-foreground text-center">
              No schedules configured
              <br />
              Create a schedule to run this pipeline automatically
            </p>
          </CardContent>
        </Card>
      ) : (
        <div className="space-y-3">
          {schedules.map((schedule) => (
            <ScheduleCard
              key={schedule.schedule_id}
              schedule={schedule}
              highlight={highlightScheduleId === schedule.schedule_id}
              busy={busyScheduleId === schedule.schedule_id}
              onEdit={() => {
                setEditingSchedule(schedule)
                setShowEditDialog(true)
              }}
              onPause={() => handlePauseSchedule(schedule.schedule_id)}
              onResume={() => handleResumeSchedule(schedule.schedule_id)}
              onTrigger={() => handleTriggerSchedule(schedule.schedule_id)}
              onDelete={() => handleDeleteSchedule(schedule.schedule_id)}
            />
          ))}
        </div>
      )}
    </div>
  )
}

interface ScheduleCardProps {
  schedule: Schedule
  highlight?: boolean
  busy?: boolean
  onEdit: () => void
  onPause: () => void
  onResume: () => void
  onTrigger: () => void
  onDelete: () => void | Promise<void>
}

function ScheduleCard({ schedule, highlight, busy, onEdit, onPause, onResume, onTrigger, onDelete }: ScheduleCardProps) {
  const formatSchedule = (schedule: Schedule) => {
    if (schedule.schedule_type === "cron") {
      return `Cron: ${schedule.schedule_spec.cron}`
    } else {
      const seconds = schedule.schedule_spec.every_seconds || 0
      const minutes = Math.floor(seconds / 60)
      const hours = Math.floor(minutes / 60)
      if (hours > 0) {
        return `Every ${hours} hour${hours > 1 ? "s" : ""}`
      } else if (minutes > 0) {
        return `Every ${minutes} minute${minutes > 1 ? "s" : ""}`
      } else {
        return `Every ${seconds} second${seconds > 1 ? "s" : ""}`
      }
    }
  }

  return (
    <Card
      data-schedule-id={schedule.schedule_id}
      className={highlight ? "ring-2 ring-violet-500 ring-offset-2 ring-offset-background" : undefined}
    >
      <CardHeader className="pb-3">
        <div className="flex items-start justify-between">
          <div className="space-y-1">
            <div className="flex items-center gap-2">
              <Clock className="h-4 w-4 text-muted-foreground" />
              <CardTitle className="text-base">{formatSchedule(schedule)}</CardTitle>
              <Badge variant={schedule.status === "active" ? "default" : "secondary"}>
                {schedule.status}
              </Badge>
              {schedule.blocked && (
                <Badge variant="outline" className="border-amber-500/60 text-amber-600 dark:text-amber-400">
                  not running
                </Badge>
              )}
            </div>
            <CardDescription className="text-xs">
              Timezone: {schedule.schedule_spec.timezone || "UTC"}
            </CardDescription>
          </div>
          <div className="flex items-center gap-1">
            <Button size="sm" variant="outline" onClick={onEdit} title="Edit trigger" disabled={busy}>
              <span className="inline-flex items-center gap-2">
                <Pencil className="h-3 w-3" />
                <span className="hidden sm:inline">Edit trigger</span>
              </span>
            </Button>
            <Button size="sm" variant="outline" onClick={onTrigger} title="Trigger now" disabled={busy}>
              <Play className="h-3 w-3" />
            </Button>
            {schedule.status === "active" ? (
              <Button size="sm" variant="outline" onClick={onPause} title="Pause" disabled={busy}>
                <Pause className="h-3 w-3" />
              </Button>
            ) : (
              <Button size="sm" variant="outline" onClick={onResume} title="Resume" disabled={busy}>
                <Play className="h-3 w-3" />
              </Button>
            )}
            <AlertDialog>
              <AlertDialogTrigger asChild>
                <Button size="sm" variant="outline" title="Delete" disabled={busy}>
              <Trash2 className="h-3 w-3" />
            </Button>
              </AlertDialogTrigger>
              <AlertDialogContent>
                <AlertDialogHeader>
                  <AlertDialogTitle>Delete schedule?</AlertDialogTitle>
                  <AlertDialogDescription>
                    This will remove the recurring trigger and stop future automated runs for this pipeline. You can create a new schedule anytime.
                  </AlertDialogDescription>
                </AlertDialogHeader>
                <AlertDialogFooter>
                  <AlertDialogCancel>Cancel</AlertDialogCancel>
                  <AlertDialogAction
                    className="bg-red-600 hover:bg-red-700 focus:ring-red-600"
                    onClick={() => void onDelete()}
                    disabled={busy}
                  >
                    Delete
                  </AlertDialogAction>
                </AlertDialogFooter>
              </AlertDialogContent>
            </AlertDialog>
          </div>
        </div>
      </CardHeader>
      {(schedule.paused_reason || schedule.blocked_reason) && (
        <CardContent className="space-y-2 pt-0">
          {schedule.paused_reason && (
            <p className="text-xs text-muted-foreground">Paused: {schedule.paused_reason}</p>
          )}
          {/* An `active` schedule that produces no runs looks identical to a
              healthy one from the badge alone — this is the only place the
              skipped ticks are visible. */}
          {schedule.blocked_reason && (
            <div className="flex items-start gap-2 rounded-md border border-amber-500/40 bg-amber-500/10 p-2">
              <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0 text-amber-600 dark:text-amber-400" />
              <p className="text-xs text-amber-700 dark:text-amber-300">{schedule.blocked_reason}</p>
            </div>
          )}
        </CardContent>
      )}
    </Card>
  )
}

interface CreateScheduleDialogProps {
  mode?: "create" | "edit"
  initialSchedule?: Schedule
  onSubmit: (scheduleType: string, spec: any) => Promise<void>
  onCancel: () => void
}

function CreateScheduleDialog({ mode = "create", initialSchedule, onSubmit, onCancel }: CreateScheduleDialogProps) {
  const initialType = (initialSchedule?.schedule_type as ScheduleType | undefined) || "interval"
  const initialTz =
    initialSchedule?.schedule_spec?.timezone ||
    (() => {
      try {
        return Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC"
      } catch {
        return "UTC"
      }
    })()
  const initialCron = initialSchedule?.schedule_spec?.cron || "0 * * * *"
  const initialEvery = String(initialSchedule?.schedule_spec?.every_seconds ?? 3600)
  const initialStatus = (initialSchedule?.status as TriggerStatus | undefined) || "active"

  const [scheduleType, setScheduleType] = useState<ScheduleType>(initialType)
  const [triggerStatus, setTriggerStatus] = useState<TriggerStatus>(initialStatus)
  const [cronUiMode, setCronUiMode] = useState<CronUiMode>("simple")

  const [cronExpr, setCronExpr] = useState(initialCron)
  const [intervalSeconds, setIntervalSeconds] = useState(initialEvery)
  const [timezone, setTimezone] = useState(initialTz)
  const [simpleCadence, setSimpleCadence] = useState<SimpleCadence>("day")
  const [simpleWeekday, setSimpleWeekday] = useState<Weekday>("1")
  const [simpleHour, setSimpleHour] = useState("02")
  const [simpleMinute, setSimpleMinute] = useState("00")
  const [showCronSyntax, setShowCronSyntax] = useState(false)

  // Full timezone list (IANA) with graceful fallback.
  const timezones = useMemo(() => {
    try {
      // Modern browsers (Chrome/Edge/Firefox): returns IANA tz names
      const list = (Intl as any).supportedValuesOf?.("timeZone") as string[] | undefined
      if (Array.isArray(list) && list.length > 0) {
        return [...list].sort((a, b) => a.localeCompare(b))
      }
    } catch {
      // ignore
    }
    // Fallback: minimal set (should almost never be used on desktop Chrome)
    return [
      "UTC",
      "America/New_York",
      "America/Chicago",
      "America/Denver",
      "America/Los_Angeles",
      "Europe/London",
      "Europe/Paris",
      "Asia/Tokyo",
      "Asia/Shanghai",
    ]
  }, [])

  const timezoneOptions = useMemo(() => {
    const now = new Date()
    const items = timezones.map((tz) => {
      const off = getTimezoneOffsetMinutes(now, tz)
      const label = off === null ? tz : `(${formatUtcOffsetFromMinutes(off)}) ${tz}`
      return { tz, label, off }
    })
    // Similar to Databricks: group by offset, then name.
    items.sort((a, b) => (a.off ?? 999999) - (b.off ?? 999999) || a.tz.localeCompare(b.tz))
    return items
  }, [timezones])

  const hours = useMemo(() => Array.from({ length: 24 }, (_, i) => pad2(i)), [])
  const minutes = useMemo(() => Array.from({ length: 60 }, (_, i) => pad2(i)), [])

  const weekdayOptions: Array<{ value: Weekday; label: string }> = useMemo(
    () => [
      { value: "0", label: "Sunday" },
      { value: "1", label: "Monday" },
      { value: "2", label: "Tuesday" },
      { value: "3", label: "Wednesday" },
      { value: "4", label: "Thursday" },
      { value: "5", label: "Friday" },
      { value: "6", label: "Saturday" },
    ],
    []
  )

  const simpleCron = useMemo(
    () => buildSimpleCron({ cadence: simpleCadence, hour: simpleHour, minute: simpleMinute, weekday: simpleWeekday }),
    [simpleCadence, simpleHour, simpleMinute, simpleWeekday]
  )

  useEffect(() => {
    // When editing, try to infer a "simple" schedule from existing cron.
    if (mode !== "edit") return
    if (!initialSchedule || initialSchedule.schedule_type !== "cron") return
    const parsed = parseSimpleCron(initialSchedule.schedule_spec?.cron || "")
    if (!parsed) {
      setCronUiMode("advanced")
      return
    }
    setCronUiMode("simple")
    setSimpleCadence(parsed.cadence)
    setSimpleMinute(parsed.minute)
    if (parsed.cadence !== "hour") setSimpleHour(parsed.hour)
    if (parsed.cadence === "week") setSimpleWeekday(parsed.weekday)
  }, [mode, initialSchedule])

  const [isSubmitting, setIsSubmitting] = useState(false)

  const handleSubmit = async () => {
    if (isSubmitting) return
    setIsSubmitting(true)
    const spec: any = { timezone, __desired_status: triggerStatus }

    if (scheduleType === "cron") {
      spec.cron = cronUiMode === "simple" ? simpleCron : cronExpr
    } else {
      spec.every_seconds = parseInt(intervalSeconds, 10)
    }

    try {
      await onSubmit(scheduleType, spec)
    } finally {
      setIsSubmitting(false)
    }
  }

  return (
    <DialogContent>
      <DialogHeader>
        <DialogTitle>Schedules &amp; Triggers</DialogTitle>
        <DialogDescription>
          {mode === "edit"
            ? "Update the existing recurring schedule for this pipeline"
            : "Configure a recurring schedule for this pipeline"}
        </DialogDescription>
      </DialogHeader>

      <div className="space-y-4 py-4">
        <div className="space-y-2">
          <Label>Trigger Status</Label>
          <RadioGroup value={triggerStatus} onValueChange={(v) => setTriggerStatus(v as TriggerStatus)} className="grid gap-2">
            <div className="flex items-center gap-2">
              <RadioGroupItem id="trigger-active" value="active" />
              <Label htmlFor="trigger-active" className="font-normal">
                Active
              </Label>
            </div>
            <div className="flex items-center gap-2">
              <RadioGroupItem id="trigger-paused" value="paused" />
              <Label htmlFor="trigger-paused" className="font-normal">
                Paused
              </Label>
            </div>
          </RadioGroup>
        </div>

        <div className="space-y-2">
          <Label>Trigger type</Label>
          <Select value={scheduleType} onValueChange={(v) => setScheduleType(v as ScheduleType)}>
            <SelectTrigger>
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="cron">Scheduled</SelectItem>
              <SelectItem value="interval">Fixed interval</SelectItem>
            </SelectContent>
          </Select>
        </div>

        {scheduleType === "cron" ? (
          <div className="space-y-2">
            <Label>Schedule type</Label>
            <Tabs value={cronUiMode} onValueChange={(v) => setCronUiMode(v as CronUiMode)}>
              <TabsList className="w-full">
                <TabsTrigger value="simple" className="flex-1">
                  Simple
                </TabsTrigger>
                <TabsTrigger value="advanced" className="flex-1">
                  Advanced
                </TabsTrigger>
              </TabsList>

              <TabsContent value="simple" className="space-y-4">
                <div className="space-y-2">
                  <Label>Schedule</Label>
                  <div className="flex flex-wrap items-center gap-2">
                    <span className="text-sm text-muted-foreground">Every</span>
                    <Select value={simpleCadence} onValueChange={(v) => setSimpleCadence(v as SimpleCadence)}>
                      <SelectTrigger className="w-[160px]">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="hour">Hour</SelectItem>
                        <SelectItem value="day">Day</SelectItem>
                        <SelectItem value="week">Week</SelectItem>
                      </SelectContent>
                    </Select>

                    {simpleCadence === "week" ? (
                      <Select value={simpleWeekday} onValueChange={(v) => setSimpleWeekday(v as Weekday)}>
                        <SelectTrigger className="w-[170px]">
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          {weekdayOptions.map((d) => (
                            <SelectItem key={d.value} value={d.value}>
                              {d.label}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    ) : null}

                    <span className="text-sm text-muted-foreground">at</span>

                    {simpleCadence === "hour" ? null : (
                      <>
                        <Select value={simpleHour} onValueChange={setSimpleHour}>
                          <SelectTrigger className="w-[88px]">
                            <SelectValue />
                          </SelectTrigger>
                          <SelectContent className="max-h-64">
                            {hours.map((h) => (
                              <SelectItem key={h} value={h}>
                                {h}
                              </SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                        <span className="text-sm text-muted-foreground">:</span>
                      </>
                    )}

                    <Select value={simpleMinute} onValueChange={setSimpleMinute}>
                      <SelectTrigger className="w-[88px]">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent className="max-h-64">
                        {minutes.map((m) => (
                          <SelectItem key={m} value={m}>
                            {m}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>
                </div>

                <div className="flex items-center gap-2">
                  <Checkbox
                    id="show-cron-syntax"
                    checked={showCronSyntax}
                    onCheckedChange={(v) => setShowCronSyntax(Boolean(v))}
                  />
                  <Label htmlFor="show-cron-syntax" className="font-normal">
                    Show cron syntax
                  </Label>
                </div>
                {showCronSyntax ? (
                  <div className="rounded-md border bg-muted/40 px-3 py-2 text-xs font-mono text-zinc-800 dark:text-zinc-200">
                    {simpleCron}
                  </div>
                ) : null}
              </TabsContent>

              <TabsContent value="advanced" className="space-y-2">
                <Label>Cron expression</Label>
                <Input value={cronExpr} onChange={(e) => setCronExpr(e.target.value)} placeholder="0 * * * *" />
                <p className="text-xs text-muted-foreground">
                  Examples: <code>0 * * * *</code> (hourly), <code>0 0 * * *</code> (daily at midnight)
                </p>
              </TabsContent>
            </Tabs>
          </div>
        ) : (
          <div className="space-y-2">
            <Label>Run every (seconds)</Label>
            <Input
              type="number"
              value={intervalSeconds}
              onChange={(e) => setIntervalSeconds(e.target.value)}
              placeholder="3600"
              min="60"
            />
            <p className="text-xs text-muted-foreground">
              Minimum: 60 seconds (1 minute). Examples: 3600 (1 hour), 86400 (1 day)
            </p>
            {/* The 60s floor is a validation bound, not a promise of cadence. A
                tick that arrives while the previous run is still going is
                dropped (Temporal overlap policy = SKIP), so an interval shorter
                than the run itself silently yields half the requested rate or
                less. Saying so here is the difference between "the scheduler is
                broken" and "the interval is shorter than the run". */}
            {Number(intervalSeconds) > 0 && Number(intervalSeconds) < 300 && (
              <p className="text-xs text-amber-600 dark:text-amber-400">
                Short intervals are not guaranteed. If a run is still going when the next
                tick is due, that tick is skipped rather than queued — a pipeline that takes
                longer than {intervalSeconds}s will effectively run at half this rate or slower.
              </p>
            )}
          </div>
        )}

        <div className="space-y-2">
          <Label>Timezone</Label>
          <Select value={timezone} onValueChange={setTimezone}>
            <SelectTrigger>
              <SelectValue placeholder="UTC" />
            </SelectTrigger>
            <SelectContent className="max-h-72">
              {timezoneOptions.map((t) => (
                <SelectItem key={t.tz} value={t.tz}>
                  {t.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          <p className="text-xs text-muted-foreground">Tip: when the dropdown is open, start typing to jump to a timezone.</p>
        </div>
      </div>

      <DialogFooter>
        <Button variant="outline" onClick={onCancel} disabled={isSubmitting}>
          Cancel
        </Button>
        <Button onClick={() => void handleSubmit()} disabled={isSubmitting}>
          {isSubmitting ? (
            <span className="inline-flex items-center gap-2">
              <Loader2 className="h-4 w-4 animate-spin" />
              {mode === "edit" ? "Saving…" : "Creating…"}
            </span>
          ) : mode === "edit" ? (
            "Save changes"
          ) : (
            "Create Schedule"
          )}
        </Button>
      </DialogFooter>
    </DialogContent>
  )
}

