"use client"

import Link from "next/link"
import { useRouter } from "next/navigation"
import {
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
// This file had its own start/end formatter, which floored to whole seconds and
// so wrote "0s" for a 500 ms run. The shared one keeps sub-second precision and
// is the same text the pipeline page shows for the same execution.
import { formatDurationBetween as formatDuration } from "@/lib/duration"
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"
import {
  Play,
  Pause,
  Settings,
  Trash2,
  Copy,
  FileText,
  Download,
  MoreVertical,
  CheckCircle2,
  XCircle,
  Loader2,
  Clock,
  Calendar,
  ArrowRight,
  Pencil,
  Edit,
  Square,
  BellRing,
} from "lucide-react"
import { formatRelativeTime } from "@/lib/utils"
import { pipelineLogsHref } from "@/lib/pipeline/pipelineMenuActions"
import { getConnectorDisplayName } from "@/lib/types/mcp-connector"

export interface PipelineListItem {
  id: string
  name: string
  description?: string
  pipeline_status: string
  created_at: string
  updated_at: string
  created_by?: string
  sync_mode?: "batch" | "cdc" | null
  cdc_mode?: "initial" | "streaming_only" | null
  source_connection?: {
    id?: string
    name?: string
    connector_type?: string
    status?: string
  }
  destination_connection?: {
    id?: string
    name?: string
    connector_type?: string
    status?: string
  }
  schedule?: {
    schedule_id?: string
    schedule_type?: string
    status?: string
    timezone?: string
  } | null
  last_execution?: {
    id?: string
    status?: string
    started_at?: string
    completed_at?: string
    error_message?: string | null
    metrics?: any
  } | null
  derived_status: string
  status_context?: {
    primary: string
    secondary?: string
  }
}

type ListConnection = PipelineListItem["source_connection"]

// The connection's own name ("orders-prod"), with the connector type in the tooltip.
// The cell used to print the raw type ("mongodb → gcs"), which says nothing when a
// workspace has two connections of the same kind.
function ConnectionLabel({ connection }: { connection: ListConnection }) {
  const type = connection?.connector_type ? getConnectorDisplayName(connection.connector_type) : ""
  const label = connection?.name?.trim() || type || "—"
  return (
    <span
      className="inline-block align-bottom text-zinc-600 dark:text-zinc-400 truncate max-w-[180px]"
      title={type && type !== label ? `${label} (${type})` : label}
    >
      {label}
    </span>
  )
}


function truncate(s: string, max = 80): string {
  const t = String(s || "").trim()
  if (!t) return ""
  if (t.length <= max) return t
  return `${t.slice(0, max - 1)}…`
}

interface PipelinesTableProps {
  pipelines: PipelineListItem[]
  currentUserId?: string
  onMenuOpenChange?: (open: boolean) => void
  onRunPipeline?: (id: string) => void
  onPausePipeline?: (id: string) => void
  onResumePipeline?: (id: string) => void
  onEditPipeline?: (id: string) => void
  onDuplicatePipeline?: (id: string) => void
  onStopPipeline?: (id: string) => void
  onDeletePipeline?: (id: string) => void
  onExportConfig?: (id: string) => void
}

// Pipeline-list-specific configs that don't exist in the shared execution-status
// helper (pipelines have lifecycle states executions don't, e.g. paused/scheduled/idle).
// For execution-derived statuses we delegate to configForExecution so the
// silent-drop, waiting-for-user, etc. variants are consistent with the executions
// list and detail page.
import { configForExecution, formatErrorMessage } from "@/lib/execution-status"
import { pipelineIsCDC } from "@/lib/pipeline/syncMode"

function getStatusConfig(pipeline: PipelineListItem) {
  const status = pipeline.derived_status
  const pipelineOnly: Record<string, { icon: React.ElementType; color: string; bgColor: string; label: string }> = {
    running: {
      icon: Loader2,
      color: "text-blue-700 dark:text-blue-300",
      bgColor: "bg-blue-100 dark:bg-blue-900/30",
      label: "Running",
    },
    passed: {
      icon: CheckCircle2,
      color: "text-green-800 dark:text-green-300",
      bgColor: "bg-green-100 dark:bg-green-900/30",
      label: "Completed",
    },
    paused: {
      icon: Pause,
      color: "text-amber-800 dark:text-amber-300",
      bgColor: "bg-amber-100 dark:bg-amber-900/30",
      label: "Paused",
    },
    stopped: {
      icon: XCircle,
      color: "text-zinc-700 dark:text-zinc-300",
      bgColor: "bg-zinc-100 dark:bg-zinc-800",
      label: "Stopped",
    },
    scheduled: {
      icon: Calendar,
      color: "text-purple-700 dark:text-purple-300",
      bgColor: "bg-purple-100 dark:bg-purple-900/30",
      label: "Scheduled",
    },
    idle: {
      icon: Clock,
      color: "text-zinc-600 dark:text-zinc-400",
      bgColor: "bg-zinc-100 dark:bg-zinc-800",
      label: "Idle",
    },
  }
  if (pipelineOnly[status]) return pipelineOnly[status]

  // Execution-derived (failed, silent_drop_detected, etc.) — defer to the
  // shared helper so badges match the executions list/detail pages. Use the
  // last execution's error_message for prefix-based status re-derivation.
  const cfg = configForExecution(status, pipeline.last_execution?.error_message ?? undefined)
  return {
    icon: cfg.icon,
    // Reuse the helper's amber/red/etc. classes by reading directly from cfg.
    color: cfg.color,
    bgColor: cfg.bg,
    label: cfg.label,
  }
}

function getPipelineType(pipeline: PipelineListItem): string {
  // Explicit sync_mode wins; cdc_mode alone only decides for a legacy row.
  if (pipelineIsCDC(pipeline)) {
    return pipeline.cdc_mode === "streaming_only" ? "CDC (changes only)" : "Backfill+CDC"
  }
  // Batch: scheduled vs manual.
  if (pipeline.schedule?.status === "active") return "Batch (scheduled)"
  return "Batch (manual)"
}

export function PipelinesTable({
  pipelines,
  currentUserId,
  onMenuOpenChange,
  onRunPipeline,
  onPausePipeline,
  onResumePipeline,
  onEditPipeline,
  onDuplicatePipeline,
  onDeletePipeline,
  onStopPipeline,
  onExportConfig,
}: PipelinesTableProps) {
  const router = useRouter()
  const canEdit = (pipeline: PipelineListItem) => {
    return !currentUserId || currentUserId === pipeline.created_by
  }

  // A run parked on user input (waiting_for_user) is still mid-execution — treat
  // it as active so it can't be re-run or deleted out from under the HITL prompt,
  // exactly as when it used to surface as "running".
  const isActive = (pipeline: PipelineListItem) =>
    pipeline.derived_status === "running" || pipeline.derived_status === "waiting_for_user"

  const canRun = (pipeline: PipelineListItem) => {
    return !isActive(pipeline)
  }

  const hasSchedule = (pipeline: PipelineListItem) => {
    return !!pipeline.schedule?.schedule_id
  }

  return (
    <div className="relative w-full overflow-x-auto">
      <table className="w-full caption-bottom text-sm">
      <TableHeader>
        <TableRow>
          <TableHead>Pipeline Name</TableHead>
          <TableHead className="hidden md:table-cell">Type</TableHead>
          <TableHead className="hidden md:table-cell">Source → Destination</TableHead>
          <TableHead>Status</TableHead>
          <TableHead className="hidden lg:table-cell">Last Run</TableHead>
          <TableHead className="hidden lg:table-cell">Created</TableHead>
            <TableHead className="text-right sticky right-0 bg-background z-20 border-l border-zinc-200 dark:border-zinc-800">
              Actions
            </TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {pipelines.length === 0 ? (
          <TableRow>
            <TableCell colSpan={7} className="text-center py-12 text-zinc-500 dark:text-zinc-400">
              No pipelines found. Try adjusting your filters or create a new pipeline.
            </TableCell>
          </TableRow>
        ) : (
          pipelines.map((pipeline) => {
            const statusConfig = getStatusConfig(pipeline)
            const StatusIcon = statusConfig.icon
            const pipelineType = getPipelineType(pipeline)

            return (
              <TableRow
                key={pipeline.id}
                  className="group cursor-pointer hover:bg-zinc-50 dark:hover:bg-zinc-900/50"
                onClick={() => router.push(`/pipelines/${pipeline.id}`)}
              >
                <TableCell>
                  <div>
                      <div className="font-medium text-zinc-900 dark:text-white">{pipeline.name}</div>
                    {/* max-w-xs, not max-w-md: a truncated line still sets the
                        column's minimum width, and at 448px it pushed the table
                        18px past a 1512px window into a sideways scroll. */}
                    {pipeline.description && (
                      <div className="text-sm text-zinc-500 dark:text-zinc-400 truncate max-w-xs">
                        {pipeline.description}
                      </div>
                    )}
                  </div>
                </TableCell>

                <TableCell className="hidden md:table-cell">
                  <Badge variant="outline" className="font-mono text-xs">
                    {pipelineType}
                  </Badge>
                </TableCell>

                <TableCell className="hidden md:table-cell">
                  <div className="flex items-center gap-2 text-sm">
                    <ConnectionLabel connection={pipeline.source_connection} />
                    <ArrowRight className="h-3 w-3 text-zinc-400 shrink-0" />
                    <ConnectionLabel connection={pipeline.destination_connection} />
                  </div>
                </TableCell>

                <TableCell>
                  <div className="space-y-1">
                    <Badge className={`${statusConfig.bgColor} ${statusConfig.color} border-0 gap-1`}>
                      <StatusIcon className={`h-3 w-3 ${pipeline.derived_status === "running" ? "animate-spin" : ""}`} />
                      {statusConfig.label}
                    </Badge>
                    {pipeline.status_context?.secondary && (
                      <div className="text-xs text-zinc-500 dark:text-zinc-400">
                        {pipeline.status_context.secondary}
                      </div>
                    )}
                    {!pipeline.status_context?.secondary &&
                      pipeline.derived_status === "failed" &&
                      pipeline.last_execution?.error_message &&
                      (() => {
                        const humanErr = formatErrorMessage(pipeline.last_execution!.error_message)
                        return humanErr ? (
                          <div
                            className="text-xs text-zinc-500 dark:text-zinc-400"
                            title={humanErr}
                          >
                            {truncate(humanErr, 72)}
                          </div>
                        ) : null
                      })()}
                  </div>
                </TableCell>

                <TableCell className="hidden lg:table-cell">
                  <div className="text-sm">
                    {pipeline.last_execution?.started_at ? (
                      <div className="space-y-1">
                        <span className="text-zinc-600 dark:text-zinc-400">
                          {formatRelativeTime(new Date(pipeline.last_execution.started_at))}
                        </span>
                        <div className="text-xs text-zinc-500 dark:text-zinc-400">
                          {pipeline.last_execution.completed_at
                            ? `Duration: ${formatDuration(pipeline.last_execution.started_at, pipeline.last_execution.completed_at)}`
                            : "—"}
                        </div>
                      </div>
                    ) : (
                      <span className="text-zinc-400">Never</span>
                    )}
                  </div>
                </TableCell>

                <TableCell className="hidden lg:table-cell">
                  <span className="text-sm text-zinc-600 dark:text-zinc-400">
                    {formatRelativeTime(new Date(pipeline.created_at))}
                  </span>
                </TableCell>

                <TableCell
                  className="text-right sticky right-0 z-10 border-l border-zinc-200 dark:border-zinc-800 bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/80 group-hover:bg-zinc-50 dark:group-hover:bg-zinc-900/50"
                  onClick={(e) => e.stopPropagation()}
                  onPointerDown={(e) => e.stopPropagation()}
                >
                  {/* Uncontrolled — let Radix manage open state internally.
                      Controlled open + stopPropagation on the trigger blocked Radix's
                      internal pointer-event handling, keeping data-state='closed'. */}
                  <DropdownMenu onOpenChange={(open) => onMenuOpenChange?.(open)}>
                    <DropdownMenuTrigger asChild>
                      <Button
                        variant="ghost"
                        size="icon"
                        className="h-8 w-8 pointer-events-auto"
                        aria-label={`Open actions for pipeline ${pipeline.name || pipeline.id}`}
                        title={`Open actions for ${pipeline.name || pipeline.id}`}
                      >
                        <MoreVertical className="h-4 w-4" />
                      </Button>
                    </DropdownMenuTrigger>
                    <DropdownMenuContent align="end" sideOffset={6}>
                      {/* The detail page's ⋯ menu items, in its order (#52). Rename and
                          Edit Tables open there, where their dialogs live. */}
                      <DropdownMenuItem onClick={(e) => {
                        e.stopPropagation()
                        router.push(`/pipelines/${pipeline.id}?rename=1`)
                      }}>
                        <Pencil className="h-4 w-4 mr-2" />
                        Rename
                      </DropdownMenuItem>
                      <DropdownMenuItem onClick={(e) => {
                        e.stopPropagation()
                        router.push(`/pipelines/${pipeline.id}?tab=table-stats&editTables=1`)
                      }}>
                        <Edit className="h-4 w-4 mr-2" />
                        Edit Tables
                      </DropdownMenuItem>
                      <DropdownMenuItem asChild>
                        <Link href={`/pipelines/${pipeline.id}/schema-changes`}>
                          <BellRing className="h-4 w-4 mr-2" />
                          Schema change alerts…
                        </Link>
                      </DropdownMenuItem>
                      {/* Same gate as the detail menu: a running CDC stream has no
                          other Stop control. */}
                      {pipelineIsCDC(pipeline) && isActive(pipeline) && onStopPipeline && (
                        <DropdownMenuItem onClick={(e) => {
                          e.stopPropagation()
                          onStopPipeline(pipeline.id)
                        }}>
                          <Square className="h-4 w-4 mr-2" />
                          Stop Pipeline
                        </DropdownMenuItem>
                      )}

                      <DropdownMenuSeparator />

                      <DropdownMenuItem onClick={(e) => {
                        e.stopPropagation()
                        router.push(pipelineLogsHref(pipeline.id, pipeline.last_execution?.id))
                      }}>
                        <FileText className="h-4 w-4 mr-2" />
                        View Logs
                      </DropdownMenuItem>
                      <DropdownMenuItem onClick={(e) => {
                        e.stopPropagation()
                        onExportConfig?.(pipeline.id)
                      }}>
                        <Download className="h-4 w-4 mr-2" />
                        Export Config
                      </DropdownMenuItem>

                      <DropdownMenuSeparator />

                      {/* Run now (only when not running) */}
                      {canRun(pipeline) && (
                        <DropdownMenuItem onClick={(e) => {
                          e.stopPropagation()
                          onRunPipeline?.(pipeline.id)
                        }}>
                          <Play className="h-4 w-4 mr-2" />
                          Run Now
                        </DropdownMenuItem>
                      )}

                      {/* Pause/Resume (only if has schedule) */}
                      {hasSchedule(pipeline) && pipeline.derived_status === "paused" && (
                        <DropdownMenuItem onClick={(e) => {
                          e.stopPropagation()
                          onResumePipeline?.(pipeline.id)
                        }}>
                          <Play className="h-4 w-4 mr-2" />
                          Resume
                        </DropdownMenuItem>
                      )}
                      {hasSchedule(pipeline) && pipeline.derived_status !== "paused" && (
                        <DropdownMenuItem onClick={(e) => {
                          e.stopPropagation()
                          onPausePipeline?.(pipeline.id)
                        }}>
                          <Pause className="h-4 w-4 mr-2" />
                          Pause
                        </DropdownMenuItem>
                      )}

                      {/* Delete: owner only. Offered while running too, as the detail
                          menu does (#52) — the confirm dialog says the run stops. */}
                      {canEdit(pipeline) && (
                        <>
                          <DropdownMenuSeparator />
                          <DropdownMenuItem
                            className="text-red-600"
                            onClick={(e) => {
                              e.stopPropagation()
                              onDeletePipeline?.(pipeline.id)
                            }}
                          >
                            <Trash2 className="h-4 w-4 mr-2" />
                            Delete
                          </DropdownMenuItem>
                        </>
                      )}
                    </DropdownMenuContent>
                  </DropdownMenu>
                </TableCell>
              </TableRow>
            )
          })
        )}
      </TableBody>
      </table>
    </div>
  )
}
