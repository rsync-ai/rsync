"use client"

import { cn } from "@/lib/utils"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { ScrollArea } from "@/components/ui/scroll-area"
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible"
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip"
import {
  Database,
  Table2,
  Columns,
  MessageSquare,
  Code,
  Shield,
  Play,
  Lightbulb,
  CheckCircle2,
  XCircle,
  Loader2,
  Clock,
  ChevronDown,
  ChevronRight,
  AlertTriangle,
  RefreshCw,
} from "lucide-react"
import { useState } from "react"

// Step types for the explorer pipeline
export type ExplorerStepType =
  | "schema_fetch"
  | "table_link"
  | "column_link"
  | "hitl"
  | "sql_generate"
  | "safety_check"
  | "execute"
  | "next_steps"

export type ExplorerStepStatus =
  | "pending"
  | "running"
  | "success"
  | "failed"
  | "skipped"
  | "waiting_hitl"

export interface ExplorerStepInput {
  key: string
  value: string | number | boolean | string[] | Record<string, unknown>
}

export interface ExplorerStepOutput {
  key: string
  value: string | number | boolean | string[] | Record<string, unknown>
}

export interface ExplorerStep {
  id: string
  type: ExplorerStepType
  status: ExplorerStepStatus
  title: string
  description?: string
  inputs?: ExplorerStepInput[]
  outputs?: ExplorerStepOutput[]
  confidence?: number
  reason?: string
  durationMs?: number
  error?: string
  recovery?: {
    action: string
    description: string
  }
  startedAt?: Date
  completedAt?: Date
}

export interface ExplorerRun {
  id: string
  conversationId?: string
  question: string
  steps: ExplorerStep[]
  startedAt: Date
  completedAt?: Date
  status: "running" | "waiting" | "completed" | "failed"
}

const stepTypeConfig: Record<
  ExplorerStepType,
  { icon: React.ElementType; label: string; color: string }
> = {
  schema_fetch: {
    icon: Database,
    label: "Schema Fetch",
    color: "text-blue-500",
  },
  table_link: {
    icon: Table2,
    label: "Table Link",
    color: "text-violet-500",
  },
  column_link: {
    icon: Columns,
    label: "Column Link",
    color: "text-indigo-500",
  },
  hitl: {
    icon: MessageSquare,
    label: "Human Review",
    color: "text-amber-500",
  },
  sql_generate: {
    icon: Code,
    label: "SQL Generate",
    color: "text-cyan-500",
  },
  safety_check: {
    icon: Shield,
    label: "Safety Check",
    color: "text-emerald-500",
  },
  execute: {
    icon: Play,
    label: "Execute",
    color: "text-green-500",
  },
  next_steps: {
    icon: Lightbulb,
    label: "Next Steps",
    color: "text-orange-500",
  },
}

const statusConfig: Record<
  ExplorerStepStatus,
  { icon: React.ElementType; label: string; color: string; bgColor: string }
> = {
  pending: {
    icon: Clock,
    label: "Pending",
    color: "text-zinc-400",
    bgColor: "bg-zinc-100 dark:bg-zinc-800",
  },
  running: {
    icon: Loader2,
    label: "Running",
    color: "text-blue-500",
    bgColor: "bg-blue-50 dark:bg-blue-950/30",
  },
  success: {
    icon: CheckCircle2,
    label: "Success",
    color: "text-emerald-500",
    bgColor: "bg-emerald-50 dark:bg-emerald-950/30",
  },
  failed: {
    icon: XCircle,
    label: "Failed",
    color: "text-red-500",
    bgColor: "bg-red-50 dark:bg-red-950/30",
  },
  skipped: {
    icon: ChevronRight,
    label: "Skipped",
    color: "text-zinc-400",
    bgColor: "bg-zinc-50 dark:bg-zinc-900",
  },
  waiting_hitl: {
    icon: MessageSquare,
    label: "Waiting",
    color: "text-amber-500",
    bgColor: "bg-amber-50 dark:bg-amber-950/30",
  },
}

interface StepNodeProps {
  step: ExplorerStep
  isLast: boolean
  isExpanded: boolean
  onToggle: () => void
}

function StepNode({ step, isLast, isExpanded, onToggle }: StepNodeProps) {
  const typeConfig = stepTypeConfig[step.type]
  const status = statusConfig[step.status]
  const TypeIcon = typeConfig.icon
  const StatusIcon = status.icon

  const formatValue = (value: unknown): string => {
    if (value === null || value === undefined) return "—"
    if (typeof value === "boolean") return value ? "Yes" : "No"
    if (Array.isArray(value)) return value.length > 3 ? `[${value.length} items]` : value.join(", ")
    if (typeof value === "object") return JSON.stringify(value).slice(0, 50) + "..."
    return String(value).slice(0, 100)
  }

  return (
    <div className="relative">
      {/* Connector line */}
      {!isLast && (
        <div
          className={cn(
            "absolute left-[15px] top-[32px] w-[2px] h-[calc(100%-16px)]",
            step.status === "success" ? "bg-emerald-200 dark:bg-emerald-800" :
            step.status === "failed" ? "bg-red-200 dark:bg-red-800" :
            step.status === "running" ? "bg-blue-200 dark:bg-blue-800" :
            "bg-zinc-200 dark:bg-zinc-700"
          )}
        />
      )}

      <Collapsible open={isExpanded} onOpenChange={onToggle}>
        <CollapsibleTrigger asChild>
          <button
            className={cn(
              "w-full flex items-start gap-3 p-2 rounded-lg transition-colors text-left",
              "hover:bg-zinc-50 dark:hover:bg-zinc-900",
              isExpanded && status.bgColor
            )}
          >
            {/* Step icon with status ring */}
            <div className="relative shrink-0">
              <div
                className={cn(
                  "w-8 h-8 rounded-full flex items-center justify-center",
                  "bg-white dark:bg-zinc-900 border-2",
                  step.status === "success" && "border-emerald-500",
                  step.status === "failed" && "border-red-500",
                  step.status === "running" && "border-blue-500",
                  step.status === "waiting_hitl" && "border-amber-500",
                  step.status === "pending" && "border-zinc-300 dark:border-zinc-600",
                  step.status === "skipped" && "border-zinc-300 dark:border-zinc-600"
                )}
              >
                <TypeIcon className={cn("h-4 w-4", typeConfig.color)} />
              </div>
              {/* Status indicator */}
              <div
                className={cn(
                  "absolute -bottom-0.5 -right-0.5 w-4 h-4 rounded-full",
                  "flex items-center justify-center bg-white dark:bg-zinc-900"
                )}
              >
                <StatusIcon
                  className={cn(
                    "h-3 w-3",
                    status.color,
                    step.status === "running" && "animate-spin"
                  )}
                />
              </div>
            </div>

            {/* Content */}
            <div className="flex-1 min-w-0">
              <div className="flex items-center gap-2">
                <span className="font-medium text-sm">{step.title}</span>
                {step.confidence !== undefined && step.confidence < 1 && (
                  <TooltipProvider>
                    <Tooltip>
                      <TooltipTrigger>
                        <Badge
                          variant="outline"
                          className={cn(
                            "text-[10px] px-1.5 py-0",
                            step.confidence >= 0.8 ? "text-emerald-600" :
                            step.confidence >= 0.5 ? "text-amber-600" :
                            "text-red-600"
                          )}
                        >
                          {Math.round(step.confidence * 100)}%
                        </Badge>
                      </TooltipTrigger>
                      <TooltipContent>
                        <p>Confidence: {Math.round(step.confidence * 100)}%</p>
                        {step.reason && <p className="text-xs text-zinc-400">{step.reason}</p>}
                      </TooltipContent>
                    </Tooltip>
                  </TooltipProvider>
                )}
                {step.durationMs !== undefined && (
                  <span className="text-[10px] text-zinc-400">{step.durationMs}ms</span>
                )}
              </div>
              {step.description && (
                <p className="text-xs text-zinc-500 truncate">{step.description}</p>
              )}
              {step.status === "failed" && step.error && (
                <div className="flex items-center gap-1 text-xs text-red-500 mt-1">
                  <AlertTriangle className="h-3 w-3" />
                  <span className="truncate">{step.error}</span>
                </div>
              )}
            </div>

            {/* Expand indicator */}
            <ChevronDown
              className={cn(
                "h-4 w-4 text-zinc-400 transition-transform shrink-0",
                isExpanded && "rotate-180"
              )}
            />
          </button>
        </CollapsibleTrigger>

        <CollapsibleContent>
          <div className="ml-11 mr-2 mb-3 space-y-3">
            {/* Inputs */}
            {step.inputs && step.inputs.length > 0 && (
              <div className="space-y-1">
                <div className="text-[10px] font-medium text-zinc-500 uppercase tracking-wider">
                  Inputs
                </div>
                <div className="bg-zinc-50 dark:bg-zinc-900 rounded-md p-2 space-y-1">
                  {step.inputs.map((input, i) => (
                    <div key={i} className="flex items-start gap-2 text-xs">
                      <span className="text-zinc-500 shrink-0">{input.key}:</span>
                      <span className="font-mono text-zinc-700 dark:text-zinc-300 break-all">
                        {formatValue(input.value)}
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {/* Outputs */}
            {step.outputs && step.outputs.length > 0 && (
              <div className="space-y-1">
                <div className="text-[10px] font-medium text-zinc-500 uppercase tracking-wider">
                  Outputs
                </div>
                <div className="bg-zinc-50 dark:bg-zinc-900 rounded-md p-2 space-y-1">
                  {step.outputs.map((output, i) => (
                    <div key={i} className="flex items-start gap-2 text-xs">
                      <span className="text-zinc-500 shrink-0">{output.key}:</span>
                      <span className="font-mono text-zinc-700 dark:text-zinc-300 break-all">
                        {formatValue(output.value)}
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {/* Recovery action */}
            {step.recovery && (
              <div className="flex items-center gap-2 p-2 bg-amber-50 dark:bg-amber-950/20 rounded-md">
                <RefreshCw className="h-3 w-3 text-amber-500" />
                <div className="text-xs">
                  <span className="font-medium text-amber-700 dark:text-amber-400">
                    {step.recovery.action}
                  </span>
                  <span className="text-amber-600 dark:text-amber-500 ml-1">
                    {step.recovery.description}
                  </span>
                </div>
              </div>
            )}
          </div>
        </CollapsibleContent>
      </Collapsible>
    </div>
  )
}

interface ExplorerStepTimelineProps {
  run: ExplorerRun | null
  onRetry?: () => void
  className?: string
}

export function ExplorerStepTimeline({ run, onRetry, className }: ExplorerStepTimelineProps) {
  const [expandedSteps, setExpandedSteps] = useState<Set<string>>(new Set())

  if (!run) {
    return (
      <div className={cn("flex flex-col items-center justify-center py-8 text-zinc-400", className)}>
        <Database className="h-8 w-8 mb-2 opacity-50" />
        <p className="text-sm">No exploration run yet</p>
        <p className="text-xs mt-1">Ask a question to see the step-by-step process</p>
      </div>
    )
  }

  const toggleStep = (stepId: string) => {
    setExpandedSteps((prev) => {
      const next = new Set(prev)
      if (next.has(stepId)) {
        next.delete(stepId)
      } else {
        next.add(stepId)
      }
      return next
    })
  }

  const expandAll = () => {
    setExpandedSteps(new Set(run.steps.map((s) => s.id)))
  }

  const collapseAll = () => {
    setExpandedSteps(new Set())
  }

  const totalDuration = run.steps.reduce((sum, s) => sum + (s.durationMs || 0), 0)
  const failedStep = run.steps.find((s) => s.status === "failed")

  return (
    <div className={cn("space-y-3", className)}>
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Badge
            variant="outline"
            className={cn(
              "text-xs",
              run.status === "completed" && "border-emerald-500 text-emerald-600",
              run.status === "failed" && "border-red-500 text-red-600",
              run.status === "running" && "border-blue-500 text-blue-600",
              run.status === "waiting" && "border-amber-500 text-amber-600"
            )}
          >
            {run.status === "running" && <Loader2 className="h-3 w-3 mr-1 animate-spin" />}
            {run.status === "waiting" && <MessageSquare className="h-3 w-3 mr-1" />}
            {run.status.charAt(0).toUpperCase() + run.status.slice(1)}
          </Badge>
          <span className="text-xs text-zinc-400">{totalDuration}ms total</span>
        </div>
        <div className="flex items-center gap-1">
          <Button variant="ghost" size="sm" className="h-6 px-2 text-xs" onClick={expandAll}>
            Expand
          </Button>
          <Button variant="ghost" size="sm" className="h-6 px-2 text-xs" onClick={collapseAll}>
            Collapse
          </Button>
        </div>
      </div>

      {/* Question */}
      <div className="p-2 bg-zinc-50 dark:bg-zinc-900 rounded-lg">
        <div className="text-[10px] text-zinc-500 uppercase tracking-wider mb-1">Question</div>
        <p className="text-sm text-zinc-700 dark:text-zinc-300">{run.question}</p>
      </div>

      {/* Steps */}
      <ScrollArea className="h-[400px] pr-2">
        <div className="space-y-1">
          {run.steps.map((step, index) => (
            <StepNode
              key={step.id}
              step={step}
              isLast={index === run.steps.length - 1}
              isExpanded={expandedSteps.has(step.id)}
              onToggle={() => toggleStep(step.id)}
            />
          ))}
        </div>
      </ScrollArea>

      {/* Failed state with retry */}
      {failedStep && onRetry && (
        <div className="flex items-center justify-between p-3 bg-red-50 dark:bg-red-950/20 rounded-lg">
          <div className="flex items-center gap-2">
            <XCircle className="h-4 w-4 text-red-500" />
            <span className="text-sm text-red-700 dark:text-red-400">
              Failed at: {failedStep.title}
            </span>
          </div>
          <Button
            variant="outline"
            size="sm"
            onClick={onRetry}
            className="border-red-200 text-red-600 hover:bg-red-50"
          >
            <RefreshCw className="h-3 w-3 mr-1" />
            Retry
          </Button>
        </div>
      )}
    </div>
  )
}

// Factory function to create initial steps for a new run
export function createInitialExplorerSteps(): ExplorerStep[] {
  return [
    {
      id: "schema_fetch",
      type: "schema_fetch",
      status: "pending",
      title: "Fetch Schema",
      description: "Loading database schema and table information",
    },
    {
      id: "table_link",
      type: "table_link",
      status: "pending",
      title: "Identify Tables",
      description: "Finding relevant tables for your question",
    },
    {
      id: "column_link",
      type: "column_link",
      status: "pending",
      title: "Map Columns",
      description: "Selecting columns and join keys",
    },
    {
      id: "sql_generate",
      type: "sql_generate",
      status: "pending",
      title: "Generate SQL",
      description: "Creating the SQL query",
    },
    {
      id: "safety_check",
      type: "safety_check",
      status: "pending",
      title: "Safety Check",
      description: "Validating query safety",
    },
    {
      id: "execute",
      type: "execute",
      status: "pending",
      title: "Execute Query",
      description: "Running the query against the database",
    },
    {
      id: "next_steps",
      type: "next_steps",
      status: "pending",
      title: "Suggest Actions",
      description: "Recommending next steps",
    },
  ]
}

// Helper to update a step in a run
export function updateExplorerStep(
  run: ExplorerRun,
  stepId: string,
  updates: Partial<ExplorerStep>
): ExplorerRun {
  return {
    ...run,
    steps: run.steps.map((step) => {
      if (step.id !== stepId) return step
      const next = { ...step, ...updates }
      // `error` and `recovery` describe one specific failure, so they only mean
      // anything while the step is still in it. A spread cannot remove a key, so
      // a plain `{status: "success"}` update overwrites the status and leaves the
      // old strings sitting on the step. That is reachable on every run after the
      // first: executeQuery only builds fresh steps when `currentRun` is null, so
      // a failed run followed by a fixed one reuses these same step objects and
      // the timeline would keep showing the error the user just resolved.
      // Clear them when this update moves the step out of "failed" -- unless the
      // caller is deliberately setting a new one in the same update.
      if (next.status !== "failed") {
        if (updates.error === undefined) delete next.error
        if (updates.recovery === undefined) delete next.recovery
      }
      return next
    }),
  }
}
