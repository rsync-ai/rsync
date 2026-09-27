import type { ElementType } from "react"
import { CheckCircle2, Clock, Loader2, XCircle } from "lucide-react"

// The execution-plan contract and the stage status palette, shared by the v1
// DAGVisualization (chat, Timeline), DAGVisualizationV2 (Steps/DAG) and the
// panels around them. It lives apart from both components so a reader that only
// needs a type or a status colour does not import a whole graph renderer.

export interface ExecutionPlanStage {
  id: string
  display_name: string
  description?: string
  icon?: string
  status: string // pending|running|complete|failed|waiting
  progress?: number
  started_at?: string
  completed_at?: string
  /**
   * Milliseconds. The canonical duration — read it through `stageDurationMs()`
   * in `dagHelpers`, never directly, so legacy rows are handled in one place.
   */
  actual_duration_ms?: number
  /**
   * SECONDS, legacy. The temporal-adapter wrote this field in seconds while six
   * of the seven readers here formatted it as milliseconds, so a 42 s stage
   * rendered "42ms". Still present because plan JSON already persisted in
   * `execution_plans` carries the old unit. Do NOT add readers.
   */
  actual_duration?: number
  /**
   * Free prose the backend writes for a human to read — today always one of
   * "Plan created" / "Plan validated" / "Pipeline executed", documented on the
   * writing struct as `"2-step plan created"`
   * (`types/execution_plan.go:55`). Render it; never parse a number out of it.
   * This comment used to read `e.g. "12,450 rows transferred"`, which described
   * no value the backend has ever sent, and a helper was written against it —
   * see the note at the top of `dagHelpers.ts`.
   */
  result_summary?: string
  error_message?: string
  node_kind?: string // For DAG: source, destination, transform, etc.
  dependencies?: string[] // For DAG: parent node IDs
  metadata?: Record<string, any> // Additional backend metadata (node_config, etc.)
}

export interface ExecutionPlan {
  pipeline_id: string
  workflow_id?: string
  stages?: ExecutionPlanStage[]
  mode?: string
  created_at?: string
  estimated_time?: number
  metadata?: {
    is_dag?: boolean
    node_count?: number
    edge_count?: number
    graph_id?: string
    [key: string]: any
  }
}

// =============================================================================
// Status Configuration
// =============================================================================

export function getStageStatusConfig(status: string) {
  const configs: Record<string, { icon: ElementType; color: string; bgColor: string; borderColor: string }> = {
    complete: {
      icon: CheckCircle2,
      color: "text-green-600",
      bgColor: "bg-green-100 dark:bg-green-900/30",
      borderColor: "border-green-500",
    },
    running: {
      icon: Loader2,
      color: "text-blue-600",
      bgColor: "bg-blue-100 dark:bg-blue-900/30",
      borderColor: "border-blue-500",
    },
    failed: {
      icon: XCircle,
      color: "text-red-600",
      bgColor: "bg-red-100 dark:bg-red-900/30",
      borderColor: "border-red-500",
    },
    waiting: {
      icon: Clock,
      color: "text-amber-600",
      bgColor: "bg-amber-100 dark:bg-amber-900/30",
      borderColor: "border-amber-500",
    },
    pending: {
      icon: Clock,
      color: "text-zinc-400",
      bgColor: "bg-zinc-100 dark:bg-zinc-800",
      borderColor: "border-zinc-300",
    },
  }
  return configs[status] || configs.pending
}
