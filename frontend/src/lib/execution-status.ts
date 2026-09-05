// Shared execution-status helpers. Both the list page and the detail
// page used to inline their own statusConfig + normalizer, which drifted
// (silent-drop fixes needed to land in two places). Single source of
// truth: normalize + render config live here.
//
// Phase 1 convention recap: the executor collapses silent-drop variants
// into status='failed' with the original status as a prefix on
// error_message ("silent_drop_detected: products: read=5 landed=0").
// `normalizeExecutionStatus` re-derives the original status so the UI
// shows the right badge.

import type { LucideIcon } from "lucide-react"
import {
  AlertTriangle,
  Ban,
  CheckCircle2,
  Clock,
  Loader2,
  XCircle,
} from "lucide-react"

export type ExecutionStatus =
  | "pending"
  | "running"
  | "success"
  | "failed"
  | "cancelled"
  | "silent_drop_detected"
  | "silent_partial_drop_detected"
  | "waiting_for_credential_reauth"
  | "waiting_for_user"

export type BadgeVariant =
  | "default"
  | "secondary"
  | "destructive"
  | "outline"
  | "success"
  | "warning"
  | "info"

export interface ExecutionStatusConfig {
  label: string
  icon: LucideIcon
  variant: BadgeVariant
  color: string
  bg: string
  animate: boolean
}

export const executionStatusConfig: Record<ExecutionStatus, ExecutionStatusConfig> = {
  pending: {
    label: "Pending",
    icon: Clock,
    variant: "secondary",
    color: "text-zinc-500",
    bg: "bg-zinc-100 dark:bg-zinc-800",
    animate: false,
  },
  running: {
    label: "Running",
    icon: Loader2,
    variant: "info",
    color: "text-blue-600 dark:text-blue-400",
    bg: "bg-blue-50 dark:bg-blue-900/20",
    animate: true,
  },
  success: {
    label: "Success",
    icon: CheckCircle2,
    variant: "success",
    color: "text-emerald-600 dark:text-emerald-400",
    bg: "bg-emerald-50 dark:bg-emerald-900/20",
    animate: false,
  },
  failed: {
    label: "Failed",
    icon: XCircle,
    variant: "destructive",
    color: "text-red-600 dark:text-red-400",
    bg: "bg-red-50 dark:bg-red-900/20",
    animate: false,
  },
  silent_drop_detected: {
    label: "Silent Drop",
    icon: AlertTriangle,
    variant: "warning",
    color: "text-amber-700 dark:text-amber-400",
    bg: "bg-amber-50 dark:bg-amber-900/20",
    animate: false,
  },
  silent_partial_drop_detected: {
    label: "Partial Silent Drop",
    icon: AlertTriangle,
    variant: "warning",
    color: "text-amber-700 dark:text-amber-400",
    bg: "bg-amber-50 dark:bg-amber-900/20",
    animate: false,
  },
  waiting_for_credential_reauth: {
    label: "Awaiting Re-Auth",
    icon: AlertTriangle,
    variant: "warning",
    color: "text-amber-700 dark:text-amber-400",
    bg: "bg-amber-50 dark:bg-amber-900/20",
    animate: false,
  },
  waiting_for_user: {
    label: "Awaiting User",
    icon: AlertTriangle,
    variant: "warning",
    color: "text-amber-700 dark:text-amber-400",
    bg: "bg-amber-50 dark:bg-amber-900/20",
    animate: false,
  },
  cancelled: {
    label: "Cancelled",
    icon: Ban,
    variant: "secondary",
    color: "text-zinc-500",
    bg: "bg-zinc-100 dark:bg-zinc-800",
    animate: false,
  },
}

export function normalizeExecutionStatus(
  status: string | null | undefined,
  errorMessage?: string | null,
): ExecutionStatus {
  const s = (status || "pending").toLowerCase()
  if (s === "completed") return "success"
  if (s === "canceled") return "cancelled"
  if (s === "failed" && typeof errorMessage === "string") {
    if (errorMessage.startsWith("silent_drop_detected")) return "silent_drop_detected"
    if (errorMessage.startsWith("silent_partial_drop_detected")) return "silent_partial_drop_detected"
    if (errorMessage.startsWith("waiting_for_credential_reauth")) return "waiting_for_credential_reauth"
    if (errorMessage.startsWith("waiting_for_credential_scope")) return "waiting_for_user"
  }
  if ((executionStatusConfig as Record<string, unknown>)[s]) {
    return s as ExecutionStatus
  }
  return "pending"
}

export function configForExecution(
  status: string | null | undefined,
  errorMessage?: string | null,
): ExecutionStatusConfig {
  return executionStatusConfig[normalizeExecutionStatus(status, errorMessage)]
}

// Convert raw backend error strings to human-readable descriptions.
export function formatErrorMessage(errorMessage: string | null | undefined): string | null {
  if (!errorMessage) return null
  const msg = errorMessage.trim()

  // silent_drop_detected: schema.table: read=N landed=0
  const silentDrop = msg.match(/^silent_drop_detected:\s*(.+?):\s*read=(\d+)\s+landed=(\d+)/i)
  if (silentDrop) {
    const table = silentDrop[1]
    const read = Number(silentDrop[2]).toLocaleString()
    return `No rows written: ${read} rows read from "${table}" but 0 reached the destination`
  }

  // zombie / healer cleanup
  if (/^zombie:/i.test(msg) || msg.includes("healer cleanup")) {
    return "Execution timed out and was cleaned up automatically"
  }

  // Temporal activity error
  if (/^Execution failed: activity error/i.test(msg)) {
    return "Pipeline execution failed due to an internal error"
  }

  return msg
}
