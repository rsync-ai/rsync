"use client"

import Link from "next/link"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { AlertTriangle, ArrowRight, ExternalLink } from "lucide-react"

// DiagnosisCard renders the assistant's "why did this fail?" response
// as a typed, actionable card instead of free-form markdown.
//
// The action is a closed enum returned by the Phase 3 diagnoser. The
// UI ONLY renders a button if the action maps to a known kind — bogus
// values render no button (fail-safe vs broken link).

export interface DiagnosisData {
  execution_id?: string
  pipeline_id?: string
  pipeline_name?: string
  status?: string
  category?: string
  action?: string
  action_label?: string
  deep_link?: string
  confidence?: number
  rationale?: string
  source_rows?: number
  landed_rows?: number
}

const knownActions: Record<string, { label: string; icon: typeof ArrowRight }> = {
  refresh_auth: { label: "Reconnect credential", icon: ExternalLink },
  regenerate_connector: { label: "Regenerate connector", icon: ExternalLink },
  backoff_retry: { label: "Retry pipeline", icon: ArrowRight },
  request_user_config: { label: "Fix configuration", icon: ExternalLink },
}

function categoryColor(cat?: string): string {
  switch (cat) {
    case "auth_expired":
    case "auth_scope":
      return "bg-amber-50 text-amber-900 border-amber-200 dark:bg-amber-900/20 dark:text-amber-200"
    case "connector_bug":
    case "schema_drift":
      return "bg-red-50 text-red-900 border-red-200 dark:bg-red-900/20 dark:text-red-200"
    case "rate_limit":
    case "network":
      return "bg-blue-50 text-blue-900 border-blue-200 dark:bg-blue-900/20 dark:text-blue-200"
    case "dest_capacity":
    case "user_config":
      return "bg-purple-50 text-purple-900 border-purple-200 dark:bg-purple-900/20 dark:text-purple-200"
    default:
      return "bg-zinc-50 text-zinc-900 border-zinc-200 dark:bg-zinc-900/30 dark:text-zinc-200"
  }
}

export function DiagnosisCard({ data }: { data: DiagnosisData }) {
  const confidence = Math.round((data.confidence ?? 0) * 100)
  const action = (data.action || "").trim()
  const showButton = action !== "" && action in knownActions && Boolean(data.deep_link)
  const buttonLabel = data.action_label || knownActions[action]?.label || "Take action"
  const ActionIcon = knownActions[action]?.icon || ArrowRight

  return (
    <div className="mt-3 rounded-lg border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-900 p-4">
      <div className="flex items-start gap-3">
        <AlertTriangle className="h-5 w-5 text-red-500 mt-0.5 flex-shrink-0" />
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 flex-wrap">
            <span className="font-medium text-zinc-900 dark:text-zinc-100">
              {data.pipeline_name ? `${data.pipeline_name}` : "Pipeline failure"}
            </span>
            {data.category && (
              <Badge variant="outline" className={`text-xs ${categoryColor(data.category)}`}>
                {data.category}
              </Badge>
            )}
            <Badge variant="outline" className="text-xs">
              {confidence}% confidence
            </Badge>
          </div>

          {data.rationale && (
            <p className="mt-2 text-sm text-zinc-700 dark:text-zinc-300">{data.rationale}</p>
          )}

          {/* Data flow signal — only shown when we have row counts and they diverge */}
          {typeof data.source_rows === "number" &&
            typeof data.landed_rows === "number" &&
            data.source_rows > 0 &&
            data.landed_rows < data.source_rows && (
              <div className="mt-2 text-xs text-zinc-600 dark:text-zinc-400">
                Source returned <span className="font-mono">{data.source_rows}</span> rows; destination
                landed <span className="font-mono">{data.landed_rows}</span>.
              </div>
            )}

          {data.execution_id && (
            <div className="mt-1 text-xs text-zinc-500 dark:text-zinc-500">
              Execution: <span className="font-mono">{data.execution_id.slice(0, 8)}…</span>
            </div>
          )}

          <div className="mt-3 flex flex-wrap items-center gap-2">
            {showButton && (
              <Link href={data.deep_link!}>
                <Button size="sm" className="h-8">
                  <ActionIcon className="h-3.5 w-3.5 mr-1.5" />
                  {buttonLabel}
                </Button>
              </Link>
            )}
            {data.execution_id && (
              <Link href={`/executions/${data.execution_id}`}>
                <Button size="sm" variant="outline" className="h-8">
                  View execution
                </Button>
              </Link>
            )}
            {!showButton && action === "escalate_to_human" && (
              <span className="text-xs text-zinc-500">
                No automatic action — this needs a human.
              </span>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}
