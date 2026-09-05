"use client"

import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { 
  GitBranch, 
  Play,
  X,
  ArrowRight,
  Database,
  Cloud,
  Zap,
  CheckCircle2,
} from "lucide-react"
import { PipelinePlan } from "@/lib/api/chat"
import { computeStrategySteps, type DataLoadingStrategy } from "@/lib/pipeline/dataLoadingStrategy"

interface PipelineConfirmationInlineProps {
  plan?: PipelinePlan
  onConfirm: (approved: boolean) => void
}

/**
 * Inline pipeline confirmation for chat - shows plan and asks for approval
 */
export function PipelineConfirmationInline({
  plan,
  onConfirm,
}: PipelineConfirmationInlineProps) {
  if (!plan) {
    return null
  }

  const strategy: DataLoadingStrategy = {
    mode: plan.sync_mode || "batch",
    // For chat-confirmation we don’t know dataset/schedule; keep it simple and deterministic.
    rerun_default: plan.sync_mode === "cdc" ? "" : "resume",
  }
  const strategySteps = computeStrategySteps(strategy)

  const getStepIcon = (tool: string) => {
    const toolLower = tool.toLowerCase()
    if (toolLower.includes("mysql") || toolLower.includes("postgres") || toolLower.includes("database")) {
      return Database
    }
    if (toolLower.includes("s3") || toolLower.includes("cloud") || toolLower.includes("snowflake")) {
      return Cloud
    }
    return Zap
  }

  return (
    <Card className="mt-4 border-green-200 dark:border-green-800 bg-gradient-to-br from-green-50/50 to-emerald-50/50 dark:from-green-950/20 dark:to-emerald-950/20">
      <CardHeader className="pb-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-green-100 dark:bg-green-900">
              <GitBranch className="h-5 w-5 text-green-600 dark:text-green-400" />
            </div>
            <div>
              <CardTitle className="text-lg">Pipeline Ready</CardTitle>
              <CardDescription>
                Review and approve the execution plan
              </CardDescription>
            </div>
          </div>
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        {/* Pipeline info */}
        <div className="flex items-center gap-4 p-3 rounded-lg bg-white dark:bg-zinc-900 border border-zinc-200 dark:border-zinc-800">
          <div className="flex-1">
            <p className="font-medium text-zinc-900 dark:text-white">
              {plan.name || `Pipeline ${plan.pipeline_id?.slice(0, 8)}`}
            </p>
            <div className="flex items-center gap-2 mt-1">
              {plan.sync_mode && (
                <Badge variant={plan.sync_mode === "cdc" ? "info" : "secondary"}>
                  {plan.sync_mode === "cdc" ? "Real-time CDC" : "Batch"}
                </Badge>
              )}
              {plan.data_path?.transport && (
                <Badge variant="outline">
                  {plan.data_path.transport}
                </Badge>
              )}
            </div>
          </div>
        </div>

        {/* Data loading strategy */}
        <div className="space-y-2">
          <p className="text-sm font-medium text-zinc-700 dark:text-zinc-300">
            Data loading strategy:
          </p>
          <div className="space-y-2">
            {strategySteps.slice(0, 5).map((s, i) => (
              <div
                key={`${i}-${s}`}
                className="flex items-start gap-3 p-3 rounded-lg bg-white dark:bg-zinc-900 border border-zinc-200 dark:border-zinc-800"
              >
                <div className="flex h-7 w-7 items-center justify-center rounded-full bg-zinc-100 dark:bg-zinc-800 text-zinc-700 dark:text-zinc-200 font-medium text-xs shrink-0">
                  {i + 1}
                </div>
                <div className="text-sm text-zinc-700 dark:text-zinc-200">{s}</div>
              </div>
            ))}
          </div>
        </div>

        {/* Steps */}
        <div className="space-y-2">
          <p className="text-sm font-medium text-zinc-700 dark:text-zinc-300">
            Execution Steps:
          </p>
          <div className="space-y-2">
            {plan.steps?.map((step, i) => {
              const StepIcon = getStepIcon(step.tool)
              return (
                <div
                  key={step.id || i}
                  className="flex items-center gap-3 p-3 rounded-lg bg-white dark:bg-zinc-900 border border-zinc-200 dark:border-zinc-800"
                >
                  <div className="flex h-8 w-8 items-center justify-center rounded-full bg-green-100 dark:bg-green-900 text-green-600 font-medium text-sm">
                    {i + 1}
                  </div>
                  <StepIcon className="h-4 w-4 text-zinc-400" />
                  <div className="flex-1">
                    <p className="text-sm font-medium text-zinc-900 dark:text-white">
                      {step.description || `${step.method} on ${step.tool}`}
                    </p>
                    <p className="text-xs text-zinc-500">
                      {step.tool} • {step.method}
                    </p>
                  </div>
                  {i < (plan.steps?.length || 0) - 1 && (
                    <ArrowRight className="h-4 w-4 text-zinc-300" />
                  )}
                </div>
              )
            })}
          </div>
        </div>

        {/* Data path explanation */}
        {plan.data_path?.reason && (
          <div className="p-3 rounded-lg bg-zinc-100 dark:bg-zinc-800 text-sm text-zinc-600 dark:text-zinc-400">
            <span className="font-medium">Data path:</span> {plan.data_path.reason}
          </div>
        )}

        {/* Actions */}
        <div className="flex justify-end gap-3 pt-2">
          <Button variant="outline" onClick={() => onConfirm(false)}>
            <X className="h-4 w-4 mr-2" />
            Cancel
          </Button>
          <Button
            onClick={() => onConfirm(true)}
            className="bg-gradient-to-r from-green-600 to-emerald-600"
          >
            <Play className="h-4 w-4 mr-2" />
            Start Pipeline
            <CheckCircle2 className="h-4 w-4 ml-2" />
          </Button>
        </div>
      </CardContent>
    </Card>
  )
}
