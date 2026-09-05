"use client"

import { useState } from "react"
import { useRouter } from "next/navigation"
import { Button } from "@/components/ui/button"
import { Card, CardContent } from "@/components/ui/card"
import { 
  StopCircle, 
  RotateCcw, 
  Play, 
  ArrowLeft,
  Loader2
} from "lucide-react"
import { cancelExecution } from "@/lib/api/executions"
import {
  AssessmentRequiredError,
  executePipelineWithRunMode,
  type AssessmentReport,
} from "@/lib/api/pipelines"
import { PreMigrationAssessmentModal } from "@/components/pipeline/PreMigrationAssessmentModal"
import Link from "next/link"

interface Props {
  execution: {
    id: string
    status: string
    pipelineId: string
  }
}

export function ExecutionDetailClient({ execution }: Props) {
  const router = useRouter()
  const [isCancelling, setIsCancelling] = useState(false)
  const [isRerunning, setIsRerunning] = useState(false)
  const [rerunMode, setRerunMode] = useState<"resume" | "reload" | null>(null)

  // Pre-migration assessment modal state. The re-run hits the same 422
  // assessment gate as PipelineActions; without this it silently fails.
  const [assessmentReport, setAssessmentReport] = useState<AssessmentReport | null>(null)
  const [pendingRunMode, setPendingRunMode] = useState<"resume" | "reload" | null>(null)
  const [submittingProceed, setSubmittingProceed] = useState(false)

  const handleCancel = async () => {
    if (isCancelling) return
    
    setIsCancelling(true)
    try {
      await cancelExecution(execution.id)
      router.refresh()
    } catch (error) {
      console.error("Failed to cancel execution:", error)
    } finally {
      setIsCancelling(false)
    }
  }

  // Re-run, surfacing the pre-migration assessment modal when the server
  // gates on warnings (422). Mirrors PipelineActions.runViaWizard:
  //   1. First attempt with ackWarnings: false (the default).
  //   2. On AssessmentRequiredError, render the modal; its Proceed callback
  //      re-invokes this with ackWarnings: true.
  const handleRerun = async (
    mode: "resume" | "reload" = "resume",
    ackWarnings = false,
    nominatedKeys?: Record<string, string[]>,
  ) => {
    if (isRerunning) return { awaitingAck: false }

    setIsRerunning(true)
    setRerunMode(mode)
    try {
      const result = await executePipelineWithRunMode(execution.pipelineId, mode, {
        ackWarnings,
        nominatedKeys,
      })
      // Navigate to the new execution
      router.push(`/executions/${result.execution_id}`)
      return { awaitingAck: false }
    } catch (error) {
      if (error instanceof AssessmentRequiredError) {
        setAssessmentReport(error.report)
        setPendingRunMode(mode)
        return { awaitingAck: true }
      }
      console.error("Failed to re-run pipeline:", error)
      return { awaitingAck: false }
    } finally {
      // If navigation happens, this component will unmount; this is still safe.
      setIsRerunning(false)
      setRerunMode(null)
    }
  }

  const isRunning = execution.status === "running" || execution.status === "pending"
  const isComplete = execution.status === "success" || execution.status === "failed" || execution.status === "cancelled"

  return (
    <>
    <Card>
      <CardContent className="p-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <Link href="/executions">
              <Button variant="outline">
                <ArrowLeft className="h-4 w-4 mr-2" />
                Back to Executions
              </Button>
            </Link>
          </div>
          
          <div className="flex items-center gap-2">
            {isRunning && (
              <Button
                variant="destructive"
                onClick={handleCancel}
                disabled={isCancelling}
              >
                {isCancelling ? (
                  <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                ) : (
                  <StopCircle className="h-4 w-4 mr-2" />
                )}
                {isCancelling ? "Cancelling..." : "Cancel Execution"}
              </Button>
            )}
            
            {isComplete && (
              <div className="flex items-center gap-2">
                <Button
                  onClick={() => handleRerun("resume")}
                  disabled={isRerunning}
                  className="bg-gradient-to-r from-violet-600 to-indigo-600 hover:from-violet-700 hover:to-indigo-700"
                >
                  {isRerunning && rerunMode === "resume" ? (
                    <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                  ) : (
                    <RotateCcw className="h-4 w-4 mr-2" />
                  )}
                  {isRerunning && rerunMode === "resume" ? "Starting..." : "Re-run (Resume)"}
                </Button>
                <Button variant="outline" onClick={() => handleRerun("reload")} disabled={isRerunning}>
                  {isRerunning && rerunMode === "reload" ? (
                    <>
                      <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                      Reloading...
                    </>
                  ) : (
                    "Re-run (Reload)"
                  )}
                </Button>
              </div>
            )}
            
            <Link href={`/pipelines/${execution.pipelineId}`}>
              <Button variant="outline">
                <Play className="h-4 w-4 mr-2" />
                View Pipeline
              </Button>
            </Link>
          </div>
        </div>
      </CardContent>
    </Card>

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
        const result = await handleRerun(pendingRunMode, true, nominatedKeys)
        // Close on success or non-ack errors; keep open on a fresh
        // 422 (the modal report will have been replaced in state).
        if (!result.awaitingAck) {
          setAssessmentReport(null)
          setPendingRunMode(null)
        }
        setSubmittingProceed(false)
      }}
    />
    </>
  )
}

