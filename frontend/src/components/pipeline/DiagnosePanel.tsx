"use client"

import { useState } from "react"
import { Loader2 } from "lucide-react"

import { authFetch } from "@/lib/api/auth-fetch"
import { API_ENDPOINTS } from "@/lib/config/api"
import { Button } from "@/components/ui/button"
import { cn } from "@/lib/utils"

// DiagnosePanel triggers an LLM-powered triage of the pipeline. Read-only:
// the model only explains, it never acts. Result is rendered inline below the
// button. Pattern copied from PipelineAccordionView's DiagnoseButton so the
// header and the Monitor tab can share a single implementation.
type DiagnoseVerdict = {
  summary?: string
  root_cause?: string
  suggested_action?: string
  confidence?: string
  evidence_pointers?: string[]
  raw?: string
  evidence?: Record<string, unknown>
}

export function DiagnosePanel({
  pipelineId,
  prompt = "Need help interpreting this run? Run a quick LLM-powered triage.",
}: {
  pipelineId: string
  prompt?: string
}) {
  const [loading, setLoading] = useState(false)
  const [verdict, setVerdict] = useState<DiagnoseVerdict | null>(null)
  const [error, setError] = useState<string | null>(null)

  const run = async () => {
    setLoading(true)
    setError(null)
    try {
      const res = await authFetch(API_ENDPOINTS.PIPELINES.DIAGNOSE(pipelineId), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: "{}",
      })
      const data = (await res.json().catch(() => ({}))) as DiagnoseVerdict & {
        message?: string
        error?: string
      }
      if (!res.ok) {
        setError(data?.message || data?.error || `diagnose ${res.status}`)
        // Server returns evidence even on LLM failure — surface it.
        if (data?.evidence) setVerdict({ evidence: data.evidence })
        return
      }
      setVerdict(data as DiagnoseVerdict)
    } catch (e) {
      setError(String((e as Error)?.message ?? e))
    } finally {
      setLoading(false)
    }
  }

  const confidenceTone =
    verdict?.confidence === "high"
      ? "text-green-700 dark:text-green-400"
      : verdict?.confidence === "medium"
        ? "text-amber-700 dark:text-amber-400"
        : "text-muted-foreground"

  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between gap-2">
        <p className="text-xs text-muted-foreground">{prompt}</p>
        <Button size="sm" variant="outline" onClick={run} disabled={loading}>
          {loading ? (
            <>
              <Loader2 className="h-3 w-3 mr-1 animate-spin" />
              Diagnosing…
            </>
          ) : (
            "Diagnose"
          )}
        </Button>
      </div>

      {error && (
        <div className="rounded border border-red-200 bg-red-50/60 p-2 text-xs text-red-700 dark:border-red-900/40 dark:bg-red-950/20 dark:text-red-300">
          {error}
        </div>
      )}

      {verdict && (verdict.summary || verdict.root_cause || verdict.raw) && (
        <div className="rounded-lg border border-border bg-muted/30 p-3 space-y-2">
          <div className="flex items-center justify-between">
            <p className="text-xs font-medium text-foreground">Diagnosis</p>
            {verdict.confidence ? (
              <span className={cn("text-[10px] uppercase tracking-wide", confidenceTone)}>
                {verdict.confidence} confidence
              </span>
            ) : null}
          </div>
          {verdict.summary && (
            <p className="text-sm font-medium text-foreground">{verdict.summary}</p>
          )}
          {verdict.root_cause && (
            <p className="text-xs text-muted-foreground whitespace-pre-wrap">{verdict.root_cause}</p>
          )}
          {verdict.suggested_action && (
            <div className="rounded border border-violet-200 bg-violet-50/60 p-2 dark:border-violet-900/40 dark:bg-violet-950/20">
              <p className="text-[11px] uppercase tracking-wide text-violet-700 dark:text-violet-300 mb-0.5">
                Suggested next step
              </p>
              <p className="text-xs text-violet-900 dark:text-violet-100">{verdict.suggested_action}</p>
            </div>
          )}
          {verdict.evidence_pointers && verdict.evidence_pointers.length > 0 && (
            <div className="text-[11px] text-muted-foreground">
              <span className="uppercase tracking-wide mr-1">evidence:</span>
              {verdict.evidence_pointers.join(" · ")}
            </div>
          )}
          {verdict.raw && !verdict.summary && (
            <pre className="text-[11px] text-muted-foreground whitespace-pre-wrap">{verdict.raw}</pre>
          )}
        </div>
      )}
    </div>
  )
}
