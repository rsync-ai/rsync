"use client"

import { useEffect, useState } from "react"
import Link from "next/link"
import { AlertTriangle, HelpCircle } from "lucide-react"
import { listSchemaChanges } from "@/lib/api/schema-changes"
import { cn } from "@/lib/utils"

/**
 * Header pill that surfaces how many schema-drift changes are awaiting approval,
 * deep-linking to the approval page (`/pipelines/:id/schema-changes`). Renders
 * nothing when there is nothing pending, so it stays invisible in the common case.
 * This is the discoverability path that complements the healer's notification.
 */
export function SchemaDriftBadge({ pipelineId, className }: { pipelineId: string; className?: string }) {
  const [pending, setPending] = useState(0)
  // Rendering nothing is this badge's encoding for "no drift awaits approval".
  // A read that never succeeded must not borrow those pixels — that is the
  // answer that stops the operator from looking.
  const [neverLoaded, setNeverLoaded] = useState(true)

  useEffect(() => {
    let cancelled = false

    const fetchPending = async () => {
      try {
        const changes = await listSchemaChanges(pipelineId)
        if (!cancelled) {
          setPending(changes.filter((c) => c.status === "pending").length)
          setNeverLoaded(false)
        }
      } catch {
        // Keep the last known count if we ever had one; `neverLoaded` stays
        // true only while we have never had an answer at all.
      }
    }

    void fetchPending()
    const interval = window.setInterval(fetchPending, 30_000)
    return () => {
      cancelled = true
      window.clearInterval(interval)
    }
  }, [pipelineId])

  if (neverLoaded) {
    // Muted, not amber: this is "we could not check", which must not look like
    // "there is drift". It still links through, because the approval page is
    // where the real error message lives (SchemaDriftApprovalList reports it).
    return (
      <Link
        href={`/pipelines/${pipelineId}/schema-changes`}
        title="Could not check for schema-drift changes — open the approval page for details"
        className={cn(
          "inline-flex items-center gap-1 rounded-full border border-zinc-300 bg-zinc-50 px-2 py-0.5 text-xs font-medium text-zinc-600 hover:bg-zinc-100 dark:border-zinc-700 dark:bg-zinc-900 dark:text-zinc-400 dark:hover:bg-zinc-800",
          className,
        )}
      >
        <HelpCircle className="h-3 w-3" />
        Drift status unavailable
      </Link>
    )
  }

  if (pending === 0) return null

  return (
    <Link
      href={`/pipelines/${pipelineId}/schema-changes`}
      title="Review schema-drift changes awaiting approval"
      className={cn(
        "inline-flex items-center gap-1 rounded-full border border-amber-300 bg-amber-50 px-2 py-0.5 text-xs font-semibold text-amber-700 hover:bg-amber-100 dark:border-amber-900 dark:bg-amber-950/30 dark:text-amber-300 dark:hover:bg-amber-900/40",
        className,
      )}
    >
      <AlertTriangle className="h-3 w-3" />
      {pending} schema {pending === 1 ? "change" : "changes"}
    </Link>
  )
}
