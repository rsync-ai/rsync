"use client"

import { useState } from "react"
import Link from "next/link"
import { Button } from "@/components/ui/button"
import { PipelineConfirmationInline } from "@/components/chat/PipelineConfirmationInline"
import { SyncModeChoiceInline } from "@/components/chat/SyncModeChoiceInline"
import { DiagnosisCard, type DiagnosisData } from "@/components/chat/DiagnosisCard"
import { UpgradeModal } from "@/components/plan/UpgradeModal"
import type { PlanLimitPayload } from "@/lib/api/pipelines"
import type { PipelinePlan } from "@/lib/api/chat"
import type { DataLoadingStrategy } from "@/lib/pipeline/dataLoadingStrategy"

export interface ChatMessage {
  id: string
  role: "user" | "assistant"
  content: string
  suggestions?: string[]
  responseType?: string
  plan?: PipelinePlan
  strategy?: DataLoadingStrategy
  confirmationContext?: {
    sourceType: string
    destType: string
    supportedSyncModes: string[]
    sourceSupportsCDC: boolean
    sourceSupportsIncrementalBatch: boolean
  }
  syncModeChosen?: string
  diagnosis?: DiagnosisData
}

interface ChatMessageItemProps {
  message: ChatMessage
  onRunPrompt: (prompt: string, displayText?: string) => void
  onSetInput: (val: string) => void
  onSyncModeChosen?: (label: string) => void
}

function detectPlanLimit(content: string): PlanLimitPayload | null {
  if (content.includes("trial_expired")) {
    return { error: "trial_expired", message: "Your trial has expired. Upgrade to continue syncing pipelines." }
  }
  if (content.includes("pipeline_limit_reached")) {
    return { error: "pipeline_limit_reached", message: "You've reached your pipeline limit. Upgrade to create more pipelines." }
  }
  return null
}

export function ChatMessageItem({ message: m, onRunPrompt, onSetInput, onSyncModeChosen }: ChatMessageItemProps) {
  const [upgradePayload, setUpgradePayload] = useState<PlanLimitPayload | null>(null)

  const planLimitHint = m.role === "assistant" ? detectPlanLimit(m.content) : null

  return (
    <div className="text-sm">
      <div className="text-xs uppercase tracking-wide text-zinc-500">
        {m.role === "user" ? "You" : "Assistant"}
      </div>
      <div className="text-zinc-900 dark:text-zinc-100 whitespace-pre-wrap">{m.content}</div>

      {/* Upgrade CTA: shown when the assistant reply signals a plan limit */}
      {planLimitHint && (
        <div className="mt-2">
          <Button
            type="button"
            size="sm"
            className="h-7 px-3 text-xs bg-gradient-to-r from-violet-600 to-indigo-600 text-white"
            onClick={() => setUpgradePayload(planLimitHint)}
          >
            Upgrade Plan
          </Button>
          <UpgradeModal payload={upgradePayload} onClose={() => setUpgradePayload(null)} />
        </div>
      )}

      {/* Diagnosis: typed action card. Closed-enum mapping is in DiagnosisCard;
          unknown actions render no button instead of a broken link. */}
      {m.role === "assistant" && m.responseType === "diagnosis" && m.diagnosis && (
        <DiagnosisCard data={m.diagnosis} />
      )}

      {/* Confirmation: render the inline plan card (includes data loading strategy + steps) */}
      {m.role === "assistant" && m.responseType === "confirmation" && m.plan && (
        <PipelineConfirmationInline
          plan={m.plan}
          onConfirm={(approved) => onRunPrompt(approved ? "Yes" : "No")}
        />
      )}

      {/* Unified confirmation card — same shape for CDC-capable and batch-only
          sources. The options list adapts to what the source supports so the
          user sees a single consistent Start/Cancel UI regardless of whether
          they're syncing MySQL (multi-mode) or Shopify (batch-only). */}
      {m.role === "assistant" &&
        m.responseType === "confirmation" &&
        !m.plan &&
        m.confirmationContext && (() => {
          const supports = m.confirmationContext.supportedSyncModes
          const opts: { id: string; label: string }[] = []
          if (supports.includes("batch")) {
            opts.push({
              id: "batch",
              label: supports.includes("cdc")
                ? "Batch (historical + incremental)"
                : "Batch (one-time + incremental)",
            })
          }
          if (supports.includes("cdc")) {
            opts.push({ id: "initial_plus_cdc", label: "CDC (snapshot + changes)" })
            opts.push({ id: "cdc_changes_only", label: "CDC (changes only)" })
          }
          if (opts.length === 0) {
            opts.push({ id: "batch", label: "Batch sync" })
          }
          return (
            <div className="mt-3">
              <SyncModeChoiceInline
                message="Confirm pipeline"
                choiceType="sync_mode"
                options={opts}
                sourceType={m.confirmationContext.sourceType}
                destType={m.confirmationContext.destType}
                initialChosenLabel={m.syncModeChosen}
                onChoice={(choiceId, _ctx, label) => {
                  onSyncModeChosen?.(label)
                  // Send the machine command to the backend, but show the
                  // human-readable label (e.g. "Batch (historical + incremental)")
                  // in the chat history instead of "Yes sync_mode=batch".
                  if (choiceId === "batch")
                    return onRunPrompt("Yes sync_mode=batch", label)
                  if (choiceId === "initial_plus_cdc")
                    return onRunPrompt("Yes sync_mode=cdc cdc_mode=initial", label)
                  if (choiceId === "cdc_changes_only")
                    return onRunPrompt("Yes sync_mode=cdc cdc_mode=streaming_only", label)
                  return onRunPrompt("Yes", label)
                }}
                onCancel={() => onRunPrompt("No")}
              />
            </div>
          )
        })()}

      {/* Quick-reply suggestion chips.
          Skipped entirely for confirmation messages — the unified card above
          already has Start/Cancel buttons, so Yes/No chips would just be
          redundant duplicates. For connector_missing the "generate X" chip
          auto-submits since it's a discrete action, not an editable hint. */}
      {m.role === "assistant" &&
        m.responseType !== "confirmation" &&
        Array.isArray(m.suggestions) &&
        m.suggestions.length > 0 && (
          <div className="mt-2 flex flex-wrap gap-2">
            {m.suggestions.slice(0, 6).map((s) => {
              const text = String(s)
              // For connector_missing responses, "generate X" chips link to
              // the MCP generator wizard rather than auto-submitting. Chat
              // doesn't have enough context (vendor, auth type, docs URL)
              // to fire generation safely — the wizard collects what's
              // needed.
              const generateMatch =
                m.responseType === "connector_missing" &&
                /^generate\s+(.+)$/i.exec(text)
              if (generateMatch) {
                const name = generateMatch[1].trim()
                return (
                  <Link
                    key={`${m.id}:${s}`}
                    href={`/connectors/generate?name=${encodeURIComponent(name)}`}
                  >
                    <Button type="button" variant="default" size="sm" className="h-7 px-2 text-xs">
                      {text}
                    </Button>
                  </Link>
                )
              }
              return (
                <Button
                  key={`${m.id}:${s}`}
                  type="button"
                  variant="outline"
                  size="sm"
                  className="h-7 px-2 text-xs"
                  onClick={() => onSetInput(text)}
                >
                  {text}
                </Button>
              )
            })}
          </div>
        )}
    </div>
  )
}
