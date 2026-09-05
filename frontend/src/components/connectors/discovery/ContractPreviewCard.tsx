"use client"

import { useState } from "react"
import {
  AlertCircle,
  CheckCircle2,
  Edit3,
  HelpCircle,
  Info,
  Lock,
} from "lucide-react"
import { cn } from "@/lib/utils"
import {
  factStatus,
  isCriticalDimension,
  type Dimension,
  type FactStatus,
  type GenerationContract,
  type Question,
  type RuntimeField,
} from "@/lib/api/discovery"

interface Props {
  contract: GenerationContract
  /**
   * Map dim → answer value the user typed/picked.
   * Parent owns this state so it can submit to /confirm.
   */
  pendingAnswers: Partial<Record<Dimension, unknown>>
  onAnswerChange: (dim: Dimension, value: unknown) => void
}

const DIM_LABEL: Record<Dimension, string> = {
  vendor: "Vendor",
  api_variant: "API",
  protocol: "Protocol",
  base_url: "Endpoint",
  auth_type: "Auth",
  auth_header: "Auth header",
  operations: "Operations",
  pagination: "Pagination",
  runtime_fields: "Runtime fields",
}

const STATUS_COLOR: Record<FactStatus, string> = {
  green: "text-emerald-600 dark:text-emerald-400",
  yellow: "text-amber-600 dark:text-amber-400",
  red: "text-red-600 dark:text-red-400",
}

export function ContractPreviewCard({
  contract,
  pendingAnswers,
  onAnswerChange,
}: Props) {
  // Critical dimensions plus auth_header + pagination, which the discovery
  // probe extracts from OpenAPI security schemes and HTML pagination
  // heuristics. They previously had DIM_LABEL entries but were dropped from
  // this render list, so the UI silently swallowed the facts. The render
  // function below already no-ops on missing dimensions, so adding them is
  // safe even when the probe didn't fill them.
  const dims: Dimension[] = [
    "vendor",
    "api_variant",
    "protocol",
    "base_url",
    "auth_type",
    "auth_header",
    "operations",
    "pagination",
  ]
  const runtimeFact = contract.facts.runtime_fields
  const runtimeFields: RuntimeField[] = Array.isArray(runtimeFact?.value)
    ? (runtimeFact.value as RuntimeField[])
    : []

  return (
    <div className="rounded-lg border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-900 overflow-hidden">
      {/* Header */}
      <div className="px-4 py-3 border-b border-zinc-200 dark:border-zinc-800 bg-zinc-50/50 dark:bg-zinc-800/30">
        <div className="flex items-center justify-between gap-2">
          <div className="flex items-center gap-2">
            <Sparkle />
            <h3 className="text-sm font-semibold">Generation contract</h3>
          </div>
          <StatusBadge can_generate={contract.can_generate} />
        </div>
        {contract.refusal_reason && (
          <div className="mt-2 text-xs text-amber-700 dark:text-amber-400 flex items-start gap-1.5">
            <AlertCircle className="h-3.5 w-3.5 shrink-0 mt-0.5" />
            <span>{contract.refusal_reason}</span>
          </div>
        )}
        {/* P0 — credential-free reachability + auth-confirmation probe */}
        {contract.metadata?.reachability_probe && (
          <div
            className={cn(
              "mt-2 flex flex-wrap items-center gap-x-2 gap-y-0.5 text-[11px] rounded-md px-2.5 py-1.5",
              contract.metadata.reachability_probe.reachable
                ? "bg-emerald-50 dark:bg-emerald-950/30 text-emerald-700 dark:text-emerald-300"
                : "bg-zinc-100 dark:bg-zinc-800/50 text-zinc-600 dark:text-zinc-400"
            )}
          >
            <span className="font-medium">
              {contract.metadata.reachability_probe.reachable
                ? "✓ Endpoint reachable"
                : "⚠ Endpoint not reachable"}
            </span>
            {contract.metadata.reachability_probe.status != null && (
              <span className="opacity-80">HTTP {contract.metadata.reachability_probe.status}</span>
            )}
            {contract.metadata.reachability_probe.auth_confirmed && (
              <span className="opacity-80">
                · confirms {String(contract.metadata.reachability_probe.auth_confirmed)} auth
              </span>
            )}
            {contract.metadata.reachability_probe.detail && (
              <span className="opacity-60">· {contract.metadata.reachability_probe.detail}</span>
            )}
          </div>
        )}
      </div>

      {/* Facts list */}
      <div className="px-4 py-3 space-y-1.5">
        {dims.map((dim) => {
          const fact = contract.facts[dim]
          if (!fact) return null
          const status = factStatus(fact)
          // Don't show as red if it's not critical
          if (status === "red" && !isCriticalDimension(dim) && fact.source === "missing") {
            return null
          }
          return <FactRow key={dim} dim={dim} status={status} fact={fact} />
        })}
      </div>

      {/* Connection-time fields preview (informational) */}
      {runtimeFields.length > 0 && (
        <div className="px-4 py-3 border-t border-zinc-200 dark:border-zinc-800 bg-blue-50/50 dark:bg-blue-950/20">
          <div className="flex items-center gap-1.5 text-xs font-semibold text-blue-900 dark:text-blue-200 mb-1.5">
            <Info className="h-3.5 w-3.5" />
            Connection-time fields
          </div>
          <div className="text-[11px] text-zinc-600 dark:text-zinc-400 mb-2">
            The user fills these in when creating a connection — not now.
          </div>
          <ul className="space-y-1">
            {runtimeFields.map((f) => (
              <li key={f.name} className="flex items-center gap-2 text-xs">
                <code className="font-mono text-zinc-700 dark:text-zinc-200">{f.name}</code>
                {f.secret && <Lock className="h-3 w-3 text-zinc-400" />}
                {f.required && (
                  <span className="text-[9px] uppercase tracking-wide text-red-600 dark:text-red-400">
                    required
                  </span>
                )}
                {f.description && (
                  <span className="text-zinc-500 truncate">— {f.description}</span>
                )}
              </li>
            ))}
          </ul>
        </div>
      )}

      {/* Open questions */}
      {contract.questions.length > 0 && (
        <div className="px-4 py-3 border-t border-zinc-200 dark:border-zinc-800 space-y-3">
          <div className="text-xs font-semibold text-zinc-700 dark:text-zinc-200">
            {contract.questions.length}{" "}
            {contract.questions.length === 1 ? "question" : "questions"} before we can generate
          </div>
          {contract.questions.map((q) => (
            <QuestionField
              key={q.dimension}
              question={q}
              value={pendingAnswers[q.dimension]}
              onChange={(v) => onAnswerChange(q.dimension, v)}
            />
          ))}
        </div>
      )}

      {/* Operations summary */}
      {contract.metadata.operations_summary?.total ? (
        <div className="px-4 py-2 border-t border-zinc-200 dark:border-zinc-800 text-[11px] text-zinc-600 dark:text-zinc-400">
          Detected{" "}
          <strong>{contract.metadata.operations_summary.total}</strong> operations
          {contract.metadata.operations_summary.queries?.length ? (
            <> — {contract.metadata.operations_summary.queries.length} queries</>
          ) : null}
          {contract.metadata.operations_summary.mutations?.length ? (
            <>, {contract.metadata.operations_summary.mutations.length} mutations</>
          ) : null}
          {contract.metadata.operations_summary.custom_scalars?.length ? (
            <>
              {" · "}
              custom scalars: {contract.metadata.operations_summary.custom_scalars.join(", ")}
            </>
          ) : null}
        </div>
      ) : null}
    </div>
  )
}

function FactRow({
  dim,
  status,
  fact,
}: {
  dim: Dimension
  status: FactStatus
  fact: GenerationContract["facts"][Dimension]
}) {
  const Icon = status === "green" ? CheckCircle2 : status === "yellow" ? AlertCircle : HelpCircle
  return (
    <div className="flex items-start gap-2 text-xs">
      <Icon className={cn("h-4 w-4 shrink-0 mt-0.5", STATUS_COLOR[status])} />
      <div className="flex-1 min-w-0">
        <div className="flex items-baseline gap-2 flex-wrap">
          <span className="font-medium text-zinc-700 dark:text-zinc-300">
            {DIM_LABEL[dim]}
          </span>
          {fact.value != null && (
            <span className="text-zinc-900 dark:text-zinc-100 font-mono truncate">
              {formatValue(fact.value)}
            </span>
          )}
          {fact.source && fact.source !== "missing" && (
            <span className="text-[10px] text-zinc-400 uppercase tracking-wide">
              {fact.source.replace(/_/g, " ")}
            </span>
          )}
          {/* P0 — provenance confidence: colour by tier so a non-technical
              user can tell a verified value from a low-confidence guess. */}
          {fact.source !== "missing" && typeof fact.confidence === "number" && (
            <span
              className={cn(
                "text-[10px] px-1.5 py-0.5 rounded-full font-medium",
                fact.confidence >= 0.8
                  ? "bg-emerald-100 dark:bg-emerald-950/40 text-emerald-700 dark:text-emerald-300"
                  : fact.confidence >= 0.6
                    ? "bg-amber-100 dark:bg-amber-950/40 text-amber-700 dark:text-amber-300"
                    : "bg-red-100 dark:bg-red-950/40 text-red-700 dark:text-red-300"
              )}
              title="Discovery's confidence in this value (by source)"
            >
              {Math.round(fact.confidence * 100)}%
            </span>
          )}
        </div>
        {fact.evidence && (
          <div className="text-[10px] text-zinc-500 truncate mt-0.5">{fact.evidence}</div>
        )}
      </div>
    </div>
  )
}

function QuestionField({
  question,
  value,
  onChange,
}: {
  question: Question
  value: unknown
  onChange: (v: unknown) => void
}) {
  return (
    <div>
      <label className="block text-xs font-medium text-zinc-800 dark:text-zinc-200 mb-1">
        {question.prompt}
        {question.required && <span className="text-red-500 ml-1">*</span>}
      </label>
      {question.kind === "choice" && question.choices ? (
        <select
          value={String(value ?? "")}
          onChange={(e) => onChange(e.target.value)}
          className="w-full text-sm px-2.5 py-1.5 rounded-md border border-zinc-300 dark:border-zinc-700 bg-white dark:bg-zinc-900 focus:outline-none focus:ring-2 focus:ring-violet-500"
        >
          <option value="">— select —</option>
          {question.choices.map((c) => (
            <option key={c.value} value={c.value}>
              {c.label}
            </option>
          ))}
        </select>
      ) : question.kind === "multi_choice" ? (
        <textarea
          value={Array.isArray(value) ? value.join("\n") : String(value ?? "")}
          onChange={(e) =>
            onChange(
              e.target.value
                .split("\n")
                .map((s) => s.trim())
                .filter(Boolean),
            )
          }
          rows={3}
          placeholder="One operation per line"
          className="w-full text-sm font-mono px-2.5 py-1.5 rounded-md border border-zinc-300 dark:border-zinc-700 bg-white dark:bg-zinc-900 focus:outline-none focus:ring-2 focus:ring-violet-500"
        />
      ) : (
        <input
          type="text"
          value={String(value ?? "")}
          onChange={(e) => onChange(e.target.value)}
          placeholder={question.help_text}
          className="w-full text-sm px-2.5 py-1.5 rounded-md border border-zinc-300 dark:border-zinc-700 bg-white dark:bg-zinc-900 focus:outline-none focus:ring-2 focus:ring-violet-500"
        />
      )}
      {question.help_text && question.kind !== "free_text" && (
        <div className="text-[10px] text-zinc-500 mt-1">{question.help_text}</div>
      )}
    </div>
  )
}

function StatusBadge({ can_generate }: { can_generate: boolean }) {
  return can_generate ? (
    <span className="inline-flex items-center gap-1 text-[10px] font-bold uppercase tracking-wide text-emerald-700 dark:text-emerald-400 bg-emerald-100 dark:bg-emerald-900/40 px-2 py-0.5 rounded-full">
      <CheckCircle2 className="h-3 w-3" />
      Ready
    </span>
  ) : (
    <span className="inline-flex items-center gap-1 text-[10px] font-bold uppercase tracking-wide text-amber-700 dark:text-amber-400 bg-amber-100 dark:bg-amber-900/40 px-2 py-0.5 rounded-full">
      <Edit3 className="h-3 w-3" />
      Needs input
    </span>
  )
}

function Sparkle() {
  return (
    <span className="inline-block h-4 w-4 rounded bg-gradient-to-br from-violet-500 to-indigo-600" />
  )
}

function formatValue(v: unknown): string {
  if (Array.isArray(v)) {
    return v.length > 3 ? `${v.slice(0, 3).join(", ")} +${v.length - 3} more` : v.join(", ")
  }
  if (typeof v === "string") return v
  if (typeof v === "object" && v !== null) return JSON.stringify(v).slice(0, 80)
  return String(v)
}
