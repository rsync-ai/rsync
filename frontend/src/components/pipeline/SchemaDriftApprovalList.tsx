"use client"

import { useCallback, useEffect, useState } from "react"
import { AlertTriangle, Ban, Check, Info, RefreshCw, ShieldAlert, X } from "lucide-react"
import { toast } from "sonner"
import { cn } from "@/lib/utils"
import {
  AlreadyActionedError,
  approveSchemaChange,
  listSchemaChanges,
  rejectSchemaChange,
  type SchemaChange,
} from "@/lib/api/schema-changes"

/** Change types that DROP or rewrite data — require explicit ack, never auto-applied. */
const DESTRUCTIVE = new Set(["drop_column", "drop_table", "modify_column"])
/** Destinations the healer can actually migrate (applyMigration is PG/MySQL only). */
const RDBMS_DESTS = new Set(["postgresql", "postgres", "mysql"])

const CHANGE_LABELS: Record<string, string> = {
  add_column: "Add column",
  drop_column: "Drop column",
  modify_column: "Modify column",
  create_table: "Create table",
  drop_table: "Drop table",
}

const CHANGE_COLORS: Record<string, string> = {
  drop_column: "text-red-600 bg-red-50 border-red-200 dark:text-red-400 dark:bg-red-950/30 dark:border-red-900",
  drop_table: "text-red-600 bg-red-50 border-red-200 dark:text-red-400 dark:bg-red-950/30 dark:border-red-900",
  modify_column: "text-amber-600 bg-amber-50 border-amber-200 dark:text-amber-400 dark:bg-amber-950/30 dark:border-amber-900",
  add_column: "text-emerald-600 bg-emerald-50 border-emerald-200 dark:text-emerald-400 dark:bg-emerald-950/30 dark:border-emerald-900",
  create_table: "text-emerald-600 bg-emerald-50 border-emerald-200 dark:text-emerald-400 dark:bg-emerald-950/30 dark:border-emerald-900",
}

const STATUS_COLORS: Record<string, string> = {
  approved: "text-emerald-700 bg-emerald-50 dark:text-emerald-400 dark:bg-emerald-950/30",
  applied: "text-emerald-700 bg-emerald-50 dark:text-emerald-400 dark:bg-emerald-950/30",
  rejected: "text-zinc-600 bg-zinc-100 dark:text-zinc-400 dark:bg-zinc-800",
  failed: "text-red-700 bg-red-50 dark:text-red-400 dark:bg-red-950/30",
}

/** Risks arrive as a JSON-array-as-string (e.g. `["data loss"]`); render it readable. */
function formatRisks(risks: string): string {
  if (!risks || risks === "[]") return ""
  return risks.replace(/[\[\]"]/g, "").replace(/,/g, " · ").trim()
}

/**
 * Whether approval only RECORDS the decision: the healer's DROP/TRUNCATE guard
 * refuses to run such DDL even after approval, so the user must apply it
 * manually. Prefer the backend's auto_applicable field (kept in lockstep with
 * that guard); fall back to the change-type heuristic for older gateways.
 */
function isManualApply(c: SchemaChange): boolean {
  if (typeof c.auto_applicable === "boolean") return !c.auto_applicable
  return DESTRUCTIVE.has(c.change_type) || isAdvisory(c)
}

/**
 * Advisory rows carry a note instead of a statement: a table that appeared in the
 * source, or a declared-type change the destination does not need (B1 —
 * VARCHAR(50) → VARCHAR(10) leaves the destination column correct, because both
 * map to the same canonical type). They are also non-auto-applicable, but telling
 * the user to "run the DDL manually" would be wrong: there is no DDL to run.
 * Mirrors isAdvisoryDDL in backend-orchestrator/internal/agents/healer/healer.go.
 */
function isAdvisory(c: SchemaChange): boolean {
  return (c.ddl ?? "").trimStart().startsWith("--")
}

interface Props {
  pipelineId: string
  /** Destination connector type — disables Approve for non-relational dests. */
  destConnectorType?: string | null
  /**
   * CDC pipelines have a different approval policy from batch: the sink applies a
   * new column the moment it streams in, so nothing ever waits here. Saying so is
   * the point — the two modes answer "who approves a schema change?" oppositely,
   * and until now only the batch answer was visible anywhere in the product.
   */
  isCDC?: boolean
}

export function SchemaDriftApprovalList({ pipelineId, destConnectorType, isCDC = false }: Props) {
  const [changes, setChanges] = useState<SchemaChange[]>([])
  const [loading, setLoading] = useState(true)
  const [loadError, setLoadError] = useState<string | null>(null)
  const [actioning, setActioning] = useState<Record<string, boolean>>({})
  const [acked, setAcked] = useState<Record<string, boolean>>({})

  // Unknown dest type -> assume RDBMS (don't false-block); the backend guard is the real gate.
  const destIsRDBMS = destConnectorType ? RDBMS_DESTS.has(destConnectorType.toLowerCase()) : true

  const fetchChanges = useCallback(async () => {
    try {
      const data = await listSchemaChanges(pipelineId)
      setChanges(data)
      setLoadError(null)
    } catch (e) {
      setLoadError(e instanceof Error ? e.message : "Failed to load schema changes")
    } finally {
      setLoading(false)
    }
  }, [pipelineId])

  useEffect(() => {
    fetchChanges()
    const iv = setInterval(fetchChanges, 30_000)
    return () => clearInterval(iv)
  }, [fetchChanges])

  const handleAction = async (change: SchemaChange, action: "approve" | "reject") => {
    setActioning((p) => ({ ...p, [change.id]: true }))
    try {
      if (action === "approve") {
        // Prefer what the server says it did over what we predicted it would do.
        const dispatched = await approveSchemaChange(pipelineId, change.id)
        const manual = dispatched === null ? isManualApply(change) : !dispatched
        if (manual) {
          toast.warning(
            "Approved — decision recorded. This DDL is NOT auto-applied; run it manually on the destination.",
          )
        } else {
          // Same correction as the card banner below: the apply happens on approval,
          // not on a run, so the toast must not imply the user has to start one.
          toast.success("Approved. Applying it to the destination now.")
        }
      } else {
        await rejectSchemaChange(pipelineId, change.id)
        toast.success("Change rejected.")
      }
      await fetchChanges()
    } catch (e) {
      if (e instanceof AlreadyActionedError) {
        toast.info("This change was already actioned. Refreshing…")
        await fetchChanges()
      } else {
        toast.error(e instanceof Error ? e.message : "Action failed.")
      }
    } finally {
      setActioning((p) => ({ ...p, [change.id]: false }))
    }
  }

  const pending = changes.filter((c) => c.status === "pending")
  const resolved = changes.filter((c) => c.status !== "pending")

  if (loading) {
    return <div className="text-sm text-zinc-500">Loading schema changes…</div>
  }

  return (
    <div className="space-y-6">
      {/* Toolbar */}
      <div className="flex items-center justify-between">
        <div className="text-sm text-zinc-600 dark:text-zinc-400">
          {pending.length > 0
            ? `${pending.length} change${pending.length === 1 ? "" : "s"} awaiting your approval`
            : "No changes awaiting approval"}
        </div>
        <button
          type="button"
          onClick={fetchChanges}
          className="inline-flex items-center gap-1.5 text-xs font-medium px-2.5 py-1.5 rounded border border-zinc-200 dark:border-zinc-700 text-zinc-600 dark:text-zinc-300 hover:bg-zinc-50 dark:hover:bg-zinc-800"
        >
          <RefreshCw className="h-3.5 w-3.5" />
          Refresh
        </button>
      </div>

      {loadError && (
        <div className="flex items-center gap-2 rounded-md border border-red-200 dark:border-red-900 bg-red-50 dark:bg-red-950/30 px-3 py-2 text-sm text-red-700 dark:text-red-400">
          <AlertTriangle className="h-4 w-4 shrink-0" />
          {loadError}
        </div>
      )}

      {/* CDC policy notice. Batch waits for a human; CDC does not for additions, and
          it says so rather than leaving the user to infer the policy from the page.
          Every sentence below is prod-observed behaviour, not intent: an added column
          is applied on arrival, a dropped column/table is retained in the destination
          and the stream keeps running, and a type change is never mirrored. The
          retained-not-halted wording matters — a reader who assumes a drop stops the
          stream would not go looking for the divergence this page is reporting. */}
      {isCDC && (
        <div className="flex items-start gap-2 rounded-md border border-sky-200 dark:border-sky-900 bg-sky-50 dark:bg-sky-950/20 px-3 py-2 text-sm text-sky-800 dark:text-sky-300">
          <Info className="h-4 w-4 shrink-0 mt-0.5" />
          <span>
            This is a <strong>CDC</strong> pipeline. New source columns are added to the destination
            automatically as they stream in — they are recorded here as <strong>applied</strong> and never wait
            for approval. Drops and type changes are <strong>not</strong> mirrored: the destination keeps the
            dropped column or table and its existing data, the stream keeps running, and they are listed
            here so you can decide whether to apply them yourself.
          </span>
        </div>
      )}

      {/* Non-RDBMS destination notice */}
      {!destIsRDBMS && (
        <div className="flex items-start gap-2 rounded-md border border-amber-200 dark:border-amber-900 bg-amber-50 dark:bg-amber-950/20 px-3 py-2 text-sm text-amber-800 dark:text-amber-300">
          <Ban className="h-4 w-4 shrink-0 mt-0.5" />
          <span>
            This pipeline&apos;s destination ({destConnectorType}) is not a relational database, so schema
            changes cannot be auto-applied. You can reject changes to clear the queue, but approval is disabled.
          </span>
        </div>
      )}

      {/* Pending */}
      {pending.length > 0 && (
        <section className="space-y-3">
          {pending.map((c) => (
            <PendingCard
              key={c.id}
              change={c}
              destructive={DESTRUCTIVE.has(c.change_type)}
              manualApply={isManualApply(c)}
              advisory={isAdvisory(c)}
              actioning={!!actioning[c.id]}
              acked={!!acked[c.id]}
              approveDisabled={
                // A notice touches nothing, so a non-relational destination is no
                // reason to block acknowledging it.
                !!actioning[c.id] ||
                (!destIsRDBMS && !isAdvisory(c)) ||
                (DESTRUCTIVE.has(c.change_type) && !acked[c.id])
              }
              onAckChange={(v) => setAcked((p) => ({ ...p, [c.id]: v }))}
              onApprove={() => handleAction(c, "approve")}
              onReject={() => handleAction(c, "reject")}
            />
          ))}
        </section>
      )}

      {pending.length === 0 && !loadError && (
        <div className="flex flex-col items-center justify-center gap-2 rounded-lg border border-dashed border-zinc-200 dark:border-zinc-800 py-12 text-center">
          <Check className="h-6 w-6 text-emerald-500" />
          <div className="text-sm font-medium text-zinc-700 dark:text-zinc-300">You&apos;re all caught up</div>
          <div className="text-xs text-zinc-500">
            {isCDC
              ? "New columns are applied as they stream in and are listed below once they arrive; drops and type changes appear here for you to review."
              : "No schema-drift changes are waiting for approval."}
          </div>
        </div>
      )}

      {/* Resolved history */}
      {resolved.length > 0 && (
        <section className="space-y-2">
          <h2 className="text-sm font-semibold text-zinc-700 dark:text-zinc-300">History</h2>
          <div className="space-y-1.5">
            {resolved.map((c) => (
              <ResolvedRow key={c.id} change={c} />
            ))}
          </div>
        </section>
      )}
    </div>
  )
}

function PendingCard({
  change,
  destructive,
  manualApply,
  advisory,
  actioning,
  acked,
  approveDisabled,
  onAckChange,
  onApprove,
  onReject,
}: {
  change: SchemaChange
  destructive: boolean
  /** True when the healer's guard blocks auto-apply — approval is record-only. */
  manualApply: boolean
  /** True when the row carries a note rather than a statement (nothing to run). */
  advisory: boolean
  actioning: boolean
  acked: boolean
  approveDisabled: boolean
  onAckChange: (v: boolean) => void
  onApprove: () => void
  onReject: () => void
}) {
  const label = CHANGE_LABELS[change.change_type] ?? change.change_type
  const colorClass =
    CHANGE_COLORS[change.change_type] ??
    "text-zinc-600 bg-zinc-50 border-zinc-200 dark:text-zinc-400 dark:bg-zinc-900 dark:border-zinc-700"
  const risks = formatRisks(change.risks)

  return (
    <div className="rounded-lg border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-900 overflow-hidden">
      {/* Header */}
      <div className="flex items-center gap-3 px-4 py-3 border-b border-zinc-100 dark:border-zinc-800">
        <span className={cn("text-[10px] font-bold uppercase px-2 py-0.5 rounded border shrink-0", colorClass)}>
          {label}
        </span>
        <div className="min-w-0">
          <div className="text-sm font-medium text-zinc-900 dark:text-zinc-100 truncate">{change.table_name}</div>
          {change.user_message && <div className="text-xs text-zinc-500 truncate">{change.user_message}</div>}
        </div>
      </div>

      {/* Body */}
      <div className="px-4 py-3 space-y-3">
        <div>
          <div className="text-[10px] uppercase tracking-wide text-zinc-400 mb-1">
            {advisory ? "Detected change" : "Detected DDL"}
          </div>
          <pre className="text-xs font-mono bg-zinc-50 dark:bg-zinc-800 rounded p-2.5 overflow-x-auto text-zinc-800 dark:text-zinc-200 whitespace-pre-wrap break-all">
            {change.ddl}
          </pre>
        </div>

        {change.reasoning && (
          <div>
            <div className="text-[10px] uppercase tracking-wide text-zinc-400 mb-0.5">Reasoning</div>
            <div className="text-xs text-zinc-600 dark:text-zinc-400 leading-snug">{change.reasoning}</div>
          </div>
        )}

        {risks && (
          <div>
            <div className="text-[10px] uppercase tracking-wide text-amber-500 mb-0.5">Risks</div>
            <div className="text-xs text-amber-700 dark:text-amber-400 leading-snug">{risks}</div>
          </div>
        )}

        {/* What approval actually does — honest per row (auto_applicable mirrors the healer's DROP/TRUNCATE guard) */}
        {destructive ? (
          <div className="flex items-start gap-2 rounded-md border border-red-200 dark:border-red-900 bg-red-50 dark:bg-red-950/20 px-3 py-2">
            <ShieldAlert className="h-4 w-4 text-red-600 dark:text-red-400 shrink-0 mt-0.5" />
            <div className="space-y-2">
              <p className="text-xs text-red-700 dark:text-red-400 leading-snug">
                {manualApply ? (
                  <>
                    This is a <strong>destructive</strong> change. For safety it is <strong>not auto-applied</strong> —
                    approving records your decision, but you must run the DDL manually on the destination.
                  </>
                ) : (
                  <>
                    This is a <strong>destructive</strong> change. Approving it <strong>applies the DDL
                    automatically</strong> on the destination — make sure downstream consumers can absorb it.
                  </>
                )}
              </p>
              <label className="flex items-center gap-2 text-xs font-medium text-red-700 dark:text-red-400 cursor-pointer select-none">
                <input
                  type="checkbox"
                  checked={acked}
                  onChange={(e) => onAckChange(e.target.checked)}
                  className="h-3.5 w-3.5 rounded border-red-300 text-red-600 focus:ring-red-500"
                />
                {manualApply
                  ? "I understand this change must be applied manually."
                  : "I understand this change will be applied automatically."}
              </label>
            </div>
          </div>
        ) : advisory ? (
          // B1 and the create_table drift note. Both are non-auto-applicable, but
          // "run it manually" would be wrong advice: there is no statement to run,
          // and for a narrowed source column running one would truncate rows the
          // destination already holds.
          <div className="flex items-start gap-2 rounded-md border border-sky-200 dark:border-sky-900 bg-sky-50 dark:bg-sky-950/20 px-3 py-2">
            <Info className="h-4 w-4 text-sky-600 dark:text-sky-400 shrink-0 mt-0.5" />
            <p className="text-xs text-sky-800 dark:text-sky-300 leading-snug">
              This is a <strong>notice</strong>, not a statement — your destination already holds the right type
              and nothing will be run against it. Acknowledge or reject to clear it from the queue.
            </p>
          </div>
        ) : manualApply ? (
          <div className="flex items-start gap-2 rounded-md border border-amber-200 dark:border-amber-900 bg-amber-50 dark:bg-amber-950/20 px-3 py-2">
            <Info className="h-4 w-4 text-amber-600 dark:text-amber-400 shrink-0 mt-0.5" />
            <p className="text-xs text-amber-700 dark:text-amber-400 leading-snug">
              This DDL is <strong>not auto-applied</strong> (it trips the destructive-DDL safety guard) — approving
              records your decision; run the DDL manually on the destination.
            </p>
          </div>
        ) : (
          // A2: this used to read "applies it automatically on the next run", which
          // is not what happens. Approval publishes to the healer, which runs
          // applyMigration immediately (healer.go:249) and flips the row to
          // `applied` — no pipeline run is involved. Proven on prod: the new column
          // appeared in the destination between Approve and the very next query,
          // with zero reruns. The old wording also contradicted the notification the
          // same approval produces ("has been applied to your destination",
          // notifier.go:288), so a user who believed the card was left waiting for a
          // run that had nothing left to do.
          <div className="flex items-start gap-2 rounded-md border border-emerald-200 dark:border-emerald-900 bg-emerald-50 dark:bg-emerald-950/20 px-3 py-2">
            <Info className="h-4 w-4 text-emerald-600 dark:text-emerald-400 shrink-0 mt-0.5" />
            <p className="text-xs text-emerald-700 dark:text-emerald-400 leading-snug">
              Additive change — approving applies it to the destination straight away. You do not need to
              wait for, or start, a pipeline run.
            </p>
          </div>
        )}
      </div>

      {/* Actions */}
      <div className="flex items-center justify-end gap-2 px-4 py-3 bg-zinc-50 dark:bg-zinc-800/40 border-t border-zinc-100 dark:border-zinc-800">
        <button
          type="button"
          onClick={onReject}
          disabled={actioning}
          className="inline-flex items-center gap-1.5 text-xs font-medium px-3 py-1.5 rounded border border-zinc-200 dark:border-zinc-700 hover:bg-zinc-100 dark:hover:bg-zinc-700 text-zinc-700 dark:text-zinc-300 disabled:opacity-50"
        >
          <X className="h-3.5 w-3.5" />
          Reject
        </button>
        <button
          type="button"
          onClick={onApprove}
          disabled={approveDisabled}
          className="inline-flex items-center gap-1.5 text-xs font-medium px-3 py-1.5 rounded bg-emerald-600 hover:bg-emerald-700 text-white disabled:opacity-40 disabled:cursor-not-allowed"
        >
          <Check className="h-3.5 w-3.5" />
          {advisory ? "Acknowledge" : manualApply ? "Approve (manual apply)" : "Approve"}
        </button>
      </div>
    </div>
  )
}

function ResolvedRow({ change }: { change: SchemaChange }) {
  const label = CHANGE_LABELS[change.change_type] ?? change.change_type
  const statusClass =
    STATUS_COLORS[change.status] ?? "text-zinc-600 bg-zinc-100 dark:text-zinc-400 dark:bg-zinc-800"
  return (
    <div className="rounded-md border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-900 px-3 py-2">
      <div className="flex items-center gap-2 text-xs">
        <span className={cn("font-semibold uppercase px-1.5 py-0.5 rounded", statusClass)}>{change.status}</span>
        <span className="text-zinc-500">{label}</span>
        <span className="font-medium text-zinc-800 dark:text-zinc-200 truncate">{change.table_name}</span>
      </div>
      {change.status === "approved" && isManualApply(change) && (
        <div className="mt-1 text-[11px] text-zinc-500 dark:text-zinc-400">
          {isAdvisory(change)
            ? // Telling the user to "run it manually" would be wrong here: the row
              // carries a note, not a statement, and there is nothing to run.
              "Acknowledged — this was a notice about the source. Nothing was run against the destination."
            : "Approval recorded — this DDL is not auto-applied. Run it manually on the destination."}
        </div>
      )}
      {change.status === "failed" && change.error_message && (
        <div className="mt-1 flex items-start gap-1.5 text-[11px] text-red-600 dark:text-red-400">
          <AlertTriangle className="h-3 w-3 shrink-0 mt-0.5" />
          <span className="break-all">{change.error_message}</span>
        </div>
      )}
    </div>
  )
}
