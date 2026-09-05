"use client"

import * as React from "react"
import { AlertTriangle, AlertCircle, Info, Loader2 } from "lucide-react"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { Button } from "@/components/ui/button"
import { cn } from "@/lib/utils"
import type {
  AssessmentFinding,
  AssessmentReport,
  AssessmentSeverity,
} from "@/lib/api/pipelines"

interface PreMigrationAssessmentModalProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  report: AssessmentReport | null
  // Called when the user clicks "Run anyway". The caller is responsible
  // for re-issuing the pipeline run with ack_warnings: true and closing
  // the modal once the run kicks off. `nominatedKeys` carries any key
  // columns the user picked for keyless tables (PR-D), shape
  // { "<table>": ["col1", …] } — empty when nothing was nominated.
  onProceed: (nominatedKeys: Record<string, string[]>) => void | Promise<void>
  // Submission spinner. Disables the Proceed button while the parent
  // re-issues the run.
  submitting?: boolean
}

// Stable identity for a table across renders and for the nominated-keys map.
// Matches the server's lookup, which accepts schema-qualified or bare names.
function tableKey(table: { name: string; schema?: string }) {
  return table.schema ? `${table.schema}.${table.name}` : table.name
}

const SEVERITY_ORDER: Record<AssessmentSeverity, number> = {
  error: 0,
  warning: 1,
  info: 2,
}

function severityIcon(s: AssessmentSeverity) {
  switch (s) {
    case "error":
      return <AlertCircle className="h-4 w-4 text-red-500 shrink-0" aria-hidden />
    case "warning":
      return <AlertTriangle className="h-4 w-4 text-amber-500 shrink-0" aria-hidden />
    default:
      return <Info className="h-4 w-4 text-blue-500 shrink-0" aria-hidden />
  }
}

function severityLabel(s: AssessmentSeverity) {
  switch (s) {
    case "error":
      return "Blocker"
    case "warning":
      return "Warning"
    default:
      return "Note"
  }
}

// Build a stable identifier for a finding so we can track its ack state
// in the parent's Set without relying on object identity.
function findingKey(tableName: string, finding: AssessmentFinding, idx: number) {
  return `${tableName}::${finding.code}::${idx}`
}

// Coerce an unknown details value into a string array (remediation steps /
// SQL come back as []string from the Go assessor). Returns [] for anything
// that isn't a non-empty array of strings.
function asStringArray(v: unknown): string[] {
  if (!Array.isArray(v)) return []
  return v.filter((x): x is string => typeof x === "string" && x.trim() !== "")
}

function asString(v: unknown): string {
  return typeof v === "string" ? v : ""
}

/**
 * Remediation block: renders the actionable "what you must do" guidance
 * the backend attaches to a finding's `details` (remediation steps, SQL to
 * run, a docs link, an effort estimate). This is what turns a source-
 * readiness blocker into something the user can actually resolve.
 */
function FindingRemediation({ details }: { details?: Record<string, unknown> }) {
  if (!details) return null
  const steps = asStringArray(details.steps)
  const sql = asStringArray(details.sql_to_run)
  const docURL = asString(details.doc_url)
  const minutes =
    typeof details.estimated_minutes === "number" && details.estimated_minutes > 0
      ? details.estimated_minutes
      : 0
  if (steps.length === 0 && sql.length === 0 && !docURL) return null

  return (
    <div className="mt-1.5 space-y-1.5 border-l-2 border-zinc-300 dark:border-zinc-700 pl-2.5">
      {steps.length > 0 && (
        <div>
          <p className="text-[10px] font-medium uppercase tracking-wide text-zinc-500">
            What to do{minutes > 0 ? ` · ~${minutes} min` : ""}
          </p>
          <ol className="mt-0.5 list-decimal list-inside space-y-0.5 text-xs text-zinc-700 dark:text-zinc-300">
            {steps.map((s, i) => (
              <li key={i} className="leading-snug">
                {s}
              </li>
            ))}
          </ol>
        </div>
      )}
      {sql.length > 0 && (
        <pre className="overflow-x-auto rounded bg-zinc-100 dark:bg-zinc-900 p-2 text-[11px] font-mono text-zinc-800 dark:text-zinc-200">
          {sql.join("\n")}
        </pre>
      )}
      {docURL && (
        <a
          href={docURL}
          target="_blank"
          rel="noopener noreferrer"
          className="inline-block text-xs text-blue-600 dark:text-blue-400 underline underline-offset-2"
        >
          View documentation →
        </a>
      )}
    </div>
  )
}

/**
 * KeyColumnPicker (PR-D column nomination): rendered for keyless / GIPK tables.
 * Lets the user pick the column(s) that uniquely identify a row so the run
 * performs true in-place upserts on those columns instead of falling back to
 * the content-hash surrogate key (which retains old row versions on update).
 * Optional — leaving it empty keeps the synthetic-hash default.
 */
function KeyColumnPicker({
  columns,
  selected,
  onToggle,
}: {
  columns: string[]
  selected: string[]
  onToggle: (column: string, checked: boolean) => void
}) {
  if (columns.length === 0) return null
  const selectedSet = new Set(selected)
  return (
    <div className="mt-2 rounded-md border border-zinc-200 dark:border-zinc-800 bg-zinc-50/60 dark:bg-zinc-900/40 p-2.5">
      <p className="text-[11px] font-medium text-zinc-700 dark:text-zinc-300">
        Nominate key column(s) for in-place updates{" "}
        <span className="font-normal text-zinc-500">(optional)</span>
      </p>
      <p className="text-[10px] text-zinc-500 leading-snug mt-0.5">
        Pick the column(s) that uniquely identify a row. Leave empty to use a
        content-hash surrogate key (updates keep the prior row version).
      </p>
      <div className="mt-1.5 flex flex-wrap gap-x-3 gap-y-1">
        {columns.map((col) => {
          const checked = selectedSet.has(col)
          return (
            <label
              key={col}
              className="flex items-center gap-1.5 cursor-pointer select-none text-xs"
            >
              <input
                type="checkbox"
                className="h-3.5 w-3.5 cursor-pointer"
                checked={checked}
                onChange={(e) => onToggle(col, e.target.checked)}
                aria-label={`Use ${col} as a key column`}
              />
              <span className="font-mono text-zinc-700 dark:text-zinc-300">
                {col}
              </span>
            </label>
          )
        })}
      </div>
      {selected.length > 0 && (
        <p className="mt-1.5 text-[10px] text-emerald-600 dark:text-emerald-400">
          Key: {selected.join(", ")} — rows will update in place on this key.
        </p>
      )}
    </div>
  )
}

/**
 * DMS-style pre-migration assessment modal.
 *
 * Behaviour:
 *   - Errors block the Proceed button entirely (no ack possible).
 *   - Warnings require an individual checkbox ack from the user
 *     before Proceed enables. Mirrors AWS DMS' pre-migration
 *     assessment workflow.
 *   - Info findings are shown but require no interaction.
 *
 * The actual pipeline-run retry happens in the parent — this modal
 * is purely presentational + ack-tracking.
 */
export function PreMigrationAssessmentModal({
  open,
  onOpenChange,
  report,
  onProceed,
  submitting,
}: PreMigrationAssessmentModalProps) {
  // Ack state: { findingKey -> true } for every warning the user has
  // checked. Resets whenever a new report arrives.
  const [acked, setAcked] = React.useState<Record<string, boolean>>({})

  // PR-D: nominated key columns per table, { tableKey -> [col, …] }. Resets on
  // a fresh report. Passed to onProceed so the run upserts on these columns.
  const [nominatedKeys, setNominatedKeys] = React.useState<
    Record<string, string[]>
  >({})

  React.useEffect(() => {
    setAcked({})
    setNominatedKeys({})
  }, [report?.generated_at])

  const toggleNominatedKey = React.useCallback(
    (tKey: string, column: string, checked: boolean) => {
      setNominatedKeys((prev) => {
        const current = prev[tKey] ?? []
        const next = checked
          ? [...current.filter((c) => c !== column), column]
          : current.filter((c) => c !== column)
        return { ...prev, [tKey]: next }
      })
    },
    [],
  )

  if (!report) return null

  const allWarnings: { key: string; tableName: string; finding: AssessmentFinding }[] = []
  let errorCount = 0
  let infoCount = 0
  report.tables.forEach((t) => {
    t.findings.forEach((f, idx) => {
      const key = findingKey(t.name, f, idx)
      if (f.severity === "warning") {
        allWarnings.push({ key, tableName: t.name, finding: f })
      } else if (f.severity === "error") {
        errorCount++
      } else if (f.severity === "info") {
        infoCount++
      }
    })
  })

  const allAcked = allWarnings.length === 0 || allWarnings.every((w) => acked[w.key])
  const canProceed = errorCount === 0 && allAcked && !submitting

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-3xl max-h-[85vh] flex flex-col">
        <DialogHeader>
          <DialogTitle>Pre-migration assessment</DialogTitle>
          <DialogDescription>
            {report.summary}
            {report.source_connector_type && report.destination_connector_type && (
              <>
                {" — "}
                <span className="font-mono text-xs">
                  {report.source_connector_type} → {report.destination_connector_type}
                </span>
              </>
            )}
          </DialogDescription>
        </DialogHeader>

        <div className="overflow-y-auto flex-1 -mx-2 px-2 space-y-4 my-4">
          {report.tables.map((table) => {
            const sortedFindings = [...table.findings].sort(
              (a, b) => SEVERITY_ORDER[a.severity] - SEVERITY_ORDER[b.severity],
            )
            // Keyless table → offer a key-column picker (PR-D). Only when the
            // source has no declared PK (synthetic-hash fallback) and we know
            // its columns.
            const tKey = tableKey(table)
            const showPicker =
              table.primary_key_source === "synthetic" &&
              Array.isArray(table.columns) &&
              table.columns.length > 0
            return (
              <section
                key={`${table.schema ?? ""}.${table.name}`}
                className="rounded-md border border-zinc-200 dark:border-zinc-800 p-3 space-y-2"
              >
                <header className="flex items-baseline justify-between gap-2">
                  <h3 className="font-mono text-sm">
                    {table.schema ? `${table.schema}.${table.name}` : table.name}
                  </h3>
                  <span className="text-xs text-zinc-500">
                    {table.column_count} columns
                    {table.json_column_count > 0 && (
                      <> · {table.json_column_count} JSON</>
                    )}
                    {table.primary_key_source && <> · pk:{table.primary_key_source}</>}
                  </span>
                </header>

                {sortedFindings.length === 0 ? (
                  <p className="text-xs text-zinc-500">No findings.</p>
                ) : (
                  <ul className="space-y-2">
                    {sortedFindings.map((finding, idx) => {
                      const key = findingKey(table.name, finding, idx)
                      const isWarning = finding.severity === "warning"
                      return (
                        <li
                          key={key}
                          className={cn(
                            "flex items-start gap-2 rounded-md p-2 text-sm",
                            finding.severity === "error" &&
                              "bg-red-50 dark:bg-red-950/30",
                            finding.severity === "warning" &&
                              "bg-amber-50 dark:bg-amber-950/30",
                            finding.severity === "info" &&
                              "bg-blue-50/60 dark:bg-blue-950/20",
                          )}
                        >
                          {severityIcon(finding.severity)}
                          <div className="flex-1 space-y-1">
                            <div className="flex items-baseline gap-2">
                              <span className="text-[10px] font-medium uppercase tracking-wide text-zinc-500">
                                {severityLabel(finding.severity)}
                              </span>
                              <span className="text-[10px] font-mono text-zinc-400">
                                {finding.code}
                              </span>
                            </div>
                            <p className="text-sm leading-snug">{finding.message}</p>
                            <FindingRemediation details={finding.details} />
                            {isWarning && (
                              <label className="flex items-center gap-2 mt-1 cursor-pointer select-none">
                                <input
                                  type="checkbox"
                                  className="h-4 w-4 cursor-pointer"
                                  checked={!!acked[key]}
                                  onChange={(e) =>
                                    setAcked((prev) => ({
                                      ...prev,
                                      [key]: e.target.checked,
                                    }))
                                  }
                                  aria-label={`Acknowledge ${finding.code} for ${table.name}`}
                                />
                                <span className="text-xs text-zinc-700 dark:text-zinc-300">
                                  I understand
                                </span>
                              </label>
                            )}
                          </div>
                        </li>
                      )
                    })}
                  </ul>
                )}

                {showPicker && (
                  <KeyColumnPicker
                    columns={table.columns ?? []}
                    selected={nominatedKeys[tKey] ?? []}
                    onToggle={(col, checked) =>
                      toggleNominatedKey(tKey, col, checked)
                    }
                  />
                )}
              </section>
            )
          })}
        </div>

        <DialogFooter>
          <Button
            variant="outline"
            onClick={() => onOpenChange(false)}
            disabled={submitting}
          >
            Cancel
          </Button>
          <Button
            onClick={() => {
              // Drop empty selections so the payload only carries real keys.
              const picked: Record<string, string[]> = {}
              for (const [k, cols] of Object.entries(nominatedKeys)) {
                if (cols.length > 0) picked[k] = cols
              }
              void onProceed(picked)
            }}
            disabled={!canProceed}
          >
            {submitting && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
            {errorCount > 0
              ? "Resolve errors first"
              : allAcked
                ? "Run anyway"
                : `Acknowledge ${allWarnings.length - Object.values(acked).filter(Boolean).length} more`}
          </Button>
        </DialogFooter>

        {infoCount > 0 && (
          <p className="text-[10px] text-zinc-400 -mt-2 text-right">
            {infoCount} info {infoCount === 1 ? "note" : "notes"} require no acknowledgement.
          </p>
        )}
      </DialogContent>
    </Dialog>
  )
}
