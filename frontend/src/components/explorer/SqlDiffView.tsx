"use client"

import { useMemo } from "react"
import { AlertTriangle } from "lucide-react"

import { diffLines, toHunks } from "./sqlDiff"

// Renders a unified diff of two SQL texts. Used for "this old version vs now" in the
// history panel and for "now vs what is being proposed" in the approval panel — the
// same rendering both times on purpose, so a reviewer reads one kind of diff.
//
// Two things here are correctness, not styling:
//
//   The +/− sign is a real character in the row, not a colour. Roughly one reader in
//   twelve cannot separate the red rows from the green ones, and "which lines are you
//   removing" is the entire question this component exists to answer.
//
//   When the differ reports exact: false it could not align the texts and returned a
//   whole-for-whole replacement. That renders as "every line changed", which is a
//   truthful diff of a misleading shape, so it gets a banner saying so. Silently
//   showing it is how someone concludes a proposal rewrites everything when it
//   changes one line.

interface SqlDiffViewProps {
  base: string
  next: string
  /** What the left/removed side is, e.g. "Version 3". Shown in the summary line. */
  baseLabel: string
  /** What the right/added side is, e.g. "Current". */
  nextLabel: string
  /** Unchanged lines to keep around each change. */
  context?: number
  className?: string
}

export function SqlDiffView({
  base,
  next,
  baseLabel,
  nextLabel,
  context = 3,
  className = "",
}: SqlDiffViewProps) {
  const diff = useMemo(() => diffLines(base, next), [base, next])
  const hunks = useMemo(() => toHunks(diff, context), [diff, context])

  return (
    <div className={className}>
      <div className="mb-1.5 flex flex-wrap items-center gap-x-2 gap-y-1 text-xs">
        <span className="text-zinc-500">
          {baseLabel} <span aria-hidden="true">→</span>
          <span className="sr-only">compared with</span> {nextLabel}
        </span>
        {diff.added === 0 && diff.removed === 0 ? (
          <span className="text-zinc-500">identical</span>
        ) : (
          <>
            <span className="font-medium text-emerald-700 dark:text-emerald-400">
              +{diff.added} added
            </span>
            <span className="font-medium text-red-700 dark:text-red-400">
              −{diff.removed} removed
            </span>
          </>
        )}
      </div>

      {!diff.exact && (
        <div className="mb-1.5 flex items-start gap-2 rounded-md border border-amber-500/40 bg-amber-500/10 p-2">
          <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0 text-amber-600 dark:text-amber-400" />
          <p className="text-xs text-amber-700 dark:text-amber-300">
            These two versions are too large to compare line by line, so every line is
            shown as replaced. That is not a sign the whole query was rewritten — read
            the two texts directly before deciding.
          </p>
        </div>
      )}

      {hunks.length === 0 ? (
        <p className="rounded-md border bg-zinc-50 p-3 text-xs text-zinc-500 dark:bg-zinc-900">
          The SQL is character-for-character the same in both.
        </p>
      ) : (
        <div className="overflow-x-auto rounded-md border bg-zinc-50 dark:bg-zinc-900">
          <table className="w-full border-collapse font-mono text-xs">
            <caption className="sr-only">
              Line-by-line differences between {baseLabel} and {nextLabel}
            </caption>
            <tbody>
              {hunks.map((hunk, hi) => (
                <HunkRows
                  key={`${hunk.baseStart}-${hunk.nextStart}`}
                  hunk={hunk}
                  showSeparator={hi > 0}
                />
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}

function HunkRows({
  hunk,
  showSeparator,
}: {
  hunk: ReturnType<typeof toHunks>[number]
  showSeparator: boolean
}) {
  return (
    <>
      {showSeparator && (
        <tr>
          <td
            colSpan={4}
            className="border-y bg-zinc-100 px-2 py-0.5 text-[10px] text-zinc-500 dark:bg-zinc-800"
          >
            @@ −{hunk.baseStart},{hunk.baseCount} +{hunk.nextStart},{hunk.nextCount} @@
          </td>
        </tr>
      )}
      {hunk.lines.map((line, li) => {
        const tone =
          line.op === "added"
            ? "bg-emerald-500/10 text-emerald-900 dark:text-emerald-200"
            : line.op === "removed"
              ? "bg-red-500/10 text-red-900 dark:text-red-200"
              : ""
        const sign = line.op === "added" ? "+" : line.op === "removed" ? "−" : " "
        return (
          <tr key={`${hunk.baseStart}-${li}`} className={tone}>
            {/* Line numbers are decoration for a screen reader walking the SQL, and
                announcing four numbers before every line makes the diff unlistenable. */}
            <td
              aria-hidden="true"
              className="select-none border-r px-1.5 text-right align-top text-zinc-400 tabular-nums"
            >
              {line.baseLine ?? ""}
            </td>
            <td
              aria-hidden="true"
              className="select-none border-r px-1.5 text-right align-top text-zinc-400 tabular-nums"
            >
              {line.nextLine ?? ""}
            </td>
            <td className="select-none px-1 text-center align-top">
              {/* The sign carries the meaning for anyone who cannot use the colour. */}
              <span aria-hidden="true">{sign}</span>
              <span className="sr-only">
                {line.op === "added" ? "added: " : line.op === "removed" ? "removed: " : ""}
              </span>
            </td>
            <td className="w-full whitespace-pre-wrap break-all px-1.5 align-top">
              {line.text || " "}
            </td>
          </tr>
        )
      })}
    </>
  )
}
