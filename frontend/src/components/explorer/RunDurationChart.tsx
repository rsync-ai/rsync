"use client"

import { useState } from "react"
import { formatAbsoluteTime } from "@/components/explorer/scheduleTime"
import { formatDuration, type SavedQueryRun } from "@/components/explorer/scheduledModel"

// The runs on the page as bars, oldest on the left. A model that has started taking three
// times as long, or fails every other night, shows as a shape before anyone reads a row.
// It is drawn from exactly the runs the table under it lists, same filter and same page,
// so the two cannot disagree about which runs they mean.

/** The green, red and amber the status badges use. */
const BAR_CLASS: Record<string, string> = {
  succeeded: "bg-emerald-500",
  failed: "bg-red-500",
  skipped: "bg-amber-400",
}
// saved_query_runs allows only those three, but a status from a newer gateway is drawn
// grey rather than dropped: a missing bar would read as a run that never happened.
const UNKNOWN_BAR_CLASS = "bg-zinc-400"

/**
 * A skipped run did no work, so its duration (about 0 ms, and -1 ms under clock jitter)
 * is not a measurement. It gets a fixed stub instead: a streak of skips stays visible and
 * cannot pass for a run of very fast rebuilds.
 */
export const SKIPPED_BAR_PERCENT = 8

/** The floor for a run that did work, so 5 ms next to 4 minutes is still a bar. */
export const MIN_BAR_PERCENT = 3

function measuredMs(r: Pick<SavedQueryRun, "duration_ms">): number {
  return Number.isFinite(r.duration_ms) ? Math.max(0, r.duration_ms) : 0
}

/** The longest run on the page that did work; skips are not measurements. */
export function longestRunMs(runs: Pick<SavedQueryRun, "status" | "duration_ms">[]): number {
  return runs.reduce((max, r) => (r.status === "skipped" ? max : Math.max(max, measuredMs(r))), 0)
}

export function barHeightPercent(r: Pick<SavedQueryRun, "status" | "duration_ms">, longestMs: number): number {
  if (r.status === "skipped") return SKIPPED_BAR_PERCENT
  if (longestMs <= 0) return MIN_BAR_PERCENT
  return Math.max(MIN_BAR_PERCENT, (measuredMs(r) / longestMs) * 100)
}

function describeBar(r: SavedQueryRun): string {
  const when = formatAbsoluteTime(r.started_at)
  return r.status === "skipped" ? `${when}: skipped` : `${when}: ${r.status} in ${formatDuration(measuredMs(r))}`
}

export function RunDurationChart({ runs, className = "" }: { runs: SavedQueryRun[]; className?: string }) {
  const [hovered, setHovered] = useState<string | null>(null)
  if (runs.length === 0) return null

  // The route sends newest first; a time axis reads left to right.
  const ordered = [...runs].reverse()
  const longestMs = longestRunMs(ordered)
  const oldest = ordered[0]
  const newest = ordered[ordered.length - 1]
  const hoveredRun = ordered.find((r) => r.run_id === hovered)

  return (
    <figure className={`border-b px-4 py-3 dark:border-zinc-800 ${className}`}>
      <figcaption className="mb-2 flex flex-wrap items-center justify-between gap-x-4 gap-y-1 text-xs">
        <span>
          <span className="font-medium text-zinc-700 dark:text-zinc-300">Duration</span>
          {longestMs > 0 && <span className="text-zinc-500 dark:text-zinc-400"> · longest {formatDuration(longestMs)}</span>}
        </span>
        <span className="flex flex-wrap gap-3 text-zinc-500 dark:text-zinc-400">
          {(["succeeded", "failed", "skipped"] as const).map((status) => (
            <span key={status} className="inline-flex items-center gap-1">
              <span className={`h-2 w-2 rounded-sm ${BAR_CLASS[status]}`} aria-hidden />
              {status}
            </span>
          ))}
        </span>
      </figcaption>

      <div
        role="list"
        aria-label="Run durations, oldest first"
        className="flex h-20 items-end gap-0.5 border-b border-zinc-200 dark:border-zinc-700"
      >
        {ordered.map((r) => (
          <div
            key={r.run_id}
            role="listitem"
            aria-label={describeBar(r)}
            title={describeBar(r)}
            data-status={r.status}
            // The whole column is the hover target, so a 3% bar is as easy to point at as a tall one.
            className="flex h-full min-w-0 max-w-[24px] flex-1 items-end"
            onMouseEnter={() => setHovered(r.run_id)}
            onMouseLeave={() => setHovered((h) => (h === r.run_id ? null : h))}
          >
            <div
              data-testid="run-bar"
              className={`w-full rounded-t-sm ${BAR_CLASS[r.status] ?? UNKNOWN_BAR_CLASS} ${
                hovered && hovered !== r.run_id ? "opacity-50" : ""
              }`}
              style={{ height: `${barHeightPercent(r, longestMs)}%` }}
            />
          </div>
        ))}
      </div>

      <div className="mt-1 flex flex-wrap justify-between gap-x-4 text-[11px] text-zinc-500 dark:text-zinc-400">
        <span>
          {formatAbsoluteTime(oldest.started_at)}
          {ordered.length > 1 && ` – ${formatAbsoluteTime(newest.started_at)}`}
        </span>
        {hoveredRun && <span className="text-zinc-700 dark:text-zinc-300">{describeBar(hoveredRun)}</span>}
      </div>
    </figure>
  )
}
