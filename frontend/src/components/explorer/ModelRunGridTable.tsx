"use client"

// The Graph tab's run grid, drawn: each column one run of the page's model with its
// duration as a bar on top, each row a drawn model or pipeline, and in each cell what that
// node did for the run. Which run belongs in which cell is decided in modelRunGrid.ts.

import { Check, Minus, X } from "lucide-react"

import { cn } from "@/lib/utils"
import { formatAbsoluteTime } from "@/components/explorer/scheduleTime"
import { formatDuration } from "@/components/explorer/scheduledModel"
import { barHeightPercent, longestRunMs } from "@/components/explorer/RunDurationChart"
import { isWaitingSkip, runDurationMs, skipLabel, type LineageNodeView, type LineageRun } from "@/components/explorer/modelLineage"
import { cellRun, type RunGrid, type RunGridCell, type RunGridColumn } from "@/components/explorer/modelRunGrid"
import type { RunSelection } from "@/components/explorer/ModelRunPanel"

const BAR_CLASS: Record<string, string> = {
  succeeded: "bg-emerald-500",
  failed: "bg-red-500",
  skipped: "bg-amber-400",
}

const CELL_BASE = "flex h-5 w-5 items-center justify-center rounded-sm"

function runCellClass(r: LineageRun): string {
  if (isWaitingSkip(r)) return "border-2 border-amber-400 bg-transparent text-amber-600"
  return cn(BAR_CLASS[r.status] ?? "bg-zinc-400", "text-white")
}

function RunGlyph({ run }: { run: LineageRun }) {
  // Shapes as well as colours, so the grid does not depend on telling red from green.
  if (isWaitingSkip(run)) return null
  if (run.status === "succeeded") return <Check className="h-3 w-3" strokeWidth={3} aria-hidden />
  if (run.status === "failed") return <X className="h-3 w-3" strokeWidth={3} aria-hidden />
  if (run.status === "skipped") return <Minus className="h-3 w-3" strokeWidth={3} aria-hidden />
  return null
}

/** Words for a run, used for a cell's label and title. */
export function describeGridRun(r: LineageRun): string {
  const parts: string[] = []
  if (r.status === "skipped") parts.push(skipLabel(r))
  else {
    const took = runDurationMs(r)
    parts.push(took === null ? r.status : `${r.status} in ${formatDuration(took)}`)
  }
  if (r.started_at && !Number.isNaN(Date.parse(r.started_at))) parts.push(`started ${formatAbsoluteTime(r.started_at)}`)
  return parts.join(", ")
}

const CELL_TEXT: Record<Exclude<RunGridCell["kind"], "run" | "waiting">, string> = {
  woke: "Woke this run; no run of its own is shown",
  running: "Running now",
  none: "No run linked to this one",
  not_loaded: "Older than the runs loaded",
  hidden: "Runs hidden: you can't open it",
  unknown: "Runs could not be loaded",
  unlinked: "Not known: linked only through a model whose runs are hidden or not loaded",
}

const PLAIN_CELL: Record<Exclude<RunGridCell["kind"], "run" | "waiting">, { className: string; glyph: string }> = {
  woke: { className: "rounded-full border-2 border-zinc-400 dark:border-zinc-500", glyph: "" },
  running: { className: "bg-blue-500 motion-safe:animate-pulse", glyph: "" },
  none: { className: "border border-dashed border-zinc-300 dark:border-zinc-600", glyph: "" },
  not_loaded: { className: "bg-zinc-100 text-zinc-400 dark:bg-zinc-800", glyph: "…" },
  hidden: { className: "bg-zinc-200 text-zinc-500 dark:bg-zinc-700 dark:text-zinc-400", glyph: "" },
  unknown: { className: "bg-zinc-100 text-zinc-500 dark:text-zinc-400 dark:bg-zinc-800", glyph: "?" },
  unlinked: { className: "bg-zinc-100 text-zinc-400 dark:bg-zinc-800", glyph: "–" },
}

function columnLabel(col: RunGridColumn): string {
  if (col.now) return "Now"
  const r = col.round?.main ?? col.round?.waiting[0]
  return r?.started_at && !Number.isNaN(Date.parse(r.started_at)) ? `Run started ${formatAbsoluteTime(r.started_at)}` : "Run"
}

export function ModelRunGridTable({
  grid,
  views,
  clickableKeys,
  selected,
  onOpen,
}: {
  grid: RunGrid
  views: Map<string, LineageNodeView>
  clickableKeys: Set<string>
  selected: { key: string; selection: RunSelection } | null
  onOpen: (key: string, selection: RunSelection) => void
}) {
  const mains = grid.columns.map((c) => c.round?.main ?? null)
  const longestMs = longestRunMs(
    mains.filter((r): r is LineageRun => !!r).map((r) => ({ status: r.status, duration_ms: r.duration_ms ?? 0 })),
  )
  const first = grid.columns.find((c) => !c.now)
  const last = [...grid.columns].reverse().find((c) => !c.now)
  const timeOf = (c: RunGridColumn | undefined) => {
    const r = c?.round?.main ?? c?.round?.waiting[0]
    return r?.started_at && !Number.isNaN(Date.parse(r.started_at)) ? formatAbsoluteTime(r.started_at) : null
  }
  const kinds = new Set(grid.rows.flatMap((row) => row.cells.map((c) => c.kind)))

  return (
    <section className="border-t px-4 py-3 dark:border-zinc-800" aria-labelledby="model-run-grid-heading">
      <h3 id="model-run-grid-heading" className="text-xs font-medium uppercase tracking-wide text-zinc-500 dark:text-zinc-400">
        Runs across the chain
      </h3>
      <p className="mb-2 text-xs text-zinc-500 dark:text-zinc-400">
        Each column is one run of this model, oldest on the left, with the runs it was woken by and the runs it woke. Click a
        square for that run and its SQL.
      </p>
      <div className="overflow-x-auto">
        <table className="border-separate border-spacing-x-1 border-spacing-y-0.5 text-xs">
          <caption className="sr-only">
            Recent runs of this model and the linked runs of each upstream and downstream
          </caption>
          <thead>
            <tr>
              <td />
              {grid.columns.map((col, i) => {
                const main = col.round?.main
                return (
                  <th key={i} scope="col" className="p-0 align-bottom font-normal">
                    <span className="sr-only">{columnLabel(col)}</span>
                    <div className="flex h-10 w-5 items-end justify-center" aria-hidden title={main ? describeGridRun(main) : columnLabel(col)}>
                      {col.now ? (
                        <div className="w-3 rounded-t-sm bg-blue-500 motion-safe:animate-pulse" style={{ height: "30%" }} />
                      ) : main ? (
                        <div
                          className={cn("w-3 rounded-t-sm", BAR_CLASS[main.status] ?? "bg-zinc-400")}
                          style={{ height: `${barHeightPercent({ status: main.status, duration_ms: main.duration_ms ?? 0 }, longestMs)}%` }}
                        />
                      ) : (
                        <div className="h-[8%] w-3 rounded-t-sm border border-dashed border-amber-400" />
                      )}
                    </div>
                  </th>
                )
              })}
            </tr>
          </thead>
          <tbody>
            {grid.rows.map((row) => {
              const v = views.get(row.node.key)!
              const clickable = clickableKeys.has(row.node.key)
              const isRoot = row.node.role === "root"
              return (
                <tr key={row.node.key}>
                  <th scope="row" className="max-w-[14rem] pr-2 text-left font-normal">
                    <span className="flex min-w-0 items-baseline gap-1.5">
                      <span className="shrink-0 text-[10px] uppercase tracking-wide text-zinc-400">{v.kindLabel}</span>
                      <span
                        title={v.title}
                        className={cn(
                          "truncate",
                          isRoot && "font-semibold",
                          v.named ? "text-zinc-900 dark:text-white" : "italic text-zinc-500 dark:text-zinc-400",
                        )}
                      >
                        {v.title}
                      </span>
                    </span>
                  </th>
                  {row.cells.map((cell, i) => (
                    <td key={i} className="p-0">
                      <GridCell
                        cell={cell}
                        title={v.title}
                        clickable={clickable}
                        selected={isSelected(selected, row.node.key, cell)}
                        onOpen={(selection) => onOpen(row.node.key, selection)}
                      />
                    </td>
                  ))}
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
      {(timeOf(first) || grid.olderRuns) && (
        <p className="mt-1 text-[11px] text-zinc-500 dark:text-zinc-400">
          {timeOf(first) && timeOf(last) && `${timeOf(first)} → ${timeOf(last)}`}
          {grid.olderRuns && " · Older runs are on the Runs tab."}
        </p>
      )}
      <GridLegend kinds={kinds} />
    </section>
  )
}

function isSelected(selected: { key: string; selection: RunSelection } | null, key: string, cell: RunGridCell): boolean {
  if (!selected || selected.key !== key || !selected.selection) return false
  if (selected.selection.kind === "now") return cell.kind === "running"
  const run = cellRun(cell)
  return !!run?.run_id && run.run_id === selected.selection.runId
}

function GridCell({
  cell,
  title,
  clickable,
  selected,
  onOpen,
}: {
  cell: RunGridCell
  title: string
  clickable: boolean
  selected: boolean
  onOpen: (selection: RunSelection) => void
}) {
  const run = cellRun(cell)
  if (run) {
    let text = describeGridRun(run)
    if (cell.kind === "run" && cell.alsoLinked > 0) {
      text += `; ${cell.alsoLinked} more run${cell.alsoLinked === 1 ? "" : "s"} of it came from the same upstream run`
    }
    const inner = (
      <span className={cn(CELL_BASE, runCellClass(run))}>
        <RunGlyph run={run} />
      </span>
    )
    if (!clickable || !run.run_id) {
      return (
        <span role="img" aria-label={`${title}: ${text}`} title={text} className="block">
          {inner}
        </span>
      )
    }
    return (
      <button
        type="button"
        aria-label={`${title}: ${text}. Open the run and its SQL`}
        title={text}
        onClick={() => onOpen({ kind: "run", runId: run.run_id! })}
        className={cn(
          "block rounded-sm outline-none ring-offset-1 hover:ring-2 hover:ring-zinc-400 focus-visible:ring-2 focus-visible:ring-zinc-900 dark:ring-offset-zinc-900 dark:focus-visible:ring-white",
          selected && "ring-2 ring-zinc-900 dark:ring-white",
        )}
      >
        {inner}
      </button>
    )
  }

  const kind = cell.kind as keyof typeof PLAIN_CELL
  const plain = PLAIN_CELL[kind]
  const text = CELL_TEXT[kind]
  const inner = <span className={cn(CELL_BASE, plain.className)}>{plain.glyph}</span>
  if (kind === "running" && clickable) {
    return (
      <button
        type="button"
        aria-label={`${title}: running now. Open it and its SQL`}
        title={text}
        onClick={() => onOpen({ kind: "now" })}
        className={cn(
          "block rounded-sm outline-none ring-offset-1 hover:ring-2 hover:ring-zinc-400 focus-visible:ring-2 focus-visible:ring-zinc-900 dark:ring-offset-zinc-900 dark:focus-visible:ring-white",
          selected && "ring-2 ring-zinc-900 dark:ring-white",
        )}
      >
        {inner}
      </button>
    )
  }
  return (
    <span role="img" aria-label={`${title}: ${text}`} title={text} className="block">
      {inner}
    </span>
  )
}

function GridLegend({ kinds }: { kinds: Set<RunGridCell["kind"]> }) {
  const items: { key: string; swatch: React.ReactNode; label: string }[] = [
    { key: "succeeded", swatch: <span className={cn(CELL_BASE, "h-3 w-3 bg-emerald-500")} />, label: "Succeeded" },
    { key: "failed", swatch: <span className={cn(CELL_BASE, "h-3 w-3 bg-red-500")} />, label: "Failed" },
    { key: "skipped", swatch: <span className={cn(CELL_BASE, "h-3 w-3 bg-amber-400")} />, label: "Skipped" },
    {
      key: "waiting",
      swatch: <span className={cn(CELL_BASE, "h-3 w-3 border-2 border-amber-400")} />,
      label: "Waiting for other upstreams",
    },
  ]
  for (const k of ["running", "woke", "none", "not_loaded", "hidden", "unknown", "unlinked"] as const) {
    if (!kinds.has(k)) continue
    items.push({
      key: k,
      swatch: <span className={cn(CELL_BASE, "h-3 w-3 text-[9px]", PLAIN_CELL[k].className)}>{PLAIN_CELL[k].glyph}</span>,
      label: CELL_TEXT[k],
    })
  }
  return (
    <ul className="mt-2 flex flex-wrap gap-x-4 gap-y-1 text-[11px] text-zinc-500 dark:text-zinc-400" aria-label="What the squares mean">
      {items.map((it) => (
        <li key={it.key} className="inline-flex items-center gap-1.5">
          <span aria-hidden>{it.swatch}</span>
          {it.label}
        </li>
      ))}
    </ul>
  )
}
