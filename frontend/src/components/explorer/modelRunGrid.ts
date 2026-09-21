// The Graph tab's run grid: one row per drawn model or pipeline, one column per recent run
// of the model the page is about, and in each cell what that node did for that run. Kept
// free of React so every rule about which run belongs in which cell is testable.
//
// Runs are tied together only by what the history records. A run woken by a model stores
// that model's run id (upstream_run_id, migration 104), so a column is the page's model's
// run, the upstream runs it names, the upstream runs those name, and the downstream runs
// that name it. Nothing is matched by time: two runs that merely overlap are not linked.

import {
  isWaitingSkip,
  type LineageNode,
  type LineageRun,
  type ModelLineage,
  type NodeLookup,
} from "@/components/explorer/modelLineage"

/** Columns drawn: the model's newest runs. */
export const MAX_GRID_COLUMNS = 15

/**
 * One rebuild of a model and the "waiting for every upstream" rows written before it. A
 * fan-in model on policy "all" writes a waiting row each time an upstream lands until the
 * last one does, then runs; each of those rows names the upstream run that landed.
 */
export interface RunRound {
  /** The row that is the model's outcome; null while it is still waiting. */
  main: LineageRun | null
  /** Waiting rows of this round, newest first. */
  waiting: LineageRun[]
  /** Older rows of this round may be past what was loaded. */
  incomplete: boolean
}

export type RunGridCell =
  /** This node's run for the column. `alsoLinked`: other runs of it name the same upstream run. */
  | { kind: "run"; run: LineageRun; alsoLinked: number }
  /** Woken for the column's run, and still waiting on its other upstreams. */
  | { kind: "waiting"; run: LineageRun }
  /** Named as what woke the run, with no run of its own to show (a pipeline, or a run no longer recorded). */
  | { kind: "woke" }
  /** Running now, per the live check. Only in the "Now" column. */
  | { kind: "running" }
  /** No run of this node is linked to the column's run. */
  | { kind: "none" }
  /** The link may be in runs older than the ones loaded. */
  | { kind: "not_loaded" }
  /** A node the caller can't open: nothing about its runs is shown. */
  | { kind: "hidden" }
  /** Its runs could not be loaded. */
  | { kind: "unknown" }
  /** Linked to the column only through a node whose runs are hidden or unknown. */
  | { kind: "unlinked" }

export interface RunGridColumn {
  /** Null for the "Now" column. */
  round: RunRound | null
  now: boolean
}

export interface RunGridRow {
  node: LineageNode
  cells: RunGridCell[]
}

export interface RunGrid {
  /** Oldest on the left, as the duration chart on the Runs tab. */
  columns: RunGridColumn[]
  /** Upstreams farthest first, then the model, then downstreams nearest first. */
  rows: RunGridRow[]
  /** The model has more runs than the columns drawn. */
  olderRuns: boolean
}

/** Groups a model's runs, newest first, into rounds, newest first. */
export function runRounds(runs: LineageRun[], hasMore: boolean): RunRound[] {
  const rounds: RunRound[] = []
  let current: RunRound | null = null
  for (const r of runs) {
    if (isWaitingSkip(r)) {
      if (!current) {
        current = { main: null, waiting: [], incomplete: false }
        rounds.push(current)
      }
      current.waiting.push(r)
    } else {
      current = { main: r, waiting: [], incomplete: false }
      rounds.push(current)
    }
  }
  // The oldest round's earlier waiting rows can be on the next page.
  if (hasMore && rounds.length) rounds[rounds.length - 1].incomplete = true
  return rounds
}

/** The round's rows, the outcome first. */
function roundRows(round: RunRound): LineageRun[] {
  return round.main ? [round.main, ...round.waiting] : round.waiting
}

function timeOf(iso: string | undefined): number | null {
  if (!iso) return null
  const t = Date.parse(iso)
  return Number.isNaN(t) ? null : t
}

interface ModelRuns {
  runs: LineageRun[]
  hasMore: boolean
  rounds: RunRound[]
  roundOf: Map<string, RunRound>
  byRunId: Map<string, LineageRun>
  /** finished_at of the oldest run loaded. */
  oldestFinished: number | null
}

function indexRuns(lookup: NodeLookup | undefined): ModelRuns | null {
  if (!lookup || lookup.status !== "model") return null
  const hasMore = !!lookup.hasMore
  const rounds = runRounds(lookup.runs, hasMore)
  const roundOf = new Map<string, RunRound>()
  const byRunId = new Map<string, LineageRun>()
  for (const round of rounds) {
    for (const r of roundRows(round)) {
      if (!r.run_id) continue
      roundOf.set(r.run_id, round)
      byRunId.set(r.run_id, r)
    }
  }
  const oldest = lookup.runs[lookup.runs.length - 1]
  return { runs: lookup.runs, hasMore, rounds, roundOf, byRunId, oldestFinished: timeOf(oldest?.finished_at) }
}

/** What a node's own route allows the grid to say, before any link is looked at. */
function ownAnswer(node: LineageNode, lookup: NodeLookup | undefined): RunGridCell | null {
  if (lookup?.status === "denied") return { kind: "hidden" }
  if (node.kind === "model" && lookup?.status !== "model") return { kind: "unknown" }
  return null
}

type Resolved = { cell: RunGridCell; round: RunRound | null }

/** Several neighbours gave no link: the least certain of what they were decides. */
function inherit(cells: RunGridCell[]): RunGridCell {
  if (cells.some((c) => c.kind === "hidden" || c.kind === "unknown" || c.kind === "unlinked" || c.kind === "woke")) {
    return { kind: "unlinked" }
  }
  if (cells.some((c) => c.kind === "not_loaded")) return { kind: "not_loaded" }
  return { kind: "none" }
}

/**
 * Builds the grid.
 *
 * `runningKeys` is the live check's answer (liveRunningKeys): when it names a drawn model,
 * a "Now" column is added on the right. Null means there is no live answer, not nothing running.
 */
export function buildRunGrid(
  lineage: ModelLineage,
  lookups: Map<string, NodeLookup>,
  options: { runningKeys?: Set<string> | null; maxColumns?: number } = {},
): RunGrid | null {
  const root = lineage.nodes.find((n) => n.role === "root")
  if (!root) return null
  const rootRuns = indexRuns(lookups.get(root.key))
  if (!rootRuns) return null

  const maxColumns = options.maxColumns ?? MAX_GRID_COLUMNS
  const rounds = rootRuns.rounds.slice(0, maxColumns)
  const olderRuns = rootRuns.rounds.length > rounds.length || rootRuns.hasMore
  const indexed = new Map<string, ModelRuns | null>()
  for (const n of lineage.nodes) indexed.set(n.key, n.kind === "model" ? indexRuns(lookups.get(n.key)) : null)

  const byKey = new Map(lineage.nodes.map((n) => [n.key, n]))
  const parentsOf = new Map<string, string[]>()
  const childrenOf = new Map<string, string[]>()
  for (const e of lineage.edges) {
    if (!byKey.has(e.from) || !byKey.has(e.to)) continue
    parentsOf.set(e.to, [...(parentsOf.get(e.to) ?? []), e.from])
    childrenOf.set(e.from, [...(childrenOf.get(e.from) ?? []), e.to])
  }

  const columns: RunGridColumn[] = [...rounds].reverse().map((round) => ({ round, now: false }))
  const cellsByKey = new Map<string, RunGridCell[]>(lineage.nodes.map((n) => [n.key, []]))
  for (const col of columns) {
    const resolved = resolveColumn(col.round!)
    for (const n of lineage.nodes) cellsByKey.get(n.key)!.push(resolved.get(n.key)!.cell)
  }

  const running = options.runningKeys
  if (running && lineage.nodes.some((n) => running.has(n.key))) {
    columns.push({ round: null, now: true })
    for (const n of lineage.nodes) {
      const own = ownAnswer(n, lookups.get(n.key))
      cellsByKey.get(n.key)!.push(own ?? (running.has(n.key) ? { kind: "running" } : { kind: "none" }))
    }
  }

  const ups = lineage.nodes.filter((n) => n.role === "upstream").sort((a, b) => b.distance - a.distance)
  const downs = lineage.nodes.filter((n) => n.role === "downstream").sort((a, b) => a.distance - b.distance)
  const rows = [...ups, root, ...downs].map((node) => ({ node, cells: cellsByKey.get(node.key)! }))
  return { columns, rows, olderRuns }

  function resolveColumn(rootRound: RunRound): Map<string, Resolved> {
    const out = new Map<string, Resolved>()
    out.set(root!.key, {
      cell: rootRound.main ? { kind: "run", run: rootRound.main, alsoLinked: 0 } : { kind: "waiting", run: rootRound.waiting[0] },
      round: rootRound,
    })

    // Upstreams: settled from the models they wake, which are nearer the page's model.
    const upstreams = lineage.nodes.filter((n) => n.role === "upstream")
    settle(upstreams, (u) => {
      const own = ownAnswer(u, lookups.get(u.key))
      if (own?.kind === "hidden") return own
      const children = (childrenOf.get(u.key) ?? []).filter((k) => byKey.get(k)!.role !== "downstream")
      const done = children.map((k) => out.get(k))
      for (const child of done) {
        if (!child || !child.round || (child.cell.kind !== "run" && child.cell.kind !== "waiting")) continue
        const row = roundRows(child.round).find((r) => r.upstream_kind === u.kind && r.upstream_id === u.id)
        if (!row) continue
        if (u.kind === "pipeline") return { cell: { kind: "woke" }, round: null }
        if (own) return own
        const runs = indexed.get(u.key)!
        const run = row.upstream_run_id ? runs.byRunId.get(row.upstream_run_id) : undefined
        if (run) return { cell: { kind: "run", run, alsoLinked: 0 }, round: runs.roundOf.get(run.run_id!) ?? null }
        if (!row.upstream_run_id) return { cell: { kind: "woke" }, round: null }
        const started = timeOf(row.started_at)
        if (runs.hasMore && (runs.oldestFinished === null || started === null || runs.oldestFinished > started)) {
          return { kind: "not_loaded" }
        }
        return { kind: "woke" }
      }
      if (done.some((c) => !c)) return undefined
      if (own) return own
      // A child's round that runs past the loaded rows may hold the row that names this node.
      if (done.some((c) => c!.round?.incomplete && (c!.cell.kind === "run" || c!.cell.kind === "waiting"))) {
        return { kind: "not_loaded" }
      }
      return inherit(done.map((c) => c!.cell))
    })

    // Downstreams: settled from the models that wake them, once every one of those is.
    const downstreams = lineage.nodes.filter((n) => n.role === "downstream")
    settle(downstreams, (d) => {
      const own = ownAnswer(d, lookups.get(d.key))
      const parents = (parentsOf.get(d.key) ?? []).filter((k) => byKey.get(k)!.kind === "model")
      const done = parents.map((k) => out.get(k))
      if (done.some((c) => !c)) return undefined
      if (own) return own
      const runs = indexed.get(d.key)!
      const withRun = parents
        .map((k, i) => ({ id: byKey.get(k)!.id, cell: done[i]!.cell }))
        .filter((p): p is { id: string; cell: Extract<RunGridCell, { kind: "run" }> } => p.cell.kind === "run" && !!p.cell.run.run_id)

      const linked = runs.runs.filter((r) =>
        withRun.some((p) => r.upstream_kind === "model" && r.upstream_id === p.id && r.upstream_run_id === p.cell.run.run_id),
      )
      if (linked.length) {
        // Newest first, so the first is the newest run linked; each round counts once.
        const linkedRounds = new Set(linked.map((r) => runs.roundOf.get(r.run_id ?? "")).filter(Boolean))
        const first = linked[0]
        const round = first.run_id ? runs.roundOf.get(first.run_id) : undefined
        const alsoLinked = Math.max(0, linkedRounds.size - 1)
        if (!isWaitingSkip(first)) return { cell: { kind: "run", run: first, alsoLinked }, round: round ?? null }
        if (round?.main) return { cell: { kind: "run", run: round.main, alsoLinked }, round }
        return { cell: { kind: "waiting", run: first }, round: round ?? null }
      }

      const others = done.map((c) => c!.cell).filter((c) => c.kind !== "run")
      if (withRun.length) {
        // The linked row is written after the upstream run finishes, so it is older than
        // nothing loaded only when the loaded rows reach back past that.
        const tooShort = withRun.some((p) => {
          const finished = timeOf(p.cell.run.finished_at)
          return runs.hasMore && (runs.oldestFinished === null || finished === null || runs.oldestFinished > finished)
        })
        if (tooShort) return { kind: "not_loaded" }
        // It may have been rebuilt through the parent whose runs this page can't follow.
        if (others.some((c) => c.kind !== "none")) return inherit(others)
        return { kind: "none" }
      }
      return inherit(others)
    })

    return out

    /**
     * Resolves nodes in any order their neighbours allow. `decide` returns undefined to wait
     * for a neighbour; a node still waiting when nothing moves (a stored ring) is unlinked.
     */
    function settle(nodes: LineageNode[], decide: (n: LineageNode) => Resolved | RunGridCell | undefined) {
      let pending = nodes
      while (pending.length) {
        const next: LineageNode[] = []
        for (const n of pending) {
          const d = decide(n)
          if (d === undefined) next.push(n)
          else out.set(n.key, "cell" in d ? d : { cell: d, round: null })
        }
        if (next.length === pending.length) {
          for (const n of next) out.set(n.key, { cell: ownAnswer(n, lookups.get(n.key)) ?? { kind: "unlinked" }, round: null })
          return
        }
        pending = next
      }
    }
  }
}

/** The run a cell stands for, when it stands for one. */
export function cellRun(cell: RunGridCell): LineageRun | null {
  return cell.kind === "run" || cell.kind === "waiting" ? cell.run : null
}
