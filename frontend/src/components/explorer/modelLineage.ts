// The chain around one model: everything whose completion eventually wakes it, and
// everything it eventually wakes. Kept free of React so every rule about what the graph
// may draw, and may say, is testable without rendering one.
//
// There is no chain endpoint. The edges come from the workspace's schedule list, where
// each after_upstream schedule carries its own upstreams, walked up as given and walked
// down by inverting them. That list is what the caller may see of the workspace: another
// member's private model has no row in it, so it is never found as a downstream.

import dagre from "dagre"

import type { ScheduledQuery } from "@/components/explorer/scheduledModel"
import { AFTER_UPSTREAM, liveCellFor, type Fetched, type RunningModelsResponse } from "@/components/explorer/liveState"
import { executionStatusConfig, normalizeExecutionStatus } from "@/lib/execution-status"

/** The most nodes one graph draws, the model itself included. */
export const MAX_LINEAGE_NODES = 50

/** GET /explorer/schedules returns at most this many rows and does not say when it stopped. */
export const SCHEDULE_LIST_LIMIT = 500

/**
 * Runs asked for per model. The run grid draws one column per run of the model, and a
 * fan-in model writes a "waiting for every upstream" row per upstream per round, so a
 * handful of rows can be a single round.
 */
export const LINEAGE_RUNS_LIMIT = 25

/**
 * When every one of those is a row that is not an outcome, one more page this size is
 * asked for (the route's maximum). A fan-in model whose upstreams finish often writes one
 * waiting row per upstream per round, so a whole first page can be waiting rows.
 */
export const LINEAGE_OLDER_RUNS_LIMIT = 200

export type LineageKind = "model" | "pipeline"

export type LineageRole = "root" | "upstream" | "downstream"

export interface LineageNode {
  /** `${kind}:${id}`. Never rendered: for a node the caller cannot open it is an id they were not given. */
  key: string
  kind: LineageKind
  id: string
  role: LineageRole
  /** Hops from the model the page is about; 0 for the model itself. */
  distance: number
  /** The model's live schedule, when the caller can see one. */
  schedule?: ScheduledQuery
  /**
   * The name some downstream's upstream list gave this node. That join does not check
   * whether the caller may see the model it names, so it is only shown once the node's
   * own route has answered.
   */
  claimedName?: string
  /** Upstreams of this model that are not drawn, because the walk never went that way. */
  upstreamsNotDrawn: number
}

export interface LineageEdge {
  /** Keys of the upstream and the model it wakes. */
  from: string
  to: string
  /**
   * The model it points at is paused, so this upstream finishing wakes nothing. Only the
   * downstream's own schedule decides that: an upstream the caller cannot see still fires.
   */
  inactive: boolean
}

export interface ModelLineage {
  nodes: LineageNode[]
  edges: LineageEdge[]
  /** Ancestors and descendants found, before the node cap. */
  upstreamsFound: number
  downstreamsFound: number
  /** How many of those were left out by the cap. */
  omitted: number
}

export function lineageKey(kind: string, id: string): string {
  return `${kind}:${id}`
}

/**
 * The upstreams that actually wake a schedule. Only an after_upstream schedule has any:
 * the gateway clears the set when a trigger is converted back to a cadence
 * (saved_query_schedules.go, the DELETE before the insert), and a row that still carried
 * one would be drawn as a link that fires nothing.
 */
function upstreamsOf(s: ScheduledQuery | undefined): { kind: LineageKind; id: string; name?: string }[] {
  if (!s || s.schedule_type !== AFTER_UPSTREAM) return []
  return (s.upstreams ?? []).filter(
    (u): u is { kind: LineageKind; id: string; name?: string } =>
      (u.kind === "model" || u.kind === "pipeline") && typeof u.id === "string" && u.id !== "",
  )
}

interface Found {
  kind: LineageKind
  id: string
  distance: number
}

export interface BuildLineageOptions {
  /**
   * The page's own schedule for the model. The list is capped at 500 rows, so the model
   * the page is about can be missing from it while the page holds its row.
   */
  rootSchedule?: ScheduledQuery | null
  maxNodes?: number
}

/**
 * Walks both ways from `rootId` over the schedule list.
 *
 * Breadth-first, so the cap drops the farthest nodes and every drawn node's path back to
 * the model is drawn too. A visited set ends any ring: the gateway refuses one at write
 * time, but a ring through a schedule deleted since is still stored, and a walk that
 * trusted the refusal would never return.
 */
export function buildModelLineage(
  rootId: string,
  schedules: ScheduledQuery[],
  options: BuildLineageOptions = {},
): ModelLineage {
  const maxNodes = options.maxNodes ?? MAX_LINEAGE_NODES
  const rootKey = lineageKey("model", rootId)
  const byModel = new Map<string, ScheduledQuery>()
  for (const s of schedules) byModel.set(s.saved_query_id, s)
  const root = options.rootSchedule
  if (root && root.saved_query_id === rootId && !byModel.has(rootId)) byModel.set(rootId, root)

  // Upstream key → the models it wakes, and the name the first of them gave it.
  const wakes = new Map<string, string[]>()
  const claimedNames = new Map<string, string>()
  for (const s of byModel.values()) {
    for (const u of upstreamsOf(s)) {
      const key = lineageKey(u.kind, u.id)
      const list = wakes.get(key)
      if (list) list.push(s.saved_query_id)
      else wakes.set(key, [s.saved_query_id])
      if (u.name?.trim() && !claimedNames.has(key)) claimedNames.set(key, u.name.trim())
    }
  }

  const seen = new Set<string>([rootKey])

  const up: Found[] = []
  let frontier: Found[] = [{ kind: "model", id: rootId, distance: 0 }]
  while (frontier.length) {
    const next: Found[] = []
    for (const f of frontier) {
      // A pipeline never waits on anything, and a model with no visible schedule has no
      // upstreams the caller can read.
      if (f.kind !== "model") continue
      for (const u of upstreamsOf(byModel.get(f.id))) {
        const key = lineageKey(u.kind, u.id)
        if (seen.has(key)) continue
        seen.add(key)
        const found = { kind: u.kind, id: u.id, distance: f.distance + 1 }
        up.push(found)
        next.push(found)
      }
    }
    frontier = next
  }

  const down: Found[] = []
  frontier = [{ kind: "model", id: rootId, distance: 0 }]
  while (frontier.length) {
    const next: Found[] = []
    for (const f of frontier) {
      for (const id of wakes.get(lineageKey(f.kind, f.id)) ?? []) {
        const key = lineageKey("model", id)
        if (seen.has(key)) continue
        seen.add(key)
        const found = { kind: "model" as const, id, distance: f.distance + 1 }
        down.push(found)
        next.push(found)
      }
    }
    frontier = next
  }

  // Nearest first, and at the same distance upstreams and downstreams take turns, so a
  // wide fan-in cannot crowd out the models this one wakes, or the other way round. Every
  // node one hop nearer still comes first, so a drawn node's path back is always drawn.
  const turn = (list: Found[], role: "upstream" | "downstream") => {
    const atDistance = new Map<number, number>()
    return list.map((f) => {
      const order = atDistance.get(f.distance) ?? 0
      atDistance.set(f.distance, order + 1)
      return { ...f, role, order, side: role === "upstream" ? 0 : 1 }
    })
  }
  const ranked = [...turn(up, "upstream"), ...turn(down, "downstream")].sort(
    (a, b) => a.distance - b.distance || a.order - b.order || a.side - b.side,
  )
  const room = Math.max(0, maxNodes - 1)
  const kept = ranked.slice(0, room)

  const nodes: LineageNode[] = [
    { kind: "model" as const, id: rootId, role: "root" as const, distance: 0 },
    ...kept.map(({ kind, id, role, distance }) => ({ kind, id, role, distance })),
  ].map((f) => {
    const key = lineageKey(f.kind, f.id)
    return {
      key,
      kind: f.kind,
      id: f.id,
      role: f.role,
      distance: f.distance,
      schedule: f.kind === "model" ? byModel.get(f.id) : undefined,
      claimedName: claimedNames.get(key),
      upstreamsNotDrawn: 0,
    }
  })

  const drawn = new Set(nodes.map((n) => n.key))
  const edges: LineageEdge[] = []
  const edgeKeys = new Set<string>()
  for (const n of nodes) {
    for (const u of upstreamsOf(n.schedule)) {
      const from = lineageKey(u.kind, u.id)
      if (!drawn.has(from)) {
        n.upstreamsNotDrawn += 1
        continue
      }
      const edgeKey = `${from}>${n.key}`
      if (edgeKeys.has(edgeKey)) continue
      edgeKeys.add(edgeKey)
      edges.push({ from, to: n.key, inactive: n.schedule?.status !== "active" })
    }
  }

  return {
    nodes,
    edges,
    upstreamsFound: up.length,
    downstreamsFound: down.length,
    omitted: ranked.length - kept.length,
  }
}

// ---------------------------------------------------------------------------------------
// What each node's own route answered.

/** The fields of a saved_query_runs row the graph reads (GET /explorer/saved/:id/runs). */
export interface LineageRun {
  run_id?: string
  status: string
  trigger_source?: string
  started_at?: string
  finished_at?: string
  duration_ms?: number
  rows_affected?: number | null
  skip_reason?: string
  error?: string
  /** What woke the run (migration 104). The name is joined only when the caller may see it. */
  upstream_kind?: string
  upstream_id?: string
  upstream_name?: string
  /** The upstream model's own run that woke this one. */
  upstream_run_id?: string
  trigger_depth?: number
  coalesced_count?: number
}

/** The fields of GET /pipelines/:id the graph reads. */
export interface LineagePipeline {
  name?: string
  status?: string
  sync_mode?: string
  data_loading_strategy?: { mode?: string } | null
  last_execution?: {
    status?: string
    started_at?: string
    completed_at?: string | null
    error_message?: string | null
  } | null
}

export type NodeLookup =
  /** `hasMore`: the route has runs older than these. */
  | { status: "model"; runs: LineageRun[]; hasMore?: boolean }
  | { status: "pipeline"; pipeline: LineagePipeline }
  /** 403 or 404: another member's private model, another workspace's pipeline, or gone. */
  | { status: "denied" }
  /** Anything else: the node's name and status are unknown, not absent. */
  | { status: "unavailable" }

/**
 * Turns one node's HTTP answer into what the graph may say about it.
 *
 * Fails closed: only a 2xx whose body has the expected shape counts as an answer. A 500
 * or a 429 says nothing about whether the caller may see the node, so it is not allowed
 * to reveal a name the way a real answer would.
 */
export function lookupFromResponse(kind: LineageKind, httpStatus: number, body: unknown): NodeLookup {
  // GET /pipelines/:id checks access first and answers {"error":"not found"} or a 403. Its
  // own query then answers 404 "pipeline_not_found" only for a row deleted after that gate
  // let the caller in; a failed read is a 500 (pipelines.go GetPipeline). So that 404 says
  // nothing about access. Neither reading shows a name.
  if (kind === "pipeline" && httpStatus === 404 && errorCode(body) === "pipeline_not_found") {
    return { status: "unavailable" }
  }
  if (httpStatus === 403 || httpStatus === 404) return { status: "denied" }
  if (httpStatus < 200 || httpStatus >= 300) return { status: "unavailable" }
  if (!body || typeof body !== "object") return { status: "unavailable" }
  if (kind === "model") {
    const runs = (body as { runs?: unknown }).runs
    if (!Array.isArray(runs)) return { status: "unavailable" }
    return {
      status: "model",
      runs: runs.filter((r): r is LineageRun => !!r && typeof r === "object" && typeof (r as LineageRun).status === "string"),
    }
  }
  return { status: "pipeline", pipeline: body as LineagePipeline }
}

function errorCode(body: unknown): string | null {
  if (!body || typeof body !== "object") return null
  const code = (body as { error?: unknown }).error
  return typeof code === "string" ? code : null
}

/** The runs route's cursor for the page after this one, when there is one. */
export function nextRunsCursor(body: unknown): string | null {
  if (!body || typeof body !== "object") return null
  const cursor = (body as { next_cursor?: unknown }).next_cursor
  return typeof cursor === "string" && cursor !== "" ? cursor : null
}

/** Every run fetched is one that says nothing about how the model last ran. */
export function needsOlderRuns(runs: LineageRun[]): boolean {
  return runs.length > 0 && runs.every((r) => !isOutcome(r))
}

// ---------------------------------------------------------------------------------------
// What one node says.

export type LineageTone = "success" | "failed" | "warning" | "running" | "neutral"

export interface LineageNodeView {
  title: string
  /** False when `title` is a placeholder ("A model you can't open") rather than a name. */
  named: boolean
  href: string | null
  kindLabel: "Model" | "Pipeline"
  tone: LineageTone
  status: string
  /** ISO time the status refers to, when it refers to one. */
  at: string | null
  /** How long that run took, for a model run that did work. */
  durationMs: number | null
  secondary: string | null
  /** The status could not be loaded, and asking again may help. */
  retryable: boolean
}

export interface DescribeLineageContext {
  /** The page's own name for the model, which it was allowed to load. */
  rootName: string
  running: Fetched<RunningModelsResponse>
  /** The schedule list came back full, so a model with no row may still have a schedule. */
  schedulesCapped: boolean
}

const SKIP_LABEL: Record<string, string> = {
  upstream_failed: "Skipped: upstream failed",
  upstream_skipped: "Skipped: upstream skipped",
  chain_depth_exceeded: "Skipped: hop limit",
}

const WAITING_SKIP = "waiting_on_upstreams"

/** A row a fan-in model writes when one upstream lands and it still waits on the others. */
export function isWaitingSkip(r: LineageRun): boolean {
  return r.skip_reason === WAITING_SKIP
}

/**
 * The error a run gets when another run of the same model held the lock
 * (saved_query_models.go runSavedQueryModel). Matched by its words because the row has no
 * skip_reason; if the words change, such a row reads as a plain "Skipped" again.
 */
const RUN_IN_PROGRESS_ERROR = "a run of this model is already in progress"

function isRunInProgressSkip(r: LineageRun): boolean {
  return r.status === "skipped" && !r.skip_reason && r.error === RUN_IN_PROGRESS_ERROR
}

/** Whether a run is how the model last ran, rather than a note that it did not try. */
function isOutcome(r: LineageRun): boolean {
  return r.skip_reason !== WAITING_SKIP && !isRunInProgressSkip(r)
}

/** How long a run that did work took. A skip did none, so its ~0 ms is not a measurement. */
export function runDurationMs(r: LineageRun): number | null {
  if (r.status === "skipped") return null
  return typeof r.duration_ms === "number" && Number.isFinite(r.duration_ms) && r.duration_ms >= 0 ? r.duration_ms : null
}

interface Status {
  tone: LineageTone
  status: string
  at: string | null
  /** How long the run the status describes took, when it did work. */
  durationMs: number | null
}

/**
 * A model's last outcome, from its newest runs.
 *
 * Two kinds of skip are not outcomes. A "waiting for every upstream" skip is written each
 * time one of several upstreams finishes, and would otherwise paint a healthy fan-in model
 * amber after every upstream but the last. A "run already in progress" skip is written
 * while another run of the model is still going, and is newer than that run's row until it
 * ends. The newest run that is neither decides.
 */
function modelRunStatus(runs: LineageRun[]): Status & { waitingSkip: boolean } {
  const waitingSkip = runs[0]?.skip_reason === WAITING_SKIP
  const run = runs.find(isOutcome)
  if (!run) {
    if (!runs.length) return { tone: "neutral", status: "Never run", at: null, durationMs: null, waitingSkip: false }
    const newest = runs[0]
    return {
      tone: "neutral",
      status: isRunInProgressSkip(newest) ? "Skipped: a run was already in progress" : "Waiting for other upstreams",
      at: newest.finished_at || newest.started_at || null,
      durationMs: null,
      waitingSkip: false,
    }
  }
  const at = run.finished_at || run.started_at || null
  const durationMs = runDurationMs(run)
  switch (run.status) {
    case "succeeded":
      return { tone: "success", status: "Succeeded", at, durationMs, waitingSkip }
    case "failed":
      return { tone: "failed", status: "Failed", at, durationMs, waitingSkip }
    case "skipped":
      return {
        tone: "warning",
        status: skipLabel(run),
        at,
        durationMs,
        waitingSkip,
      }
    default:
      return { tone: "neutral", status: run.status, at, durationMs, waitingSkip }
  }
}

/** "Skipped: upstream failed", or plain "Skipped" for a row that gives no reason. */
export function skipLabel(r: LineageRun): string {
  if (isRunInProgressSkip(r)) return "Skipped: a run was already in progress"
  if (r.skip_reason === WAITING_SKIP) return "Waiting for other upstreams"
  return r.skip_reason ? (SKIP_LABEL[r.skip_reason] ?? `Skipped: ${r.skip_reason}`) : "Skipped"
}

function isCdcPipeline(p: LineagePipeline): boolean {
  return p.data_loading_strategy?.mode === "cdc" || p.sync_mode === "cdc"
}

function pipelineRunStatus(p: LineagePipeline): Status {
  const exec = p.last_execution
  const failed = !!exec && normalizeExecutionStatus(exec.status, exec.error_message) === "failed"
  const at = exec ? exec.completed_at || exec.started_at || null : null
  // A CDC pipeline's executions are its setup, not each change it streams, so the newest
  // one's success says little about whether changes are flowing now. Only a failure is
  // worth repeating.
  if (isCdcPipeline(p) && !failed) return { tone: "neutral", status: "CDC pipeline", at: null, durationMs: null }
  if (!exec) return { tone: "neutral", status: "Never run", at: null, durationMs: null }
  const normalized = normalizeExecutionStatus(exec.status, exec.error_message)
  switch (normalized) {
    case "success":
      return { tone: "success", status: "Succeeded", at, durationMs: null }
    case "failed":
      return { tone: "failed", status: "Failed", at, durationMs: null }
    case "running":
      return { tone: "running", status: "Running", at, durationMs: null }
    case "cancelled":
      return { tone: "neutral", status: "Cancelled", at, durationMs: null }
    case "silent_drop_detected":
    case "silent_partial_drop_detected":
    case "waiting_for_credential_reauth":
    case "waiting_for_user":
      return { tone: "warning", status: executionStatusConfig[normalized].label, at, durationMs: null }
    default: {
      // normalizeExecutionStatus reads every token it does not know as "pending".
      const raw = (exec.status || "").trim()
      if (!raw || raw.toLowerCase() === "pending") return { tone: "neutral", status: "Pending", at, durationMs: null }
      return { tone: "neutral", status: raw, at, durationMs: null }
    }
  }
}

const PIPELINE_STATE_LABEL: Record<string, string> = {
  paused: "Paused",
  stopped: "Stopped",
  draft: "Draft",
}

export function modelHref(id: string): string {
  return `/explorer/schedules/${encodeURIComponent(id)}?tab=graph`
}

/**
 * The title, link, colour and words for one node.
 *
 * A name or link is shown only for a node the caller was shown by a route that checks
 * access: its own schedule row, or its own runs or pipeline answering 2xx. Anything else
 * reads as a placeholder, so a private model's name that leaked into a downstream's
 * upstream list is never what the graph prints.
 */
export function describeLineageNode(
  node: LineageNode,
  lookup: NodeLookup | undefined,
  ctx: DescribeLineageContext,
): LineageNodeView {
  const found: NodeLookup = lookup ?? { status: "unavailable" }

  if (node.kind === "pipeline") {
    if (found.status === "denied") {
      return placeholder("A pipeline you can't open", "Pipeline", "Status hidden", false)
    }
    if (found.status !== "pipeline") {
      return placeholder("A pipeline", "Pipeline", "Status unavailable", true)
    }
    const p = found.pipeline
    const s = pipelineRunStatus(p)
    const name = typeof p.name === "string" ? p.name.trim() : ""
    return {
      title: name || "Unnamed pipeline",
      named: !!name,
      href: `/pipelines/${encodeURIComponent(node.id)}`,
      kindLabel: "Pipeline",
      ...s,
      secondary: PIPELINE_STATE_LABEL[(p.status || "").toLowerCase()] ?? null,
      retryable: false,
    }
  }

  const row = node.schedule
  const isRoot = node.role === "root"

  // Its own schedule row is the list route vouching for it; the root is the page's model;
  // otherwise only its runs answering 2xx does.
  let title: string
  let named: boolean
  if (isRoot) {
    title = ctx.rootName
    named = true
  } else if (row) {
    title = row.name?.trim() || "Unnamed model"
    named = !!row.name?.trim()
  } else if (found.status === "model") {
    title = node.claimedName || "Unnamed model"
    named = !!node.claimedName
  } else if (found.status === "denied") {
    return placeholder("A model you can't open", "Model", "Status hidden", false)
  } else {
    return placeholder("A model", "Model", "Status unavailable", true)
  }

  let s: Status
  let waitingSkip = false
  if (found.status === "model") {
    const r = modelRunStatus(found.runs)
    s = { tone: r.tone, status: r.status, at: r.at, durationMs: r.durationMs }
    waitingSkip = r.waitingSkip
  } else {
    s = { tone: "neutral", status: "Status unavailable", at: null, durationMs: null }
  }

  let liveWaiting = false
  if (row && row.schedule_type === AFTER_UPSTREAM) {
    const cell = liveCellFor(row.schedule_type, node.id, ctx.running)
    if (cell.kind === "rebuilding") s = { tone: "running", status: "Rebuilding now", at: null, durationMs: null }
    else if (cell.kind === "running_no_detail") s = { tone: "running", status: "Running", at: null, durationMs: null }
    else if (cell.kind === "waiting") liveWaiting = true
  }

  const secondary = modelSecondary(node, row, { liveWaiting, waitingSkip, schedulesCapped: ctx.schedulesCapped })

  return {
    title,
    named,
    // Past the placeholders, every non-root node here is vouched for. The root is this page.
    href: isRoot ? null : modelHref(node.id),
    kindLabel: "Model",
    ...s,
    secondary,
    retryable: found.status === "unavailable",
  }
}

/**
 * One line under the status: the schedule's state, then any upstreams not drawn. Both are
 * kept, because a paused model that also has an upstream off the page must still say it is
 * paused, and the list has no dashed edge to say it instead.
 */
function modelSecondary(
  node: LineageNode,
  row: ScheduledQuery | undefined,
  flags: { liveWaiting: boolean; waitingSkip: boolean; schedulesCapped: boolean },
): string | null {
  const parts = [scheduleState(node, row, flags)]
  // A fan-in model with an upstream off the page would otherwise read as waiting only on
  // what is drawn.
  if (node.upstreamsNotDrawn > 0) {
    parts.push(`+${node.upstreamsNotDrawn} more upstream${node.upstreamsNotDrawn === 1 ? "" : "s"} not shown`)
  }
  const text = parts.filter((p): p is string => !!p).join(" · ")
  return text || null
}

function scheduleState(
  node: LineageNode,
  row: ScheduledQuery | undefined,
  flags: { liveWaiting: boolean; waitingSkip: boolean; schedulesCapped: boolean },
): string | null {
  if (!row) {
    // The root's row came from the page, which asked for it by id; any other model's may
    // just be past the list's cap.
    if (flags.schedulesCapped && node.role !== "root") return "Schedule not loaded (500+ schedules)"
    return "Not scheduled"
  }
  if (row.status !== "active") {
    if (row.status === "paused") return row.auto_paused_reason ? "Paused automatically" : "Paused"
    return row.status
  }
  if (flags.liveWaiting) return "Waiting on upstreams"
  if (flags.waitingSkip) return "Waiting for other upstreams"
  const count = upstreamsOf(row).length
  if (row.upstream_policy === "all" && count > 1) return `Waits on all ${count}`
  return null
}

/**
 * The drawn models the live check says are running right now, which the graph shows as
 * running instead of their last run. Null when there is no live answer to read, which is
 * not the same as nothing running.
 */
export function liveRunningKeys(nodes: LineageNode[], running: Fetched<RunningModelsResponse>): Set<string> | null {
  if (running.status !== "ok") return null
  const keys = new Set<string>()
  for (const n of nodes) {
    if (n.kind !== "model" || n.schedule?.schedule_type !== AFTER_UPSTREAM) continue
    const cell = liveCellFor(AFTER_UPSTREAM, n.id, running)
    if (cell.kind === "rebuilding" || cell.kind === "running_no_detail") keys.add(n.key)
  }
  return keys
}

function placeholder(
  title: string,
  kindLabel: "Model" | "Pipeline",
  status: string,
  retryable: boolean,
): LineageNodeView {
  return {
    title,
    named: false,
    href: null,
    kindLabel,
    tone: "neutral",
    status,
    at: null,
    durationMs: null,
    secondary: null,
    retryable,
  }
}

// ---------------------------------------------------------------------------------------
// Where each node goes.

export const LINEAGE_NODE_WIDTH = 216
export const LINEAGE_NODE_HEIGHT = 76

export interface Rect {
  x: number
  y: number
  width: number
  height: number
}

/** Upstreams on the left, downstreams on the right. Positions are top-left corners. */
export function layoutLineage(
  nodeKeys: string[],
  edges: { from: string; to: string }[],
): { positions: Map<string, { x: number; y: number }>; bounds: Rect } {
  const g = new dagre.graphlib.Graph()
  g.setGraph({ rankdir: "LR", nodesep: 20, ranksep: 56, marginx: 0, marginy: 0 })
  g.setDefaultEdgeLabel(() => ({}))
  for (const key of nodeKeys) g.setNode(key, { width: LINEAGE_NODE_WIDTH, height: LINEAGE_NODE_HEIGHT })
  for (const e of edges) g.setEdge(e.from, e.to)
  dagre.layout(g)

  const positions = new Map<string, { x: number; y: number }>()
  let minX = Infinity
  let minY = Infinity
  let maxX = -Infinity
  let maxY = -Infinity
  for (const key of nodeKeys) {
    const n = g.node(key) as { x: number; y: number }
    const x = n.x - LINEAGE_NODE_WIDTH / 2
    const y = n.y - LINEAGE_NODE_HEIGHT / 2
    positions.set(key, { x, y })
    minX = Math.min(minX, x)
    minY = Math.min(minY, y)
    maxX = Math.max(maxX, x + LINEAGE_NODE_WIDTH)
    maxY = Math.max(maxY, y + LINEAGE_NODE_HEIGHT)
  }
  const bounds = nodeKeys.length
    ? { x: minX, y: minY, width: maxX - minX, height: maxY - minY }
    : { x: 0, y: 0, width: 0, height: 0 }
  return { positions, bounds }
}

/** Below this zoom a node's words are too small to read, so the model is shown instead of all of it. */
export const MIN_READABLE_FIT_ZOOM = 0.75

const VIEWPORT_PADDING = 24

/**
 * The first view: the whole chain when it fits at a readable size, otherwise the model
 * itself at full size, with the rest a pan away.
 */
export function initialViewport(
  bounds: Rect,
  root: Rect,
  size: { width: number; height: number },
): { x: number; y: number; zoom: number } {
  if (!(size.width > 0) || !(size.height > 0) || !(bounds.width > 0) || !(bounds.height > 0)) {
    return { x: 0, y: 0, zoom: 1 }
  }
  const fit = Math.min(
    (size.width - 2 * VIEWPORT_PADDING) / bounds.width,
    (size.height - 2 * VIEWPORT_PADDING) / bounds.height,
    1,
  )
  if (fit >= MIN_READABLE_FIT_ZOOM) {
    return {
      x: (size.width - bounds.width * fit) / 2 - bounds.x * fit,
      y: (size.height - bounds.height * fit) / 2 - bounds.y * fit,
      zoom: fit,
    }
  }
  return {
    x: size.width / 2 - (root.x + root.width / 2),
    y: size.height / 2 - (root.y + root.height / 2),
    zoom: 1,
  }
}
