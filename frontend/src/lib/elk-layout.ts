// elkjs is used under EPL-2.0; its licence ships as public/third-party/elkjs-LICENSE.txt.
import ELKConstructor, { type ELK, type ElkNode } from "elkjs/lib/elk-api.js"

// Left-to-right graph layout for the lineage canvases, on ELK's layered (Sugiyama)
// algorithm. The ~1.6 MB engine runs in a Web Worker, so neither its download nor a
// layout blocks the page. Where there is no worker (jsdom, a browser that refuses one),
// the same engine runs on the main thread, loaded only when first needed.

export interface LayoutBox {
  id: string
  width: number
  height: number
}

export interface LayoutLink {
  from: string
  to: string
}

export interface Rect {
  x: number
  y: number
  width: number
  height: number
}

/** Positions are top-left corners; bounds start at the origin. */
export interface GraphLayout {
  positions: Map<string, { x: number; y: number }>
  bounds: Rect
}

export interface GraphLayoutOptions {
  /** Horizontal gap between columns. */
  layerGap?: number
  /** Vertical gap between boxes in a column. */
  nodeGap?: number
}

/**
 * Lays out `boxes` in columns, each link pointing right. Links to an id that is not a
 * box, self-links and repeated links are ignored; a repeated box id keeps its first size.
 */
export async function layoutGraph(
  boxes: LayoutBox[],
  links: LayoutLink[],
  { layerGap = 80, nodeGap = 24 }: GraphLayoutOptions = {},
): Promise<GraphLayout> {
  const index = new Map<string, number>()
  const children: ElkNode[] = []
  for (const box of boxes) {
    if (index.has(box.id)) continue
    // ELK ids are ours, not the caller's: a caller's id may be anything, "graph" included.
    index.set(box.id, children.length)
    children.push({ id: `n${children.length}`, width: box.width, height: box.height })
  }
  if (children.length === 0) return { positions: new Map(), bounds: { x: 0, y: 0, width: 0, height: 0 } }

  const seen = new Set<string>()
  const edges: { id: string; sources: string[]; targets: string[] }[] = []
  for (const link of links) {
    const from = index.get(link.from)
    const to = index.get(link.to)
    if (from === undefined || to === undefined || from === to) continue
    const key = `${from}>${to}`
    if (seen.has(key)) continue
    seen.add(key)
    edges.push({ id: `e${edges.length}`, sources: [`n${from}`], targets: [`n${to}`] })
  }

  const laidOut = await runLayout({
    id: "graph",
    layoutOptions: {
      "elk.algorithm": "layered",
      "elk.direction": "RIGHT",
      "elk.padding": "[top=0,left=0,bottom=0,right=0]",
      "elk.spacing.nodeNode": String(nodeGap),
      "elk.spacing.componentComponent": String(nodeGap * 2),
      "elk.layered.spacing.nodeNodeBetweenLayers": String(layerGap),
      "elk.layered.nodePlacement.strategy": "BRANDES_KOEPF",
      "elk.layered.nodePlacement.bk.fixedAlignment": "BALANCED",
      // Keep the caller's order where it costs no crossings, so the same graph lays out
      // the same way every time.
      "elk.layered.considerModelOrder.strategy": "NODES_AND_EDGES",
    },
    children,
    edges,
  })

  const ids = [...index.keys()]
  const placed = (laidOut.children ?? []).map((child) => ({
    id: ids[Number(child.id.slice(1))],
    x: child.x ?? 0,
    y: child.y ?? 0,
    width: child.width ?? 0,
    height: child.height ?? 0,
  }))
  const minX = Math.min(...placed.map((p) => p.x))
  const minY = Math.min(...placed.map((p) => p.y))
  const positions = new Map<string, { x: number; y: number }>()
  let width = 0
  let height = 0
  for (const p of placed) {
    const x = p.x - minX
    const y = p.y - minY
    positions.set(p.id, { x, y })
    width = Math.max(width, x + p.width)
    height = Math.max(height, y + p.height)
  }
  return { positions, bounds: { x: 0, y: 0, width, height } }
}

/** The worker could not start or died; its pending layouts will never answer. */
class WorkerUnavailable extends Error {}

interface WorkerEngine {
  elk: ELK
  /** Rejects with WorkerUnavailable once the worker fails. Never resolves. */
  failed: Promise<never>
}

let workerEngine: WorkerEngine | null = null
let workerBroken = false
let mainThreadEngine: Promise<ELK> | null = null

async function runLayout(graph: ElkNode): Promise<ElkNode> {
  const engine = typeof Worker === "undefined" || workerBroken ? null : startWorker()
  if (engine) {
    try {
      return await Promise.race([engine.elk.layout(graph), engine.failed])
    } catch (err) {
      if (!(err instanceof WorkerUnavailable)) throw err
      workerBroken = true
      engine.elk.terminateWorker()
      workerEngine = null
    }
  }
  mainThreadEngine ??= import("./elk-layout.fallback").then(({ default: ElkOnMainThread }) => new ElkOnMainThread())
  return (await mainThreadEngine).layout(graph)
}

function startWorker(): WorkerEngine | null {
  if (workerEngine) return workerEngine
  let fail: (reason: WorkerUnavailable) => void = () => {}
  const failed = new Promise<never>((_, reject) => {
    fail = reject
  })
  // Nobody may be waiting when the worker dies.
  failed.catch(() => {})
  try {
    const elk = new ELKConstructor({
      workerFactory: () => {
        const worker = new Worker(new URL("./elk-layout.worker.ts", import.meta.url))
        // elk-api only listens for messages: a worker whose script fails to load or throws
        // would leave every layout waiting forever.
        worker.addEventListener("error", (event) => {
          event.preventDefault()
          fail(new WorkerUnavailable(event.message || "layout worker failed"))
        })
        return worker
      },
    })
    workerEngine = { elk, failed }
    return workerEngine
  } catch {
    // `new Worker` itself refused (a policy, an unsupported URL).
    workerBroken = true
    return null
  }
}
