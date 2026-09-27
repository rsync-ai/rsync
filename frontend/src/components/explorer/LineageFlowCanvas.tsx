"use client"

// What the two lineage drawings share: the workspace Lineage page (AssetLineageView) and a
// model's Graph tab (ModelLineageGraph). Each builds its own cards and edges; this file
// frames them — sizing, the first view, the controls, moving cards, full screen and the
// overview map.

import { useLayoutEffect, useMemo, useRef, useState, useSyncExternalStore, type ReactNode } from "react"
import { createPortal } from "react-dom"
import { useTheme } from "next-themes"
import {
  Handle,
  MiniMap,
  Position,
  ReactFlow,
  ReactFlowProvider,
  useReactFlow,
  type Edge,
  type EdgeTypes,
  type Node,
  type NodeChange,
  type NodeTypes,
  type ReactFlowInstance,
  type XYPosition,
} from "@xyflow/react"
import { Expand, Loader2, Lock, LockOpen, Minus, Plus, RotateCcw } from "lucide-react"

import { Button } from "@/components/ui/button"
import { Dialog, DialogContent, DialogTitle } from "@/components/ui/dialog"
import { MIN_READABLE_FIT_ZOOM, fitZoom, initialViewport, type Rect } from "@/components/explorer/modelLineage"

/** A canvas needs a pointer to pan and room to show more than one node; anything else gets the list. */
const CANVAS_MEDIA_QUERY = "(min-width: 640px) and (pointer: fine)"

function subscribeCanvasMedia(onChange: () => void): () => void {
  if (typeof window === "undefined" || typeof window.matchMedia !== "function") return () => {}
  const mq = window.matchMedia(CANVAS_MEDIA_QUERY)
  mq.addEventListener?.("change", onChange)
  return () => mq.removeEventListener?.("change", onChange)
}

function canvasMediaMatches(): boolean {
  return typeof window !== "undefined" && typeof window.matchMedia === "function" && window.matchMedia(CANVAS_MEDIA_QUERY).matches
}

/** Whether this screen gets the drawing; a phone or a touch-only screen gets the list. */
export function useCanvasCapable(): boolean {
  return useSyncExternalStore(subscribeCanvasMedia, canvasMediaMatches, () => false)
}

export function GraphListToggle({
  label,
  value,
  onChange,
}: {
  label: string
  value: "graph" | "list"
  onChange: (v: "graph" | "list") => void
}) {
  return (
    <div role="group" aria-label={label} className="flex gap-1">
      {(["graph", "list"] as const).map((v) => (
        <Button
          key={v}
          size="sm"
          variant={value === v ? "secondary" : "ghost"}
          className="h-7 px-2 text-xs"
          aria-pressed={value === v}
          onClick={() => onChange(v)}
        >
          {v === "graph" ? "Graph" : "List"}
        </Button>
      ))}
    </div>
  )
}

const HIDDEN_HANDLE_STYLE = { opacity: 0, pointerEvents: "none" as const }

/**
 * The ends edges attach to: one on each side of a card, never drawn, `top` px down it.
 * A card that grows (a table's columns) keeps its edges on its header, not its middle.
 */
export function CardHandles({ top }: { top: number }) {
  const style = { ...HIDDEN_HANDLE_STYLE, top }
  return (
    <>
      <Handle type="target" position={Position.Left} isConnectable={false} style={style} />
      <Handle type="source" position={Position.Right} isConnectable={false} style={style} />
    </>
  )
}

/** Where `CardHandles` sit, given up front so edges draw on the first paint, before cards are measured. */
export function nodeHandles(width: number, top: number) {
  return [
    { type: "target" as const, position: Position.Left, x: 0, y: top, width: 1, height: 1 },
    { type: "source" as const, position: Position.Right, x: width - 1, y: top, width: 1, height: 1 },
  ]
}

/** The colours edges are drawn in: a neutral line, and one that stands out. */
export function useEdgeColors(): { stroke: string; accent: string } {
  const dark = useTheme().resolvedTheme === "dark"
  return { stroke: dark ? "#a1a1aa" : "#71717a", accent: dark ? "#60a5fa" : "#2563eb" }
}

export function LegendLine({ dash, color = "currentColor", label }: { dash?: string; color?: string; label: string }) {
  return (
    <span className="inline-flex items-center gap-1.5">
      <svg width="20" height="6" aria-hidden>
        <line x1="0" y1="3" x2="20" y2="3" stroke={color} strokeWidth="1.5" strokeDasharray={dash} />
      </svg>
      {label}
    </span>
  )
}

export interface LineageFlowCanvasProps<N extends Node> {
  nodes: N[]
  edges: Edge[]
  nodeTypes: NodeTypes
  edgeTypes?: EdgeTypes
  /** The drawing's extent, and the card the first view centres on when the whole drawing is too big to read. */
  bounds: Rect
  root: Rect | null
  /** Changes only when what is drawn changes, so a data refresh keeps the reader's pan. */
  signature: string
  /**
   * Names the layout drawn, when a new one can land without `signature` changing (a card
   * opened its next hop). Cards the reader moved belong to one layout. Defaults to `signature`.
   */
  layoutKey?: string
  onNodeClick: (flowId: string) => void
  /** For screen readers, which the drawing is hidden from: where the same content is. */
  description: string
  /** Names the full-screen view. */
  title: string
  legend: ReactNode
  minimapColor?: (node: N) => string
  /** A newer layout is on its way; what is drawn is the previous one. */
  pending?: boolean
  /**
   * The card the reader just acted on. When the drawing moves under it (it opened its
   * columns) the view moves with it, so it stays where the reader's pointer is. When it
   * grew neighbours, the view fits the whole drawing if that is readable.
   */
  anchor?: string | null
}

/**
 * A lineage drawing: opens where every card is readable, and never zooms out further
 * than that on its own — a drawing too big to read at that size opens on its root, with
 * an overview map to find the rest.
 */
export function LineageFlowCanvas<N extends Node>(props: LineageFlowCanvasProps<N>) {
  const [expanded, setExpanded] = useState(false)
  return (
    <div>
      <p className="sr-only">{props.description}</p>
      <ReactFlowProvider>
        <CanvasFrame {...props} onExpand={() => setExpanded(true)} />
      </ReactFlowProvider>
      {props.legend}
      {expanded &&
        createPortal(
          <Dialog open onOpenChange={setExpanded}>
            <DialogContent className="flex h-[calc(100vh-2rem)] max-w-none flex-col gap-2 p-3">
              <DialogTitle className="text-sm">{props.title}</DialogTitle>
              <ReactFlowProvider>
                <CanvasFrame {...props} fill />
              </ReactFlowProvider>
              {props.legend}
            </DialogContent>
          </Dialog>,
          document.body,
        )}
    </div>
  )
}

const NO_MOVES: ReadonlyMap<string, XYPosition> = new Map()

/** Room left between a card brought into view and the canvas's edge. */
const REVEAL_MARGIN = 24

/**
 * How far to pan along one axis so the span lo..hi is on a canvas `size` long: only as far as
 * it needs, and only as far as keeps keepLo..keepHi (the card the reader acted on) on it.
 * All in screen pixels; a positive shift moves the drawing right or down.
 */
export function revealShift(lo: number, hi: number, keepLo: number, keepHi: number, size: number): number {
  let shift = 0
  if (hi > size - REVEAL_MARGIN) shift = size - REVEAL_MARGIN - hi
  // Wider than the canvas: show where it starts.
  if (lo + shift < REVEAL_MARGIN) shift = REVEAL_MARGIN - lo
  if (shift < 0) shift = Math.max(shift, Math.min(0, REVEAL_MARGIN - keepLo))
  if (shift > 0) shift = Math.min(shift, Math.max(0, size - REVEAL_MARGIN - keepHi))
  return shift
}

function sizeOf(n: Node): { width: number; height: number } {
  return { width: n.width ?? n.measured?.width ?? 0, height: n.height ?? n.measured?.height ?? 0 }
}

function CanvasFrame<N extends Node>({
  nodes,
  edges,
  nodeTypes,
  edgeTypes,
  bounds,
  root,
  signature,
  layoutKey = signature,
  onNodeClick,
  minimapColor,
  pending = false,
  anchor = null,
  fill = false,
  onExpand,
}: LineageFlowCanvasProps<N> & { fill?: boolean; onExpand?: () => void }) {
  const dark = useTheme().resolvedTheme === "dark"
  const { zoomIn, zoomOut, setViewport, getViewport } = useReactFlow<N, Edge>()
  const wrapperRef = useRef<HTMLDivElement>(null)
  const [size, setSize] = useState<{ width: number; height: number } | null>(null)
  const [locked, setLocked] = useState(true)
  // Cards the reader moved, for this layout only: the next one puts every card in its place.
  const [moved, setMoved] = useState<{ layoutKey: string; at: ReadonlyMap<string, XYPosition> }>({
    layoutKey,
    at: NO_MOVES,
  })
  const movedAt = moved.layoutKey === layoutKey ? moved.at : NO_MOVES

  const canvasSize = () => ({ width: wrapperRef.current?.clientWidth ?? 0, height: wrapperRef.current?.clientHeight ?? 0 })
  const firstView = () => initialViewport(bounds, root, canvasSize())

  const onInit = (instance: ReactFlowInstance<N, Edge>) => {
    const measured = canvasSize()
    void instance.setViewport(initialViewport(bounds, root, measured))
    setSize(measured)
  }
  // The map is for finding what the first view leaves out.
  const overview = size !== null && size.width > 0 && size.height > 0 && fitZoom(bounds, size) < MIN_READABLE_FIT_ZOOM

  const drawn = useMemo(
    () =>
      movedAt.size === 0
        ? nodes
        : nodes.map((n) => {
            const at = movedAt.get(n.id)
            return at ? { ...n, position: at } : n
          }),
    [nodes, movedAt],
  )

  const onNodesChange = (changes: NodeChange<N>[]) => {
    const drags = changes.flatMap((c) => (c.type === "position" && c.position ? [[c.id, c.position] as const] : []))
    if (drags.length === 0) return
    setMoved((prev) => {
      const at = new Map(prev.layoutKey === layoutKey ? prev.at : NO_MOVES)
      for (const [id, position] of drags) at.set(id, position)
      return { layoutKey, at }
    })
  }

  // Keep the anchor card still on screen when a new layout moves it: from where the reader
  // last saw it, dragged or not, to where the new layout puts it. A drag only moves the card.
  // A layout that added cards (a +N) is fitted whole instead, when it can be read whole. One
  // too big for that zooms out only to the readable size, keeping the anchor still, then
  // brings the added cards into view as far as that keeps the anchor in view.
  const anchorAt = anchor ? (movedAt.get(anchor) ?? nodes.find((n) => n.id === anchor)?.position) : undefined
  const lastLayout = useRef<{
    signature: string
    layoutKey: string
    ids: ReadonlySet<string>
    anchor: { id: string; x: number; y: number } | null
  } | null>(null)
  useLayoutEffect(() => {
    const prev = lastLayout.current
    lastLayout.current = {
      signature,
      layoutKey,
      ids: new Set(nodes.map((n) => n.id)),
      anchor: anchor && anchorAt ? { id: anchor, x: anchorAt.x, y: anchorAt.y } : null,
    }
    if (!prev?.anchor || !anchor || !anchorAt || prev.signature !== signature || prev.anchor.id !== anchor) return
    if (prev.layoutKey === layoutKey) return
    const view = getViewport()
    const width = wrapperRef.current?.clientWidth ?? 0
    const height = wrapperRef.current?.clientHeight ?? 0
    const added = nodes.filter((n) => !prev.ids.has(n.id))
    const anchorNode = nodes.find((n) => n.id === anchor)
    const grew = added.length > 0 && anchorNode !== undefined && width > 0 && height > 0
    if (grew && fitZoom(bounds, { width, height }) >= MIN_READABLE_FIT_ZOOM) {
      void setViewport(initialViewport(bounds, null, { width, height }))
      return
    }

    const zoom = grew ? Math.min(view.zoom, MIN_READABLE_FIT_ZOOM) : view.zoom
    let x = view.x + prev.anchor.x * view.zoom - anchorAt.x * zoom
    let y = view.y + prev.anchor.y * view.zoom - anchorAt.y * zoom
    if (grew) {
      const boxes = added.map((n) => ({ ...n.position, ...sizeOf(n) }))
      const [left, top] = [Math.min(...boxes.map((b) => b.x)), Math.min(...boxes.map((b) => b.y))]
      const [right, bottom] = [Math.max(...boxes.map((b) => b.x + b.width)), Math.max(...boxes.map((b) => b.y + b.height))]
      const keep = sizeOf(anchorNode)
      x += revealShift(left * zoom + x, right * zoom + x, anchorAt.x * zoom + x, (anchorAt.x + keep.width) * zoom + x, width)
      y += revealShift(top * zoom + y, bottom * zoom + y, anchorAt.y * zoom + y, (anchorAt.y + keep.height) * zoom + y, height)
    }
    if (x !== view.x || y !== view.y || zoom !== view.zoom) void setViewport({ x, y, zoom })
  }, [signature, layoutKey, nodes, bounds, anchor, anchorAt, getViewport, setViewport])

  const resetView = () => {
    setMoved({ layoutKey, at: NO_MOVES })
    void setViewport(firstView())
  }

  const controlClass = "h-7 w-7 bg-white dark:bg-zinc-900"
  return (
    <div className={fill ? "relative min-h-0 flex-1" : "relative"}>
      <div
        ref={wrapperRef}
        data-lineage-canvas
        aria-hidden="true"
        className={fill ? "h-full w-full bg-zinc-50 dark:bg-zinc-950" : "h-[clamp(420px,65vh,760px)] w-full bg-zinc-50 dark:bg-zinc-950"}
      >
        <ReactFlow<N, Edge>
          key={signature}
          nodes={drawn}
          edges={edges}
          nodeTypes={nodeTypes}
          edgeTypes={edgeTypes}
          onInit={onInit}
          onNodesChange={onNodesChange}
          onNodeClick={(_, n) => onNodeClick(n.id)}
          colorMode={dark ? "dark" : "light"}
          nodesDraggable={!locked}
          nodesConnectable={false}
          nodesFocusable={false}
          edgesFocusable={false}
          elementsSelectable={false}
          disableKeyboardA11y
          zoomOnScroll={false}
          zoomOnDoubleClick={false}
          preventScrolling={false}
          minZoom={0.2}
          maxZoom={1.5}
          proOptions={{ hideAttribution: true }}
        >
          {overview && (
            <MiniMap<N>
              pannable
              zoomable
              nodeColor={minimapColor}
              nodeBorderRadius={4}
              className="!bg-white dark:!bg-zinc-900"
            />
          )}
        </ReactFlow>
      </div>
      {pending && (
        <div className="pointer-events-none absolute left-2 top-2 flex items-center gap-1.5 rounded-md border bg-white/90 px-2 py-1 text-xs text-muted-foreground dark:bg-zinc-900/90">
          <Loader2 className="h-3.5 w-3.5 animate-spin" aria-hidden />
          Laying out…
        </div>
      )}
      <div className="absolute right-2 top-2 flex flex-col gap-1">
        <Button size="icon" variant="outline" className={controlClass} aria-label="Zoom in" onClick={() => void zoomIn()}>
          <Plus className="h-3.5 w-3.5" aria-hidden />
        </Button>
        <Button size="icon" variant="outline" className={controlClass} aria-label="Zoom out" onClick={() => void zoomOut()}>
          <Minus className="h-3.5 w-3.5" aria-hidden />
        </Button>
        <Button size="icon" variant="outline" className={controlClass} aria-label="Reset view" onClick={resetView}>
          <RotateCcw className="h-3.5 w-3.5" aria-hidden />
        </Button>
        <Button
          size="icon"
          variant={locked ? "outline" : "secondary"}
          className={locked ? controlClass : "h-7 w-7"}
          aria-label={locked ? "Unlock to move cards" : "Lock cards in place"}
          aria-pressed={!locked}
          title={locked ? "Unlock to move cards" : "Lock cards in place"}
          onClick={() => setLocked((l) => !l)}
        >
          {locked ? <Lock className="h-3.5 w-3.5" aria-hidden /> : <LockOpen className="h-3.5 w-3.5" aria-hidden />}
        </Button>
        {onExpand && (
          <Button size="icon" variant="outline" className={controlClass} aria-label="Full screen" onClick={onExpand}>
            <Expand className="h-3.5 w-3.5" aria-hidden />
          </Button>
        )}
      </div>
    </div>
  )
}

/** Stands in for a canvas that has nothing laid out yet, at the same size. */
export function CanvasStatus({ children }: { children: ReactNode }) {
  return (
    <div className="flex h-[clamp(420px,65vh,760px)] w-full items-center justify-center gap-2 bg-zinc-50 text-sm text-muted-foreground dark:bg-zinc-950">
      {children}
    </div>
  )
}
