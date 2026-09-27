/**
 * The Steps/DAG graph's layout.
 *
 * A CDC pipeline's setup is a straight chain of eight steps. Laid out top to
 * bottom it was 1,454 px tall, so fitting it into the 540 px pane scaled every
 * node to 73 px wide (zoom 0.32) and no label could be read (seen live on
 * 2026-09-25, pipeline c228373b). A chain now wraps into rows of four that
 * snake — left to right, then right to left under the row's last step — so
 * eight steps take two rows and fit the pane at a readable zoom. A graph that
 * branches keeps the dagre top-to-bottom layout, where the branches read.
 */

import { describe, expect, it } from "vitest"

import { CHAIN_COLUMNS, NODE_HEIGHT, NODE_WIDTH, buildElements } from "@/components/pipeline/DAGVisualizationV2"
import type { ExecutionPlanStage } from "@/components/pipeline/dagTypes"

const stage = (id: string, dependencies?: string[]): ExecutionPlanStage =>
  ({ id, display_name: id, status: "complete", dependencies }) as ExecutionPlanStage

const setupSteps = ["validate", "discover", "connectors", "publication", "slot", "debezium", "sink", "verify"]

function bounds(nodes: { position: { x: number; y: number } }[]) {
  const xs = nodes.map((n) => n.position.x)
  const ys = nodes.map((n) => n.position.y)
  return {
    width: Math.max(...xs) - Math.min(...xs) + NODE_WIDTH,
    height: Math.max(...ys) - Math.min(...ys) + NODE_HEIGHT,
  }
}

// React Flow's fitView zoom (getViewportForBounds): the smaller of the two axes'
// fits, capped at the component's maxZoom.
function fitZoom(b: { width: number; height: number }, pane = { width: 1148, height: 538 }, padding = 0.15) {
  return Math.min(pane.width / (b.width * (1 + padding)), pane.height / (b.height * (1 + padding)), 1.2)
}

describe("Steps/DAG graph layout", () => {
  it("wraps an eight-step setup chain into two rows of four", () => {
    const { nodes } = buildElements(setupSteps.map((id) => stage(id)))
    const rows = new Set(nodes.map((n) => n.position.y))
    expect(rows.size).toBe(2)
    expect(nodes.filter((n) => n.position.y === Math.min(...rows))).toHaveLength(CHAIN_COLUMNS)
  })

  it("fits the pane at a zoom where the labels can be read", () => {
    const { nodes } = buildElements(setupSteps.map((id) => stage(id)))
    // Top to bottom this was 0.32.
    expect(fitZoom(bounds(nodes))).toBeGreaterThan(0.85)
  })

  it("snakes: the second row starts under the first row's last step and runs back", () => {
    const { nodes } = buildElements(setupSteps.map((id) => stage(id)))
    const at = (id: string) => nodes.find((n) => n.id === id)!.position
    expect(at("slot").x).toBe(at("publication").x)
    expect(at("slot").y).toBeGreaterThan(at("publication").y)
    expect(at("debezium").x).toBeLessThan(at("slot").x)
    expect(at("verify").x).toBe(at("validate").x)
  })

  it("routes each edge between the facing sides of its two steps", () => {
    const { edges } = buildElements(setupSteps.map((id) => stage(id)))
    const edge = (from: string, to: string) => edges.find((e) => e.source === from && e.target === to)!
    // Along the first row: right side to left side.
    expect(edge("validate", "discover")).toMatchObject({ sourceHandle: "right", targetHandle: "left" })
    // Down the turn.
    expect(edge("publication", "slot")).toMatchObject({ sourceHandle: "bottom", targetHandle: "top" })
    // Back along the second row: left side to right side.
    expect(edge("slot", "debezium")).toMatchObject({ sourceHandle: "left-out", targetHandle: "right-in" })
  })

  it("follows declared dependencies, not array order, when a chain is declared", () => {
    const { nodes } = buildElements([stage("c", ["b"]), stage("a", []), stage("b", ["a"])])
    const x = (id: string) => nodes.find((n) => n.id === id)!.position.x
    expect(x("a")).toBeLessThan(x("b"))
    expect(x("b")).toBeLessThan(x("c"))
  })

  it("keeps a branching graph top to bottom", () => {
    const { nodes, edges } = buildElements([
      stage("orders"),
      stage("customers", []),
      stage("join", ["orders", "customers"]),
      stage("warehouse", ["join"]),
    ])
    const y = (id: string) => nodes.find((n) => n.id === id)!.position.y
    expect(y("orders")).toBe(y("customers"))
    expect(y("join")).toBeGreaterThan(y("orders"))
    expect(y("warehouse")).toBeGreaterThan(y("join"))
    for (const e of edges) expect(e).toMatchObject({ sourceHandle: "bottom", targetHandle: "top" })
  })
})
